import { formatToolResult } from "./human_format.js?v=20260718.1";
import { elapsedMs, formatDuration } from "./timing.js?v=20260715.2";
import { newIdempotencyId } from "./api.js?v=20260718.3";

export const TOOL_CALL_PREFIX = "KENNEDY_TOOL_CALLS";
export const MAX_RESET_SELF_MESSAGE_CHARACTERS = 400_000;
export const MAX_SELF_TIME_HANDOFF_MESSAGE_CHARACTERS = 400_000;

function splitToolEnvelope(value) {
  if (!value.startsWith("{")) throw Object.assign(new Error("The tool request must contain one JSON object immediately after the marker."), { code: "invalid_tool_protocol" });
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = 0; index < value.length; index++) {
    const character = value[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') inString = true;
    else if (character === "{") depth += 1;
    else if (character === "}") {
      depth -= 1;
      if (depth === 0) {
        return value.slice(0, index + 1);
      }
      if (depth < 0) break;
    }
  }
  return value;
}

export function truncateToolResponse(content) {
  const trimmed = String(content || "").trim();
  if (!trimmed.startsWith(`${TOOL_CALL_PREFIX}\n`)) return content;
  const envelope = splitToolEnvelope(trimmed.slice(TOOL_CALL_PREFIX.length).trim());
  return `${TOOL_CALL_PREFIX}\n${envelope}`;
}

export function parseToolCalls(content) {
  if (typeof content !== "string") throw Object.assign(new Error("Kennedy returned no text."), { code: "invalid_tool_protocol" });
  const trimmed = content.trim();
  if (!trimmed.startsWith(TOOL_CALL_PREFIX)) {
    if (trimmed.includes(TOOL_CALL_PREFIX)) throw Object.assign(new Error(`${TOOL_CALL_PREFIX} must be the first text in a tool-request response, with no commentary before it.`), { code: "invalid_tool_protocol" });
    return null;
  }
  if (!trimmed.startsWith(`${TOOL_CALL_PREFIX}\n`)) throw Object.assign(new Error(`Tool requests must put JSON on the line after ${TOOL_CALL_PREFIX}.`), { code: "invalid_tool_protocol" });
  let envelope;
  try { envelope = JSON.parse(splitToolEnvelope(trimmed.slice(TOOL_CALL_PREFIX.length).trim())); }
  catch (error) {
    if (error?.code === "invalid_tool_protocol") throw error;
    throw Object.assign(new Error("The tool request after KENNEDY_TOOL_CALLS was not valid JSON."), { code: "invalid_tool_protocol" });
  }
  if (!envelope || typeof envelope !== "object" || Array.isArray(envelope) || Object.keys(envelope).length !== 1 || !Array.isArray(envelope.calls) || envelope.calls.length === 0) {
    throw Object.assign(new Error('The tool request must be exactly {"calls":[...]} with at least one call.'), { code: "invalid_tool_protocol" });
  }
  return envelope.calls.map((call, index) => {
    if (!call || typeof call !== "object" || Array.isArray(call) || Object.keys(call).length !== 2 || typeof call.name !== "string" || !call.arguments || typeof call.arguments !== "object" || Array.isArray(call.arguments)) {
      throw Object.assign(new Error(`Tool call ${index + 1} must contain exactly a string name and an object arguments field.`), { code: "invalid_tool_protocol" });
    }
    return { id: `text_call_${index + 1}`, name: call.name, arguments: call.arguments };
  });
}

function validateObject(args, keys, optionalKeys = []) {
  if (!args || typeof args !== "object" || Array.isArray(args)) throw Object.assign(new Error("Arguments must be a JSON object."), { code: "invalid_arguments" });
  const actual = Object.keys(args);
  const allowed = new Set([...keys, ...optionalKeys]);
  if (keys.some(key => !actual.includes(key)) || actual.some(key => !allowed.has(key))) {
    const optional = optionalKeys.length ? ` Optional: ${optionalKeys.join(", ")}.` : "";
    throw Object.assign(new Error(`Expected exactly: ${keys.join(", ")}.${optional}`), { code: "invalid_arguments" });
  }
}
function integer(value, name) { if (!Number.isInteger(value) || value < 1) throw Object.assign(new Error(`${name} must be a positive integer.`), { code: "invalid_arguments" }); return value; }
function integerArray(value, name, minimum = 0) { if (!Array.isArray(value) || value.length < minimum) throw Object.assign(new Error(`${name} must contain at least ${minimum} identifiers.`), { code: "invalid_arguments" }); value.forEach((v, i) => integer(v, `${name}[${i}]`)); if (new Set(value).size !== value.length) throw Object.assign(new Error(`${name} must not contain duplicates.`), { code: "invalid_arguments" }); return value; }
function string(value, name) { if (typeof value !== "string") throw Object.assign(new Error(`${name} must be a string.`), { code: "invalid_arguments" }); return value; }
function nonemptyString(value, name, maximum) { string(value, name); const trimmed = value.trim(); if (!trimmed || [...trimmed].length > maximum) throw Object.assign(new Error(`${name} must contain between 1 and ${maximum} characters.`), { code: "invalid_arguments" }); return trimmed; }
function nonemptyPreservedString(value, name, maximum) { string(value, name); if (!value.trim() || [...value].length > maximum) throw Object.assign(new Error(`${name} must contain between 1 and ${maximum} characters.`), { code: "invalid_arguments" }); return value; }
function choice(value, name, choices) { string(value, name); if (!choices.includes(value)) throw Object.assign(new Error(`${name} must be one of: ${choices.join(", ")}.`), { code: "invalid_arguments" }); return value; }

function fixedConnectionIds(node) {
  if (Array.isArray(node?.fixed_connections) && node.fixed_connections.every(value => typeof value === "string")) return [...node.fixed_connections];
  return (node?.fixed_connections || []).map(connection => connection.id);
}

function recentConnectionIds(node) {
  if (Array.isArray(node?.recent_connections)) return [...node.recent_connections];
  return [...(node?.active_connections || []), ...(node?.fanout_connections || [])].map(connection => connection.id);
}

export class ToolExecutor {
  constructor({ mode, context, api, intelligence = null, provider = null, model = null, modelAttribution = "unknown-model-unknown-thinking", provenanceId = null, loadLimit, sessionType = mode, onUpdate = () => {}, beforeMutation = async () => {}, endSession = null, toolGate = null, requestTimeoutSeconds = null }) {
    this.mode = mode; this.context = context; this.api = api; this.intelligence = intelligence; this.provider = provider; this.model = model; this.provenanceId = provenanceId;
    this.modelAttribution = modelAttribution; this.loadLimit = loadLimit; this.sessionType = sessionType; this.loadCalls = 0; this.toolLog = []; this.onUpdate = onUpdate;
    this.beforeMutation = beforeMutation; this.endSession = endSession; this.toolGate = toolGate; this.requestTimeoutSeconds = requestTimeoutSeconds;
  }

  resetLoadCalls() { this.loadCalls = 0; }

  consumeContextLoadBudget() {
    this.loadCalls += 1;
    if (this.loadCalls > this.loadLimit) throw Object.assign(new Error(`Context-loading budget of ${this.loadLimit} is exhausted.`), { code: "load_budget_exhausted" });
  }

  fullDurable(identifier) {
    const id = this.context.resolve(identifier);
    if (!this.context.fullNodeIds.has(id)) throw Object.assign(new Error(`Identifier ${identifier} is only a connection summary; load it before using this tool.`), { code: "node_not_full" });
    return id;
  }

  resultMessage(call, content, durationMs) {
    const displayRole = call.name === "WebSearch" || call.name === "WebFetch" ? "Web tool result" : "Memory tool result";
    return {
      role: "user",
      display_role: displayRole,
      tool_name: call.name,
      tool_result: content,
      content: [`Kennedy tool result · ${call.name} · ${formatDuration(durationMs)}`, "", formatToolResult(call.name, content)].join("\n"),
    };
  }

  record(entry) {
    this.toolLog.push(entry);
    const report = this.intelligence?.recordTiming?.({
      action: "tool",
      name: entry.name,
      status: entry.ok ? "ok" : "error",
      sessionType: this.sessionType,
      durationMs: entry.durationMs,
    });
    Promise.resolve(report).catch(() => {});
    this.onUpdate();
  }

  failure(call, code, message) {
    const durationMs = 0;
    const result = this.resultMessage(call, { ok: false, error: { code, message } }, durationMs);
    this.record({ name: call.name, arguments: call.arguments, ok: false, code, durationMs });
    return { message: result, reset: false, durationMs };
  }

  async execute(call, { signal = null, operationId = null } = {}) {
    const started = performance.now();
    try {
      if (signal?.aborted) throw Object.assign(new Error("Kennedy's response was stopped."), { code: "turn_stopped" });
      if (this.toolGate) await this.toolGate(call.name);
      let outcome;
      switch (call.name) {
        case "LoadNode": outcome = await this.loadNode(call.arguments); break;
        case "ResetContext": outcome = await this.resetContext(call.arguments); break;
        case "ConnectNodes": outcome = await this.connectNodes(call.arguments); break;
        case "ConsolidateFanout": outcome = await this.consolidateFanout(call.arguments); break;
        case "SetFixedConnection": outcome = await this.setFixedConnection(call.arguments); break;
        case "CreateNode": outcome = await this.createNode(call.arguments); break;
        case "UpdateNode": outcome = await this.updateNode(call.arguments); break;
        case "WebSearch": outcome = await this.webSearch(call.arguments, { signal, operationId }); break;
        case "WebFetch": outcome = await this.webFetch(call.arguments, { signal, operationId }); break;
        case "EndSelfTimeSession":
        case "EndFreeTimeSession": outcome = await this.endSelfTimeSession(call.arguments); break;
        default: throw Object.assign(new Error(`Tool ${call.name} is not available.`), { code: "unknown_tool" });
      }
      const durationMs = elapsedMs(started);
      const message = this.resultMessage(call, { ok: true, result: outcome.result }, durationMs);
      this.record({ name: call.name, arguments: call.arguments, ok: true, durationMs });
      return { message, reset: Boolean(outcome.reset), endSession: Boolean(outcome.endSession), selfMessage: outcome.selfMessage ?? null, resetHistoryEntry: outcome.resetHistoryEntry ?? null, previousContext: outcome.previousContext ?? null, durationMs };
    } catch (error) {
      if (signal?.aborted || error?.name === "AbortError" || ["operation_cancelled", "turn_stopped", "ingress_cancelled"].includes(error?.code)) throw error;
      const code = error.code || "tool_failed";
      const message = error.message || "Tool execution failed.";
      const durationMs = elapsedMs(started);
      this.record({ name: call.name, arguments: call.arguments, ok: false, code, message, durationMs });
      return { message: this.resultMessage(call, { ok: false, error: { code, message } }, durationMs), reset: false, durationMs };
    }
  }

  async loadNode(args) {
    validateObject(args, ["identifier"]); integer(args.identifier, "identifier");
    this.consumeContextLoadBudget();
    const durable = this.context.resolve(args.identifier);
    return { result: await this.context.loadDurable(durable) };
  }

  async resetContext(args) {
    validateObject(args, ["identifiers"], ["selfMessage"]); integerArray(args.identifiers, "identifiers");
    const selfMessage = Object.hasOwn(args, "selfMessage")
      ? nonemptyPreservedString(args.selfMessage, "selfMessage", MAX_RESET_SELF_MESSAGE_CHARACTERS)
      : null;
    this.consumeContextLoadBudget();
    const durable = args.identifiers.map(id => this.context.resolve(id));
    const retainedNodeNames = durable.map(id => this.context.nodesById.get(id)?.short_name || "Unnamed memory");
    const previousContext = this.context.snapshot();
    return {
      result: await this.context.reset(durable),
      reset: true,
      previousContext,
      selfMessage,
      resetHistoryEntry: { retainedNodeNames, budgetUsed: this.loadCalls, budgetLimit: this.loadLimit },
    };
  }

  async connectNodes(args) {
    this.assertWrite(); validateObject(args, ["identifiers"]); integerArray(args.identifiers, "identifiers", 2);
    const durable = args.identifiers.map(id => this.fullDurable(id));
    await this.beforeMutation();
    const nodes = [];
    for (const sourceId of durable) {
      const source = this.context.nodesById.get(sourceId);
      const promoted = durable.filter(id => id !== sourceId);
      const recent = [...promoted, ...recentConnectionIds(source).filter(id => !promoted.includes(id))];
      const payload = await this.writeStoredNode(sourceId, source, { recent_connections: recent });
      this.context.refresh([payload.node]);
      nodes.push(payload.node);
    }
    return { result: { nodes: nodes.map(node => this.context.toContextNode(node)) } };
  }

  async consolidateFanout(args) {
    this.assertWrite(); validateObject(args, ["parentIdentifier", "aggregatorIdentifier", "fanoutIdentifiers"]);
    integer(args.parentIdentifier, "parentIdentifier"); integer(args.aggregatorIdentifier, "aggregatorIdentifier");
    integerArray(args.fanoutIdentifiers, "fanoutIdentifiers", 1);
    const parentId = this.fullDurable(args.parentIdentifier);
    const aggregatorId = this.fullDurable(args.aggregatorIdentifier);
    const fanoutIds = args.fanoutIdentifiers.map(id => this.context.resolve(id));
    if (parentId === aggregatorId || fanoutIds.includes(parentId) || fanoutIds.includes(aggregatorId)) {
      throw Object.assign(new Error("The parent, aggregator, and moved fanout nodes must all be distinct."), { code: "invalid_arguments" });
    }
    await this.beforeMutation();
    const parent = this.context.nodesById.get(parentId);
    const aggregator = this.context.nodesById.get(aggregatorId);
    const parentRecent = recentConnectionIds(parent);
    const parentFanout = new Set((parent.fanout_connections || []).map(connection => connection.id));
    if (!parentFanout.has(aggregatorId)) throw Object.assign(new Error("The aggregator must currently be a fanout connection of the parent."), { code: "invalid_arguments" });
    if (fanoutIds.some(id => !parentFanout.has(id))) throw Object.assign(new Error("Every consolidated node must currently be a fanout connection of the parent."), { code: "invalid_arguments" });
    const parentPayload = await this.writeStoredNode(parentId, parent, { recent_connections: parentRecent.filter(id => !fanoutIds.includes(id)) });
    const aggregatorRecent = recentConnectionIds(aggregator).filter(id => !fanoutIds.includes(id));
    const aggregatorPayload = await this.writeStoredNode(aggregatorId, aggregator, { recent_connections: [...aggregatorRecent, ...fanoutIds] });
    const nodes = [parentPayload.node, aggregatorPayload.node];
    this.context.refresh(nodes);
    return { result: { nodes: nodes.map(node => this.context.toContextNode(node)) } };
  }

  async setFixedConnection(args) {
    this.assertWrite(); validateObject(args, ["parentIdentifier", "childIdentifier", "slot"]);
    integer(args.parentIdentifier, "parentIdentifier");
    if (args.childIdentifier !== "blank") integer(args.childIdentifier, "childIdentifier");
    if (![1, 2, 3].includes(args.slot)) throw Object.assign(new Error("slot must be 1, 2, or 3."), { code: "invalid_arguments" });
    const parentId = this.fullDurable(args.parentIdentifier);
    const childId = args.childIdentifier === "blank" ? null : this.fullDurable(args.childIdentifier);
    if (childId === parentId) throw Object.assign(new Error("A node cannot be its own fixed connection."), { code: "invalid_arguments" });
    await this.beforeMutation();
    const parent = this.context.nodesById.get(parentId);
    const fixed = fixedConnectionIds(parent);
    if (childId && args.slot > fixed.length + 1) {
      throw Object.assign(new Error("Fixed connection positions must remain contiguous."), { code: "invalid_arguments" });
    }
    const replacedId = fixed[args.slot - 1] || null;
    if (childId) {
      const withoutChild = fixed.filter(id => id !== childId);
      withoutChild.splice(Math.min(args.slot - 1, withoutChild.length), replacedId ? 1 : 0, childId);
      fixed.splice(0, fixed.length, ...withoutChild);
    } else if (replacedId) {
      fixed.splice(args.slot - 1, 1);
    }
    const payload = await this.writeStoredNode(parentId, parent, { fixed_connections: fixed });
    this.context.refresh([payload.node]);
    const replacedFixedConnection = replacedId && replacedId !== childId
      ? { ...this.context.summary({ id: replacedId }), slot: args.slot }
      : null;
    return { result: { node: this.context.toContextNode(payload.node), replacedFixedConnection, cleared: childId === null } };
  }

  assertWrite() { if (!["ingress", "free-time"].includes(this.mode) || !this.provenanceId) throw Object.assign(new Error("This tool is only available during history ingress or self time."), { code: "tool_unavailable" }); }
  assertWeb() { if (!["conversation", "ingress", "free-time"].includes(this.mode) || !this.intelligence) throw Object.assign(new Error("This web tool is not available in this session."), { code: "tool_unavailable" }); }

  async webSearch(args, { signal = null, operationId = null } = {}) {
    this.assertWeb(); validateObject(args, ["question", "mode"]);
    const question = nonemptyString(args.question, "question", 4000);
    const mode = choice(args.mode, "mode", ["quality", "balanced", "fast"]);
    const timeoutSeconds = this.requestTimeoutSeconds?.();
    return { result: await this.intelligence.webSearch({ provider: this.provider, model: this.model, question, mode, ...(timeoutSeconds ? { timeout_seconds: timeoutSeconds } : {}) }, { signal, operationId }) };
  }

  async webFetch(args, { signal = null, operationId = null } = {}) {
    this.assertWeb(); validateObject(args, ["url"]);
    const url = nonemptyString(args.url, "url", 4096);
    return { result: await this.intelligence.webFetch({ url }, { signal, operationId }) };
  }

  async createNode(args) {
    this.assertWrite(); validateObject(args, ["parentIdentifiers", "ownerIdentifier", "shortName", "shortDescription", "longDescription"]);
    integerArray(args.parentIdentifiers, "parentIdentifiers", 1);
    const parentIds = args.parentIdentifiers.map(id => this.fullDurable(id));
    integer(args.ownerIdentifier, "ownerIdentifier");
    const ownerRootId = this.fullDurable(args.ownerIdentifier);
    await this.beforeMutation();
    const payload = await this.api.createNode({ idempotency_id: newIdempotencyId(), provenance_id: this.provenanceId, model_attribution: this.modelAttribution, owner_node_id: ownerRootId, short_name: string(args.shortName, "shortName"), short_description: string(args.shortDescription, "shortDescription"), long_description: string(args.longDescription, "longDescription"), fixed_connections: [], recent_connections: parentIds });
    const refreshed = [payload.node];
    for (const parentId of parentIds) {
      const parent = this.context.nodesById.get(parentId);
      const recent = [payload.node.id, ...recentConnectionIds(parent).filter(id => id !== payload.node.id)];
      const parentPayload = await this.writeStoredNode(parentId, parent, { recent_connections: recent });
      refreshed.push(parentPayload.node);
    }
    this.context.refresh(refreshed);
    return { result: { node: this.context.toContextNode(payload.node), historyNodeCreated: true } };
  }

  async updateNode(args) {
    this.assertWrite(); validateObject(args, ["identifier", "ownerIdentifier", "newShortName", "newShortDescription", "newLongDescription"]);
    integer(args.identifier, "identifier"); const durable = this.fullDurable(args.identifier);
    integer(args.ownerIdentifier, "ownerIdentifier"); const ownerRootId = this.fullDurable(args.ownerIdentifier);
    await this.beforeMutation();
    const current = this.context.nodesById.get(durable);
    const payload = await this.writeStoredNode(durable, current, { owner_node_id: ownerRootId, short_name: string(args.newShortName, "newShortName"), short_description: string(args.newShortDescription, "newShortDescription"), long_description: string(args.newLongDescription, "newLongDescription") });
    this.context.refresh([payload.node]);
    return { result: { node: this.context.toContextNode(payload.node), historyNodeCreated: true } };
  }

  async writeStoredNode(id, node, overrides = {}) {
    return this.api.updateNode(id, {
      idempotency_id: newIdempotencyId(),
      provenance_id: this.provenanceId,
      model_attribution: this.modelAttribution,
      owner_node_id: node.owner_node_id || node.owner_root_node_id || "unowned",
      short_name: node.short_name,
      short_description: node.short_description,
      long_description: node.long_description,
      fixed_connections: fixedConnectionIds(node),
      recent_connections: recentConnectionIds(node),
      ...overrides,
    });
  }

  async endSelfTimeSession(args) {
    validateObject(args, [], ["message"]);
    const message = Object.hasOwn(args, "message")
      ? nonemptyPreservedString(args.message, "message", MAX_SELF_TIME_HANDOFF_MESSAGE_CHARACTERS)
      : null;
    if (this.mode !== "free-time" || typeof this.endSession !== "function") {
      throw Object.assign(new Error("This tool is only available during self time."), { code: "tool_unavailable" });
    }
    const result = await this.endSession(message);
    return { result, endSession: true };
  }
}
