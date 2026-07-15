import { formatToolResult } from "./human_format.js?v=20260714.7";

export const TOOL_CALL_PREFIX = "KENNEDY_TOOL_CALLS";

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
        const trailing = value.slice(index + 1).trim();
        if (trailing) throw Object.assign(new Error("A tool request cannot contain commentary or any other text after the JSON object's final brace."), { code: "invalid_tool_protocol" });
        return value.slice(0, index + 1);
      }
      if (depth < 0) break;
    }
  }
  return value;
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

function validateObject(args, keys) {
  if (!args || typeof args !== "object" || Array.isArray(args)) throw Object.assign(new Error("Arguments must be a JSON object."), { code: "invalid_arguments" });
  const actual = Object.keys(args);
  if (actual.length !== keys.length || keys.some(key => !actual.includes(key))) throw Object.assign(new Error(`Expected exactly: ${keys.join(", ")}.`), { code: "invalid_arguments" });
}
function integer(value, name) { if (!Number.isInteger(value) || value < 1) throw Object.assign(new Error(`${name} must be a positive integer.`), { code: "invalid_arguments" }); return value; }
function integerArray(value, name, minimum = 0) { if (!Array.isArray(value) || value.length < minimum) throw Object.assign(new Error(`${name} must contain at least ${minimum} identifiers.`), { code: "invalid_arguments" }); value.forEach((v, i) => integer(v, `${name}[${i}]`)); if (new Set(value).size !== value.length) throw Object.assign(new Error(`${name} must not contain duplicates.`), { code: "invalid_arguments" }); return value; }
function string(value, name) { if (typeof value !== "string") throw Object.assign(new Error(`${name} must be a string.`), { code: "invalid_arguments" }); return value; }
function nonemptyString(value, name, maximum) { string(value, name); const trimmed = value.trim(); if (!trimmed || [...trimmed].length > maximum) throw Object.assign(new Error(`${name} must contain between 1 and ${maximum} characters.`), { code: "invalid_arguments" }); return trimmed; }
function choice(value, name, choices) { string(value, name); if (!choices.includes(value)) throw Object.assign(new Error(`${name} must be one of: ${choices.join(", ")}.`), { code: "invalid_arguments" }); return value; }

export class ToolExecutor {
  constructor({ mode, context, api, intelligence = null, provider = null, model = null, modelAttribution = "unknown-model-unknown-thinking", provenanceId = null, loadLimit, onUpdate = () => {} }) {
    this.mode = mode; this.context = context; this.api = api; this.intelligence = intelligence; this.provider = provider; this.model = model; this.provenanceId = provenanceId;
    this.modelAttribution = modelAttribution; this.loadLimit = loadLimit; this.loadCalls = 0; this.toolLog = []; this.onUpdate = onUpdate;
  }

  resetLoadCalls() { this.loadCalls = 0; }

  fullDurable(identifier) {
    const id = this.context.resolve(identifier);
    if (!this.context.fullNodeIds.has(id)) throw Object.assign(new Error(`Identifier ${identifier} is only a connection summary; load it before using this tool.`), { code: "node_not_full" });
    return id;
  }

  resultMessage(call, content) {
    const displayRole = call.name === "WebSearch" || call.name === "WebFetch" ? "Web tool result" : "Memory tool result";
    return {
      role: "user",
      display_role: displayRole,
      content: ["Kennedy tool result", `Tool: ${call.name}`, "", formatToolResult(call.name, content)].join("\n"),
    };
  }

  failure(call, code, message) {
    const result = this.resultMessage(call, { ok: false, error: { code, message } });
    this.toolLog.push({ name: call.name, arguments: call.arguments, ok: false, code, durationMs: 0 });
    this.onUpdate();
    return { message: result, reset: false };
  }

  async execute(call) {
    const started = performance.now();
    try {
      let outcome;
      switch (call.name) {
        case "LoadNode": outcome = await this.loadNode(call.arguments); break;
        case "ResetContext": outcome = await this.resetContext(call.arguments); break;
        case "ConnectNodes": outcome = await this.connectNodes(call.arguments); break;
        case "ConsolidateFanout": outcome = await this.consolidateFanout(call.arguments); break;
        case "AssignTask": outcome = await this.assignTask(call.arguments); break;
        case "CreateNode": outcome = await this.createNode(call.arguments); break;
        case "UpdateNode": outcome = await this.updateNode(call.arguments); break;
        case "WebSearch": outcome = await this.webSearch(call.arguments); break;
        case "WebFetch": outcome = await this.webFetch(call.arguments); break;
        default: throw Object.assign(new Error(`Tool ${call.name} is not available.`), { code: "unknown_tool" });
      }
      const message = this.resultMessage(call, { ok: true, result: outcome.result });
      this.toolLog.push({ name: call.name, arguments: call.arguments, ok: true, durationMs: Math.round(performance.now() - started) });
      this.onUpdate();
      return { message, reset: Boolean(outcome.reset) };
    } catch (error) {
      const code = error.code || "tool_failed";
      const message = error.message || "Tool execution failed.";
      this.toolLog.push({ name: call.name, arguments: call.arguments, ok: false, code, message, durationMs: Math.round(performance.now() - started) });
      this.onUpdate();
      return { message: this.resultMessage(call, { ok: false, error: { code, message } }), reset: false };
    }
  }

  async loadNode(args) {
    validateObject(args, ["identifier"]); integer(args.identifier, "identifier");
    this.loadCalls += 1;
    if (this.loadCalls > this.loadLimit) throw Object.assign(new Error(`LoadNode budget of ${this.loadLimit} is exhausted.`), { code: "load_budget_exhausted" });
    const durable = this.context.resolve(args.identifier);
    return { result: await this.context.loadDurable(durable) };
  }

  async resetContext(args) {
    validateObject(args, ["identifiers"]); integerArray(args.identifiers, "identifiers");
    const durable = args.identifiers.map(id => this.context.resolve(id));
    return { result: await this.context.reset(durable), reset: true };
  }

  async connectNodes(args) {
    this.assertIngress(); validateObject(args, ["identifiers"]); integerArray(args.identifiers, "identifiers", 2);
    const durable = args.identifiers.map(id => this.fullDurable(id));
    const payload = await this.api.connect(durable, this.modelAttribution);
    this.context.refresh(payload.nodes);
    return { result: { nodes: payload.nodes.map(node => this.context.toContextNode(node)) } };
  }

  async consolidateFanout(args) {
    this.assertIngress(); validateObject(args, ["parentIdentifier", "aggregatorIdentifier", "fanoutIdentifiers"]);
    integer(args.parentIdentifier, "parentIdentifier"); integer(args.aggregatorIdentifier, "aggregatorIdentifier");
    integerArray(args.fanoutIdentifiers, "fanoutIdentifiers", 1);
    const parentId = this.fullDurable(args.parentIdentifier);
    const aggregatorId = this.fullDurable(args.aggregatorIdentifier);
    const fanoutIds = args.fanoutIdentifiers.map(id => this.context.resolve(id));
    const payload = await this.api.consolidateFanout({ parent_node_id: parentId, aggregator_node_id: aggregatorId, fanout_node_ids: fanoutIds, model_attribution: this.modelAttribution });
    this.context.recordModelAttribution([parentId, aggregatorId, ...fanoutIds], this.modelAttribution);
    this.context.refresh(payload.nodes);
    return { result: { nodes: payload.nodes.map(node => this.context.toContextNode(node)) } };
  }

  async assignTask(args) {
    this.assertIngress(); validateObject(args, ["parentIdentifier", "childIdentifier", "priority"]);
    integer(args.parentIdentifier, "parentIdentifier");
    if (args.childIdentifier !== "blank") integer(args.childIdentifier, "childIdentifier");
    if (!["high", "medium", "low"].includes(args.priority)) throw Object.assign(new Error("priority must be high, medium, or low."), { code: "invalid_arguments" });
    const parentId = this.fullDurable(args.parentIdentifier);
    const childId = args.childIdentifier === "blank" ? null : this.fullDurable(args.childIdentifier);
    const payload = await this.api.assignTask({ parent_node_id: parentId, child_node_id: childId, priority: args.priority, model_attribution: this.modelAttribution });
    const attributedIds = [parentId];
    if (childId) attributedIds.push(childId);
    if (payload.replaced_task?.id) attributedIds.push(payload.replaced_task.id);
    this.context.recordModelAttribution(attributedIds, this.modelAttribution);
    this.context.refresh([payload.node]);
    const replacedTask = payload.replaced_task ? { ...this.context.summary(payload.replaced_task), priority: payload.replaced_task.priority } : null;
    return { result: { node: this.context.toContextNode(payload.node), replacedTask, cleared: childId === null } };
  }

  assertIngress() { if (this.mode !== "ingress" || !this.provenanceId) throw Object.assign(new Error("This tool is only available during history ingress."), { code: "tool_unavailable" }); }
  assertConversationWeb() { if (this.mode !== "conversation" || !this.intelligence) throw Object.assign(new Error("This web tool is only available during a live conversation."), { code: "tool_unavailable" }); }

  async webSearch(args) {
    this.assertConversationWeb(); validateObject(args, ["question", "mode"]);
    const question = nonemptyString(args.question, "question", 4000);
    const mode = choice(args.mode, "mode", ["quality", "balanced", "fast"]);
    return { result: await this.intelligence.webSearch({ provider: this.provider, model: this.model, question, mode }) };
  }

  async webFetch(args) {
    this.assertConversationWeb(); validateObject(args, ["url"]);
    const url = nonemptyString(args.url, "url", 4096);
    return { result: await this.intelligence.webFetch({ url }) };
  }

  async createNode(args) {
    this.assertIngress(); validateObject(args, ["parentIdentifiers", "shortName", "shortDescription", "longDescription"]);
    integerArray(args.parentIdentifiers, "parentIdentifiers", 1);
    const parentIds = args.parentIdentifiers.map(id => this.fullDurable(id));
    const payload = await this.api.createNode({ provenance_id: this.provenanceId, model_attribution: this.modelAttribution, parent_node_ids: parentIds, short_name: string(args.shortName, "shortName"), short_description: string(args.shortDescription, "shortDescription"), long_description: string(args.longDescription, "longDescription") });
    this.context.refresh(payload.nodes || [payload.node]);
    return { result: { node: this.context.toContextNode(payload.node), historyNodeCreated: true } };
  }

  async updateNode(args) {
    this.assertIngress(); validateObject(args, ["identifier", "newShortName", "newShortDescription", "newLongDescription"]);
    integer(args.identifier, "identifier"); const durable = this.fullDurable(args.identifier);
    const payload = await this.api.updateNode(durable, { provenance_id: this.provenanceId, model_attribution: this.modelAttribution, short_name: string(args.newShortName, "newShortName"), short_description: string(args.newShortDescription, "newShortDescription"), long_description: string(args.newLongDescription, "newLongDescription") });
    this.context.refresh([payload.node]);
    return { result: { node: this.context.toContextNode(payload.node), historyNodeCreated: true } };
  }
}
