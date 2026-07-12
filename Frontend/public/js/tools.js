import { formatToolResult } from "./human_format.js";

const scalar = (type) => ({ type });
const arrayOfIntegers = { type: "array", items: scalar("integer") };
const objectSchema = (properties, required = Object.keys(properties)) => ({ type: "object", properties, required, additionalProperties: false });

const BASE_TOOLS = [
  { name: "LoadNode", description: "Load one known kmap node and its active connections into context.", input_schema: objectSchema({ identifier: scalar("integer") }) },
  { name: "ResetContext", description: "Replace kmap context with the user root and selected known nodes.", input_schema: objectSchema({ identifiers: arrayOfIntegers }) },
  { name: "ConnectNodes", description: "Record that two or more full in-context nodes are useful together.", input_schema: objectSchema({ identifiers: arrayOfIntegers }) },
];

const INGRESS_TOOLS = [
  { name: "CreateNode", description: "Create a durable knowledge node from the current provenance and connect it to parents.", input_schema: objectSchema({ parentIdentifiers: arrayOfIntegers, shortName: scalar("string"), shortDescription: scalar("string"), longDescription: scalar("string") }) },
  { name: "UpdateNode", description: "Replace a knowledge node's descriptive fields using the current provenance.", input_schema: objectSchema({ identifier: scalar("integer"), newShortName: scalar("string"), newShortDescription: scalar("string"), newLongDescription: scalar("string") }) },
];

export function toolDefinitions(mode) { return mode === "ingress" ? [...BASE_TOOLS, ...INGRESS_TOOLS] : BASE_TOOLS; }

function validateObject(args, keys) {
  if (!args || typeof args !== "object" || Array.isArray(args)) throw Object.assign(new Error("Arguments must be a JSON object."), { code: "invalid_arguments" });
  const actual = Object.keys(args);
  if (actual.length !== keys.length || keys.some(key => !actual.includes(key))) throw Object.assign(new Error(`Expected exactly: ${keys.join(", ")}.`), { code: "invalid_arguments" });
}
function integer(value, name) { if (!Number.isInteger(value) || value < 1) throw Object.assign(new Error(`${name} must be a positive integer.`), { code: "invalid_arguments" }); return value; }
function integerArray(value, name, minimum = 0) { if (!Array.isArray(value) || value.length < minimum) throw Object.assign(new Error(`${name} must contain at least ${minimum} identifiers.`), { code: "invalid_arguments" }); value.forEach((v, i) => integer(v, `${name}[${i}]`)); if (new Set(value).size !== value.length) throw Object.assign(new Error(`${name} must not contain duplicates.`), { code: "invalid_arguments" }); return value; }
function string(value, name) { if (typeof value !== "string") throw Object.assign(new Error(`${name} must be a string.`), { code: "invalid_arguments" }); return value; }

export class ToolExecutor {
  constructor({ mode, context, api, provenanceId = null, loadLimit, onUpdate = () => {} }) {
    this.mode = mode; this.context = context; this.api = api; this.provenanceId = provenanceId;
    this.loadLimit = loadLimit; this.loadCalls = 0; this.toolLog = []; this.onUpdate = onUpdate;
  }

  resetLoadCalls() { this.loadCalls = 0; }

  fullDurable(identifier) {
    const id = this.context.resolve(identifier);
    if (!this.context.fullNodeIds.has(id)) throw Object.assign(new Error(`Identifier ${identifier} is only a connection summary; load it before using this tool.`), { code: "node_not_full" });
    return id;
  }

  resultMessage(call, content) { return { role: "tool", tool_call_id: call.id, name: call.name, content: formatToolResult(call.name, content) }; }

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
        case "CreateNode": outcome = await this.createNode(call.arguments); break;
        case "UpdateNode": outcome = await this.updateNode(call.arguments); break;
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
    validateObject(args, ["identifiers"]); integerArray(args.identifiers, "identifiers", 2);
    const durable = args.identifiers.map(id => this.fullDurable(id));
    const payload = await this.api.connect(durable);
    this.context.refresh(payload.nodes);
    return { result: { nodes: payload.nodes.map(node => this.context.toContextNode(node)) } };
  }

  assertIngress() { if (this.mode !== "ingress" || !this.provenanceId) throw Object.assign(new Error("This tool is only available during history ingress."), { code: "tool_unavailable" }); }

  async createNode(args) {
    this.assertIngress(); validateObject(args, ["parentIdentifiers", "shortName", "shortDescription", "longDescription"]);
    integerArray(args.parentIdentifiers, "parentIdentifiers", 1);
    const parentIds = args.parentIdentifiers.map(id => this.fullDurable(id));
    const payload = await this.api.createNode({ provenance_id: this.provenanceId, parent_node_ids: parentIds, short_name: string(args.shortName, "shortName"), short_description: string(args.shortDescription, "shortDescription"), long_description: string(args.longDescription, "longDescription") });
    this.context.refresh([payload.node]);
    return { result: { node: this.context.toContextNode(payload.node), historyNodeCreated: true } };
  }

  async updateNode(args) {
    this.assertIngress(); validateObject(args, ["identifier", "newShortName", "newShortDescription", "newLongDescription"]);
    integer(args.identifier, "identifier"); const durable = this.fullDurable(args.identifier);
    const payload = await this.api.updateNode(durable, { provenance_id: this.provenanceId, short_name: string(args.newShortName, "newShortName"), short_description: string(args.newShortDescription, "newShortDescription"), long_description: string(args.newLongDescription, "newLongDescription") });
    this.context.refresh([payload.node]);
    return { result: { node: this.context.toContextNode(payload.node), historyNodeCreated: true } };
  }
}
