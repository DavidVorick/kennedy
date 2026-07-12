import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { KwebContext } from "../public/js/kweb_context.js";
import { Chatend } from "../public/js/chatend.js";
import { ToolExecutor } from "../public/js/tools.js";
import { ConversationSession } from "../public/js/conversation.js";
import { inspectorText } from "../public/js/render.js";
import { composePrompt } from "../public/js/prompt_composer.js";
import { formatKmapContext } from "../public/js/human_format.js";

const id = n => n.toString(16).padStart(40, "0");
const summary = n => ({ id: id(n), short_name: `Node ${n}`, short_description: `Summary ${n}` });
const node = (n, active = [], fanout = []) => ({ id: id(n), short_name: `Node ${n}`, short_description: `Summary ${n}`, long_description: `Details ${n}`, active_connections: active.map(summary), fanout_connections: fanout.map(summary), history_head_id: id(100 + n) });

class MockKweb {
  constructor(nodes) { this.nodes = new Map(nodes.map(n => [n.id, n])); this.connected = null; }
  async context(nodeId) { const requested = this.nodes.get(nodeId); return { requested_node: requested, active_connection_nodes: requested.active_connections.map(item => this.nodes.get(item.id)) }; }
  async connect(nodeIds) { this.connected = nodeIds; return { nodes: nodeIds.map(nodeId => this.nodes.get(nodeId)) }; }
}

test("short IDs are stable within a context and reset from one", async () => {
  const api = new MockKweb([node(1, [2]), node(2), node(3)]);
  const context = new KwebContext(api, id(1));
  await context.initialize();
  assert.equal(context.shortId(id(1)), 1);
  assert.equal(context.shortId(id(2)), 2);
  assert.equal(context.shortId(id(2)), 2);
  await context.reset([id(3)]);
  assert.equal(context.shortId(id(1)), 1);
  assert.equal(context.resolve(context.shortId(id(3))), id(3));
});

test("seven direct loads are enforced", async () => {
  const nodes = Array.from({ length: 8 }, (_, index) => node(index + 1));
  const context = new KwebContext(new MockKweb(nodes), id(1));
  await context.initialize();
  for (let n = 2; n <= 7; n++) await context.loadDurable(id(n));
  await assert.rejects(() => context.loadDurable(id(8)), error => error.code === "loaded_node_limit");
});

test("LoadNode attempts consume the tool budget, including failures", async () => {
  const api = new MockKweb([node(1, [2]), node(2)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  const executor = new ToolExecutor({ mode: "conversation", context, api, loadLimit: 1 });
  const first = await executor.execute({ id: "a", name: "LoadNode", arguments: { identifier: 999 } });
  assert.match(first.message.content, /Unknown memory identifier 999/);
  const second = await executor.execute({ id: "b", name: "LoadNode", arguments: { identifier: 2 } });
  assert.match(second.message.content, /LoadNode budget of 1 is exhausted/);
  assert.equal(executor.loadCalls, 2);
});

test("ConnectNodes translates short IDs to durable IDs", async () => {
  const api = new MockKweb([node(1, [2]), node(2)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  const executor = new ToolExecutor({ mode: "conversation", context, api, loadLimit: 20 });
  const result = await executor.execute({ id: "a", name: "ConnectNodes", arguments: { identifiers: [1, 2] } });
  assert.match(result.message.content, /Memory connections updated/);
  assert.deepEqual(api.connected, [id(1), id(2)]);
});

test("chatend reset retains clean session messages and drops old tool activity", async () => {
  const api = new MockKweb([node(1)]); const context = new KwebContext(api, id(1)); await context.initialize();
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "hello" }]);
  chatend.append({ role: "assistant", content: null, tool_calls: [{ id: "old", name: "LoadNode", arguments: { identifier: 2 } }] });
  const resetCall = { role: "assistant", content: null, tool_calls: [{ id: "reset", name: "ResetContext", arguments: { identifiers: [] } }] };
  const resetResult = { role: "tool", tool_call_id: "reset", name: "ResetContext", content: { ok: true } };
  chatend.rebuildAfterReset(resetCall, resetResult);
  assert.equal(chatend.messages.some(message => message.tool_calls?.[0]?.id === "old"), false);
  assert.equal(chatend.messages.some(message => message.content === "hello"), true);
  assert.equal(chatend.messages.at(-1).tool_call_id, "reset");
});

test("conversation provenance contains only clean dialog", () => {
  const session = new ConversationSession({});
  session.transcript = [{ role: "user", content: "Hi" }, { role: "kennedy", content: "Hello" }];
  assert.equal(session.serialize(), "David: Hi\n\nKennedy: Hello");
});

test("Kmap context is readable text rather than JSON", async () => {
  const context = new KwebContext(new MockKweb([node(1, [2]), node(2)]), id(1));
  await context.initialize();
  const formatted = formatKmapContext(context.snapshot());
  assert.match(formatted, /Current Kmap context/);
  assert.match(formatted, /Node 1: Node 1/);
  assert.match(formatted, /Active connections:\n  - 2: Node 2/);
  assert.equal(formatted.includes('{'), false);
});

test("system prompt composition uses readable sections rather than markup wrappers", () => {
  const prompt = composePrompt({ shared: "Shared paragraph.", conversation: "Conversation paragraph.", ingress: "Ingress paragraph." }, "conversation");
  assert.equal(prompt, "Kennedy's shared instructions\n\nShared paragraph.\n\nConversation session instructions\n\nConversation paragraph.");
  assert.equal(prompt.includes("<kennedy_"), false);
});

test("chatend inspector shows only readable text and hides structured calls", () => {
  const chatend = [
    { role: "system", content: "Readable instructions." },
    { role: "user", content: "Hello." },
    { role: "assistant", content: null, tool_calls: [{ id: "call_1", name: "LoadNode", arguments: { identifier: 2 } }], provider_items: [{ type: "reasoning", encrypted_content: "opaque" }] },
    { role: "tool", content: "Memory load completed.\n\nNode 2: Project", tool_call_id: "call_1", name: "LoadNode" },
    { role: "assistant", content: "Here is what I found." },
  ];
  const rendered = inspectorText({ chatend, context: { privateDiagnostic: true }, toolLog: [{ name: "LoadNode" }] });
  assert.match(rendered, /System context\n\nReadable instructions/);
  assert.match(rendered, /David\n\nHello/);
  assert.match(rendered, /Memory context\n\nMemory load completed/);
  assert.match(rendered, /Kennedy\n\nHere is what I found/);
  assert.equal(rendered.includes("call_1"), false);
  assert.equal(rendered.includes("provider_items"), false);
  assert.equal(rendered.includes("privateDiagnostic"), false);
});

test("production frontend never uses HTML string insertion", async () => {
  const files = ["render.js", "memory_explorer.js", "app.js"];
  for (const file of files) {
    const source = await readFile(new URL(`../public/js/${file}`, import.meta.url), "utf8");
    assert.equal(source.includes("innerHTML"), false, `${file} uses innerHTML`);
  }
});
