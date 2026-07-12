import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { KwebContext } from "../public/js/kweb_context.js";
import { Chatend } from "../public/js/chatend.js";
import { ToolExecutor, parseToolCalls } from "../public/js/tools.js";
import { ConversationSession } from "../public/js/conversation.js";
import { inspectorText } from "../public/js/render.js";
import { ContinuationState, UsageTracker, runAgentLoop } from "../public/js/intelligence.js";
import { composePrompt, loadPromptManuals } from "../public/js/prompt_composer.js";
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

test("Kmap snapshot distinguishes direct loads from active expansions", async () => {
  const context = new KwebContext(new MockKweb([node(1, [2]), node(2)]), id(1));
  await context.initialize();
  let snapshot = context.snapshot();
  assert.deepEqual(snapshot.nodes.find(item => item.identifier === 1).contextSources, ["direct"]);
  assert.deepEqual(snapshot.nodes.find(item => item.identifier === 2).contextSources, ["active"]);
  await context.loadDurable(id(2));
  snapshot = context.snapshot();
  assert.deepEqual(snapshot.nodes.find(item => item.identifier === 2).contextSources, ["active", "direct"]);
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

test("WebSearch and WebFetch expose only minimal model-facing arguments", async () => {
  const calls = [];
  const intelligence = {
    webSearch: async body => { calls.push(["search", body]); return { answer: "Two candidates.", sources: [{ title: "Guide", url: "https://example.com/guide" }] }; },
    webFetch: async body => { calls.push(["fetch", body]); return { url: body.url, title: "Guide", retrieved_at: "2026-07-12T00:00:00Z", content_type: "text/html", content: "Page evidence.", truncated: false }; },
  };
  const executor = new ToolExecutor({ mode: "conversation", context: {}, api: {}, intelligence, provider: "primary", model: "model", loadLimit: 20 });
  const search = await executor.execute({ id: "search", name: "WebSearch", arguments: { question: "best brunch in El Salvador" } });
  const fetch = await executor.execute({ id: "fetch", name: "WebFetch", arguments: { url: "https://example.com/guide" } });
  assert.deepEqual(calls, [
    ["search", { provider: "primary", model: "model", question: "best brunch in El Salvador" }],
    ["fetch", { url: "https://example.com/guide" }],
  ]);
  assert.equal(search.message.display_role, "Web tool result");
  assert.match(search.message.content, /Web research completed/);
  assert.match(search.message.content, /https:\/\/example.com\/guide/);
  assert.match(fetch.message.content, /Readable page content:\n  Page evidence/);
});

test("web tools reject extra retrieval knobs and remain unavailable during ingress", async () => {
  const intelligence = { webSearch: async () => { throw new Error("must not run"); } };
  const conversation = new ToolExecutor({ mode: "conversation", context: {}, api: {}, intelligence, loadLimit: 20 });
  const extra = await conversation.execute({ id: "search", name: "WebSearch", arguments: { question: "topic", maxResults: 10 } });
  assert.match(extra.message.content, /Expected exactly: question/);
  const ingress = new ToolExecutor({ mode: "ingress", context: {}, api: {}, intelligence, provenanceId: "p", loadLimit: 50 });
  const unavailable = await ingress.execute({ id: "search", name: "WebSearch", arguments: { question: "topic" } });
  assert.match(unavailable.message.content, /only available during a live conversation/);
});

test("chatend reset retains clean session messages and drops old tool activity", async () => {
  const api = new MockKweb([node(1)]); const context = new KwebContext(api, id(1)); await context.initialize();
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "hello" }]);
  chatend.append({ role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}}]}' });
  const resetCall = { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"ResetContext","arguments":{"identifiers":[]}}]}' };
  const resetResult = { role: "user", display_role: "Memory tool result", content: "Memory context reset completed." };
  chatend.rebuildAfterReset(resetCall, resetResult);
  assert.equal(chatend.messages.some(message => message.content?.includes('"LoadNode"')), false);
  assert.equal(chatend.messages.some(message => message.content === "hello"), true);
  assert.equal(chatend.messages.at(-1).display_role, "Memory tool result");
});

test("transparent tool protocol parses multiple calls from one model response", () => {
  const calls = parseToolCalls('KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}},{"name":"ConnectNodes","arguments":{"identifiers":[1,2]}}]}');
  assert.equal(calls.length, 2);
  assert.equal(calls[0].name, "LoadNode");
  assert.deepEqual(calls[1].arguments, { identifiers: [1, 2] });
  assert.equal(parseToolCalls("A normal answer."), null);
});

test("agent loop executes multiple text tool calls before the next generation", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "work" }]);
  const requests = [];
  const intelligence = { generate: async request => {
    requests.push(request);
    if (requests.length === 1) return {
      status: "complete", response_id: "resp_1",
      message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"First","arguments":{}},{"name":"Second","arguments":{}}]}' },
      usage: { input_tokens: 100, output_tokens: 10, cached_tokens: 80, cache_write_tokens: 20, reasoning_tokens: 4 },
    };
    return { status: "complete", response_id: "resp_2", message: { role: "assistant", content: "Finished." }, usage: { input_tokens: 130, output_tokens: 5, cached_tokens: 100, cache_write_tokens: 0, reasoning_tokens: 0 } };
  } };
  const executed = [];
  const executor = {
    execute: async call => { executed.push(call.name); return { reset: false, message: { role: "user", display_role: "Memory tool result", content: `${call.name} completed.` } }; },
    failure: () => { throw new Error("unexpected failure"); },
  };
  const continuation = new ContinuationState("kennedy-test");
  const usage = new UsageTracker({ contextWindowTokens: 1000, maxInputTokens: 900 });
  assert.equal(usage.snapshot().contextRemaining, 1000);
  const answer = await runAgentLoop({ intelligence, provider: "p", model: "m", chatend, executor, continuation, usage });
  assert.equal(answer, "Finished.");
  assert.deepEqual(executed, ["First", "Second"]);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].previous_response_id, "resp_1");
  assert.deepEqual(requests[1].messages.map(message => message.content), ["First completed.", "Second completed."]);
  assert.equal("tools" in requests[0], false);
  assert.equal(usage.snapshot().totalCachedTokens, 180);
  assert.equal(usage.snapshot().contextTokens, 135);
  assert.equal(usage.snapshot().contextRemaining, 865);
  assert.equal(usage.snapshot().cacheReadPercent, (100 * 180) / 230);
});

test("ResetContext abandons continuation and resends the rebuilt full chatend", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "reset it" }]);
  const requests = [];
  const intelligence = { generate: async request => {
    requests.push(request);
    if (requests.length === 1) return { status: "complete", response_id: "resp_old", message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"ResetContext","arguments":{"identifiers":[]}}]}' }, usage: null };
    return { status: "complete", response_id: "resp_new", message: { role: "assistant", content: "Fresh context." }, usage: null };
  } };
  const executor = {
    execute: async () => ({ reset: true, message: { role: "user", display_role: "Memory tool result", content: "Memory context reset completed." } }),
    failure: () => { throw new Error("unexpected failure"); },
  };
  await runAgentLoop({ intelligence, provider: "p", model: "m", chatend, executor, continuation: new ContinuationState("kennedy-test"), usage: new UsageTracker() });
  assert.equal(requests[0].previous_response_id, null);
  assert.equal(requests[1].previous_response_id, null);
  assert.equal(requests[1].messages[0].role, "system");
  assert.equal(requests[1].messages.some(message => message.content === "Memory context reset completed."), true);
});

test("conversation provenance contains only clean dialog", () => {
  const session = new ConversationSession({});
  session.transcript = [{ role: "user", content: "Hi" }, { role: "kennedy", content: "Hello" }];
  assert.equal(session.serialize(), "David: Hi\n\nKennedy: Hello");
});

test("conversation checkpoints the pending query before any model request", async () => {
  const events = [];
  const kweb = new MockKweb([node(1)]);
  const intelligence = { generate: async () => {
    events.push("generate");
    return { status: "complete", response_id: "response", message: { role: "assistant", content: "Saved answer." }, usage: null };
  } };
  const session = new ConversationSession({
    kweb, intelligence, manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async state => events.push(state.pendingTurn ? "checkpoint-pending" : "checkpoint-complete"), onUpdate: () => {},
  });
  await session.initialize();
  await session.send("Saved question");
  assert.deepEqual(events, ["checkpoint-pending", "generate", "checkpoint-complete"]);
  assert.deepEqual(session.transcript.map(item => item.content), ["Saved question", "Saved answer."]);
});

test("restored pending conversation resumes from durable transcript and context", async () => {
  const kweb = new MockKweb([node(1), node(2)]);
  let generated = 0;
  const session = new ConversationSession({
    kweb,
    intelligence: { generate: async () => { generated += 1; return { status: "complete", response_id: "response", message: { role: "assistant", content: "Recovered answer." }, usage: null }; } },
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m", persist: async () => {}, onUpdate: () => {},
  });
  await session.initialize({ startedAt: "2026-07-12T00:00:00Z", transcript: [{ role: "user", content: "Interrupted query" }], loadedNodeIds: [id(1), id(2)], pendingTurn: true });
  assert.deepEqual(session.context.loadedNodeIds, [id(1), id(2)]);
  await session.resumePendingTurn();
  assert.equal(generated, 1);
  assert.equal(session.pendingTurn, false);
  assert.equal(session.transcript.at(-1).content, "Recovered answer.");
});

test("an answer is not exposed as complete when its durable checkpoint fails", async () => {
  const kweb = new MockKweb([node(1)]);
  let checkpoints = 0;
  const session = new ConversationSession({
    kweb,
    intelligence: { generate: async () => ({ status: "complete", response_id: "response", message: { role: "assistant", content: "Unsaved answer." }, usage: null }) },
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async () => { checkpoints += 1; if (checkpoints === 2) throw new Error("history unavailable"); }, onUpdate: () => {},
  });
  await session.initialize();
  await assert.rejects(() => session.send("Question"), /history unavailable/);
  assert.equal(session.pendingTurn, true);
  assert.deepEqual(session.transcript.map(item => item.content), ["Question"]);
  assert.equal(session.chatend.messages.some(message => message.content === "Unsaved answer."), false);
});

test("retry persists an initially failed pending checkpoint before generation", async () => {
  const kweb = new MockKweb([node(1)]);
  const events = [];
  let fail = true;
  const session = new ConversationSession({
    kweb,
    intelligence: { generate: async () => { events.push("generate"); return { status: "complete", response_id: "response", message: { role: "assistant", content: "Answer." }, usage: null }; } },
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async state => { events.push(state.pendingTurn ? "persist-pending" : "persist-complete"); if (fail) { fail = false; throw new Error("history unavailable"); } }, onUpdate: () => {},
  });
  await session.initialize();
  await assert.rejects(() => session.send("Question"), /history unavailable/);
  assert.deepEqual(events, ["persist-pending"]);
  await session.resumePendingTurn();
  assert.deepEqual(events, ["persist-pending", "persist-pending", "generate", "persist-complete"]);
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

test("chatend inspector system view excludes conversation and Kmap context", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const chatend = new Chatend("Agent manual text.", context, [{ role: "user", content: "Conversation provenance." }]);
  const diagnostic = { chatend: chatend.messages, memory: context.snapshot() };
  assert.match(inspectorText(diagnostic, "system"), /Agent manual text/);
  assert.equal(inspectorText(diagnostic, "system").includes("Conversation provenance"), false);
  assert.equal(inspectorText(diagnostic, "system").includes("Current Kmap context"), false);
  assert.match(inspectorText(diagnostic, "memory"), /Current Kmap context/);
});

test("system prompt loader requests the renamed Kmap manual", async () => {
  const originalFetch = globalThis.fetch;
  const requested = [];
  globalThis.fetch = async path => {
    requested.push(path);
    return { ok: true, text: async () => path };
  };
  try {
    await loadPromptManuals("/base");
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.deepEqual(requested.sort(), [
    "/base/system-prompts/ConversationAgentManual.txt",
    "/base/system-prompts/HistoryIngressAgentManual.txt",
    "/base/system-prompts/KmapAgentManual.txt",
  ]);
});

test("chatend inspector exposes text tool requests and readable results", () => {
  const chatend = [
    { role: "system", content: "Readable instructions." },
    { role: "user", content: "Hello." },
    { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}}]}' },
    { role: "user", display_role: "Memory tool result", content: "Memory load completed.\n\nNode 2: Project" },
    { role: "assistant", content: "Here is what I found." },
  ];
  const rendered = inspectorText({ chatend, context: { privateDiagnostic: true }, toolLog: [{ name: "LoadNode" }] });
  assert.match(rendered, /System context\n\nReadable instructions/);
  assert.match(rendered, /David\n\nHello/);
  assert.match(rendered, /Memory tool result\n\nMemory load completed/);
  assert.match(rendered, /Kennedy\n\nHere is what I found/);
  assert.match(rendered, /KENNEDY_TOOL_CALLS/);
  assert.match(rendered, /"LoadNode"/);
  assert.equal(rendered.includes("privateDiagnostic"), false);
});

test("production frontend never uses HTML string insertion", async () => {
  const files = ["render.js", "memory_explorer.js", "app.js"];
  for (const file of files) {
    const source = await readFile(new URL(`../public/js/${file}`, import.meta.url), "utf8");
    assert.equal(source.includes("innerHTML"), false, `${file} uses innerHTML`);
  }
});

test("frontend exposes full, system, and memory inspector controls", async () => {
  const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
  for (const id of ["usage-metrics", "inspector-full", "inspector-system", "inspector-memory"]) assert.match(html, new RegExp(`id="${id}"`));
  assert.match(html, /\/js\/app\.js\?v=\d{8}\.\d+/);
  assert.match(html, /\/css\/styles\.css\?v=\d{8}\.\d+/);
});
