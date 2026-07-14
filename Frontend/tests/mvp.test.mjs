import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { KwebContext } from "../public/js/kweb_context.js";
import { Chatend } from "../public/js/chatend.js";
import { ToolExecutor, parseToolCalls } from "../public/js/tools.js";
import { ConversationSession } from "../public/js/conversation.js";
import { runHistoryIngress } from "../public/js/history_ingress.js";
import { conversationControlState, conversationIngressActivity, conversationTitle, ingressMutationSummary, inspectorText } from "../public/js/render.js";
import { ContinuationState, UsageTracker, runAgentLoop } from "../public/js/intelligence.js";
import { composePrompt, formatModelAttribution, loadPromptManuals } from "../public/js/prompt_composer.js";
import { formatKmapContext } from "../public/js/human_format.js";
import { MemoryExplorer } from "../public/js/memory_explorer.js";
import { ConversationHistoryAPI, IntelligenceAPI, TelegramRelayAPI } from "../public/js/api.js";

const id = n => n.toString(16).padStart(40, "0");
const summary = n => ({ id: id(n), short_name: `Node ${n}`, short_description: `Summary ${n}` });
const node = (n, active = [], fanout = [], tasks = [], lastModifiedBy = "legacy-unknown") => ({ id: id(n), short_name: `Node ${n}`, short_description: `Summary ${n}`, long_description: `Details ${n}`, last_modified_by: lastModifiedBy, task_connections: tasks.map(([task, priority]) => ({ ...summary(task), priority })), active_connections: active.map(summary), fanout_connections: fanout.map(summary), history_head_id: id(100 + n) });

class MockKweb {
  constructor(nodes) { this.nodes = new Map(nodes.map(n => [n.id, n])); this.connected = null; }
  async context(nodeId) { const requested = this.nodes.get(nodeId); return { requested_node: requested, active_connection_nodes: requested.active_connections.map(item => this.nodes.get(item.id)) }; }
  async connect(nodeIds, modelAttribution) { this.connected = nodeIds; this.modelAttribution = modelAttribution; return { nodes: nodeIds.map(nodeId => ({ ...this.nodes.get(nodeId), last_modified_by: modelAttribution })) }; }
}

test("short IDs are stable within a context and reset from one", async () => {
  const api = new MockKweb([node(1, [3]), node(2), node(3)]);
  const context = new KwebContext(api, [id(1), id(2)]);
  await context.initialize();
  assert.equal(context.shortId(id(1)), 1);
  assert.equal(context.shortId(id(2)), 2);
  assert.equal(context.shortId(id(2)), 2);
  assert.equal(context.shortId(id(3)), 3);
  await context.reset([id(3)]);
  assert.equal(context.shortId(id(1)), 1);
  assert.equal(context.shortId(id(2)), 2);
  assert.deepEqual(context.loadedNodeIds, [id(1), id(2), id(3)]);
  assert.equal(context.resolve(context.shortId(id(3))), id(3));
});

test("both roots load automatically and survive every reset", async () => {
  const api = new MockKweb([node(1), node(2), node(3), node(4)]);
  const context = new KwebContext(api, [id(1), id(2)]);
  await context.initialize();
  assert.deepEqual(context.loadedNodeIds, [id(1), id(2)]);
  await context.loadDurable(id(3));
  await context.reset([id(4)]);
  assert.deepEqual(context.loadedNodeIds, [id(1), id(2), id(4)]);
  assert.deepEqual(context.snapshot().rootIdentifiers, [1, 2]);
  await assert.rejects(() => context.reset([id(1)]), error => error.code === "root_in_reset");
  await assert.rejects(() => context.reset([id(2)]), error => error.code === "root_in_reset");
});

test("memory explorer provides direct navigation to both Kmap roots", async () => {
  const explorer = new MemoryExplorer({
    api: {}, rootNodeIds: [id(1), id(2)], content: {}, backButton: {}, forwardButton: {},
  });
  const opened = [];
  explorer.open = async nodeId => { opened.push(nodeId); };
  await explorer.home();
  await explorer.kennedyHome();
  assert.deepEqual(opened, [id(1), id(2)]);
  assert.throws(
    () => new MemoryExplorer({ api: {}, rootNodeIds: [id(1), id(1)], content: {}, backButton: {}, forwardButton: {} }),
    /distinct user and Kennedy root node identifiers/,
  );
});

test("conversation history client permanently discards unstarted records", async () => {
  const originalFetch = globalThis.fetch;
  let request = null;
  globalThis.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ discarded: 2, discarded_ids: ["a", "b"] }),
    };
  };
  try {
    const result = await ConversationHistoryAPI("http://history").discardUnstarted();
    assert.deepEqual(result, { discarded: 2, discarded_ids: ["a", "b"] });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(request.url, "http://history/api/v1/conversations/unstarted");
  assert.equal(request.options.method, "DELETE");
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

test("legacy nodes without explicit task connections behave as though they have none", async () => {
  const legacy = node(1);
  delete legacy.task_connections;
  const context = new KwebContext(new MockKweb([legacy]), id(1));
  await context.initialize();
  assert.deepEqual(context.snapshot().nodes[0].taskConnections, []);
});

test("Kmap archives restore raw nodes and short identifiers exactly", async () => {
  const api = new MockKweb([node(1, [2]), node(2), node(3)]);
  const context = new KwebContext(api, id(1));
  await context.initialize();
  await context.loadDurable(id(3));
  const archived = context.archive();
  const restored = new KwebContext(api, id(1));
  restored.restore(archived);
  assert.deepEqual(restored.archive(), archived);
  assert.equal(restored.resolve(context.shortId(id(2))), id(2));
  assert.deepEqual(restored.snapshot(), context.snapshot());
});

test("ten direct loads are enforced across both root graphs", async () => {
  const nodes = Array.from({ length: 11 }, (_, index) => node(index + 1));
  const context = new KwebContext(new MockKweb(nodes), [id(1), id(2)]);
  await context.initialize();
  for (let n = 3; n <= 10; n++) await context.loadDurable(id(n));
  assert.equal(context.loadedNodeIds.length, 10);
  await assert.rejects(() => context.loadDurable(id(11)), error => error.code === "loaded_node_limit");
  await assert.rejects(() => context.reset(Array.from({ length: 9 }, (_, index) => id(index + 3))), error => error.code === "loaded_node_limit");
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
  const executor = new ToolExecutor({ mode: "ingress", context, api, provenanceId: "prov", modelAttribution: "gpt-test-xhigh", loadLimit: 20 });
  const result = await executor.execute({ id: "a", name: "ConnectNodes", arguments: { identifiers: [1, 2] } });
  assert.match(result.message.content, /Memory connections updated/);
  assert.deepEqual(api.connected, [id(1), id(2)]);
  assert.equal(api.modelAttribution, "gpt-test-xhigh");
  assert.match(result.message.content, /Last modified by: gpt-test-xhigh/);
});

test("ConsolidateFanout and AssignTask translate short IDs and refresh task connections", async () => {
  const api = new MockKweb([node(1, [], [2, 3, 4]), node(2), node(3), node(4)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  await context.loadDurable(id(2));
  api.consolidateFanout = async body => {
    api.consolidated = body;
    return { nodes: [node(1, [], [2], [], body.model_attribution), node(2, [], [3, 4], [], body.model_attribution)] };
  };
  api.assignTask = async body => {
    api.assigned = body;
    return { node: body.child_node_id ? node(1, [], [3, 4], [[2, body.priority]], body.model_attribution) : node(1, [], [3, 4], [], body.model_attribution), replaced_task: null };
  };
  const executor = new ToolExecutor({ mode: "ingress", context, api, provenanceId: "prov", modelAttribution: "gpt-test-xhigh", loadLimit: 20 });
  const consolidated = await executor.execute({ id: "a", name: "ConsolidateFanout", arguments: { parentIdentifier: 1, aggregatorIdentifier: 2, fanoutIdentifiers: [3, 4] } });
  assert.match(consolidated.message.content, /Fanout connections consolidated/);
  assert.deepEqual(api.consolidated, { parent_node_id: id(1), aggregator_node_id: id(2), fanout_node_ids: [id(3), id(4)], model_attribution: "gpt-test-xhigh" });
  assert.deepEqual(context.diagnostics().fullNodeIds.sort(), [id(1), id(2)].sort());

  const assigned = await executor.execute({ id: "b", name: "AssignTask", arguments: { parentIdentifier: 1, childIdentifier: 2, priority: "high" } });
  assert.match(assigned.message.content, /Task connection assigned/);
  assert.deepEqual(api.assigned, { parent_node_id: id(1), child_node_id: id(2), priority: "high", model_attribution: "gpt-test-xhigh" });
  assert.equal(context.snapshot().nodes.find(item => item.identifier === 1).taskConnections[0].priority, "high");

  const cleared = await executor.execute({ id: "c", name: "AssignTask", arguments: { parentIdentifier: 1, childIdentifier: "blank", priority: "high" } });
  assert.match(cleared.message.content, /Task slot cleared/);
  assert.deepEqual(api.assigned, { parent_node_id: id(1), child_node_id: null, priority: "high", model_attribution: "gpt-test-xhigh" });
});

test("CreateNode and UpdateNode add model attribution outside Kennedy's arguments", async () => {
  const api = new MockKweb([node(1)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  api.createNode = async body => {
    api.created = body;
    const created = node(2, [], [], [], body.model_attribution);
    return { node: created, nodes: [node(1, [2], [], [], body.model_attribution), created] };
  };
  api.updateNode = async (nodeId, body) => {
    api.updated = [nodeId, body];
    return { node: node(2, [], [], [], body.model_attribution) };
  };
  const executor = new ToolExecutor({ mode: "ingress", context, api, provenanceId: "prov", modelAttribution: "gpt-5.6-sol-xhigh", loadLimit: 20 });
  const createArguments = { parentIdentifiers: [1], shortName: "New Memory", shortDescription: "Summary.", longDescription: "Details." };
  const created = await executor.execute({ id: "create", name: "CreateNode", arguments: createArguments });
  assert.equal(api.created.model_attribution, "gpt-5.6-sol-xhigh");
  assert.equal("model_attribution" in createArguments, false);
  assert.match(created.message.content, /Last modified by: gpt-5.6-sol-xhigh/);

  const updateArguments = { identifier: 2, newShortName: "Updated Memory", newShortDescription: "Updated.", newLongDescription: "Updated details." };
  await executor.execute({ id: "update", name: "UpdateNode", arguments: updateArguments });
  assert.equal(api.updated[0], id(2));
  assert.equal(api.updated[1].model_attribution, "gpt-5.6-sol-xhigh");
  assert.equal("model_attribution" in updateArguments, false);
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

test("live conversations cannot mutate the Kmap", async () => {
  const api = new MockKweb([node(1, [2]), node(2)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  const executor = new ToolExecutor({ mode: "conversation", context, api, loadLimit: 20 });
  for (const call of [
    { name: "ConnectNodes", arguments: { identifiers: [1, 2] } },
    { name: "ConsolidateFanout", arguments: { parentIdentifier: 1, aggregatorIdentifier: 2, fanoutIdentifiers: [2] } },
    { name: "AssignTask", arguments: { parentIdentifier: 1, childIdentifier: 2, priority: "high" } },
    { name: "CreateNode", arguments: { parentIdentifiers: [1], shortName: "Task", shortDescription: "Task.", longDescription: "Task." } },
    { name: "UpdateNode", arguments: { identifier: 1, newShortName: "Root", newShortDescription: "Root.", newLongDescription: "Root." } },
  ]) {
    const result = await executor.execute({ id: call.name, ...call });
    assert.match(result.message.content, /only available during history ingress/);
  }
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

test("tool protocol rejects narration before or after an otherwise valid envelope", () => {
  const envelope = 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"WebSearch","arguments":{"question":"Compare {official} sources and escape \\\"quoted\\\" names."}}]}';
  assert.equal(parseToolCalls(`${envelope}\n  `)[0].name, "WebSearch");
  assert.throws(
    () => parseToolCalls(`${envelope}\nI’m looking this up now.`),
    /cannot contain commentary or any other text after the JSON object's final brace/,
  );
  assert.throws(
    () => parseToolCalls(`I’m looking this up now.\n${envelope}`),
    /must be the first text in a tool-request response/,
  );
  assert.throws(
    () => parseToolCalls(`\`\`\`json\n${envelope}\n\`\`\``),
    /must be the first text in a tool-request response/,
  );
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
  const checkpoints = [];
  assert.equal(usage.snapshot().contextRemaining, 1000);
  const answer = await runAgentLoop({
    intelligence, provider: "p", model: "m", chatend, executor, continuation, usage,
    checkpoint: async () => checkpoints.push(chatend.messages.map(message => message.content)),
  });
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
  assert.equal(checkpoints.length, 1);
  assert.equal(checkpoints[0].includes("First completed."), true);
  assert.equal(checkpoints[0].includes("Second completed."), true);
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

test("conversation provenance preserves the complete structured Chatend", async () => {
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]), intelligence: {},
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1),
    provider: "p", model: "m", onUpdate: () => {},
  });
  await session.initialize();
  session.transcript = [{ role: "user", content: "Hi" }, { role: "kennedy", content: "Hello" }];
  session.chatend.append({ role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":1}}]}' });
  session.chatend.append({ role: "user", display_role: "Memory tool result", content: "Kennedy tool result\nTool: LoadNode\n\nLoaded." });
  session.chatend.append({
    role: "user",
    content: [{ type: "input_text", text: "Look at this" }, { type: "input_image", image_url: "data:image/png;base64,AAAA" }],
  });
  const archive = JSON.parse(session.serialize());
  assert.equal(archive.format, "kennedy-chatend");
  assert.equal(archive.version, 2);
  assert.equal("modelAttribution" in archive, false);
  assert.match(archive.systemPrompt, /Shared/);
  assert.equal(archive.context.snapshot.nodes[0].longDescription, "Details 1");
  assert.match(archive.messages.find(message => typeof message.content === "string" && message.content.includes("KENNEDY_TOOL_CALLS")).content, /LoadNode/);
  assert.match(archive.messages.find(message => message.display_role === "Memory tool result").content, /Loaded/);
  assert.equal(archive.messages.at(-1).content[1].image_url, "data:image/png;base64,AAAA");
});

test("a structured Chatend archive retains activity while refreshing current manuals and context", async () => {
  const kweb = new MockKweb([node(1), node(2), node(3)]);
  const source = new ConversationSession({
    kweb, intelligence: {}, manuals: { shared: "Shared", conversation: "Conversation" },
    rootNodeId: id(1), provider: "p", model: "m", onUpdate: () => {},
  });
  await source.initialize();
  await source.context.loadDurable(id(2));
  source.chatend.append({ role: "assistant", content: "KENNEDY_TOOL_CALLS\n{\"calls\":[{\"name\":\"LoadNode\",\"arguments\":{\"identifier\":2}}]}" });
  source.chatend.append({ role: "user", display_role: "Memory tool result", content: "Kennedy tool result\nTool: LoadNode\n\nLoaded." });
  source.executor.loadCalls = 3;
  source.executor.toolLog.push({ name: "LoadNode", ok: true });
  source.usage.record({ input_tokens: 12, output_tokens: 3, cached_tokens: 4, cache_write_tokens: 0, reasoning_tokens: 1 });
  const saved = source.snapshot();

  const restored = new ConversationSession({
    kweb, intelligence: {}, manuals: { shared: "Changed", conversation: "Changed" },
    rootNodeIds: [id(1), id(3)], provider: "p", model: "m", onUpdate: () => {},
  });
  await restored.initialize(saved);
  assert.match(restored.chatend.messages[0].content, /Changed/);
  assert.equal(restored.chatend.messages.some(message => message.content?.includes?.("LoadNode")), true);
  assert.deepEqual(restored.context.loadedNodeIds, [id(1), id(2), id(3)]);
  assert.match(restored.chatend.messages.find(message => message.context_kind === "memory").content, /Always-loaded root identifiers: 1, 3/);
  assert.equal(restored.executor.loadCalls, 3);
  assert.deepEqual(restored.executor.toolLog, [{ name: "LoadNode", ok: true }]);
  assert.equal(restored.usage.snapshot().totalInputTokens, 12);
});

test("history ingress checkpoints its whole Chatend and completed archives resume without regeneration", async () => {
  const kweb = new MockKweb([node(1)]);
  kweb.provenance = async () => ({ source: "conversation", source_created_at: "2026-07-13T00:00:00Z", data: '{"format":"kennedy-chatend","media":[{"kind":"voice","dataUrl":"data:audio/ogg;base64,AAAA"}]}' });
  let generations = 0;
  const intelligence = { generate: async () => {
    generations += 1;
    return { status: "complete", response_id: "ingress-response", message: { role: "assistant", content: "Memory review complete." }, usage: null };
  } };
  const checkpoints = [];
  await runHistoryIngress({
    kweb, intelligence, manuals: { shared: "Shared", ingress: "Ingress" },
    rootNodeId: id(1), provenanceId: "provenance", provider: "p", model: "m",
    checkpoint: async archive => checkpoints.push(structuredClone(archive)), onUpdate: () => {},
  });
  assert.equal(generations, 1);
  assert.equal(checkpoints[0].completed, false);
  assert.equal(checkpoints.at(-1).completed, true);
  assert.equal("modelAttribution" in checkpoints.at(-1), false);
  assert.match(checkpoints.at(-1).systemPrompt, /Ingress/);
  assert.match(checkpoints.at(-1).retained[0].content, /Archived Chatend \(JSON\)/);
  assert.match(checkpoints.at(-1).retained[0].content, /Original audio retained in provenance/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /base64,AAAA/);
  assert.equal(checkpoints.at(-1).messages.at(-1).content, "Memory review complete.");
  assert.equal(checkpoints.at(-1).context.snapshot.nodes[0].longDescription, "Details 1");

  const resumed = [];
  await runHistoryIngress({
    kweb,
    intelligence: { generate: async () => { throw new Error("completed ingress must not regenerate"); } },
    manuals: { shared: "Changed", ingress: "Changed" }, rootNodeId: id(1),
    provenanceId: "provenance", provider: "p", model: "m",
    restoredArchive: checkpoints.at(-1), checkpoint: async archive => resumed.push(archive), onUpdate: () => {},
  });
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0].completed, true);
  assert.equal(resumed[0].messages.at(-1).content, "Memory review complete.");
});

test("conversation history titles use the first durable user message", () => {
  const record = { state: { transcript: [
    { role: "user", content: "  Plan   a long weekend in San Salvador with excellent coffee and museums  " },
    { role: "kennedy", content: "Let's do it." },
  ] } };
  assert.equal(conversationTitle(record, 32), "Plan a long weekend in San Salv…");
  assert.equal(conversationTitle({ state: { transcript: [] } }), "New conversation");
});

test("conversation sidebar distinguishes continuable and closed records", async () => {
  const render = await readFile(new URL("../public/js/render.js", import.meta.url), "utf8");
  assert.match(render, /active: "Live · Continue"/);
  assert.match(render, /ingress_pending: "Closed · Memory queued"/);
  assert.match(render, /complete: "Saved · Read only"/);
});

test("composer stays editable but cannot send during a conversation transition", () => {
  const controls = conversationControlState({
    hasSession: true, sessionBusy: false, transitionBusy: true,
    ingressRequired: true, pendingTurn: false, viewingHistory: false, transcriptLength: 0,
  });
  assert.equal(controls.inputDisabled, false);
  assert.equal(controls.sendDisabled, true);
  assert.equal(controls.endDisabled, true);
  assert.equal(controls.newDisabled, true);
});

test("next message stays editable but cannot send while Kennedy is working", () => {
  const controls = conversationControlState({
    hasSession: true, sessionBusy: true, transitionBusy: false,
    ingressRequired: false, pendingTurn: false, viewingHistory: false, transcriptLength: 1,
  });
  assert.equal(controls.inputDisabled, false);
  assert.equal(controls.sendDisabled, true);
  assert.equal(controls.endDisabled, true);
  assert.equal(controls.newDisabled, false);
});

test("a saved unanswered query is retryable only when no response is in flight", () => {
  const idle = conversationControlState({
    hasSession: true, sessionBusy: false, transitionBusy: false,
    pendingTurn: true, viewingHistory: false, transcriptLength: 1,
  });
  assert.equal(idle.endDisabled, false);
  assert.equal(idle.sendDisabled, true);

  const responding = conversationControlState({
    hasSession: true, sessionBusy: true, transitionBusy: false,
    pendingTurn: true, viewingHistory: false, transcriptLength: 1,
  });
  assert.equal(responding.endDisabled, true);
});

test("closed conversations do not render a message composer", async () => {
  const controls = conversationControlState({
    hasSession: false, sessionBusy: false, transitionBusy: false,
    pendingTurn: false, viewingHistory: true, transcriptLength: 0,
  });
  assert.equal(controls.composerHidden, true);
  assert.equal(controls.inputDisabled, true);
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  assert.match(app, /message_form\.classList\.toggle\("hidden", controls\.composerHidden\)/);
});

test("message composer supports manual resizing and a large editor mode", async () => {
  const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
  const styles = await readFile(new URL("../public/css/styles.css", import.meta.url), "utf8");
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  assert.match(html, /id="message-size-button"[^>]+aria-controls="message-input"/);
  assert.match(html, /id="message-resize-handle"[^>]+role="separator"[^>]+aria-controls="message-input"/);
  assert.match(styles, /\.composer textarea \{[^}]*max-height: min\(64vh,720px,calc\(100vh - 250px\)\);[^}]*resize: vertical;/s);
  assert.match(styles, /\.message-resize-handle \{[^}]*cursor: ns-resize;[^}]*touch-action: none;/s);
  assert.match(styles, /\.composer\.composer-expanded textarea \{ height: clamp\(320px,52vh,620px\); \}/);
  assert.match(app, /message_size_button\.addEventListener\("click"/);
  assert.match(app, /message_resize_handle\.addEventListener\("pointerdown"/);
  assert.match(app, /startHeight \+ composerResize\.startY - event\.clientY/);
  assert.match(app, /message_resize_handle\.addEventListener\("keydown"/);
  assert.doesNotMatch(styles, /max-height: 220px/);
});

test("history ingress activity belongs only to its selected conversation", () => {
  const archive = {
    format: "kennedy-chatend", sessionType: "history-ingress",
    messages: [{ role: "assistant", content: "Archived ingress." }], usage: { requests: 1 },
  };
  const record = { id: "old", phase: "complete", state: { historyIngress: archive } };
  assert.equal(conversationIngressActivity({ record: null, liveRecordId: "old", liveDiagnostic: {} }), null);
  const saved = conversationIngressActivity({ record });
  assert.equal(saved.active, false);
  assert.equal(saved.diagnostic.chatend.messages[0].content, "Archived ingress.");
  const liveDiagnostic = { chatend: { messages: [{ role: "assistant", content: "Live ingress." }] } };
  const live = conversationIngressActivity({ record: { ...record, phase: "ingress_in_progress" }, liveRecordId: "old", liveDiagnostic });
  assert.equal(live.active, true);
  assert.equal(live.diagnostic, liveDiagnostic);
  assert.equal(conversationIngressActivity({ record, dismissedId: "old" }), null);
});

test("history ingress summary counts only successful memory mutations", () => {
  const toolLog = [
    { name: "CreateNode", ok: true },
    { name: "CreateNode", ok: false },
    { name: "UpdateNode", ok: true },
    { name: "UpdateNode", ok: true },
    { name: "ConnectNodes", ok: true },
    { name: "LoadNode", ok: true },
  ];
  assert.deepEqual(ingressMutationSummary({ executor: { toolLog } }), {
    nodesAdded: 1,
    nodesUpdated: 2,
    connectCalls: 1,
  });
  const record = {
    id: "old", phase: "complete", state: { historyIngress: {
      format: "kennedy-chatend", sessionType: "history-ingress", messages: [], tools: { log: toolLog },
    } },
  };
  const archived = conversationIngressActivity({ record });
  assert.deepEqual(ingressMutationSummary(archived.diagnostic), {
    nodesAdded: 1,
    nodesUpdated: 2,
    connectCalls: 1,
  });
});

test("conversation checkpoints the pending query before any model request", async () => {
  const events = [];
  const metadata = [];
  const kweb = new MockKweb([node(1)]);
  const intelligence = { generate: async () => {
    events.push("generate");
    return { status: "complete", response_id: "response", message: { role: "assistant", content: "Saved answer." }, usage: null };
  } };
  const session = new ConversationSession({
    kweb, intelligence, manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async (state, details) => { events.push(state.pendingTurn ? "checkpoint-pending" : "checkpoint-complete"); metadata.push(details); }, onUpdate: () => {},
  });
  await session.initialize();
  await session.send("Saved question");
  assert.deepEqual(events, ["checkpoint-pending", "generate", "checkpoint-complete"]);
  assert.deepEqual(metadata, [{ userActivity: true }, {}]);
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

test("a restored user tail is retryable even when an older checkpoint omitted pendingTurn", async () => {
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]),
    intelligence: { generate: async () => ({ status: "complete", response_id: "response", message: { role: "assistant", content: "Recovered." }, usage: null }) },
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async () => {}, onUpdate: () => {},
  });
  await session.initialize({ transcript: [{ role: "user", content: "Unanswered" }], pendingTurn: false });
  assert.equal(session.pendingTurn, true);
  assert.equal(session.busy, false);
});

test("cold start leaves saved conversation retries under explicit user control", async () => {
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  assert.doesNotMatch(app, /for \(const record of activeRecords\)[\s\S]{0,240}resumeSavedQuery\(record\.id\)/);
  assert.match(app, /end_button\.addEventListener\("click", \(\) => selectedSession\(\)\?\.pendingTurn \? resumeSavedQuery\(\) : endConversation\(\)\)/);
});

test("a structured pending Chatend resumes from cold start without duplicating its user query", async () => {
  const kweb = new MockKweb([node(1)]);
  let saved;
  const interrupted = new ConversationSession({
    kweb,
    intelligence: { generate: async () => { throw new Error("connection lost"); } },
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async state => { saved = structuredClone(state); }, onUpdate: () => {},
  });
  await interrupted.initialize();
  await assert.rejects(() => interrupted.send("Cold-start query"), /connection lost/);
  assert.equal(saved.pendingTurn, true);
  assert.equal(saved.archive.messages.filter(message => message.content === "Cold-start query").length, 1);

  const requests = [];
  const restored = new ConversationSession({
    kweb,
    intelligence: { generate: async request => {
      requests.push(request);
      return { status: "complete", response_id: "response", message: { role: "assistant", content: "Recovered once." }, usage: null };
    } },
    manuals: { shared: "Changed", conversation: "Changed" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async () => {}, onUpdate: () => {},
  });
  await restored.initialize(saved);
  await restored.resumePendingTurn();
  assert.equal(requests.length, 1);
  assert.equal(requests[0].previous_response_id, null);
  assert.equal(requests[0].messages.filter(message => message.content === "Cold-start query").length, 1);
  assert.deepEqual(restored.transcript.map(item => item.content), ["Cold-start query", "Recovered once."]);
});

test("a failed mid-loop checkpoint rolls back transient tool state before retry", async () => {
  const kweb = new MockKweb([node(1, [2]), node(2)]);
  let generations = 0;
  let checkpoints = 0;
  const session = new ConversationSession({
    kweb,
    intelligence: { generate: async () => {
      generations += 1;
      if (generations === 1) return {
        status: "complete", response_id: "tool-response",
        message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}}]}' }, usage: { input_tokens: 10, output_tokens: 2 },
      };
      return { status: "complete", response_id: "retry-response", message: { role: "assistant", content: "Recovered cleanly." }, usage: null };
    } },
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1), provider: "p", model: "m",
    persist: async () => { checkpoints += 1; if (checkpoints === 2) throw new Error("checkpoint interrupted"); }, onUpdate: () => {},
  });
  await session.initialize();
  await assert.rejects(() => session.send("Load more context"), /checkpoint interrupted/);
  assert.deepEqual(session.context.loadedNodeIds, [id(1)]);
  assert.deepEqual(session.executor.toolLog, []);
  assert.equal(session.usage.snapshot().requests, 0);
  assert.equal(session.chatend.messages.some(message => message.content?.includes?.("LoadNode")), false);
  assert.equal(session.continuation.previousResponseId, null);

  await session.resumePendingTurn();
  assert.equal(generations, 2);
  assert.equal(session.pendingTurn, false);
  assert.equal(session.transcript.at(-1).content, "Recovered cleanly.");
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
  const context = new KwebContext(new MockKweb([node(1, [2], [], [[2, "high"]]), node(2)]), id(1));
  await context.initialize();
  const formatted = formatKmapContext(context.snapshot());
  assert.match(formatted, /Current Kmap context/);
  assert.match(formatted, /Node 1: Node 1/);
  assert.match(formatted, /Last modified by: legacy-unknown/);
  assert.match(formatted, /Task connections:\n  - high: 2: Node 2/);
  assert.match(formatted, /Active connections:\n  - 2: Node 2/);
  assert.equal(formatted.includes('{'), false);
});

test("system prompt composition uses readable sections rather than markup wrappers", () => {
  const prompt = composePrompt({ shared: "Shared paragraph.", conversation: "Conversation paragraph.", ingress: "Ingress paragraph." }, "conversation", { model: "gpt-5.6-sol", reasoningEffort: "xhigh" });
  assert.equal(prompt, "Kennedy's identity\n\nShared paragraph.\n\nConversation session instructions\n\nConversation paragraph.\n\nCurrent session\n\nThis is a conversation session in Kennedy's browser UI.\n\nCurrent runtime\n\nYou are currently running on gpt-5.6-sol with xhigh thinking mode.");
  assert.match(composePrompt({ shared: "Shared.", conversation: "Conversation." }, "conversation", { sessionType: "telegram" }), /This is a telegram session/);
  assert.match(composePrompt({ shared: "Shared.", ingress: "Ingress." }, "ingress", { sourceSessionType: "telegram" }), /ingressing an archived telegram session/);
  assert.equal(formatModelAttribution("gpt-5.6-sol", "xhigh"), "gpt-5.6-sol-xhigh");
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

test("system prompt loader requests the identity and mode manuals", async () => {
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
    "/base/system-prompts/ConversationManual.txt",
    "/base/system-prompts/HistoryIngress.txt",
    "/base/system-prompts/KennedyIdentity.txt",
  ]);
});

test("session manuals enforce exclusive tool-request responses", async () => {
  for (const file of ["ConversationManual.txt", "HistoryIngress.txt"]) {
    const manual = await readFile(new URL(`../SystemPrompts/${file}`, import.meta.url), "utf8");
    assert.match(manual, /closing brace must be the final non-whitespace character/);
    assert.match(manual, /Do not use a Markdown fence/);
    assert.match(manual, /include any other text before or after the object/);
  }
});

test("mode manuals expose technical contracts without embedding Kmap strategy", async () => {
  const identity = await readFile(new URL("../SystemPrompts/KennedyIdentity.txt", import.meta.url), "utf8");
  assert.match(identity, /use the kmap itself to understand the best way to use the kmap/i);
  const conversation = await readFile(new URL("../SystemPrompts/ConversationManual.txt", import.meta.url), "utf8");
  assert.doesNotMatch(conversation, /ConnectNodes\n  Call:/);
  assert.doesNotMatch(conversation, /ConsolidateFanout\n  Call:/);
  assert.doesNotMatch(conversation, /AssignTask\n  Call:/);
  assert.match(conversation, /Kmap is read-only in this mode/);
  assert.match(conversation, /entire archived Chatend is passed to a separate read-write history-ingress mode/);
  assert.match(conversation, /At most ten nodes may be directly loaded at once, including both roots/);
  const ingress = await readFile(new URL("../SystemPrompts/HistoryIngress.txt", import.meta.url), "utf8");
  assert.match(ingress, /ConsolidateFanout\n  Call:/);
  assert.match(ingress, /AssignTask\n  Call:/);
  assert.match(ingress, /Use the string "blank" as childIdentifier/);
  for (const manual of [conversation, ingress]) {
    assert.doesNotMatch(manual, /knowledge-hungry|use a promising|navigate enough|prefer to|consider assigning/i);
  }
});

test("transcript rerenders follow only readers already at the bottom", async () => {
  const render = await readFile(new URL("../public/js/render.js", import.meta.url), "utf8");
  assert.match(render, /wasAtBottom = container\.scrollHeight - container\.clientHeight - container\.scrollTop <= 1/);
  assert.match(render, /container\.scrollTop = wasAtBottom \? container\.scrollHeight : previousTop/);
});

test("history ingress is an inline continuation with no independent scroller", async () => {
  const render = await readFile(new URL("../public/js/render.js", import.meta.url), "utf8");
  const styles = await readFile(new URL("../public/css/styles.css", import.meta.url), "utf8");
  const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
  assert.match(render, /renderTranscript\(container, transcript, ingressActivity = null\)/);
  assert.match(render, /renderIngressActivity\(container, ingressActivity\.diagnostic, ingressActivity\.active\)[\s\S]*?container\.scrollTop = wasAtBottom \? container\.scrollHeight : previousTop/);
  assert.match(styles, /\.ingress-continuation\s*\{/);
  assert.equal(styles.includes(".ingress-panel"), false);
  assert.equal(styles.includes(".ingress-log"), false);
  assert.equal(html.includes('id="ingress-panel"'), false);
  assert.equal(/\.ingress-usage\s*\{[^}]*position:\s*sticky/s.test(styles), false);
});

test("chatend inspector exposes text tool requests and readable results", () => {
  const chatend = [
    { role: "system", content: "Readable instructions." },
    { role: "user", content: "Hello." },
    { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}}]}' },
    { role: "user", display_role: "Memory tool result", content: "Memory load completed.\n\nNode 2: Project" },
    { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"WebSearch","arguments":{"question":"current evidence"}}]}' },
    { role: "user", display_role: "Web tool result", content: "Kennedy tool result\nTool: WebSearch\n\nWeb research completed." },
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

  const tools = inspectorText({ chatend }, "tools");
  assert.match(tools, /KENNEDY_TOOL_CALLS/);
  assert.match(tools, /"LoadNode"/);
  assert.match(tools, /Memory tool result\n\nMemory load completed/);
  assert.match(tools, /"WebSearch"/);
  assert.match(tools, /Web tool result\n\nKennedy tool result/);
  assert.ok(tools.indexOf('"LoadNode"') < tools.indexOf("Memory tool result"));
  assert.ok(tools.indexOf("Memory tool result") < tools.indexOf('"WebSearch"'));
  assert.ok(tools.indexOf('"WebSearch"') < tools.indexOf("Web tool result"));
  assert.equal(tools.includes("Readable instructions"), false);
  assert.equal(tools.includes("Hello."), false);
  assert.equal(tools.includes("Here is what I found."), false);
  assert.equal(inspectorText({ chatend: [{ role: "user", content: "No tools yet." }] }, "tools"), "No tool calls are currently in the Chatend.");
});

test("production frontend never uses HTML string insertion", async () => {
  const files = ["render.js", "memory_explorer.js", "app.js"];
  for (const file of files) {
    const source = await readFile(new URL(`../public/js/${file}`, import.meta.url), "utf8");
    assert.equal(source.includes("innerHTML"), false, `${file} uses innerHTML`);
  }
});

test("frontend exposes full, system, tools, memory inspector, and both Kmap root controls", async () => {
  const [html, app] = await Promise.all([
    readFile(new URL("../public/index.html", import.meta.url), "utf8"),
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
  ]);
  for (const id of ["usage-metrics", "inspector-full", "inspector-system", "inspector-tools", "inspector-memory", "memory-home", "memory-kennedy-home", "new-conversation", "conversation-history", "tg-tab", "voice-button"]) assert.match(html, new RegExp(`id="${id}"`));
  assert.match(app, /new MemoryExplorer\(\{ api: kweb, rootNodeIds,/);
  assert.match(app, /memory_kennedy_home\.addEventListener\("click", \(\) => explorer\?\.kennedyHome\(\)\)/);
  assert.ok(app.indexOf("await conversationHistory.discardUnstarted()") < app.indexOf("historyRecords = (await conversationHistory.list())"));
  assert.match(html, /\/js\/app\.js\?v=\d{8}\.\d+/);
  assert.match(html, /\/css\/styles\.css\?v=\d{8}\.\d+/);
});

test("telegram voice sessions archive media, correlate delivery, and emit context notices outside the Chatend", async () => {
  const checkpoints = [];
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]),
    intelligence: { generate: async () => ({
      status: "complete", response_id: "telegram-response",
      message: { role: "assistant", content: "I heard you." },
      usage: { input_tokens: 100001, output_tokens: 20, cached_tokens: 90000, cache_write_tokens: 0, reasoning_tokens: 0 },
    }) },
    manuals: { shared: "Shared", conversation: "Conversation" }, rootNodeId: id(1),
    provider: "p", model: "m", contextWindowTokens: 1050000, sessionType: "telegram",
    channel: { telegramUserId: 42, username: "taek42" },
    persist: async state => checkpoints.push(structuredClone(state)), onUpdate: () => {},
  });
  await session.initialize();
  await session.send("There is music behind me.", {
    externalEventId: "tg-event", inputKind: "voice", transcriptionModel: "gpt-4o-transcribe",
    media: { id: "voice-1", mimeType: "audio/ogg", dataUrl: "data:audio/ogg;base64,AAAA" },
  });
  const answer = session.answerForExternalEvent("tg-event");
  assert.equal(answer.content, "I heard you.");
  assert.match(answer.contextWarning, /100,021 out of 1,050,000 context tokens used/);
  assert.equal(session.chatend.messages.some(message => message.content === answer.contextWarning), false);
  assert.match(session.chatend.messages.find(message => message.role === "user" && message.content.includes("paid transcription")).content, /There is music behind me/);
  assert.equal(session.archive().sessionType, "telegram");
  assert.equal(session.archive().media[0].dataUrl, "data:audio/ogg;base64,AAAA");
  assert.equal(checkpoints.at(-1).pendingExternalEventId, null);
});

test("audio and Telegram API clients use multipart and durable relay endpoints", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, options = {}) => {
    requests.push({ url, options });
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ text: "Transcript", events: [] }),
    };
  };
  try {
    await IntelligenceAPI("http://intelligence").transcribe({ provider: "p", model: "m", file: new Blob(["audio"], { type: "audio/ogg" }), fileName: "note.ogg" });
    await TelegramRelayAPI("http://telegram").bind("event", "019f5ca7-020f-7b63-be2f-82785fb68c03");
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(requests[0].url, "http://intelligence/api/v1/audio/transcriptions");
  assert.equal(requests[0].options.body instanceof FormData, true);
  assert.equal(requests[0].options.headers, undefined);
  assert.equal(requests[1].url, "http://telegram/api/v1/events/event/bind");
  assert.match(requests[1].options.body, /conversationId/);
});
