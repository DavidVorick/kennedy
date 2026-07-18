import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { KwebContext } from "../public/js/kweb_context.js";
import { Chatend } from "../public/js/chatend.js";
import { MAX_RESET_SELF_MESSAGE_CHARACTERS, MAX_SELF_TIME_HANDOFF_MESSAGE_CHARACTERS, RUST_LIB_TOOL_NAMES, ToolExecutor, parseToolCalls, truncateToolResponse } from "../public/js/tools.js";
import { ConversationSession } from "../public/js/conversation.js";
import { runHistoryIngress } from "../public/js/history_ingress.js";
import { audioRecordingTitle, conversationControlState, conversationIngressActivity, conversationTitle, ingressEntryPresentation, ingressMutationSummary, inspectorText, mainViewEntries, reconcileConversationHistory, sortConversationHistory } from "../public/js/render.js";
import { AGENT_LOOP_SESSION_ENDED, ContinuationState, UsageTracker, runAgentLoop } from "../public/js/intelligence.js";
import { composePrompt, formatModelAttribution, loadPromptManuals, promptsReady, requiredPromptKeys } from "../public/js/prompt_composer.js";
import { formatContextNode, formatKmapContext, formatToolResult } from "../public/js/human_format.js";
import { MemoryExplorer } from "../public/js/memory_explorer.js";
import { AudioIngressAPI, ConversationHistoryAPI, IntelligenceAPI, KwebAPI, RustLibsAPI, TelegramRelayAPI, newIdempotencyId } from "../public/js/api.js";
import { formatDuration } from "../public/js/timing.js";
import { formatChatend, formatContextWindowProgress } from "../public/js/chatend_format.js";
import { selectNextMemoryIngress } from "../public/js/memory_ingress_coordinator.js";
import { FREE_TIME_CONTINUATION_MINIMUM_MS, FREE_TIME_HARD_STOP_GRACE_MS, FREE_TIME_WARNING_MS, MAX_SELF_TIME_PROMPT_CHARACTERS, freeTimeCanStartNewSession, freeTimeNoAnswerContinuationMessage, freeTimeOpeningMessage, freeTimeRequestTimeoutSeconds, freeTimeTiming, freeTimeTurnContinuationMessage, nextFreeTimeSlice, parseFreeTimeMinutes, parseSelfTimePrompt } from "../public/js/self_time.js";
import { TELEGRAM_RESPONSE_TIMEOUT_MS, telegramEventDeadlineMs, telegramEventTimeoutMs } from "../public/js/telegram_timing.js";

const id = n => n.toString(16).padStart(40, "0");
const summary = n => ({ id: id(n), short_name: `Node ${n}`, short_description: `Summary ${n}` });
const node = (n, active = [], fanout = [], fixed = [], lastModifiedBy = "legacy-unknown") => ({ id: id(n), short_name: `Node ${n}`, short_description: `Summary ${n}`, long_description: `Details ${n}`, last_modified_by: lastModifiedBy, owner_root_node_id: id(1), fixed_connections: fixed.map(([target, slot]) => ({ ...summary(target), slot })), active_connections: active.map(summary), fanout_connections: fanout.map(summary), history_head_id: id(100 + n) });
const promptManuals = (label = "Shared") => ({
  identity: `${label} identity`,
  conversationSession: `${label} conversation session`,
  freeTimeSession: `${label} free-time session`,
  historyIngressSession: `${label} history-ingress session`,
  audioIngressSession: `${label} audio-ingress session`,
  codexHarness: `${label} Codex outer-harness note`,
  kmapBasics: `${label} Kmap basics`,
  readTools: `${label} read and web tools`,
  writeTools: `${label} write tools`,
});

class MockKweb {
  constructor(nodes) { this.nodes = new Map(nodes.map(n => [n.id, n])); this.updatedCalls = []; }
  async context(nodeId) { const requested = this.nodes.get(nodeId); return { requested_node: requested, active_connection_nodes: requested.active_connections.map(item => this.nodes.get(item.id)) }; }
  async updateNode(nodeId, body) {
    this.updatedCalls.push([nodeId, body]);
    const current = this.nodes.get(nodeId);
    const recent = (body.recent_connections || []).map(value => ({ id: value, short_name: this.nodes.get(value)?.short_name, short_description: this.nodes.get(value)?.short_description }));
    const updated = {
      ...current,
      short_name: body.short_name,
      short_description: body.short_description,
      long_description: body.long_description,
      owner_node_id: body.owner_node_id,
      owner_root_node_id: body.owner_node_id,
      last_modified_by: body.model_attribution,
      fixed_connections: (body.fixed_connections || []).map((value, index) => ({ id: value, slot: index + 1 })),
      active_connections: recent.slice(0, 8),
      fanout_connections: recent.slice(8),
      recent_connections: body.recent_connections || [],
    };
    this.nodes.set(nodeId, updated);
    return { node: updated };
  }
}

test("Kmap client uses namespaced storage routes and derives active context", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const stored = new Map([
    [id(1), { id: id(1), short_name: "Root Node", short_description: "", long_description: "", last_modified_by: "test", last_modified_at: "2026-07-18T00:00:00Z", owner_node_id: id(1), fixed_connections: [id(3)], recent_connections: [id(2)] }],
    [id(2), { id: id(2), short_name: "Active Node", short_description: "", long_description: "", last_modified_by: "test", last_modified_at: "2026-07-18T00:00:00Z", owner_node_id: id(1), fixed_connections: [], recent_connections: [] }],
  ]);
  globalThis.fetch = async url => {
    calls.push(String(url));
    const nodeId = String(url).split("/").at(-1);
    return new Response(JSON.stringify(stored.get(nodeId)), { status: 200, headers: { "content-type": "application/json" } });
  };
  try {
    const payload = await KwebAPI("http://local").context(id(1));
    assert.deepEqual(calls, [
      `http://local/api/v1/kmap/nodes/${id(1)}`,
      `http://local/api/v1/kmap/nodes/${id(2)}`,
    ]);
    assert.deepEqual(payload.requested_node.fixed_connections, [{ id: id(3), slot: 1 }]);
    assert.deepEqual(payload.requested_node.active_connections, [{ id: id(2) }]);
    assert.equal(Object.hasOwn(payload.requested_node, "recent_connections"), false);
    assert.equal(payload.active_connection_nodes[0].short_name, "Active Node");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Kmap mutations retain their idempotency ID across an ambiguous network retry", async () => {
  const generated = newIdempotencyId();
  assert.match(generated, /^[0-9a-f]{32}$/);

  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, options) => {
    requests.push({ url: String(url), options });
    if (requests.length === 1) throw new TypeError("connection lost after send");
    return new Response(JSON.stringify({ id: id(9) }), { status: 201, headers: { "content-type": "application/json" } });
  };
  try {
    const idempotencyId = "12".repeat(16);
    const result = await KwebAPI("http://local").createProvenance({
      idempotency_id: idempotencyId,
      data: "source text",
      source: "test",
      source_created_at: "2026-07-18T00:00:00Z",
    });
    assert.equal(result.id, id(9));
    assert.equal(requests.length, 2);
    assert.equal(requests[0].url, "http://local/api/v1/kmap/provenance");
    assert.equal(requests[0].options.body, requests[1].options.body);
    assert.equal(JSON.parse(requests[0].options.body).idempotency_id, idempotencyId);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Rust library client carries hidden session identity through the internal server bridge", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, options = {}) => {
    requests.push({ url: String(url), options });
    const result = String(url).endsWith("/execute")
      ? { result: { name: "example", version: "0.1.0", files: [] } }
      : { released: 1 };
    return new Response(JSON.stringify(result), { status: 200, headers: { "content-type": "application/json" } });
  };
  try {
    const api = RustLibsAPI("http://local");
    assert.equal((await api.execute("kennedy:session", "OpenRustLib", { name: "example" })).name, "example");
    assert.deepEqual(await api.release("kennedy:session"), { released: 1 });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(requests[0].url, "http://local/api/v1/rust-libs/execute");
  assert.deepEqual(JSON.parse(requests[0].options.body), {
    session_id: "kennedy:session",
    name: "OpenRustLib",
    arguments: { name: "example" },
  });
  assert.equal(requests[1].url, "http://local/api/v1/rust-libs/release");
  assert.deepEqual(JSON.parse(requests[1].options.body), { session_id: "kennedy:session" });
});

test("Kweb archive provenance moves media into replay-stable multipart artifacts", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, options) => {
    if (String(url).startsWith("data:")) {
      return new Response(new Blob(["voice-note"], { type: "audio/wav" }), {
        status: 200,
        headers: { "content-type": "audio/wav" },
      });
    }
    requests.push({ url: String(url), options });
    if (requests.length === 1) throw new TypeError("connection lost after send");
    return new Response(JSON.stringify({ id: id(10) }), { status: 201, headers: { "content-type": "application/json" } });
  };
  try {
    const archive = {
      messages: [],
      media: [{
        fileName: "telegram-vnote.wav",
        mimeType: "audio/wav",
        dataUrl: "data:audio/wav;base64,dm9pY2Utbm90ZQ==",
      }],
    };
    const result = await KwebAPI("http://local").createProvenanceArchive({
      idempotency_id: "34".repeat(16),
      archive,
      source: "conversation-history",
      source_created_at: "2026-07-18T00:00:00Z",
    });
    assert.equal(result.id, id(10));
    assert.equal(requests.length, 2);
    assert.equal(requests[0].url, "http://local/api/v1/kmap/provenance-with-artifacts");
    assert.equal(requests[0].options.body, requests[1].options.body);
    const form = requests[0].options.body;
    assert.equal(form.get("idempotency_id"), "34".repeat(16));
    assert.equal(form.get("data_filename"), "conversation-history-archive.json");
    const stored = JSON.parse(form.get("data"));
    assert.equal(stored.media[0].dataUrl, undefined);
    assert.equal(stored.media[0].provenanceArtifactIndex, 0);
    const artifact = form.get("artifact");
    assert.equal(artifact.name, "telegram-vnote.wav");
    assert.equal(artifact.type, "audio/wav");
    assert.equal(await artifact.text(), "voice-note");
    assert.match(archive.media[0].dataUrl, /^data:/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Kweb provenance artifacts are fetched from their encoded namespaced path", async () => {
  const originalFetch = globalThis.fetch;
  let requested;
  globalThis.fetch = async url => {
    requested = String(url);
    return new Response(new Blob(["artifact"]), { status: 200 });
  };
  try {
    const blob = await KwebAPI("http://local").provenanceArtifact("a_/voice note.123.wav");
    assert.equal(requested, "http://local/api/v1/kmap/provenance-artifacts/a_/voice%20note.123.wav");
    assert.equal(await blob.text(), "artifact");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

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

test("all declared roots load automatically and survive every reset", async () => {
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

test("a three-root group context leaves seven direct-load slots", async () => {
  const api = new MockKweb(Array.from({ length: 11 }, (_, index) => node(index + 1)));
  const context = new KwebContext(api, [id(1), id(2), id(3)]);
  await context.initialize();
  await context.reset([id(4), id(5), id(6), id(7), id(8), id(9), id(10)]);
  assert.deepEqual(context.loadedNodeIds, Array.from({ length: 10 }, (_, index) => id(index + 1)));
  await assert.rejects(
    () => context.reset([id(4), id(5), id(6), id(7), id(8), id(9), id(10), id(11)]),
    error => error.code === "loaded_node_limit",
  );
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

test("conversation history client lists compact summaries instead of complete archives", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, options = {}) => {
    requests.push({ url, options });
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ conversations: [] }),
    };
  };
  try {
    assert.deepEqual(await ConversationHistoryAPI("http://history").list(), { conversations: [] });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(requests[0].url, "http://history/api/v1/conversations/summaries");
});

test("conversation history client permanently purges one versioned record", async () => {
  const originalFetch = globalThis.fetch;
  let request = null;
  globalThis.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ purged: true, conversation_id: "stuck" }),
    };
  };
  try {
    const result = await ConversationHistoryAPI("http://history").purge("stuck", { expected_version: 7 });
    assert.deepEqual(result, { purged: true, conversation_id: "stuck" });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(request.url, "http://history/api/v1/conversations/stuck");
  assert.equal(request.options.method, "DELETE");
  assert.equal(request.options.body, '{"expected_version":7}');
});

test("conversation history client records ingress failures durably", async () => {
  const originalFetch = globalThis.fetch;
  let request = null;
  globalThis.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ phase: "ingress_failed", ingress_failure_count: 5 }),
    };
  };
  try {
    await ConversationHistoryAPI("http://history").ingressFailure("conversation", {
      expected_version: 9, stage: "model_loop", code: "provider_error", message: "Context exhausted.",
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(request.url, "http://history/api/v1/conversations/conversation/ingress-failure");
  assert.equal(request.options.method, "POST");
  assert.match(request.options.body, /"stage":"model_loop"/);
});

test("conversation history client can complete self time without ingress", async () => {
  const originalFetch = globalThis.fetch;
  let request = null;
  globalThis.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ phase: "complete", state: { sessionType: "free-time" } }),
    };
  };
  try {
    await ConversationHistoryAPI("http://history").completeWithoutIngress("self-time", {
      expected_version: 4,
      state: { sessionType: "free-time" },
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(request.url, "http://history/api/v1/conversations/self-time/complete");
  assert.equal(request.options.method, "POST");
  assert.equal(request.options.body, '{"expected_version":4,"state":{"sessionType":"free-time"}}');
});

test("conversation history client can explicitly restart terminal ingress", async () => {
  const originalFetch = globalThis.fetch;
  let request = null;
  globalThis.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ phase: "ingress_pending", ingress_failure_count: 0 }),
    };
  };
  try {
    await ConversationHistoryAPI("http://history").retryIngress("conversation", {
      expected_version: 10,
      state: { archive: { format: "kennedy-chatend" } },
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(request.url, "http://history/api/v1/conversations/conversation/retry-ingress");
  assert.equal(request.options.method, "POST");
  assert.match(request.options.body, /"expected_version":10/);
  assert.doesNotMatch(request.options.body, /historyIngress/);
});

test("intelligence client tags cancellable work and exposes an explicit cancel request", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, options) => {
    requests.push({ url, options });
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => url.endsWith("/cancel")
        ? { cancelled: true }
        : { status: "complete", response_id: "response", message: { role: "assistant", content: "Done." } },
    };
  };
  const controller = new AbortController();
  try {
    const api = IntelligenceAPI("http://intelligence");
    await api.generate({ provider: "p", model: "m", chatend: "David\n\nHi" }, {
      signal: controller.signal,
      operationId: "11111111-1111-4111-8111-111111111111",
    });
    await api.cancelOperation("11111111-1111-4111-8111-111111111111");
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(requests[0].options.signal, controller.signal);
  assert.match(requests[0].options.body, /"operation_id":"11111111-1111-4111-8111-111111111111"/);
  assert.equal(requests[1].url, "http://intelligence/api/v1/operations/11111111-1111-4111-8111-111111111111/cancel");
  assert.equal(requests[1].options.method, "POST");
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

test("LoadNode results omit nodes whose full bodies are already in context", async () => {
  const context = new KwebContext(new MockKweb([
    node(1, [3]),
    node(2, [3]),
    node(3, [4]),
    node(4),
  ]), id(1));
  await context.initialize();

  const overlappingLoad = await context.loadDurable(id(2));
  assert.equal(overlappingLoad.requestedNode.shortName, "Node 2");
  assert.equal(overlappingLoad.requestedNodeAlreadyLoaded, false);
  assert.deepEqual(overlappingLoad.activeConnectionNodes, []);

  const previouslyExpandedLoad = await context.loadDurable(id(3));
  assert.equal(previouslyExpandedLoad.requestedNode, null);
  assert.equal(previouslyExpandedLoad.requestedNodeAlreadyLoaded, true);
  assert.equal(previouslyExpandedLoad.requestedNodeIdentifier, context.shortId(id(3)));
  assert.deepEqual(previouslyExpandedLoad.activeConnectionNodes.map(item => item.shortName), ["Node 4"]);

  const formatted = formatToolResult("LoadNode", { ok: true, result: previouslyExpandedLoad });
  assert.match(formatted, /Node 2 was already available in full context and is now directly loaded/);
  assert.doesNotMatch(formatted, /Details 3/);
  assert.match(formatted, /Details 4/);
  assert.equal(context.snapshot().nodes.filter(item => item.identifier === context.shortId(id(3))).length, 1);
});

test("LoadNode classifies and deduplicates direct and indirect fanout references", async () => {
  const context = new KwebContext(new MockKweb([
    node(1, [2], [5]),
    node(2, [], [5, 6]),
    node(3, [4], [6, 7]),
    node(4, [], [7, 8]),
  ]), id(1));
  await context.initialize();

  const loaded = await context.loadDurable(id(3));
  assert.deepEqual(loaded.directFanoutNodes.map(item => item.shortName), ["Node 6", "Node 7"]);
  assert.deepEqual(loaded.indirectFanoutNodes.map(item => item.shortName), ["Node 8"]);

  const formatted = formatToolResult("LoadNode", { ok: true, result: loaded });
  assert.match(formatted, /Directly loaded nodes[\s\S]*Node 3/);
  assert.match(formatted, /Full active-connection nodes[\s\S]*Node 4/);
  assert.doesNotMatch(formatted, /Summary: Summary 4/);
  assert.match(formatted, /Fanout nodes of directly loaded nodes[\s\S]*Summary: Summary 6[\s\S]*Summary: Summary 7/);
  assert.match(formatted, /Fanout nodes only of active-connection nodes[\s\S]*Node 8/);
  assert.doesNotMatch(formatted, /Summary: Summary 8/);
});

test("legacy nodes without explicit fixed connections behave as though they have none", async () => {
  const legacy = node(1);
  delete legacy.fixed_connections;
  const context = new KwebContext(new MockKweb([legacy]), id(1));
  await context.initialize();
  assert.deepEqual(context.snapshot().nodes[0].fixedConnections, []);
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
  assert.match(second.message.content, /Context-loading budget of 1 is exhausted/);
  assert.equal(executor.loadCalls, 2);
});

test("ResetContext and LoadNode share one context-loading budget", async () => {
  const api = new MockKweb([node(1)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  const executor = new ToolExecutor({ mode: "conversation", context, api, loadLimit: 2 });
  const reset = await executor.execute({ id: "reset", name: "ResetContext", arguments: { identifiers: [] } });
  assert.equal(reset.reset, true);
  assert.deepEqual(reset.resetHistoryEntry, { retainedNodeNames: [], budgetUsed: 1, budgetLimit: 2 });
  assert.match(reset.message.content, /rebuilt Kmap context above contains the newly loaded nodes/);
  assert.doesNotMatch(reset.message.content, /Details 1/);
  const failedLoad = await executor.execute({ id: "load", name: "LoadNode", arguments: { identifier: 999 } });
  assert.match(failedLoad.message.content, /Unknown memory identifier 999/);
  const exhausted = await executor.execute({ id: "reset", name: "ResetContext", arguments: { identifiers: [] } });
  assert.equal(exhausted.reset, false);
  assert.match(exhausted.message.content, /Context-loading budget of 2 is exhausted/);
  assert.equal(executor.loadCalls, 3);
});

test("ResetContext accepts an optional bounded self-message", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const executor = new ToolExecutor({ mode: "conversation", context, api: context.api, loadLimit: 20 });
  const selfMessage = ` ${"x".repeat(MAX_RESET_SELF_MESSAGE_CHARACTERS - 2)} `;
  const accepted = await executor.execute({ id: "reset", name: "ResetContext", arguments: { identifiers: [], selfMessage } });
  assert.equal(accepted.reset, true);
  assert.equal(accepted.selfMessage, selfMessage);
  assert.deepEqual(accepted.resetHistoryEntry, { retainedNodeNames: [], budgetUsed: 1, budgetLimit: 20 });

  const omitted = await executor.execute({ id: "reset", name: "ResetContext", arguments: { identifiers: [] } });
  assert.equal(omitted.reset, true);
  assert.equal(omitted.selfMessage, null);

  const tooLong = await executor.execute({ id: "reset", name: "ResetContext", arguments: { identifiers: [], selfMessage: `${selfMessage}x` } });
  assert.equal(tooLong.reset, false);
  assert.match(tooLong.message.content, /selfMessage must contain between 1 and 400000 characters/);
  const tooLongAfterTrimming = await executor.execute({ id: "reset", name: "ResetContext", arguments: { identifiers: [], selfMessage: `${selfMessage} ` } });
  assert.equal(tooLongAfterTrimming.reset, false);
  const empty = await executor.execute({ id: "reset", name: "ResetContext", arguments: { identifiers: [], selfMessage: "   " } });
  assert.equal(empty.reset, false);
});

test("ConnectNodes translates short IDs to durable IDs", async () => {
  const api = new MockKweb([node(1, [2]), node(2)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  const executor = new ToolExecutor({ mode: "ingress", context, api, provenanceId: "prov", modelAttribution: "gpt-test-xhigh", loadLimit: 20 });
  const result = await executor.execute({ id: "a", name: "ConnectNodes", arguments: { identifiers: [1, 2] } });
  assert.match(result.message.content, /Memory connections updated/);
  assert.deepEqual(api.updatedCalls.map(call => call[0]), [id(1), id(2)]);
  assert.deepEqual(api.updatedCalls.map(call => call[1].recent_connections), [[id(2)], [id(1)]]);
  assert.ok(api.updatedCalls.every(call => call[1].model_attribution === "gpt-test-xhigh"));
  assert.match(result.message.content, /Last modified by: gpt-test-xhigh/);
});

test("history ingress rechecks authorization before a Kmap mutation", async () => {
  const api = new MockKweb([node(1), node(2)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  await context.loadDurable(id(2));
  let checks = 0;
  const executor = new ToolExecutor({
    mode: "ingress", context, api, provenanceId: id(9), loadLimit: 50,
    beforeMutation: async () => {
      checks += 1;
      throw Object.assign(new Error("Conversation was purged."), { code: "ingress_cancelled" });
    },
  });
  await assert.rejects(
    () => executor.execute({ name: "ConnectNodes", arguments: { identifiers: [1, 2] } }),
    error => error.code === "ingress_cancelled",
  );
  assert.equal(checks, 1);
  assert.equal(api.updatedCalls.length, 0);
});

test("ConsolidateFanout and SetFixedConnection translate short IDs and refresh fixed connections", async () => {
  const api = new MockKweb([node(1, [], [2, 3, 4]), node(2), node(3), node(4)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  await context.loadDurable(id(2));
  const executor = new ToolExecutor({ mode: "ingress", context, api, provenanceId: "prov", modelAttribution: "gpt-test-xhigh", loadLimit: 20 });
  const consolidated = await executor.execute({ id: "a", name: "ConsolidateFanout", arguments: { parentIdentifier: 1, aggregatorIdentifier: 2, fanoutIdentifiers: [3, 4] } });
  assert.match(consolidated.message.content, /Fanout connections consolidated/);
  assert.deepEqual(api.updatedCalls.slice(0, 2).map(call => call[0]), [id(1), id(2)]);
  assert.deepEqual(context.diagnostics().fullNodeIds.sort(), [id(1), id(2)].sort());

  const assigned = await executor.execute({ id: "b", name: "SetFixedConnection", arguments: { parentIdentifier: 1, childIdentifier: 2, slot: 1 } });
  assert.match(assigned.message.content, /Fixed connection assigned/);
  assert.deepEqual(api.updatedCalls.at(-1)[1].fixed_connections, [id(2)]);
  assert.equal(context.snapshot().nodes.find(item => item.identifier === 1).fixedConnections[0].slot, 1);

  const cleared = await executor.execute({ id: "c", name: "SetFixedConnection", arguments: { parentIdentifier: 1, childIdentifier: "blank", slot: 1 } });
  assert.match(cleared.message.content, /Fixed connection slot cleared/);
  assert.deepEqual(api.updatedCalls.at(-1)[1].fixed_connections, []);
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
  const createArguments = { parentIdentifiers: [1], ownerIdentifier: 1, shortName: "New Memory", shortDescription: "Summary.", longDescription: "Details." };
  const created = await executor.execute({ id: "create", name: "CreateNode", arguments: createArguments });
  assert.equal(api.created.model_attribution, "gpt-5.6-sol-xhigh");
  assert.match(api.created.idempotency_id, /^[0-9a-f]{32}$/);
  assert.equal("model_attribution" in createArguments, false);
  assert.equal("idempotency_id" in createArguments, false);
  assert.match(created.message.content, /Last modified by: gpt-5.6-sol-xhigh/);

  const updateArguments = { identifier: 2, ownerIdentifier: 1, newShortName: "Updated Memory", newShortDescription: "Updated.", newLongDescription: "Updated details." };
  await executor.execute({ id: "update", name: "UpdateNode", arguments: updateArguments });
  assert.equal(api.updated[0], id(2));
  assert.equal(api.updated[1].model_attribution, "gpt-5.6-sol-xhigh");
  assert.match(api.updated[1].idempotency_id, /^[0-9a-f]{32}$/);
  assert.notEqual(api.updated[1].idempotency_id, api.created.idempotency_id);
  assert.equal("model_attribution" in updateArguments, false);
  assert.equal("idempotency_id" in updateArguments, false);
});

test("WebSearch and WebFetch expose only minimal model-facing arguments", async () => {
  const calls = [];
  const timings = [];
  const intelligence = {
    webSearch: async body => { calls.push(["search", body]); return { answer: "Two candidates.", sources: [{ title: "Guide", url: "https://example.com/guide" }] }; },
    webFetch: async body => { calls.push(["fetch", body]); return { url: body.url, title: "Guide", retrieved_at: "2026-07-12T00:00:00Z", content_type: "text/html", content: "Page evidence.", truncated: false }; },
    recordTiming: timing => timings.push(timing),
  };
  const executor = new ToolExecutor({ mode: "conversation", context: {}, api: {}, intelligence, provider: "primary", model: "model", loadLimit: 20 });
  const search = await executor.execute({ id: "search", name: "WebSearch", arguments: { question: "best brunch in El Salvador", mode: "balanced" } });
  const fetch = await executor.execute({ id: "fetch", name: "WebFetch", arguments: { url: "https://example.com/guide" } });
  assert.deepEqual(calls, [
    ["search", { provider: "primary", model: "model", question: "best brunch in El Salvador", mode: "balanced" }],
    ["fetch", { url: "https://example.com/guide" }],
  ]);
  assert.equal(search.message.display_role, "Web tool result");
  assert.equal(search.message.tool_name, "WebSearch");
  assert.equal(search.message.tool_result.ok, true);
  assert.match(search.message.content, /^Kennedy tool result · WebSearch · \d+ ms/);
  assert.match(search.message.content, /Web research completed/);
  assert.match(search.message.content, /https:\/\/example.com\/guide/);
  assert.match(fetch.message.content, /^Kennedy tool result · WebFetch · \d+ ms/);
  assert.match(fetch.message.content, /Readable page content:\n  Page evidence/);
  assert.deepEqual(timings.map(timing => [timing.action, timing.name, timing.status]), [
    ["tool", "WebSearch", "ok"],
    ["tool", "WebFetch", "ok"],
  ]);
});

test("Rust library tools validate complete writes and return readable coding results", async () => {
  const calls = [];
  const rustLibs = {
    execute: async (sessionId, name, args) => {
      calls.push({ sessionId, name, args });
      if (name === "OpenRustLib") return {
        name: args.name,
        version: "0.1.0",
        documentation: "Reference",
        files: [{ path: "src/lib.rs", contents: "pub fn answer() -> u8 { 42 }\n" }],
      };
      if (name === "WriteRustLib") return { name: args.name, version: "0.2.0", written_paths: args.files.map(file => file.path) };
      if (name === "CheckRustLib") return { name: args.name, passed: false, stages: [{ stage: "clippy", success: false, exit_code: 1, stdout: "", stderr: "warning\n" }] };
      return { name: args.name, version: "0.2.0", published: true };
    },
  };
  const executor = new ToolExecutor({ mode: "conversation", context: {}, api: {}, rustLibs, toolSessionId: "kennedy:tool-session", loadLimit: 20 });
  const opened = await executor.execute({ name: "OpenRustLib", arguments: { name: "example-lib" } });
  assert.equal(opened.message.display_role, "Coding tool result");
  assert.match(opened.message.content, /Complete contents as a JSON string:\n"pub fn answer\(\) -> u8 \{ 42 \}\\n"/);

  const written = await executor.execute({ name: "WriteRustLib", arguments: { name: "example-lib", files: [{ path: "src/lib.rs", contents: "" }] } });
  assert.match(written.message.content, /Canonical version: 0.2.0/);
  const checked = await executor.execute({ name: "CheckRustLib", arguments: { name: "example-lib" } });
  assert.match(checked.message.content, /Rust library check did not pass/);
  assert.match(checked.message.content, /Failed: clippy \(exit 1\)/);
  const published = await executor.execute({ name: "PublishRustLib", arguments: { name: "example-lib" } });
  assert.match(published.message.content, /Published version: 0.2.0/);
  assert.deepEqual(calls.map(call => call.name), ["OpenRustLib", "WriteRustLib", "CheckRustLib", "PublishRustLib"]);
  assert.ok(calls.every(call => call.sessionId === "kennedy:tool-session"));
  assert.deepEqual(RUST_LIB_TOOL_NAMES, ["CreateRustLib", "OpenRustLib", "WriteRustLib", "CheckRustLib", "PublishRustLib"]);

  const duplicate = await executor.execute({ name: "WriteRustLib", arguments: { name: "example-lib", files: [{ path: "src/lib.rs", contents: "a" }, { path: "src/lib.rs", contents: "b" }] } });
  assert.match(duplicate.message.content, /duplicate path src\/lib.rs/);
  assert.equal(calls.length, 4);

  const ingress = new ToolExecutor({ mode: "ingress", context: {}, api: {}, rustLibs, toolSessionId: "kennedy:ingress", loadLimit: 50 });
  const ingressOpened = await ingress.execute({ name: "OpenRustLib", arguments: { name: "example-lib" } });
  assert.equal(ingressOpened.message.tool_result.ok, true);
  assert.equal(calls.at(-1).sessionId, "kennedy:ingress");
});

test("history and audio ingress can invoke Rust library tools", async () => {
  for (const sourceSessionType of ["conversation", "audio"]) {
    const kweb = new MockKweb([node(1)]);
    kweb.provenance = async () => ({
      source: sourceSessionType,
      source_created_at: "2026-07-18T00:00:00Z",
      data: JSON.stringify({ format: "kennedy-chatend", messages: [{ role: "user", content: "Maintain the crate." }] }),
    });
    let generation = 0;
    const intelligence = { generate: async () => {
      generation += 1;
      return generation === 1
        ? { status: "complete", response_id: "tool", message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"OpenRustLib","arguments":{"name":"example-lib"}}]}' }, usage: null }
        : { status: "complete", response_id: "done", message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"EndHistoryIngress","arguments":{}}]}' }, usage: null };
    } };
    const calls = [];
    const rustLibs = { execute: async (sessionId, name, args) => {
      calls.push({ sessionId, name, args });
      return { name: args.name, version: "0.1.0", documentation: "Reference", files: [] };
    } };
    const checkpoints = [];
    const toolSessionId = `kennedy:test-${sourceSessionType}`;
    await runHistoryIngress({
      kweb, intelligence, rustLibs, toolSessionId,
      manuals: promptManuals("Ingress"), rootNodeId: id(1), provenanceId: "provenance",
      provider: "p", model: "m", sourceSessionType,
      checkpoint: async archive => checkpoints.push(structuredClone(archive)), onUpdate: () => {},
    });
    assert.deepEqual(calls, [{ sessionId: toolSessionId, name: "OpenRustLib", args: { name: "example-lib" } }]);
    assert.equal(checkpoints.at(-1).rustLibSessionId, toolSessionId);
    assert.equal(checkpoints.at(-1).completed, true);
  }
});

test("latency formatting retains millisecond precision", () => {
  assert.equal(formatDuration(7), "7 ms");
  assert.equal(formatDuration(1234), "1.234 s");
  assert.equal(formatDuration(62_345), "1m 2.345s");
});

test("live conversations cannot mutate the Kmap", async () => {
  const api = new MockKweb([node(1, [2]), node(2)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  const executor = new ToolExecutor({ mode: "conversation", context, api, loadLimit: 20 });
  for (const call of [
    { name: "ConnectNodes", arguments: { identifiers: [1, 2] } },
    { name: "ConsolidateFanout", arguments: { parentIdentifier: 1, aggregatorIdentifier: 2, fanoutIdentifiers: [2] } },
    { name: "SetFixedConnection", arguments: { parentIdentifier: 1, childIdentifier: 2, slot: 1 } },
    { name: "CreateNode", arguments: { parentIdentifiers: [1], ownerIdentifier: 1, shortName: "Memory", shortDescription: "Memory.", longDescription: "Memory." } },
    { name: "UpdateNode", arguments: { identifier: 1, ownerIdentifier: 1, newShortName: "Root", newShortDescription: "Root.", newLongDescription: "Root." } },
  ]) {
    const result = await executor.execute({ id: call.name, ...call });
    assert.match(result.message.content, /only available during history ingress/);
  }
});

test("self time authorizes Kmap writes and exposes its clean-slate end tool only there", async () => {
  const api = new MockKweb([node(1, [2]), node(2)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  let endCalls = 0;
  let forwardedMessage = null;
  const executor = new ToolExecutor({
    mode: "free-time", context, api, provenanceId: "free-time-provenance", loadLimit: 50,
    endSession: async message => { endCalls += 1; forwardedMessage = message; return { totalTimeReduced: false, remaining: "12:00", messageForwarded: Boolean(message) }; },
  });
  const connected = await executor.execute({ id: "connect", name: "ConnectNodes", arguments: { identifiers: [1, 2] } });
  assert.equal(connected.message.tool_result.ok, true);
  assert.deepEqual(api.updatedCalls.map(call => call[0]), [id(1), id(2)]);

  const ended = await executor.execute({ id: "end", name: "EndSelfTimeSession", arguments: { message: "Continue with node 7." } });
  assert.equal(ended.endSession, true);
  assert.equal(ended.message.tool_result.result.totalTimeReduced, false);
  assert.equal(ended.message.tool_result.result.messageForwarded, true);
  assert.equal(forwardedMessage, "Continue with node 7.");
  assert.equal(endCalls, 1);

  const invalidMessage = await executor.execute({ id: "invalid-end", name: "EndSelfTimeSession", arguments: { message: "" } });
  assert.equal(invalidMessage.message.tool_result.ok, false);
  assert.match(invalidMessage.message.content, /message must contain between 1 and 400000 characters/);
  assert.equal(endCalls, 1);
  assert.equal(MAX_SELF_TIME_HANDOFF_MESSAGE_CHARACTERS, 400_000);

  const conversation = new ToolExecutor({ mode: "conversation", context, api, loadLimit: 20 });
  const unavailable = await conversation.execute({ id: "end", name: "EndFreeTimeSession", arguments: {} });
  assert.equal(unavailable.message.tool_result.ok, false);
  assert.match(unavailable.message.content, /only available during self time/);

  const ingress = new ToolExecutor({ mode: "ingress", context, api, provenanceId: "ingress-provenance", loadLimit: 50 });
  const ingressEnded = await ingress.execute({ id: "end-ingress", name: "EndHistoryIngress", arguments: {} });
  assert.equal(ingressEnded.endSession, true);
  assert.equal(ingressEnded.message.tool_result.result.ingressEnding, true);
  assert.match(ingressEnded.message.content, /final checkpoint is being saved/);

  const ingressUnavailable = await conversation.execute({ id: "end-ingress", name: "EndHistoryIngress", arguments: {} });
  assert.equal(ingressUnavailable.message.tool_result.ok, false);
  assert.match(ingressUnavailable.message.content, /only available during history ingress/);
});

test("self-time clocks, rollover threshold, and custom prompt preserve one durable run", () => {
  const now = Date.parse("2026-07-17T12:00:00Z");
  const freeTime = { deadlineAt: "2026-07-17T12:02:30Z" };
  const timing = freeTimeTiming(freeTime, now);
  assert.equal(timing.warningDue, true);
  assert.equal(timing.remainingMs, 150_000);
  assert.equal(timing.hardStopMs, Date.parse(freeTime.deadlineAt) + FREE_TIME_HARD_STOP_GRACE_MS);
  assert.equal(freeTimeRequestTimeoutSeconds(freeTime, now), 270);
  assert.equal(freeTimeRequestTimeoutSeconds(freeTime, timing.hardStopMs - 29_500), 30);
  assert.equal(FREE_TIME_WARNING_MS, 180_000);
  assert.equal(FREE_TIME_CONTINUATION_MINIMUM_MS, 300_000);
  assert.equal(freeTimeCanStartNewSession({ deadlineAt: new Date(now + 300_000).toISOString() }, now), true);
  assert.equal(freeTimeCanStartNewSession({ deadlineAt: new Date(now + 299_999).toISOString() }, now), false);
  assert.equal(parseFreeTimeMinutes("0.1"), 0.1);
  assert.equal(parseFreeTimeMinutes("480"), 480);
  assert.throws(() => parseFreeTimeMinutes("0"), /between 0.1/);
  assert.equal(parseSelfTimePrompt("  Study the Telegram relay.  "), "Study the Telegram relay.");
  assert.throws(() => parseSelfTimePrompt("x".repeat(MAX_SELF_TIME_PROMPT_CHARACTERS + 1)), /at most 20,000 characters/);
  const opening = freeTimeOpeningMessage({
    runId: "self-time-run",
    sliceIndex: 2,
    runStartedAt: new Date(now).toISOString(),
    deadlineAt: new Date(now + 300_000).toISOString(),
    customPrompt: "Study the Telegram relay.",
  }, now);
  assert.equal(opening, "Self time session 2 is open, you have 5:00 remaining in the shared sessions run.");
  assert.match(
    freeTimeTurnContinuationMessage({ deadlineAt: new Date(now + 300_000).toISOString() }, now),
    /still active, with 5:00 remaining.*normal answer does not end self time.*EndSelfTimeSession/s,
  );
  assert.match(
    freeTimeNoAnswerContinuationMessage({ deadlineAt: new Date(now + 300_000).toISOString() }, now),
    /no assistant answer.*5:00 remaining.*concrete answer or Kennedy tool call/s,
  );
  const next = nextFreeTimeSlice({
    runId: "self-time-run", sliceIndex: 2, deadlineAt: new Date(now + 600_000).toISOString(),
    customPrompt: "Study the Telegram relay.", handoffMessage: "Old message.",
    nextSessionMessage: "Continue with node 7.", warningNoticeAt: "now", sliceEndedAt: "now", sliceEndedReason: "tool",
  });
  assert.equal(next.sliceIndex, 3);
  assert.equal(next.customPrompt, "Study the Telegram relay.");
  assert.equal(next.handoffMessage, "Continue with node 7.");
  assert.equal("nextSessionMessage" in next, false);
  assert.equal("warningNoticeAt" in next, false);
});

test("self-time opening keeps the automatic, launch, and handoff messages distinct", async () => {
  let now = Date.parse("2026-07-17T12:00:00Z");
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]), intelligence: {}, manuals: promptManuals(), rootNodeIds: [id(1)],
    provider: "p", model: "m", sessionType: "free-time", provenanceId: "prov", now: () => now,
    freeTime: {
      runId: "run-1", runStartedAt: new Date(now).toISOString(), deadlineAt: new Date(now + 600_000).toISOString(),
      durationMinutes: 10, sliceIndex: 3, provenanceId: "prov", customPrompt: "Study the Telegram relay.",
      handoffMessage: "Continue from node 7.\nKeep the uncertainty.",
    },
  });
  await session.initialize();
  assert.equal(session.stageFreeTimeOpening(), true);
  assert.deepEqual(session.transcript.map(item => item.content), [
    "Self time session 3 is open, you have 10:00 remaining in the shared sessions run.",
    "Study the Telegram relay.",
    "Message passed from the previous self time session:\n\nContinue from node 7.\nKeep the uncertainty.",
  ]);

  const ended = await session.executor.execute({ id: "end", name: "EndSelfTimeSession", arguments: { message: "Inspect node 11 next." } });
  assert.equal(ended.message.tool_result.result.messageForwarded, true);
  assert.equal(session.freeTime.nextSessionMessage, "Inspect node 11 next.");
  assert.equal(session.snapshot().archive.freeTime.nextSessionMessage, "Inspect node 11 next.");

  now += 360_001;
  const noNext = await session.executor.execute({ id: "end-late", name: "EndSelfTimeSession", arguments: { message: "This cannot be forwarded." } });
  assert.equal(noNext.message.tool_result.result.messageForwarded, false);
  assert.equal("nextSessionMessage" in session.freeTime, false);
});

test("web tools reject extra retrieval knobs and remain available during ingress", async () => {
  const searches = [];
  const intelligence = { webSearch: async body => { searches.push(body); return { answer: "Ingress evidence.", sources: [] }; } };
  const conversation = new ToolExecutor({ mode: "conversation", context: {}, api: {}, intelligence, loadLimit: 20 });
  const extra = await conversation.execute({ id: "search", name: "WebSearch", arguments: { question: "topic", mode: "fast", maxResults: 10 } });
  assert.match(extra.message.content, /Expected exactly: question, mode/);
  const invalidMode = await conversation.execute({ id: "search", name: "WebSearch", arguments: { question: "topic", mode: "turbo" } });
  assert.match(invalidMode.message.content, /mode must be one of: quality, balanced, fast/);
  const ingress = new ToolExecutor({ mode: "ingress", context: {}, api: {}, intelligence, provider: "primary", model: "model", provenanceId: "p", loadLimit: 50 });
  const result = await ingress.execute({ id: "search", name: "WebSearch", arguments: { question: "topic", mode: "fast" } });
  assert.equal(result.message.tool_result.ok, true);
  assert.deepEqual(searches, [{ provider: "primary", model: "model", question: "topic", mode: "fast" }]);
});

test("chatend reset orders retained self-messages before roots and requested nodes", async () => {
  const api = new MockKweb([node(1, [3]), node(2), node(3), node(4)]); const context = new KwebContext(api, [id(1), id(2)]); await context.initialize();
  await context.reset([id(4), id(3)]);
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "hello" }]);
  chatend.append({ role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}}]}' });
  const resetCall = { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"ResetContext","arguments":{"identifiers":[4,3],"selfMessage":"latest ideas"}}]}' };
  const resetResult = { role: "user", display_role: "Memory tool result", content: "Memory context reset completed." };
  chatend.rebuildAfterReset("earlier ideas", { retainedNodeNames: ["Node 4", "Node 3"], budgetUsed: 1, budgetLimit: 20 }, resetCall, resetResult);
  chatend.append({ role: "assistant", content: "discard this tool activity" });
  chatend.rebuildAfterReset("latest ideas", { retainedNodeNames: ["Node 3", "Node 4"], budgetUsed: 2, budgetLimit: 20 }, resetCall, resetResult);
  assert.equal(chatend.messages.some(message => message.content?.includes('"LoadNode"')), false);
  assert.deepEqual(chatend.messages.slice(1, 6).map(message => message.display_role || message.content), [
    "hello",
    "Kennedy note to self",
    "ResetContext history",
    "Kennedy note to self",
    "Kmap context",
  ]);
  assert.deepEqual(chatend.messages.filter(message => message.context_kind === "reset-note").map(message => message.content), ["earlier ideas", "latest ideas"]);
  const resetHistory = chatend.messages.find(message => message.context_kind === "reset-history");
  assert.match(resetHistory.content, /2 successful calls · shared context-load budget at latest reset: 2\/20/);
  assert.match(resetHistory.content, /2× Node 3 \| Node 4/);
  assert.equal(resetHistory.reset_history_entries.length, 2);
  const memory = chatend.messages.find(message => message.context_kind === "memory").content;
  assert.ok(memory.indexOf("Node 1:") < memory.indexOf("Node 2:"));
  assert.ok(memory.indexOf("Node 2:") < memory.indexOf("Node 4:"));
  assert.ok(memory.indexOf("Node 4:") < memory.indexOf("Node 3:"));
  assert.equal(chatend.messages.some(message => message.content === "discard this tool activity"), false);
  assert.equal(chatend.messages.at(-1).display_role, "Memory tool result");
});

test("compact ResetContext history survives restoration without truncation", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const resetCall = { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"ResetContext","arguments":{"identifiers":[]}}]}' };
  const resetResult = { role: "user", content: "Reset completed." };
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "work" }]);
  for (let call = 1; call <= 21; call++) {
    chatend.rebuildAfterReset(null, { retainedNodeNames: ["Area B", "Area A"], budgetUsed: ((call - 1) % 20) + 1, budgetLimit: 20 }, resetCall, resetResult);
  }
  let history = chatend.messages.find(message => message.context_kind === "reset-history");
  assert.equal(history.reset_history_entries.length, 21);
  assert.match(history.content, /21 successful calls/);
  assert.match(history.content, /21× Area A \| Area B/);

  const restored = new Chatend("instructions", context, JSON.parse(JSON.stringify(chatend.retained)));
  restored.rebuildAfterReset(null, { retainedNodeNames: [], budgetUsed: 1, budgetLimit: 20 }, resetCall, resetResult);
  history = restored.messages.find(message => message.context_kind === "reset-history");
  assert.equal(history.reset_history_entries.length, 22);
  assert.match(history.content, /22 successful calls/);
  assert.match(history.content, /21× Area A \| Area B/);
  assert.match(history.content, /1× roots only/);
});

test("transparent tool protocol parses multiple calls from one model response", () => {
  const calls = parseToolCalls('KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}},{"name":"ConnectNodes","arguments":{"identifiers":[1,2]}}]}');
  assert.equal(calls.length, 2);
  assert.equal(calls[0].name, "LoadNode");
  assert.deepEqual(calls[1].arguments, { identifiers: [1, 2] });
  assert.equal(parseToolCalls("A normal answer."), null);
});

test("tool protocol truncates everything after its first valid envelope and still rejects leading narration", () => {
  const envelope = 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"WebSearch","arguments":{"question":"Compare {official} sources and escape \\\"quoted\\\" names.","mode":"quality"}}]}';
  assert.equal(parseToolCalls(`${envelope}\n  `)[0].name, "WebSearch");
  assert.equal(parseToolCalls(`${envelope}\nI’m looking this up now.`)[0].name, "WebSearch");
  const second = 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":9}}]}';
  const repeated = `${envelope}\n${second}`;
  assert.equal(parseToolCalls(repeated).length, 1);
  assert.equal(parseToolCalls(repeated)[0].name, "WebSearch");
  assert.equal(truncateToolResponse(repeated), envelope);
  assert.throws(
    () => parseToolCalls(`I’m looking this up now.\n${envelope}`),
    /must be the first text in a tool-request response/,
  );
  assert.throws(
    () => parseToolCalls(`\`\`\`json\n${envelope}\n\`\`\``),
    /must be the first text in a tool-request response/,
  );
});

test("a truncated tool response abandons the provider thread and replays only its first envelope", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "work" }]);
  const requests = [];
  const firstEnvelope = 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"First","arguments":{}}]}';
  const ignoredEnvelope = 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"Ignored","arguments":{}}]}';
  const intelligence = { generate: async request => {
    requests.push(request);
    if (requests.length === 1) {
      return {
        status: "complete", response_id: "dirty-thread",
        message: { role: "assistant", content: `${firstEnvelope}\nTrailing commentary.\n${ignoredEnvelope}` },
        usage: null,
      };
    }
    return { status: "complete", response_id: "clean-thread", message: { role: "assistant", content: "Finished." }, usage: null };
  } };
  const executed = [];
  await runAgentLoop({
    intelligence, provider: "p", model: "m", chatend,
    executor: {
      execute: async call => {
        executed.push(call.name);
        return { reset: false, message: { role: "user", display_role: "Memory tool result", content: `${call.name} completed.` } };
      },
      failure: () => { throw new Error("unexpected failure"); },
    },
    continuation: new ContinuationState("kennedy-test"), usage: new UsageTracker(),
  });
  assert.deepEqual(executed, ["First"]);
  assert.equal(requests[1].previous_response_id, null);
  assert.match(requests[1].chatend, /Agent manuals\n\ninstructions/);
  assert.match(requests[1].chatend, /First completed\./);
  assert.doesNotMatch(requests[1].chatend, /Trailing commentary|Ignored/);
  assert.equal(chatend.messages.some(message => message.content?.includes?.("Trailing commentary")), false);
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
      usage: { input_tokens: 100, output_tokens: 10, cached_tokens: 80, cache_write_tokens: 20, reasoning_tokens: 4, cumulative: true, last_input_tokens: 100, last_output_tokens: 10 },
    };
    return { status: "complete", response_id: "resp_2", message: { role: "assistant", content: "Finished." }, usage: { input_tokens: 230, output_tokens: 15, cached_tokens: 180, cache_write_tokens: 20, reasoning_tokens: 4, cumulative: true, last_input_tokens: 130, last_output_tokens: 5 } };
  } };
  const executed = [];
  const executor = {
    execute: async call => { executed.push(call.name); return { reset: false, message: { role: "user", display_role: "Memory tool result", content: `${call.name} completed.` } }; },
    failure: () => { throw new Error("unexpected failure"); },
  };
  const continuation = new ContinuationState("kennedy-test");
  const usage = new UsageTracker({ contextWindowTokens: 1000, maxInputTokens: 900 });
  const checkpoints = [];
  assert.equal(usage.snapshot().contextKnown, false);
  assert.equal(usage.snapshot().contextRemaining, null);
  const answer = await runAgentLoop({
    intelligence, provider: "p", model: "m", chatend, executor, continuation, usage,
    checkpoint: async () => checkpoints.push(chatend.messages.map(message => message.content)),
  });
  assert.equal(answer, "Finished.");
  assert.deepEqual(executed, ["First", "Second"]);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].previous_response_id, "resp_1");
  assert.match(requests[1].chatend, /^Latency\n\nLatency: LLM call \d+ ms/);
  assert.match(requests[1].chatend, /Memory tool result\n\nFirst completed\./);
  assert.match(requests[1].chatend, /Memory tool result\n\nSecond completed\./);
  assert.match(requests[0].chatend, /context window usage: unknown \/ 1,000$/);
  assert.match(requests[1].chatend, /context window usage: 110 \/ 1,000$/);
  assert.equal("messages" in requests[1], false);
  assert.equal("tools" in requests[0], false);
  assert.equal(usage.snapshot().totalCachedTokens, 180);
  assert.equal(usage.snapshot().contextTokens, 135);
  assert.equal(usage.snapshot().contextRemaining, 865);
  assert.equal(usage.snapshot().cacheReadPercent, (100 * 180) / 230);
  assert.equal(checkpoints.length, 1);
  assert.equal(checkpoints[0].includes("First completed."), true);
  assert.equal(checkpoints[0].includes("Second completed."), true);
});

test("EndSelfTimeSession checkpoints its result and returns loop control without another generation", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const chatend = new Chatend("free-time instructions", context, [{ role: "user", content: "Have fun." }]);
  let requests = 0;
  let checkpoints = 0;
  const intelligence = {
    generate: async () => {
      requests += 1;
      return {
        status: "complete", response_id: "free-time-thread", usage: null,
        message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"EndSelfTimeSession","arguments":{}}]}' },
      };
    },
  };
  const executor = new ToolExecutor({
    mode: "free-time", context, api: context.api, provenanceId: "prov", loadLimit: 50,
    endSession: () => ({ totalTimeReduced: false }),
  });
  const result = await runAgentLoop({
    intelligence, provider: "p", model: "m", chatend, executor,
    continuation: new ContinuationState("free-time-test"), usage: new UsageTracker(),
    checkpoint: async () => { checkpoints += 1; },
  });
  assert.equal(result, AGENT_LOOP_SESSION_ENDED);
  assert.equal(requests, 1);
  assert.equal(checkpoints, 1);
  assert.match(chatend.messages.at(-1).content, /total time unchanged/i);
});

test("self time continues after empty and ordinary answers until EndSelfTimeSession", async () => {
  const now = Date.parse("2026-07-17T12:00:00Z");
  const requests = [];
  let generations = 0;
  const intelligence = {
    generate: async request => {
      requests.push(request);
      generations += 1;
      if (generations === 1) {
        throw Object.assign(new Error("Codex turn failed: Codex returned no assistant message."), {
          code: "empty_assistant_message",
        });
      }
      if (generations === 2) {
        return {
          status: "complete", response_id: "ordinary-answer-thread", usage: null,
          message: { role: "assistant", content: "Session ended before the work was completed." },
        };
      }
      return {
        status: "complete", response_id: "end-session-thread", usage: null,
        message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"EndSelfTimeSession","arguments":{}}]}' },
      };
    },
  };
  const checkpoints = [];
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]), intelligence, manuals: promptManuals(), rootNodeIds: [id(1)],
    provider: "p", model: "m", sessionType: "free-time", provenanceId: "prov", now: () => now,
    freeTime: {
      runId: "run-1", runStartedAt: new Date(now).toISOString(), deadlineAt: new Date(now + 600_000).toISOString(),
      durationMinutes: 10, sliceIndex: 1, provenanceId: "prov",
    },
    persist: async state => checkpoints.push(structuredClone(state)),
  });
  await session.initialize();
  session.stageFreeTimeOpening();
  await assert.rejects(
    () => session.finalizeFreeTime(),
    /only after EndSelfTimeSession or the shared deadline/,
  );

  const result = await session.resumePendingTurn();

  assert.equal(result.description, AGENT_LOOP_SESSION_ENDED.description);
  assert.equal(generations, 3);
  assert.equal(session.freeTimeEndReason, "tool");
  assert.equal(session.pendingTurn, false);
  const controllerMessages = session.chatend.messages.filter(message => message.context_kind === "free-time-continuation");
  assert.equal(controllerMessages.length, 2);
  assert.match(controllerMessages[0].content, /no assistant answer.*10:00 remaining/s);
  assert.match(controllerMessages[1].content, /still active, with 10:00 remaining.*normal answer does not end self time/s);
  assert.equal(requests[1].previous_response_id, null);
  assert.match(requests[1].chatend, /no assistant answer/);
  assert.equal(requests[2].previous_response_id, "ordinary-answer-thread");
  assert.match(requests[2].chatend, /normal answer does not end self time/);
  assert.ok(checkpoints.some(state => state.archive.messages.some(message => message.content?.includes?.("no assistant answer"))));
  assert.ok(checkpoints.some(state => state.archive.messages.some(message => message.content?.includes?.("normal answer does not end self time"))));
});

test("a pre-boundary Codex thread is replayed from the full visible Chatend", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const chatend = new Chatend("visible instructions", context, [{ role: "user", content: "earlier" }]);
  const continuation = new ContinuationState("kennedy-test");
  continuation.accept("legacy-thread", chatend.messages.length);
  chatend.append({ role: "user", content: "new question" });
  const requests = [];
  const intelligence = { generate: async request => {
    requests.push(request);
    if (requests.length === 1) {
      throw Object.assign(new Error("stale"), { code: "stale_codex_thread" });
    }
    return { status: "complete", response_id: "clean-thread", message: { role: "assistant", content: "Clean answer." }, usage: null };
  } };
  const answer = await runAgentLoop({
    intelligence, provider: "p", model: "m", chatend,
    executor: { execute: async () => { throw new Error("unexpected tool"); } },
    continuation, usage: new UsageTracker(),
  });
  assert.equal(answer, "Clean answer.");
  assert.equal(requests[0].previous_response_id, "legacy-thread");
  assert.doesNotMatch(requests[0].chatend, /visible instructions/);
  assert.equal(requests[1].previous_response_id, null);
  assert.match(requests[1].chatend, /visible instructions/);
  assert.match(requests[1].chatend, /new question/);
});

test("concise context usage is model-visible and stale thread usage is cleared on reset", () => {
  const usage = new UsageTracker({ contextWindowTokens: 258400, maxInputTokens: 258400 });
  assert.equal(
    formatContextWindowProgress(usage.snapshot()),
    "context window usage: unknown / 258,400",
  );
  usage.record({
    input_tokens: 200000, output_tokens: 1000, cached_tokens: 150000,
    cache_write_tokens: 0, reasoning_tokens: 500, cumulative: true,
    last_input_tokens: 200000, last_output_tokens: 1000,
  });
  assert.equal(
    formatContextWindowProgress(usage.snapshot()),
    "context window usage: 201,000 / 258,400",
  );
  usage.resetThread();
  assert.equal(usage.snapshot().contextKnown, false);
  assert.equal(formatContextWindowProgress(usage.snapshot()), "context window usage: unknown / 258,400");
  assert.equal(usage.snapshot().totalInputTokens, 200000);
});

test("cumulative multi-pass usage is never presented as current context occupancy", () => {
  const usage = new UsageTracker({ contextWindowTokens: 258400, maxInputTokens: 258400 });
  usage.record({
    input_tokens: 281555, output_tokens: 238, cached_tokens: 140032,
    cache_write_tokens: 0, reasoning_tokens: 0, cumulative: true,
  });
  assert.equal(usage.snapshot().contextKnown, false);
  assert.equal(usage.snapshot().contextTokens, 0);
  assert.equal(usage.snapshot().last.inputTokens, 281555);
  assert.equal(usage.snapshot().totalInputTokens, 281555);
  assert.equal(formatContextWindowProgress(usage.snapshot()), "context window usage: unknown / 258,400");

  const restored = new UsageTracker({ contextWindowTokens: 258400, maxInputTokens: 258400 });
  restored.restore({
    totalInputTokens: 281555,
    last: { inputTokens: 281555, outputTokens: 238 },
  });
  assert.equal(restored.snapshot().contextKnown, false);

  const exact = new UsageTracker({ contextWindowTokens: 258400, maxInputTokens: 258400 });
  exact.record({
    input_tokens: 281555, output_tokens: 238, cached_tokens: 140032,
    cache_write_tokens: 0, reasoning_tokens: 127, cumulative: true,
    last_input_tokens: 140857, last_output_tokens: 92,
  });
  assert.equal(exact.snapshot().contextKnown, true);
  assert.equal(exact.snapshot().contextTokens, 140949);
  assert.equal(formatContextWindowProgress(exact.snapshot()), "context window usage: 140,949 / 258,400");
});

test("ResetContext abandons continuation and resends the rebuilt full chatend", async () => {
  const context = new KwebContext(new MockKweb([node(1)]), id(1)); await context.initialize();
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "reset it" }]);
  chatend.append({ role: "assistant", content: "Old pre-reset activity." });
  const requests = [];
  const intelligence = { generate: async request => {
    requests.push(request);
    if (requests.length === 1) return { status: "complete", response_id: "resp_old", message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"ResetContext","arguments":{"identifiers":[],"selfMessage":"carry this forward"}}]}' }, usage: null };
    return { status: "complete", response_id: "resp_new", message: { role: "assistant", content: "Fresh context." }, usage: null };
  } };
  const executor = {
    execute: async () => ({
      reset: true,
      selfMessage: "carry this forward",
      resetHistoryEntry: { retainedNodeNames: [], budgetUsed: 1, budgetLimit: 20 },
      previousContext: context.snapshot(),
      message: { role: "user", display_role: "Memory tool result", content: "Memory context reset completed." },
    }),
    failure: () => { throw new Error("unexpected failure"); },
  };
  await runAgentLoop({ intelligence, provider: "p", model: "m", chatend, executor, continuation: new ContinuationState("kennedy-test"), usage: new UsageTracker() });
  assert.equal(requests[0].previous_response_id, null);
  assert.equal(requests[1].previous_response_id, null);
  assert.match(requests[1].chatend, /^Agent manuals\n\ninstructions/);
  assert.match(requests[1].chatend, /1× roots only/);
  assert.match(requests[1].chatend, /Kennedy note to self\n\ncarry this forward/);
  assert.match(requests[1].chatend, /Memory tool result\n\nMemory context reset completed\./);
  assert.equal((requests[1].chatend.match(/Current Kmap context/g) || []).length, 1);
  assert.match(requests[1].chatend, /context window usage: unknown$/);
  assert.ok(requests[1].chatend.indexOf("1× roots only") < requests[1].chatend.indexOf("carry this forward"));
  assert.ok(requests[1].chatend.indexOf("carry this forward") < requests[1].chatend.indexOf("Kmap context"));
  assert.doesNotMatch(requests[1].chatend, /\"role\":\"system\"/);
  assert.equal(chatend.historySegments.length, 1);
  assert.equal(chatend.historySegments[0].reason, "ResetContext");
  assert.equal(chatend.historySegments[0].messages.some(message => message.content === "Old pre-reset activity."), true);
  assert.equal(chatend.messages.some(message => message.content === "Old pre-reset activity."), false);
  assert.equal(chatend.historySegments[0].memory.nodes[0].longDescription, "Details 1");
});

test("a repeated ResetContext loop cannot successfully reset more than the shared budget", async () => {
  const api = new MockKweb([node(1)]);
  const context = new KwebContext(api, id(1)); await context.initialize();
  const chatend = new Chatend("instructions", context, [{ role: "user", content: "work across memory" }]);
  let requests = 0;
  const intelligence = { generate: async () => {
    requests += 1;
    return {
      status: "complete",
      response_id: `resp_${requests}`,
      message: {
        role: "assistant",
        content: requests <= 22
          ? 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"ResetContext","arguments":{"identifiers":[]}}]}'
          : "Stopped resetting.",
      },
      usage: null,
    };
  } };
  const executor = new ToolExecutor({ mode: "conversation", context, api, loadLimit: 20 });
  const answer = await runAgentLoop({
    intelligence, provider: "p", model: "m", chatend, executor,
    continuation: new ContinuationState("kennedy-test"), usage: new UsageTracker(),
  });
  assert.equal(answer, "Stopped resetting.");
  assert.equal(executor.loadCalls, 22);
  assert.equal(executor.toolLog.filter(entry => entry.name === "ResetContext" && entry.ok).length, 20);
  assert.equal(executor.toolLog.filter(entry => entry.name === "ResetContext" && !entry.ok && entry.code === "load_budget_exhausted").length, 2);
  const history = chatend.messages.find(message => message.context_kind === "reset-history");
  assert.equal(history.reset_history_entries.length, 20);
  assert.match(history.content, /20× roots only/);
});

test("conversation provenance preserves the complete structured Chatend", async () => {
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]), intelligence: {},
    manuals: promptManuals("Shared"), rootNodeId: id(1),
    provider: "p", providerKind: "codex", model: "m", onUpdate: () => {},
  });
  await session.initialize();
  session.transcript = [{ role: "user", content: "Hi" }, { role: "kennedy", content: "Hello" }];
  session.chatend.append({ role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":1}}]}' });
  session.chatend.append({ role: "user", display_role: "Memory tool result", content: "Kennedy tool result\nTool: LoadNode\n\nLoaded." });
  session.chatend.append({
    role: "user",
    content: [{ type: "input_text", text: "Look at this" }, { type: "input_image", image_url: "data:image/png;base64,AAAA" }],
  });
  session.chatend.historySegments.push({ reason: "ResetContext", messages: [{ role: "assistant", content: "Earlier context." }], memory: session.context.snapshot(), usage: null });
  const archive = JSON.parse(session.serialize());
  assert.equal(archive.format, "kennedy-chatend");
  assert.equal(archive.version, 2);
  assert.equal("modelAttribution" in archive, false);
  assert.match(archive.systemPrompt, /Shared/);
  assert.match(archive.systemPrompt, /Codex harness\n\nShared Codex outer-harness note/);
  assert.equal(archive.context.snapshot.nodes[0].longDescription, "Details 1");
  assert.match(archive.messages.find(message => typeof message.content === "string" && message.content.includes("KENNEDY_TOOL_CALLS")).content, /LoadNode/);
  assert.match(archive.messages.find(message => message.display_role === "Memory tool result").content, /Loaded/);
  assert.equal(archive.messages.at(-1).content[1].image_url, "data:image/png;base64,AAAA");
  assert.equal(archive.fullHistory.segments[0].messages[0].content, "Earlier context.");
});

test("conversation archives retain the hidden Rust-library session and release its handles", async () => {
  const released = [];
  const rustLibs = { release: async sessionId => { released.push(sessionId); return { released: 2 }; } };
  const kweb = new MockKweb([node(1)]);
  const source = new ConversationSession({
    kweb, rustLibs, manuals: promptManuals(), rootNodeId: id(1), provider: "primary", providerKind: "codex", model: "model", reasoningEffort: "high",
  });
  await source.initialize();
  const snapshot = source.snapshot();
  assert.match(snapshot.rustLibSessionId, /^kennedy:/);
  assert.equal(snapshot.archive.rustLibSessionId, snapshot.rustLibSessionId);

  const restored = new ConversationSession({
    kweb, rustLibs, manuals: promptManuals(), rootNodeId: id(1), provider: "primary", providerKind: "codex", model: "model", reasoningEffort: "high",
  });
  await restored.initialize(snapshot);
  assert.equal(restored.rustLibSessionId, source.rustLibSessionId);
  assert.deepEqual(await restored.releaseRustLibs(), { released: 2 });
  assert.deepEqual(released, [source.rustLibSessionId]);
});

test("free-time sessions durably inject one warning and one deadline wrap-up notice", async () => {
  let now = Date.parse("2026-07-17T12:27:30Z");
  const deadlineAt = "2026-07-17T12:30:00.000Z";
  const checkpoints = [];
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]), intelligence: {}, manuals: promptManuals(), rootNodeIds: [id(1)],
    provider: "p", providerKind: "direct-api", model: "m", reasoningEffort: "high",
    sessionType: "free-time", provenanceId: "prov", now: () => now,
    freeTime: {
      runId: "run-1", runStartedAt: "2026-07-17T12:00:00.000Z", deadlineAt,
      durationMinutes: 30, sliceIndex: 1, provenanceId: "prov",
    },
    persist: async state => checkpoints.push(state),
  });
  await session.initialize();
  session.stageFreeTimeOpening();
  await session.prepareFreeTimeRound();
  await session.prepareFreeTimeRound();
  assert.equal(session.chatend.messages.filter(message => message.context_kind === "free-time-timer").length, 1);
  assert.match(session.chatend.messages.find(message => message.context_kind === "free-time-timer").content, /final three minutes/);
  assert.ok(checkpoints.length >= 1);

  now = Date.parse(deadlineAt) + 1;
  const directive = await session.prepareFreeTimeRound();
  assert.deepEqual(directive, { endAfterResponse: true });
  assert.equal(session.chatend.messages.filter(message => message.context_kind === "free-time-timer").length, 2);
  assert.match(session.chatend.messages.at(-1).content, /final wrap-up round/);
  const blocked = await session.executor.execute({ id: "load", name: "LoadNode", arguments: { identifier: 1 } });
  assert.equal(blocked.message.tool_result.ok, false);
  assert.match(blocked.message.content, /tools are no longer available/);
});

test("a structured Chatend archive retains activity while refreshing current manuals and context", async () => {
  const kweb = new MockKweb([node(1), node(2), node(3)]);
  const source = new ConversationSession({
    kweb, intelligence: {}, manuals: promptManuals("Shared"),
    rootNodeId: id(1), provider: "p", model: "m", onUpdate: () => {},
  });
  await source.initialize();
  await source.context.loadDurable(id(2));
  source.chatend.append({ role: "assistant", content: "KENNEDY_TOOL_CALLS\n{\"calls\":[{\"name\":\"LoadNode\",\"arguments\":{\"identifier\":2}}]}" });
  source.chatend.append({ role: "user", display_role: "Memory tool result", content: "Kennedy tool result\nTool: LoadNode\n\nLoaded." });
  source.chatend.historySegments.push({ reason: "ResetContext", messages: [{ role: "assistant", content: "Pre-reset archived activity." }], memory: source.context.snapshot(), usage: null });
  source.executor.loadCalls = 3;
  source.executor.toolLog.push({ name: "LoadNode", ok: true });
  source.usage.record({ input_tokens: 12, output_tokens: 3, cached_tokens: 4, cache_write_tokens: 0, reasoning_tokens: 1 });
  const saved = source.snapshot();

  const restored = new ConversationSession({
    kweb, intelligence: {}, manuals: promptManuals("Changed"),
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
  assert.deepEqual(restored.chatend.historySegments, source.chatend.historySegments);
});

test("history ingress compacts archived Kmap nodes to titles and short descriptions and resumes completed archives", async () => {
  const kweb = new MockKweb([node(1)]);
  const archivedMessages = [
    { role: "system", display_role: "Agent manuals", content: "Archived instructions" },
    { role: "user", content: "Archived words." },
    { role: "system", display_role: "Kmap context", context_kind: "memory", content: "FULL NODE DETAILS MUST NOT BE INGRESSED" },
    { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}}]}' },
    {
      role: "user", display_role: "Memory tool result", tool_name: "LoadNode", content: "ANOTHER FULL NODE COPY",
      tool_result: { ok: true, result: { indirectFanoutNodes: [{ shortName: "Distant title", shortDescription: "DISTANT DESCRIPTION MUST BE OMITTED" }] } },
    },
    { role: "system", display_role: "Latency", context_kind: "timing", content: "SOURCE TIMING NOISE" },
  ];
  kweb.provenance = async () => ({
    source: "conversation", source_created_at: "2026-07-13T00:00:00Z",
    data: JSON.stringify({
      format: "kennedy-chatend", messages: archivedMessages,
      context: { snapshot: { nodes: [
        { identifier: 2, shortName: "Roadmap", shortDescription: "Current product direction.", longDescription: "Very large details." },
      ] } },
      fullHistory: { segments: [{
        messages: [{ role: "assistant", content: "INSPECTOR ONLY PRE-RESET SECRET" }],
        memory: { nodes: [
          { identifier: 5, shortName: "Launch notes", shortDescription: "Historical launch decisions.", longDescription: "More very large details." },
        ] },
      }] },
      media: [{ kind: "voice", dataUrl: "data:audio/ogg;base64,AAAA" }],
    }),
  });
  let generations = 0;
  const requests = [];
  const timings = [];
  const intelligence = { generate: async request => {
    generations += 1;
    requests.push(request);
    return generations === 1
      ? { status: "complete", response_id: "ingress-response", message: { role: "assistant", content: "Memory review complete." }, usage: null }
      : { status: "complete", response_id: "ingress-ended", message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"EndHistoryIngress","arguments":{}}]}' }, usage: null };
  }, recordTiming: timing => timings.push(timing) };
  const checkpoints = [];
  await runHistoryIngress({
    kweb, intelligence, manuals: promptManuals("Ingress"),
    rootNodeId: id(1), provenanceId: "provenance", provider: "p", providerKind: "codex", model: "m",
    checkpoint: async archive => checkpoints.push(structuredClone(archive)), onUpdate: () => {},
  });
  assert.equal(generations, 2);
  assert.equal(checkpoints[0].completed, false);
  assert.equal(checkpoints.at(-1).completed, true);
  assert.equal("modelAttribution" in checkpoints.at(-1), false);
  assert.match(checkpoints.at(-1).systemPrompt, /Ingress/);
  assert.match(checkpoints.at(-1).systemPrompt, /Codex harness\n\nIngress Codex outer-harness note/);
  assert.match(checkpoints.at(-1).retained[0].content, /Archived Chatend\n\nAgent manuals\n\nArchived instructions/);
  assert.match(checkpoints.at(-1).retained[0].content, /David\n\nArchived words\./);
  assert.match(checkpoints.at(-1).retained[0].content, /Loaded Kmap node summaries from the archived session/);
  assert.match(checkpoints.at(-1).retained[0].content, /Roadmap\n  Current product direction\./);
  assert.match(checkpoints.at(-1).retained[0].content, /Launch notes\n  Historical launch decisions\./);
  assert.match(checkpoints.at(-1).retained[0].content, /- Distant title/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /DISTANT DESCRIPTION MUST BE OMITTED/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /FULL NODE DETAILS MUST NOT BE INGRESSED/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /ANOTHER FULL NODE COPY/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /KENNEDY_TOOL_CALLS/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /SOURCE TIMING NOISE/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /Very large details/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /base64,AAAA/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /"format":"kennedy-chatend"/);
  assert.doesNotMatch(checkpoints.at(-1).retained[0].content, /INSPECTOR ONLY PRE-RESET SECRET/);
  assert.equal(requests[0].chatend, formatChatend(checkpoints[0].messages, checkpoints[0].usage));
  assert.equal(requests[0].chatend, inspectorText({ chatend: checkpoints[0].messages, usage: checkpoints[0].usage }));
  assert.equal("messages" in requests[0], false);
  assert.equal(checkpoints.at(-1).messages.some(message => message.content === "Memory review complete."), true);
  assert.equal(checkpoints.at(-1).messages.some(message => message.context_kind === "history-ingress-continuation"), true);
  assert.match(checkpoints.at(-1).messages.find(message => message.context_kind === "history-ingress-continuation").content, /You do have access to Kennedy tool calls.*EndHistoryIngress/s);
  assert.equal(checkpoints.at(-1).tools.log.at(-1).name, "EndHistoryIngress");
  assert.equal(checkpoints.at(-1).retained[0].context_kind, "provenance");
  assert.match(checkpoints.at(-1).messages.at(-1).content, /final checkpoint is being saved/);
  assert.equal(checkpoints.at(-1).context.snapshot.nodes[0].longDescription, "Details 1");
  assert.deepEqual(checkpoints.at(-1).fullHistory.segments, []);
  assert.deepEqual(timings.map(timing => [timing.action, timing.status, timing.sessionType]), [
    ["tool", "ok", "history-ingress"],
    ["turn", "ok", "history-ingress"],
  ]);

  const resumed = [];
  await runHistoryIngress({
    kweb,
    intelligence: { generate: async () => { throw new Error("completed ingress must not regenerate"); } },
    manuals: promptManuals("Changed"), rootNodeId: id(1),
    provenanceId: "provenance", provider: "p", model: "m",
    restoredArchive: checkpoints.at(-1), checkpoint: async archive => resumed.push(archive), onUpdate: () => {},
  });
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0].completed, true);
  assert.equal(resumed[0].messages.some(message => message.content === "Memory review complete."), true);
  assert.match(resumed[0].messages.at(-1).content, /final checkpoint is being saved/);
  assert.equal(resumed[0].roundsUsed, 2);

  const exhausted = structuredClone(checkpoints.at(-1));
  exhausted.completed = false;
  exhausted.roundsUsed = 100;
  await assert.rejects(() => runHistoryIngress({
    kweb,
    intelligence: { generate: async () => { throw new Error("round limit must prevent generation"); } },
    manuals: promptManuals("Changed"), rootNodeId: id(1),
    provenanceId: "provenance", provider: "p", model: "m",
    restoredArchive: exhausted, checkpoint: async () => {}, onUpdate: () => {},
  }), /100-round tool-loop safety limit/);
});

test("history ingress durably checkpoints pre-reset Full History segments", async () => {
  const kweb = new MockKweb([node(1)]);
  kweb.provenance = async () => ({
    source: "conversation", source_created_at: "2026-07-13T00:00:00Z",
    data: JSON.stringify({ format: "kennedy-chatend", messages: [{ role: "user", content: "Archived conversation." }] }),
  });
  let requests = 0;
  const intelligence = { generate: async () => {
    requests += 1;
    return requests === 1
      ? { status: "complete", response_id: "before-reset", message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"ResetContext","arguments":{"identifiers":[]}}]}' }, usage: null }
      : requests === 2
        ? { status: "complete", response_id: "after-reset", message: { role: "assistant", content: "Ingress complete after reset." }, usage: null }
        : { status: "complete", response_id: "after-reset-end", message: { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"EndHistoryIngress","arguments":{}}]}' }, usage: null };
  } };
  const checkpoints = [];
  await runHistoryIngress({
    kweb, intelligence, manuals: promptManuals("Ingress"),
    rootNodeId: id(1), provenanceId: "provenance", provider: "p", model: "m",
    checkpoint: async archive => checkpoints.push(structuredClone(archive)), onUpdate: () => {},
  });
  const final = checkpoints.at(-1);
  assert.equal(final.completed, true);
  assert.equal(final.fullHistory.segments.length, 1);
  assert.equal(final.fullHistory.segments[0].reason, "ResetContext");
  assert.equal(final.fullHistory.segments[0].messages.some(message => message.content?.includes?.("ResetContext")), true);
  assert.equal(final.fullHistory.segments[0].memory.nodes[0].longDescription, "Details 1");
  assert.equal(final.messages.some(message => message.content === "Ingress complete after reset."), true);
});

test("conversation history titles use the first durable user message", () => {
  const record = { state: { transcript: [
    { role: "user", content: "  Plan   a long weekend in San Salvador with excellent coffee and museums  " },
    { role: "kennedy", content: "Let's do it." },
  ] } };
  assert.equal(conversationTitle(record, 32), "Plan a long weekend in San Salv…");
  assert.equal(conversationTitle({ state: { transcript: [] } }), "New conversation");
});

test("self-time history titles identify clean-slate slices", () => {
  assert.equal(conversationTitle({
    state: { sessionType: "free-time", freeTime: { sliceIndex: 4, customPrompt: "  Explore   memory ownership  " }, transcript: [] },
  }), "Explore memory ownership · session 4");
  assert.equal(conversationTitle({
    state: { sessionType: "free-time", freeTime: { sliceIndex: 2 }, transcript: [] },
  }), "Self time · session 2");
  assert.equal(conversationTitle({
    state: { sessionType: "free-time", freeTime: { sliceIndex: 4, customPrompt: "Explore memory ownership and provenance carefully" }, transcript: [] },
  }, 32), "Explore memory owne… · session 4");
});

test("audio history titles use the durable original filename", () => {
  assert.equal(audioRecordingTitle({ original_filename: "2026-07-16-vnote.wav" }), "2026-07-16-vnote.wav");
  assert.equal(audioRecordingTitle({ original_filename: "x".repeat(80) }, 20), `${"x".repeat(19)}…`);
});

test("conversation sidebar distinguishes continuable and closed records", async () => {
  const render = await readFile(new URL("../public/js/render.js", import.meta.url), "utf8");
  assert.match(render, /active: "Live · Continue"/);
  assert.match(render, /ingress_pending: "Closed · Memory queued"/);
  assert.match(render, /ingress_failed: "Closed · Memory failed"/);
  assert.match(render, /complete: "Saved · Read only"/);
});

test("the selected history row exposes a guarded force-purge action", async () => {
  const [app, coordinator, render] = await Promise.all([
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/memory_ingress_coordinator.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/render.js", import.meta.url), "utf8"),
  ]);
  assert.match(render, /record\.id === selectedId/);
  assert.match(render, /Permanently purge/);
  assert.match(app, /window\.confirm\(`Permanently purge this conversation/);
  assert.match(app, /conversationHistory\.purge\(id, \{ expected_version: latest\.version \}\)/);
  assert.match(app, /The conversation will not be sent through history ingress/);
  assert.match(coordinator, /beforeMutation: async \(\) =>/);
  const select = app.match(/async function selectConversation\(id\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(select);
  assert.ok(select.indexOf("selectedConversationId = id") < select.indexOf("await buildConversation(record)"));
  assert.match(select, /You can still purge it/);
});

test("conversation history keeps live, finalizing, and finalized records in separate groups", () => {
  const records = [
    { id: "complete-new", phase: "complete", updated_at: "2026-07-16T12:00:00Z" },
    { id: "pending-old", phase: "ingress_pending", updated_at: "2026-07-16T08:00:00Z" },
    { id: "active-old", phase: "active", updated_at: "2026-07-15T08:00:00Z" },
    { id: "failed-new", phase: "ingress_failed", updated_at: "2026-07-16T11:00:00Z" },
    { id: "active-new", phase: "active", updated_at: "2026-07-16T09:00:00Z" },
    { id: "progress-middle", phase: "ingress_in_progress", updated_at: "2026-07-16T10:00:00Z" },
    { id: "complete-old", phase: "complete", updated_at: "2026-07-14T12:00:00Z" },
  ];
  assert.deepEqual(sortConversationHistory(records).map(record => record.id), [
    "active-new",
    "active-old",
    "failed-new",
    "progress-middle",
    "pending-old",
    "complete-new",
    "complete-old",
  ]);
  assert.equal(records[0].id, "complete-new", "sorting must not mutate backend results");
});

test("conversation history reconciliation never regresses a cached record version", () => {
  const cached = [
    { id: "active", version: 4, phase: "active", updated_at: "2026-07-17T12:04:00Z" },
    { id: "removed", version: 1, phase: "complete", updated_at: "2026-07-17T12:01:00Z" },
  ];
  const staleResponse = [
    { id: "active", version: 3, phase: "active", updated_at: "2026-07-17T12:03:00Z" },
    { id: "new", version: 1, phase: "complete", updated_at: "2026-07-17T12:02:00Z" },
  ];
  const reconciled = reconcileConversationHistory(cached, staleResponse);
  assert.equal(reconciled.find(record => record.id === "active")?.version, 4);
  assert.equal(reconciled.some(record => record.id === "removed"), false);
  assert.equal(reconciled.find(record => record.id === "new")?.version, 1);

  const currentResponse = [{ id: "active", version: 5, phase: "active", updated_at: "2026-07-17T12:05:00Z" }];
  assert.equal(reconcileConversationHistory(reconciled, currentResponse)[0].version, 5);

  const hydrated = [{ id: "active", version: 5, phase: "active", state: { transcript: [{ role: "user", content: "Full history" }] } }];
  const sameVersionSummary = [{ id: "active", version: 5, phase: "active", summary: true, state: { transcript: [{ role: "user", content: "Full history" }] } }];
  assert.equal(reconcileConversationHistory(hydrated, sameVersionSummary)[0], hydrated[0]);
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
  assert.equal(controls.stopHidden, false);
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
  assert.equal(responding.stopHidden, false);
});

test("closed conversations do not render a message composer", async () => {
  const controls = conversationControlState({
    hasSession: false, sessionBusy: false, transitionBusy: false,
    pendingTurn: false, viewingHistory: true, transcriptLength: 0,
  });
  assert.equal(controls.composerHidden, true);
  assert.equal(controls.inputDisabled, true);
  assert.equal(controls.stopHidden, true);
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

test("composer exposes an explicit PDF upload button", async () => {
  const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
  assert.match(html, /id="attach-button"[^>]*>Upload PDF<\/button>/);
  assert.match(html, /id="attachment-input"[^>]+type="file"[^>]+accept="[^"]*\.pdf/);
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
  const failed = conversationIngressActivity({ record: {
    ...record,
    phase: "ingress_failed",
    ingress_failure_count: 5,
    ingress_failures: [{ attempt: 5, stage: "model_loop", code: "provider_error", message: "Context exhausted." }],
  } });
  assert.equal(failed.active, false);
  assert.equal(failed.failed, true);
  assert.equal(failed.failures[0].message, "Context exhausted.");
  assert.equal(conversationIngressActivity({ record, dismissedId: "old" }), null);
});

test("history ingress worker records five failures before abandoning a poisoned session", async () => {
  const [app, coordinator] = await Promise.all([
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/memory_ingress_coordinator.js", import.meta.url), "utf8"),
  ]);
  assert.match(coordinator, /const INGRESS_FAILURE_LIMIT = 5/);
  assert.match(coordinator, /conversationHistory\.ingressFailure/);
  assert.match(coordinator, /failedRecord\.phase === "ingress_failed"/);
  assert.match(coordinator, /History ingress stopped after \$\{failedRecord\.ingress_failure_count\} failed attempts/);
  assert.match(app, /conversationHistory\.retryIngress\(record\.id/);
  assert.match(app, /delete fresh\.historyIngress/);
});

test("memory ingress coordinator resumes claimed work before selecting the oldest pending source", () => {
  const conversation = { id: "conversation", phase: "ingress_pending", started_at: "2026-07-17T12:00:00Z" };
  const audio = { id: "audio", phase: "ingress_pending", source_created_at: "2026-07-17T11:00:00Z" };
  assert.deepEqual(selectNextMemoryIngress(conversation, audio), { kind: "audio", record: audio });
  conversation.phase = "ingress_in_progress";
  assert.deepEqual(selectNextMemoryIngress(conversation, audio), { kind: "conversation", record: conversation });
  conversation.phase = "ingress_pending";
  audio.phase = "ingress_in_progress";
  assert.deepEqual(selectNextMemoryIngress(conversation, audio), { kind: "audio", record: audio });
});

test("failed conversation ingress exposes retry actions in the sidebar and central view", async () => {
  const [app, render] = await Promise.all([
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/render.js", import.meta.url), "utf8"),
  ]);
  assert.match(render, /record\.phase === "ingress_failed"[\s\S]*?history-item-retry/);
  assert.match(render, /function ingressRetryNotice/);
  assert.match(render, /Retry history ingress/);
  assert.match(app, /onRetryIngress: retryConversationIngress/);
  assert.match(app, /record\?\.phase === "ingress_failed"[\s\S]*?onRetry: \(\) => retryConversationIngress\(record\)/);
});

test("audio ingress client can explicitly requeue a terminal piece", async () => {
  const originalFetch = globalThis.fetch;
  let request = null;
  globalThis.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ phase: "ingress_pending", ingress_failure_count: 0 }),
    };
  };
  try {
    await AudioIngressAPI("http://audio").retryIngress("piece", { expected_version: 8 });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(request.url, "http://audio/api/v1/audio-ingress/pieces/piece/retry-ingress");
  assert.equal(request.options.method, "POST");
  assert.match(request.options.body, /"expected_version":8/);
});

test("audio ingress UI exposes an always-visible terminal retry and durable retry scheduling", async () => {
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  const render = await readFile(new URL("../public/js/render.js", import.meta.url), "utf8");
  assert.match(app, /audioIngress\.retryIngress\(piece\.id/);
  assert.match(app, /onRetryIngress: retryAudioIngressRecording/);
  assert.match(app, /state: freshIngressState\(piece\.state\)/);
  assert.match(app, /kickHistoryIngress\(\)/);
  assert.match(render, /piece\.phase === "ingress_failed"/);
  assert.match(render, /const retryPanel = element\("section", "audio-retry-panel"\)/);
  assert.match(render, /container\.append\(retryPanel\)/);
  assert.match(render, /Retry Kennedy ingress/);
  assert.ok(
    render.indexOf("container.append(retryPanel)") < render.indexOf('const pieces = element("section", "audio-history-section")'),
    "the retry panel should render before the collapsed transcript-piece disclosures",
  );
  assert.doesNotMatch(render, /disclosure\.append\(retry\)/);
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

test("history ingress starts Kennedy and memory tool details collapsed", () => {
  assert.deepEqual(ingressEntryPresentation({
    role: "assistant",
    content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}}]}',
  }), { collapsed: true, label: "Kennedy tool call" });
  assert.deepEqual(ingressEntryPresentation({
    role: "user", display_role: "Memory tool result", content: "Loaded.",
  }), { collapsed: true, label: "Memory tool result" });
  assert.deepEqual(ingressEntryPresentation({
    role: "assistant", content: "Memory review complete.",
  }), { collapsed: false, label: "Kennedy" });
});

test("Full History treats conversation provenance as a collapsed disclosure entry", () => {
  const entries = mainViewEntries({ chatend: [
    { role: "user", display_role: "Conversation provenance", context_kind: "provenance", content: "Conversation provenance\n\nLarge archived source." },
  ] });
  assert.equal(entries[1].kind, "provenance");
  assert.equal(entries[1].label, "Conversation provenance");
  assert.match(entries[1].content, /Large archived source/);

  const legacy = mainViewEntries({ chatend: [
    { role: "user", content: "Conversation provenance\n\nLegacy archived source." },
  ] });
  assert.equal(legacy[1].kind, "provenance");
});

test("Telegram documents are acknowledged on extraction failure so the queue can advance", async () => {
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  assert.match(app, /else if \(event\.kind === "document"\)/);
  assert.match(app, /document = await telegramDocumentInput\(event\);\s*\} catch \(error\) \{\s*await telegramRelay\.reply\(/s);
  assert.match(app, /if \(document\) await session\.send\(document\.text, document\.metadata\)/);
  assert.match(app, /Please try sending it again\./);
});

test("conversation checkpoints the pending query before any model request", async () => {
  const events = [];
  const metadata = [];
  const timings = [];
  const kweb = new MockKweb([node(1)]);
  const intelligence = {
    generate: async () => {
      events.push("generate");
      return { status: "complete", response_id: "response", message: { role: "assistant", content: "Saved answer." }, usage: null };
    },
    recordTiming: timing => timings.push(timing),
  };
  const session = new ConversationSession({
    kweb, intelligence, manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m",
    persist: async (state, details) => { events.push(state.pendingTurn ? "checkpoint-pending" : "checkpoint-complete"); metadata.push(details); }, onUpdate: () => {},
  });
  await session.initialize();
  await session.send("Saved question");
  assert.deepEqual(events, ["checkpoint-pending", "generate", "checkpoint-complete"]);
  assert.deepEqual(metadata, [{ userActivity: true }, {}]);
  assert.deepEqual(session.transcript.map(item => item.content), ["Saved question", "Saved answer."]);
  const turn = timings.find(timing => timing.action === "turn");
  assert.equal(turn.status, "ok");
  assert.equal(turn.sessionType, "conversation");
  assert.equal(Number.isInteger(turn.durationMs), true);
  assert.equal(turn.stepCount, 3);
  const summary = session.chatend.messages.find(message => message.display_role === "Latency summary");
  assert.match(summary.content, /^Turn latency: \d+ ms total · \d+ ms in LLM\/tools$/);
  assert.equal(summary.content.includes("\n"), false);
});

test("a final user message is durable without starting a Kennedy turn", async () => {
  let generations = 0;
  const checkpoints = [];
  const session = new ConversationSession({
    kweb: new MockKweb([node(1)]),
    intelligence: { generate: async () => { generations += 1; throw new Error("must not generate"); } },
    manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m",
    persist: async (state, metadata) => checkpoints.push({ state: structuredClone(state), metadata }),
    onUpdate: () => {},
  });
  await session.initialize();
  assert.equal(await session.appendFinalUserMessage("One last thing for memory."), true);
  assert.equal(generations, 0);
  assert.equal(session.pendingTurn, false);
  assert.equal(session.transcript.at(-1).content, "One last thing for memory.");
  assert.equal(checkpoints.at(-1).state.pendingTurn, false);
  assert.equal(checkpoints.at(-1).metadata.userActivity, true);
  assert.equal(checkpoints.at(-1).state.archive.messages.at(-1).content, "One last thing for memory.");
});

test("an in-flight conversation can be stopped and remains explicitly retryable", async () => {
  const kweb = new MockKweb([node(1)]);
  let generateMode = "wait";
  let generateStarted;
  let releaseGenerateStarted;
  let seenOptions = null;
  let cancelledOperationId = null;
  generateStarted = new Promise(resolve => { releaseGenerateStarted = resolve; });
  const intelligence = {
    generate: async (_request, options) => {
      seenOptions = options;
      if (generateMode === "answer") {
        return { status: "complete", response_id: "response", message: { role: "assistant", content: "Recovered." }, usage: null };
      }
      releaseGenerateStarted();
      return new Promise((_resolve, reject) => {
        options.signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
      });
    },
    cancelOperation: async operationId => { cancelledOperationId = operationId; return { cancelled: true }; },
  };
  const session = new ConversationSession({
    kweb, intelligence, manuals: promptManuals("Shared"),
    rootNodeId: id(1), provider: "p", model: "m", persist: async () => {}, onUpdate: () => {},
  });
  await session.initialize();
  const response = session.send("Stop this loop");
  await generateStarted;
  assert.equal(session.busy, true);
  assert.equal(session.canStop, true);
  await session.stopPendingTurn();
  await assert.rejects(response, error => error.code === "turn_stopped");
  assert.equal(seenOptions.signal.aborted, true);
  assert.equal(cancelledOperationId, seenOptions.operationId);
  assert.equal(session.busy, false);
  assert.equal(session.pendingTurn, true);
  assert.deepEqual(session.transcript.map(item => item.content), ["Stop this loop"]);

  generateMode = "answer";
  await session.resumePendingTurn();
  assert.equal(session.pendingTurn, false);
  assert.deepEqual(session.transcript.map(item => item.content), ["Stop this loop", "Recovered."]);
});

test("document attachments become model-readable text without duplicating extraction in media", async () => {
  const kweb = new MockKweb([node(1)]);
  const intelligence = { generate: async () => ({
    status: "complete", response_id: "document-response",
    message: { role: "assistant", content: "I read the report." }, usage: null,
  }) };
  const session = new ConversationSession({
    kweb, intelligence, manuals: promptManuals("Shared"),
    rootNodeId: id(1), provider: "p", model: "m", onUpdate: () => {},
  });
  await session.initialize();
  await session.send("", { attachments: [{
    id: "document-1", kind: "document", fileName: "report.pdf", mimeType: "application/pdf",
    sizeBytes: 123, dataUrl: "data:application/pdf;base64,AAAA", format: "pdf",
    text: "Quarterly revenue increased.", characters: 28, truncated: false, extractionDurationMs: 17,
  }] });
  assert.equal(session.transcript[0].content, "Attached report.pdf.");
  assert.equal(session.transcript[0].inputKind, "document");
  assert.equal(session.transcript[0].attachments[0].fileName, "report.pdf");
  const attachmentMessage = session.chatend.messages.find(message => message.role === "user" && message.content.includes("Quarterly revenue"));
  assert.match(attachmentMessage.content, /Attachment 1: report\.pdf/);
  assert.match(attachmentMessage.content, /Latency: document extraction 17 ms/);
  assert.equal(session.media[0].dataUrl, "data:application/pdf;base64,AAAA");
  assert.equal("text" in session.media[0], false);
  assert.equal("extractionDurationMs" in session.media[0], false);
  session.chatend.rebuild();
  assert.equal(session.chatend.messages.some(message => message.content.includes("Quarterly revenue increased.")), true);
});

test("restored pending conversation resumes from durable transcript and context", async () => {
  const kweb = new MockKweb([node(1), node(2)]);
  let generated = 0;
  const session = new ConversationSession({
    kweb,
    intelligence: { generate: async () => { generated += 1; return { status: "complete", response_id: "response", message: { role: "assistant", content: "Recovered answer." }, usage: null }; } },
    manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m", persist: async () => {}, onUpdate: () => {},
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
    manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m",
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

test("ending a conversation keeps its ingress record selected until New is explicit", async () => {
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  const closeConversation = app.match(/async function closeConversation\(id, session, record\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(closeConversation);
  assert.match(closeConversation, /selectedConversationId = id;/);
  assert.match(closeConversation, /selectedByView\.conversation = id;/);
  assert.match(closeConversation, /kickHistoryIngress\(\);/);
  assert.doesNotMatch(closeConversation, /historyRecords\.find\(item => item\.phase === "active"/);
  assert.doesNotMatch(closeConversation, /createNewConversation\(\)/);
});

test("ending self time archives it without waking history ingress", async () => {
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  const closeSelfTime = app.match(/async function closeFreeTimeSession\(id, session\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(closeSelfTime);
  assert.match(closeSelfTime, /conversationHistory\.completeWithoutIngress\(id,/);
  assert.doesNotMatch(closeSelfTime, /requestIngress|kickHistoryIngress/);
});

test("a structured pending Chatend resumes from cold start without duplicating its user query", async () => {
  const kweb = new MockKweb([node(1)]);
  let saved;
  const interrupted = new ConversationSession({
    kweb,
    intelligence: { generate: async () => { throw new Error("connection lost"); } },
    manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m",
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
    manuals: promptManuals("Changed"), rootNodeId: id(1), provider: "p", model: "m",
    persist: async () => {}, onUpdate: () => {},
  });
  await restored.initialize(saved);
  await restored.resumePendingTurn();
  assert.equal(requests.length, 1);
  assert.equal(requests[0].previous_response_id, null);
  assert.equal(requests[0].chatend.match(/Cold-start query/g)?.length, 1);
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
    manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m",
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
    manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m",
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
    manuals: promptManuals("Shared"), rootNodeId: id(1), provider: "p", model: "m",
    persist: async state => { events.push(state.pendingTurn ? "persist-pending" : "persist-complete"); if (fail) { fail = false; throw new Error("history unavailable"); } }, onUpdate: () => {},
  });
  await session.initialize();
  await assert.rejects(() => session.send("Question"), /history unavailable/);
  assert.deepEqual(events, ["persist-pending"]);
  await session.resumePendingTurn();
  assert.deepEqual(events, ["persist-pending", "persist-pending", "generate", "persist-complete"]);
});

test("Kmap context uses compact role-based node and fanout representations", () => {
  const formatted = formatKmapContext({
    rootIdentifiers: [1],
    directlyLoadedIdentifiers: [1],
    nodes: [
      {
        identifier: 1, shortName: "Direct Node", shortDescription: "Direct summary", longDescription: "Direct details", lastModifiedBy: "model-a",
        ownerIdentifier: 1,
        fixedConnections: [{ identifier: 4, shortName: "Pinned Node", shortDescription: "Pinned node summary", slot: 1 }],
        activeConnections: [{ identifier: 2, shortName: "Active Node", shortDescription: "Active summary" }],
        fanoutConnections: [{ identifier: 3, shortName: "Direct Fanout", shortDescription: "Direct fanout summary" }],
      },
      {
        identifier: 2, shortName: "Active Node", shortDescription: "ACTIVE SHORT DESCRIPTION MUST BE OMITTED", longDescription: "Active details", lastModifiedBy: "model-b",
        ownerIdentifier: 1,
        fixedConnections: [{ identifier: 7, shortName: "Nested Fixed", shortDescription: "Nested fixed summary", slot: 3 }],
        activeConnections: [{ identifier: 5, shortName: "Nested Active", shortDescription: "Nested active summary" }],
        fanoutConnections: [
          { identifier: 3, shortName: "Direct Fanout", shortDescription: "Direct fanout summary" },
          { identifier: 6, shortName: "Indirect Fanout", shortDescription: "INDIRECT SHORT DESCRIPTION MUST BE OMITTED" },
        ],
      },
    ],
  });
  assert.match(formatted, /Current Kmap context/);
  assert.match(formatted, /Directly loaded nodes[\s\S]*Node 1: Direct Node[\s\S]*Summary: Direct summary[\s\S]*Details:\n  Direct details/);
  assert.match(formatted, /Fixed connection identifiers: slot 1: 4/);
  assert.match(formatted, /Active connection identifiers: 2/);
  assert.match(formatted, /Fanout connection identifiers: 3/);
  assert.match(formatted, /Full active-connection nodes[\s\S]*Node 2: Active Node[\s\S]*Details:\n  Active details/);
  assert.doesNotMatch(formatted, /ACTIVE SHORT DESCRIPTION MUST BE OMITTED|Nested Fixed|Nested Active/);
  assert.match(formatted, /Fanout nodes of directly loaded nodes[\s\S]*3: Direct Fanout[\s\S]*Summary: Direct fanout summary/);
  assert.match(formatted, /Fanout nodes only of active-connection nodes[\s\S]*6: Indirect Fanout/);
  assert.doesNotMatch(formatted, /INDIRECT SHORT DESCRIPTION MUST BE OMITTED/);
  assert.equal((formatted.match(/3: Direct Fanout/g) || []).length, 1);
  assert.equal(formatted.includes('{'), false);
});

test("compact Kmap projection materially reduces dense loaded-node context", () => {
  const connection = (identifier, prefix) => ({
    identifier,
    shortName: `${prefix} ${identifier}`,
    shortDescription: "Connection summary ".repeat(8),
  });
  const denseNode = identifier => ({
    identifier,
    shortName: `Dense Node ${identifier}`,
    shortDescription: "Node summary ".repeat(10),
    longDescription: "Durable details ".repeat(80),
    lastModifiedBy: "model-x",
    fixedConnections: [],
    activeConnections: Array.from({ length: 8 }, (_, index) => connection(10_000 + identifier * 10 + index, "Active")),
    fanoutConnections: Array.from({ length: 64 }, (_, index) => connection(20_000 + identifier * 100 + index, "Fanout")),
  });
  const nodes = Array.from({ length: 9 }, (_, index) => denseNode(index + 1));
  const legacyCharacters = nodes.map(formatContextNode).join("\n\n").length;
  const compactCharacters = formatKmapContext({ rootIdentifiers: [1], directlyLoadedIdentifiers: [1], nodes }).length;
  assert.ok(compactCharacters < legacyCharacters / 2, `${compactCharacters} compact characters versus ${legacyCharacters} legacy characters`);
});

test("system prompt composition uses readable sections rather than markup wrappers", () => {
  const manuals = {
    identity: "Identity.",
    conversationSession: "Conversation session.",
    freeTimeSession: "Self time. Have fun and use EndSelfTimeSession for a clean slate.",
    historyIngressSession: "History session.",
    audioIngressSession: "Audio session.",
    codexHarness: "The outer harness catches Kennedy tool calls.",
    kmapBasics: "Kmap basics.",
    readTools: "Kmap read tools.\n\nWeb tools.",
    writeTools: "Write tools.",
  };
  const prompt = composePrompt(manuals, "conversation", { model: "gpt-5.6-sol", reasoningEffort: "xhigh" });
  assert.equal(prompt, "Kennedy's identity\n\nIdentity.\n\nSession type\n\nConversation session.\n\nChannel: Kennedy's browser UI.\n\nKmap basics\n\nKmap basics.\n\nRead-only tools\n\nKmap read tools.\n\nWeb tools.\n\nCurrent runtime\n\nYou are currently running on gpt-5.6-sol with xhigh thinking mode.");
  assert.match(composePrompt(manuals, "conversation", { sessionType: "telegram" }), /Channel: Telegram/);
  const freeTime = composePrompt(manuals, "conversation", {
    sessionType: "free-time",
    sessionContext: "The shared deadline is 2026-07-17T12:30:00Z.",
  });
  assert.match(freeTime, /Self time\. Have fun/);
  assert.match(freeTime, /Write tools\n\nWrite tools\./);
  assert.match(freeTime, /Self-time schedule\n\nThe shared deadline/);
  const history = composePrompt(manuals, "ingress", { sourceSessionType: "telegram" });
  assert.match(history, /Source: an archived Telegram conversation/);
  assert.match(history, /Write tools\n\nWrite tools\./);
  assert.match(history, /Web tools\./);
  const audio = composePrompt(manuals, "ingress", { sourceSessionType: "audio" });
  assert.match(audio, /Session type\n\nAudio session\./);
  assert.doesNotMatch(audio, /History session\./);
  assert.ok(audio.indexOf("Kennedy's identity") < audio.indexOf("Session type"));
  assert.ok(audio.indexOf("Session type") < audio.indexOf("Kmap basics"));
  assert.ok(audio.indexOf("Kmap basics") < audio.indexOf("Read-only tools"));
  assert.ok(audio.indexOf("Read-only tools") < audio.indexOf("Write tools"));
  assert.equal(formatModelAttribution("gpt-5.6-sol", "xhigh"), "gpt-5.6-sol-xhigh");
  const codex = composePrompt(manuals, "conversation", { providerKind: "codex" });
  assert.match(codex, /Codex harness\n\nThe outer harness catches Kennedy tool calls\./);
  assert.doesNotMatch(composePrompt(manuals, "conversation", { providerKind: "direct-api" }), /Codex harness|outer harness/);
  assert.equal(prompt.includes("<kennedy_"), false);
  assert.deepEqual(requiredPromptKeys("conversation"), ["identity", "conversationSession", "kmapBasics", "readTools"]);
  assert.deepEqual(requiredPromptKeys("conversation", { providerKind: "codex" }), ["identity", "conversationSession", "kmapBasics", "readTools", "codexHarness"]);
  assert.deepEqual(requiredPromptKeys("conversation", { sessionType: "free-time" }), ["identity", "freeTimeSession", "kmapBasics", "readTools", "writeTools"]);
  assert.deepEqual(requiredPromptKeys("ingress"), ["identity", "historyIngressSession", "kmapBasics", "readTools", "writeTools"]);
  assert.equal(promptsReady(manuals, "ingress", { sourceSessionType: "audio" }), true);
  assert.equal(promptsReady({ ...manuals, codexHarness: "" }, "conversation", { providerKind: "codex" }), false);
  assert.equal(promptsReady({ ...manuals, codexHarness: "" }, "conversation", { providerKind: "direct-api" }), true);
  assert.throws(() => composePrompt({ ...manuals, readTools: "" }, "conversation"), /Missing system prompt sections: readTools/);
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

test("system prompt loader requests every composable prompt layer", async () => {
  const originalFetch = globalThis.fetch;
  const requested = [];
  globalThis.fetch = async path => {
    requested.push(path);
    return { ok: true, text: async () => path };
  };
  let loaded;
  try {
    loaded = await loadPromptManuals("/base");
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.deepEqual(requested.sort(), [
    "/base/system-prompts/AudioIngressSession.txt",
    "/base/system-prompts/CodexHarness.txt",
    "/base/system-prompts/ConversationSession.txt",
    "/base/system-prompts/HistoryIngressSession.txt",
    "/base/system-prompts/KennedyIdentity.txt",
    "/base/system-prompts/KmapBasics.txt",
    "/base/system-prompts/ReadTools.txt",
    "/base/system-prompts/SelfTimeSession.txt",
    "/base/system-prompts/WriteTools.txt",
  ]);
  assert.equal(loaded.errors.audioIngressSession, undefined);
  assert.equal(loaded.manuals.audioIngressSession, "/base/system-prompts/AudioIngressSession.txt");
  assert.equal(loaded.manuals.codexHarness, "/base/system-prompts/CodexHarness.txt");
});

test("a missing audio prompt disables only audio memory ingress", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async path => ({
    ok: !String(path).endsWith("/AudioIngressSession.txt"),
    text: async () => `Loaded ${path}`,
  });
  let loaded;
  try {
    loaded = await loadPromptManuals("/base");
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.match(loaded.errors.audioIngressSession, /Could not load system prompt AudioIngressSession\.txt/);
  assert.match(loaded.manuals.identity, /KennedyIdentity\.txt/);
  assert.match(loaded.manuals.conversationSession, /ConversationSession\.txt/);
  assert.match(loaded.manuals.historyIngressSession, /HistoryIngressSession\.txt/);
  assert.equal(promptsReady(loaded.manuals, "ingress", { sourceSessionType: "conversation" }), true);
  assert.equal(promptsReady(loaded.manuals, "ingress", { sourceSessionType: "audio" }), false);
});

test("shared Kmap basics enforce exclusive tool-request responses", async () => {
  const basics = await readFile(new URL("../SystemPrompts/KmapBasics.txt", import.meta.url), "utf8");
  assert.match(basics, /harness truncates all of them without reading or executing them/);
  assert.match(basics, /only the first object's calls are considered/);
  assert.match(basics, /Do not use a Markdown fence/);
  assert.match(basics, /put text before the marker/);
  assert.match(basics, /Additional tools and their documentation may be available in the kmap/);
});

test("layered prompt assets separate session, shared read, web, and write contracts", async () => {
  const identity = await readFile(new URL("../SystemPrompts/KennedyIdentity.txt", import.meta.url), "utf8");
  assert.match(identity, /make liberal use of the kmap/i);
  const conversation = await readFile(new URL("../SystemPrompts/ConversationSession.txt", import.meta.url), "utf8");
  const history = await readFile(new URL("../SystemPrompts/HistoryIngressSession.txt", import.meta.url), "utf8");
  const audio = await readFile(new URL("../SystemPrompts/AudioIngressSession.txt", import.meta.url), "utf8");
  const codexHarness = await readFile(new URL("../SystemPrompts/CodexHarness.txt", import.meta.url), "utf8");
  const readTools = await readFile(new URL("../SystemPrompts/ReadTools.txt", import.meta.url), "utf8");
  const writeTools = await readFile(new URL("../SystemPrompts/WriteTools.txt", import.meta.url), "utf8");
  assert.match(conversation, /kmap is read-only in this session/i);
  assert.match(history, /kmap is writable in this session/i);
  assert.match(audio, /kmap is writable in this session/i);
  assert.match(codexHarness, /outer harness is calling you through Codex/i);
  assert.match(codexHarness, /APIs or tools are limited/i);
  assert.match(codexHarness, /outer harness will catch them/i);
  for (const session of [conversation, history, audio]) {
    assert.doesNotMatch(session, /KENNEDY_TOOL_CALLS|LoadNode\n  Call:/);
  }
  assert.match(readTools, /At most ten nodes may be directly loaded at once, including every always-loaded root/);
  assert.match(readTools, /WebSearch\n  Call:/);
  assert.match(readTools, /WebFetch\n  Call:/);
  assert.match(writeTools, /ConsolidateFanout\n  Call:/);
  assert.match(writeTools, /SetFixedConnection\n  Call:/);
  assert.match(writeTools, /Use the string "blank" as childIdentifier/);
  assert.doesNotMatch(writeTools, /WebSearch\n  Call:|WebFetch\n  Call:/);
  for (const manual of [conversation, history, audio, readTools, writeTools]) {
    assert.doesNotMatch(manual, /knowledge-hungry|use a promising|navigate enough|prefer to|consider assigning/i);
  }
});

test("background rerenders preserve the current view and follow only readers already at the bottom", async () => {
  const render = await readFile(new URL("../public/js/render.js", import.meta.url), "utf8");
  assert.match(render, /sameView = container\.dataset\.renderKey === viewKey/);
  assert.match(render, /wasAtBottom: container\.scrollHeight - container\.clientHeight - container\.scrollTop <= 1/);
  assert.match(render, /details\[open\]\[data-view-key\]/);
  assert.match(render, /details\.open = state\.openKeys\.has\(details\.dataset\.viewKey\)/);
  assert.match(render, /nestedScroll: sameView/);
  assert.match(render, /node\.scrollTop = saved\.wasAtBottom \? node\.scrollHeight : saved\.top/);
  assert.match(render, /container\.scrollTop = state\.wasAtBottom \? container\.scrollHeight : state\.previousTop/);
  assert.match(render, /focus\?\.\(\{ preventScroll: true \}\)/);
  assert.match(render, /log\.scrollTop = wasAtBottom \? log\.scrollHeight : previousTop/);
});

test("history ingress is an inline continuation with no independent scroller", async () => {
  const render = await readFile(new URL("../public/js/render.js", import.meta.url), "utf8");
  const styles = await readFile(new URL("../public/css/styles.css", import.meta.url), "utf8");
  const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
  assert.match(render, /renderTranscript\(container, transcript, ingressActivity = null, viewKey = "transcript", retryAction = null\)/);
  assert.match(render, /renderIngressActivity\([\s\S]*?ingressActivity\.diagnostic,[\s\S]*?\{ namespace: `\$\{viewKey\}:history-ingress` \},[\s\S]*?restoreViewState\(container, viewKey, viewState\)/);
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
    { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"WebSearch","arguments":{"question":"current evidence","mode":"fast"}}]}' },
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

test("main inspector keeps conversation visible and turns context activity into disclosure entries", () => {
  const direct = {
    identifier: 3, shortName: "Project", shortDescription: "Project summary", longDescription: "Project details", lastModifiedBy: "model-thinking",
    fixedConnections: [], activeConnections: [{ identifier: 4, shortName: "Related", shortDescription: "Related summary" }], fanoutConnections: [],
  };
  const active = {
    identifier: 4, shortName: "Related", shortDescription: "Related summary", longDescription: "Related details", lastModifiedBy: "model-thinking",
    fixedConnections: [], activeConnections: [{ identifier: 5, shortName: "Distant", shortDescription: "Summary only" }], fanoutConnections: [],
  };
  const diagnostic = {
    chatend: [
      { role: "system", context_kind: "instructions", content: "Agent manuals." },
      { role: "system", context_kind: "memory", content: "Formatted Kmap." },
      { role: "user", content: "What changed?" },
      { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":3}}]}' },
      { role: "system", display_role: "Latency", context_kind: "timing", content: "Latency: LLM call 120 ms" },
      {
        role: "user", display_role: "Memory tool result", tool_name: "LoadNode",
        tool_result: { ok: true, result: { requestedNode: direct, activeConnectionNodes: [active] } },
        content: "Kennedy tool result · LoadNode · 8 ms\n\nMemory load completed.",
      },
      { role: "assistant", content: "Here is the answer." },
      { role: "system", display_role: "Latency", context_kind: "timing", content: "Latency: LLM call 90 ms" },
      { role: "system", display_role: "Latency summary", context_kind: "timing", content: "Turn latency: 240 ms total · 218 ms in LLM/tools" },
    ],
    memory: { directlyLoadedIdentifiers: [3], nodes: [{ ...direct, contextSources: ["direct"] }, { ...active, contextSources: ["active"] }] },
  };
  const entries = mainViewEntries(diagnostic);
  assert.deepEqual(entries.map(entry => entry.kind), ["context", "memory", "conversation", "tool-call", "loaded-node", "conversation"]);
  assert.equal(entries[0].label, "System prompt");
  assert.equal(entries[2].content, "What changed?");
  assert.equal(entries[3].label, "Tool call · LoadNode");
  assert.deepEqual(entries.filter(entry => entry.kind === "loaded-node").map(entry => [entry.relation, entry.node.shortName]), [["direct", "Project"]]);
  assert.equal(entries.find(entry => entry.kind === "loaded-node").node.activeConnections[0].shortName, "Related");
  assert.deepEqual(entries.find(entry => entry.kind === "loaded-node").timing, ["Latency: LLM call 120 ms", "LoadNode 8 ms"]);
  assert.equal(entries.at(-1).content, "Here is the answer.");
  assert.deepEqual(entries.at(-1).timing, ["Latency: LLM call 90 ms", "Turn latency: 240 ms total · 218 ms in LLM/tools"]);
  assert.equal(entries.some(entry => entry.label === "Latency" || entry.label === "Latency summary"), false);
  assert.equal(inspectorText(diagnostic, "main"), inspectorText(diagnostic, "full"));
});

test("main inspector truncates only Kennedy responses longer than 500 characters", () => {
  const long = `${"x".repeat(500)}YZ`;
  const entries = mainViewEntries({ chatend: [
    { role: "user", content: long },
    { role: "assistant", content: "y".repeat(500) },
    { role: "assistant", content: long },
  ] });
  const conversations = entries.filter(entry => entry.kind === "conversation");
  assert.equal(conversations[0].preview, null);
  assert.equal(conversations[1].preview, null);
  assert.equal([...conversations[2].preview].length, 500);
  assert.equal(conversations[2].preview, "x".repeat(500));
  assert.equal(conversations[2].content, long);
  assert.equal(conversations[2].hiddenCharacters, 2);
});

test("collapsed inspector entries expose exact character-size metadata", async () => {
  const [render, styles] = await Promise.all([
    readFile(new URL("../public/js/render.js", import.meta.url), "utf8"),
    readFile(new URL("../public/css/styles.css", import.meta.url), "utf8"),
  ]);
  assert.match(render, /characterLabel\(entry\.hiddenCharacters\)/);
  assert.match(render, /disclosureSummary\([\s\S]*?characterCount\(entry\.content\)/);
  assert.match(render, /"ingress-entry-size", characterLabel\(message\.content\)/);
  assert.match(render, /"audio-disclosure-size", characterLabel\(content\)/);
  assert.match(styles, /\.ingress-entry-size \{/);
});

test("main inspector moves tool latency from the result header to result footer metadata", () => {
  const entries = mainViewEntries({ chatend: [
    { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"WebSearch","arguments":{"question":"test","mode":"fast"}}]}' },
    { role: "system", display_role: "Latency", context_kind: "timing", content: "Latency: LLM call 1.200 s" },
    { role: "user", display_role: "Web tool result", tool_name: "WebSearch", tool_result: { ok: true }, content: "Kennedy tool result · WebSearch · 2.500 s\n\nWeb research completed." },
  ] });
  assert.deepEqual(entries.map(entry => entry.kind), ["memory", "tool-call", "tool-result"]);
  assert.equal(entries.at(-1).content, "Web research completed.");
  assert.deepEqual(entries.at(-1).timing, ["Latency: LLM call 1.200 s", "WebSearch 2.500 s"]);
});

test("full history preserves context segments and reset barriers in order", () => {
  const diagnostic = {
    mode: "history ingress",
    fullHistory: { phases: [
      {
        label: "Conversation", status: "closed",
        segments: [{ reason: "ResetContext", messages: [{ role: "assistant", content: "Before conversation reset." }], memory: { directlyLoadedIdentifiers: [], nodes: [] }, usage: null }],
        current: { messages: [{ role: "assistant", content: "Final conversation context." }], memory: { directlyLoadedIdentifiers: [], nodes: [] }, usage: null },
      },
      {
        label: "History ingress", status: "complete",
        segments: [{ reason: "ResetContext", messages: [{ role: "assistant", content: "Before ingress reset." }], memory: { directlyLoadedIdentifiers: [], nodes: [] }, usage: null }],
        current: { messages: [{ role: "assistant", content: "Final ingress context." }], memory: { directlyLoadedIdentifiers: [], nodes: [] }, usage: null },
      },
    ] },
  };
  const text = inspectorText(diagnostic, "history");
  for (const value of ["Before conversation reset.", "Final conversation context.", "History ingress began", "Before ingress reset.", "Final ingress context."]) assert.match(text, new RegExp(value));
  assert.equal((text.match(/ResetContext · context reset/g) || []).length, 2);
  assert.ok(text.indexOf("Before conversation reset.") < text.indexOf("Final conversation context."));
  assert.ok(text.indexOf("Final conversation context.") < text.indexOf("History ingress began"));
  assert.ok(text.indexOf("Before ingress reset.") < text.indexOf("Final ingress context."));
});

test("production frontend never uses HTML string insertion", async () => {
  const files = ["render.js", "memory_explorer.js", "app.js"];
  for (const file of files) {
    const source = await readFile(new URL(`../public/js/${file}`, import.meta.url), "utf8");
    assert.equal(source.includes("innerHTML"), false, `${file} uses innerHTML`);
  }
});

test("user-visible errors are logged below history instead of inside the chat panel", async () => {
  const [html, render] = await Promise.all([
    readFile(new URL("../public/index.html", import.meta.url), "utf8"),
    readFile(new URL("../public/js/render.js", import.meta.url), "utf8"),
  ]);
  const historyPanel = html.match(/<aside class="panel history-panel"[\s\S]*?<\/aside>/)?.[0];
  const conversationPanel = html.match(/<section class="panel conversation-panel"[\s\S]*?<\/section>/)?.[0];
  assert.ok(historyPanel);
  assert.ok(conversationPanel);
  assert.match(historyPanel, /id="conversation-history"[\s\S]*id="user-log-section"[\s\S]*id="error-banner"/);
  assert.doesNotMatch(conversationPanel, /id="error-banner"/);
  assert.match(render, /log\.append\(entry\)/);
  assert.match(render, /previous\?\.dataset\.message === text/);
});

test("frontend initialization and ingress queues degrade by feature", async () => {
  const [app, coordinator] = await Promise.all([
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/memory_ingress_coordinator.js", import.meta.url), "utf8"),
  ]);
  assert.doesNotMatch(app, /Promise\.all\(\[kweb\.health\(\), kweb\.user\(\), loadPromptManuals/);
  assert.match(app, /audioIngressReady && audioPromptsReady\(\)/);
  assert.match(coordinator, /conversationHistory\.nextIngress\(\)\.catch/);
  assert.match(coordinator, /audioIngress\.nextIngress\(\)\.catch/);
  assert.match(app, /Audio preparation and history remain available, but audio memory ingress is paused/);
  assert.match(app, /providerKind = selected\.kind/);
  assert.match(app, /promptsReady\(manuals, "conversation", \{ providerKind \}\)/);
  assert.match(app, /provider, providerKind, model, reasoningEffort/);
});

test("frontend defaults to main with full and full-history inspectors and retains both Kmap root controls", async () => {
  const [html, app] = await Promise.all([
    readFile(new URL("../public/index.html", import.meta.url), "utf8"),
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
  ]);
  for (const id of ["usage-metrics", "inspector-main", "inspector-full", "inspector-history", "memory-home", "memory-kennedy-home", "new-conversation", "conversation-history", "user-log-section", "clear-log", "tg-tab", "audio-tab", "voice-button", "stop-button", "send-end-button"]) assert.match(html, new RegExp(`id="${id}"`));
  for (const id of ["inspector-system", "inspector-tools", "inspector-memory"]) assert.doesNotMatch(html, new RegExp(`id="${id}"`));
  assert.match(app, /const INSPECTOR_MODES = \["main", "full", "history"\]/);
  assert.match(app, /let inspectorMode = "main"/);
  assert.match(app, /record\?\.state\?\.historyIngress/);
  assert.match(app, /mode: "history ingress"/);
  assert.match(app, /new MemoryExplorer\(\{ api: kweb, rootNodeIds,/);
  assert.match(app, /memory_kennedy_home\.addEventListener\("click", \(\) => explorer\?\.kennedyHome\(\)\)/);
  const initialize = app.match(/async function initialize\(\) \{[\s\S]*?\n\}\n\nui\.message_form/)?.[0];
  assert.ok(initialize);
  assert.ok(initialize.indexOf("await conversationHistory.discardUnstarted()") < initialize.indexOf("conversationHistory.list()"));
  assert.match(app, /renderAudioHistory\(ui\.conversation_history, audioRecords/);
  assert.match(app, /const detail = selectedAudioDetail\(\)/);
  assert.match(app, /renderAudioRecording\(ui\.transcript, detail/);
  assert.match(app, /session\.stopPendingTurn\(\)/);
  assert.match(app, /stop_button\.addEventListener\("click"/);
  assert.match(app, /send_end_button\.addEventListener\("click", \(\) => sendAndEndConversation\(\)\)/);
  assert.match(app, /session\.appendFinalUserMessage\(text, metadata\)/);
  assert.match(html, /id="stop-button"[^>]*>Stop Kennedy<\/button>/);
  assert.match(html, /id="send-end-button"[^>]*>Send &amp; end<\/button>/);
  assert.match(html, /\/js\/app\.js\?v=\d{8}\.\d+/);
  assert.match(html, /\/css\/styles\.css\?v=\d{8}\.\d+/);
});

test("the browser exposes self time in a dedicated tab with a durable custom prompt", async () => {
  const [html, app, manual] = await Promise.all([
    readFile(new URL("../public/index.html", import.meta.url), "utf8"),
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
    readFile(new URL("../SystemPrompts/SelfTimeSession.txt", import.meta.url), "utf8"),
  ]);
  assert.match(html, /id="self-time-tab"[^>]*>Self Time<\/button>/);
  assert.match(html, /id="self-time-panel"/);
  assert.match(html, /id="self-time-prompt"[^>]*maxlength="20000"/);
  assert.match(html, /id="self-time-minutes"[^>]*min="0\.1"[^>]*max="10080"[^>]*value="30"/);
  assert.match(html, /id="start-self-time"[^>]*>Start self time<\/button>/);
  assert.match(app, /if \(sessionType === "free-time"\) return "self-time"/);
  assert.match(app, /customPrompt = parseSelfTimePrompt\(ui\.self_time_prompt\.value\)/);
  assert.match(app, /customPrompt,/);
  assert.match(app, /navigator\.locks\.request\("kennedy-free-time"/);
  assert.match(app, /if \(freeTimeStartPromise\) return freeTimeStartPromise/);
  assert.match(app, /freeTimeStarting = true;\s+update\(\);\s+const work = startFreeTimeCoordinated\(\)/);
  assert.match(app, /error\?\.code !== "free_time_already_active"/);
  assert.match(app, /FREE_TIME_HARD_STOP_GRACE_MS/);
  assert.match(app, /if \(freeTimeCanStartNewSession\(finalMetadata\) && !purgedConversationIds\.has\(id\)\)/);
  assert.match(app, /if \(!freeTimeCanStartNewSession\(freeTime\)\) \{\s+finishRun\(\)/);
  assert.match(app, /session\.stageFreeTimeOpening\(\)/);
  assert.match(app, /chatRuntimeReady\(\) \|\| freeTimeRuntimeReady\(\)/);
  assert.match(manual, /free to do whatever you want/i);
  assert.match(manual, /EndSelfTimeSession/);
  assert.match(manual, /\{\} or \{"message":"A message for the next self time session\."\}/);
  assert.match(manual, /allocating all remaining time/);
});

test("audio recording view starts large artifacts collapsed and includes inline history ingress", async () => {
  const [render, app] = await Promise.all([
    readFile(new URL("../public/js/render.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
  ]);
  assert.match(render, /audioDisclosure\(\s*"Final reconciled transcript",\s*finalTranscript,\s*`\$\{viewKey\}:final-transcript`,\s*\)/s);
  assert.doesNotMatch(render, /"Final reconciled transcript",\s*finalTranscript,\s*[^,]+,\s*true/s);
  assert.match(render, /`History ingress \(\$\{detail\.pieces\?\.length \|\| 0\}\)`/);
  assert.match(render, /sourceLabel: `Transcript piece \$\{piece\.piece_index \+ 1\}\/\$\{piece\.piece_count\}`/);
  assert.match(render, /renderIngressActivity\(\s*ingress,/s);
  assert.match(app, /ingressActivities: audioIngressActivities\(detail\)/);
  assert.match(app, /loading: audioDetailLoading\.has\(selectedAudioId\) && !detail/);
});

test("Telegram queue heads run independently without blocking later relay polls", async () => {
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  assert.match(app, /function launchTelegramEvent\(event\) \{[\s\S]*telegramInFlight\.add\(event\.id\);[\s\S]*void runTelegramEvent\(event\)/);
  assert.match(app, /for \(const event of events\) launchTelegramEvent\(event\);/);
  assert.doesNotMatch(app, /await Promise\.all\(events\.map/);
});

test("Telegram processing has a durable 30-minute deadline and orphan recovery path", async () => {
  const now = Date.parse("2026-07-17T19:00:00Z");
  assert.equal(TELEGRAM_RESPONSE_TIMEOUT_MS, 30 * 60 * 1000);
  assert.equal(telegramEventDeadlineMs({}, now), now + TELEGRAM_RESPONSE_TIMEOUT_MS);
  assert.equal(telegramEventTimeoutMs({ processingStartedAt: "2026-07-17T18:45:00Z" }, now), 15 * 60 * 1000);
  assert.equal(telegramEventTimeoutMs({ processingStartedAt: "2026-07-17T18:00:00Z" }, now), 0);

  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  assert.match(app, /event\.conversationId !== record\.id \|\| !event\.processingStartedAt/);
  assert.match(app, /telegramRelay\.bind\(event\.id, record\.id, event\.conversationId \|\| null\)/);
  assert.match(app, /runtime\.session\.stopPendingTurn\(\)/);
  assert.match(app, /telegramRelay\.abort\(event\.id, runtime\.conversationId, TELEGRAM_TIMEOUT_NOTICE\)/);
  assert.match(app, /closeTimedOutTelegramConversation\(runtime\)/);
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
    manuals: promptManuals("Shared"), rootNodeId: id(1),
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

test("Telegram group sessions load the invoker, group, and Kennedy roots while registering every other participant root", async () => {
  const groupContext = {
    groupTitle: "Trusted friends",
    chatId: -100,
    invokingTelegramUserId: 42,
    groupRootNodeId: id(2),
    groupRootReady: true,
    participants: [
      { telegramUserId: 42, username: "taek42", displayName: "David", rootNodeId: id(1) },
      { telegramUserId: 77, username: "friend", displayName: "Friend", rootNodeId: id(4) },
    ],
    messages: [
      { messageId: 9, telegramUserId: 77, username: "friend", displayName: "Friend", text: "Earlier context", sentByKennedy: false },
      { messageId: 10, telegramUserId: 42, username: "taek42", displayName: "David", text: "@kennedy thoughts?", sentByKennedy: false },
    ],
  };
  const session = new ConversationSession({
    kweb: new MockKweb([node(1), node(2), node(3), node(4)]), intelligence: {}, manuals: promptManuals("Shared"),
    rootNodeIds: [id(1), id(2), id(3)], referenceRootNodeIds: [id(4)],
    provider: "p", model: "m", sessionType: "telegram-group",
    channel: { kind: "telegram-group", telegramUserId: 42, chatId: -100, groupContext },
    onUpdate: () => {},
  });
  await session.initialize();
  assert.deepEqual(session.context.loadedNodeIds, [id(1), id(2), id(3)]);
  assert.equal(session.context.resolve(4), id(4));
  assert.match(session.chatend.systemPrompt, /persistent session scoped to one participant and one group/);
  assert.match(session.chatend.systemPrompt, /invoking participant's root \(1\), the group root \(2\), and Kennedy's root \(3\)/);
  assert.match(session.chatend.systemPrompt, /leaving room for 7 additional directly loaded nodes/);
  assert.match(session.chatend.systemPrompt, /Friend · @friend · Telegram user ID 77 · root node identifier 4/);
  assert.match(session.chatend.systemPrompt, /Telegram messages supplied as context \(2\)/);
  assert.deepEqual(session.archive().rootNodeIds, [id(1), id(2), id(3)]);
  assert.deepEqual(session.archive().referenceRootNodeIds, [id(4)]);
});

test("persistent Telegram group sessions append only unseen context and scope warnings to one user", async () => {
  const initialContext = {
    groupTitle: "Trusted friends", chatId: -100, invokingTelegramUserId: 42, groupRootNodeId: id(2),
    participants: [{ telegramUserId: 42, username: "taek42", displayName: "David", rootNodeId: id(1) }],
    messages: [{ messageId: 10, telegramUserId: 42, username: "taek42", displayName: "David", text: "Earlier invocation", sentByKennedy: false }],
  };
  const session = new ConversationSession({
    kweb: new MockKweb([node(1), node(2), node(3), node(4)]),
    intelligence: { generate: async () => ({
      status: "complete", response_id: "group-response",
      message: { role: "assistant", content: "Group answer." },
      usage: { input_tokens: 100001, output_tokens: 20, cached_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0 },
    }) },
    manuals: promptManuals("Shared"), rootNodeIds: [id(1), id(2), id(3)],
    provider: "p", model: "m", contextWindowTokens: 400000, sessionType: "telegram-group",
    channel: { kind: "telegram-group", telegramUserId: 42, username: "taek42", displayName: "David", chatId: -100, groupRootNodeId: id(2), groupContext: initialContext, lastGroupContextMessageId: 10 },
    persist: async () => {}, onUpdate: () => {},
  });
  await session.initialize();
  session.refreshTelegramGroupContext({
    ...initialContext,
    participants: [
      ...initialContext.participants,
      { telegramUserId: 77, username: "friend", displayName: "Friend", rootNodeId: id(4) },
    ],
    messages: [
      ...initialContext.messages,
      { messageId: 11, telegramUserId: 77, username: "friend", displayName: "Friend", text: "New group context", sentByKennedy: false },
      { messageId: 12, telegramUserId: 42, username: "taek42", displayName: "David", text: "Current invocation", sentByKennedy: false },
    ],
  }, 12);
  const contextUpdate = session.chatend.retained.find(message => message.content.includes("Updated Telegram group context"));
  assert.match(contextUpdate.content, /New group context/);
  assert.doesNotMatch(contextUpdate.content, /Current invocation/);
  assert.equal(session.context.resolve(4), id(4));
  await session.send("Current invocation", { externalEventId: "group-event" });
  const answer = session.answerForExternalEvent("group-event");
  assert.match(answer.contextWarning, /@taek42, your Kennedy session in this group/);
  assert.match(answer.contextWarning, /This applies only to @taek42; other members have separate sessions/);
  assert.equal(session.archive().channel.lastGroupContextMessageId, 12);
});

test("passive Telegram group context retains other users' media and Kennedy replies without generating", async () => {
  let generations = 0;
  const session = new ConversationSession({
    kweb: new MockKweb([node(1), node(2), node(3)]),
    intelligence: { generate: async () => { generations += 1; throw new Error("must not generate"); } },
    manuals: promptManuals("Shared"), rootNodeIds: [id(1), id(2), id(3)],
    provider: "p", model: "m", sessionType: "telegram-group",
    channel: {
      kind: "telegram-group", telegramUserId: 42, chatId: -100, lastGroupContextMessageId: 20,
      groupContext: { groupTitle: "Trusted friends", chatId: -100, invokingTelegramUserId: 42, participants: [], messages: [] },
    },
    onUpdate: () => {},
  });
  await session.initialize();
  session.refreshTelegramGroupContext({
    groupTitle: "Trusted friends", chatId: -100, invokingTelegramUserId: 42, throughMessageId: 23,
    participants: [],
    messages: [
      {
        messageId: 21, telegramUserId: 77, displayName: "Friend", kind: "voice",
        text: "[Voice note transcription]\nBring the blueprints.", sentByKennedy: false,
        mediaRef: { kind: "voice", source: "telegram-group", chatId: -100, messageId: 21, mimeType: "audio/ogg" },
      },
      { messageId: 22, telegramUserId: null, displayName: "Kennedy", kind: "text", text: "I will review them.", sentByKennedy: true },
    ],
  });
  assert.equal(generations, 0);
  assert.equal(session.channel.lastGroupContextMessageId, 23);
  assert.match(session.chatend.retained.at(-1).content, /Bring the blueprints/);
  assert.match(session.chatend.retained.at(-1).content, /I will review them/);
  assert.deepEqual(session.media.at(-1), {
    id: "telegram-group:-100:21", kind: "voice", source: "telegram-group",
    chatId: -100, messageId: 21, mimeType: "audio/ogg",
  });
});

test("background Telegram group ingress directly loads the group and Kennedy roots", async () => {
  const app = await readFile(new URL("../public/js/app.js", import.meta.url), "utf8");
  assert.match(app, /const directRoots = \[batch\.groupRootNodeId, kennedyRootNodeId\]/);
  assert.match(app, /groupRootNodeId: batch\.groupRootNodeId/);
  assert.match(app, /if \(!batch\.groupRootReady\)/);
});

test("Telegram relay client exposes identity provisioning and group-ingress queues", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, options = {}) => {
    requests.push({ url, options });
    return {
      ok: true,
      headers: { get: name => name === "content-type" && url.endsWith("/media") ? "audio/ogg" : "application/json" },
      json: async () => ({}),
      blob: async () => new Blob(["media"], { type: "audio/ogg" }),
    };
  };
  try {
    const api = TelegramRelayAPI("http://telegram");
    await api.provisioningUsers();
    await api.userByHandle("@Taek42");
    await api.completeHandleRoot("taek42", id(1));
    await api.userById(42);
    await api.provisioningGroups();
    await api.groupById(-100);
    await api.completeGroupRoot(-100, id(2));
    await api.groupIngress();
    await api.completeGroupIngress("batch");
    await api.groupSessionUpdates();
    await api.acknowledgeGroupContext("019f5ca7-020f-7b63-be2f-82785fb68c03", 51);
    await api.completeSilentGroupReset("019f5ca7-020f-7b63-be2f-82785fb68c03");
    await api.groupMessageMedia(-100, 50);
    await api.saveGroupMessagePreparation(-100, 50, { text: "Prepared", model: "transcriber" });
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.deepEqual(requests.map(request => request.url), [
    "http://telegram/api/v1/users/provisioning",
    "http://telegram/api/v1/users/by-handle/%40Taek42",
    "http://telegram/api/v1/users/by-handle/taek42/root-ready",
    "http://telegram/api/v1/users/42",
    "http://telegram/api/v1/groups/provisioning",
    "http://telegram/api/v1/groups/-100",
    "http://telegram/api/v1/groups/-100/root-ready",
    "http://telegram/api/v1/group-ingress",
    "http://telegram/api/v1/group-ingress/batch/complete",
    "http://telegram/api/v1/group-sessions/updates",
    "http://telegram/api/v1/group-sessions/019f5ca7-020f-7b63-be2f-82785fb68c03/context-ack",
    "http://telegram/api/v1/group-sessions/019f5ca7-020f-7b63-be2f-82785fb68c03/silent-reset-completed",
    "http://telegram/api/v1/group-messages/-100/50/media",
    "http://telegram/api/v1/group-messages/-100/50/preparation",
  ]);
  assert.match(requests[2].options.body, new RegExp(id(1)));
  assert.match(requests[6].options.body, new RegExp(id(2)));
  assert.match(requests[10].options.body, /"throughMessageId":51/);
  assert.match(requests[13].options.body, /"text":"Prepared"/);
});

test("audio, document, durable vnote, and Telegram API clients use their queue endpoints", async () => {
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
    await IntelligenceAPI("http://intelligence").extractDocument({ file: new Blob(["report"], { type: "application/pdf" }), fileName: "report.pdf" });
    await IntelligenceAPI("http://intelligence").recordTiming({ action: "tool", name: "LoadNode", status: "ok", sessionType: "conversation", durationMs: 12 });
    await ConversationHistoryAPI("http://history").releaseIngressRepairs();
    await AudioIngressAPI("http://audio").nextIngress();
    await AudioIngressAPI("http://audio").releaseIngressRepairs();
    await AudioIngressAPI("http://audio").history("recording");
    await AudioIngressAPI("http://audio").ingressCheckpoint("piece", { expected_version: 2, state: { historyIngress: {} } });
    await TelegramRelayAPI("http://telegram").bind("event", "019f5ca7-020f-7b63-be2f-82785fb68c03", "029f5ca7-020f-7b63-be2f-82785fb68c03");
    await TelegramRelayAPI("http://telegram").abort("event", "019f5ca7-020f-7b63-be2f-82785fb68c03", "Timed out");
  } finally {
    globalThis.fetch = originalFetch;
  }
  assert.equal(requests[0].url, "http://intelligence/api/v1/audio/transcriptions");
  assert.equal(requests[0].options.body instanceof FormData, true);
  assert.equal(requests[0].options.headers, undefined);
  assert.equal(requests[1].url, "http://intelligence/api/v1/documents/extract");
  assert.equal(requests[1].options.body instanceof FormData, true);
  assert.equal(requests[2].url, "http://intelligence/api/v1/timings");
  assert.match(requests[2].options.body, /"durationMs":12/);
  assert.equal(requests[3].url, "http://history/api/v1/conversations/ingress/repairs/release");
  assert.equal(requests[3].options.method, "POST");
  assert.equal(requests[4].url, "http://audio/api/v1/audio-ingress/ingress/next");
  assert.equal(requests[5].url, "http://audio/api/v1/audio-ingress/ingress/repairs/release");
  assert.equal(requests[5].options.method, "POST");
  assert.equal(requests[6].url, "http://audio/api/v1/audio-ingress/recording/history");
  assert.equal(requests[7].url, "http://audio/api/v1/audio-ingress/pieces/piece/ingress-checkpoint");
  assert.match(requests[7].options.body, /historyIngress/);
  assert.equal(requests[8].url, "http://telegram/api/v1/events/event/bind");
  assert.match(requests[8].options.body, /"expectedConversationId":"029f5ca7-020f-7b63-be2f-82785fb68c03"/);
  assert.equal(requests[9].url, "http://telegram/api/v1/events/event/abort");
  assert.match(requests[9].options.body, /"message":"Timed out"/);
});
