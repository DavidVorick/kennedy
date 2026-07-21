import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  AudioIngressAPI,
  ConversationHistoryAPI,
  IntelligenceAPI,
  KwebAPI,
  TelegramDirectoryAPI,
  TelegramRelayAPI,
  newIdempotencyId,
} from "../public/js/api.js";
import {
  audioRecordingTitle,
  conversationControlState,
  conversationIngressActivity,
  conversationTitle,
  inspectorText,
  mainViewEntries,
  reconcileConversationHistory,
  sortConversationHistory,
} from "../public/js/render.js";
import { formatContextNode, formatToolResult } from "../public/js/human_format.js";
import {
  contextUsageMeasurement,
  formatChatend,
  formatContextWindowProgress,
} from "../public/js/chatend_format.js";
import {
  FREE_TIME_HARD_STOP_GRACE_MS,
  FREE_TIME_WARNING_MS,
  freeTimeCanStartNewSession,
  freeTimeTiming,
  nextFreeTimeSlice,
  parseFreeTimeMinutes,
  parseSelfTimePrompt,
} from "../public/js/self_time.js";

const id = value => value.toString(16).padStart(40, "0");
const summary = value => ({ id: id(value), short_name: `Node ${value}`, short_description: `Summary ${value}` });

async function withMockFetch(handler, operation) {
  const original = globalThis.fetch;
  globalThis.fetch = handler;
  try {
    return await operation();
  } finally {
    globalThis.fetch = original;
  }
}

function jsonResponse(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

test("Kmap client uses namespaced routes and derives active context", async () => {
  const calls = [];
  const stored = new Map([
    [id(1), {
      id: id(1), short_name: "Root", short_description: "", long_description: "Root details",
      owner_node_id: id(1), fixed_connections: [id(3)],
      recent_connections: [id(2), id(4), id(5), id(6), id(7), id(8), id(9), id(10), id(11)],
      connection_summaries: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map(summary),
    }],
    ...[2, 4, 5, 6, 7, 8, 9, 10].map(value => [id(value), {
      id: id(value), short_name: `Node ${value}`, short_description: `Summary ${value}`,
      long_description: `Details ${value}`, owner_node_id: id(1), fixed_connections: [],
      recent_connections: [], connection_summaries: [],
    }]),
  ]);

  await withMockFetch(async url => {
    calls.push(String(url));
    return jsonResponse(stored.get(String(url).split("/").at(-1)));
  }, async () => {
    const context = await KwebAPI("http://local").context(id(1));
    assert.equal(context.requested_node.active_connections.length, 8);
    assert.deepEqual(context.requested_node.fanout_connections, [summary(11)]);
    assert.deepEqual(context.requested_node.fixed_connections, [{ ...summary(3), slot: 1 }]);
    assert.equal(context.active_connection_nodes.length, 8);
  });

  assert.equal(calls[0], `http://local/api/v1/kmap/nodes/${id(1)}`);
  assert.equal(calls.length, 9);
});

test("Kmap mutation retries preserve the caller's idempotency identifier", async () => {
  const generated = newIdempotencyId();
  assert.match(generated, /^[0-9a-f]{32}$/);
  const requests = [];
  await withMockFetch(async (url, options) => {
    requests.push({ url: String(url), body: options.body });
    if (requests.length === 1) throw new TypeError("ambiguous disconnect");
    return jsonResponse({ id: "saved" }, 201);
  }, async () => {
    await KwebAPI("http://local").createProvenance({
      idempotency_id: generated,
      data: "source",
      source: "test",
      source_created_at: "2026-07-20T00:00:00Z",
    });
  });
  assert.equal(requests.length, 2);
  assert.equal(requests[0].url, "http://local/api/v1/kmap/provenance");
  assert.equal(requests[0].body, requests[1].body);
  assert.equal(JSON.parse(requests[1].body).idempotency_id, generated);
});

test("production API clients retain backend-owned queue boundaries", async () => {
  const calls = [];
  await withMockFetch(async (url, options = {}) => {
    calls.push({ url: String(url), method: options.method || "GET", body: options.body });
    return jsonResponse({ ok: true });
  }, async () => {
    const history = ConversationHistoryAPI("http://kennedy");
    await history.health();
    await history.start({ idempotency_id: "start", session_type: "conversation", started_at: "now" });
    await history.queueCommand("conversation", { idempotency_id: "command", kind: "send" });
    await history.claimCommand("command");
    await history.completeCommand("command", { delivered: true });

    const audio = AudioIngressAPI("http://kennedy");
    await audio.health();
    await audio.nextIngress();
    await audio.retryIngress("piece", { expected_version: 3 });

    const intelligence = IntelligenceAPI("http://kennedy");
    await intelligence.generate({ provider: "codex", messages: [] }, { operationId: "operation" });
    await intelligence.cancelOperation("operation");

    const directory = TelegramDirectoryAPI("http://kennedy");
    await directory.completeHandleRoot("@david", id(1));
    const relay = TelegramRelayAPI("http://telegram");
    await relay.completeGroupIngress("group-ingress");
  });

  assert.deepEqual(calls.map(call => [call.method, call.url]), [
    ["GET", "http://kennedy/api/v1/conversations/health"],
    ["POST", "http://kennedy/api/v1/conversations/start"],
    ["POST", "http://kennedy/api/v1/conversations/conversation/commands"],
    ["POST", "http://kennedy/api/v1/conversation-commands/command/claim"],
    ["POST", "http://kennedy/api/v1/conversation-commands/command/complete"],
    ["GET", "http://kennedy/api/v1/audio-ingress/health"],
    ["GET", "http://kennedy/api/v1/audio-ingress/ingress/next"],
    ["POST", "http://kennedy/api/v1/audio-ingress/pieces/piece/retry-ingress"],
    ["POST", "http://kennedy/api/v1/generate"],
    ["POST", "http://kennedy/api/v1/operations/operation/cancel"],
    ["POST", `http://kennedy/api/v1/telegram-directory/users/by-handle/%40david/root-ready`],
    ["POST", "http://telegram/api/v1/group-ingress/group-ingress/complete"],
  ]);
  assert.equal(JSON.parse(calls[8].body).operation_id, "operation");
});

test("conversation and audio titles use durable source data", () => {
  const record = { state: { transcript: [
    { role: "user", content: "  Plan   a long weekend in San Salvador with excellent coffee  " },
    { role: "assistant", content: "Let's do it." },
  ] } };
  assert.equal(conversationTitle(record, 32), "Plan a long weekend in San Salv…");
  assert.equal(conversationTitle({ state: { transcript: [] } }), "New conversation");
  assert.equal(conversationTitle({
    state: { sessionType: "free-time", freeTime: { sliceIndex: 4, customPrompt: "Explore memory" } },
  }), "Explore memory · session 4");
  assert.equal(audioRecordingTitle({ original_filename: "2026-07-20-vnote.wav" }), "2026-07-20-vnote.wav");
});

test("conversation history groups phases without mutating backend results", () => {
  const records = [
    { id: "complete", phase: "complete", updated_at: "2026-07-20T12:00:00Z" },
    { id: "pending", phase: "ingress_pending", updated_at: "2026-07-20T10:00:00Z" },
    { id: "active-old", phase: "active", updated_at: "2026-07-19T10:00:00Z" },
    { id: "failed", phase: "ingress_failed", updated_at: "2026-07-20T11:00:00Z" },
    { id: "active-new", phase: "active", updated_at: "2026-07-20T09:00:00Z" },
  ];
  assert.deepEqual(sortConversationHistory(records).map(record => record.id), [
    "active-new", "active-old", "failed", "pending", "complete",
  ]);
  assert.equal(records[0].id, "complete");
});

test("conversation history reconciliation never regresses a hydrated record", () => {
  const hydrated = [{
    id: "active", version: 5, phase: "active",
    state: { transcript: [{ role: "user", content: "Complete history" }] },
  }];
  const stale = [{ id: "active", version: 4, phase: "active", summary: true, state: {} }];
  assert.equal(reconcileConversationHistory(hydrated, stale)[0], hydrated[0]);

  const sameVersionSummary = [{
    id: "active", version: 5, phase: "active", summary: true,
    state: { transcript: [{ role: "user", content: "Complete history" }] },
  }];
  assert.equal(reconcileConversationHistory(hydrated, sameVersionSummary)[0], hydrated[0]);
});

test("conversation controls keep drafting available while preventing overlapping work", () => {
  const transitioning = conversationControlState({
    hasSession: true, sessionBusy: false, transitionBusy: true,
    pendingTurn: false, viewingHistory: false, transcriptLength: 1,
  });
  assert.equal(transitioning.inputDisabled, false);
  assert.equal(transitioning.sendDisabled, true);
  assert.equal(transitioning.endDisabled, true);

  const working = conversationControlState({
    hasSession: true, sessionBusy: true, transitionBusy: false,
    pendingTurn: false, viewingHistory: false, transcriptLength: 1,
  });
  assert.equal(working.inputDisabled, false);
  assert.equal(working.sendDisabled, true);
  assert.equal(working.stopHidden, false);
});

test("ingress activity is scoped to the selected record", () => {
  const record = {
    id: "selected", phase: "ingress_in_progress",
    state: { historyIngress: {
      format: "kennedy-chatend", sessionType: "history-ingress",
      messages: [{ role: "assistant", content: "Saved ingress" }],
    } },
  };
  const saved = conversationIngressActivity({ record, liveRecordId: "other", liveDiagnostic: { round: 9 } });
  assert.equal(saved.diagnostic.chatend.messages[0].content, "Saved ingress");
  const live = conversationIngressActivity({ record, liveRecordId: "selected", liveDiagnostic: { round: 3 } });
  assert.equal(live.diagnostic.round, 3);
  assert.equal(conversationIngressActivity({ record, dismissedId: "selected" }), null);
});

test("self-time validation and deadline calculations preserve the shared run", () => {
  assert.equal(parseFreeTimeMinutes("30"), 30);
  assert.throws(() => parseFreeTimeMinutes("0"));
  assert.equal(parseSelfTimePrompt("  investigate memory  "), "investigate memory");
  const now = Date.parse("2026-07-20T12:00:00Z");
  const freeTime = {
    runId: "run", sliceIndex: 1, deadlineAt: new Date(now + FREE_TIME_WARNING_MS).toISOString(),
    nextSessionMessage: "continue this thread",
  };
  const timing = freeTimeTiming(freeTime, now);
  assert.equal(timing.warningDue, true);
  assert.equal(timing.hardStopMs, timing.deadlineMs + FREE_TIME_HARD_STOP_GRACE_MS);
  assert.equal(freeTimeCanStartNewSession(freeTime, now), false);
  const next = nextFreeTimeSlice(freeTime);
  assert.equal(next.sliceIndex, 2);
  assert.equal(next.handoffMessage, "continue this thread");
});

test("Chatend formatting uses the latest complete context measurement", () => {
  const usage = {
    contextWindowTokens: 128_000,
    lastContext: { inputTokens: 20_000, outputTokens: 1_000 },
  };
  assert.deepEqual(contextUsageMeasurement(usage), {
    contextKnown: true,
    contextTokens: 21_000,
    contextWindowTokens: 128_000,
    contextRemaining: 107_000,
  });
  assert.equal(formatContextWindowProgress(usage), "context window usage: 21,000 / 128,000");
  assert.match(formatChatend([
    { role: "system", content: "Instructions" },
    { role: "user", content: "Question" },
    { role: "assistant", content: "Answer" },
  ], usage), /System context[\s\S]*David[\s\S]*Kennedy[\s\S]*21,000/);
});

test("human-readable memory and tool results avoid raw implementation shapes", () => {
  const node = {
    identifier: 2,
    shortName: "Project",
    shortDescription: "Current work",
    longDescription: "Detailed project memory",
    lastModifiedBy: "gpt-5-high",
    fixedConnections: [], activeConnections: [], fanoutConnections: [],
  };
  assert.match(formatContextNode(node), /Node 2: Project/);
  assert.match(formatToolResult("LoadNode", {
    ok: true,
    result: { requestedNode: node, activeConnectionNodes: [] },
  }), /Memory load completed/);
  assert.match(formatToolResult("WebFetch", {
    ok: true,
    result: { url: "https://example.com", title: "Example", content: "Readable", retrieved_at: "now" },
  }), /Readable page content/);
});

test("Full inspector displays the exact backend Chatend string without reconstruction", () => {
  const chatend = [
    { role: "system", content: "FRONTEND RECONSTRUCTION MUST NOT APPEAR" },
  ];
  const exact = "Backend-owned Chatend\n\n  spacing is preserved  \n\n{ordinary tool request JSON remains text}";
  const rendered = inspectorText({ chatend, chatendText: exact, usage: { contextKnown: true, contextTokens: 999 }, context: { privateDiagnostic: true } });
  assert.equal(rendered, exact);
  assert.doesNotMatch(rendered, /FRONTEND RECONSTRUCTION|privateDiagnostic|999/);
  assert.equal(inspectorText({ chatend }, "full"), "");
});

test("Main inspector consumes one combined result for a LoadNode batch", () => {
  const direct = identifier => ({
    identifier,
    shortName: `Node ${identifier}`,
    shortDescription: `Summary ${identifier}`,
    longDescription: `Details ${identifier}`,
    fixedConnections: [], activeConnections: [], fanoutConnections: [],
  });
  const entries = mainViewEntries({
    chatend: [
      { role: "assistant", content: 'KENNEDY_TOOL_CALLS\n{"calls":[{"name":"LoadNode","arguments":{"identifier":2}},{"name":"LoadNode","arguments":{"identifier":3}}]}' },
      {
        role: "user",
        display_role: "Memory tool result",
        tool_name: "LoadNode",
        tool_call_count: 2,
        tool_result: { ok: true, result: { directNodes: [direct(2)], directNodePromotions: [3], activeConnectionNodes: [] } },
        content: "Kennedy tool result · LoadNode · 8 ms\n\nMemory load completed.",
      },
      { role: "assistant", content: "Done." },
    ],
    memory: { directlyLoadedIdentifiers: [2, 3], nodes: [direct(2), direct(3)] },
  });
  assert.deepEqual(
    entries.filter(entry => entry.kind === "loaded-node").map(entry => entry.node.identifier),
    [2, 3],
  );
  assert.equal(entries.filter(entry => entry.kind === "tool-result").length, 0);
  assert.equal(entries.at(-1).content, "Done.");
});

test("production frontend is server-driven and uses the consolidated origin", async () => {
  const [app, api, render, humanFormat] = await Promise.all([
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/api.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/render.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/human_format.js", import.meta.url), "utf8"),
  ]);
  assert.match(app, /kwebBase: window\.location\.origin/);
  assert.match(app, /intelligenceBase: window\.location\.origin/);
  assert.match(app, /conversationHistoryBase: window\.location\.origin/);
  assert.match(app, /audioIngressBase: window\.location\.origin/);
  assert.doesNotMatch(app, /4323|4325/);
  assert.doesNotMatch(`${app}\n${api}\n${render}`, /innerHTML|insertAdjacentHTML|outerHTML/);
  assert.doesNotMatch(`${api}\n${humanFormat}`, /RustLib|rust-libs/);
  assert.doesNotMatch(app, /legacy_orchestration/);
});

test("codex-safe mounts the documented runtime catalog cache", async () => {
  const [launcher, readme] = await Promise.all([
    readFile(new URL("../../scripts/codex-safe", import.meta.url), "utf8"),
    readFile(new URL("../../README.md", import.meta.url), "utf8"),
  ]);
  const defaultPath = "${TMPDIR:-/tmp}/kcode-codex-catalogs";
  assert.ok(launcher.includes(`catalog_dir=\${CODEX_SAFE_CATALOG_DIR:-${defaultPath}}`));
  assert.ok(readme.includes(defaultPath));
  assert.doesNotMatch(launcher, /kennedy-codex-catalogs/);
});

test("native orchestration remains a Rust backend concern", async () => {
  const [worker, session, frontend] = await Promise.all([
    readFile(new URL("../../KennedyServer/src/orchestration/worker.rs", import.meta.url), "utf8"),
    readFile(new URL("../../KennedyServer/src/orchestration/session.rs", import.meta.url), "utf8"),
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
  ]);
  assert.match(worker, /Native Rust orchestration worker ready/);
  assert.match(session, /struct Session/);
  assert.doesNotMatch(frontend, /runAgentLoop|ToolExecutor|ConversationSession/);
});
