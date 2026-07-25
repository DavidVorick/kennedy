import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  AudioIngressAPI,
  IntelligenceAPI,
  KwebAPI,
  SessionHistoryAPI,
  TelegramRelayAPI,
  newIdempotencyId,
} from "../public/js/api.js";
import {
  audioRecordingTitle,
  conversationIngressActivity,
  conversationTitle,
  inspectorText,
  mainViewEntries,
  reconcileConversationHistory,
  sortConversationHistory,
} from "../public/js/render.js";
import {
  contextUsageMeasurement,
  formatChatend,
  formatContextWindowProgress,
} from "../public/js/chatend_format.js";
import {
  freeTimeTiming,
  parseFreeTimeMinutes,
  parseSelfTimePrompt,
} from "../public/js/self_time.js";
import {
  projectSessionLog,
  projectSessionRecord,
} from "../public/js/session_log_view.js";

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

function sessionEvent(role, kind, recordedAt = "2026-07-24T00:00:00Z") {
  return {
    role,
    text: JSON.stringify({
      contextEventVersion: 1,
      recordedAt,
      kind,
    }),
  };
}

function combinedConversationAndIngressLog() {
  const events = [
    sessionEvent("system-message", {
      type: "session_configured",
      effective_context_tokens: 128_000,
      kind: "conversation",
    }),
    sessionEvent("system-message", {
      type: "box_created",
      box_id: 2,
      name: "Kennedy system prompt",
      owner: { kind: "system" },
      content: { text: "Conversation instructions", metadata: {} },
    }),
    sessionEvent("user-message", {
      type: "box_created",
      box_id: 3,
      name: "User message",
      owner: { kind: "user" },
      content: {
        text: "Plan the release",
        objects: ["pending:3"],
        metadata: {
          inputKind: "voice",
          attachments: [{
            pendingId: "pending:3",
            fileName: "release.txt",
            mimeType: "text/plain",
          }],
        },
      },
    }),
    sessionEvent("kennedy-message", {
      type: "box_created",
      box_id: 4,
      name: "Kennedy message",
      owner: { kind: "kennedy" },
      content: { text: "Here is the release plan.", metadata: {} },
    }),
    sessionEvent("system-message", {
      type: "source_terminated",
      reason: "history_ingress",
    }),
    sessionEvent("system-message", {
      type: "canonical_updated",
      box_id: 2,
      content: { text: "History ingress instructions", metadata: {} },
    }),
    sessionEvent("system-message", { type: "history_ingress_started" }),
    sessionEvent("system-message", {
      type: "note",
      label: "provider_input",
      value: "exact ingress provider input",
    }),
    sessionEvent("kennedy-tool-call", {
      type: "tool_invoked",
      tool_instance: "CreateNode:operation",
      tool_name: "CreateNode",
      arguments: { shortName: "Release plan" },
    }),
    sessionEvent("kennedy-message", {
      type: "box_created",
      box_id: 10,
      name: "Kennedy tool call: CreateNode",
      owner: { kind: "kennedy" },
      content: { text: "duplicate tool presentation", metadata: {} },
    }),
    sessionEvent("system-message", {
      type: "box_created",
      box_id: 11,
      name: "Kennedy tool result",
      owner: { kind: "controller" },
      content: { text: "duplicate tool result presentation", metadata: {} },
    }),
    sessionEvent("tool-result", {
      type: "tool_completed",
      tool_instance: "call_ktool",
      tool_name: "call_ktool",
      outcome: { ok: true, result: "Created staged node pending:12." },
    }),
    sessionEvent("system-message", {
      type: "provider_receipt",
      manifest_hash: "manifest",
      input_tokens: 2_000,
      output_tokens: 100,
      raw_context_tokens: 1_900,
      provider_data: {
        inputTokens: 2_000,
        outputTokens: 100,
        cachedInputTokens: 800,
      },
    }),
    sessionEvent("kennedy-message", {
      type: "box_created",
      box_id: 14,
      name: "Kennedy message",
      owner: { kind: "kennedy" },
      content: { text: "Internal ingress response", metadata: {} },
    }),
  ];
  return {
    header: {
      formatVersion: "0.2.1",
      sessionId: "session-1",
      createdAt: "2026-07-24T00:00:00Z",
    },
    events,
  };
}

test("browser idempotency identifiers are random hexadecimal values", () => {
  const generated = newIdempotencyId();
  assert.match(generated, /^[0-9a-f]{32}$/);
});

test("browser API clients expose only browser-owned reads and commands", async () => {
  const calls = [];
  await withMockFetch(async (url, options = {}) => {
    calls.push({ url: String(url), method: options.method || "GET", body: options.body });
    return jsonResponse({ ok: true });
  }, async () => {
    const history = SessionHistoryAPI("http://kennedy");
    await history.health();
    await history.start({ idempotency_id: "start", session_type: "conversation", started_at: "now" });
    await history.queueCommand("conversation", { idempotency_id: "command", kind: "send" });

    const audio = AudioIngressAPI("http://kennedy");
    await audio.health();
    await audio.retryIngress("piece", { expected_version: 3 });

    const intelligence = IntelligenceAPI("http://kennedy");
    await intelligence.health();
    const kmap = KwebAPI("http://kennedy");
    await kmap.roots();
    const relay = TelegramRelayAPI("http://telegram");
    await relay.health();
  });

  assert.deepEqual(calls.map(call => [call.method, call.url]), [
    ["GET", "http://kennedy/api/v1/conversations/health"],
    ["POST", "http://kennedy/api/v1/conversations/start"],
    ["POST", "http://kennedy/api/v1/conversations/conversation/commands"],
    ["GET", "http://kennedy/api/v1/audio-ingress/health"],
    ["POST", "http://kennedy/api/v1/audio-ingress/pieces/piece/retry-ingress"],
    ["GET", "http://kennedy/health"],
    ["GET", "http://kennedy/api/v1/kmap/roots"],
    ["GET", "http://telegram/health"],
  ]);
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

test("session-log projection separates source conversation from history ingress", () => {
  const log = combinedConversationAndIngressLog();
  const projection = projectSessionLog(log, { provider: "openai", model: "gpt-test" });

  assert.deepEqual(
    projection.transcript.map(item => [item.role, item.content]),
    [
      ["user", "Plan the release"],
      ["kennedy", "Here is the release plan."],
    ],
  );
  assert.equal(projection.transcript[0].inputKind, "voice");
  assert.deepEqual(projection.transcript[0].objects, ["pending:3"]);
  assert.equal(projection.transcript[0].attachments[0].fileName, "release.txt");
  assert.equal(projection.firstUserMessage, "Plan the release");
  assert.deepEqual(projection.boundaries, {
    sourceEnd: 4,
    transitionStart: 4,
    historyStart: 6,
  });

  const sourceText = projection.conversationDiagnostic.chatend
    .map(message => message.content).join("\n");
  const ingressText = projection.ingressDiagnostic.chatend
    .map(message => message.content).join("\n");
  assert.match(sourceText, /Plan the release|Here is the release plan/);
  assert.doesNotMatch(sourceText, /Internal ingress response|Created staged node/);
  assert.match(ingressText, /Internal ingress response|Created staged node/);
  assert.doesNotMatch(ingressText, /Plan the release|Here is the release plan/);
  assert.doesNotMatch(ingressText, /duplicate tool presentation|duplicate tool result presentation/);

  assert.deepEqual(projection.ingressDiagnostic.toolLog, [{
    name: "CreateNode",
    ok: true,
    result: "Created staged node pending:12.",
  }]);
  assert.equal(
    projection.ingressDiagnostic.chatend
      .find(message => message.display_role === "Memory tool result")
      ?.tool_name,
    "CreateNode",
  );
  assert.equal(projection.ingressDiagnostic.usage.requests, 1);
  assert.equal(projection.ingressDiagnostic.usage.contextWindowTokens, 128_000);
  assert.equal(projection.ingressDiagnostic.chatendText, "exact ingress provider input");
  assert.equal(projection.ingressDiagnostic.historySegments.length, 1);
  assert.equal(
    projection.boundaries.sourceEnd
      + (projection.boundaries.historyStart - projection.boundaries.transitionStart + 1)
      + (log.events.length - projection.boundaries.historyStart - 1),
    log.events.length,
  );
});

test("live and completed records project the same canonical session log", () => {
  const log = combinedConversationAndIngressLog();
  const live = {
    id: "session-1",
    phase: "ingress_in_progress",
    started_at: log.header.createdAt,
    state: {
      sessionId: log.header.sessionId,
      startedAt: log.header.createdAt,
      events: log.events,
      historyIngress: {
        format: "kennedy-chatend",
        sessionType: "history-ingress",
      },
    },
  };
  const completed = {
    id: "archive-1",
    phase: "complete",
    state: {
      sessionType: "conversation",
      sessionObjectId: "archive-1",
      archive: log,
    },
  };
  const liveProjection = projectSessionRecord(live);
  const completedProjection = projectSessionRecord(completed);

  assert.deepEqual(completedProjection.transcript, liveProjection.transcript);
  assert.deepEqual(
    completedProjection.conversationDiagnostic.chatend,
    liveProjection.conversationDiagnostic.chatend,
  );
  assert.deepEqual(
    completedProjection.ingressDiagnostic.chatend,
    liveProjection.ingressDiagnostic.chatend,
  );
  assert.equal(conversationTitle(live), "Plan the release");
  assert.equal(conversationTitle(completed), "Plan the release");
  assert.equal(conversationTitle({
    state: { firstUserMessage: "Summary-only title", transcript: [] },
  }), "Summary-only title");
});

test("session history groups phases without mutating backend results", () => {
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

test("session history reconciliation never regresses a hydrated record", () => {
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
  const reconstructed = conversationIngressActivity({
    record,
    savedDiagnostic: {
      chatend: [{ role: "assistant", content: "Reconstructed ingress" }],
      usage: { contextTokens: 12 },
      toolLog: [{ name: "EndSession", ok: true }],
    },
  });
  assert.equal(reconstructed.diagnostic.chatend.messages[0].content, "Reconstructed ingress");
  assert.equal(reconstructed.diagnostic.usage.snapshot().contextTokens, 12);
  const live = conversationIngressActivity({ record, liveRecordId: "selected", liveDiagnostic: { round: 3 } });
  assert.equal(live.diagnostic.round, 3);
  assert.equal(conversationIngressActivity({ record, dismissedId: "selected" }), null);
  const preparing = conversationIngressActivity({
    record: { id: "queued", phase: "ingress_pending", state: {} },
  });
  assert.equal(preparing.active, true);
  assert.deepEqual(preparing.diagnostic.chatend.messages, []);
});

test("self-time validation and display timing use the backend deadline", () => {
  assert.equal(parseFreeTimeMinutes("30"), 30);
  assert.throws(() => parseFreeTimeMinutes("0"));
  assert.equal(parseSelfTimePrompt("  investigate memory  "), "investigate memory");
  const now = Date.parse("2026-07-20T12:00:00Z");
  const freeTime = {
    runId: "run", sliceIndex: 1, deadlineAt: new Date(now + 90_000).toISOString(),
  };
  const timing = freeTimeTiming(freeTime, now);
  assert.equal(timing.warningDue, true);
  assert.equal(timing.remainingMs, 90_000);
  assert.ok(timing.hardStopMs > timing.deadlineMs);
});

test("Chatend formatting uses the latest complete context measurement", () => {
  const usage = {
    contextWindowTokens: 128_000,
    contextKnown: true,
    contextTokens: 88_000,
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

test("Full inspector displays the exact backend provider input without reconstruction", () => {
  const chatend = [
    { role: "system", content: "FRONTEND RECONSTRUCTION MUST NOT APPEAR" },
  ];
  const exact = '{"id":3,"method":"turn/start","params":{"input":[{"type":"text","text":"Exact input"}]}}\n';
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
      { role: "assistant", context_kind: "tool-call", tool_name: "LoadNode", tool_arguments: { identifier: 2 }, content: 'call_ktool · LoadNode\n\n{"identifier":2}' },
      { role: "assistant", context_kind: "tool-call", tool_name: "LoadNode", tool_arguments: { identifier: 3 }, content: 'call_ktool · LoadNode\n\n{"identifier":3}' },
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
  const [app, api, render, sessionLogView, humanFormat] = await Promise.all([
    readFile(new URL("../public/js/app.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/api.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/render.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/session_log_view.js", import.meta.url), "utf8"),
    readFile(new URL("../public/js/human_format.js", import.meta.url), "utf8"),
  ]);
  assert.match(app, /kwebBase: window\.location\.origin/);
  assert.match(app, /intelligenceBase: window\.location\.origin/);
  assert.match(app, /conversationHistoryBase: window\.location\.origin/);
  assert.match(app, /audioIngressBase: window\.location\.origin/);
  assert.doesNotMatch(app, /4323|4325/);
  assert.doesNotMatch(`${app}\n${api}\n${render}\n${sessionLogView}`, /innerHTML|insertAdjacentHTML|outerHTML/);
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
