import { Chatend } from "./chatend.js?v=20260715.7";
import { KwebContext } from "./kweb_context.js?v=20260714.7";
import { composePrompt, formatModelAttribution } from "./prompt_composer.js?v=20260714.7";
import { ToolExecutor } from "./tools.js?v=20260715.7";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260715.9";
import { createTurnTiming, elapsedMs } from "./timing.js?v=20260715.2";
import { formatChatend } from "./chatend_format.js?v=20260715.9";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function modelReadableProvenance(data) {
  let archive;
  try {
    archive = JSON.parse(data);
  } catch {
    if (typeof data === "string" && data.trim()) return data.trim();
    throw new Error("Conversation provenance does not contain a valid archived Chatend.");
  }
  if (!Array.isArray(archive?.messages)) throw new Error("Conversation provenance does not contain a valid archived Chatend.");
  return formatChatend(archive.messages, archive.usage || null);
}

export async function runHistoryIngress({ kweb, intelligence, manuals, rootNodeIds, rootNodeId, provenanceId, provider, model, reasoningEffort, contextWindowTokens = 0, maxInputTokens = 0, sourceSessionType = "conversation", restoredArchive = null, checkpoint = async () => {}, onUpdate }) {
  const provenance = await kweb.provenance(provenanceId);
  rootNodeIds = rootNodeIds || [rootNodeId];
  const context = new KwebContext(kweb, rootNodeIds); await context.initialize();
  const retained = [{ role: "user", content: [
    "Conversation provenance",
    "",
    `Source: ${provenance.source}`,
    `Created: ${provenance.source_created_at}`,
    "",
    "Archived Chatend",
    "",
    modelReadableProvenance(provenance.data),
  ].join("\n") }];
  const archive = restoredArchive?.format === "kennedy-chatend" && restoredArchive?.sessionType === "history-ingress" ? restoredArchive : null;
  if (archive?.context?.state) {
    context.restore(archive.context.state);
    await context.ensureRootsLoaded();
  } else {
    for (const durableId of archive?.context?.diagnostics?.loadedNodeIds || []) {
      if (!rootNodeIds.includes(durableId) && !context.loadedNodeIds.includes(durableId)) await context.loadDurable(durableId);
    }
  }
  const modelAttribution = formatModelAttribution(model, reasoningEffort);
  sourceSessionType = restoredArchive?.sourceSessionType || sourceSessionType;
  const chatend = new Chatend(composePrompt(manuals, "ingress", { model, reasoningEffort, sourceSessionType }), context, retained);
  if (Array.isArray(archive?.messages)) {
    chatend.restoreMessages(jsonCopy(archive.messages), Array.isArray(archive.retained) ? jsonCopy(archive.retained) : retained);
  }
  const continuation = new ContinuationState(createCacheKey("ingress"));
  const usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  usage.restore(archive?.usage);
  if (archive && !archive.completed) usage.resetThread();
  let completed = Boolean(archive?.completed);
  let roundsUsed = Number.isInteger(archive?.roundsUsed) ? archive.roundsUsed : Number(archive?.usage?.requests) || 0;
  const snapshot = () => ({ chatend, context, executor, continuation, usage, completed, roundsUsed });
  const executor = new ToolExecutor({ mode: "ingress", context, api: kweb, intelligence, provider, model, modelAttribution, provenanceId, loadLimit: 50, sessionType: "history-ingress", onUpdate: () => onUpdate(snapshot()) });
  if (archive?.tools) {
    executor.loadCalls = Number.isInteger(archive.tools.loadCalls) ? archive.tools.loadCalls : 0;
    executor.toolLog = Array.isArray(archive.tools.log) ? jsonCopy(archive.tools.log) : [];
  }
  const archiveSnapshot = () => ({
    format: "kennedy-chatend",
    version: 2,
    sessionType: "history-ingress",
    sourceSessionType,
    provenanceId,
    completed,
    provider,
    model,
    systemPrompt: chatend.systemPrompt,
    retained: jsonCopy(chatend.retained),
    messages: jsonCopy(chatend.messages),
    context: {
      snapshot: jsonCopy(context.snapshot()),
      diagnostics: jsonCopy(context.diagnostics()),
      state: jsonCopy(context.archive()),
    },
    tools: {
      loadCalls: executor.loadCalls,
      loadLimit: executor.loadLimit,
      log: jsonCopy(executor.toolLog),
    },
    usage: jsonCopy(usage.snapshot()),
    roundsUsed,
    media: [],
  });
  onUpdate(snapshot());
  await checkpoint(archiveSnapshot());
  if (!completed) {
    const timing = createTurnTiming("history-ingress");
    try {
      await runAgentLoop({
        intelligence, provider, model, chatend, executor, continuation, usage, timing,
        onUpdate: () => onUpdate(snapshot()),
        checkpoint: () => checkpoint(archiveSnapshot()),
        roundOffset: roundsUsed,
        onRoundStart: async currentRound => {
          roundsUsed = currentRound;
          onUpdate(snapshot());
          await checkpoint(archiveSnapshot());
        },
      });
      completed = true;
      onUpdate(snapshot());
      await checkpoint(archiveSnapshot());
      Promise.resolve(intelligence.recordTiming?.({
        action: "turn", status: "ok", sessionType: "history-ingress",
        durationMs: elapsedMs(timing.startedAt), llmDurationMs: timing.llmDurationMs,
        toolDurationMs: timing.toolDurationMs, stepCount: timing.steps.length,
      })).catch(() => {});
    } catch (error) {
      Promise.resolve(intelligence.recordTiming?.({
        action: "turn", status: "error", sessionType: "history-ingress",
        durationMs: elapsedMs(timing.startedAt), llmDurationMs: timing.llmDurationMs,
        toolDurationMs: timing.toolDurationMs, stepCount: timing.steps.length,
      })).catch(() => {});
      throw error;
    }
  }
  return snapshot();
}
