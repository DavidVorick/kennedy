import { Chatend } from "./chatend.js?v=20260717.6";
import { KwebContext } from "./kweb_context.js?v=20260718.1";
import { composePrompt, formatModelAttribution, formatTelegramGroupContext } from "./prompt_composer.js?v=20260717.9";
import { ToolExecutor } from "./tools.js?v=20260718.5";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260718.4";
import { createTurnTiming, elapsedMs } from "./timing.js?v=20260715.2";
import { formatChatend } from "./chatend_format.js?v=20260718.2";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function nodeSummary(node, includeShortDescription = true) {
  const rawName = node?.shortName ?? node?.short_name;
  const rawDescription = node?.shortDescription ?? node?.short_description;
  const shortName = typeof rawName === "string" ? rawName.trim() : "";
  const shortDescription = typeof rawDescription === "string" ? rawDescription.trim() : "";
  if (!shortName && (!includeShortDescription || !shortDescription)) return null;
  return {
    shortName: shortName || "Unnamed memory",
    shortDescription: includeShortDescription ? shortDescription || "(no short description)" : null,
  };
}

function collectNodeSummaries(archive) {
  const summaries = new Map();
  const add = (node, includeShortDescription = true) => {
    const summary = nodeSummary(node, includeShortDescription);
    if (!summary) return;
    summaries.set(`${summary.shortName}\u0000${summary.shortDescription || ""}`, summary);
  };
  const addSnapshot = snapshot => {
    for (const node of snapshot?.nodes || []) add(node);
  };
  const addToolResult = result => {
    add(result?.requestedNode);
    for (const node of result?.activeConnectionNodes || []) add(node);
    for (const node of result?.directFanoutNodes || []) add(node);
    for (const node of result?.indirectFanoutNodes || []) add(node, false);
    for (const node of result?.nodes || []) add(node);
    add(result?.node);
    addSnapshot(result?.context);
  };
  addSnapshot(archive?.context?.snapshot);
  for (const entry of archive?.context?.state?.nodesById || []) add(entry?.[1]);
  for (const segment of archive?.fullHistory?.segments || []) addSnapshot(segment?.memory);
  for (const message of archive?.messages || []) addToolResult(message?.tool_result?.result);
  return [...summaries.values()].sort((left, right) =>
    left.shortName.localeCompare(right.shortName) ||
    (left.shortDescription || "").localeCompare(right.shortDescription || "")
  );
}

function compactArchivedMessages(archive, summaries) {
  if (!summaries.length) return archive.messages;
  const isMemoryToolRequest = message => {
    if (message?.role !== "assistant" || typeof message.content !== "string") return false;
    const content = message.content.trim();
    if (!content.startsWith("KENNEDY_TOOL_CALLS\n")) return false;
    try {
      const envelope = JSON.parse(content.slice("KENNEDY_TOOL_CALLS".length).trim());
      const calls = Array.isArray(envelope?.calls) ? envelope.calls : [];
      return calls.length > 0 && calls.every(call => ["LoadNode", "ResetContext"].includes(call?.name));
    } catch {
      return false;
    }
  };
  return archive.messages.filter(message =>
    message?.context_kind !== "memory" &&
    message?.context_kind !== "timing" &&
    message?.display_role !== "Memory tool result" &&
    !isMemoryToolRequest(message)
  );
}

function modelReadableProvenance(data) {
  let archive;
  try {
    archive = JSON.parse(data);
  } catch {
    if (typeof data === "string" && data.trim()) return data.trim();
    throw new Error("Ingress provenance does not contain readable source data.");
  }
  if (!Array.isArray(archive?.messages)) throw new Error("Ingress provenance does not contain readable source data.");
  const summaries = collectNodeSummaries(archive);
  const chatend = formatChatend(compactArchivedMessages(archive, summaries), archive.usage || null);
  const loadedNodes = summaries.length
    ? [
      "Loaded Kmap node summaries from the archived session",
      "",
      ...summaries.flatMap(summary => [
        `- ${summary.shortName}`,
        ...(summary.shortDescription ? [`  ${summary.shortDescription}`] : []),
      ]),
    ].join("\n")
    : "";
  return [chatend, loadedNodes].filter(Boolean).join("\n\n────────────────────────\n\n");
}

export async function runHistoryIngress({ kweb, intelligence, manuals, rootNodeIds, rootNodeId, referenceRootNodeIds = [], groupContext = null, provenanceId, provider, providerKind, model, reasoningEffort, contextWindowTokens = 0, maxInputTokens = 0, sourceSessionType = "conversation", restoredArchive = null, checkpoint = async () => {}, onUpdate, signal = null, operationId = null, beforeMutation = async () => {} }) {
  const provenance = await kweb.provenance(provenanceId);
  rootNodeIds = rootNodeIds || [rootNodeId];
  const context = new KwebContext(kweb, rootNodeIds); await context.initialize();
  const audioSource = sourceSessionType === "audio";
  const retained = [{ role: "user", display_role: audioSource ? "Audio transcript provenance" : "Conversation provenance", context_kind: "provenance", content: [
    audioSource ? "Audio transcript provenance" : "Conversation provenance",
    "",
    `Source: ${provenance.source}`,
    `Created: ${provenance.source_created_at}`,
    ...(audioSource ? [
      "Recording-time semantics: Created is when this vnote began, not when it was uploaded or ingressed. Use it to judge whether its statements are historical, superseded, or still current.",
    ] : []),
    "",
    audioSource ? "Final transcript piece" : "Archived Chatend",
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
  const savedReferences = restoredArchive?.referenceRootNodeIds || referenceRootNodeIds;
  referenceRootNodeIds = [...new Set((Array.isArray(savedReferences) ? savedReferences : []).filter(id => typeof id === "string" && id && !rootNodeIds.includes(id)))];
  for (const durableId of referenceRootNodeIds) context.registerReference(durableId);
  const modelAttribution = formatModelAttribution(model, reasoningEffort);
  sourceSessionType = restoredArchive?.sourceSessionType || sourceSessionType;
  groupContext = restoredArchive?.groupContext || groupContext;
  const sessionContext = sourceSessionType === "telegram-group"
    ? formatTelegramGroupContext(groupContext, context)
    : "";
  const chatend = new Chatend(composePrompt(manuals, "ingress", { providerKind, model, reasoningEffort, sourceSessionType, sessionContext }), context, retained);
  if (Array.isArray(archive?.messages)) {
    chatend.restoreMessages(jsonCopy(archive.messages), Array.isArray(archive.retained) ? jsonCopy(archive.retained) : retained);
  }
  chatend.restoreFullHistory(archive?.fullHistory?.segments);
  const continuation = new ContinuationState(createCacheKey("ingress"));
  const usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  usage.restore(archive?.usage);
  if (archive && !archive.completed) usage.resetThread();
  let completed = Boolean(archive?.completed);
  let roundsUsed = Number.isInteger(archive?.roundsUsed) ? archive.roundsUsed : Number(archive?.usage?.requests) || 0;
  const snapshot = () => ({ chatend, context, executor, continuation, usage, completed, roundsUsed });
  const executor = new ToolExecutor({ mode: "ingress", context, api: kweb, intelligence, provider, model, modelAttribution, provenanceId, loadLimit: 50, sessionType: "history-ingress", onUpdate: () => onUpdate(snapshot()), beforeMutation });
  if (archive?.tools) {
    executor.loadCalls = Number.isInteger(archive.tools.loadCalls) ? archive.tools.loadCalls : 0;
    executor.toolLog = Array.isArray(archive.tools.log) ? jsonCopy(archive.tools.log) : [];
  }
  const archiveSnapshot = () => ({
    format: "kennedy-chatend",
    version: 2,
    sessionType: "history-ingress",
    sourceSessionType,
    rootNodeIds: [...rootNodeIds],
    referenceRootNodeIds: [...referenceRootNodeIds],
    groupContext: jsonCopy(groupContext),
    provenanceId,
    completed,
    provider,
    model,
    systemPrompt: chatend.systemPrompt,
    retained: jsonCopy(chatend.retained),
    messages: jsonCopy(chatend.messages),
    fullHistory: chatend.fullHistorySnapshot(),
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
        signal,
        operationId,
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
