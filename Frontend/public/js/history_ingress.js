import { Chatend } from "./chatend.js?v=20260714.7";
import { KwebContext } from "./kweb_context.js?v=20260714.7";
import { composePrompt, formatModelAttribution } from "./prompt_composer.js?v=20260714.7";
import { ToolExecutor } from "./tools.js?v=20260714.7";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260714.7";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function modelReadableProvenance(data) {
  try {
    const archive = JSON.parse(data);
    if (Array.isArray(archive?.media)) {
      archive.media = archive.media.map(item => {
        if (!item || typeof item !== "object" || typeof item.dataUrl !== "string") return item;
        const { dataUrl: _archivedBytes, ...metadata } = item;
        return { ...metadata, archivedOriginal: "Original audio retained in provenance; binary data omitted from model context." };
      });
    }
    return JSON.stringify(archive, null, 2);
  } catch {
    return data;
  }
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
    "Archived Chatend (JSON)",
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
  let completed = Boolean(archive?.completed);
  const snapshot = () => ({ chatend, context, executor, continuation, usage, completed });
  const executor = new ToolExecutor({ mode: "ingress", context, api: kweb, intelligence, provider, model, modelAttribution, provenanceId, loadLimit: 50, onUpdate: () => onUpdate(snapshot()) });
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
    media: [],
  });
  onUpdate(snapshot());
  await checkpoint(archiveSnapshot());
  if (!completed) {
    await runAgentLoop({
      intelligence, provider, model, chatend, executor, continuation, usage,
      onUpdate: () => onUpdate(snapshot()),
      checkpoint: () => checkpoint(archiveSnapshot()),
    });
    completed = true;
    onUpdate(snapshot());
    await checkpoint(archiveSnapshot());
  }
  return snapshot();
}
