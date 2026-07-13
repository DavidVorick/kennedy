import { Chatend } from "./chatend.js?v=20260713.6";
import { KwebContext } from "./kweb_context.js?v=20260713.6";
import { composePrompt } from "./prompt_composer.js?v=20260713.6";
import { ToolExecutor } from "./tools.js?v=20260713.7";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260713.6";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

export async function runHistoryIngress({ kweb, intelligence, manuals, rootNodeId, provenanceId, provider, model, contextWindowTokens = 0, maxInputTokens = 0, restoredArchive = null, checkpoint = async () => {}, onUpdate }) {
  const provenance = await kweb.provenance(provenanceId);
  const context = new KwebContext(kweb, rootNodeId); await context.initialize();
  const retained = [{ role: "user", content: [
    "Conversation provenance",
    "",
    `Source: ${provenance.source}`,
    `Created: ${provenance.source_created_at}`,
    "",
    "Archived Chatend (JSON)",
    "",
    provenance.data,
  ].join("\n") }];
  const archive = restoredArchive?.format === "kennedy-chatend" && restoredArchive?.sessionType === "history-ingress" ? restoredArchive : null;
  if (archive?.context?.state) {
    context.restore(archive.context.state);
  } else {
    for (const durableId of archive?.context?.diagnostics?.loadedNodeIds || []) {
      if (durableId !== rootNodeId && !context.loadedNodeIds.includes(durableId)) await context.loadDurable(durableId, { internal: true });
    }
  }
  const chatend = new Chatend(composePrompt(manuals, "ingress"), context, retained);
  if (Array.isArray(archive?.messages)) {
    chatend.messages = jsonCopy(archive.messages);
    chatend.systemPrompt = archive.systemPrompt || chatend.systemPrompt;
    chatend.retained = Array.isArray(archive.retained) ? jsonCopy(archive.retained) : retained;
  }
  const continuation = new ContinuationState(createCacheKey("ingress"));
  const usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  usage.restore(archive?.usage);
  let completed = Boolean(archive?.completed);
  const snapshot = () => ({ chatend, context, executor, continuation, usage, completed });
  const executor = new ToolExecutor({ mode: "ingress", context, api: kweb, intelligence, provider, model, provenanceId, loadLimit: 50, onUpdate: () => onUpdate(snapshot()) });
  if (archive?.tools) {
    executor.loadCalls = Number.isInteger(archive.tools.loadCalls) ? archive.tools.loadCalls : 0;
    executor.toolLog = Array.isArray(archive.tools.log) ? jsonCopy(archive.tools.log) : [];
  }
  const archiveSnapshot = () => ({
    format: "kennedy-chatend",
    version: 1,
    sessionType: "history-ingress",
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
