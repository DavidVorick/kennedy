import { Chatend } from "./chatend.js?v=20260712.3";
import { KwebContext } from "./kweb_context.js?v=20260712.3";
import { composePrompt } from "./prompt_composer.js?v=20260712.3";
import { ToolExecutor } from "./tools.js?v=20260712.3";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260712.3";

export async function runHistoryIngress({ kweb, intelligence, manuals, rootNodeId, provenanceId, provider, model, contextWindowTokens = 0, maxInputTokens = 0, onUpdate }) {
  const provenance = await kweb.provenance(provenanceId);
  const context = new KwebContext(kweb, rootNodeId); await context.initialize();
  const retained = [{ role: "user", content: [
    "Conversation provenance",
    "",
    `Source: ${provenance.source}`,
    `Created: ${provenance.source_created_at}`,
    "",
    "Transcript",
    "",
    provenance.data,
  ].join("\n") }];
  const chatend = new Chatend(composePrompt(manuals, "ingress"), context, retained);
  const continuation = new ContinuationState(createCacheKey("ingress"));
  const usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  const snapshot = () => ({ chatend, context, executor, continuation, usage });
  const executor = new ToolExecutor({ mode: "ingress", context, api: kweb, intelligence, provider, model, provenanceId, loadLimit: 50, onUpdate: () => onUpdate(snapshot()) });
  onUpdate(snapshot());
  await runAgentLoop({ intelligence, provider, model, chatend, executor, continuation, usage, onUpdate: () => onUpdate(snapshot()) });
  return snapshot();
}
