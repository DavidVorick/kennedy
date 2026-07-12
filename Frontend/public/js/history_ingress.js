import { Chatend } from "./chatend.js";
import { KwebContext } from "./kweb_context.js";
import { composePrompt } from "./prompt_composer.js";
import { ToolExecutor, toolDefinitions } from "./tools.js";
import { runAgentLoop } from "./intelligence.js";

export async function runHistoryIngress({ kweb, intelligence, manuals, rootNodeId, provenanceId, provider, model, onUpdate }) {
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
  const executor = new ToolExecutor({ mode: "ingress", context, api: kweb, provenanceId, loadLimit: 50, onUpdate: () => onUpdate({ chatend, context, executor }) });
  onUpdate({ chatend, context, executor });
  await runAgentLoop({ intelligence, provider, model, chatend, tools: toolDefinitions("ingress"), executor, onUpdate: () => onUpdate({ chatend, context, executor }) });
  return { chatend, context, executor };
}
