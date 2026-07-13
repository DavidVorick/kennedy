import { formatKmapContext } from "./human_format.js?v=20260713.6";

export class Chatend {
  constructor(systemPrompt, context, retained = []) { this.systemPrompt = systemPrompt; this.context = context; this.retained = retained; this.messages = []; this.rebuild(); }

  contextMessage() { return { role: "system", display_role: "Kmap context", context_kind: "memory", content: formatKmapContext(this.context.snapshot()) }; }

  rebuild() { this.messages = [{ role: "system", display_role: "Agent manuals", context_kind: "instructions", content: this.systemPrompt }, ...this.retained.map(item => ({ ...item })), this.contextMessage()]; }

  rebuildAfterReset(assistantMessage, toolResult) { this.rebuild(); this.messages.push(assistantMessage, toolResult); }
  append(message) { this.messages.push(message); }
  replaceRetained(retained) { this.retained = retained; this.rebuild(); }
}
