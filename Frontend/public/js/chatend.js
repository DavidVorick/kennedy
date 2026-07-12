import { formatKmapContext } from "./human_format.js";

export class Chatend {
  constructor(systemPrompt, context, retained = []) { this.systemPrompt = systemPrompt; this.context = context; this.retained = retained; this.messages = []; this.rebuild(); }

  contextMessage() { return { role: "system", content: formatKmapContext(this.context.snapshot()) }; }

  rebuild() { this.messages = [{ role: "system", content: this.systemPrompt }, ...this.retained.map(item => ({ ...item })), this.contextMessage()]; }

  rebuildAfterReset(assistantMessage, toolResult) { this.rebuild(); this.messages.push(assistantMessage, toolResult); }
  append(message) { this.messages.push(message); }
  replaceRetained(retained) { this.retained = retained; this.rebuild(); }
}
