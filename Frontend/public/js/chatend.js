export class Chatend {
  constructor(systemPrompt, context, retained = []) { this.systemPrompt = systemPrompt; this.context = context; this.retained = retained; this.messages = []; this.rebuild(); }

  contextMessage() { return { role: "system", content: `<current_kmap_context>\n${JSON.stringify(this.context.snapshot(), null, 2)}\n</current_kmap_context>` }; }

  rebuild() { this.messages = [{ role: "system", content: this.systemPrompt }, ...this.retained.map(item => ({ ...item })), this.contextMessage()]; }

  rebuildAfterReset(assistantMessage, toolResult) { this.rebuild(); this.messages.push(assistantMessage, toolResult); }
  append(message) { this.messages.push(message); }
  replaceRetained(retained) { this.retained = retained; this.rebuild(); }
}

