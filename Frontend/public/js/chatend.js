import { formatKmapContext } from "./human_format.js?v=20260714.7";

export class Chatend {
  constructor(systemPrompt, context, retained = []) { this.systemPrompt = systemPrompt; this.context = context; this.retained = retained; this.messages = []; this.rebuild(); }

  contextMessage() { return { role: "system", display_role: "Kmap context", context_kind: "memory", content: formatKmapContext(this.context.snapshot()) }; }

  rebuild() { this.messages = [{ role: "system", display_role: "Agent manuals", context_kind: "instructions", content: this.systemPrompt }, ...this.retained.map(item => ({ ...item })), this.contextMessage()]; }

  restoreMessages(messages, retained = this.retained) {
    this.retained = retained;
    this.messages = messages.map(message => ({ ...message }));
    const instructionIndex = this.messages.findIndex(message => message.context_kind === "instructions");
    if (instructionIndex >= 0) this.messages[instructionIndex] = { ...this.messages[instructionIndex], content: this.systemPrompt };
    else this.messages.unshift({ role: "system", display_role: "Agent manuals", context_kind: "instructions", content: this.systemPrompt });
    const memoryIndex = this.messages.findIndex(message => message.context_kind === "memory");
    if (memoryIndex >= 0) this.messages[memoryIndex] = this.contextMessage();
    else this.messages.push(this.contextMessage());
  }

  rebuildAfterReset(assistantMessage, toolResult) { this.rebuild(); this.messages.push(assistantMessage, toolResult); }
  append(message) { this.messages.push(message); }
  replaceRetained(retained) { this.retained = retained; this.rebuild(); }
}
