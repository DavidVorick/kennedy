import { Chatend } from "./chatend.js";
import { KwebContext } from "./kweb_context.js";
import { composePrompt } from "./prompt_composer.js";
import { ToolExecutor, toolDefinitions } from "./tools.js";
import { runAgentLoop } from "./intelligence.js";

export class ConversationSession {
  constructor({ kweb, intelligence, manuals, rootNodeId, provider, model, onUpdate }) {
    this.kweb = kweb; this.intelligence = intelligence; this.manuals = manuals; this.rootNodeId = rootNodeId;
    this.provider = provider; this.model = model; this.onUpdate = onUpdate;
    this.transcript = []; this.startedAt = new Date().toISOString(); this.busy = false;
  }

  async initialize() {
    this.context = new KwebContext(this.kweb, this.rootNodeId);
    await this.context.initialize();
    this.chatend = new Chatend(composePrompt(this.manuals, "conversation"), this.context, []);
    this.executor = new ToolExecutor({ mode: "conversation", context: this.context, api: this.kweb, loadLimit: 20, onUpdate: this.onUpdate });
    this.onUpdate();
  }

  retainedTranscript() { return this.transcript.map(item => ({ role: item.role === "kennedy" ? "assistant" : "user", content: item.content })); }

  async send(text) {
    const content = text.trim(); if (!content) return;
    this.busy = true; this.transcript.push({ role: "user", content });
    this.chatend.retained = this.retainedTranscript();
    this.chatend.append({ role: "user", content });
    this.executor.resetLoadCalls(); this.onUpdate();
    try {
      const answer = await runAgentLoop({ intelligence: this.intelligence, provider: this.provider, model: this.model, chatend: this.chatend, tools: toolDefinitions("conversation"), executor: this.executor, onUpdate: this.onUpdate });
      this.transcript.push({ role: "kennedy", content: answer });
      this.chatend.retained = this.retainedTranscript();
      return answer;
    } finally { this.busy = false; this.onUpdate(); }
  }

  serialize() { return this.transcript.map(item => `${item.role === "kennedy" ? "Kennedy" : "David"}: ${item.content}`).join("\n\n"); }
}

