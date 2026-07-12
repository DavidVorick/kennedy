import { Chatend } from "./chatend.js?v=20260712.1";
import { KwebContext } from "./kweb_context.js?v=20260712.1";
import { composePrompt } from "./prompt_composer.js?v=20260712.1";
import { ToolExecutor } from "./tools.js?v=20260712.1";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260712.1";

export class ConversationSession {
  constructor({ kweb, intelligence, manuals, rootNodeId, provider, model, contextWindowTokens = 0, maxInputTokens = 0, onUpdate }) {
    this.kweb = kweb; this.intelligence = intelligence; this.manuals = manuals; this.rootNodeId = rootNodeId;
    this.provider = provider; this.model = model; this.onUpdate = onUpdate;
    this.transcript = []; this.startedAt = new Date().toISOString(); this.busy = false;
    this.continuation = new ContinuationState(createCacheKey("conversation"));
    this.usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
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
      const answer = await runAgentLoop({ intelligence: this.intelligence, provider: this.provider, model: this.model, chatend: this.chatend, executor: this.executor, continuation: this.continuation, usage: this.usage, onUpdate: this.onUpdate });
      this.transcript.push({ role: "kennedy", content: answer });
      this.chatend.retained = this.retainedTranscript();
      return answer;
    } finally { this.busy = false; this.onUpdate(); }
  }

  serialize() { return this.transcript.map(item => `${item.role === "kennedy" ? "Kennedy" : "David"}: ${item.content}`).join("\n\n"); }
}
