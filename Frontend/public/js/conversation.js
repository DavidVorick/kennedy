import { Chatend } from "./chatend.js?v=20260712.3";
import { KwebContext } from "./kweb_context.js?v=20260712.3";
import { composePrompt } from "./prompt_composer.js?v=20260712.3";
import { ToolExecutor } from "./tools.js?v=20260712.3";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260712.3";

export class ConversationSession {
  constructor({ kweb, intelligence, manuals, rootNodeId, provider, model, contextWindowTokens = 0, maxInputTokens = 0, persist = async () => {}, onUpdate }) {
    this.kweb = kweb; this.intelligence = intelligence; this.manuals = manuals; this.rootNodeId = rootNodeId;
    this.provider = provider; this.model = model; this.persist = persist; this.onUpdate = onUpdate;
    this.transcript = []; this.startedAt = new Date().toISOString(); this.pendingTurn = false; this.pendingCheckpointed = false; this.busy = false;
    this.continuation = new ContinuationState(createCacheKey("conversation"));
    this.usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  }

  async initialize(restored = null) {
    if (restored) {
      this.transcript = Array.isArray(restored.transcript) ? restored.transcript.map(item => ({ role: item.role, content: item.content })) : [];
      this.startedAt = restored.startedAt || this.startedAt;
      this.pendingTurn = Boolean(restored.pendingTurn);
      this.pendingCheckpointed = this.pendingTurn;
    }
    this.context = new KwebContext(this.kweb, this.rootNodeId);
    await this.context.initialize();
    for (const durableId of restored?.loadedNodeIds || []) {
      if (durableId !== this.rootNodeId && !this.context.loadedNodeIds.includes(durableId)) await this.context.loadDurable(durableId, { internal: true });
    }
    this.chatend = new Chatend(composePrompt(this.manuals, "conversation"), this.context, this.retainedTranscript());
    this.executor = new ToolExecutor({ mode: "conversation", context: this.context, api: this.kweb, intelligence: this.intelligence, provider: this.provider, model: this.model, loadLimit: 20, onUpdate: this.onUpdate });
    this.onUpdate();
  }

  retainedTranscript() { return this.transcript.map(item => ({ role: item.role === "kennedy" ? "assistant" : "user", content: item.content })); }

  snapshot() {
    return {
      startedAt: this.startedAt,
      transcript: this.transcript.map(item => ({ ...item })),
      loadedNodeIds: [...(this.context?.loadedNodeIds || [])],
      pendingTurn: this.pendingTurn,
    };
  }

  async runPendingTurn() {
    if (!this.pendingTurn) return null;
    const answer = await runAgentLoop({ intelligence: this.intelligence, provider: this.provider, model: this.model, chatend: this.chatend, executor: this.executor, continuation: this.continuation, usage: this.usage, onUpdate: this.onUpdate });
    this.transcript.push({ role: "kennedy", content: answer });
    this.chatend.retained = this.retainedTranscript();
    try {
      await this.persist({ ...this.snapshot(), pendingTurn: false });
    } catch (error) {
      this.transcript.pop();
      this.chatend = new Chatend(composePrompt(this.manuals, "conversation"), this.context, this.retainedTranscript());
      this.continuation.reset();
      throw error;
    }
    this.pendingTurn = false;
    this.pendingCheckpointed = false;
    this.onUpdate();
    return answer;
  }

  async send(text) {
    if (this.pendingTurn) throw new Error("Kennedy must finish the saved pending query before accepting another message.");
    const content = text.trim(); if (!content) return;
    this.busy = true; this.transcript.push({ role: "user", content });
    this.pendingTurn = true; this.pendingCheckpointed = false;
    this.chatend.retained = this.retainedTranscript();
    this.chatend.append({ role: "user", content });
    this.executor.resetLoadCalls(); this.onUpdate();
    try {
      await this.persist(this.snapshot());
      this.pendingCheckpointed = true;
      return await this.runPendingTurn();
    } finally { this.busy = false; this.onUpdate(); }
  }

  async resumePendingTurn() {
    if (!this.pendingTurn || this.busy) return null;
    this.busy = true; this.executor.resetLoadCalls(); this.onUpdate();
    try {
      if (!this.pendingCheckpointed) {
        await this.persist(this.snapshot());
        this.pendingCheckpointed = true;
      }
      return await this.runPendingTurn();
    }
    finally { this.busy = false; this.onUpdate(); }
  }

  serialize() { return this.transcript.map(item => `${item.role === "kennedy" ? "Kennedy" : "David"}: ${item.content}`).join("\n\n"); }
}
