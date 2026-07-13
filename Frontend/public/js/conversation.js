import { Chatend } from "./chatend.js?v=20260713.6";
import { KwebContext } from "./kweb_context.js?v=20260713.6";
import { composePrompt } from "./prompt_composer.js?v=20260713.6";
import { ToolExecutor } from "./tools.js?v=20260713.7";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260713.6";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

export class ConversationSession {
  constructor({ kweb, intelligence, manuals, rootNodeId, provider, model, contextWindowTokens = 0, maxInputTokens = 0, persist = async () => {}, onUpdate }) {
    this.kweb = kweb; this.intelligence = intelligence; this.manuals = manuals; this.rootNodeId = rootNodeId;
    this.provider = provider; this.model = model; this.persist = persist; this.onUpdate = onUpdate;
    this.transcript = []; this.startedAt = new Date().toISOString(); this.pendingTurn = false; this.pendingCheckpointed = false; this.busy = false;
    this.continuation = new ContinuationState(createCacheKey("conversation"));
    this.usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  }

  async initialize(restored = null) {
    const archive = restored?.archive?.format === "kennedy-chatend" ? restored.archive : null;
    if (restored) {
      const savedTranscript = Array.isArray(restored.transcript) ? restored.transcript : archive?.transcript;
      this.transcript = Array.isArray(savedTranscript) ? jsonCopy(savedTranscript) : [];
      this.startedAt = restored.startedAt || archive?.startedAt || this.startedAt;
      this.pendingTurn = Boolean(restored.pendingTurn);
      this.pendingCheckpointed = this.pendingTurn;
    }
    this.context = new KwebContext(this.kweb, this.rootNodeId);
    if (archive?.context?.state) {
      this.context.restore(archive.context.state);
    } else {
      await this.context.initialize();
      const loadedNodeIds = restored?.loadedNodeIds || archive?.context?.diagnostics?.loadedNodeIds || [];
      for (const durableId of loadedNodeIds) {
        if (durableId !== this.rootNodeId && !this.context.loadedNodeIds.includes(durableId)) await this.context.loadDurable(durableId, { internal: true });
      }
    }
    this.chatend = new Chatend(composePrompt(this.manuals, "conversation"), this.context, this.retainedTranscript());
    if (Array.isArray(archive?.messages)) {
      this.chatend.messages = jsonCopy(archive.messages);
      this.chatend.systemPrompt = archive.systemPrompt || this.chatend.systemPrompt;
      this.chatend.retained = Array.isArray(archive.retained) ? jsonCopy(archive.retained) : this.retainedTranscript();
    }
    this.executor = new ToolExecutor({ mode: "conversation", context: this.context, api: this.kweb, intelligence: this.intelligence, provider: this.provider, model: this.model, loadLimit: 20, onUpdate: this.onUpdate });
    if (archive?.tools) {
      this.executor.loadCalls = Number.isInteger(archive.tools.loadCalls) ? archive.tools.loadCalls : 0;
      this.executor.toolLog = Array.isArray(archive.tools.log) ? jsonCopy(archive.tools.log) : [];
    }
    this.usage.restore(archive?.usage);
    this.durableState = this.snapshot();
    this.onUpdate();
  }

  retainedTranscript() { return this.transcript.map(item => ({ role: item.role === "kennedy" ? "assistant" : "user", content: item.content })); }

  snapshot() {
    return {
      stateVersion: 2,
      startedAt: this.startedAt,
      transcript: jsonCopy(this.transcript),
      loadedNodeIds: [...(this.context?.loadedNodeIds || [])],
      pendingTurn: this.pendingTurn,
      archive: this.archive(),
    };
  }

  archive() {
    return {
      format: "kennedy-chatend",
      version: 1,
      sessionType: "conversation",
      startedAt: this.startedAt,
      provider: this.provider,
      model: this.model,
      systemPrompt: this.chatend?.systemPrompt || "",
      retained: jsonCopy(this.chatend?.retained || []),
      transcript: jsonCopy(this.transcript),
      messages: jsonCopy(this.chatend?.messages || []),
      context: {
        snapshot: jsonCopy(this.context?.snapshot() || { directlyLoadedIdentifiers: [], nodes: [] }),
        diagnostics: jsonCopy(this.context?.diagnostics() || {}),
        state: jsonCopy(this.context?.archive() || {}),
      },
      tools: {
        loadCalls: this.executor?.loadCalls || 0,
        loadLimit: this.executor?.loadLimit || 20,
        log: jsonCopy(this.executor?.toolLog || []),
      },
      usage: jsonCopy(this.usage?.snapshot() || null),
      media: [],
    };
  }

  async persistSnapshot(state = this.snapshot(), metadata = {}) {
    await this.persist(state, metadata);
    this.durableState = jsonCopy(state);
  }

  restoreDurableState() {
    const state = this.durableState;
    const archive = state?.archive;
    if (!archive || !Array.isArray(archive.messages) || !archive.context?.state) return;
    this.transcript = jsonCopy(state.transcript || archive.transcript || []);
    this.pendingTurn = Boolean(state.pendingTurn);
    this.pendingCheckpointed = this.pendingTurn;
    this.chatend.messages = jsonCopy(archive.messages);
    this.chatend.systemPrompt = archive.systemPrompt || this.chatend.systemPrompt;
    this.chatend.retained = jsonCopy(archive.retained || this.retainedTranscript());
    this.context.restore(archive.context.state);
    this.executor.loadCalls = Number.isInteger(archive.tools?.loadCalls) ? archive.tools.loadCalls : 0;
    this.executor.toolLog = jsonCopy(archive.tools?.log || []);
    this.usage.restore(archive.usage);
  }

  async runPendingTurn() {
    if (!this.pendingTurn) return null;
    let answer;
    try {
      answer = await runAgentLoop({
        intelligence: this.intelligence, provider: this.provider, model: this.model,
        chatend: this.chatend, executor: this.executor, continuation: this.continuation,
        usage: this.usage, onUpdate: this.onUpdate,
        checkpoint: () => this.persistSnapshot(),
      });
    } catch (error) {
      this.restoreDurableState();
      this.continuation.reset();
      throw error;
    }
    this.transcript.push({ role: "kennedy", content: answer });
    this.chatend.retained = this.retainedTranscript();
    try {
      await this.persistSnapshot({ ...this.snapshot(), pendingTurn: false });
    } catch (error) {
      this.restoreDurableState();
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
      await this.persistSnapshot(this.snapshot(), { userActivity: true });
      this.pendingCheckpointed = true;
      return await this.runPendingTurn();
    } finally { this.busy = false; this.onUpdate(); }
  }

  async resumePendingTurn() {
    if (!this.pendingTurn || this.busy) return null;
    this.busy = true; this.onUpdate();
    try {
      if (!this.pendingCheckpointed) {
        await this.persistSnapshot(this.snapshot(), { userActivity: true });
        this.pendingCheckpointed = true;
      }
      return await this.runPendingTurn();
    }
    finally { this.busy = false; this.onUpdate(); }
  }

  serialize() { return JSON.stringify(this.archive(), null, 2); }
}
