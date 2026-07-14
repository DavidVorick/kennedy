import { Chatend } from "./chatend.js?v=20260714.7";
import { KwebContext } from "./kweb_context.js?v=20260714.7";
import { composePrompt, formatModelAttribution } from "./prompt_composer.js?v=20260714.7";
import { ToolExecutor } from "./tools.js?v=20260714.7";
import { ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260714.7";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function transcriptEndsWithUnansweredUser(transcript) {
  return Array.isArray(transcript) && transcript.at(-1)?.role === "user";
}

export class ConversationSession {
  constructor({ kweb, intelligence, manuals, rootNodeIds, rootNodeId, provider, model, reasoningEffort, contextWindowTokens = 0, maxInputTokens = 0, sessionType = "conversation", channel = null, persist = async () => {}, onUpdate = () => {} }) {
    this.kweb = kweb; this.intelligence = intelligence; this.manuals = manuals;
    this.rootNodeIds = rootNodeIds || [rootNodeId]; this.rootNodeId = this.rootNodeIds[0];
    this.provider = provider; this.model = model; this.reasoningEffort = reasoningEffort;
    this.modelAttribution = formatModelAttribution(model, reasoningEffort);
    if (!["conversation", "telegram"].includes(sessionType)) throw new Error("Unsupported Kennedy session type.");
    this.sessionType = sessionType;
    this.channel = channel ? jsonCopy(channel) : null;
    this.persist = persist; this.onUpdate = onUpdate;
    this.transcript = []; this.media = []; this.startedAt = new Date().toISOString(); this.pendingTurn = false; this.pendingCheckpointed = false; this.pendingExternalEventId = null; this.lastContextWarningBand = 0; this.busy = false;
    this.continuation = new ContinuationState(createCacheKey("conversation"));
    this.usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  }

  async initialize(restored = null) {
    const archive = restored?.archive?.format === "kennedy-chatend" ? restored.archive : null;
    if (restored) {
      const savedTranscript = Array.isArray(restored.transcript) ? restored.transcript : archive?.transcript;
      this.transcript = Array.isArray(savedTranscript) ? jsonCopy(savedTranscript) : [];
      this.startedAt = restored.startedAt || archive?.startedAt || this.startedAt;
      this.sessionType = restored.sessionType || archive?.sessionType || this.sessionType;
      this.channel = jsonCopy(restored.channel || archive?.channel || this.channel);
      this.media = jsonCopy(restored.media || archive?.media || []);
      this.pendingTurn = Boolean(restored.pendingTurn) || transcriptEndsWithUnansweredUser(this.transcript);
      this.pendingExternalEventId = restored.pendingExternalEventId || archive?.pendingExternalEventId || null;
      this.lastContextWarningBand = Number(restored.lastContextWarningBand ?? archive?.lastContextWarningBand) || 0;
      this.pendingCheckpointed = this.pendingTurn;
    }
    this.context = new KwebContext(this.kweb, this.rootNodeIds);
    if (archive?.context?.state) {
      this.context.restore(archive.context.state);
      await this.context.ensureRootsLoaded();
    } else {
      await this.context.initialize();
      const loadedNodeIds = restored?.loadedNodeIds || archive?.context?.diagnostics?.loadedNodeIds || [];
      for (const durableId of loadedNodeIds) {
        if (!this.rootNodeIds.includes(durableId) && !this.context.loadedNodeIds.includes(durableId)) await this.context.loadDurable(durableId);
      }
    }
    this.chatend = new Chatend(composePrompt(this.manuals, "conversation", { model: this.model, reasoningEffort: this.reasoningEffort, sessionType: this.sessionType }), this.context, this.retainedTranscript());
    if (Array.isArray(archive?.messages)) {
      this.chatend.restoreMessages(
        jsonCopy(archive.messages),
        Array.isArray(archive.retained) ? jsonCopy(archive.retained) : this.retainedTranscript(),
      );
    }
    this.executor = new ToolExecutor({ mode: "conversation", context: this.context, api: this.kweb, intelligence: this.intelligence, provider: this.provider, model: this.model, modelAttribution: this.modelAttribution, loadLimit: 20, onUpdate: this.onUpdate });
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
      sessionType: this.sessionType,
      channel: jsonCopy(this.channel),
      startedAt: this.startedAt,
      transcript: jsonCopy(this.transcript),
      media: jsonCopy(this.media),
      loadedNodeIds: [...(this.context?.loadedNodeIds || [])],
      pendingTurn: this.pendingTurn,
      pendingExternalEventId: this.pendingExternalEventId,
      lastContextWarningBand: this.lastContextWarningBand,
      archive: this.archive(),
    };
  }

  archive() {
    return {
      format: "kennedy-chatend",
      version: 2,
      sessionType: this.sessionType,
      channel: jsonCopy(this.channel),
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
      pendingExternalEventId: this.pendingExternalEventId,
      lastContextWarningBand: this.lastContextWarningBand,
      media: jsonCopy(this.media),
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
    this.pendingTurn = Boolean(state.pendingTurn) || transcriptEndsWithUnansweredUser(this.transcript);
    this.pendingExternalEventId = state.pendingExternalEventId || archive.pendingExternalEventId || null;
    this.lastContextWarningBand = Number(state.lastContextWarningBand ?? archive.lastContextWarningBand) || 0;
    this.media = jsonCopy(state.media || archive.media || []);
    this.pendingCheckpointed = this.pendingTurn;
    this.chatend.restoreMessages(jsonCopy(archive.messages), jsonCopy(archive.retained || this.retainedTranscript()));
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
    const response = { role: "kennedy", content: answer };
    if (this.pendingExternalEventId) response.externalEventId = this.pendingExternalEventId;
    const usage = this.usage.snapshot();
    if (this.sessionType === "telegram" && usage.contextWindowTokens) {
      const band = Math.floor(usage.contextTokens / 100000);
      if (band > this.lastContextWarningBand) {
        response.contextWarning = `${usage.contextTokens.toLocaleString("en-US")} out of ${usage.contextWindowTokens.toLocaleString("en-US")} context tokens used. Consider resetting with /reset.`;
      }
      this.lastContextWarningBand = band;
    }
    this.transcript.push(response);
    this.chatend.retained = this.retainedTranscript();
    this.pendingTurn = false;
    this.pendingExternalEventId = null;
    this.pendingCheckpointed = false;
    try {
      await this.persistSnapshot(this.snapshot());
    } catch (error) {
      this.restoreDurableState();
      this.continuation.reset();
      throw error;
    }
    this.onUpdate();
    return answer;
  }

  async send(text, metadata = {}) {
    if (this.pendingTurn) throw new Error("Kennedy must finish the saved pending query before accepting another message.");
    const content = text.trim(); if (!content) return;
    const externalEventId = typeof metadata.externalEventId === "string" ? metadata.externalEventId : null;
    if (externalEventId && this.transcript.some(item => item.externalEventId === externalEventId)) {
      return this.answerForExternalEvent(externalEventId)?.content || null;
    }
    const inputKind = metadata.inputKind === "voice" ? "voice" : "text";
    let chatendContent = content;
    const transcriptItem = { role: "user", content, inputKind };
    if (externalEventId) transcriptItem.externalEventId = externalEventId;
    if (inputKind === "voice") {
      const mediaId = metadata.media?.id || crypto.randomUUID();
      transcriptItem.mediaId = mediaId;
      transcriptItem.transcriptionModel = metadata.transcriptionModel || null;
      if (metadata.media) this.media.push({ ...jsonCopy(metadata.media), id: mediaId, transcription: content, transcriptionModel: metadata.transcriptionModel || null });
      chatendContent = [
        "The user sent a voice note. The selected model transport does not support native audio, so the intelligence backend produced this paid transcription:",
        "",
        content,
      ].join("\n");
    }
    this.busy = true; this.transcript.push(transcriptItem);
    this.pendingTurn = true; this.pendingCheckpointed = false;
    this.pendingExternalEventId = externalEventId;
    this.chatend.retained = this.retainedTranscript();
    this.chatend.append({ role: "user", content: chatendContent });
    this.executor.resetLoadCalls(); this.onUpdate();
    try {
      await this.persistSnapshot(this.snapshot(), { userActivity: true });
      this.pendingCheckpointed = true;
      return await this.runPendingTurn();
    } finally { this.busy = false; this.onUpdate(); }
  }

  answerForExternalEvent(id) {
    return [...this.transcript].reverse().find(item => item.role === "kennedy" && item.externalEventId === id) || null;
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
