import { Chatend } from "./chatend.js?v=20260717.6";
import { KwebContext } from "./kweb_context.js?v=20260717.7";
import { composePrompt, formatModelAttribution, formatTelegramGroupContext } from "./prompt_composer.js?v=20260717.9";
import { ToolExecutor } from "./tools.js?v=20260717.9";
import { AGENT_LOOP_SESSION_ENDED, ContinuationState, UsageTracker, createCacheKey, runAgentLoop } from "./intelligence.js?v=20260717.9";
import { addTimingStep, createTurnTiming, elapsedMs, formatDuration, updateTimingSummary } from "./timing.js?v=20260715.2";
import { freeTimeCanStartNewSession, freeTimeExpiredMessage, freeTimeNoAnswerContinuationMessage, freeTimeOpeningMessage, freeTimeRequestTimeoutSeconds, freeTimeScheduleText, freeTimeTiming, freeTimeTurnContinuationMessage, freeTimeWarningMessage, formatFreeTimeRemaining } from "./self_time.js?v=20260717.2";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function transcriptEndsWithUnansweredUser(transcript) {
  return Array.isArray(transcript) && transcript.at(-1)?.role === "user";
}

function turnStoppedError() {
  return Object.assign(
    new Error("Kennedy's response was stopped. The saved query is ready to retry."),
    { code: "turn_stopped" },
  );
}

export class ConversationSession {
  constructor({ kweb, intelligence, manuals, rootNodeIds, rootNodeId, referenceRootNodeIds = [], provider, providerKind, model, reasoningEffort, contextWindowTokens = 0, maxInputTokens = 0, sessionType = "conversation", channel = null, freeTime = null, provenanceId = null, persist = async () => {}, onUpdate = () => {}, now = () => Date.now() }) {
    this.kweb = kweb; this.intelligence = intelligence; this.manuals = manuals;
    this.rootNodeIds = rootNodeIds || [rootNodeId]; this.rootNodeId = this.rootNodeIds[0];
    this.referenceRootNodeIds = [...new Set(referenceRootNodeIds.filter(id => typeof id === "string" && id && !this.rootNodeIds.includes(id)))];
    this.provider = provider; this.providerKind = providerKind; this.model = model; this.reasoningEffort = reasoningEffort;
    this.modelAttribution = formatModelAttribution(model, reasoningEffort);
    if (!["conversation", "telegram", "telegram-group", "free-time"].includes(sessionType)) throw new Error("Unsupported Kennedy session type.");
    this.sessionType = sessionType;
    this.channel = channel ? jsonCopy(channel) : null;
    this.freeTime = freeTime ? jsonCopy(freeTime) : null;
    this.provenanceId = provenanceId;
    this.now = now;
    this.freeTimeEndReason = null;
    this.persist = persist; this.onUpdate = onUpdate;
    this.transcript = []; this.media = []; this.startedAt = new Date().toISOString(); this.pendingTurn = false; this.pendingCheckpointed = false; this.pendingExternalEventId = null; this.lastContextWarningBand = 0; this.busy = false; this.stopping = false; this.activeTurn = null;
    this.continuation = new ContinuationState(createCacheKey(sessionType === "free-time" ? "free-time" : "conversation"));
    this.usage = new UsageTracker({ contextWindowTokens, maxInputTokens });
  }

  async initialize(restored = null) {
    const archive = restored?.archive?.format === "kennedy-chatend" ? restored.archive : null;
    if (restored) {
      const savedReferences = restored.referenceRootNodeIds || archive?.referenceRootNodeIds;
      if (!this.referenceRootNodeIds.length && Array.isArray(savedReferences)) {
        this.referenceRootNodeIds = [...new Set(savedReferences.filter(id => typeof id === "string" && id && !this.rootNodeIds.includes(id)))];
      }
      const savedTranscript = Array.isArray(restored.transcript) ? restored.transcript : archive?.transcript;
      this.transcript = Array.isArray(savedTranscript) ? jsonCopy(savedTranscript) : [];
      this.startedAt = restored.startedAt || archive?.startedAt || this.startedAt;
      this.sessionType = restored.sessionType || archive?.sessionType || this.sessionType;
      this.channel = jsonCopy(restored.channel || archive?.channel || this.channel);
      this.freeTime = jsonCopy(restored.freeTime || archive?.freeTime || this.freeTime);
      this.provenanceId = restored.provenanceId || archive?.provenanceId || this.provenanceId;
      this.media = jsonCopy(restored.media || archive?.media || []);
      this.pendingTurn = Boolean(restored.pendingTurn) || (!this.freeTime?.sliceEndedAt && transcriptEndsWithUnansweredUser(this.transcript));
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
    for (const durableId of this.referenceRootNodeIds) this.context.registerReference(durableId);
    const sessionContext = this.sessionType === "telegram-group"
      ? formatTelegramGroupContext(this.channel?.groupContext, this.context)
      : this.sessionType === "free-time" ? freeTimeScheduleText(this.freeTime, this.now()) : "";
    this.chatend = new Chatend(composePrompt(this.manuals, "conversation", { providerKind: this.providerKind, model: this.model, reasoningEffort: this.reasoningEffort, sessionType: this.sessionType, sessionContext }), this.context, this.retainedTranscript());
    if (Array.isArray(archive?.messages)) {
      this.chatend.restoreMessages(
        jsonCopy(archive.messages),
        Array.isArray(archive.retained) ? jsonCopy(archive.retained) : this.retainedTranscript(),
      );
    }
    this.chatend.restoreFullHistory(archive?.fullHistory?.segments);
    this.executor = new ToolExecutor({
      mode: this.sessionType === "free-time" ? "free-time" : "conversation",
      context: this.context,
      api: this.kweb,
      intelligence: this.intelligence,
      provider: this.provider,
      model: this.model,
      modelAttribution: this.modelAttribution,
      provenanceId: this.provenanceId,
      loadLimit: this.sessionType === "free-time" ? 50 : 20,
      sessionType: this.sessionType,
      onUpdate: this.onUpdate,
      beforeMutation: () => this.assertFreeTimeToolAllowed("Kmap write"),
      toolGate: name => this.assertFreeTimeToolAllowed(name),
      endSession: message => this.requestFreeTimeSessionEnd(message),
      requestTimeoutSeconds: () => this.freeTimeRequestTimeoutSeconds(),
    });
    if (archive?.tools) {
      this.executor.loadCalls = Number.isInteger(archive.tools.loadCalls) ? archive.tools.loadCalls : 0;
      this.executor.toolLog = Array.isArray(archive.tools.log) ? jsonCopy(archive.tools.log) : [];
    }
    this.usage.restore(archive?.usage);
    if (archive) this.usage.resetThread();
    this.durableState = this.snapshot();
    this.onUpdate();
  }

  retainedTranscript() { return this.transcript.map(item => ({ role: item.role === "kennedy" ? "assistant" : "user", content: item.content })); }

  snapshot() {
    return {
      stateVersion: 2,
      sessionType: this.sessionType,
      channel: jsonCopy(this.channel),
      freeTime: jsonCopy(this.freeTime),
      provenanceId: this.provenanceId,
      rootNodeIds: [...this.rootNodeIds],
      referenceRootNodeIds: [...this.referenceRootNodeIds],
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
      freeTime: jsonCopy(this.freeTime),
      provenanceId: this.provenanceId,
      rootNodeIds: [...this.rootNodeIds],
      referenceRootNodeIds: [...this.referenceRootNodeIds],
      startedAt: this.startedAt,
      provider: this.provider,
      model: this.model,
      systemPrompt: this.chatend?.systemPrompt || "",
      retained: jsonCopy(this.chatend?.retained || []),
      transcript: jsonCopy(this.transcript),
      messages: jsonCopy(this.chatend?.messages || []),
      fullHistory: this.chatend?.fullHistorySnapshot() || { segments: [] },
      context: {
        snapshot: jsonCopy(this.context?.snapshot() || { directlyLoadedIdentifiers: [], nodes: [] }),
        diagnostics: jsonCopy(this.context?.diagnostics() || {}),
        state: jsonCopy(this.context?.archive() || {}),
      },
      tools: {
        loadCalls: this.executor?.loadCalls || 0,
        loadLimit: this.executor?.loadLimit || (this.sessionType === "free-time" ? 50 : 20),
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
    this.freeTime = jsonCopy(state.freeTime || archive.freeTime || this.freeTime);
    this.pendingTurn = Boolean(state.pendingTurn) || (!this.freeTime?.sliceEndedAt && transcriptEndsWithUnansweredUser(this.transcript));
    this.pendingExternalEventId = state.pendingExternalEventId || archive.pendingExternalEventId || null;
    this.lastContextWarningBand = Number(state.lastContextWarningBand ?? archive.lastContextWarningBand) || 0;
    this.media = jsonCopy(state.media || archive.media || []);
    this.channel = jsonCopy(state.channel || archive.channel || this.channel);
    this.provenanceId = state.provenanceId || archive.provenanceId || this.provenanceId;
    this.referenceRootNodeIds = [...new Set((state.referenceRootNodeIds || archive.referenceRootNodeIds || this.referenceRootNodeIds)
      .filter(id => typeof id === "string" && id && !this.rootNodeIds.includes(id)))];
    this.pendingCheckpointed = this.pendingTurn;
    this.chatend.restoreMessages(jsonCopy(archive.messages), jsonCopy(archive.retained || this.retainedTranscript()));
    this.chatend.restoreFullHistory(archive.fullHistory?.segments);
    this.context.restore(archive.context.state);
    this.executor.loadCalls = Number.isInteger(archive.tools?.loadCalls) ? archive.tools.loadCalls : 0;
    this.executor.toolLog = jsonCopy(archive.tools?.log || []);
    this.executor.provenanceId = this.provenanceId;
    this.usage.restore(archive.usage);
  }

  refreshTelegramGroupContext(groupContext, currentMessageId = null) {
    if (this.sessionType !== "telegram-group" || !groupContext) return;
    const previousMessageId = Number(this.channel?.lastGroupContextMessageId) || 0;
    const messages = Array.isArray(groupContext.messages) ? groupContext.messages : [];
    const newestMessageId = messages.reduce(
      (latest, message) => Math.max(latest, Number(message.messageId) || 0),
      Math.max(previousMessageId, Number(groupContext.throughMessageId) || 0),
    );
    const unseenMessages = messages.filter(message => {
      const messageId = Number(message.messageId) || 0;
      return messageId > previousMessageId && String(message.messageId) !== String(currentMessageId);
    });
    this.channel = {
      ...(this.channel || {}),
      username: groupContext.invokingUsername || this.channel?.username || null,
      groupContext: jsonCopy({
        ...groupContext,
        messages: messages.filter(message => String(message.messageId) !== String(currentMessageId)),
      }),
      lastGroupContextMessageId: newestMessageId,
    };
    for (const participant of groupContext.participants || []) {
      const rootNodeId = participant?.rootNodeId;
      if (typeof rootNodeId !== "string" || !rootNodeId || this.rootNodeIds.includes(rootNodeId)) continue;
      if (!this.referenceRootNodeIds.includes(rootNodeId)) this.referenceRootNodeIds.push(rootNodeId);
      this.context.registerReference(rootNodeId);
    }
    for (const message of unseenMessages) {
      if (!message?.mediaRef) continue;
      const mediaId = `telegram-group:${groupContext.chatId}:${message.messageId}`;
      if (this.media.some(item => item.id === mediaId)) continue;
      this.media.push({ id: mediaId, ...jsonCopy(message.mediaRef) });
    }
    if (!unseenMessages.length) return;
    const update = formatTelegramGroupContext({ ...groupContext, messages: unseenMessages }, this.context);
    const content = `Updated Telegram group context since this user's previous invocation:\n\n${update}`;
    this.chatend.retained.push({ role: "user", content });
    this.chatend.append({ role: "user", content });
  }

  reportTurnTiming(timing, status) {
    if (!timing || timing.reported) return;
    timing.reported = true;
    const report = this.intelligence?.recordTiming?.({
      action: "turn",
      status,
      sessionType: this.sessionType,
      durationMs: timing.totalDurationMs ?? elapsedMs(timing.startedAt),
      llmDurationMs: timing.llmDurationMs,
      toolDurationMs: timing.toolDurationMs,
      stepCount: timing.steps.length,
    });
    Promise.resolve(report).catch(() => {});
  }

  beginTurn() {
    const turn = {
      controller: new AbortController(),
      operationId: crypto.randomUUID(),
      cancellable: true,
    };
    this.activeTurn = turn;
    this.stopping = false;
    this.busy = true;
    this.onUpdate();
    return turn;
  }

  finishTurn(turn) {
    if (this.activeTurn === turn) this.activeTurn = null;
    this.stopping = false;
    this.busy = false;
    this.onUpdate();
  }

  get canStop() {
    return Boolean(this.busy && this.activeTurn?.cancellable);
  }

  async stopPendingTurn() {
    const turn = this.activeTurn;
    if (!this.busy || !turn?.cancellable || this.stopping) return false;
    this.stopping = true;
    this.onUpdate();
    const cancellation = Promise.resolve(this.intelligence?.cancelOperation?.(turn.operationId)).catch(() => null);
    turn.controller.abort();
    await cancellation;
    return true;
  }

  async runPendingTurn(timing = createTurnTiming(this.sessionType), turn = this.activeTurn) {
    if (!this.pendingTurn) return null;
    try {
      const answer = await runAgentLoop({
        intelligence: this.intelligence, provider: this.provider, model: this.model,
        chatend: this.chatend, executor: this.executor, continuation: this.continuation,
        usage: this.usage, timing, onUpdate: this.onUpdate,
        checkpoint: () => this.persistSnapshot(),
        signal: turn?.controller.signal,
        operationId: turn?.operationId,
        onRoundStart: () => this.prepareFreeTimeRound(),
        onResponse: () => this.prepareFreeTimeRound(),
        onFinal: () => this.freeTimeContinuationDirective("final"),
        onNoAnswer: () => this.freeTimeContinuationDirective("no-answer"),
        requestTimeoutSeconds: () => this.freeTimeRequestTimeoutSeconds(),
      });
      if (turn?.controller.signal.aborted) throw turnStoppedError();
      turn.cancellable = false;
      this.onUpdate();
      if (answer === AGENT_LOOP_SESSION_ENDED) {
        if (typeof this.executor.sessionEndContent === "string" && this.executor.sessionEndContent.trim()) {
          this.transcript.push({ role: "kennedy", content: this.executor.sessionEndContent });
          this.chatend.retained.push({ role: "assistant", content: this.executor.sessionEndContent });
          this.executor.sessionEndContent = null;
        }
        this.pendingTurn = false;
        this.pendingExternalEventId = null;
        this.pendingCheckpointed = false;
        const finalSaveStarted = performance.now();
        await this.persistSnapshot(this.snapshot());
        addTimingStep(timing, "checkpoint", "Self-time session save", elapsedMs(finalSaveStarted));
        updateTimingSummary(timing);
        this.reportTurnTiming(timing, "ok");
        this.onUpdate();
        return answer;
      }
      const response = { role: "kennedy", content: answer };
      if (this.pendingExternalEventId) response.externalEventId = this.pendingExternalEventId;
      const usage = this.usage.snapshot();
      if (["telegram", "telegram-group"].includes(this.sessionType) && usage.contextWindowTokens) {
        const band = Math.floor(usage.contextTokens / 100000);
        if (band > this.lastContextWarningBand) {
          if (this.sessionType === "telegram-group") {
            const username = String(this.channel?.username || "").replace(/^@/, "");
            const identity = username
              ? `@${username}`
              : `${this.channel?.displayName || "This participant"} (Telegram user ${this.channel?.telegramUserId})`;
            response.contextWarning = `${identity}, your Kennedy session in this group is using ${usage.contextTokens.toLocaleString("en-US")} out of ${usage.contextWindowTokens.toLocaleString("en-US")} context tokens. This applies only to ${identity}; other members have separate sessions. Use /reset to begin a new session.`;
          } else {
            response.contextWarning = `${usage.contextTokens.toLocaleString("en-US")} out of ${usage.contextWindowTokens.toLocaleString("en-US")} context tokens used. Consider resetting with /reset.`;
          }
        }
        this.lastContextWarningBand = band;
      }
      this.transcript.push(response);
      this.chatend.retained.push({ role: "assistant", content: answer });
      this.pendingTurn = false;
      this.pendingExternalEventId = null;
      this.pendingCheckpointed = false;
      const finalSaveStarted = performance.now();
      await this.persistSnapshot(this.snapshot());
      addTimingStep(timing, "checkpoint", "Final response save", elapsedMs(finalSaveStarted));
      updateTimingSummary(timing);
      this.reportTurnTiming(timing, "ok");
      this.onUpdate();
      return answer;
    } catch (error) {
      const stopped = turn?.controller.signal.aborted
        || error?.name === "AbortError"
        || ["operation_cancelled", "turn_stopped"].includes(error?.code);
      this.restoreDurableState();
      this.continuation.reset();
      updateTimingSummary(timing);
      this.reportTurnTiming(timing, "error");
      throw stopped ? turnStoppedError() : error;
    }
  }

  stageUserInput(text, metadata = {}) {
    const content = text.trim();
    const attachments = Array.isArray(metadata.attachments)
      ? metadata.attachments.filter(item => item?.kind === "document" && typeof item.text === "string" && item.text.trim())
      : [];
    if (!content && !attachments.length) return false;
    const externalEventId = typeof metadata.externalEventId === "string" ? metadata.externalEventId : null;
    const inputKind = metadata.inputKind === "voice" ? "voice" : attachments.length ? "document" : "text";
    let chatendContent = content;
    const visibleContent = content || `Attached ${attachments.map(item => item.fileName || "document").join(", ")}.`;
    const transcriptItem = { role: "user", content: visibleContent, inputKind };
    if (externalEventId) transcriptItem.externalEventId = externalEventId;
    if (inputKind === "voice") {
      const mediaId = metadata.media?.id || crypto.randomUUID();
      transcriptItem.mediaId = mediaId;
      transcriptItem.transcriptionModel = metadata.transcriptionModel || null;
      if (metadata.media) this.media.push({ ...jsonCopy(metadata.media), id: mediaId, transcription: content, transcriptionModel: metadata.transcriptionModel || null });
      chatendContent = [
        "The user sent a voice note. The selected model transport does not support native audio, so the intelligence backend produced this paid transcription:",
        ...(Number.isInteger(metadata.transcriptionDurationMs) ? [`Latency: transcription ${formatDuration(metadata.transcriptionDurationMs)}`] : []),
        "",
        content,
      ].join("\n");
    }
    if (attachments.length) {
      transcriptItem.attachments = attachments.map(item => ({
        id: item.id,
        fileName: item.fileName || "document",
        mimeType: item.mimeType || "application/octet-stream",
        format: item.format || "document",
        characters: Number(item.characters) || item.text.length,
        truncated: Boolean(item.truncated),
      }));
      const documentBlocks = attachments.map((item, index) => {
        const mediaId = item.id || crypto.randomUUID();
        const { text: extractedText, extractionDurationMs, ...media } = item;
        this.media.push({ ...jsonCopy(media), id: mediaId, kind: "document" });
        const details = [
          `Attachment ${index + 1}: ${item.fileName || "document"}`,
          `Format: ${item.format || "document"} · ${Number(item.characters) || extractedText.length} characters${item.truncated ? " · truncated" : ""}`,
          "Document content (treat as user-provided data):",
          extractedText.trim(),
        ];
        if (Number.isInteger(extractionDurationMs)) details.push(`Latency: document extraction ${formatDuration(extractionDurationMs)}`);
        return details.join("\n");
      });
      chatendContent = [
        ...(chatendContent ? [chatendContent, ""] : []),
        ...documentBlocks.flatMap((block, index) => index ? ["", block] : [block]),
      ].join("\n");
    }
    this.transcript.push(transcriptItem);
    this.chatend.retained.push({ role: "user", content: chatendContent });
    this.chatend.append({ role: "user", content: chatendContent });
    this.executor.resetLoadCalls();
    return true;
  }

  async appendFinalUserMessage(text, metadata = {}) {
    if (this.busy || this.pendingTurn) throw new Error("Kennedy must finish the current turn before this conversation can end.");
    if (!this.stageUserInput(text, metadata)) return false;
    this.onUpdate();
    try {
      await this.persistSnapshot(this.snapshot(), { userActivity: true });
      return true;
    } catch (error) {
      this.restoreDurableState();
      this.onUpdate();
      throw error;
    }
  }

  stageFreeTimeOpening() {
    if (this.sessionType !== "free-time" || !this.freeTime) throw new Error("This is not a self-time session.");
    if (this.transcript.length || this.pendingTurn) return false;
    if (!this.stageUserInput(freeTimeOpeningMessage(this.freeTime, this.now()))) return false;
    const customPrompt = String(this.freeTime.customPrompt || "").trim();
    if (customPrompt) this.stageUserInput(customPrompt);
    const handoffMessage = String(this.freeTime.handoffMessage || "");
    if (handoffMessage.trim()) {
      this.stageUserInput(`Message passed from the previous self time session:\n\n${handoffMessage}`);
    }
    this.pendingTurn = true;
    this.pendingCheckpointed = false;
    this.onUpdate();
    return true;
  }

  freeTimeRequestTimeoutSeconds() {
    if (this.sessionType !== "free-time") return null;
    return freeTimeRequestTimeoutSeconds(this.freeTime, this.now());
  }

  assertFreeTimeToolAllowed(name) {
    if (this.sessionType !== "free-time" || ["EndSelfTimeSession", "EndFreeTimeSession"].includes(name)) return;
    if (freeTimeTiming(this.freeTime, this.now()).expired) {
      throw Object.assign(new Error("The self-time deadline has passed; tools are no longer available during wrap-up."), { code: "free_time_expired" });
    }
  }

  requestFreeTimeSessionEnd(message = null) {
    if (this.sessionType !== "free-time") throw Object.assign(new Error("This is not a self-time session."), { code: "tool_unavailable" });
    this.freeTimeEndReason = "tool";
    const now = this.now();
    const remaining = freeTimeTiming(this.freeTime, now).remainingMs;
    const willContinue = freeTimeCanStartNewSession(this.freeTime, now);
    delete this.freeTime.nextSessionMessage;
    if (willContinue && message) this.freeTime.nextSessionMessage = message;
    return {
      sessionEnding: true,
      totalTimeReduced: false,
      remaining: formatFreeTimeRemaining(remaining),
      messageForwarded: Boolean(willContinue && message),
      next: willContinue
        ? "A new clean-slate self-time session will open with the same deadline."
        : remaining > 0
          ? "Less than five minutes remain, so the self-time run will end instead of opening another session."
          : "The shared self-time deadline has arrived, so the run will end.",
    };
  }

  appendFreeTimeTimerMessage(content) {
    const message = { role: "user", display_role: "Self time timer", context_kind: "free-time-timer", content };
    this.chatend.retained.push(jsonCopy(message));
    this.chatend.append(message);
  }

  freeTimeContinuationDirective(kind) {
    if (this.sessionType !== "free-time") return null;
    const content = kind === "no-answer"
      ? freeTimeNoAnswerContinuationMessage(this.freeTime, this.now())
      : freeTimeTurnContinuationMessage(this.freeTime, this.now());
    return {
      continueWith: {
        role: "user",
        display_role: "Self time controller",
        context_kind: "free-time-continuation",
        content,
      },
    };
  }

  async continueFreeTimeAfterUnexpectedCompletion() {
    if (this.sessionType !== "free-time" || this.pendingTurn || this.freeTimeEndReason) return false;
    delete this.freeTime.sliceEndedReason;
    delete this.freeTime.sliceEndedAt;
    this.chatend.append(this.freeTimeContinuationDirective("final").continueWith);
    this.pendingTurn = true;
    this.pendingCheckpointed = false;
    await this.persistSnapshot(this.snapshot());
    this.pendingCheckpointed = true;
    this.onUpdate();
    return true;
  }

  async prepareFreeTimeRound() {
    if (this.sessionType !== "free-time") return null;
    const timing = freeTimeTiming(this.freeTime, this.now());
    if (timing.expired) {
      if (!this.freeTime.expiredNoticeAt) {
        this.freeTime.expiredNoticeAt = new Date(this.now()).toISOString();
        this.appendFreeTimeTimerMessage(freeTimeExpiredMessage());
        await this.persistSnapshot(this.snapshot());
      }
      this.freeTimeEndReason = "deadline";
      return { endAfterResponse: true };
    }
    if (timing.warningDue && !this.freeTime.warningNoticeAt) {
      this.freeTime.warningNoticeAt = new Date(this.now()).toISOString();
      this.appendFreeTimeTimerMessage(freeTimeWarningMessage(this.freeTime, this.now()));
      await this.persistSnapshot(this.snapshot());
    }
    return null;
  }

  async finalizeFreeTime(reason = this.freeTimeEndReason) {
    if (this.sessionType !== "free-time") return;
    const timing = freeTimeTiming(this.freeTime, this.now());
    const explicitToolEnd = reason === "tool" && this.freeTimeEndReason === "tool";
    const timerEnd = ["deadline", "hard-stop"].includes(reason) && timing.expired;
    if (!explicitToolEnd && !timerEnd) {
      throw new Error("Self time can finalize only after EndSelfTimeSession or the shared deadline.");
    }
    if (timing.expired && !this.freeTime.expiredNoticeAt) {
      this.freeTime.expiredNoticeAt = new Date(this.now()).toISOString();
      this.appendFreeTimeTimerMessage(freeTimeExpiredMessage());
    }
    this.freeTimeEndReason = reason;
    this.freeTime.sliceEndedReason = reason;
    this.freeTime.sliceEndedAt = new Date(this.now()).toISOString();
    this.pendingTurn = false;
    this.pendingCheckpointed = false;
    this.pendingExternalEventId = null;
    await this.persistSnapshot(this.snapshot());
    this.onUpdate();
  }

  async send(text, metadata = {}) {
    if (this.pendingTurn) throw new Error("Kennedy must finish the saved pending query before accepting another message.");
    const externalEventId = typeof metadata.externalEventId === "string" ? metadata.externalEventId : null;
    if (externalEventId && this.transcript.some(item => item.externalEventId === externalEventId)) {
      return this.answerForExternalEvent(externalEventId)?.content || null;
    }
    if (!this.stageUserInput(text, metadata)) return null;
    const timing = createTurnTiming(this.sessionType);
    const turn = this.beginTurn();
    this.pendingTurn = true; this.pendingCheckpointed = false;
    this.pendingExternalEventId = externalEventId;
    this.onUpdate();
    try {
      const pendingSaveStarted = performance.now();
      await this.persistSnapshot(this.snapshot(), { userActivity: true });
      addTimingStep(timing, "checkpoint", "Pending turn save", elapsedMs(pendingSaveStarted));
      this.pendingCheckpointed = true;
      if (turn.controller.signal.aborted) throw turnStoppedError();
      return await this.runPendingTurn(timing, turn);
    } catch (error) {
      if (!timing.reported) {
        updateTimingSummary(timing);
        this.reportTurnTiming(timing, "error");
      }
      throw error;
    } finally { this.finishTurn(turn); }
  }

  answerForExternalEvent(id) {
    return [...this.transcript].reverse().find(item => item.role === "kennedy" && item.externalEventId === id) || null;
  }

  async resumePendingTurn() {
    if (!this.pendingTurn || this.busy) return null;
    const timing = createTurnTiming(this.sessionType);
    const turn = this.beginTurn();
    try {
      if (!this.pendingCheckpointed) {
        const pendingSaveStarted = performance.now();
        await this.persistSnapshot(this.snapshot(), { userActivity: true });
        addTimingStep(timing, "checkpoint", "Pending turn save", elapsedMs(pendingSaveStarted));
        this.pendingCheckpointed = true;
      }
      if (turn.controller.signal.aborted) throw turnStoppedError();
      return await this.runPendingTurn(timing, turn);
    }
    catch (error) {
      if (!timing.reported) {
        updateTimingSummary(timing);
        this.reportTurnTiming(timing, "error");
      }
      throw error;
    }
    finally { this.finishTurn(turn); }
  }

  serialize() { return JSON.stringify(this.archive(), null, 2); }
}
