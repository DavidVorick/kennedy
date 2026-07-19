import { newIdempotencyId } from "./api.js?v=20260718.3";
import { runHistoryIngress } from "./history_ingress.js?v=20260719.1";

const INGRESS_FAILURE_LIMIT = 5;

function ingressMetrics(state, live = null) {
  const archived = state?.historyIngress;
  const usage = live?.usage?.snapshot?.() || archived?.usage || null;
  const roundCandidates = [live?.roundsUsed, archived?.roundsUsed].filter(Number.isInteger);
  return {
    rounds_used: roundCandidates.length ? Math.max(...roundCandidates) : null,
    context_tokens: Number.isFinite(usage?.contextTokens) ? usage.contextTokens : null,
    context_window_tokens: Number.isFinite(usage?.contextWindowTokens) ? usage.contextWindowTokens : null,
  };
}

export function selectNextMemoryIngress(conversation, audio) {
  if (!conversation && !audio) return null;
  if (conversation?.phase === "ingress_in_progress") return { kind: "conversation", record: conversation };
  if (audio?.phase === "ingress_in_progress") return { kind: "audio", record: audio };
  if (!conversation) return { kind: "audio", record: audio };
  if (!audio) return { kind: "conversation", record: conversation };
  return String(audio.source_created_at).localeCompare(String(conversation.started_at)) < 0
    ? { kind: "audio", record: audio }
    : { kind: "conversation", record: conversation };
}

export class MemoryIngressCoordinator {
  constructor({
    kweb,
    intelligence,
    rustLibs = null,
    conversationHistory,
    audioIngress,
    telegramRelay,
    getRuntime,
    isReady,
    rootsForRecord,
    referencesForRecord,
    groupContextOf,
    upsertHistory,
    refreshHistory,
    refreshAudioHistory,
    onUpdate = () => {},
    onError = () => {},
    onStatus = () => {},
    isConversationPurging = () => false,
    isConversationPurged = () => false,
    locks = globalThis.navigator?.locks,
    schedule = (callback, delay) => setTimeout(callback, delay),
  }) {
    this.kweb = kweb;
    this.intelligence = intelligence;
    this.rustLibs = rustLibs;
    this.conversationHistory = conversationHistory;
    this.audioIngress = audioIngress;
    this.telegramRelay = telegramRelay;
    this.getRuntime = getRuntime;
    this.isReady = isReady;
    this.rootsForRecord = rootsForRecord;
    this.referencesForRecord = referencesForRecord;
    this.groupContextOf = groupContextOf;
    this.upsertHistory = upsertHistory;
    this.refreshHistory = refreshHistory;
    this.refreshAudioHistory = refreshAudioHistory;
    this.onUpdate = onUpdate;
    this.onError = onError;
    this.onStatus = onStatus;
    this.isConversationPurging = isConversationPurging;
    this.isConversationPurged = isConversationPurged;
    this.locks = locks;
    this.schedule = schedule;
    this.running = false;
    this.activeRecord = null;
    this.activeAudioPiece = null;
    this.diagnostic = null;
    this.activeConversationTurn = null;
  }

  clearConversation(id) {
    if (this.activeRecord?.id === id) {
      this.activeRecord = null;
      this.diagnostic = null;
      this.onUpdate();
    }
  }

  async cancelConversation(id) {
    const turn = this.activeConversationTurn;
    if (turn?.recordId !== id || turn.controller.signal.aborted) return;
    turn.controller.abort();
    await Promise.resolve(this.intelligence.cancelOperation(turn.operationId)).catch(() => null);
  }

  async releaseRustLibs(sessionId) {
    if (!this.rustLibs) return;
    await this.rustLibs.release(sessionId).catch(() => {});
  }

  async nextWork() {
    const runtime = this.getRuntime();
    const [conversationResult, audioResult] = await Promise.all([
      runtime.conversationIngressReady
        ? this.conversationHistory.nextIngress().catch(error => {
          this.onError(`Conversation memory queue is temporarily unavailable: ${error.message}`);
          return { conversation: null };
        })
        : { conversation: null },
      runtime.audioIngressReady
        ? this.audioIngress.nextIngress().catch(error => {
          this.onError(`Audio memory queue is temporarily unavailable: ${error.message}`);
          return { piece: null };
        })
        : { piece: null },
    ]);
    return selectNextMemoryIngress(conversationResult.conversation, audioResult.piece);
  }

  async processAudioPiece(initialPiece) {
    let piece = initialPiece;
    const rustLibSessionId = `kennedy:audio-ingress:${piece.id}`;
    let stage = "prepare";
    let liveDiagnostic = null;
    this.activeAudioPiece = piece;
    this.diagnostic = null;
    try {
      if (piece.phase === "ingress_pending") {
        stage = "provenance";
        const provenance = await this.kweb.createProvenance({
          idempotency_id: newIdempotencyId(),
          data: [
            "Vnote final transcript piece",
            "",
            `Recording began: ${piece.source_created_at}`,
            `Recording SHA-256: ${piece.sha256}`,
            `Original filename: ${piece.original_filename}`,
            `Transcript piece: ${piece.piece_index + 1} of ${piece.piece_count}`,
            "",
            piece.transcript_text,
          ].join("\n"),
          source: "audio-vnote",
          source_created_at: piece.source_created_at,
        });
        stage = "claim";
        try {
          piece = await this.audioIngress.ingressStarted(piece.id, {
            expected_version: piece.version,
            provenance_id: provenance.id,
            completion_protocol: "end-turn-v1",
          });
          this.activeAudioPiece = piece;
        } catch (error) {
          if (error.code === "state_conflict") {
            this.activeAudioPiece = null;
            return;
          }
          throw error;
        }
      }
      if (piece.phase !== "ingress_in_progress") {
        this.activeAudioPiece = null;
        return;
      }
      stage = "model_loop";
      if (!piece.provenance_id) throw new Error("The queued audio transcript is missing its provenance.");
      const persistIngress = async archive => {
        const state = { ...piece.state, historyIngress: archive };
        try {
          piece = await this.audioIngress.ingressCheckpoint(piece.id, {
            expected_version: piece.version,
            state,
          });
          this.activeAudioPiece = piece;
        } catch (error) {
          if (error.code !== "state_conflict") throw error;
          const latest = await this.audioIngress.getPiece(piece.id);
          if (latest.phase !== "ingress_in_progress" || JSON.stringify(latest.state) !== JSON.stringify(state)) throw error;
          piece = latest;
        }
      };
      const runtime = this.getRuntime();
      await runHistoryIngress({
        kweb: this.kweb,
        intelligence: this.intelligence,
        rustLibs: this.rustLibs,
        toolSessionId: rustLibSessionId,
        manuals: runtime.manuals,
        rootNodeIds: runtime.rootNodeIds,
        provenanceId: piece.provenance_id,
        provider: runtime.provider,
        providerKind: runtime.providerKind,
        model: runtime.model,
        reasoningEffort: runtime.reasoningEffort,
        contextWindowTokens: runtime.contextWindowTokens,
        maxInputTokens: runtime.maxInputTokens,
        sourceSessionType: "audio",
        restoredArchive: piece.state?.historyIngress,
        checkpoint: persistIngress,
        onUpdate: value => {
          liveDiagnostic = value;
          this.diagnostic = value;
          this.onUpdate();
        },
      });
      this.diagnostic = null;
      stage = "completion";
      await this.audioIngress.ingressCompleted(piece.id, { expected_version: piece.version });
      await this.releaseRustLibs(rustLibSessionId);
      this.activeAudioPiece = null;
      await this.refreshAudioHistory();
    } catch (error) {
      const latest = await this.audioIngress.getPiece(piece.id);
      if (!["ingress_pending", "ingress_in_progress"].includes(latest.phase)) {
        await this.releaseRustLibs(rustLibSessionId);
        this.activeAudioPiece = null;
        return;
      }
      const failed = await this.audioIngress.ingressFailure(latest.id, {
        expected_version: latest.version,
        stage,
        code: typeof error?.code === "string" ? error.code : "ingress_error",
        message: typeof error?.message === "string" ? error.message : "Audio ingress failed without an error message.",
        ...ingressMetrics(latest.state, liveDiagnostic),
      });
      console.error("Audio ingress attempt failed", {
        recordingId: failed.recording_id,
        piece: failed.piece_index,
        stage,
        attempt: failed.ingress_failure_count,
        terminal: failed.phase === "ingress_failed",
        error,
      });
      this.diagnostic = null;
      this.activeAudioPiece = null;
      await this.refreshAudioHistory();
      if (failed.phase === "ingress_failed") {
        await this.releaseRustLibs(rustLibSessionId);
        this.onStatus("Audio memory ingestion failed");
        this.onError(`Audio transcript ingress stopped after ${failed.ingress_failure_count} failed attempts. Recording ${failed.recording_id} remains preserved for inspection.`);
      }
    }
  }

  async recordConversationFailure(record, error, stage) {
    let latest;
    try {
      latest = await this.conversationHistory.get(record.id);
    } catch (fetchError) {
      if (fetchError.code === "not_found") return null;
      throw fetchError;
    }
    if (!["ingress_pending", "ingress_in_progress"].includes(latest.phase)) return latest;
    const live = this.activeRecord?.id === latest.id ? this.diagnostic : null;
    return this.conversationHistory.ingressFailure(latest.id, {
      expected_version: latest.version,
      stage,
      code: typeof error?.code === "string" ? error.code : "ingress_error",
      message: typeof error?.message === "string" ? error.message : "History ingress failed without an error message.",
      ...ingressMetrics(latest.state, live),
    });
  }

  async processConversation(initialRecord) {
    let record = initialRecord;
    const rustLibSessionId = `kennedy:history-ingress:${record.id}`;
    let stage = "prepare";
    this.activeRecord = record;
    this.diagnostic = null;
    this.upsertHistory(record);
    this.onUpdate();
    try {
      if (record.phase === "ingress_pending") {
        const archive = record.state?.archive;
        if (archive?.format !== "kennedy-chatend") throw new Error("The queued conversation is missing its durable Chatend archive.");
        const source = archive.sessionType === "telegram-group"
          ? "telegram-group"
          : archive.sessionType === "telegram" ? "telegram"
            : archive.sessionType === "free-time" ? "free-time" : "conversation";
        stage = "provenance";
        const provenance = await this.kweb.createProvenanceArchive({
          idempotency_id: newIdempotencyId(),
          archive,
          source,
          source_created_at: record.started_at,
        });
        stage = "claim";
        try {
          record = await this.conversationHistory.ingressStarted(record.id, {
            expected_version: record.version,
            provenance_id: provenance.id,
            completion_protocol: "end-turn-v1",
          });
        } catch (error) {
          if (error.code === "state_conflict") {
            await this.refreshHistory();
            return;
          }
          throw error;
        }
        this.activeRecord = record;
        this.upsertHistory(record);
        this.onUpdate();
      }
      if (record.phase !== "ingress_in_progress") return;
      stage = "model_loop";
      if (!record.provenance_id) throw new Error("The queued conversation is missing its history provenance.");
      const persistIngress = async archive => {
        const state = { ...record.state, historyIngress: archive };
        try {
          record = await this.conversationHistory.ingressCheckpoint(record.id, {
            expected_version: record.version,
            state,
          });
        } catch (error) {
          if (error.code !== "state_conflict") throw error;
          const latest = await this.conversationHistory.get(record.id);
          if (latest.phase !== "ingress_in_progress" || JSON.stringify(latest.state) !== JSON.stringify(state)) throw error;
          record = latest;
        }
        this.activeRecord = record;
        this.upsertHistory(record);
        this.onUpdate();
      };
      const turn = {
        recordId: record.id,
        controller: new AbortController(),
        operationId: crypto.randomUUID(),
      };
      this.activeConversationTurn = turn;
      const runtime = this.getRuntime();
      try {
        await runHistoryIngress({
          kweb: this.kweb,
          intelligence: this.intelligence,
          rustLibs: this.rustLibs,
          toolSessionId: rustLibSessionId,
          manuals: runtime.manuals,
          rootNodeIds: this.rootsForRecord(record),
          referenceRootNodeIds: this.referencesForRecord(record),
          groupContext: this.groupContextOf(record),
          provenanceId: record.provenance_id,
          provider: runtime.provider,
          providerKind: runtime.providerKind,
          model: runtime.model,
          reasoningEffort: runtime.reasoningEffort,
          contextWindowTokens: runtime.contextWindowTokens,
          maxInputTokens: runtime.maxInputTokens,
          sourceSessionType: record.state?.archive?.sessionType || "conversation",
          restoredArchive: record.state?.historyIngress,
          checkpoint: persistIngress,
          onUpdate: value => {
            this.diagnostic = value;
            this.onUpdate();
          },
          signal: turn.controller.signal,
          operationId: turn.operationId,
          beforeMutation: async () => {
            let latest;
            try {
              latest = await this.conversationHistory.get(record.id);
            } catch (error) {
              if (error.code !== "not_found") throw error;
              throw Object.assign(new Error("This conversation was purged before the Kmap mutation."), { code: "ingress_cancelled" });
            }
            if (latest.phase !== "ingress_in_progress") {
              throw Object.assign(new Error("This conversation is no longer approved for history ingress."), { code: "ingress_cancelled" });
            }
          },
        });
      } finally {
        if (this.activeConversationTurn === turn) this.activeConversationTurn = null;
      }
      this.diagnostic = null;
      stage = "completion";
      record = await this.conversationHistory.ingressCompleted(record.id, {
        expected_version: record.version,
      });
      await this.releaseRustLibs(rustLibSessionId);
      this.upsertHistory(record);
      const groupIngressBatchId = record.state?.channel?.groupIngressBatchId
        || record.state?.archive?.channel?.groupIngressBatchId;
      if (groupIngressBatchId) await this.telegramRelay.completeGroupIngress(groupIngressBatchId);
      this.activeRecord = null;
      await this.refreshHistory();
    } catch (error) {
      if (this.isConversationPurging(record.id) || this.isConversationPurged(record.id)) {
        await this.releaseRustLibs(rustLibSessionId);
        this.diagnostic = null;
        this.activeRecord = null;
        return;
      }
      const failedRecord = await this.recordConversationFailure(record, error, stage);
      if (!failedRecord) {
        await this.releaseRustLibs(rustLibSessionId);
        this.diagnostic = null;
        this.activeRecord = null;
        await this.refreshHistory();
        return;
      }
      this.upsertHistory(failedRecord);
      console.error("History ingress attempt failed", {
        conversationId: record.id,
        stage,
        attempt: failedRecord.ingress_failure_count,
        limit: INGRESS_FAILURE_LIMIT,
        terminal: failedRecord.phase === "ingress_failed",
        error,
      });
      this.diagnostic = null;
      this.activeRecord = null;
      if (failedRecord.phase === "ingress_failed") {
        await this.releaseRustLibs(rustLibSessionId);
        this.onStatus("Memory ingestion failed");
        this.onError(`History ingress stopped after ${failedRecord.ingress_failure_count} failed attempts. Select the conversation to inspect its failure log.`);
      }
      await this.refreshHistory();
    }
  }

  async processQueue() {
    while (true) {
      const work = await this.nextWork();
      if (!work) return;
      if (work.kind === "audio") {
        this.activeRecord = null;
        await this.processAudioPiece(work.record);
      } else {
        await this.processConversation(work.record);
      }
    }
  }

  kick() {
    if (this.running || !this.isReady()) return;
    this.running = true;
    const run = () => this.processQueue();
    const work = this.locks?.request
      ? this.locks.request("kennedy-history-ingress", run)
      : run();
    Promise.resolve(work).catch(error => {
      this.onStatus("Memory ingestion needs attention");
      this.onError(`History ingress will retry: ${error.message}`);
    }).finally(() => {
      this.running = false;
      this.activeRecord = null;
      this.activeAudioPiece = null;
      this.onUpdate();
      this.schedule(() => this.kick(), 5000);
    });
  }
}
