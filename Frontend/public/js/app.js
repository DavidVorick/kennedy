import { KwebAPI, IntelligenceAPI, ConversationHistoryAPI, AudioIngressAPI, TelegramRelayAPI } from "./api.js?v=20260717.5";
import { loadPromptManuals, promptsReady } from "./prompt_composer.js?v=20260717.8";
import { ConversationSession } from "./conversation.js?v=20260717.10";
import { MemoryIngressCoordinator } from "./memory_ingress_coordinator.js?v=20260717.2";
import { MemoryExplorer } from "./memory_explorer.js?v=20260717.7";
import { renderTranscript, renderConversationHistory, renderAudioHistory, renderAudioRecording, conversationControlState, conversationIngressActivity, renderInspector, renderUsage, inspectorText, showError, clearError, sortConversationHistory, element } from "./render.js?v=20260717.8";
import { DEFAULT_FREE_TIME_MINUTES, FREE_TIME_HARD_STOP_GRACE_MS, FREE_TIME_WARNING_MS, formatFreeTimeRemaining, freeTimeTiming, parseFreeTimeMinutes } from "./free_time.js?v=20260717.1";

const CONFIG = {
  kwebBase: window.location.origin,
  intelligenceBase: "http://127.0.0.1:4322",
  conversationHistoryBase: "http://127.0.0.1:4323",
  telegramRelayBase: "http://127.0.0.1:4324",
  audioIngressBase: "http://127.0.0.1:4325",
  webUserHandle: "taek42",
};

const ui = Object.fromEntries([
  "service-status", "free-time-minutes", "start-free-time", "free-time-status", "chat-view", "memory-view", "chat-tab", "tg-tab", "audio-tab", "memory-tab", "transcript", "error-banner", "user-log-section", "clear-log", "message-form", "message-input", "message-resize-handle", "message-size-button", "send-button", "send-end-button", "stop-button", "voice-button", "attach-button", "attachment-input", "attachment-status", "clear-attachments", "end-button", "activity", "context-inspector", "copy-context", "usage-metrics", "inspector-main", "inspector-full", "inspector-history", "memory-content", "memory-back", "memory-forward", "memory-home", "memory-kennedy-home", "new-conversation", "conversation-history", "history-eyebrow", "history-title", "chatend-title",
].map(id => [id.replaceAll("-", "_"), document.getElementById(id)]));

const INSPECTOR_MODES = ["main", "full", "history"];
const kweb = KwebAPI(CONFIG.kwebBase);
const intelligence = IntelligenceAPI(CONFIG.intelligenceBase);
const conversationHistory = ConversationHistoryAPI(CONFIG.conversationHistoryBase);
const telegramRelay = TelegramRelayAPI(CONFIG.telegramRelayBase);
const audioIngress = AudioIngressAPI(CONFIG.audioIngressBase);

let manuals = {};
let rootNodeIds = null;
let legacyUserRootNodeId = null;
let kennedyRootNodeId = null;
let webDirectoryUser = null;
let provider = null;
let providerKind = null;
let model = null;
let reasoningEffort = null;
let contextWindowTokens = 0;
let maxInputTokens = 0;
let inputModalities = ["text"];
let transcriptionAvailable = false;
let explorer = null;
let historyRecords = [];
let selectedConversationId = null;
let selectedByView = { conversation: null, telegram: null };
let audioRecords = [];
let selectedAudioId = null;
let audioDetails = new Map();
let audioDetailLoading = new Set();
let audioDetailErrors = new Map();
let retryingAudioPieces = new Set();
let retryingAudioRecordings = new Set();
let retryingConversationIds = new Set();
let purgingConversationIds = new Set();
let purgedConversationIds = new Set();
let activeView = "conversation";
let liveSessions = new Map();
let drafts = new Map();
let conversationErrors = new Map();
let endingIds = new Set();
let creatingConversation = false;
let inspectorMode = "main";
let recorder = null;
let recorderChunks = [];
let recordingStream = null;
let voiceDrafts = new Map();
let attachmentDrafts = new Map();
let extractingAttachments = new Set();
let freeTimeRun = null;
let freeTimeStarting = false;
let freeTimeRunnerIds = new Set();
let freeTimeContinuationRuns = new Set();
let telegramBridgeRunning = false;
let telegramInFlight = new Set();
const telegramGroupPreparations = new Map();
let kwebReady = false;
let conversationHistoryReady = false;
let intelligenceReady = false;
let audioIngressReady = false;
let telegramRelayReady = false;

function conversationPromptsReady() {
  return promptsReady(manuals, "conversation", { providerKind });
}

function freeTimePromptsReady() {
  return promptsReady(manuals, "conversation", { providerKind, sessionType: "free-time" });
}

function historyPromptsReady() {
  return promptsReady(manuals, "ingress", { sourceSessionType: "conversation", providerKind });
}

function audioPromptsReady() {
  return promptsReady(manuals, "ingress", { sourceSessionType: "audio", providerKind });
}

function chatRuntimeReady() {
  return kwebReady && conversationHistoryReady && intelligenceReady && conversationPromptsReady();
}

function freeTimeRuntimeReady() {
  return kwebReady && conversationHistoryReady && intelligenceReady && freeTimePromptsReady();
}

function memoryIngressRuntimeReady() {
  return kwebReady && intelligenceReady && (
    (conversationHistoryReady && historyPromptsReady())
    || (audioIngressReady && audioPromptsReady())
  );
}

const memoryIngress = new MemoryIngressCoordinator({
  kweb,
  intelligence,
  conversationHistory,
  audioIngress,
  telegramRelay,
  getRuntime: () => ({
    manuals,
    rootNodeIds,
    provider,
    providerKind,
    model,
    reasoningEffort,
    contextWindowTokens,
    maxInputTokens,
    conversationIngressReady: conversationHistoryReady && historyPromptsReady(),
    audioIngressReady: audioIngressReady && audioPromptsReady(),
  }),
  isReady: memoryIngressRuntimeReady,
  rootsForRecord,
  referencesForRecord,
  groupContextOf,
  upsertHistory,
  refreshHistory,
  refreshAudioHistory: () => refreshAudioHistory(activeView === "audio"),
  onUpdate: update,
  onError: message => showError(ui.error_banner, message),
  onStatus: message => { ui.service_status.textContent = message; },
  isConversationPurging: id => purgingConversationIds.has(id),
  isConversationPurged: id => purgedConversationIds.has(id),
});

function kickHistoryIngress() {
  memoryIngress.kick();
}

function sessionTypeOf(record) {
  return record?.state?.sessionType || record?.state?.archive?.sessionType || "conversation";
}

function viewForSessionType(sessionType) {
  return String(sessionType).startsWith("telegram") ? "telegram" : "conversation";
}

function freeTimeOf(record) {
  return record?.state?.freeTime || record?.state?.archive?.freeTime || null;
}

function activeFreeTimeRecord() {
  return historyRecords.find(record => record.phase === "active" && sessionTypeOf(record) === "free-time") || null;
}

function renderFreeTimeControls() {
  const metadata = freeTimeRun || freeTimeOf(activeFreeTimeRecord());
  const active = Boolean(metadata);
  ui.free_time_minutes.disabled = active || freeTimeStarting;
  ui.start_free_time.disabled = active || freeTimeStarting || !freeTimeRuntimeReady();
  ui.start_free_time.textContent = freeTimeStarting ? "Starting…" : active ? "Running" : "Start";
  if (!metadata) {
    ui.free_time_status.textContent = "";
    return;
  }
  try {
    const timing = freeTimeTiming(metadata);
    ui.free_time_status.textContent = timing.expired
      ? `Wrapping up · hard stop in ${formatFreeTimeRemaining(timing.hardStopMs - Date.now())}`
      : `Session ${metadata.sliceIndex} · ${formatFreeTimeRemaining(timing.remainingMs)} left`;
  } catch {
    ui.free_time_status.textContent = "Schedule unavailable";
  }
}

function recordsForView(view = activeView) {
  const type = view === "telegram" ? "telegram" : "conversation";
  return sortConversationHistory(historyRecords.filter(record => viewForSessionType(sessionTypeOf(record)) === type));
}

function groupContextOf(record) {
  return record?.state?.channel?.groupContext
    || record?.state?.archive?.channel?.groupContext
    || record?.state?.historyIngress?.groupContext
    || null;
}

function channelOf(record) {
  return record?.state?.channel || record?.state?.archive?.channel || null;
}

function groupSessionMatches(record, event) {
  if (sessionTypeOf(record) !== "telegram-group") return false;
  const channel = channelOf(record);
  const sameUser = String(channel?.telegramUserId) === String(event.telegramUserId);
  const eventGroupRoot = event.groupRootNodeId || event.groupContext?.groupRootNodeId;
  const channelGroupRoot = channel?.groupRootNodeId || channel?.groupContext?.groupRootNodeId;
  const sameGroup = eventGroupRoot && channelGroupRoot
    ? eventGroupRoot === channelGroupRoot
    : String(channel?.chatId) === String(event.chatId);
  return sameUser && sameGroup;
}

function rootsForRecord(record) {
  const archived = record?.state?.archive;
  const saved = record?.state?.rootNodeIds || archived?.rootNodeIds;
  return Array.isArray(saved) && saved.length ? [...saved] : [...rootNodeIds];
}

function referencesForGroup(groupContext, directRoots) {
  if (!Array.isArray(groupContext?.participants)) return [];
  return [...new Set(groupContext.participants
    .map(participant => participant?.rootNodeId)
    .filter(id => typeof id === "string" && id && !directRoots.includes(id)))];
}

function referencesForRecord(record, directRoots = rootsForRecord(record)) {
  const archived = record?.state?.archive;
  const saved = record?.state?.referenceRootNodeIds || archived?.referenceRootNodeIds;
  return Array.isArray(saved) ? [...saved] : referencesForGroup(groupContextOf(record), directRoots);
}

async function provisionDirectoryRoots() {
  if (!kwebReady || !telegramRelayReady) return;
  const [pendingUsers, pendingGroups] = await Promise.all([
    telegramRelay.provisioningUsers(),
    telegramRelay.provisioningGroups(),
  ]);
  for (const entry of pendingUsers.users || []) {
    const isWebUser = String(entry.handle).toLowerCase() === CONFIG.webUserHandle.toLowerCase();
    const targetRoot = isWebUser ? legacyUserRootNodeId : entry.rootNodeId;
    if (!isWebUser) await kweb.bootstrapNode(targetRoot);
    await telegramRelay.completeHandleRoot(entry.handle, targetRoot);
  }
  for (const group of pendingGroups.groups || []) {
    await kweb.bootstrapNode(group.rootNodeId, "Group Root");
    await telegramRelay.completeGroupRoot(group.chatId, group.rootNodeId);
  }
  webDirectoryUser = await telegramRelay.userByHandle(CONFIG.webUserHandle);
  rootNodeIds = [webDirectoryUser.rootNodeId, kennedyRootNodeId];
}

async function directoryUserForEvent(event) {
  await provisionDirectoryRoots();
  const user = await telegramRelay.userById(event.telegramUserId);
  if (!user.rootReady) {
    await kweb.bootstrapNode(user.rootNodeId);
    return telegramRelay.completeUserRoot(user.telegramUserId, user.rootNodeId);
  }
  return user;
}

async function provisionGroupRoot(chatId) {
  await provisionDirectoryRoots();
  let group = await telegramRelay.groupById(chatId);
  if (!group.rootReady) {
    await kweb.bootstrapNode(group.rootNodeId, "Group Root");
    group = await telegramRelay.completeGroupRoot(group.chatId, group.rootNodeId);
  }
  return group;
}

async function directoryGroupForEvent(event) {
  return event.sessionKind === "group" ? provisionGroupRoot(event.chatId) : null;
}

function selectedRecord() {
  return historyRecords.find(record => record.id === selectedConversationId) || null;
}

function selectedSession() {
  return liveSessions.get(selectedConversationId) || null;
}

function selectedAudioDetail() {
  return audioDetails.get(selectedAudioId) || null;
}

function freshIngressState(state) {
  const fresh = { ...(state || {}) };
  delete fresh.historyIngress;
  return fresh;
}

const EMPTY_MEMORY = { directlyLoadedIdentifiers: [], nodes: [] };

function archivedDiagnostic(archive, mode, transcript = []) {
  return {
    mode, provider, model,
    chatend: archive?.messages || transcript.map(item => ({ role: item.role === "kennedy" ? "assistant" : "user", content: item.content })),
    context: archive?.context?.diagnostics || {},
    loadCalls: archive?.tools?.loadCalls || 0,
    loadLimit: archive?.tools?.loadLimit || 0,
    toolLog: archive?.tools?.log || [],
    usage: archive?.usage || null,
    memory: archive?.context?.snapshot || EMPTY_MEMORY,
    historySegments: archive?.fullHistory?.segments || [],
  };
}

function conversationDiagnostic(record, session) {
  if (session) {
    return {
      mode: session.sessionType === "free-time" ? "free time" : "conversation", provider, model,
      chatend: session.chatend?.messages || [],
      context: session.context?.diagnostics() || {},
      loadCalls: session.executor?.loadCalls || 0,
      loadLimit: session.executor?.loadLimit || 20,
      toolLog: session.executor?.toolLog || [],
      usage: session.usage?.snapshot() || null,
      memory: session.context?.snapshot() || EMPTY_MEMORY,
      historySegments: session.chatend?.historySegments || [],
    };
  }
  if (!record) return null;
  const transcript = Array.isArray(record.state?.transcript) ? record.state.transcript : [];
  const archive = record.state?.archive?.format === "kennedy-chatend" ? record.state.archive : null;
  return archivedDiagnostic(archive, "saved conversation", transcript);
}

function historyIngressDiagnostic(record) {
  if (record?.id === memoryIngress.activeRecord?.id && memoryIngress.diagnostic) {
    return {
      mode: "history ingress", provider, model,
      chatend: memoryIngress.diagnostic.chatend?.messages || [],
      context: memoryIngress.diagnostic.context?.diagnostics?.() || {},
      loadCalls: memoryIngress.diagnostic.executor?.loadCalls || 0,
      loadLimit: memoryIngress.diagnostic.executor?.loadLimit || 50,
      toolLog: memoryIngress.diagnostic.executor?.toolLog || [],
      usage: memoryIngress.diagnostic.usage?.snapshot?.() || null,
      memory: memoryIngress.diagnostic.context?.snapshot?.() || EMPTY_MEMORY,
      historySegments: memoryIngress.diagnostic.chatend?.historySegments || [],
    };
  }
  const archive = record?.state?.historyIngress;
  return archive?.format === "kennedy-chatend" && archive?.sessionType === "history-ingress"
    ? archivedDiagnostic(archive, "history ingress")
    : null;
}

function ingressStatus(record, ingress) {
  if (record?.phase === "ingress_pending") return "queued";
  if (record?.phase === "ingress_failed") return "failed";
  if (record?.phase === "ingress_in_progress") return ingress?.usage?.requests ? "in progress" : "starting";
  if (ingress) return "complete";
  return null;
}

function historyPhase(label, status, source) {
  return {
    label,
    status,
    segments: source?.historySegments || [],
    current: source ? { messages: source.chatend, memory: source.memory, usage: source.usage } : null,
  };
}

function diagnostic() {
  if (activeView === "audio") return audioRecordingDiagnostic();
  const record = selectedRecord();
  const conversation = conversationDiagnostic(record, selectedSession());
  const ingress = historyIngressDiagnostic(record);
  const status = ingressStatus(record, ingress);
  const current = ingress || conversation || {
    mode: "offline", provider, model, chatend: [], context: {}, loadCalls: 0, loadLimit: 0,
    toolLog: [], usage: null, memory: EMPTY_MEMORY, historySegments: [],
  };
  const phases = [];
  if (conversation) phases.push(historyPhase(sessionTypeOf(record) === "free-time" ? "Free time" : "Conversation", record?.phase === "active" ? "live" : "closed", conversation));
  if (status) phases.push(historyPhase("History ingress", status, ingress));
  return { ...current, ingressStatus: status, fullHistory: { phases } };
}

function audioPieceDiagnostic(piece) {
  if (piece?.id === memoryIngress.activeAudioPiece?.id && memoryIngress.diagnostic) {
    return {
      mode: "audio ingress", provider, model,
      chatend: memoryIngress.diagnostic.chatend?.messages || [],
      context: memoryIngress.diagnostic.context?.diagnostics?.() || {},
      loadCalls: memoryIngress.diagnostic.executor?.loadCalls || 0,
      loadLimit: memoryIngress.diagnostic.executor?.loadLimit || 50,
      toolLog: memoryIngress.diagnostic.executor?.toolLog || [],
      usage: memoryIngress.diagnostic.usage?.snapshot?.() || null,
      memory: memoryIngress.diagnostic.context?.snapshot?.() || EMPTY_MEMORY,
      historySegments: memoryIngress.diagnostic.chatend?.historySegments || [],
    };
  }
  const archive = piece?.state?.historyIngress;
  return archive?.format === "kennedy-chatend"
    ? archivedDiagnostic(archive, "audio ingress")
    : null;
}

function audioPieceIngressActivity(piece) {
  const currentPiece = piece?.id === memoryIngress.activeAudioPiece?.id
    ? memoryIngress.activeAudioPiece
    : piece;
  let diagnostic = null;
  if (currentPiece?.id === memoryIngress.activeAudioPiece?.id && memoryIngress.diagnostic) {
    diagnostic = memoryIngress.diagnostic;
  } else {
    const archive = currentPiece?.state?.historyIngress;
    if (archive?.format === "kennedy-chatend") {
      diagnostic = {
        chatend: { messages: archive.messages || [] },
        usage: { snapshot: () => archive.usage || null },
        toolLog: archive.tools?.log || [],
      };
    }
  }
  const failed = currentPiece?.phase === "ingress_failed";
  const active = currentPiece?.phase === "ingress_pending" || currentPiece?.phase === "ingress_in_progress";
  if (!diagnostic) {
    diagnostic = {
      chatend: { messages: [] },
      usage: { snapshot: () => null },
      toolLog: [],
    };
  }
  return {
    diagnostic,
    active,
    failed,
    failures: Array.isArray(currentPiece?.ingress_failures) ? currentPiece.ingress_failures : [],
  };
}

function audioIngressActivities(detail) {
  return new Map((detail?.pieces || []).map(piece => [piece.id, audioPieceIngressActivity(piece)]));
}

function audioRecordingDiagnostic() {
  const detail = selectedAudioDetail();
  const phases = (detail?.pieces || []).map(piece => {
    const source = audioPieceDiagnostic(piece);
    return historyPhase(
      `Transcript piece ${piece.piece_index + 1}/${piece.piece_count}`,
      piece.phase.replaceAll("_", " "),
      source,
    );
  });
  const current = [...(detail?.pieces || [])].reverse()
    .map(audioPieceDiagnostic)
    .find(Boolean) || {
      mode: "audio ingress", provider, model, chatend: [], context: {}, loadCalls: 0, loadLimit: 50,
      toolLog: [], usage: null, memory: EMPTY_MEMORY, historySegments: [],
    };
  return { ...current, fullHistory: { phases } };
}

function visibleIngressActivity() {
  return conversationIngressActivity({
    record: selectedRecord(),
    liveRecordId: memoryIngress.activeRecord?.id,
    liveDiagnostic: memoryIngress.diagnostic,
  });
}

function update() {
  renderFreeTimeControls();
  if (activeView === "audio") {
    const detail = selectedAudioDetail();
    const audioViewKey = detail?.recording?.id
      ? `audio-recording:${detail.recording.id}`
      : selectedAudioId ? `audio-loading:${selectedAudioId}` : "audio-recording:none";
    renderAudioRecording(ui.transcript, detail, {
      loading: audioDetailLoading.has(selectedAudioId) && !detail,
      error: detail ? null : audioDetailErrors.get(selectedAudioId) || null,
      retryingPieceIds: retryingAudioPieces,
      onRetryPiece: retryAudioIngressPiece,
      ingressActivities: audioIngressActivities(detail),
      viewKey: audioViewKey,
    });
    renderAudioHistory(ui.conversation_history, audioRecords, {
      selectedId: selectedAudioId,
      onSelect: id => selectAudioRecording(id),
      retryingIds: retryingAudioRecordings,
      onRetryIngress: retryAudioIngressRecording,
      viewKey: "sidebar:audio",
    });
    const currentDiagnostic = diagnostic();
    renderInspector(
      ui.context_inspector,
      currentDiagnostic,
      inspectorMode,
      `inspector:audio:${selectedAudioId || "none"}:${inspectorMode}`,
    );
    renderUsage(ui.usage_metrics, currentDiagnostic);
    for (const mode of INSPECTOR_MODES) {
      const button = ui[`inspector_${mode}`];
      const active = inspectorMode === mode;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    }
    ui.message_form.classList.add("hidden");
    ui.new_conversation.classList.add("hidden");
    ui.history_eyebrow.textContent = "AUDIO INGRESS";
    ui.history_title.textContent = "Vnote history";
    ui.chatend_title.textContent = "Kennedy audio-ingress history";
    return;
  }
  const record = selectedRecord();
  const session = selectedSession();
  const viewingHistory = Boolean(record && (record.phase !== "active" || !session));
  const telegramView = activeView === "telegram";
  const freeTimeView = sessionTypeOf(record) === "free-time";
  const ingressActivity = visibleIngressActivity();
  renderTranscript(
    ui.transcript,
    viewingHistory ? (record?.state?.transcript || []) : (session?.transcript || []),
    ingressActivity,
    `${activeView}:${selectedConversationId || "none"}`,
    record?.phase === "ingress_failed"
      ? { retrying: retryingConversationIds.has(record.id), onRetry: () => retryConversationIngress(record) }
      : null,
  );
  if (telegramView && !(viewingHistory ? record?.state?.transcript : session?.transcript)?.length && !ingressActivity?.diagnostic) {
    ui.transcript.replaceChildren(element("div", "telegram-empty", "Telegram conversations appear here as messages arrive. Keep this page open: the relay queues messages while it is closed, and this visible UI owns Kennedy's Chatend and tool loop."));
  }
  renderConversationHistory(ui.conversation_history, recordsForView(), {
    selectedId: selectedConversationId,
    onSelect: id => selectConversation(id).catch(error => showError(ui.error_banner, error.message)),
    retryingIds: retryingConversationIds,
    onRetryIngress: retryConversationIngress,
    purgingIds: purgingConversationIds,
    onPurge: forcePurgeConversation,
    viewKey: `sidebar:${activeView}`,
  });
  const currentDiagnostic = diagnostic();
  renderInspector(
    ui.context_inspector,
    currentDiagnostic,
    inspectorMode,
    `inspector:${activeView}:${selectedConversationId || "none"}:${inspectorMode}`,
  );
  renderUsage(ui.usage_metrics, currentDiagnostic);
  for (const mode of INSPECTOR_MODES) {
    const button = ui[`inspector_${mode}`];
    const active = inspectorMode === mode;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  }
  const controls = conversationControlState({
    hasSession: Boolean(session),
    sessionBusy: Boolean(session?.busy),
    transitionBusy: creatingConversation || endingIds.has(selectedConversationId),
    pendingTurn: Boolean(session?.pendingTurn),
    viewingHistory: viewingHistory || telegramView || freeTimeView,
    transcriptLength: session?.transcript.length || 0,
  });
  const extractingAttachment = extractingAttachments.has(selectedConversationId);
  ui.message_form.classList.toggle("hidden", controls.composerHidden);
  ui.message_input.disabled = controls.inputDisabled;
  ui.send_button.disabled = controls.sendDisabled || extractingAttachment;
  ui.send_end_button.disabled = controls.sendDisabled || extractingAttachment;
  ui.end_button.disabled = controls.endDisabled;
  ui.stop_button.classList.toggle("hidden", controls.stopHidden || !session?.canStop);
  ui.stop_button.disabled = Boolean(session?.stopping);
  ui.stop_button.textContent = session?.stopping ? "Stopping…" : "Stop Kennedy";
  ui.new_conversation.disabled = controls.newDisabled || !chatRuntimeReady();
  ui.new_conversation.classList.toggle("hidden", telegramView);
  ui.voice_button.disabled = controls.sendDisabled || !transcriptionAvailable
    || !navigator.mediaDevices?.getUserMedia || typeof MediaRecorder !== "function";
  const attachments = attachmentDrafts.get(selectedConversationId) || [];
  ui.attach_button.disabled = controls.sendDisabled || extractingAttachment;
  ui.attachment_status.textContent = attachments.length
    ? `${attachments.length} attached: ${attachments.map(item => item.fileName).join(", ")}`
    : "PDF, Word, spreadsheet, or text";
  ui.clear_attachments.classList.toggle("hidden", !attachments.length);
  ui.clear_attachments.disabled = controls.sendDisabled || extractingAttachment;
  ui.history_eyebrow.textContent = telegramView ? "TELEGRAM SESSIONS" : "YOUR CONVERSATIONS";
  ui.history_title.textContent = telegramView ? "Bot chats" : "History";
  ui.chatend_title.textContent = currentDiagnostic.mode === "history ingress"
    ? `History ingress · ${currentDiagnostic.ingressStatus || "in progress"}`
    : freeTimeView ? "Free-time Chatend"
    : telegramView ? "Telegram Chatend" : currentDiagnostic.ingressStatus
      ? `Chatend · ingress ${currentDiagnostic.ingressStatus}`
      : "Chatend";
  ui.end_button.textContent = session?.pendingTurn ? "Retry saved query" : "End conversation";
  ui.activity.textContent = freeTimeView
    ? session?.stopping ? "Free time reached its hard stop"
      : session?.busy ? "Kennedy is enjoying free time"
        : "This free-time session is closing"
    : telegramView
    ? session?.busy ? "Kennedy is answering this Telegram message" : "Messages are delivered automatically"
    : viewingHistory
    ? record?.phase === "active" ? "Chat is unavailable; this saved conversation is read only" : "This conversation is closed and read only"
    : session?.stopping
      ? "Stopping Kennedy — the saved query will remain retryable"
    : session?.busy
      ? "Kennedy is working — you can draft your next message"
      : session?.pendingTurn
        ? "Saved query needs a response — you can keep drafting"
      : "";
}

function upsertHistory(record) {
  if (!record) return;
  historyRecords = sortConversationHistory([record, ...historyRecords.filter(item => item.id !== record.id)]);
}

function saveDraft() {
  if (activeView === "conversation" && selectedSession()) drafts.set(selectedConversationId, ui.message_input.value);
}

function restoreDraft() {
  ui.message_input.value = selectedSession() ? (drafts.get(selectedConversationId) || "") : "";
}

function composerHeightBounds() {
  const min = Number.parseFloat(getComputedStyle(ui.message_input).minHeight) || 96;
  const max = Math.max(min, Math.min(window.innerHeight * .64, 720, window.innerHeight - 250));
  return { min, max };
}

function syncComposerResizeValue() {
  const { min, max } = composerHeightBounds();
  const height = Math.round(ui.message_input.getBoundingClientRect().height);
  ui.message_resize_handle.setAttribute("aria-valuemin", String(Math.round(min)));
  ui.message_resize_handle.setAttribute("aria-valuemax", String(Math.round(max)));
  ui.message_resize_handle.setAttribute("aria-valuenow", String(height));
}

function setMessageInputHeight(height) {
  const { min, max } = composerHeightBounds();
  const nextHeight = Math.min(max, Math.max(min, height));
  ui.message_input.style.height = `${nextHeight}px`;
  syncComposerResizeValue();
}

function setComposerExpanded(expanded) {
  ui.message_form.classList.toggle("composer-expanded", expanded);
  setMessageInputHeight(expanded ? Math.min(620, Math.max(320, window.innerHeight * .52)) : 96);
  ui.message_size_button.setAttribute("aria-expanded", String(expanded));
  ui.message_size_button.textContent = expanded ? "Use compact size" : "Make larger";
}

let composerResize = null;

function finishComposerResize(event) {
  if (!composerResize || (event.pointerId !== undefined && event.pointerId !== composerResize.pointerId)) return;
  composerResize = null;
  ui.message_resize_handle.classList.remove("resizing");
}

function blobToDataUrl(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error || new Error("Could not archive the voice recording."));
    reader.readAsDataURL(blob);
  });
}

const MAX_ATTACHMENT_FILES = 5;
const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;

async function attachSelectedFiles() {
  const id = selectedConversationId;
  const files = Array.from(ui.attachment_input.files || []);
  ui.attachment_input.value = "";
  if (!files.length || !selectedSession() || activeView !== "conversation") return;
  const existing = attachmentDrafts.get(id) || [];
  if (existing.length + files.length > MAX_ATTACHMENT_FILES) {
    showError(ui.error_banner, `Attach at most ${MAX_ATTACHMENT_FILES} files to one message.`);
    return;
  }
  const oversized = files.find(file => !file.size || file.size > MAX_ATTACHMENT_BYTES);
  if (oversized) {
    showError(ui.error_banner, `${oversized.name} must be between 1 byte and 20 MiB.`);
    return;
  }
  const totalBytes = [...existing, ...files].reduce((total, item) => total + (Number(item.sizeBytes ?? item.size) || 0), 0);
  if (totalBytes > MAX_ATTACHMENT_BYTES) {
    showError(ui.error_banner, "Attachments for one message must total 20 MiB or less.");
    return;
  }
  extractingAttachments.add(id);
  update();
  ui.activity.textContent = `Reading ${files.length === 1 ? files[0].name : `${files.length} files`}…`;
  try {
    const extracted = [];
    for (const file of files) {
      const started = performance.now();
      const result = await intelligence.extractDocument({ file, fileName: file.name });
      extracted.push({
        id: crypto.randomUUID(),
        kind: "document",
        fileName: result.file_name || file.name,
        mimeType: file.type || result.content_type || "application/octet-stream",
        sizeBytes: file.size,
        dataUrl: await blobToDataUrl(file),
        format: result.format,
        text: result.text,
        characters: result.characters,
        truncated: Boolean(result.truncated),
        extractionDurationMs: Math.max(0, Math.round(performance.now() - started)),
      });
    }
    if (liveSessions.has(id)) attachmentDrafts.set(id, [...existing, ...extracted]);
  } catch (error) {
    showError(ui.error_banner, `Attachment could not be read: ${error.message}`);
  } finally {
    extractingAttachments.delete(id);
    update();
  }
}

function clearAttachmentDraft(id = selectedConversationId) {
  attachmentDrafts.delete(id);
  update();
}

function audioExtension(mimeType) {
  if (mimeType.includes("ogg")) return "ogg";
  if (mimeType.includes("mp4")) return "m4a";
  if (mimeType.includes("mpeg")) return "mp3";
  if (mimeType.includes("wav")) return "wav";
  return "webm";
}

async function finishVoiceRecording() {
  const id = selectedConversationId;
  const mimeType = recorder?.mimeType || recorderChunks[0]?.type || "audio/webm";
  const blob = new Blob(recorderChunks, { type: mimeType });
  recordingStream?.getTracks().forEach(track => track.stop());
  recordingStream = null;
  recorder = null;
  recorderChunks = [];
  ui.voice_button.classList.remove("recording");
  ui.voice_button.setAttribute("aria-pressed", "false");
  ui.voice_button.textContent = "Record voice";
  if (!blob.size || selectedConversationId !== id) return;
  ui.activity.textContent = "Transcribing voice note with OpenAI…";
  ui.voice_button.disabled = true;
  try {
    if (inputModalities.includes("audio")) throw new Error("The selected native-audio transport is not enabled in this UI build.");
    const fileName = `voice-note.${audioExtension(mimeType)}`;
    const transcriptionStarted = performance.now();
    const result = await intelligence.transcribe({ provider, model, file: blob, fileName });
    const transcriptionDurationMs = Math.max(0, Math.round(performance.now() - transcriptionStarted));
    const dataUrl = await blobToDataUrl(blob);
    voiceDrafts.set(id, {
      inputKind: "voice",
      transcriptionModel: result.transcription_model,
      transcriptionDurationMs,
      media: { id: crypto.randomUUID(), kind: "voice", mimeType, fileName, dataUrl, sizeBytes: blob.size },
    });
    ui.message_input.value = result.text;
    drafts.set(id, result.text);
    ui.message_input.focus();
  } catch (error) {
    showError(ui.error_banner, `Voice note could not be transcribed: ${error.message}`);
  }
  update();
}

async function toggleVoiceRecording() {
  if (recorder?.state === "recording") {
    recorder.stop();
    return;
  }
  try {
    recordingStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    recorderChunks = [];
    recorder = new MediaRecorder(recordingStream);
    recorder.addEventListener("dataavailable", event => { if (event.data.size) recorderChunks.push(event.data); });
    recorder.addEventListener("stop", () => finishVoiceRecording());
    recorder.start();
    ui.voice_button.classList.add("recording");
    ui.voice_button.setAttribute("aria-pressed", "true");
    ui.voice_button.textContent = "Stop recording";
    ui.activity.textContent = "Recording voice note…";
  } catch (error) {
    recordingStream?.getTracks().forEach(track => track.stop());
    recordingStream = null;
    recorder = null;
    showError(ui.error_banner, `Microphone access failed: ${error.message}`);
  }
}

function reconcileLiveSessions(records) {
  historyRecords = sortConversationHistory(records);
  const activeIds = new Set(historyRecords.filter(record => record.phase === "active").map(record => record.id));
  for (const id of liveSessions.keys()) {
    if (!activeIds.has(id)) liveSessions.delete(id);
  }
}

async function refreshHistory() {
  const records = (await conversationHistory.list()).conversations || [];
  reconcileLiveSessions(records);
  update();
}

async function loadAudioRecording(id, force = false) {
  if (!id || audioDetailLoading.has(id) || (!force && audioDetails.has(id))) return;
  const hadCachedDetail = audioDetails.has(id);
  audioDetailLoading.add(id);
  if (!hadCachedDetail) audioDetailErrors.delete(id);
  update();
  try {
    audioDetails.set(id, await audioIngress.history(id));
    audioDetailErrors.delete(id);
  } catch (error) {
    const message = error.message || "Unknown audio-ingress history error.";
    if (hadCachedDetail) {
      showError(ui.error_banner, `Audio history refresh will retry: ${message}`);
    } else {
      audioDetailErrors.set(id, message);
    }
  } finally {
    audioDetailLoading.delete(id);
    update();
  }
}

function selectAudioRecording(id) {
  selectedAudioId = id;
  update();
  loadAudioRecording(id).catch(error => showError(ui.error_banner, error.message));
}

async function refreshAudioHistory(refreshSelected = false) {
  audioRecords = (await audioIngress.list(50_000)).recordings || [];
  if (!audioRecords.some(record => record.id === selectedAudioId)) {
    selectedAudioId = audioRecords[0]?.id || null;
  }
  if (refreshSelected && selectedAudioId) await loadAudioRecording(selectedAudioId, true);
  update();
}

async function retryAudioIngressPiece(piece) {
  if (!piece?.id || retryingAudioPieces.has(piece.id)) return;
  retryingAudioPieces.add(piece.id);
  update();
  try {
    await audioIngress.retryIngress(piece.id, {
      expected_version: piece.version,
      state: freshIngressState(piece.state),
    });
    await refreshAudioHistory(true);
    kickHistoryIngress();
  } catch (error) {
    showError(ui.error_banner, `Audio memory ingress could not be retried: ${error.message}`);
  } finally {
    retryingAudioPieces.delete(piece.id);
    update();
  }
}

async function retryAudioIngressRecording(record) {
  if (!record?.id || retryingAudioRecordings.has(record.id)) return;
  retryingAudioRecordings.add(record.id);
  update();
  const scheduled = [];
  try {
    const detail = await audioIngress.history(record.id);
    const failedPieces = (detail.pieces || []).filter(piece =>
      piece.phase === "ingress_failed" && !retryingAudioPieces.has(piece.id)
    );
    if (!failedPieces.length) throw new Error("No failed transcript pieces are available to retry.");
    for (const piece of failedPieces) {
      retryingAudioPieces.add(piece.id);
      scheduled.push(piece.id);
    }
    update();
    await Promise.all(failedPieces.map(piece => audioIngress.retryIngress(piece.id, {
      expected_version: piece.version,
      state: freshIngressState(piece.state),
    })));
    kickHistoryIngress();
  } catch (error) {
    showError(ui.error_banner, `Audio memory ingress could not be retried: ${error.message}`);
  } finally {
    retryingAudioRecordings.delete(record.id);
    for (const pieceId of scheduled) retryingAudioPieces.delete(pieceId);
    try {
      await refreshAudioHistory(activeView === "audio" && selectedAudioId === record.id);
    } catch (error) {
      showError(ui.error_banner, `Audio history could not be refreshed after retry: ${error.message}`);
    }
    update();
  }
}

async function retryConversationIngress(record) {
  if (!record?.id || record.phase !== "ingress_failed" || retryingConversationIds.has(record.id)) return;
  retryingConversationIds.add(record.id);
  update();
  try {
    const retried = await conversationHistory.retryIngress(record.id, {
      expected_version: record.version,
      state: freshIngressState(record.state),
    });
    upsertHistory(retried);
    kickHistoryIngress();
  } catch (error) {
    showError(ui.error_banner, `History ingress could not be retried: ${error.message}`);
  } finally {
    retryingConversationIds.delete(record.id);
    update();
  }
}

function purgeWarning(record) {
  const irreversible = "Its transcript and recovery checkpoints will be permanently deleted. This cannot be undone.";
  if (record.phase === "ingress_in_progress") {
    return `${irreversible}\n\nHistory ingress has already started. Kennedy will stop future work, but any Kmap changes already applied cannot be rolled back.`;
  }
  if (record.phase === "complete") {
    return `${irreversible}\n\nHistory ingress already completed, so existing Kmap changes will not be rolled back.`;
  }
  return `${irreversible}\n\nThe conversation will not be sent through history ingress.`;
}

function removePurgedConversation(id) {
  const purged = historyRecords.find(record => record.id === id);
  historyRecords = historyRecords.filter(record => record.id !== id);
  liveSessions.delete(id);
  drafts.delete(id);
  voiceDrafts.delete(id);
  attachmentDrafts.delete(id);
  extractingAttachments.delete(id);
  conversationErrors.delete(id);
  endingIds.delete(id);
  retryingConversationIds.delete(id);
  memoryIngress.clearConversation(id);
  const sameActiveRunRemains = historyRecords.some(record => record.phase === "active" && freeTimeOf(record)?.runId === freeTimeOf(purged)?.runId);
  if (sessionTypeOf(purged) === "free-time" && !sameActiveRunRemains && freeTimeRun?.runId === freeTimeOf(purged)?.runId) {
    freeTimeRun = null;
    freeTimeContinuationRuns.delete(freeTimeOf(purged)?.runId);
  }
  if (selectedConversationId === id) {
    const replacement = recordsForView()[0]?.id || null;
    selectedConversationId = replacement;
    selectedByView[activeView] = replacement;
    restoreDraft();
  }
}

async function cancelConversationWork(id) {
  const session = liveSessions.get(id);
  if (session?.canStop) await session.stopPendingTurn();
  await memoryIngress.cancelConversation(id);
}

async function forcePurgeConversation(record) {
  if (!record?.id || purgingConversationIds.has(record.id)) return;
  if (!window.confirm(`Permanently purge this conversation?\n\n${purgeWarning(record)}`)) return;
  const id = record.id;
  purgingConversationIds.add(id);
  update();
  try {
    await cancelConversationWork(id);

    let deleted = false;
    let lastError = null;
    for (let attempt = 0; attempt < 3 && !deleted; attempt += 1) {
      let latest;
      try {
        latest = await conversationHistory.get(id);
      } catch (error) {
        if (error.code === "not_found") {
          deleted = true;
          break;
        }
        throw error;
      }
      try {
        await conversationHistory.purge(id, { expected_version: latest.version });
        deleted = true;
      } catch (error) {
        if (error.code === "not_found") {
          deleted = true;
        } else if (error.code === "state_conflict") {
          lastError = error;
        } else {
          throw error;
        }
      }
    }
    if (!deleted) throw lastError || new Error("The conversation kept changing while it was being purged.");
    await cancelConversationWork(id);
    purgedConversationIds.add(id);
    removePurgedConversation(id);
  } catch (error) {
    showError(ui.error_banner, `Conversation could not be purged: ${error.message}`);
  } finally {
    purgingConversationIds.delete(id);
    update();
  }
}

async function persistSession(id, state, metadata = {}) {
  let record = historyRecords.find(item => item.id === id);
  if (!record || record.phase !== "active") throw new Error("This conversation is no longer live.");
  const body = { expected_version: record.version, state, user_activity: Boolean(metadata.userActivity) };
  try {
    record = await conversationHistory.checkpoint(id, body);
  } catch (error) {
    if (error.code !== "state_conflict") throw error;
    const latest = await conversationHistory.get(id);
    if (latest.phase !== "active" || JSON.stringify(latest.state) !== JSON.stringify(state)) throw error;
    record = latest;
  }
  upsertHistory(record);
  if (metadata.userActivity) {
    await refreshHistory();
    kickHistoryIngress();
  }
}

async function buildConversation(record) {
  const sessionType = sessionTypeOf(record);
  const sessionRoots = rootsForRecord(record);
  const referenceRootNodeIds = referencesForRecord(record, sessionRoots);
  const session = new ConversationSession({
    kweb, intelligence, manuals, rootNodeIds: sessionRoots, referenceRootNodeIds, provider, providerKind, model, reasoningEffort, contextWindowTokens, maxInputTokens,
    sessionType,
    channel: record.state?.channel || record.state?.archive?.channel || null,
    freeTime: freeTimeOf(record),
    provenanceId: record.state?.provenanceId || record.state?.archive?.provenanceId || null,
    persist: (state, metadata) => persistSession(record.id, state, metadata),
    onUpdate: update,
  });
  await session.initialize(record.state || null);
  liveSessions.set(record.id, session);
  return session;
}

async function createNewConversation() {
  if (creatingConversation) return;
  if (!chatRuntimeReady()) throw new Error("A new conversation cannot start until Kweb, conversation history, intelligence, and the conversation prompts are available.");
  creatingConversation = true;
  saveDraft();
  update();
  try {
    const session = new ConversationSession({
      kweb, intelligence, manuals, rootNodeIds, provider, providerKind, model, reasoningEffort, contextWindowTokens, maxInputTokens,
      sessionType: "conversation",
      onUpdate: update,
    });
    await session.initialize();
    const record = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
    session.persist = (state, metadata) => persistSession(record.id, state, metadata);
    liveSessions.set(record.id, session);
    drafts.set(record.id, "");
    upsertHistory(record);
    selectedConversationId = record.id;
    selectedByView.conversation = record.id;
    restoreDraft();
    update();
    ui.transcript.scrollTop = ui.transcript.scrollHeight;
    ui.message_input.focus();
  } finally {
    creatingConversation = false;
    update();
  }
}

function freeTimeTimerAt(timestamp, callback) {
  const delay = Math.max(0, Math.min(2_147_483_647, timestamp - Date.now()));
  return setTimeout(callback, delay);
}

async function closeFreeTimeSession(id, session) {
  let record = historyRecords.find(item => item.id === id) || await conversationHistory.get(id);
  if (record.phase === "active") {
    record = await conversationHistory.requestIngress(id, {
      expected_version: record.version,
      state: session.snapshot(),
    });
    upsertHistory(record);
  }
  liveSessions.delete(id);
  drafts.delete(id);
  voiceDrafts.delete(id);
  attachmentDrafts.delete(id);
  update();
  kickHistoryIngress();
  return record;
}

function nextFreeTimeSlice(freeTime) {
  const {
    warningNoticeAt: _warningNoticeAt,
    expiredNoticeAt: _expiredNoticeAt,
    sliceEndedAt: _sliceEndedAt,
    sliceEndedReason: _sliceEndedReason,
    ...shared
  } = freeTime;
  return { ...shared, sliceIndex: Number(freeTime.sliceIndex || 0) + 1 };
}

async function createFreeTimeSlice(freeTime, { select = false } = {}) {
  const session = new ConversationSession({
    kweb, intelligence, manuals, rootNodeIds, provider, providerKind, model, reasoningEffort,
    contextWindowTokens, maxInputTokens, sessionType: "free-time", freeTime,
    provenanceId: freeTime.provenanceId, onUpdate: update,
  });
  await session.initialize();
  session.stageFreeTimeOpening();
  let record = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
  session.persist = (state, metadata) => persistSession(record.id, state, metadata);
  liveSessions.set(record.id, session);
  drafts.set(record.id, "");
  upsertHistory(record);
  await session.persistSnapshot(session.snapshot(), { userActivity: true });
  session.pendingCheckpointed = true;
  record = historyRecords.find(item => item.id === record.id) || record;
  freeTimeRun = freeTime;
  if (select) {
    if (activeView !== "conversation") showView("conversation");
    selectedConversationId = record.id;
    selectedByView.conversation = record.id;
    restoreDraft();
  }
  update();
  launchFreeTimeRecord(record.id);
  return record;
}

async function startFreeTime() {
  if (navigator.locks?.request) {
    return navigator.locks.request("kennedy-free-time-start", async () => {
      await refreshHistory();
      return startFreeTimeUnlocked();
    });
  }
  await refreshHistory();
  return startFreeTimeUnlocked();
}

async function startFreeTimeUnlocked() {
  if (freeTimeStarting || freeTimeRun || activeFreeTimeRecord()) return;
  if (!freeTimeRuntimeReady()) throw new Error("Free time cannot start until Kweb, conversation history, intelligence, and the free-time prompts are available.");
  const durationMinutes = parseFreeTimeMinutes(ui.free_time_minutes.value || DEFAULT_FREE_TIME_MINUTES);
  const runStartedAt = new Date().toISOString();
  const runId = crypto.randomUUID();
  const freeTime = {
    runId,
    runStartedAt,
    deadlineAt: new Date(Date.now() + Math.round(durationMinutes * 60_000)).toISOString(),
    durationMinutes,
    sliceIndex: 1,
  };
  freeTimeStarting = true;
  update();
  try {
    const provenance = await kweb.createProvenance({
      data: JSON.stringify({ kind: "free-time", ...freeTime }, null, 2),
      source: "free-time",
      source_created_at: runStartedAt,
      idempotency_key: `free-time:${runId}`,
    });
    freeTime.provenanceId = provenance.id;
    await createFreeTimeSlice(freeTime, { select: true });
  } finally {
    freeTimeStarting = false;
    update();
  }
}

function launchFreeTimeRecord(id) {
  if (!id || freeTimeRunnerIds.has(id)) return;
  freeTimeRunnerIds.add(id);
  const run = () => runFreeTimeRecord(id);
  const work = navigator.locks?.request
    ? navigator.locks.request("kennedy-free-time", { ifAvailable: true }, lock => {
      if (lock) return run();
      setTimeout(() => launchFreeTimeRecord(id), 2_000);
      return undefined;
    })
    : run();
  Promise.resolve(work).catch(error => {
    if (!purgedConversationIds.has(id)) {
      showError(ui.error_banner, `Free time will retry its saved session: ${error.message}`);
      setTimeout(() => launchFreeTimeRecord(id), 2_000);
    }
  }).finally(() => {
    freeTimeRunnerIds.delete(id);
    renderFreeTimeControls();
  });
}

function scheduleFreeTimeContinuation(freeTime, select) {
  if (freeTimeContinuationRuns.has(freeTime.runId)) return;
  freeTimeContinuationRuns.add(freeTime.runId);
  freeTimeRun = freeTime;
  const attempt = async () => {
    if (Date.now() >= Date.parse(freeTime.deadlineAt)) {
      freeTimeContinuationRuns.delete(freeTime.runId);
      if (freeTimeRun?.runId === freeTime.runId) freeTimeRun = null;
      update();
      return;
    }
    try {
      await createFreeTimeSlice(freeTime, { select });
      freeTimeContinuationRuns.delete(freeTime.runId);
    } catch (error) {
      showError(ui.error_banner, `The next free-time session will retry opening: ${error.message}`);
      setTimeout(attempt, 2_000);
    }
  };
  Promise.resolve(attempt());
}

async function runFreeTimeRecord(id) {
  let record = await conversationHistory.get(id);
  upsertHistory(record);
  if (record.phase !== "active" || sessionTypeOf(record) !== "free-time") {
    await refreshHistory();
    const current = activeFreeTimeRecord();
    if (current) {
      freeTimeRun = freeTimeOf(current);
      launchFreeTimeRecord(current.id);
    } else if (freeTimeRun && Date.now() >= Date.parse(freeTimeRun.deadlineAt)) {
      freeTimeRun = null;
      update();
    } else if (freeTimeRun) {
      // Another tab may be between closing one slice and durably creating the
      // next. Keep following the run instead of leaving this tab's controls in
      // a stale active state until reload.
      setTimeout(() => launchFreeTimeRecord(id), 2_000);
    }
    return;
  }
  let session = liveSessions.get(id);
  if (!session) session = await buildConversation(record);
  const metadata = session.freeTime || freeTimeOf(record);
  const timing = freeTimeTiming(metadata);
  freeTimeRun = metadata;
  const warningTimer = freeTimeTimerAt(timing.deadlineMs - FREE_TIME_WARNING_MS, () => {
    renderFreeTimeControls();
    if (!session.busy && session.pendingTurn) {
      session.prepareFreeTimeRound().catch(error => showError(ui.error_banner, `The free-time warning could not be saved: ${error.message}`));
    }
  });
  const deadlineTimer = freeTimeTimerAt(timing.deadlineMs, renderFreeTimeControls);
  const hardStopTimer = freeTimeTimerAt(timing.hardStopMs, () => {
    renderFreeTimeControls();
    if (session.canStop) Promise.resolve(session.stopPendingTurn()).catch(() => {});
  });
  let reason = session.freeTime?.sliceEndedReason || null;
  let lastRetryError = null;
  try {
    while (!reason) {
      const currentTiming = freeTimeTiming(metadata);
      if (currentTiming.hardExpired) {
        if (session.canStop) await session.stopPendingTurn().catch(() => {});
        reason = "hard-stop";
        break;
      }
      if (!session.pendingTurn && !session.transcript.length) {
        session.stageFreeTimeOpening();
        await session.persistSnapshot(session.snapshot(), { userActivity: true });
        session.pendingCheckpointed = true;
      }
      if (!session.pendingTurn) {
        reason = session.freeTimeEndReason || (currentTiming.expired ? "deadline" : "completed");
        break;
      }
      try {
        await session.resumePendingTurn();
        lastRetryError = null;
      } catch (error) {
        const stoppedAtHardLimit = error?.code === "turn_stopped" && freeTimeTiming(metadata).hardExpired;
        if (stoppedAtHardLimit) {
          reason = "hard-stop";
          break;
        }
        if (lastRetryError !== error.message) {
          lastRetryError = error.message;
          showError(ui.error_banner, `Free time is retrying Kennedy's saved round: ${error.message}`);
        }
        await new Promise(resolve => setTimeout(resolve, 2_000));
      }
    }
    await session.finalizeFreeTime(reason);
    const selectNext = selectedConversationId === id;
    await closeFreeTimeSession(id, session);
    if (Date.now() < timing.deadlineMs && !purgedConversationIds.has(id)) {
      scheduleFreeTimeContinuation(nextFreeTimeSlice(metadata), selectNext);
    } else if (freeTimeRun?.runId === metadata.runId) {
      freeTimeRun = null;
      update();
    }
  } finally {
    clearTimeout(warningTimer);
    clearTimeout(deadlineTimer);
    clearTimeout(hardStopTimer);
  }
}

async function selectConversation(id) {
  if (id === selectedConversationId) return;
  saveDraft();
  const record = historyRecords.find(item => item.id === id) || await conversationHistory.get(id);
  upsertHistory(record);
  selectedConversationId = id;
  selectedByView[viewForSessionType(sessionTypeOf(record))] = id;
  restoreDraft();
  update();
  if (record.phase === "active" && !liveSessions.has(id) && chatRuntimeReady()) {
    try {
      await buildConversation(record);
    } catch (error) {
      update();
      throw new Error(`This live conversation could not be restored. You can still purge it: ${error.message}`);
    }
  }
  restoreDraft();
  update();
  ui.transcript.scrollTop = ui.transcript.scrollHeight;
  if (selectedSession()) ui.message_input.focus();
  const error = conversationErrors.get(id);
  if (error) showError(ui.error_banner, error);
}

async function submitMessage(event) {
  event.preventDefault();
  const session = selectedSession();
  const id = selectedConversationId;
  if (!session || session.busy || session.pendingTurn || endingIds.has(id) || extractingAttachments.has(id)) return;
  const text = ui.message_input.value;
  const attachments = attachmentDrafts.get(id) || [];
  if (!text.trim() && !attachments.length) return;
  ui.message_input.value = "";
  drafts.set(id, "");
  const metadata = { ...(voiceDrafts.get(id) || {}), attachments };
  voiceDrafts.delete(id);
  attachmentDrafts.delete(id);
  conversationErrors.delete(id);
  try {
    await session.send(text, metadata);
  } catch (error) {
    if (error?.code === "turn_stopped") {
      conversationErrors.delete(id);
      update();
      return;
    }
    const message = error.message || "Kennedy could not answer the saved query.";
    conversationErrors.set(id, message);
    if (selectedConversationId === id) showError(ui.error_banner, message);
  }
  update();
}

async function resumeSavedQuery(id = selectedConversationId) {
  const session = liveSessions.get(id);
  if (!session?.pendingTurn || session.busy) return;
  conversationErrors.delete(id);
  try {
    await session.resumePendingTurn();
  } catch (error) {
    if (error?.code === "turn_stopped") {
      conversationErrors.delete(id);
      update();
      return;
    }
    const message = `The saved query could not be resumed: ${error.message}`;
    conversationErrors.set(id, message);
    if (selectedConversationId === id) showError(ui.error_banner, message);
  }
  update();
}

async function stopConversationTurn() {
  const id = selectedConversationId;
  const session = selectedSession();
  if (!session?.canStop) return;
  conversationErrors.delete(id);
  try {
    await session.stopPendingTurn();
  } catch (error) {
    showError(ui.error_banner, `Kennedy could not be stopped cleanly: ${error.message}`);
  }
  update();
}

async function closeConversation(id, session, record) {
  const latest = historyRecords.find(item => item.id === id) || record;
  const closed = await conversationHistory.requestIngress(id, {
    expected_version: latest.version,
    state: session.snapshot(),
  });
  upsertHistory(closed);
  liveSessions.delete(id);
  drafts.delete(id);
  voiceDrafts.delete(id);
  attachmentDrafts.delete(id);
  selectedConversationId = id;
  selectedByView.conversation = id;
  restoreDraft();
  update();
  kickHistoryIngress();
  return closed;
}

async function endConversation() {
  const id = selectedConversationId;
  const session = selectedSession();
  const record = selectedRecord();
  if (!session || !record || session.busy || session.pendingTurn || endingIds.has(id)) return;
  endingIds.add(id);
  update();
  try {
    await closeConversation(id, session, record);
  } catch (error) {
    showError(ui.error_banner, error.message);
  } finally {
    endingIds.delete(id);
    update();
  }
}

async function sendAndEndConversation() {
  const id = selectedConversationId;
  const session = selectedSession();
  const record = selectedRecord();
  if (!session || !record || session.busy || session.pendingTurn || endingIds.has(id) || extractingAttachments.has(id)) return;
  const text = ui.message_input.value;
  const attachments = attachmentDrafts.get(id) || [];
  if (!text.trim() && !attachments.length) return;
  const metadata = { ...(voiceDrafts.get(id) || {}), attachments };
  endingIds.add(id);
  update();
  try {
    const appended = await session.appendFinalUserMessage(text, metadata);
    if (!appended) return;
    ui.message_input.value = "";
    drafts.set(id, "");
    voiceDrafts.delete(id);
    attachmentDrafts.delete(id);
    await closeConversation(id, session, record);
  } catch (error) {
    showError(ui.error_banner, `The final message was not fully closed into history ingress: ${error.message}`);
  } finally {
    endingIds.delete(id);
    update();
  }
}

async function createTelegramConversation(event) {
  const directoryUser = await directoryUserForEvent(event);
  const sessionType = event.sessionKind === "group" ? "telegram-group" : "telegram";
  const directoryGroup = await directoryGroupForEvent(event);
  const sessionRoots = directoryGroup
    ? [directoryUser.rootNodeId, directoryGroup.rootNodeId, kennedyRootNodeId]
    : [directoryUser.rootNodeId, kennedyRootNodeId];
  const latestGroupMessageId = (event.groupContext?.messages || [])
    .reduce((latest, message) => Math.max(latest, Number(message.messageId) || 0), 0);
  const groupContext = sessionType === "telegram-group" && event.groupContext
    ? { ...event.groupContext, messages: (event.groupContext.messages || []).filter(message => String(message.messageId) !== String(event.messageId)) }
    : null;
  const referenceRootNodeIds = referencesForGroup(groupContext, sessionRoots);
  const channel = {
    kind: sessionType,
    telegramUserId: event.telegramUserId,
    chatId: event.chatId,
    username: event.username || null,
    displayName: event.displayName,
    groupRootNodeId: directoryGroup?.rootNodeId || event.groupRootNodeId || null,
    groupContext,
    lastGroupContextMessageId: latestGroupMessageId,
  };
  const session = new ConversationSession({
    kweb, intelligence, manuals, rootNodeIds: sessionRoots, referenceRootNodeIds, provider, providerKind, model, reasoningEffort,
    contextWindowTokens, maxInputTokens, sessionType, channel, onUpdate: update,
  });
  await session.initialize();
  const record = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
  session.persist = (state, metadata) => persistSession(record.id, state, metadata);
  liveSessions.set(record.id, session);
  upsertHistory(record);
  return { record, session };
}

async function telegramConversationFor(event) {
  const group = event.sessionKind === "group";
  let record = event.conversationId
    ? historyRecords.find(item => item.id === event.conversationId) || await conversationHistory.get(event.conversationId).catch(() => null)
    : null;
  if (record && group && !groupSessionMatches(record, event)) record = null;
  if (!record && !group) {
    record = historyRecords.find(item => item.phase === "active"
      && sessionTypeOf(item) === "telegram"
      && String(item.state?.channel?.telegramUserId) === String(event.telegramUserId));
  }
  if (!record && group) {
    record = historyRecords.find(item => item.phase === "active" && groupSessionMatches(item, event));
  }
  let session = record?.phase === "active" ? liveSessions.get(record.id) : null;
  if (record?.phase === "active" && !session) session = await buildConversation(record);
  let created = false;
  if (!record || record.phase !== "active") {
    ({ record, session } = await createTelegramConversation(event));
    created = true;
  }
  if (event.conversationId !== record.id) await telegramRelay.bind(event.id, record.id);
  if (group && !created) {
    session.channel = {
      ...(session.channel || {}),
      username: event.username || null,
      displayName: event.displayName,
      groupRootNodeId: event.groupRootNodeId || event.groupContext?.groupRootNodeId || session.channel?.groupRootNodeId || null,
    };
    session.refreshTelegramGroupContext(event.groupContext, event.messageId);
    await session.persistSnapshot(session.snapshot());
  }
  return { record, session };
}

async function processTelegramReset(event) {
  const group = event.sessionKind === "group";
  let record = event.conversationId
    ? historyRecords.find(item => item.id === event.conversationId) || await conversationHistory.get(event.conversationId).catch(() => null)
    : null;
  if (record && group && !groupSessionMatches(record, event)) record = null;
  if (!record || record.phase !== "active") {
    record = historyRecords.find(item => item.phase === "active" && (group
      ? groupSessionMatches(item, event)
      : sessionTypeOf(item) === "telegram" && String(channelOf(item)?.telegramUserId) === String(event.telegramUserId)));
  }
  if (!record || record.phase !== "active") {
    const scope = group ? " for you in this group" : "";
    await telegramRelay.resetCompleted(event.id, `There is no active Telegram session${scope} to reset. Your next message will begin one.`);
    return;
  }
  let session = liveSessions.get(record.id);
  if (!session) session = await buildConversation(record);
  if (session.busy) throw new Error("The Telegram session is still completing its previous message.");
  if (session.pendingTurn) await session.resumePendingTurn();
  if (group && event.groupContext) {
    session.refreshTelegramGroupContext(event.groupContext, event.messageId);
    await session.persistSnapshot(session.snapshot());
  }
  const latest = historyRecords.find(item => item.id === record.id) || record;
  const closed = await conversationHistory.requestIngress(record.id, { expected_version: latest.version, state: session.snapshot() });
  upsertHistory(closed);
  liveSessions.delete(record.id);
  if (selectedConversationId === record.id) update();
  kickHistoryIngress();
  const scope = group ? "Your session in this group" : "The Telegram session";
  await telegramRelay.resetCompleted(event.id, `Conversation reset. ${scope} has been queued for memory ingress; your next message will begin a new session.`);
}

async function telegramVoiceInput(event) {
  const blob = await telegramRelay.media(event.id);
  let text = event.transcription;
  let transcriptionModel = event.transcriptionModel;
  let transcriptionDurationMs = null;
  if (!text) {
    if (inputModalities.includes("audio")) throw new Error("The selected model advertises native audio, but this Kennedy transport cannot yet forward it.");
    const mimeType = blob.type || event.mimeType || "audio/ogg";
    const transcriptionStarted = performance.now();
    const result = await intelligence.transcribe({
      provider, model, file: blob, fileName: `telegram-voice.${audioExtension(mimeType)}`,
    });
    transcriptionDurationMs = Math.max(0, Math.round(performance.now() - transcriptionStarted));
    text = result.text;
    transcriptionModel = result.transcription_model;
    await telegramRelay.saveTranscription(event.id, text, transcriptionModel);
  }
  const mimeType = blob.type || event.mimeType || "audio/ogg";
  return {
    text,
    metadata: {
      externalEventId: event.id,
      inputKind: "voice",
      transcriptionModel,
      transcriptionDurationMs,
      media: {
        id: `telegram:${event.id}`,
        kind: "voice",
        source: "telegram",
        mimeType,
        fileName: `telegram-voice.${audioExtension(mimeType)}`,
        dataUrl: await blobToDataUrl(blob),
        sizeBytes: blob.size,
        durationSeconds: event.durationSeconds,
      },
    },
  };
}

async function telegramDocumentInput(event) {
  const blob = await telegramRelay.media(event.id);
  const fileName = event.fileName || "telegram-document";
  const extractionStarted = performance.now();
  const result = await intelligence.extractDocument({ file: blob, fileName });
  const extractionDurationMs = Math.max(0, Math.round(performance.now() - extractionStarted));
  return {
    text: event.text || "",
    metadata: {
      externalEventId: event.id,
      inputKind: "document",
      attachments: [{
        id: `telegram:${event.id}`,
        kind: "document",
        source: "telegram",
        fileName: result.file_name || fileName,
        mimeType: blob.type || event.mimeType || result.content_type || "application/octet-stream",
        sizeBytes: blob.size,
        dataUrl: await blobToDataUrl(blob),
        format: result.format,
        text: result.text,
        characters: result.characters,
        truncated: Boolean(result.truncated),
        extractionDurationMs,
      }],
    },
  };
}

function preparedGroupMessageText(message) {
  const base = typeof message.text === "string" ? message.text.trim() : "";
  if (message.kind === "voice") {
    const model = message.preparationModel ? ` · ${message.preparationModel}` : "";
    return [`[Voice note transcription${model}]`, message.preparedText || "Voice note transcription unavailable."].join("\n");
  }
  if (message.kind === "document") {
    const details = [
      `[Document: ${message.fileName || "telegram-document"}${message.documentFormat ? ` · ${message.documentFormat}` : ""}${message.preparationTruncated ? " · truncated" : ""}]`,
      message.preparedText || "Document text extraction unavailable.",
    ].join("\n");
    return [base, details].filter(Boolean).join("\n\n");
  }
  return base;
}

async function prepareTelegramGroupMessage(chatId, message) {
  if (!message || message.sentByKennedy || !["voice", "document"].includes(message.kind)) {
    return { ...message, text: preparedGroupMessageText(message || {}) };
  }
  const preparationKey = `${chatId}:${message.messageId}`;
  let preparation = telegramGroupPreparations.get(preparationKey);
  if (!preparation) {
    preparation = (async () => {
      const prepared = { ...message };
      if (!prepared.preparedText && prepared.hasMedia) {
        try {
          const blob = await telegramRelay.groupMessageMedia(chatId, prepared.messageId);
          if (prepared.kind === "voice") {
            if (inputModalities.includes("audio")) throw new Error("Native Telegram group audio forwarding is not implemented for this model transport.");
            const mimeType = blob.type || prepared.mimeType || "audio/ogg";
            const result = await intelligence.transcribe({
              provider, model, file: blob, fileName: prepared.fileName || `telegram-group-voice.${audioExtension(mimeType)}`,
            });
            prepared.preparedText = result.text;
            prepared.preparationModel = result.transcription_model;
          } else {
            const result = await intelligence.extractDocument({ file: blob, fileName: prepared.fileName || "telegram-document" });
            prepared.preparedText = result.text;
            prepared.documentFormat = result.format;
            prepared.preparationTruncated = Boolean(result.truncated);
          }
          await telegramRelay.saveGroupMessagePreparation(chatId, prepared.messageId, {
            text: prepared.preparedText,
            model: prepared.preparationModel || null,
            format: prepared.documentFormat || null,
            truncated: Boolean(prepared.preparationTruncated),
          });
        } catch (error) {
          prepared.preparedText = `${prepared.kind === "voice" ? "Voice transcription" : "Document extraction"} failed: ${error.message}`;
          prepared.preparationModel = "preparation-error";
          await telegramRelay.saveGroupMessagePreparation(chatId, prepared.messageId, {
            text: prepared.preparedText,
            model: prepared.preparationModel,
            truncated: false,
          }).catch(() => {});
        }
      }
      return prepared;
    })();
    telegramGroupPreparations.set(preparationKey, preparation);
    if (telegramGroupPreparations.size > 512) {
      telegramGroupPreparations.delete(telegramGroupPreparations.keys().next().value);
    }
  }
  const prepared = { ...message, ...await preparation };
  prepared.mediaRef = {
    kind: prepared.kind,
    source: "telegram-group",
    chatId,
    messageId: prepared.messageId,
    fileName: prepared.fileName || null,
    mimeType: prepared.mimeType || null,
    durationSeconds: prepared.durationSeconds || null,
  };
  prepared.text = preparedGroupMessageText(prepared);
  return prepared;
}

async function prepareTelegramGroupContext(groupContext, excludedMessageId = null) {
  if (!groupContext || !Array.isArray(groupContext.messages)) return groupContext;
  const messages = [];
  for (const message of groupContext.messages) {
    if (String(message.messageId) === String(excludedMessageId)) {
      messages.push(message);
    } else {
      messages.push(await prepareTelegramGroupMessage(groupContext.chatId, message));
    }
  }
  return { ...groupContext, messages };
}

async function closeTelegramGroupSessionSilently(updateRecord, session) {
  const latest = historyRecords.find(item => item.id === updateRecord.id) || updateRecord;
  const closed = await conversationHistory.requestIngress(latest.id, {
    expected_version: latest.version,
    state: session.snapshot(),
  });
  upsertHistory(closed);
  liveSessions.delete(latest.id);
  if (selectedConversationId === latest.id) update();
  await telegramRelay.completeSilentGroupReset(latest.id);
  kickHistoryIngress();
}

async function syncGroupSessionUpdates() {
  if (!telegramRelayReady || !conversationHistoryReady || !chatRuntimeReady()) return;
  const updates = (await telegramRelay.groupSessionUpdates()).updates || [];
  for (const pending of updates) {
    let record = historyRecords.find(item => item.id === pending.conversationId)
      || await conversationHistory.get(pending.conversationId).catch(() => null);
    if (!record || record.phase !== "active") {
      if (pending.resetRequired) await telegramRelay.completeSilentGroupReset(pending.conversationId);
      continue;
    }
    let session = liveSessions.get(record.id);
    if (!session) session = await buildConversation(record);
    if (session.busy || session.pendingTurn) continue;
    const groupContext = await prepareTelegramGroupContext({
      ...pending.groupContext,
      throughMessageId: pending.throughMessageId,
    });
    session.refreshTelegramGroupContext(groupContext);
    await session.persistSnapshot(session.snapshot());
    record = historyRecords.find(item => item.id === record.id) || record;
    if (pending.resetRequired) {
      await closeTelegramGroupSessionSilently(record, session);
    } else {
      await telegramRelay.acknowledgeGroupContext(record.id, pending.throughMessageId);
    }
  }
}

async function processTelegramEvent(event) {
  const processingStarted = performance.now();
  await directoryUserForEvent(event);
  if (event.sessionKind === "group" && event.groupContext) {
    event = {
      ...event,
      groupContext: await prepareTelegramGroupContext(event.groupContext, event.messageId),
    };
  }
  if (event.kind === "reset") {
    await processTelegramReset(event);
  } else {
    const { record, session } = await telegramConversationFor(event);
    let response = session.answerForExternalEvent(event.id);
    if (!response) {
      if (session.pendingTurn && session.pendingExternalEventId === event.id) {
        await session.resumePendingTurn();
      } else if (session.pendingTurn) {
        throw new Error("This Telegram session has an earlier saved query to finish.");
      } else if (event.kind === "voice") {
        const voice = await telegramVoiceInput(event);
        await session.send(voice.text, voice.metadata);
      } else if (event.kind === "document") {
        let document;
        try {
          document = await telegramDocumentInput(event);
        } catch (error) {
          await telegramRelay.reply(
            event.id,
            record.id,
            `I couldn't read ${event.fileName || "that document"}: ${error.message} Please try sending it again.`,
          );
          response = { content: "" };
        }
        if (document) await session.send(document.text, document.metadata);
      } else {
        await session.send(event.text || "", { externalEventId: event.id, inputKind: "text" });
      }
      if (!response) response = session.answerForExternalEvent(event.id);
    }
    if (!response) throw new Error("Kennedy completed the turn without a recoverable Telegram response.");
    if (response.content) await telegramRelay.reply(event.id, record.id, response.content, response.contextWarning || null);
  }
  const processingDurationMs = Math.max(0, Math.round(performance.now() - processingStarted));
  const receivedAt = Date.parse(event.createdAt);
  const durationMs = Number.isFinite(receivedAt)
    ? Math.max(processingDurationMs, Date.now() - receivedAt)
    : processingDurationMs;
  Promise.resolve(intelligence.recordTiming({
    action: "delivery", status: "ok", sessionType: event.sessionKind === "group" ? "telegram-group" : "telegram", durationMs, processingDurationMs,
  })).catch(() => {});
}

function groupMessageContent(message) {
  const handle = message.username ? ` @${String(message.username).replace(/^@/, "")}` : "";
  return `${message.displayName || "Telegram participant"}${handle}: ${message.text || ""}`;
}

function groupIngressState(batch) {
  const groupContext = {
    groupTitle: batch.groupTitle || "Telegram group",
    chatId: batch.chatId,
    invokingTelegramUserId: null,
    groupRootNodeId: batch.groupRootNodeId,
    groupRootReady: batch.groupRootReady,
    participants: batch.participants || [],
    messages: batch.messages || [],
  };
  if (typeof batch.groupRootNodeId !== "string" || !batch.groupRootNodeId) {
    throw new Error(`Telegram group ${batch.chatId} does not have a provisioned root.`);
  }
  const directRoots = [batch.groupRootNodeId, kennedyRootNodeId];
  const referenceRootNodeIds = referencesForGroup(groupContext, directRoots);
  const transcript = groupContext.messages.map(message => ({
    role: message.sentByKennedy ? "kennedy" : "user",
    content: message.sentByKennedy ? message.text : groupMessageContent(message),
  }));
  const messages = transcript.map(message => ({
    role: message.role === "kennedy" ? "assistant" : "user",
    content: message.content,
  }));
  const channel = {
    kind: "telegram-group",
    chatId: batch.chatId,
    groupIngressBatchId: batch.id,
    backgroundIngress: true,
    groupContext,
  };
  const archive = {
    format: "kennedy-chatend",
    version: 2,
    sessionType: "telegram-group",
    channel,
    rootNodeIds: directRoots,
    referenceRootNodeIds,
    startedAt: batch.createdAt,
    provider,
    model,
    systemPrompt: "",
    retained: messages,
    transcript,
    messages,
    fullHistory: { segments: [] },
    context: {},
    tools: { loadCalls: 0, loadLimit: 0, log: [] },
    usage: null,
    media: [],
  };
  return {
    stateVersion: 2,
    sessionType: "telegram-group",
    channel,
    rootNodeIds: directRoots,
    referenceRootNodeIds,
    startedAt: batch.createdAt,
    transcript,
    archive,
  };
}

async function syncGroupIngressBatches() {
  if (!telegramRelayReady || !conversationHistoryReady) return;
  await provisionDirectoryRoots();
  const batches = (await telegramRelay.groupIngress()).batches || [];
  for (const batch of batches) {
    if (!batch.groupRootReady) {
      const group = await provisionGroupRoot(batch.chatId);
      batch.groupTitle = group.title;
      batch.groupRootNodeId = group.rootNodeId;
      batch.groupRootReady = group.rootReady;
    }
    let record = historyRecords.find(item =>
      item.state?.channel?.groupIngressBatchId === batch.id
      || item.state?.archive?.channel?.groupIngressBatchId === batch.id
    );
    if (!record) {
      const state = groupIngressState(batch);
      record = await conversationHistory.create({ started_at: state.startedAt, state });
      upsertHistory(record);
    }
    if (record.phase === "active") {
      record = await conversationHistory.requestIngress(record.id, { expected_version: record.version, state: record.state });
      upsertHistory(record);
      kickHistoryIngress();
    } else if (record.phase === "complete") {
      await telegramRelay.completeGroupIngress(batch.id);
    }
  }
}

async function pollTelegramEvents() {
  await provisionDirectoryRoots();
  await syncGroupSessionUpdates();
  await syncGroupIngressBatches();
  const events = (await telegramRelay.events()).events || [];
  if (ui.service_status.textContent === "Telegram relay unavailable") ui.service_status.textContent = `Ready · ${model}`;
  await Promise.all(events.map(async event => {
    if (telegramInFlight.has(event.id)) return;
    telegramInFlight.add(event.id);
    try {
      await processTelegramEvent(event);
      await refreshHistory();
      if (ui.service_status.textContent === "Telegram queue needs attention") ui.service_status.textContent = `Ready · ${model}`;
    } catch (error) {
      console.error("Telegram event processing failed", event.id, error);
      ui.service_status.textContent = "Telegram queue needs attention";
      showError(ui.error_banner, `Telegram delivery will retry: ${error.message}`);
    } finally {
      telegramInFlight.delete(event.id);
    }
  }));
}

async function telegramBridgeLoop() {
  while (telegramBridgeRunning) {
    await pollTelegramEvents().catch(error => {
      console.error("Telegram relay poll failed", error);
      ui.service_status.textContent = "Telegram relay unavailable";
      showError(ui.error_banner, `Telegram relay polling will retry: ${error.message}`);
    });
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

function startTelegramBridge() {
  if (telegramBridgeRunning) return;
  telegramBridgeRunning = true;
  const run = () => telegramBridgeLoop();
  const work = navigator.locks?.request
    ? navigator.locks.request("kennedy-telegram-bridge", { ifAvailable: true }, lock => {
      if (lock) return run();
      telegramBridgeRunning = false;
      setTimeout(startTelegramBridge, 2000);
      return undefined;
    })
    : run();
  Promise.resolve(work).catch(error => console.error("Telegram bridge stopped", error));
}

function showView(view) {
  if (!["conversation", "telegram", "audio", "memory"].includes(view)) return;
  if (["conversation", "telegram"].includes(activeView)) selectedByView[activeView] = selectedConversationId;
  saveDraft();
  activeView = view;
  const memory = view === "memory";
  ui.chat_view.classList.toggle("hidden", memory);
  ui.memory_view.classList.toggle("hidden", !memory);
  ui.chat_tab.classList.toggle("active", view === "conversation");
  ui.tg_tab.classList.toggle("active", view === "telegram");
  ui.audio_tab.classList.toggle("active", view === "audio");
  ui.memory_tab.classList.toggle("active", memory);
  if (view === "audio") {
    if (!audioRecords.some(record => record.id === selectedAudioId)) selectedAudioId = audioRecords[0]?.id || null;
    update();
    refreshAudioHistory(true).catch(error => showError(ui.error_banner, error.message));
  } else if (!memory) {
    const records = recordsForView(view);
    const preferred = selectedByView[view];
    selectedConversationId = records.some(record => record.id === preferred) ? preferred : records[0]?.id || null;
    selectedByView[view] = selectedConversationId;
    restoreDraft();
    update();
  }
  if (memory && explorer && !explorer.currentNodeId) explorer.home();
}

async function initialize() {
  update();

  try {
    const [health, user] = await Promise.all([kweb.health(), kweb.user()]);
    legacyUserRootNodeId = user.user_root_node_id || user.root_node_id;
    kennedyRootNodeId = user.kennedy_root_node_id;
    rootNodeIds = [legacyUserRootNodeId, kennedyRootNodeId];
    if (rootNodeIds.some(id => typeof id !== "string" || !id)) throw new Error("Kweb did not provide both required root nodes.");
    kwebReady = true;
    ui.service_status.textContent = `${health.status} · memory ready`;
  } catch (error) {
    ui.service_status.textContent = "Kweb unavailable";
    showError(ui.error_banner, `Memory is unavailable: ${error.message}`);
  }

  if (kwebReady) {
    try {
      await telegramRelay.health();
      telegramRelayReady = true;
      await provisionDirectoryRoots();
    } catch (error) {
      telegramRelayReady = false;
      showError(ui.error_banner, `Telegram identity routing is unavailable; the web UI is using its legacy root until it recovers: ${error.message}`);
    }
    explorer = new MemoryExplorer({ api: kweb, rootNodeIds, content: ui.memory_content, backButton: ui.memory_back, forwardButton: ui.memory_forward });
  }

  if (kwebReady) {
    const loaded = await loadPromptManuals(CONFIG.kwebBase);
    manuals = loaded.manuals;
    const promptImpact = {
      identity: "Conversation and memory-ingress model sessions are unavailable",
      kmapBasics: "Conversation and memory-ingress model sessions are unavailable",
      readTools: "Conversation and memory-ingress model sessions are unavailable",
      conversationSession: "New and restored conversations are unavailable",
      freeTimeSession: "New and restored free-time sessions are unavailable",
      historyIngressSession: "Conversation-history memory ingress is paused",
      audioIngressSession: "Audio preparation and history remain available, but audio memory ingress is paused",
      codexHarness: "Codex-backed conversation and memory-ingress model sessions are unavailable",
      writeTools: "Conversation-history ingress, audio memory ingress, and free time are paused",
    };
    for (const [key, message] of Object.entries(loaded.errors)) {
      showError(ui.error_banner, `${promptImpact[key] || "A model mode is unavailable"}: ${message}`);
    }
  }

  try {
    await conversationHistory.health();
    await conversationHistory.discardUnstarted();
    historyRecords = sortConversationHistory((await conversationHistory.list()).conversations || []);
    conversationHistoryReady = true;
  } catch (error) {
    showError(ui.error_banner, `Conversation history is unavailable: ${error.message}`);
  }

  try {
    await audioIngress.health();
    audioRecords = (await audioIngress.list(50_000)).recordings || [];
    audioIngressReady = true;
  } catch (error) {
    showError(ui.error_banner, `Audio ingress is unavailable: ${error.message}`);
  }

  try {
    await intelligence.health();
    const providers = await intelligence.providers();
    provider = providers.default_provider;
    const selected = providers.providers.find(item => item.name === provider);
    if (!selected) throw new Error("The intelligence service did not provide its configured default provider.");
    providerKind = selected.kind;
    model = selected.default_model;
    reasoningEffort = selected.reasoning_effort;
    const modelCapabilities = selected.model_capabilities?.[model] || {};
    inputModalities = modelCapabilities.input_modalities || selected.input_modalities || ["text"];
    transcriptionAvailable = Boolean(selected.transcription_available);
    if (typeof providerKind !== "string" || !providerKind) throw new Error("The intelligence service did not identify the selected provider kind.");
    if (typeof reasoningEffort !== "string" || !reasoningEffort) throw new Error("The intelligence service did not provide the model thinking mode.");
    contextWindowTokens = Number(modelCapabilities.context_window_tokens ?? selected.context_window_tokens) || 0;
    maxInputTokens = Number(modelCapabilities.max_input_tokens ?? selected.max_input_tokens) || 0;
    if (contextWindowTokens <= 0 || maxInputTokens <= 0) throw new Error("The intelligence service did not provide the model's advertised effective context window.");
    intelligenceReady = true;
  } catch (error) {
    showError(ui.error_banner, `Kennedy's model service is unavailable: ${error.message}`);
  }

  const activeRecords = historyRecords.filter(record => record.phase === "active");
  if (chatRuntimeReady() || freeTimeRuntimeReady()) {
    for (const record of activeRecords) {
      if (record.state?.channel?.backgroundIngress || record.state?.archive?.channel?.backgroundIngress) continue;
      const freeTimeRecord = sessionTypeOf(record) === "free-time";
      if ((freeTimeRecord && !freeTimeRuntimeReady()) || (!freeTimeRecord && !chatRuntimeReady())) continue;
      try {
        await buildConversation(record);
      } catch (error) {
        showError(ui.error_banner, `Saved ${sessionTypeOf(record)} session ${record.id} could not be restored: ${error.message}`);
      }
    }
  }
  if (freeTimeRuntimeReady()) {
    for (const record of activeRecords.filter(record => sessionTypeOf(record) === "free-time" && liveSessions.has(record.id))) {
      freeTimeRun = freeTimeOf(record);
      launchFreeTimeRecord(record.id);
    }
  }
  if (chatRuntimeReady()) {
    const activeConversations = activeRecords.filter(record =>
      sessionTypeOf(record) === "conversation" && liveSessions.has(record.id)
    );
    if (activeConversations.length) {
      selectedConversationId = activeConversations[0].id;
      selectedByView.conversation = selectedConversationId;
      restoreDraft();
    } else {
      try {
        await createNewConversation();
      } catch (error) {
        showError(ui.error_banner, `A new conversation could not be started: ${error.message}`);
      }
    }
  } else {
    selectedConversationId = recordsForView("conversation")[0]?.id || null;
    selectedByView.conversation = selectedConversationId;
  }

  if (chatRuntimeReady() && telegramRelayReady) startTelegramBridge();

  kickHistoryIngress();
  const readyFeatures = [
    chatRuntimeReady() ? "chat" : null,
    kwebReady ? "memory" : null,
    audioIngressReady ? "audio" : null,
    telegramRelayReady ? "Telegram" : null,
  ].filter(Boolean);
  ui.service_status.textContent = readyFeatures.length
    ? `Ready · ${readyFeatures.join(", ")}${model ? ` · ${model}` : ""}`
    : "Kennedy services unavailable";

  if (!chatRuntimeReady() && !historyRecords.length && audioIngressReady) {
    showView("audio");
  } else {
    update();
  }
}

ui.message_form.addEventListener("submit", submitMessage);
ui.start_free_time.addEventListener("click", () => startFreeTime().catch(error => showError(ui.error_banner, error.message)));
ui.message_input.addEventListener("input", () => {
  if (!selectedSession()) return;
  drafts.set(selectedConversationId, ui.message_input.value);
  if (!ui.message_input.value.trim()) voiceDrafts.delete(selectedConversationId);
});
ui.message_input.addEventListener("keydown", event => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    event.preventDefault();
    ui.message_form.requestSubmit();
  }
});
ui.message_size_button.addEventListener("click", () => {
  setComposerExpanded(!ui.message_form.classList.contains("composer-expanded"));
  ui.message_input.focus();
});
ui.voice_button.addEventListener("click", () => toggleVoiceRecording());
ui.attach_button.addEventListener("click", () => ui.attachment_input.click());
ui.attachment_input.addEventListener("change", () => attachSelectedFiles());
ui.clear_attachments.addEventListener("click", () => clearAttachmentDraft());
ui.message_resize_handle.addEventListener("pointerdown", event => {
  if (event.button !== 0) return;
  event.preventDefault();
  composerResize = {
    pointerId: event.pointerId,
    startY: event.clientY,
    startHeight: ui.message_input.getBoundingClientRect().height,
  };
  ui.message_resize_handle.setPointerCapture(event.pointerId);
  ui.message_resize_handle.classList.add("resizing");
});
ui.message_resize_handle.addEventListener("pointermove", event => {
  if (!composerResize || event.pointerId !== composerResize.pointerId) return;
  setMessageInputHeight(composerResize.startHeight + composerResize.startY - event.clientY);
});
ui.message_resize_handle.addEventListener("pointerup", finishComposerResize);
ui.message_resize_handle.addEventListener("pointercancel", finishComposerResize);
ui.message_resize_handle.addEventListener("lostpointercapture", finishComposerResize);
ui.message_resize_handle.addEventListener("keydown", event => {
  const currentHeight = ui.message_input.getBoundingClientRect().height;
  const step = event.shiftKey ? 72 : 24;
  const { min, max } = composerHeightBounds();
  const requestedHeight = event.key === "ArrowUp" ? currentHeight + step
    : event.key === "ArrowDown" ? currentHeight - step
      : event.key === "Home" ? min
        : event.key === "End" ? max
          : null;
  if (requestedHeight === null) return;
  event.preventDefault();
  setMessageInputHeight(requestedHeight);
});
const messageInputResizeObserver = typeof ResizeObserver === "function" ? new ResizeObserver(syncComposerResizeValue) : null;
messageInputResizeObserver?.observe(ui.message_input);
setInterval(renderFreeTimeControls, 1_000);
ui.end_button.addEventListener("click", () => selectedSession()?.pendingTurn ? resumeSavedQuery() : endConversation());
ui.send_end_button.addEventListener("click", () => sendAndEndConversation());
ui.stop_button.addEventListener("click", () => stopConversationTurn());
ui.new_conversation.addEventListener("click", () => createNewConversation().catch(error => showError(ui.error_banner, error.message)));
ui.clear_log.addEventListener("click", () => clearError(ui.error_banner));
for (const mode of INSPECTOR_MODES) ui[`inspector_${mode}`].addEventListener("click", () => { inspectorMode = mode; update(); });
ui.chat_tab.addEventListener("click", () => showView("conversation"));
ui.tg_tab.addEventListener("click", () => showView("telegram"));
ui.audio_tab.addEventListener("click", () => showView("audio"));
ui.memory_tab.addEventListener("click", () => showView("memory"));
ui.memory_back.addEventListener("click", () => explorer?.goBack());
ui.memory_forward.addEventListener("click", () => explorer?.goForward());
ui.memory_home.addEventListener("click", () => explorer?.home());
ui.memory_kennedy_home.addEventListener("click", () => explorer?.kennedyHome());
ui.copy_context.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(inspectorText(diagnostic(), inspectorMode));
    ui.copy_context.textContent = "Copied";
    setTimeout(() => { ui.copy_context.textContent = "Copy view"; }, 1200);
  } catch {
    showError(ui.error_banner, "Could not copy Kennedy's context to the clipboard.");
  }
});

initialize().catch(error => {
  ui.service_status.textContent = "Startup failed";
  showError(ui.error_banner, `Kennedy could not initialize: ${error.message}`);
  console.error("Kennedy startup failed", error);
});
