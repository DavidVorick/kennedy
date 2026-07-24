import { KwebAPI, IntelligenceAPI, SessionHistoryAPI, AudioIngressAPI, TelegramRelayAPI, newIdempotencyId } from "./api.js?v=20260723.2";
import { MemoryExplorer } from "./memory_explorer.js?v=20260723.1";
import { renderTranscript, renderConversationHistory, renderAudioHistory, renderAudioRecording, conversationIngressActivity, renderInspector, renderUsage, inspectorText, showError, clearError, sortConversationHistory, reconcileConversationHistory, element } from "./render.js?v=20260724.1";
import { DEFAULT_FREE_TIME_MINUTES, formatFreeTimeRemaining, freeTimeTiming, parseFreeTimeMinutes, parseSelfTimePrompt } from "./self_time.js?v=20260720.2";

const CONFIG = {
  kwebBase: window.location.origin,
  intelligenceBase: window.location.origin,
  conversationHistoryBase: window.location.origin,
  telegramRelayBase: "http://127.0.0.1:4324",
  audioIngressBase: window.location.origin,
};

const ui = Object.fromEntries([
  "service-status", "self-time-panel", "self-time-prompt", "self-time-minutes", "start-self-time", "self-time-status", "chat-view", "memory-view", "chat-tab", "self-time-tab", "tg-tab", "audio-tab", "memory-tab", "transcript", "error-banner", "user-log-section", "clear-log", "message-form", "message-input", "message-resize-handle", "message-size-button", "send-button", "send-end-button", "retry-button", "stop-button", "voice-button", "attach-button", "attachment-input", "attachment-status", "clear-attachments", "end-button", "activity", "context-inspector", "copy-context", "usage-metrics", "inspector-main", "inspector-full", "inspector-history", "memory-content", "memory-back", "memory-forward", "memory-home", "memory-kennedy-home", "new-conversation", "conversation-history", "history-eyebrow", "history-title", "chatend-title",
].map(id => [id.replaceAll("-", "_"), document.getElementById(id)]));

const INSPECTOR_MODES = ["main", "full", "history"];
const kweb = KwebAPI(CONFIG.kwebBase);
const intelligence = IntelligenceAPI(CONFIG.intelligenceBase);
const conversationHistory = SessionHistoryAPI(CONFIG.conversationHistoryBase);
const telegramRelay = TelegramRelayAPI(CONFIG.telegramRelayBase);
const audioIngress = AudioIngressAPI(CONFIG.audioIngressBase);

let rootNodeIds = null;
let provider = null;
let model = null;
let inputModalities = ["text"];
let transcriptionAvailable = false;
let explorer = null;
let historyRecords = [];
let conversationCommandHeads = new Map();
let selectedConversationId = null;
let selectedByView = { conversation: null, "self-time": null, telegram: null };
let audioRecords = [];
let selectedAudioId = null;
let audioDetails = new Map();
let audioDetailLoading = new Set();
let audioDetailErrors = new Map();
let retryingAudioPieces = new Set();
let retryingAudioRecordings = new Set();
let retryingConversationIds = new Set();
let activeView = "conversation";
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
let freeTimeStarting = false;
let freeTimeStartPromise = null;
let backgroundRefreshRunning = false;
let kwebReady = false;
let conversationHistoryReady = false;
let audioIngressReady = false;
let telegramRelayReady = false;

function chatRuntimeReady() {
  return conversationHistoryReady;
}

function freeTimeRuntimeReady() {
  return conversationHistoryReady;
}

function sessionTypeOf(record) {
  return record?.state?.sessionType || record?.state?.archive?.sessionType || "conversation";
}

function viewForSessionType(sessionType) {
  if (sessionType === "free-time") return "self-time";
  return String(sessionType).startsWith("telegram") ? "telegram" : "conversation";
}

function freeTimeOf(record) {
  return record?.state?.freeTime || record?.state?.archive?.freeTime || null;
}

function activeFreeTimeRecord() {
  return historyRecords.find(record => record.phase === "active" && sessionTypeOf(record) === "free-time") || null;
}

function renderSelfTimeControls() {
  const activeRecord = activeFreeTimeRecord();
  const metadata = freeTimeOf(activeRecord);
  const active = Boolean(activeRecord);
  ui.self_time_prompt.disabled = active || freeTimeStarting;
  ui.self_time_minutes.disabled = active || freeTimeStarting;
  ui.start_self_time.disabled = active || freeTimeStarting || !freeTimeRuntimeReady();
  ui.start_self_time.textContent = freeTimeStarting ? "Starting…" : active ? "Self time running" : "Start self time";
  ui.start_self_time.setAttribute("aria-busy", String(freeTimeStarting));
  if (!metadata) {
    ui.self_time_status.textContent = active || freeTimeStarting ? "Backend is starting self time…" : "";
    return;
  }
  const savedPrompt = String(metadata.customPrompt || "");
  if (ui.self_time_prompt.value !== savedPrompt) ui.self_time_prompt.value = savedPrompt;
  if (!metadata.deadlineAt) {
    ui.self_time_status.textContent = "Backend is starting self time…";
    return;
  }
  try {
    const timing = freeTimeTiming(metadata);
    ui.self_time_status.textContent = timing.expired
      ? `Wrapping up · hard stop in ${formatFreeTimeRemaining(timing.hardStopMs - Date.now())}`
      : `One run · slice ${metadata.sliceIndex} · ${formatFreeTimeRemaining(timing.remainingMs)} left`;
  } catch {
    ui.self_time_status.textContent = "Schedule unavailable";
  }
}

function recordsForView(view = activeView) {
  return sortConversationHistory(historyRecords.filter(record => viewForSessionType(sessionTypeOf(record)) === view));
}

function selectedRecord() {
  return historyRecords.find(record => record.id === selectedConversationId) || null;
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

function isSessionLogArchive(value) {
  return typeof value?.header?.formatVersion === "string"
    && typeof value?.header?.sessionId === "string"
    && Array.isArray(value?.events);
}

function decodedSessionEvent(event) {
  if (typeof event?.text !== "string") return null;
  try {
    const decoded = JSON.parse(event.text);
    return decoded && typeof decoded === "object" ? decoded : null;
  } catch {
    return null;
  }
}

function sessionEventMessage(event, position) {
  const decoded = decodedSessionEvent(event);
  const kind = decoded?.kind;
  const type = kind?.type;
  let content = event?.text || "";
  let displayRole = null;
  if (type === "box_created") {
    content = kind?.content?.text || "";
    displayRole = kind?.name || null;
  } else if (type === "tool_invoked") {
    content = JSON.stringify({
      name: kind?.tool_name,
      arguments: kind?.arguments || {},
    }, null, 2);
    displayRole = `Tool call · ${kind?.tool_name || "unknown"}`;
  } else if (type === "tool_completed") {
    content = typeof kind?.outcome?.result === "string"
      ? kind.outcome.result : JSON.stringify(kind?.outcome || {}, null, 2);
    displayRole = `Tool result · ${kind?.tool_name || "unknown"}`;
  } else if (decoded) {
    content = JSON.stringify(kind || decoded, null, 2);
  }
  const role = event?.role === "user-message" ? "user"
    : event?.role === "kennedy-message" ? "assistant" : "system";
  return {
    role,
    content,
    display_role: displayRole || `Event ${position + 1} · ${event?.role || "unknown"}`,
    context_kind: [
      "kennedy-tool-call", "tool-result", "tool-error", "object", "pending-object",
    ].includes(event?.role) ? "box" : null,
  };
}

function sessionLogDiagnostic(archive, mode) {
  const messages = archive.events.map(sessionEventMessage);
  return {
    mode, provider, model,
    chatend: messages,
    chatendText: messages
      .map(message => `[${message.display_role}]\n${message.content}`)
      .join("\n\n"),
    context: { boxCount: 0, eventCount: archive.events.length, staleBoxes: [] },
    loadCalls: 0,
    loadLimit: 0,
    toolLog: [],
    usage: null,
    memory: EMPTY_MEMORY,
    historySegments: [],
    events: archive.events,
    boxes: [],
  };
}

function archivedDiagnostic(archive, mode, transcript = []) {
  if (isSessionLogArchive(archive)) return sessionLogDiagnostic(archive, mode);
  if (archive?.version === 1 && archive?.boxes) return boxDiagnostic(archive, mode);
  return {
    mode, provider, model,
    chatend: archive?.messages || transcript.map(item => ({
      role: item.role === "kennedy" ? "assistant" : item.role === "system" ? "system" : "user",
      content: item.content,
    })),
    chatendText: typeof archive?.chatendText === "string" ? archive.chatendText : null,
    context: archive?.context?.diagnostics || {},
    loadCalls: archive?.tools?.loadCalls || 0,
    loadLimit: archive?.tools?.loadLimit || 0,
    toolLog: archive?.tools?.log || [],
    usage: archive?.usage || null,
    memory: archive?.context?.snapshot || EMPTY_MEMORY,
    historySegments: archive?.fullHistory?.segments || [],
  };
}

function boxDiagnostic(source, mode) {
  const context = source?.context || {};
  const projectedText = Array.isArray(context.items)
    ? [...context.items.map(item => item?.text).filter(text => typeof text === "string"), context.footer]
        .filter(text => typeof text === "string" && text.length)
        .join("\n\n")
    : null;
  const projectedOrder = new Map(
    (context.items || [])
      .filter(item => item?.marker !== true)
      .map((item, index) => [Number(item.boxId), index]),
  );
  const boxes = Object.values(source?.boxes || {});
  const messages = boxes
    .filter(box => box?.active !== false)
    .sort((left, right) => {
      const leftProjected = projectedOrder.get(Number(left?.id));
      const rightProjected = projectedOrder.get(Number(right?.id));
      if (leftProjected !== undefined || rightProjected !== undefined) {
        return (leftProjected ?? Number.MAX_SAFE_INTEGER) - (rightProjected ?? Number.MAX_SAFE_INTEGER);
      }
      return Number(left?.occurrenceEvents?.at(-1) || 0) - Number(right?.occurrenceEvents?.at(-1) || 0);
    })
    .map(box => {
      const representation = box?.representation || {};
      const stale = representation.kind !== "hydrated"
        && Number(representation.based_on) !== Number(box?.canonical?.eventId);
      let content = box?.canonical?.content?.text || "";
      if (representation.kind === "dehydrated") content = `[Box ${box.id} is dehydrated.]`;
      if (representation.kind === "summarized") content = representation.text || "";
      if (stale) content = `[Stale representation]\n\n${content}`;
      const owner = box?.owner || {};
      const kweb = owner.kind === "tool" && owner.tool_instance === "kweb";
      return {
        role: owner.kind === "kennedy" ? "assistant"
          : owner.kind === "user" ? "user" : "system",
        context_kind: kweb ? "kweb-box"
          : owner.kind === "system" ? "instructions"
          : owner.kind === "user" || (owner.kind === "kennedy" && box.name === "Kennedy message") ? null
          : "box",
        display_role: `Box ${box.id} · ${box.name || "Context"}`,
        content,
      };
    });
  const effective = Number(String(context.footer || "").match(/effective=(\d+)/)?.[1]) || 0;
  return {
    mode, provider, model,
    chatend: messages,
    chatendText: typeof source?.chatendText === "string" ? source.chatendText : projectedText,
    context: {
      boxCount: boxes.length,
      eventCount: Array.isArray(source?.events) ? source.events.length : 0,
      staleBoxes: context.staleBoxes || [],
    },
    loadCalls: 0,
    loadLimit: 0,
    toolLog: [],
    usage: {
      contextKnown: true,
      contextTokens: context.estimatedTokens || 0,
      contextWindowTokens: effective,
    },
    memory: EMPTY_MEMORY,
    historySegments: [],
    events: source?.events || [],
    boxes,
  };
}

function conversationDiagnostic(record) {
  if (!record) return null;
  if (record.state?.boxes && !Array.isArray(record.state.boxes)) {
    return boxDiagnostic(record.state, "session Chatend");
  }
  const transcript = Array.isArray(record.state?.transcript) ? record.state.transcript : [];
  const archive = isSessionLogArchive(record.state?.archive)
    || record.state?.archive?.format === "kennedy-chatend"
    ? record.state.archive : null;
  return archivedDiagnostic(archive, "saved conversation", transcript);
}

function historyIngressDiagnostic(record) {
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
    current: source ? { messages: source.chatend, chatendText: source.chatendText, memory: source.memory, usage: source.usage } : null,
  };
}

function diagnostic() {
  if (activeView === "audio") return audioRecordingDiagnostic();
  const record = selectedRecord();
  const conversation = conversationDiagnostic(record);
  const ingress = historyIngressDiagnostic(record);
  const status = ingressStatus(record, ingress);
  const current = ingress || conversation || {
    mode: "offline", provider, model, chatend: [], context: {}, loadCalls: 0, loadLimit: 0,
    toolLog: [], usage: null, memory: EMPTY_MEMORY, historySegments: [],
  };
  const phases = [];
  if (conversation) phases.push(historyPhase(sessionTypeOf(record) === "free-time" ? "Self time" : "Conversation", record?.phase === "active" ? "live" : "closed", conversation));
  if (status) phases.push(historyPhase("History ingress", status, ingress));
  return { ...current, ingressStatus: status, fullHistory: { phases } };
}

function audioPieceDiagnostic(piece) {
  const archive = piece?.state?.historyIngress;
  return archive?.format === "kennedy-chatend"
    ? archivedDiagnostic(archive, "audio ingress")
    : null;
}

function audioPieceIngressActivity(piece) {
  const currentPiece = piece;
  let diagnostic = null;
  const source = audioPieceDiagnostic(currentPiece);
  if (source) {
    diagnostic = {
      chatend: { messages: source.chatend || [] },
      usage: { snapshot: () => source.usage || null },
      toolLog: source.toolLog || [],
    };
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
      mode: "audio ingress", provider, model, chatend: [], context: {}, loadCalls: 0, loadLimit: 0,
      toolLog: [], usage: null, memory: EMPTY_MEMORY, historySegments: [],
    };
  return { ...current, fullHistory: { phases } };
}

function visibleIngressActivity() {
  const record = selectedRecord();
  return conversationIngressActivity({
    record,
    savedDiagnostic: historyIngressDiagnostic(record),
  });
}

function update() {
  renderSelfTimeControls();
  ui.self_time_panel.classList.toggle("hidden", activeView !== "self-time");
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
  const viewingHistory = Boolean(record);
  const telegramView = activeView === "telegram";
  const selfTimeView = activeView === "self-time";
  const freeTimeView = sessionTypeOf(record) === "free-time";
  const ingressActivity = visibleIngressActivity();
  renderTranscript(
    ui.transcript,
    record?.state?.transcript || [],
    ingressActivity,
    `${activeView}:${selectedConversationId || "none"}`,
    record?.phase === "ingress_failed"
      ? { retrying: retryingConversationIds.has(record.id), onRetry: () => retryConversationIngress(record) }
      : null,
  );
  if (telegramView && !(record?.state?.transcript || []).length && !ingressActivity?.diagnostic) {
    ui.transcript.replaceChildren(element("div", "telegram-empty", "Telegram conversations appear here as messages arrive. Kennedy's backend continues answering even when no browser is open."));
  } else if (selfTimeView && !(record?.state?.transcript || []).length && !ingressActivity?.diagnostic) {
    ui.transcript.replaceChildren(element("div", "empty-state", "Start a self-time run above. Kennedy can follow your optional prompt or explore freely, and every clean-slate slice will remain visible here."));
  }
  renderConversationHistory(ui.conversation_history, recordsForView(), {
    selectedId: selectedConversationId,
    onSelect: id => selectConversation(id).catch(error => showError(ui.error_banner, error.message)),
    retryingIds: retryingConversationIds,
    onRetryIngress: retryConversationIngress,
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
  const activeConversation = activeView === "conversation" && record?.phase === "active"
    && sessionTypeOf(record) === "conversation";
  const pendingTurn = Boolean(record?.state?.pendingTurn || record?.state?.archive?.pendingTurn);
  const backendStatus = record?.state?.orchestration?.status || record?.state?.archive?.orchestration?.status;
  const commandHead = conversationCommandHeads.get(record?.id) || null;
  const busy = Boolean(commandHead) || (pendingTurn && backendStatus !== "stopped");
  const retryable = pendingTurn && backendStatus === "stopped" && !commandHead;
  const stoppable = commandHead?.status === "processing"
    && ["message", "retry"].includes(commandHead.kind)
    && !commandHead.cancelRequested;
  const transitionBusy = creatingConversation || endingIds.has(selectedConversationId);
  const endingConversation = endingIds.has(selectedConversationId)
    || ["end", "send-and-end"].includes(commandHead?.kind);
  const composerHidden = !activeConversation || telegramView || selfTimeView || freeTimeView;
  const extractingAttachment = extractingAttachments.has(selectedConversationId);
  ui.message_form.classList.toggle("hidden", composerHidden);
  ui.message_input.disabled = !activeConversation || transitionBusy;
  ui.send_button.disabled = !activeConversation || busy || transitionBusy || extractingAttachment;
  ui.send_end_button.disabled = !activeConversation || busy || transitionBusy || extractingAttachment;
  ui.end_button.disabled = !activeConversation || transitionBusy || endingConversation;
  ui.retry_button.classList.toggle("hidden", !activeConversation || !retryable);
  ui.retry_button.disabled = !activeConversation || !retryable || transitionBusy;
  ui.stop_button.classList.toggle("hidden", !activeConversation || !stoppable);
  ui.stop_button.disabled = false;
  ui.stop_button.textContent = "Stop Kennedy";
  ui.new_conversation.disabled = creatingConversation || !chatRuntimeReady();
  ui.new_conversation.classList.toggle("hidden", telegramView || selfTimeView);
  ui.voice_button.disabled = !activeConversation || busy || transitionBusy || !transcriptionAvailable
    || !navigator.mediaDevices?.getUserMedia || typeof MediaRecorder !== "function";
  const attachments = attachmentDrafts.get(selectedConversationId) || [];
  ui.attach_button.disabled = !activeConversation || busy || transitionBusy || extractingAttachment;
  ui.attachment_status.textContent = attachments.length
    ? `${attachments.length} attached: ${attachments.map(item => item.fileName).join(", ")}`
    : "PDF, Word, spreadsheet, or text";
  ui.clear_attachments.classList.toggle("hidden", !attachments.length);
  ui.clear_attachments.disabled = !activeConversation || busy || transitionBusy || extractingAttachment;
  ui.history_eyebrow.textContent = telegramView ? "TELEGRAM SESSIONS" : selfTimeView ? "SELF-TIME SESSIONS" : "YOUR CONVERSATIONS";
  ui.history_title.textContent = telegramView ? "Bot chats" : selfTimeView ? "Self time" : "History";
  ui.chatend_title.textContent = currentDiagnostic.mode === "history ingress"
    ? `History ingress · ${currentDiagnostic.ingressStatus || "in progress"}`
    : freeTimeView ? "Self-time Chatend"
    : telegramView ? "Telegram Chatend" : currentDiagnostic.ingressStatus
      ? `Chatend · ingress ${currentDiagnostic.ingressStatus}`
      : "Chatend";
  ui.end_button.textContent = endingConversation ? "Ending conversation…" : "End conversation";
  ui.activity.textContent = freeTimeView
    ? record?.phase === "active" ? "Kennedy's backend is running self time" : "This self-time session is closed"
    : telegramView
    ? pendingTurn ? "Kennedy's backend is answering a Telegram message" : "Messages are delivered automatically"
    : record?.phase !== "active"
      ? "This conversation is closed and read only"
      : commandHead?.cancelRequested
          ? "Kennedy's backend is stopping"
          : commandHead?.status === "pending"
            ? "Your request is queued in Kennedy's backend"
            : backendStatus === "stopped"
              ? "Saved query needs a response — retry when ready"
              : busy ? "Kennedy's backend is working — you can draft your next message" : "";
}

function upsertHistory(record) {
  if (!record) return;
  historyRecords = sortConversationHistory([record, ...historyRecords.filter(item => item.id !== record.id)]);
}

async function hydrateHistoryRecord(recordOrId) {
  const id = typeof recordOrId === "string" ? recordOrId : recordOrId?.id;
  if (!id) throw new Error("Session History record is missing an ID.");
  const cached = historyRecords.find(item => item.id === id);
  if (cached && !cached.summary) return cached;
  let record = await conversationHistory.get(id);
  const objectId = record?.state?.sessionObjectId;
  if (record?.phase === "complete" && objectId) {
    const archive = await kweb.sessionArchive(objectId);
    record = {
      ...record,
      started_at: archive?.header?.createdAt || archive?.startedAt || archive?.session?.createdAt || record.started_at,
      state: {
        archive,
        sessionType: record.state?.sessionType
          || archive?.sessionType || archive?.session?.kind || "conversation",
        sessionObjectId: objectId,
      },
    };
  }
  upsertHistory(record);
  return record;
}

function saveDraft() {
  if (activeView === "conversation" && selectedRecord()?.phase === "active") drafts.set(selectedConversationId, ui.message_input.value);
}

function restoreDraft() {
  const record = selectedRecord();
  ui.message_input.value = activeView === "conversation" && record?.phase === "active"
    ? (drafts.get(selectedConversationId) || "")
    : "";
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

const MAX_ATTACHMENT_BYTES = 32 * 1024 * 1024 * 1024;

async function attachSelectedFiles() {
  const id = selectedConversationId;
  const files = Array.from(ui.attachment_input.files || []);
  ui.attachment_input.value = "";
  if (!files.length || selectedRecord()?.phase !== "active" || activeView !== "conversation") return;
  const existing = attachmentDrafts.get(id) || [];
  const oversized = files.find(file => !file.size || file.size > MAX_ATTACHMENT_BYTES);
  if (oversized) {
    showError(ui.error_banner, `${oversized.name} must be between 1 byte and 32 GiB.`);
    return;
  }
  const totalBytes = [...existing, ...files].reduce((total, item) => total + (Number(item.sizeBytes ?? item.size) || 0), 0);
  if (totalBytes > MAX_ATTACHMENT_BYTES) {
    showError(ui.error_banner, "Attachments for one session transaction must total 32 GiB or less.");
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
      const staged = await conversationHistory.stageObject(id, file, file.name);
      extracted.push({
        id: crypto.randomUUID(),
        kind: "document",
        fileName: result.file_name || file.name,
        mimeType: file.type || result.content_type || "application/octet-stream",
        sizeBytes: file.size,
        pendingId: staged.pendingId,
        format: result.format,
        text: result.text,
        characters: result.characters,
        truncated: Boolean(result.truncated),
        extractionDurationMs: Math.max(0, Math.round(performance.now() - started)),
      });
    }
    if (selectedConversationId === id && selectedRecord()?.phase === "active") {
      attachmentDrafts.set(id, [...existing, ...extracted]);
    }
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
    const staged = await conversationHistory.stageObject(id, blob, fileName);
    voiceDrafts.set(id, {
      inputKind: "voice",
      transcriptionModel: result.transcription_model,
      transcriptionDurationMs,
      media: { id: crypto.randomUUID(), kind: "voice", mimeType, fileName, pendingId: staged.pendingId, sizeBytes: blob.size },
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

function reconcileHistory(records) {
  historyRecords = reconcileConversationHistory(historyRecords, records);
  for (const id of endingIds) {
    if (!historyRecords.some(record => record.id === id && record.phase === "active")) {
      endingIds.delete(id);
    }
  }
}

async function refreshHistory() {
  const [history, commands] = await Promise.all([
    conversationHistory.list(),
    conversationHistory.commandHeads(),
  ]);
  const records = history.conversations || [];
  conversationCommandHeads = new Map((commands.commands || []).map(command => [command.conversationId, command]));
  reconcileHistory(records);
  const completedSummaries = historyRecords.filter(
    record => record.phase === "complete" && record.summary,
  );
  if (completedSummaries.length) {
    const results = await Promise.allSettled(
      completedSummaries.map(record => hydrateHistoryRecord(record)),
    );
    const failure = results.find(
      result => result.status === "rejected" && result.reason?.code !== "not_found",
    );
    if (failure) throw failure.reason;
  }
  const selected = historyRecords.find(record => record.id === selectedConversationId);
  if (selected?.summary) {
    try { await hydrateHistoryRecord(selected); } catch (error) {
      if (error?.code !== "not_found") throw error;
    }
  }
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
    const latest = await hydrateHistoryRecord(record);
    const retried = await conversationHistory.retryIngress(record.id, {
      expected_version: latest.version,
      state: freshIngressState(latest.state),
    });
    upsertHistory(retried);
  } catch (error) {
    showError(ui.error_banner, `History ingress could not be retried: ${error.message}`);
  } finally {
    retryingConversationIds.delete(record.id);
    update();
  }
}

async function createNewConversation() {
  if (creatingConversation) return;
  if (!chatRuntimeReady()) throw new Error("A new conversation cannot start until Session History is available.");
  creatingConversation = true;
  saveDraft();
  update();
  try {
    const record = await conversationHistory.start({
      idempotency_id: newIdempotencyId(),
      started_at: new Date().toISOString(),
      session_type: "conversation",
    });
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

async function startFreeTime() {
  if (freeTimeStartPromise) return freeTimeStartPromise;
  if (activeFreeTimeRecord()) return null;
  freeTimeStarting = true;
  update();
  const work = startFreeTimeIntent();
  freeTimeStartPromise = work;
  try {
    return await work;
  } finally {
    if (freeTimeStartPromise === work) freeTimeStartPromise = null;
    freeTimeStarting = false;
    update();
  }
}

async function startFreeTimeIntent() {
  await refreshHistory();
  if (activeFreeTimeRecord()) return null;
  if (!freeTimeRuntimeReady()) throw new Error("Self time cannot start until Session History is available.");
  const durationMinutes = parseFreeTimeMinutes(ui.self_time_minutes.value || DEFAULT_FREE_TIME_MINUTES);
  const customPrompt = parseSelfTimePrompt(ui.self_time_prompt.value);
  const record = await conversationHistory.start({
    idempotency_id: newIdempotencyId(),
    started_at: new Date().toISOString(),
    session_type: "free-time",
    duration_minutes: durationMinutes,
    custom_prompt: customPrompt,
  });
  upsertHistory(record);
  if (activeView !== "self-time") showView("self-time");
  selectedConversationId = record.id;
  selectedByView["self-time"] = record.id;
  update();
}

async function selectConversation(id) {
  const cached = historyRecords.find(item => item.id === id);
  if (id === selectedConversationId && cached && !cached.summary) return;
  saveDraft();
  selectedConversationId = id;
  let record = cached;
  update();
  record = await hydrateHistoryRecord(record || id);
  selectedByView[viewForSessionType(sessionTypeOf(record))] = id;
  restoreDraft();
  update();
  ui.transcript.scrollTop = ui.transcript.scrollHeight;
  if (activeView === "conversation" && record.phase === "active") ui.message_input.focus();
  const error = conversationErrors.get(id);
  if (error) showError(ui.error_banner, error);
}

async function submitMessage(event) {
  event.preventDefault();
  const record = selectedRecord();
  const id = selectedConversationId;
  if (!record || record.phase !== "active" || endingIds.has(id) || extractingAttachments.has(id)) return;
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
    const command = await conversationHistory.queueCommand(id, {
      idempotency_id: newIdempotencyId(),
      kind: "message",
      payload: { text, metadata },
    });
    conversationCommandHeads.set(id, command);
    await refreshHistory().catch(error => {
      showError(ui.error_banner, `The message was queued, but the refreshed backend view is delayed: ${error.message}`);
    });
  } catch (error) {
    const message = error.message || "The message could not be queued for Kennedy's backend.";
    conversationErrors.set(id, message);
    if (selectedConversationId === id) showError(ui.error_banner, message);
  }
  update();
}

async function resumeSavedQuery(id = selectedConversationId) {
  const record = historyRecords.find(item => item.id === id);
  if (!record || record.phase !== "active") return;
  conversationErrors.delete(id);
  try {
    const command = await conversationHistory.queueCommand(id, {
      idempotency_id: newIdempotencyId(),
      kind: "retry",
      payload: {},
    });
    conversationCommandHeads.set(id, command);
  } catch (error) {
    const message = `The saved query could not be resumed: ${error.message}`;
    conversationErrors.set(id, message);
    if (selectedConversationId === id) showError(ui.error_banner, message);
  }
  update();
}

async function stopConversationTurn() {
  const id = selectedConversationId;
  if (!selectedRecord() || selectedRecord().phase !== "active") return;
  conversationErrors.delete(id);
  try {
    const stopped = await conversationHistory.stop(id);
    const command = conversationCommandHeads.get(id);
    if (stopped.stop_requested && command) {
      conversationCommandHeads.set(id, { ...command, cancelRequested: true });
    }
  } catch (error) {
    showError(ui.error_banner, `Kennedy could not be stopped cleanly: ${error.message}`);
  }
  update();
}

async function endConversation() {
  const id = selectedConversationId;
  const record = selectedRecord();
  if (!record || record.phase !== "active" || endingIds.has(id)) return;
  endingIds.add(id);
  update();
  let queued = false;
  try {
    const command = await conversationHistory.queueCommand(id, {
      idempotency_id: newIdempotencyId(),
      kind: "end",
      payload: {},
    });
    conversationCommandHeads.set(id, command);
    queued = true;
  } catch (error) {
    showError(ui.error_banner, error.message);
  } finally {
    if (!queued) endingIds.delete(id);
    update();
  }
}

async function sendAndEndConversation() {
  const id = selectedConversationId;
  const record = selectedRecord();
  if (!record || record.phase !== "active" || record.state?.pendingTurn || endingIds.has(id) || extractingAttachments.has(id)) return;
  const text = ui.message_input.value;
  const attachments = attachmentDrafts.get(id) || [];
  if (!text.trim() && !attachments.length) return;
  const metadata = { ...(voiceDrafts.get(id) || {}), attachments };
  endingIds.add(id);
  update();
  try {
    const command = await conversationHistory.queueCommand(id, {
      idempotency_id: newIdempotencyId(),
      kind: "send-and-end",
      payload: { text, metadata },
    });
    conversationCommandHeads.set(id, command);
    ui.message_input.value = "";
    drafts.set(id, "");
    voiceDrafts.delete(id);
    attachmentDrafts.delete(id);
  } catch (error) {
    showError(ui.error_banner, `The final message was not fully closed into history ingress: ${error.message}`);
  } finally {
    endingIds.delete(id);
    update();
  }
}

function showView(view) {
  if (!["conversation", "self-time", "telegram", "audio", "memory"].includes(view)) return;
  if (["conversation", "self-time", "telegram"].includes(activeView)) selectedByView[activeView] = selectedConversationId;
  saveDraft();
  activeView = view;
  const memory = view === "memory";
  ui.chat_view.classList.toggle("hidden", memory);
  ui.memory_view.classList.toggle("hidden", !memory);
  ui.chat_tab.classList.toggle("active", view === "conversation");
  ui.self_time_tab.classList.toggle("active", view === "self-time");
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
  if (!memory) void refreshObservedState();
}

async function refreshObservedState() {
  if (backgroundRefreshRunning) return;
  backgroundRefreshRunning = true;
  try {
    if (conversationHistoryReady) {
      await refreshHistory();
      const records = recordsForView();
      if (!records.some(record => record.id === selectedConversationId)) {
        selectedConversationId = records[0]?.id || null;
        selectedByView[activeView] = selectedConversationId;
      }
    }
    if (audioIngressReady && activeView === "audio") {
      await refreshAudioHistory(true);
    }
  } catch (error) {
    showError(ui.error_banner, `Backend view refresh will retry: ${error.message}`);
  } finally {
    backgroundRefreshRunning = false;
  }
}

async function initialize() {
  update();

  try {
    const [health, roots] = await Promise.all([kweb.health(), kweb.roots()]);
    rootNodeIds = [roots.user_root_node_id, roots.kennedy_root_node_id];
    if (rootNodeIds.some(id => typeof id !== "string" || !id)) {
      throw new Error("Kweb did not provide both required root nodes.");
    }
    kwebReady = true;
    ui.service_status.textContent = `${health.status} · memory ready`;
    explorer = new MemoryExplorer({
      api: kweb,
      rootNodeIds,
      content: ui.memory_content,
      backButton: ui.memory_back,
      forwardButton: ui.memory_forward,
    });
  } catch (error) {
    ui.service_status.textContent = "Kweb unavailable";
    showError(ui.error_banner, `Memory is unavailable: ${error.message}`);
  }

  try {
    await conversationHistory.health();
    const [history, commands] = await Promise.all([
      conversationHistory.list(),
      conversationHistory.commandHeads(),
    ]);
    historyRecords = sortConversationHistory(history.conversations || []);
    conversationCommandHeads = new Map((commands.commands || []).map(command => [command.conversationId, command]));
    conversationHistoryReady = true;
  } catch (error) {
    showError(ui.error_banner, `Session History is unavailable: ${error.message}`);
  }

  try {
    await audioIngress.health();
    audioRecords = (await audioIngress.list(50_000)).recordings || [];
    audioIngressReady = true;
  } catch (error) {
    showError(ui.error_banner, `Audio ingress is unavailable: ${error.message}`);
  }

  try {
    await telegramRelay.health();
    telegramRelayReady = true;
  } catch (error) {
    showError(ui.error_banner, `Telegram status is unavailable: ${error.message}`);
  }

  try {
    await intelligence.health();
    const providers = await intelligence.providers();
    provider = providers.default_provider;
    const selected = providers.providers.find(item => item.name === provider);
    if (!selected) throw new Error("The intelligence service did not provide its configured default provider.");
    model = selected.default_model;
    const modelCapabilities = selected.model_capabilities?.[model] || {};
    inputModalities = modelCapabilities.input_modalities || selected.input_modalities || ["text"];
    transcriptionAvailable = Boolean(selected.transcription_available);
  } catch (error) {
    showError(ui.error_banner, `Kennedy's model service is unavailable: ${error.message}`);
  }

  if (conversationHistoryReady) {
    const activeConversations = recordsForView("conversation").filter(record => record.phase === "active");
    if (activeConversations.length) {
      selectedConversationId = activeConversations[0].id;
      selectedByView.conversation = selectedConversationId;
      await hydrateHistoryRecord(selectedConversationId).catch(() => null);
      restoreDraft();
    } else {
      try {
        await createNewConversation();
      } catch (error) {
        showError(ui.error_banner, `A new conversation could not be requested: ${error.message}`);
      }
    }
  }

  const readyFeatures = [
    conversationHistoryReady ? "backend chat" : null,
    kwebReady ? "memory" : null,
    audioIngressReady ? "audio" : null,
    telegramRelayReady ? "Telegram" : null,
  ].filter(Boolean);
  ui.service_status.textContent = readyFeatures.length
    ? `Ready · ${readyFeatures.join(", ")}${model ? ` · ${model}` : ""}`
    : "Kennedy services unavailable";

  if (!conversationHistoryReady && !historyRecords.length && audioIngressReady) showView("audio");
  else update();
}
ui.message_form.addEventListener("submit", submitMessage);
ui.start_self_time.addEventListener("click", () => startFreeTime().catch(error => showError(ui.error_banner, error.message)));
ui.message_input.addEventListener("input", () => {
  if (activeView !== "conversation" || selectedRecord()?.phase !== "active") return;
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
setInterval(renderSelfTimeControls, 1_000);
setInterval(() => { void refreshObservedState(); }, 1_000);
ui.end_button.addEventListener("click", () => endConversation());
ui.retry_button.addEventListener("click", () => resumeSavedQuery());
ui.send_end_button.addEventListener("click", () => sendAndEndConversation());
ui.stop_button.addEventListener("click", () => stopConversationTurn());
ui.new_conversation.addEventListener("click", () => createNewConversation().catch(error => showError(ui.error_banner, error.message)));
ui.clear_log.addEventListener("click", () => clearError(ui.error_banner));
for (const mode of INSPECTOR_MODES) ui[`inspector_${mode}`].addEventListener("click", () => { inspectorMode = mode; update(); });
ui.chat_tab.addEventListener("click", () => showView("conversation"));
ui.self_time_tab.addEventListener("click", () => showView("self-time"));
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
