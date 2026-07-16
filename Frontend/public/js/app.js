import { KwebAPI, IntelligenceAPI, ConversationHistoryAPI, AudioIngressAPI, TelegramRelayAPI } from "./api.js?v=20260716.3";
import { loadPromptManuals } from "./prompt_composer.js?v=20260716.2";
import { ConversationSession } from "./conversation.js?v=20260715.11";
import { runHistoryIngress } from "./history_ingress.js?v=20260716.1";
import { MemoryExplorer } from "./memory_explorer.js?v=20260714.7";
import { renderTranscript, renderConversationHistory, renderAudioHistory, renderAudioRecording, conversationControlState, conversationIngressActivity, renderInspector, renderUsage, inspectorText, showError, clearError, element } from "./render.js?v=20260716.4";

const CONFIG = {
  kwebBase: window.location.origin,
  intelligenceBase: "http://127.0.0.1:4322",
  conversationHistoryBase: "http://127.0.0.1:4323",
  telegramRelayBase: "http://127.0.0.1:4324",
  audioIngressBase: "http://127.0.0.1:4325",
};

const ui = Object.fromEntries([
  "service-status", "chat-view", "memory-view", "chat-tab", "tg-tab", "audio-tab", "memory-tab", "transcript", "error-banner", "user-log-section", "clear-log", "message-form", "message-input", "message-resize-handle", "message-size-button", "send-button", "voice-button", "attach-button", "attachment-input", "attachment-status", "clear-attachments", "end-button", "activity", "context-inspector", "copy-context", "usage-metrics", "inspector-main", "inspector-full", "inspector-history", "memory-content", "memory-back", "memory-forward", "memory-home", "memory-kennedy-home", "new-conversation", "conversation-history", "history-eyebrow", "history-title", "chatend-title",
].map(id => [id.replaceAll("-", "_"), document.getElementById(id)]));

const INSPECTOR_MODES = ["main", "full", "history"];
const kweb = KwebAPI(CONFIG.kwebBase);
const intelligence = IntelligenceAPI(CONFIG.intelligenceBase);
const conversationHistory = ConversationHistoryAPI(CONFIG.conversationHistoryBase);
const telegramRelay = TelegramRelayAPI(CONFIG.telegramRelayBase);
const audioIngress = AudioIngressAPI(CONFIG.audioIngressBase);

let manuals = {};
let rootNodeIds = null;
let provider = null;
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
let activeView = "conversation";
let liveSessions = new Map();
let drafts = new Map();
let conversationErrors = new Map();
let endingIds = new Set();
let creatingConversation = false;
let ingressWorkerRunning = false;
let activeIngressRecord = null;
let ingressDiagnostic = null;
let activeAudioIngressPiece = null;
let inspectorMode = "main";
let recorder = null;
let recorderChunks = [];
let recordingStream = null;
let voiceDrafts = new Map();
let attachmentDrafts = new Map();
let extractingAttachments = new Set();
let telegramBridgeRunning = false;
let telegramInFlight = new Set();
let kwebReady = false;
let conversationHistoryReady = false;
let intelligenceReady = false;
let audioIngressReady = false;
let telegramRelayReady = false;
const INGRESS_FAILURE_LIMIT = 5;

function conversationPromptsReady() {
  return Boolean(manuals.identity && manuals.conversation);
}

function historyPromptsReady() {
  return Boolean(manuals.identity && manuals.ingress);
}

function audioPromptsReady() {
  return historyPromptsReady() && Boolean(manuals.audioIngress);
}

function chatRuntimeReady() {
  return kwebReady && conversationHistoryReady && intelligenceReady && conversationPromptsReady();
}

function memoryIngressRuntimeReady() {
  return kwebReady && intelligenceReady && historyPromptsReady()
    && (conversationHistoryReady || (audioIngressReady && audioPromptsReady()));
}

function sessionTypeOf(record) {
  return record?.state?.sessionType || record?.state?.archive?.sessionType || "conversation";
}

function recordsForView(view = activeView) {
  const type = view === "telegram" ? "telegram" : "conversation";
  return historyRecords.filter(record => sessionTypeOf(record) === type);
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
      mode: "conversation", provider, model,
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
  if (record?.id === activeIngressRecord?.id && ingressDiagnostic) {
    return {
      mode: "history ingress", provider, model,
      chatend: ingressDiagnostic.chatend?.messages || [],
      context: ingressDiagnostic.context?.diagnostics?.() || {},
      loadCalls: ingressDiagnostic.executor?.loadCalls || 0,
      loadLimit: ingressDiagnostic.executor?.loadLimit || 50,
      toolLog: ingressDiagnostic.executor?.toolLog || [],
      usage: ingressDiagnostic.usage?.snapshot?.() || null,
      memory: ingressDiagnostic.context?.snapshot?.() || EMPTY_MEMORY,
      historySegments: ingressDiagnostic.chatend?.historySegments || [],
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
  if (conversation) phases.push(historyPhase("Conversation", record?.phase === "active" ? "live" : "closed", conversation));
  if (status) phases.push(historyPhase("History ingress", status, ingress));
  return { ...current, ingressStatus: status, fullHistory: { phases } };
}

function audioPieceDiagnostic(piece) {
  if (piece?.id === activeAudioIngressPiece?.id && ingressDiagnostic) {
    return {
      mode: "audio ingress", provider, model,
      chatend: ingressDiagnostic.chatend?.messages || [],
      context: ingressDiagnostic.context?.diagnostics?.() || {},
      loadCalls: ingressDiagnostic.executor?.loadCalls || 0,
      loadLimit: ingressDiagnostic.executor?.loadLimit || 50,
      toolLog: ingressDiagnostic.executor?.toolLog || [],
      usage: ingressDiagnostic.usage?.snapshot?.() || null,
      memory: ingressDiagnostic.context?.snapshot?.() || EMPTY_MEMORY,
      historySegments: ingressDiagnostic.chatend?.historySegments || [],
    };
  }
  const archive = piece?.state?.historyIngress;
  return archive?.format === "kennedy-chatend"
    ? archivedDiagnostic(archive, "audio ingress")
    : null;
}

function audioPieceIngressActivity(piece) {
  const currentPiece = piece?.id === activeAudioIngressPiece?.id
    ? activeAudioIngressPiece
    : piece;
  let diagnostic = null;
  if (currentPiece?.id === activeAudioIngressPiece?.id && ingressDiagnostic) {
    diagnostic = ingressDiagnostic;
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
    liveRecordId: activeIngressRecord?.id,
    liveDiagnostic: ingressDiagnostic,
  });
}

function update() {
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
  const ingressActivity = visibleIngressActivity();
  renderTranscript(
    ui.transcript,
    viewingHistory ? (record?.state?.transcript || []) : (session?.transcript || []),
    ingressActivity,
    `${activeView}:${selectedConversationId || "none"}`,
  );
  if (telegramView && !(viewingHistory ? record?.state?.transcript : session?.transcript)?.length && !ingressActivity?.diagnostic) {
    ui.transcript.replaceChildren(element("div", "telegram-empty", "Telegram conversations appear here as messages arrive. Keep this page open: the relay queues messages while it is closed, and this visible UI owns Kennedy's Chatend and tool loop."));
  }
  renderConversationHistory(ui.conversation_history, recordsForView(), {
    selectedId: selectedConversationId,
    onSelect: id => selectConversation(id).catch(error => showError(ui.error_banner, error.message)),
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
    viewingHistory: viewingHistory || telegramView,
    transcriptLength: session?.transcript.length || 0,
  });
  const extractingAttachment = extractingAttachments.has(selectedConversationId);
  ui.message_form.classList.toggle("hidden", controls.composerHidden);
  ui.message_input.disabled = controls.inputDisabled;
  ui.send_button.disabled = controls.sendDisabled || extractingAttachment;
  ui.end_button.disabled = controls.endDisabled;
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
    : telegramView ? "Telegram Chatend" : currentDiagnostic.ingressStatus
      ? `Chatend · ingress ${currentDiagnostic.ingressStatus}`
      : "Chatend";
  ui.end_button.textContent = session?.pendingTurn ? "Retry saved query" : "End conversation";
  ui.activity.textContent = telegramView
    ? session?.busy ? "Kennedy is answering this Telegram message" : "Messages are delivered automatically"
    : viewingHistory
    ? record?.phase === "active" ? "Chat is unavailable; this saved conversation is read only" : "This conversation is closed and read only"
    : session?.busy
      ? "Kennedy is working — you can draft your next message"
      : session?.pendingTurn
        ? "Saved query needs a response — you can keep drafting"
      : "";
}

function upsertHistory(record) {
  if (!record) return;
  historyRecords = [record, ...historyRecords.filter(item => item.id !== record.id)]
    .sort((left, right) => String(right.updated_at).localeCompare(String(left.updated_at)));
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
  historyRecords = [...records].sort((left, right) => String(right.updated_at).localeCompare(String(left.updated_at)));
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
    await audioIngress.retryIngress(piece.id, { expected_version: piece.version });
    await refreshAudioHistory(true);
    kickHistoryIngress();
  } catch (error) {
    showError(ui.error_banner, `Audio memory ingress could not be retried: ${error.message}`);
  } finally {
    retryingAudioPieces.delete(piece.id);
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
  const session = new ConversationSession({
    kweb, intelligence, manuals, rootNodeIds, provider, model, reasoningEffort, contextWindowTokens, maxInputTokens,
    sessionType,
    channel: record.state?.channel || record.state?.archive?.channel || null,
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
      kweb, intelligence, manuals, rootNodeIds, provider, model, reasoningEffort, contextWindowTokens, maxInputTokens,
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

async function selectConversation(id) {
  if (id === selectedConversationId) return;
  saveDraft();
  const record = historyRecords.find(item => item.id === id) || await conversationHistory.get(id);
  upsertHistory(record);
  if (record.phase === "active" && !liveSessions.has(id) && chatRuntimeReady()) await buildConversation(record);
  selectedConversationId = id;
  selectedByView[sessionTypeOf(record)] = id;
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
    const message = `The saved query could not be resumed: ${error.message}`;
    conversationErrors.set(id, message);
    if (selectedConversationId === id) showError(ui.error_banner, message);
  }
  update();
}

async function endConversation() {
  const id = selectedConversationId;
  const session = selectedSession();
  const record = selectedRecord();
  if (!session || !record || session.busy || session.pendingTurn || endingIds.has(id)) return;
  endingIds.add(id);
  update();
  try {
    const closed = await conversationHistory.requestIngress(id, { expected_version: record.version, state: session.snapshot() });
    upsertHistory(closed);
    liveSessions.delete(id);
    drafts.delete(id);
    attachmentDrafts.delete(id);
    selectedConversationId = id;
    selectedByView.conversation = id;
    restoreDraft();
    update();
    kickHistoryIngress();
  } catch (error) {
    showError(ui.error_banner, error.message);
  } finally {
    endingIds.delete(id);
    update();
  }
}

async function createTelegramConversation(event) {
  const channel = {
    kind: "telegram",
    telegramUserId: event.telegramUserId,
    chatId: event.chatId,
    username: event.username || null,
    displayName: event.displayName,
  };
  const session = new ConversationSession({
    kweb, intelligence, manuals, rootNodeIds, provider, model, reasoningEffort,
    contextWindowTokens, maxInputTokens, sessionType: "telegram", channel, onUpdate: update,
  });
  await session.initialize();
  const record = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
  session.persist = (state, metadata) => persistSession(record.id, state, metadata);
  liveSessions.set(record.id, session);
  upsertHistory(record);
  return { record, session };
}

async function telegramConversationFor(event) {
  let record = event.conversationId
    ? historyRecords.find(item => item.id === event.conversationId) || await conversationHistory.get(event.conversationId).catch(() => null)
    : null;
  if (!record) {
    record = historyRecords.find(item => item.phase === "active"
      && sessionTypeOf(item) === "telegram"
      && String(item.state?.channel?.telegramUserId) === String(event.telegramUserId));
  }
  let session = record?.phase === "active" ? liveSessions.get(record.id) : null;
  if (record?.phase === "active" && !session) session = await buildConversation(record);
  if (!record || record.phase !== "active") ({ record, session } = await createTelegramConversation(event));
  if (event.conversationId !== record.id) await telegramRelay.bind(event.id, record.id);
  return { record, session };
}

async function processTelegramReset(event) {
  let record = event.conversationId
    ? historyRecords.find(item => item.id === event.conversationId) || await conversationHistory.get(event.conversationId).catch(() => null)
    : null;
  if (!record || record.phase !== "active") {
    record = historyRecords.find(item => item.phase === "active" && sessionTypeOf(item) === "telegram"
      && String(item.state?.channel?.telegramUserId) === String(event.telegramUserId));
  }
  if (!record || record.phase !== "active") {
    await telegramRelay.resetCompleted(event.id, "There is no active Telegram session to reset. Your next message will begin one.");
    return;
  }
  let session = liveSessions.get(record.id);
  if (!session) session = await buildConversation(record);
  if (session.busy) throw new Error("The Telegram session is still completing its previous message.");
  if (session.pendingTurn) await session.resumePendingTurn();
  const latest = historyRecords.find(item => item.id === record.id) || record;
  const closed = await conversationHistory.requestIngress(record.id, { expected_version: latest.version, state: session.snapshot() });
  upsertHistory(closed);
  liveSessions.delete(record.id);
  if (selectedConversationId === record.id) update();
  kickHistoryIngress();
  await telegramRelay.resetCompleted(event.id, "Conversation reset. The Telegram session has been queued for memory ingress; your next message will begin a new session.");
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

async function processTelegramEvent(event) {
  const processingStarted = performance.now();
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
    action: "delivery", status: "ok", sessionType: "telegram", durationMs, processingDurationMs,
  })).catch(() => {});
}

async function pollTelegramEvents() {
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

function ingressFailureMetrics(record) {
  const live = activeIngressRecord?.id === record.id ? ingressDiagnostic : null;
  const archived = record.state?.historyIngress;
  const usage = live?.usage?.snapshot?.() || archived?.usage || null;
  const roundCandidates = [live?.roundsUsed, archived?.roundsUsed].filter(Number.isInteger);
  return {
    rounds_used: roundCandidates.length ? Math.max(...roundCandidates) : null,
    context_tokens: Number.isFinite(usage?.contextTokens) ? usage.contextTokens : null,
    context_window_tokens: Number.isFinite(usage?.contextWindowTokens) ? usage.contextWindowTokens : null,
  };
}

async function recordIngressAttemptFailure(record, error, stage) {
  const latest = await conversationHistory.get(record.id);
  if (!["ingress_pending", "ingress_in_progress"].includes(latest.phase)) return latest;
  return conversationHistory.ingressFailure(latest.id, {
    expected_version: latest.version,
    stage,
    code: typeof error?.code === "string" ? error.code : "ingress_error",
    message: typeof error?.message === "string" ? error.message : "History ingress failed without an error message.",
    ...ingressFailureMetrics(latest),
  });
}

function archivedIngressMetrics(state, live = null) {
  const archived = state?.historyIngress;
  const usage = live?.usage?.snapshot?.() || archived?.usage || null;
  const roundCandidates = [live?.roundsUsed, archived?.roundsUsed].filter(Number.isInteger);
  return {
    rounds_used: roundCandidates.length ? Math.max(...roundCandidates) : null,
    context_tokens: Number.isFinite(usage?.contextTokens) ? usage.contextTokens : null,
    context_window_tokens: Number.isFinite(usage?.contextWindowTokens) ? usage.contextWindowTokens : null,
  };
}

async function nextMemoryIngress() {
  const [conversationResult, audioResult] = await Promise.all([
    conversationHistoryReady
      ? conversationHistory.nextIngress().catch(error => {
        showError(ui.error_banner, `Conversation memory queue is temporarily unavailable: ${error.message}`);
        return { conversation: null };
      })
      : { conversation: null },
    audioIngressReady && audioPromptsReady()
      ? audioIngress.nextIngress().catch(error => {
        showError(ui.error_banner, `Audio memory queue is temporarily unavailable: ${error.message}`);
        return { piece: null };
      })
      : { piece: null },
  ]);
  const conversation = conversationResult.conversation;
  const audio = audioResult.piece;
  if (!conversation && !audio) return null;
  if (conversation?.phase === "ingress_in_progress") return { kind: "conversation", record: conversation };
  if (audio?.phase === "ingress_in_progress") return { kind: "audio", record: audio };
  if (!conversation) return { kind: "audio", record: audio };
  if (!audio) return { kind: "conversation", record: conversation };
  return String(audio.source_created_at).localeCompare(String(conversation.started_at)) < 0
    ? { kind: "audio", record: audio }
    : { kind: "conversation", record: conversation };
}

async function processAudioIngressPiece(initialPiece) {
  let piece = initialPiece;
  let stage = "prepare";
  let liveDiagnostic = null;
  activeAudioIngressPiece = piece;
  ingressDiagnostic = null;
  try {
    if (piece.phase === "ingress_pending") {
      stage = "provenance";
      const provenance = await kweb.createProvenance({
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
        idempotency_key: `audio:${piece.sha256}:piece:${piece.piece_index}`,
      });
      stage = "claim";
      try {
        piece = await audioIngress.ingressStarted(piece.id, { expected_version: piece.version, provenance_id: provenance.id });
        activeAudioIngressPiece = piece;
      } catch (error) {
        if (error.code === "state_conflict") {
          activeAudioIngressPiece = null;
          return;
        }
        throw error;
      }
    }
    if (piece.phase !== "ingress_in_progress") {
      activeAudioIngressPiece = null;
      return;
    }
    stage = "model_loop";
    if (!piece.provenance_id) throw new Error("The queued audio transcript is missing its provenance.");
    const persistIngress = async archive => {
      const state = { ...piece.state, historyIngress: archive };
      try {
        piece = await audioIngress.ingressCheckpoint(piece.id, { expected_version: piece.version, state });
        activeAudioIngressPiece = piece;
      } catch (error) {
        if (error.code !== "state_conflict") throw error;
        const latest = await audioIngress.getPiece(piece.id);
        if (latest.phase !== "ingress_in_progress" || JSON.stringify(latest.state) !== JSON.stringify(state)) throw error;
        piece = latest;
      }
    };
    await runHistoryIngress({
      kweb, intelligence, manuals, rootNodeIds, provenanceId: piece.provenance_id,
      provider, model, reasoningEffort, contextWindowTokens, maxInputTokens,
      sourceSessionType: "audio",
      restoredArchive: piece.state?.historyIngress,
      checkpoint: persistIngress,
      onUpdate: value => { liveDiagnostic = value; ingressDiagnostic = value; update(); },
    });
    ingressDiagnostic = null;
    stage = "completion";
    await audioIngress.ingressCompleted(piece.id, { expected_version: piece.version });
    activeAudioIngressPiece = null;
    await refreshAudioHistory(activeView === "audio");
  } catch (error) {
    const latest = await audioIngress.getPiece(piece.id);
    if (!["ingress_pending", "ingress_in_progress"].includes(latest.phase)) {
      activeAudioIngressPiece = null;
      return;
    }
    const failed = await audioIngress.ingressFailure(latest.id, {
      expected_version: latest.version,
      stage,
      code: typeof error?.code === "string" ? error.code : "ingress_error",
      message: typeof error?.message === "string" ? error.message : "Audio ingress failed without an error message.",
      ...archivedIngressMetrics(latest.state, liveDiagnostic),
    });
    console.error("Audio ingress attempt failed", {
      recordingId: failed.recording_id, piece: failed.piece_index,
      stage, attempt: failed.ingress_failure_count, terminal: failed.phase === "ingress_failed", error,
    });
    if (failed.phase === "ingress_failed") {
      ingressDiagnostic = null;
      activeAudioIngressPiece = null;
      await refreshAudioHistory(activeView === "audio");
      ui.service_status.textContent = "Audio memory ingestion failed";
      showError(ui.error_banner, `Audio transcript ingress stopped after ${failed.ingress_failure_count} failed attempts. Recording ${failed.recording_id} remains preserved for inspection.`);
      return;
    }
    activeAudioIngressPiece = failed;
    const failureMessage = failed.ingress_failures?.at?.(-1)?.message || "No error detail was recorded.";
    throw new Error(`Audio ingress attempt ${failed.ingress_failure_count}/${INGRESS_FAILURE_LIMIT} failed during ${stage}: ${failureMessage}`);
  }
}

async function processIngressQueue() {
  while (true) {
    const work = await nextMemoryIngress();
    if (!work) return;
    if (work.kind === "audio") {
      activeIngressRecord = null;
      await processAudioIngressPiece(work.record);
      continue;
    }
    let record = work.record;
    let stage = "prepare";
    activeIngressRecord = record;
    ingressDiagnostic = null;
    upsertHistory(record);
    update();
    try {
      if (record.phase === "ingress_pending") {
        const archive = record.state?.archive;
        if (archive?.format !== "kennedy-chatend") throw new Error("The queued conversation is missing its durable Chatend archive.");
        stage = "provenance";
        const provenance = await kweb.createProvenance({
          data: JSON.stringify(archive, null, 2),
          source: archive.sessionType === "telegram" ? "telegram" : "conversation",
          source_created_at: record.started_at,
          idempotency_key: `${archive.sessionType === "telegram" ? "telegram" : "conversation"}:${record.id}`,
        });
        stage = "claim";
        try {
          record = await conversationHistory.ingressStarted(record.id, { expected_version: record.version, provenance_id: provenance.id });
        } catch (error) {
          if (error.code === "state_conflict") {
            await refreshHistory();
            continue;
          }
          throw error;
        }
        activeIngressRecord = record;
        upsertHistory(record);
        update();
      }
      if (record.phase === "ingress_in_progress") {
        stage = "model_loop";
        if (!record.provenance_id) throw new Error("The queued conversation is missing its history provenance.");
        const persistIngress = async archive => {
          const state = { ...record.state, historyIngress: archive };
          try {
            record = await conversationHistory.ingressCheckpoint(record.id, { expected_version: record.version, state });
          } catch (error) {
            if (error.code !== "state_conflict") throw error;
            const latest = await conversationHistory.get(record.id);
            if (latest.phase !== "ingress_in_progress" || JSON.stringify(latest.state) !== JSON.stringify(state)) throw error;
            record = latest;
          }
          activeIngressRecord = record;
          upsertHistory(record);
          update();
        };
        await runHistoryIngress({
          kweb, intelligence, manuals, rootNodeIds, provenanceId: record.provenance_id,
          provider, model, reasoningEffort, contextWindowTokens, maxInputTokens,
          sourceSessionType: record.state?.archive?.sessionType || "conversation",
          restoredArchive: record.state?.historyIngress,
          checkpoint: persistIngress,
          onUpdate: value => { ingressDiagnostic = value; update(); },
        });
        ingressDiagnostic = null;
        stage = "completion";
        record = await conversationHistory.ingressCompleted(record.id, { expected_version: record.version });
        upsertHistory(record);
        activeIngressRecord = null;
        await refreshHistory();
      }
    } catch (error) {
      const failedRecord = await recordIngressAttemptFailure(record, error, stage);
      upsertHistory(failedRecord);
      console.error("History ingress attempt failed", {
        conversationId: record.id,
        stage,
        attempt: failedRecord.ingress_failure_count,
        limit: INGRESS_FAILURE_LIMIT,
        terminal: failedRecord.phase === "ingress_failed",
        error,
      });
      if (failedRecord.phase === "ingress_failed") {
        ingressDiagnostic = null;
        activeIngressRecord = null;
        ui.service_status.textContent = "Memory ingestion failed";
        showError(ui.error_banner, `History ingress stopped after ${failedRecord.ingress_failure_count} failed attempts. Select the conversation to inspect its failure log.`);
        await refreshHistory();
        continue;
      }
      if (!["ingress_pending", "ingress_in_progress"].includes(failedRecord.phase)) {
        activeIngressRecord = null;
        await refreshHistory();
        continue;
      }
      const failureMessage = failedRecord.ingress_failures?.at?.(-1)?.message || "No error detail was recorded.";
      throw new Error(`History ingress attempt ${failedRecord.ingress_failure_count}/${INGRESS_FAILURE_LIMIT} failed during ${stage}: ${failureMessage}`);
    }
  }
}

function kickHistoryIngress() {
  if (ingressWorkerRunning || !memoryIngressRuntimeReady()) return;
  ingressWorkerRunning = true;
  const run = () => processIngressQueue();
  const work = navigator.locks?.request
    ? navigator.locks.request("kennedy-history-ingress", run)
    : run();
  Promise.resolve(work).catch(error => {
    ui.service_status.textContent = "Memory ingestion needs attention";
    showError(ui.error_banner, `History ingress will retry: ${error.message}`);
  }).finally(() => {
    ingressWorkerRunning = false;
    activeIngressRecord = null;
    activeAudioIngressPiece = null;
    update();
    setTimeout(kickHistoryIngress, 5000);
  });
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
    rootNodeIds = [user.user_root_node_id || user.root_node_id, user.kennedy_root_node_id];
    if (rootNodeIds.some(id => typeof id !== "string" || !id)) throw new Error("Kweb did not provide both required root nodes.");
    explorer = new MemoryExplorer({ api: kweb, rootNodeIds, content: ui.memory_content, backButton: ui.memory_back, forwardButton: ui.memory_forward });
    kwebReady = true;
    ui.service_status.textContent = `${health.status} · memory ready`;
  } catch (error) {
    ui.service_status.textContent = "Kweb unavailable";
    showError(ui.error_banner, `Memory is unavailable: ${error.message}`);
  }

  if (kwebReady) {
    const loaded = await loadPromptManuals(CONFIG.kwebBase);
    manuals = loaded.manuals;
    const promptImpact = {
      identity: "Conversation and memory-ingress model sessions are unavailable",
      conversation: "New and restored conversations are unavailable",
      ingress: "Conversation-history and audio memory ingress are paused",
      audioIngress: "Audio preparation and history remain available, but audio memory ingress is paused",
    };
    for (const [key, message] of Object.entries(loaded.errors)) {
      showError(ui.error_banner, `${promptImpact[key] || "A model mode is unavailable"}: ${message}`);
    }
  }

  try {
    await conversationHistory.health();
    await conversationHistory.discardUnstarted();
    historyRecords = (await conversationHistory.list()).conversations || [];
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
    model = selected.default_model;
    reasoningEffort = selected.reasoning_effort;
    const modelCapabilities = selected.model_capabilities?.[model] || {};
    inputModalities = modelCapabilities.input_modalities || selected.input_modalities || ["text"];
    transcriptionAvailable = Boolean(selected.transcription_available);
    if (typeof reasoningEffort !== "string" || !reasoningEffort) throw new Error("The intelligence service did not provide the model thinking mode.");
    contextWindowTokens = Number(modelCapabilities.context_window_tokens ?? selected.context_window_tokens) || 0;
    maxInputTokens = Number(modelCapabilities.max_input_tokens ?? selected.max_input_tokens) || 0;
    if (contextWindowTokens <= 0 || maxInputTokens <= 0) throw new Error("The intelligence service did not provide the model's advertised effective context window.");
    intelligenceReady = true;
  } catch (error) {
    showError(ui.error_banner, `Kennedy's model service is unavailable: ${error.message}`);
  }

  if (chatRuntimeReady()) {
    const activeRecords = historyRecords.filter(record => record.phase === "active");
    for (const record of activeRecords) {
      try {
        await buildConversation(record);
      } catch (error) {
        showError(ui.error_banner, `Saved ${sessionTypeOf(record)} session ${record.id} could not be restored: ${error.message}`);
      }
    }
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

  if (chatRuntimeReady()) {
    try {
      await telegramRelay.health();
      telegramRelayReady = true;
      startTelegramBridge();
    } catch (error) {
      showError(ui.error_banner, `Telegram is unavailable: ${error.message}`);
    }
  }

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
ui.end_button.addEventListener("click", () => selectedSession()?.pendingTurn ? resumeSavedQuery() : endConversation());
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
