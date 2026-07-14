import { KwebAPI, IntelligenceAPI, ConversationHistoryAPI, TelegramRelayAPI } from "./api.js?v=20260714.7";
import { loadPromptManuals } from "./prompt_composer.js?v=20260714.7";
import { ConversationSession } from "./conversation.js?v=20260714.8";
import { runHistoryIngress } from "./history_ingress.js?v=20260714.7";
import { MemoryExplorer } from "./memory_explorer.js?v=20260714.7";
import { renderTranscript, renderConversationHistory, conversationControlState, conversationIngressActivity, renderInspector, renderUsage, inspectorText, showError, clearError, element } from "./render.js?v=20260714.7";

const CONFIG = {
  kwebBase: window.location.origin,
  intelligenceBase: "http://127.0.0.1:4322",
  conversationHistoryBase: "http://127.0.0.1:4323",
  telegramRelayBase: "http://127.0.0.1:4324",
};

const MODEL_LIMITS = {
  "gpt-5.6": { contextWindowTokens: 1050000, maxInputTokens: 922000 },
  "gpt-5.6-sol": { contextWindowTokens: 1050000, maxInputTokens: 922000 },
};

const ui = Object.fromEntries([
  "service-status", "chat-view", "memory-view", "chat-tab", "tg-tab", "memory-tab", "transcript", "error-banner", "message-form", "message-input", "message-resize-handle", "message-size-button", "send-button", "voice-button", "end-button", "activity", "context-inspector", "copy-context", "usage-metrics", "inspector-full", "inspector-system", "inspector-tools", "inspector-memory", "memory-content", "memory-back", "memory-forward", "memory-home", "memory-kennedy-home", "new-conversation", "conversation-history", "history-eyebrow", "history-title", "chatend-title",
].map(id => [id.replaceAll("-", "_"), document.getElementById(id)]));

const INSPECTOR_MODES = ["full", "system", "tools", "memory"];
const kweb = KwebAPI(CONFIG.kwebBase);
const intelligence = IntelligenceAPI(CONFIG.intelligenceBase);
const conversationHistory = ConversationHistoryAPI(CONFIG.conversationHistoryBase);
const telegramRelay = TelegramRelayAPI(CONFIG.telegramRelayBase);

let manuals = null;
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
let activeView = "conversation";
let liveSessions = new Map();
let drafts = new Map();
let conversationErrors = new Map();
let endingIds = new Set();
let creatingConversation = false;
let ingressWorkerRunning = false;
let activeIngressRecord = null;
let ingressDiagnostic = null;
let inspectorMode = "full";
let recorder = null;
let recorderChunks = [];
let recordingStream = null;
let voiceDrafts = new Map();
let telegramBridgeRunning = false;
let telegramInFlight = new Set();

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

function diagnostic() {
  const record = selectedRecord();
  const session = selectedSession();
  if (record && record.phase !== "active") {
    const transcript = Array.isArray(record.state?.transcript) ? record.state.transcript : [];
    const archive = record.state?.archive?.format === "kennedy-chatend" ? record.state.archive : null;
    return {
      mode: "saved conversation", provider, model,
      chatend: archive?.messages || transcript.map(item => ({ role: item.role === "kennedy" ? "assistant" : "user", content: item.content })),
      context: archive?.context?.diagnostics || {},
      loadCalls: archive?.tools?.loadCalls || 0,
      loadLimit: archive?.tools?.loadLimit || 0,
      toolLog: archive?.tools?.log || [],
      usage: archive?.usage || null,
      memory: archive?.context?.snapshot || { directlyLoadedIdentifiers: [], nodes: [] },
    };
  }
  if (!session) return { mode: "offline", provider, model, chatend: [], context: {}, loadCalls: 0, loadLimit: 0, toolLog: [], usage: null, memory: { directlyLoadedIdentifiers: [], nodes: [] } };
  return {
    mode: "conversation", provider, model,
    chatend: session.chatend?.messages || [],
    context: session.context?.diagnostics() || {},
    loadCalls: session.executor?.loadCalls || 0,
    loadLimit: session.executor?.loadLimit || 20,
    toolLog: session.executor?.toolLog || [],
    usage: session.usage?.snapshot() || null,
    memory: session.context?.snapshot() || { directlyLoadedIdentifiers: [], nodes: [] },
  };
}

function visibleIngressActivity() {
  return conversationIngressActivity({
    record: selectedRecord(),
    liveRecordId: activeIngressRecord?.id,
    liveDiagnostic: ingressDiagnostic,
  });
}

function update() {
  const record = selectedRecord();
  const session = selectedSession();
  const viewingHistory = Boolean(record && record.phase !== "active");
  const telegramView = activeView === "telegram";
  const ingressActivity = visibleIngressActivity();
  renderTranscript(
    ui.transcript,
    viewingHistory ? (record.state?.transcript || []) : (session?.transcript || []),
    ingressActivity,
  );
  if (telegramView && !(viewingHistory ? record?.state?.transcript : session?.transcript)?.length && !ingressActivity?.diagnostic) {
    ui.transcript.replaceChildren(element("div", "telegram-empty", "Telegram conversations appear here as messages arrive. Keep this page open: the relay queues messages while it is closed, and this visible UI owns Kennedy's Chatend and tool loop."));
  }
  renderConversationHistory(ui.conversation_history, recordsForView(), {
    selectedId: selectedConversationId,
    onSelect: id => selectConversation(id).catch(error => showError(ui.error_banner, error.message)),
  });
  const currentDiagnostic = diagnostic();
  renderInspector(ui.context_inspector, currentDiagnostic, inspectorMode);
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
  ui.message_form.classList.toggle("hidden", controls.composerHidden);
  ui.message_input.disabled = controls.inputDisabled;
  ui.send_button.disabled = controls.sendDisabled;
  ui.end_button.disabled = controls.endDisabled;
  ui.new_conversation.disabled = controls.newDisabled;
  ui.new_conversation.classList.toggle("hidden", telegramView);
  ui.voice_button.disabled = controls.sendDisabled || !transcriptionAvailable
    || !navigator.mediaDevices?.getUserMedia || typeof MediaRecorder !== "function";
  ui.history_eyebrow.textContent = telegramView ? "TELEGRAM SESSIONS" : "YOUR CONVERSATIONS";
  ui.history_title.textContent = telegramView ? "Bot chats" : "History";
  ui.chatend_title.textContent = telegramView ? "Telegram Chatend" : "Chatend";
  ui.end_button.textContent = session?.pendingTurn ? "Retry saved query" : "End conversation";
  ui.activity.textContent = telegramView
    ? session?.busy ? "Kennedy is answering this Telegram message" : "Messages are delivered automatically"
    : viewingHistory
    ? "This conversation is closed and read only"
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
  clearError(ui.error_banner);
  try {
    if (inputModalities.includes("audio")) throw new Error("The selected native-audio transport is not enabled in this UI build.");
    const fileName = `voice-note.${audioExtension(mimeType)}`;
    const result = await intelligence.transcribe({ provider, model, file: blob, fileName });
    const dataUrl = await blobToDataUrl(blob);
    voiceDrafts.set(id, {
      inputKind: "voice",
      transcriptionModel: result.transcription_model,
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
  clearError(ui.error_banner);
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
  creatingConversation = true;
  saveDraft();
  clearError(ui.error_banner);
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
  clearError(ui.error_banner);
  const record = historyRecords.find(item => item.id === id) || await conversationHistory.get(id);
  upsertHistory(record);
  if (record.phase === "active" && !liveSessions.has(id)) await buildConversation(record);
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
  if (!session || session.busy || session.pendingTurn || endingIds.has(id)) return;
  const text = ui.message_input.value;
  if (!text.trim()) return;
  ui.message_input.value = "";
  drafts.set(id, "");
  const metadata = voiceDrafts.get(id) || {};
  voiceDrafts.delete(id);
  conversationErrors.delete(id);
  clearError(ui.error_banner);
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
  if (selectedConversationId === id) clearError(ui.error_banner);
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
  clearError(ui.error_banner);
  update();
  try {
    const closed = await conversationHistory.requestIngress(id, { expected_version: record.version, state: session.snapshot() });
    upsertHistory(closed);
    liveSessions.delete(id);
    drafts.delete(id);
    selectedConversationId = historyRecords.find(item => item.phase === "active"
      && sessionTypeOf(item) === "conversation"
      && liveSessions.has(item.id))?.id || id;
    restoreDraft();
    update();
    kickHistoryIngress();
    if (!selectedSession()) await createNewConversation();
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
  if (!text) {
    if (inputModalities.includes("audio")) throw new Error("The selected model advertises native audio, but this Kennedy transport cannot yet forward it.");
    const mimeType = blob.type || event.mimeType || "audio/ogg";
    const result = await intelligence.transcribe({
      provider, model, file: blob, fileName: `telegram-voice.${audioExtension(mimeType)}`,
    });
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

async function processTelegramEvent(event) {
  if (event.kind === "reset") {
    await processTelegramReset(event);
    return;
  }
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
    } else {
      await session.send(event.text || "", { externalEventId: event.id, inputKind: "text" });
    }
    response = session.answerForExternalEvent(event.id);
  }
  if (!response) throw new Error("Kennedy completed the turn without a recoverable Telegram response.");
  await telegramRelay.reply(event.id, record.id, response.content, response.contextWarning || null);
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
      if (activeView === "telegram") showError(ui.error_banner, `Telegram delivery will retry: ${error.message}`);
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

async function processIngressQueue() {
  while (true) {
    let record = (await conversationHistory.nextIngress()).conversation;
    if (!record) return;
    activeIngressRecord = record;
    ingressDiagnostic = null;
    upsertHistory(record);
    update();
    if (record.phase === "ingress_pending") {
      const archive = record.state?.archive;
      if (archive?.format !== "kennedy-chatend") throw new Error("The queued conversation is missing its durable Chatend archive.");
      const provenance = await kweb.createProvenance({
        data: JSON.stringify(archive, null, 2),
        source: archive.sessionType === "telegram" ? "telegram" : "conversation",
        source_created_at: record.started_at,
        idempotency_key: `${archive.sessionType === "telegram" ? "telegram" : "conversation"}:${record.id}`,
      });
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
      record = await conversationHistory.ingressCompleted(record.id, { expected_version: record.version });
      upsertHistory(record);
      activeIngressRecord = null;
      await refreshHistory();
    }
  }
}

function kickHistoryIngress() {
  if (ingressWorkerRunning) return;
  ingressWorkerRunning = true;
  const run = () => processIngressQueue();
  const work = navigator.locks?.request
    ? navigator.locks.request("kennedy-history-ingress", run)
    : run();
  Promise.resolve(work).catch(error => {
    ui.service_status.textContent = "Memory ingestion needs attention";
    showError(ui.error_banner, `History ingress will retry: ${error.message}`);
    setTimeout(kickHistoryIngress, 5000);
  }).finally(() => {
    ingressWorkerRunning = false;
    activeIngressRecord = null;
    update();
  });
}

function showView(view) {
  if (!["conversation", "telegram", "memory"].includes(view)) return;
  if (activeView !== "memory") selectedByView[activeView] = selectedConversationId;
  saveDraft();
  activeView = view;
  const memory = view === "memory";
  ui.chat_view.classList.toggle("hidden", memory);
  ui.memory_view.classList.toggle("hidden", !memory);
  ui.chat_tab.classList.toggle("active", view === "conversation");
  ui.tg_tab.classList.toggle("active", view === "telegram");
  ui.memory_tab.classList.toggle("active", memory);
  if (!memory) {
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
    const [health, user, loadedManuals] = await Promise.all([kweb.health(), kweb.user(), loadPromptManuals(CONFIG.kwebBase)]);
    rootNodeIds = [user.user_root_node_id || user.root_node_id, user.kennedy_root_node_id];
    if (rootNodeIds.some(id => typeof id !== "string" || !id)) throw new Error("Kweb did not provide both required root nodes.");
    manuals = loadedManuals;
    explorer = new MemoryExplorer({ api: kweb, rootNodeIds, content: ui.memory_content, backButton: ui.memory_back, forwardButton: ui.memory_forward });
    ui.service_status.textContent = `${health.status} · memory ready`;
  } catch (error) {
    ui.service_status.textContent = "Kweb unavailable";
    showError(ui.error_banner, error.message);
    update();
    return;
  }
  try {
    await conversationHistory.health();
    await conversationHistory.discardUnstarted();
    await intelligence.health();
    const providers = await intelligence.providers();
    provider = providers.default_provider;
    const selected = providers.providers.find(item => item.name === provider);
    model = selected.default_model;
    reasoningEffort = selected.reasoning_effort;
    inputModalities = selected.model_capabilities?.[model]?.input_modalities || selected.input_modalities || ["text"];
    transcriptionAvailable = Boolean(selected.transcription_available);
    if (typeof reasoningEffort !== "string" || !reasoningEffort) throw new Error("The intelligence service did not provide the model thinking mode.");
    const fallbackLimits = MODEL_LIMITS[model] || {};
    contextWindowTokens = selected.context_window_tokens || fallbackLimits.contextWindowTokens || 0;
    maxInputTokens = selected.max_input_tokens || fallbackLimits.maxInputTokens || 0;
    historyRecords = (await conversationHistory.list()).conversations || [];
    const activeRecords = historyRecords.filter(record => record.phase === "active");
    for (const record of activeRecords) await buildConversation(record);
    const activeConversations = activeRecords.filter(record => sessionTypeOf(record) === "conversation");
    if (activeConversations.length) {
      selectedConversationId = activeConversations[0].id;
      selectedByView.conversation = selectedConversationId;
      restoreDraft();
      update();
    } else {
      await createNewConversation();
    }
    await telegramRelay.health();
    startTelegramBridge();
    kickHistoryIngress();
    ui.service_status.textContent = `Ready · ${model}`;
  } catch (error) {
    ui.service_status.textContent = "Chat offline · memory ready";
    showError(ui.error_banner, `The memory explorer is available, but chat is offline: ${error.message}`);
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
for (const mode of INSPECTOR_MODES) ui[`inspector_${mode}`].addEventListener("click", () => { inspectorMode = mode; update(); });
ui.chat_tab.addEventListener("click", () => showView("conversation"));
ui.tg_tab.addEventListener("click", () => showView("telegram"));
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
