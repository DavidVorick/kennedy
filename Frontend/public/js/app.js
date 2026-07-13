import { KwebAPI, IntelligenceAPI, ConversationHistoryAPI } from "./api.js?v=20260713.7";
import { loadPromptManuals } from "./prompt_composer.js?v=20260713.7";
import { ConversationSession } from "./conversation.js?v=20260713.7";
import { runHistoryIngress } from "./history_ingress.js?v=20260713.7";
import { MemoryExplorer } from "./memory_explorer.js?v=20260713.7";
import { renderTranscript, renderConversationHistory, conversationControlState, conversationIngressActivity, renderInspector, renderUsage, inspectorText, showError, clearError } from "./render.js?v=20260713.8";

const CONFIG = {
  kwebBase: window.location.origin,
  intelligenceBase: "http://127.0.0.1:4322",
  conversationHistoryBase: "http://127.0.0.1:4323",
};

const MODEL_LIMITS = {
  "gpt-5.6": { contextWindowTokens: 1050000, maxInputTokens: 922000 },
  "gpt-5.6-sol": { contextWindowTokens: 1050000, maxInputTokens: 922000 },
};

const ui = Object.fromEntries([
  "service-status", "chat-view", "memory-view", "chat-tab", "memory-tab", "transcript", "error-banner", "message-form", "message-input", "send-button", "end-button", "activity", "context-inspector", "copy-context", "usage-metrics", "inspector-full", "inspector-system", "inspector-tools", "inspector-memory", "memory-content", "memory-back", "memory-forward", "memory-home", "new-conversation", "conversation-history",
].map(id => [id.replaceAll("-", "_"), document.getElementById(id)]));

const INSPECTOR_MODES = ["full", "system", "tools", "memory"];
const kweb = KwebAPI(CONFIG.kwebBase);
const intelligence = IntelligenceAPI(CONFIG.intelligenceBase);
const conversationHistory = ConversationHistoryAPI(CONFIG.conversationHistoryBase);

let manuals = null;
let rootNodeId = null;
let provider = null;
let model = null;
let contextWindowTokens = 0;
let maxInputTokens = 0;
let explorer = null;
let historyRecords = [];
let selectedConversationId = null;
let liveSessions = new Map();
let drafts = new Map();
let conversationErrors = new Map();
let endingIds = new Set();
let creatingConversation = false;
let ingressWorkerRunning = false;
let activeIngressRecord = null;
let ingressDiagnostic = null;
let inspectorMode = "full";

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
  const ingressActivity = visibleIngressActivity();
  renderTranscript(
    ui.transcript,
    viewingHistory ? (record.state?.transcript || []) : (session?.transcript || []),
    ingressActivity,
  );
  renderConversationHistory(ui.conversation_history, historyRecords, {
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
    viewingHistory,
    transcriptLength: session?.transcript.length || 0,
  });
  ui.message_input.disabled = controls.inputDisabled;
  ui.send_button.disabled = controls.sendDisabled;
  ui.end_button.disabled = controls.endDisabled;
  ui.new_conversation.disabled = controls.newDisabled;
  ui.end_button.textContent = session?.pendingTurn ? "Retry saved query" : "End conversation";
  ui.activity.textContent = viewingHistory
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
  if (selectedSession()) drafts.set(selectedConversationId, ui.message_input.value);
}

function restoreDraft() {
  ui.message_input.value = selectedSession() ? (drafts.get(selectedConversationId) || "") : "";
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
  const session = new ConversationSession({
    kweb, intelligence, manuals, rootNodeId, provider, model, contextWindowTokens, maxInputTokens,
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
      kweb, intelligence, manuals, rootNodeId, provider, model, contextWindowTokens, maxInputTokens,
      onUpdate: update,
    });
    await session.initialize();
    const record = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
    session.persist = (state, metadata) => persistSession(record.id, state, metadata);
    liveSessions.set(record.id, session);
    drafts.set(record.id, "");
    upsertHistory(record);
    selectedConversationId = record.id;
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
  conversationErrors.delete(id);
  clearError(ui.error_banner);
  try {
    await session.send(text);
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
    selectedConversationId = historyRecords.find(item => item.phase === "active" && liveSessions.has(item.id))?.id || id;
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
        source: "conversation",
        source_created_at: record.started_at,
        idempotency_key: `conversation:${record.id}`,
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
        kweb, intelligence, manuals, rootNodeId, provenanceId: record.provenance_id,
        provider, model, contextWindowTokens, maxInputTokens,
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

function showView(memory) {
  ui.chat_view.classList.toggle("hidden", memory);
  ui.memory_view.classList.toggle("hidden", !memory);
  ui.chat_tab.classList.toggle("active", !memory);
  ui.memory_tab.classList.toggle("active", memory);
  if (memory && explorer && !explorer.currentNodeId) explorer.home();
}

async function initialize() {
  update();
  try {
    const [health, user, loadedManuals] = await Promise.all([kweb.health(), kweb.user(), loadPromptManuals(CONFIG.kwebBase)]);
    rootNodeId = user.root_node_id;
    manuals = loadedManuals;
    explorer = new MemoryExplorer({ api: kweb, rootNodeId, content: ui.memory_content, backButton: ui.memory_back, forwardButton: ui.memory_forward });
    ui.service_status.textContent = `${health.status} · memory ready`;
  } catch (error) {
    ui.service_status.textContent = "Kweb unavailable";
    showError(ui.error_banner, error.message);
    update();
    return;
  }
  try {
    await Promise.all([intelligence.health(), conversationHistory.health()]);
    const providers = await intelligence.providers();
    provider = providers.default_provider;
    const selected = providers.providers.find(item => item.name === provider);
    model = selected.default_model;
    const fallbackLimits = MODEL_LIMITS[model] || {};
    contextWindowTokens = selected.context_window_tokens || fallbackLimits.contextWindowTokens || 0;
    maxInputTokens = selected.max_input_tokens || fallbackLimits.maxInputTokens || 0;
    historyRecords = (await conversationHistory.list()).conversations || [];
    const activeRecords = historyRecords.filter(record => record.phase === "active");
    for (const record of activeRecords) await buildConversation(record);
    if (activeRecords.length) {
      selectedConversationId = activeRecords[0].id;
      restoreDraft();
      update();
    } else {
      await createNewConversation();
    }
    for (const record of activeRecords) {
      if (liveSessions.get(record.id)?.pendingTurn) resumeSavedQuery(record.id);
    }
    kickHistoryIngress();
    ui.service_status.textContent = `Ready · ${model}`;
  } catch (error) {
    ui.service_status.textContent = "Chat offline · memory ready";
    showError(ui.error_banner, `The memory explorer is available, but chat is offline: ${error.message}`);
    update();
  }
}

ui.message_form.addEventListener("submit", submitMessage);
ui.message_input.addEventListener("input", () => { if (selectedSession()) drafts.set(selectedConversationId, ui.message_input.value); });
ui.message_input.addEventListener("keydown", event => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    event.preventDefault();
    ui.message_form.requestSubmit();
  }
});
ui.end_button.addEventListener("click", () => selectedSession()?.pendingTurn ? resumeSavedQuery() : endConversation());
ui.new_conversation.addEventListener("click", () => createNewConversation().catch(error => showError(ui.error_banner, error.message)));
for (const mode of INSPECTOR_MODES) ui[`inspector_${mode}`].addEventListener("click", () => { inspectorMode = mode; update(); });
ui.chat_tab.addEventListener("click", () => showView(false));
ui.memory_tab.addEventListener("click", () => showView(true));
ui.memory_back.addEventListener("click", () => explorer?.goBack());
ui.memory_forward.addEventListener("click", () => explorer?.goForward());
ui.memory_home.addEventListener("click", () => explorer?.home());
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
