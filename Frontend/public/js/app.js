import { KwebAPI, IntelligenceAPI, ConversationHistoryAPI } from "./api.js?v=20260713.1";
import { loadPromptManuals } from "./prompt_composer.js?v=20260713.1";
import { ConversationSession } from "./conversation.js?v=20260713.1";
import { runHistoryIngress } from "./history_ingress.js?v=20260713.1";
import { MemoryExplorer } from "./memory_explorer.js?v=20260713.1";
import { renderTranscript, renderConversationHistory, conversationControlState, renderInspector, renderUsage, renderIngressActivity, inspectorText, showError, clearError } from "./render.js?v=20260713.1";

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
  "service-status", "chat-view", "memory-view", "chat-tab", "memory-tab", "transcript", "error-banner", "message-form", "message-input", "send-button", "end-button", "activity", "context-inspector", "copy-context", "usage-metrics", "inspector-full", "inspector-system", "inspector-tools", "inspector-memory", "ingress-panel", "ingress-title", "ingress-log", "dismiss-ingress", "memory-content", "memory-back", "memory-forward", "memory-home", "new-conversation", "conversation-history",
].map(id => [id.replaceAll("-", "_"), document.getElementById(id)]));

const INSPECTOR_MODES = ["full", "system", "tools", "memory"];

const kweb = KwebAPI(CONFIG.kwebBase);
const intelligence = IntelligenceAPI(CONFIG.intelligenceBase);
const conversationHistory = ConversationHistoryAPI(CONFIG.conversationHistoryBase);
let session = null;
let historyRecord = null;
let manuals = null;
let rootNodeId = null;
let provider = null;
let model = null;
let contextWindowTokens = 0;
let maxInputTokens = 0;
let explorer = null;
let ingressDiagnostic = null;
let ingressActivity = null;
let ending = false;
let ingressRequired = false;
let ingressSourceSession = null;
let ingressRecord = null;
let historyRecords = [];
let viewedRecord = null;
let inspectorMode = "full";

function diagnostic() {
  if (viewedRecord) {
    const transcript = Array.isArray(viewedRecord.state?.transcript) ? viewedRecord.state.transcript : [];
    return {
      mode: "saved conversation", provider, model,
      chatend: transcript.map(item => ({ role: item.role === "kennedy" ? "assistant" : "user", content: item.content })),
      context: {}, loadCalls: 0, loadLimit: 0, toolLog: [], usage: null,
      memory: { directlyLoadedIdentifiers: [], nodes: [] },
    };
  }
  if (ingressDiagnostic) return {
    mode: "history ingress", provider, model,
    chatend: ingressDiagnostic.chatend.messages,
    context: ingressDiagnostic.context.diagnostics(),
    loadCalls: ingressDiagnostic.executor.loadCalls, loadLimit: ingressDiagnostic.executor.loadLimit,
    toolLog: ingressDiagnostic.executor.toolLog,
    usage: ingressDiagnostic.usage.snapshot(),
    memory: ingressDiagnostic.context.snapshot(),
  };
  if (!session) return { mode: "offline", provider, model, chatend: [], context: {}, loadCalls: 0, loadLimit: 0, toolLog: [], usage: null, memory: { directlyLoadedIdentifiers: [], nodes: [] } };
  return {
    mode: "conversation", provider, model,
    chatend: session.chatend?.messages || [],
    context: session.context?.diagnostics() || {},
    loadCalls: session.executor?.loadCalls || 0, loadLimit: session.executor?.loadLimit || 20,
    toolLog: session.executor?.toolLog || [],
    usage: session.usage?.snapshot() || null,
    memory: session.context?.snapshot() || { directlyLoadedIdentifiers: [], nodes: [] },
  };
}

function update() {
  const visibleTranscript = viewedRecord?.state?.transcript || session?.transcript || [];
  renderTranscript(ui.transcript, visibleTranscript);
  const selectedId = viewedRecord?.id || (!ingressRequired ? historyRecord?.id : null);
  renderConversationHistory(ui.conversation_history, historyRecords, { selectedId, onSelect: selectConversation });
  const currentDiagnostic = diagnostic();
  renderInspector(ui.context_inspector, currentDiagnostic, inspectorMode);
  renderUsage(ui.usage_metrics, currentDiagnostic);
  for (const mode of INSPECTOR_MODES) {
    const button = ui[`inspector_${mode}`];
    const active = inspectorMode === mode;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  }
  ui.ingress_panel.classList.toggle("hidden", !ingressActivity);
  if (ingressActivity) {
    ui.ingress_title.textContent = ingressDiagnostic ? "History ingress · live" : "History ingress · complete";
    renderIngressActivity(ui.ingress_log, ingressActivity, Boolean(ingressDiagnostic));
  }
  const viewingHistory = Boolean(viewedRecord);
  const controls = conversationControlState({
    hasSession: Boolean(session), sessionBusy: Boolean(session?.busy), transitionBusy: ending,
    ingressRequired, pendingTurn: Boolean(session?.pendingTurn), viewingHistory,
    transcriptLength: session?.transcript.length || 0,
  });
  ui.message_input.disabled = controls.inputDisabled;
  ui.send_button.disabled = controls.sendDisabled;
  ui.end_button.disabled = controls.endDisabled;
  ui.new_conversation.disabled = controls.newDisabled;
  ui.end_button.textContent = session?.pendingTurn ? "Retry saved query" : ingressRequired ? "Retry memory update" : "End conversation";
  ui.activity.textContent = viewingHistory ? "Viewing a saved conversation" : ending && ingressRequired ? "Updating memory in the background — you can write your next message" : session?.busy ? "Kennedy is working…" : session?.pendingTurn ? "Saved query needs a response" : ingressRequired ? "Write your next message; Send unlocks after the memory update" : "";
}

function upsertHistory(record) {
  if (!record) return;
  historyRecords = [record, ...historyRecords.filter(item => item.id !== record.id)]
    .sort((left, right) => String(right.updated_at).localeCompare(String(left.updated_at)));
}

async function selectConversation(id) {
  clearError(ui.error_banner);
  if (!ingressRequired && id === historyRecord?.id) {
    viewedRecord = null;
  } else {
    viewedRecord = historyRecords.find(item => item.id === id) || await conversationHistory.get(id);
  }
  update();
}

async function persistSession(state) {
  if (!historyRecord || historyRecord.phase !== "active") throw new Error("The conversation history backend is not ready to checkpoint this conversation.");
  try {
    historyRecord = await conversationHistory.checkpoint(historyRecord.id, { expected_version: historyRecord.version, state });
  } catch (error) {
    if (error.code !== "state_conflict") throw error;
    const latest = await conversationHistory.get(historyRecord.id);
    if (latest.phase !== "active" || JSON.stringify(latest.state) !== JSON.stringify(state)) throw error;
    historyRecord = latest;
  }
  upsertHistory(historyRecord);
}

async function buildConversation(restoredState = null) {
  ingressDiagnostic = null;
  session = new ConversationSession({ kweb, intelligence, manuals, rootNodeId, provider, model, contextWindowTokens, maxInputTokens, persist: persistSession, onUpdate: update });
  await session.initialize(restoredState);
}

async function startConversation(restoredRecord = null) {
  viewedRecord = null;
  historyRecord = restoredRecord;
  await buildConversation(restoredRecord?.state || null);
  if (!restoredRecord) {
    historyRecord = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
  }
  upsertHistory(historyRecord);
  ingressRequired = historyRecord.phase !== "active";
  update();
  if (!ingressRequired && !session.pendingTurn) ui.message_input.focus();
}

async function prepareNextConversation() {
  viewedRecord = null;
  ingressRequired = true;
  await buildConversation();
  update();
  ui.message_input.focus();
}

async function submitMessage(event) {
  event.preventDefault(); if (!session || session.busy || ingressRequired || viewedRecord) return;
  const text = ui.message_input.value; if (!text.trim()) return;
  ui.message_input.value = ""; clearError(ui.error_banner);
  try { await session.send(text); }
  catch (error) { showError(ui.error_banner, error.message); }
  update();
}

async function resumeSavedQuery() {
  if (!session?.pendingTurn || session.busy) return;
  clearError(ui.error_banner);
  try { await session.resumePendingTurn(); }
  catch (error) { showError(ui.error_banner, `The saved query could not be resumed: ${error.message}`); }
  update();
}

async function beginNewConversation() {
  if (ending || session?.busy || session?.pendingTurn) return;
  if (ingressRequired) {
    viewedRecord = null;
    update();
    ui.message_input.focus();
    return;
  }
  if (!session?.transcript.length) {
    viewedRecord = null;
    update();
    ui.message_input.focus();
    return;
  }
  ingressSourceSession = session;
  ingressRecord = historyRecord;
  ingressDiagnostic = null;
  ingressActivity = null;
  clearError(ui.error_banner);
  ui.message_input.value = "";
  await prepareNextConversation();
  finishHistoryIngress();
}

async function finishHistoryIngress() {
  if (ending || !ingressRecord || !ingressSourceSession) return;
  ending = true;
  ingressRequired = true;
  clearError(ui.error_banner);
  update();
  try {
    let record = await conversationHistory.get(ingressRecord.id);
    if (record.phase === "active") {
      record = await conversationHistory.requestIngress(record.id, { expected_version: record.version, state: ingressSourceSession.snapshot() });
      ingressRecord = record; historyRecord = record; upsertHistory(record); update();
    }
    if (record.phase === "ingress_pending") {
      const provenance = await kweb.createProvenance({ data: ingressSourceSession.serialize(), source: "conversation", source_created_at: ingressSourceSession.startedAt, idempotency_key: `conversation:${record.id}` });
      record = await conversationHistory.ingressStarted(record.id, { expected_version: record.version, provenance_id: provenance.id });
      ingressRecord = record; historyRecord = record; upsertHistory(record); update();
    }
    if (record.phase === "ingress_in_progress") {
      if (!record.provenance_id) throw new Error("The saved conversation is missing its history provenance.");
      ingressActivity = await runHistoryIngress({ kweb, intelligence, manuals, rootNodeId, provenanceId: record.provenance_id, provider, model, contextWindowTokens, maxInputTokens, onUpdate: value => { ingressDiagnostic = value; ingressActivity = value; update(); } });
      ingressDiagnostic = null;
      record = await conversationHistory.ingressCompleted(record.id, { expected_version: record.version });
    }
    if (record.phase !== "complete") throw new Error("The saved conversation did not reach a completed history state.");
    upsertHistory(record);
    historyRecord = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
    upsertHistory(historyRecord);
    ingressRecord = null;
    ingressSourceSession = null;
    ingressRequired = false;
    ui.service_status.textContent = "Conversation saved to memory";
    ui.message_input.focus();
  } catch (error) {
    showError(ui.error_banner, `Your next message is waiting for memory ingestion to finish: ${error.message}`);
    ui.service_status.textContent = "Memory ingestion needs attention";
  } finally {
    ending = false;
    update();
  }
}

function showView(memory) {
  ui.chat_view.classList.toggle("hidden", memory); ui.memory_view.classList.toggle("hidden", !memory);
  ui.chat_tab.classList.toggle("active", !memory); ui.memory_tab.classList.toggle("active", memory);
  if (memory && explorer && !explorer.currentNodeId) explorer.home();
}

async function initialize() {
  update();
  try {
    const [health, user, loadedManuals] = await Promise.all([kweb.health(), kweb.user(), loadPromptManuals(CONFIG.kwebBase)]);
    rootNodeId = user.root_node_id; manuals = loadedManuals;
    explorer = new MemoryExplorer({ api: kweb, rootNodeId, content: ui.memory_content, backButton: ui.memory_back, forwardButton: ui.memory_forward });
    ui.service_status.textContent = `${health.status} · memory ready`;
  } catch (error) {
    ui.service_status.textContent = "Kweb unavailable"; showError(ui.error_banner, error.message); update(); return;
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
    const current = (await conversationHistory.current()).conversation;
    await startConversation(current);
    if (historyRecord.phase !== "active") {
      ingressSourceSession = session;
      ingressRecord = historyRecord;
      await prepareNextConversation();
      finishHistoryIngress();
    } else if (session.pendingTurn) {
      ui.service_status.textContent = `Restoring saved query · ${model}`; update();
      await resumeSavedQuery();
    }
    if (!ingressRequired) ui.service_status.textContent = `Ready · ${model}`;
  } catch (error) {
    ui.service_status.textContent = "Chat offline · memory ready";
    showError(ui.error_banner, `The memory explorer is available, but chat is offline: ${error.message}`); update();
  }
}

ui.message_form.addEventListener("submit", submitMessage);
ui.message_input.addEventListener("keydown", event => { if ((event.ctrlKey || event.metaKey) && event.key === "Enter") { event.preventDefault(); ui.message_form.requestSubmit(); } });
ui.end_button.addEventListener("click", () => session?.pendingTurn ? resumeSavedQuery() : ingressRequired ? finishHistoryIngress() : beginNewConversation().catch(error => showError(ui.error_banner, error.message)));
ui.new_conversation.addEventListener("click", () => beginNewConversation().catch(error => showError(ui.error_banner, error.message)));
ui.dismiss_ingress.addEventListener("click", () => { ingressActivity = null; update(); });
for (const mode of INSPECTOR_MODES) ui[`inspector_${mode}`].addEventListener("click", () => { inspectorMode = mode; update(); });
ui.chat_tab.addEventListener("click", () => showView(false));
ui.memory_tab.addEventListener("click", () => showView(true));
ui.memory_back.addEventListener("click", () => explorer?.goBack());
ui.memory_forward.addEventListener("click", () => explorer?.goForward());
ui.memory_home.addEventListener("click", () => explorer?.home());
ui.copy_context.addEventListener("click", async () => { try { await navigator.clipboard.writeText(inspectorText(diagnostic(), inspectorMode)); ui.copy_context.textContent = "Copied"; setTimeout(() => { ui.copy_context.textContent = "Copy view"; }, 1200); } catch { showError(ui.error_banner, "Could not copy Kennedy's context to the clipboard."); } });

initialize().catch(error => {
  ui.service_status.textContent = "Startup failed";
  showError(ui.error_banner, `Kennedy could not initialize: ${error.message}`);
  console.error("Kennedy startup failed", error);
});
