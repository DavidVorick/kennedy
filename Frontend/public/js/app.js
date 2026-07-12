import { KwebAPI, IntelligenceAPI, ConversationHistoryAPI } from "./api.js?v=20260712.3";
import { loadPromptManuals } from "./prompt_composer.js?v=20260712.3";
import { ConversationSession } from "./conversation.js?v=20260712.3";
import { runHistoryIngress } from "./history_ingress.js?v=20260712.3";
import { MemoryExplorer } from "./memory_explorer.js?v=20260712.3";
import { renderTranscript, renderInspector, renderUsage, renderIngressActivity, inspectorText, showError, clearError } from "./render.js?v=20260712.3";

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
  "service-status", "chat-view", "memory-view", "chat-tab", "memory-tab", "transcript", "error-banner", "message-form", "message-input", "send-button", "end-button", "activity", "context-inspector", "copy-context", "usage-metrics", "inspector-full", "inspector-system", "inspector-memory", "ingress-panel", "ingress-title", "ingress-log", "dismiss-ingress", "memory-content", "memory-back", "memory-forward", "memory-home",
].map(id => [id.replaceAll("-", "_"), document.getElementById(id)]));

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
let inspectorMode = "full";

function diagnostic() {
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
  renderTranscript(ui.transcript, session?.transcript || []);
  const currentDiagnostic = diagnostic();
  renderInspector(ui.context_inspector, currentDiagnostic, inspectorMode);
  renderUsage(ui.usage_metrics, currentDiagnostic);
  for (const mode of ["full", "system", "memory"]) {
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
  const busy = Boolean(session?.busy || ending);
  const blocked = busy || ingressRequired || session?.pendingTurn;
  ui.message_input.disabled = blocked || !session;
  ui.send_button.disabled = blocked || !session;
  ui.end_button.disabled = busy || !session || (!ingressRequired && !session.pendingTurn && !session.transcript.length);
  ui.end_button.textContent = session?.pendingTurn ? "Retry saved query" : ingressRequired ? "Retry memory update" : "End conversation";
  ui.activity.textContent = ending ? "Saving conversation to memory…" : session?.busy ? "Kennedy is working…" : session?.pendingTurn ? "Saved query needs a response" : ingressRequired ? "Memory ingestion must finish before a new conversation" : "";
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
}

async function startConversation(restoredRecord = null) {
  ingressDiagnostic = null;
  historyRecord = restoredRecord;
  session = new ConversationSession({ kweb, intelligence, manuals, rootNodeId, provider, model, contextWindowTokens, maxInputTokens, persist: persistSession, onUpdate: update });
  await session.initialize(restoredRecord?.state || null);
  if (!restoredRecord) historyRecord = await conversationHistory.create({ started_at: session.startedAt, state: session.snapshot() });
  ingressRequired = historyRecord.phase !== "active";
  update();
  if (!ingressRequired && !session.pendingTurn) ui.message_input.focus();
}

async function submitMessage(event) {
  event.preventDefault(); if (!session || session.busy) return;
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

async function endConversation() {
  if ((!session?.transcript.length && !ingressRequired) || ending) return;
  ending = true; ingressRequired = true; ingressDiagnostic = null; ingressActivity = null; clearError(ui.error_banner); update();
  try {
    historyRecord = await conversationHistory.get(historyRecord.id);
    if (historyRecord.phase === "complete") {
      ingressRequired = false;
      await startConversation();
      return;
    }
    if (historyRecord.phase === "active") {
      historyRecord = await conversationHistory.requestIngress(historyRecord.id, { expected_version: historyRecord.version, state: session.snapshot() });
    }
    if (historyRecord.phase === "ingress_pending") {
      const provenance = await kweb.createProvenance({ data: session.serialize(), source: "conversation", source_created_at: session.startedAt, idempotency_key: `conversation:${historyRecord.id}` });
      historyRecord = await conversationHistory.ingressStarted(historyRecord.id, { expected_version: historyRecord.version, provenance_id: provenance.id });
    }
    if (historyRecord.phase !== "ingress_in_progress" || !historyRecord.provenance_id) throw new Error("The saved conversation is not ready for history ingress.");
    ingressActivity = await runHistoryIngress({ kweb, intelligence, manuals, rootNodeId, provenanceId: historyRecord.provenance_id, provider, model, contextWindowTokens, maxInputTokens, onUpdate: value => { ingressDiagnostic = value; ingressActivity = value; update(); } });
    ingressDiagnostic = null;
    historyRecord = await conversationHistory.ingressCompleted(historyRecord.id, { expected_version: historyRecord.version });
    ingressRequired = false;
    ui.service_status.textContent = "Conversation saved to memory";
    await startConversation();
  } catch (error) {
    showError(ui.error_banner, `A new conversation cannot start until memory ingestion succeeds: ${error.message}`);
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
    const current = (await conversationHistory.current()).conversation;
    await startConversation(current);
    if (historyRecord.phase !== "active") {
      await endConversation();
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
ui.end_button.addEventListener("click", () => session?.pendingTurn ? resumeSavedQuery() : endConversation());
ui.dismiss_ingress.addEventListener("click", () => { ingressActivity = null; update(); });
for (const mode of ["full", "system", "memory"]) ui[`inspector_${mode}`].addEventListener("click", () => { inspectorMode = mode; update(); });
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
