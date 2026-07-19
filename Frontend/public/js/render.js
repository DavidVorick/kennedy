import { formatKmapContext } from "./human_format.js?v=20260719.1";
import { contextUsageMeasurement, formatChatend } from "./chatend_format.js?v=20260719.2";

const RESPONSE_PREVIEW_CHARACTERS = 500;

function characterCount(value) {
  return [...String(value ?? "")].length;
}

function characterLabel(value) {
  const count = typeof value === "number" ? value : characterCount(value);
  return `${new Intl.NumberFormat("en-US").format(count)} character${count === 1 ? "" : "s"}`;
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function captureViewState(container, viewKey) {
  const sameView = container.dataset.renderKey === viewKey;
  const focused = sameView ? document.activeElement : null;
  return {
    sameView,
    previousTop: container.scrollTop,
    wasAtBottom: container.scrollHeight - container.clientHeight - container.scrollTop <= 1,
    openKeys: sameView
      ? new Set([...container.querySelectorAll("details[open][data-view-key]")].map(details => details.dataset.viewKey))
      : new Set(),
    nestedScroll: sameView
      ? new Map([...container.querySelectorAll("[data-scroll-key]")].map(node => [
        node.dataset.scrollKey,
        {
          top: node.scrollTop,
          left: node.scrollLeft,
          wasAtBottom: node.scrollHeight - node.clientHeight - node.scrollTop <= 1,
        },
      ]))
      : new Map(),
    focusKey: focused?.dataset?.focusKey || focused?.closest?.("[data-focus-key]")?.dataset?.focusKey || null,
  };
}

function restoreViewState(container, viewKey, state) {
  container.dataset.renderKey = viewKey;
  if (!state.sameView) {
    container.scrollTop = 0;
    return;
  }
  for (const details of container.querySelectorAll("details[data-view-key]")) {
    details.open = state.openKeys.has(details.dataset.viewKey);
  }
  for (const node of container.querySelectorAll("[data-scroll-key]")) {
    const saved = state.nestedScroll.get(node.dataset.scrollKey);
    if (!saved) continue;
    node.scrollTop = saved.wasAtBottom ? node.scrollHeight : saved.top;
    node.scrollLeft = saved.left;
  }
  container.scrollTop = state.wasAtBottom ? container.scrollHeight : state.previousTop;
  if (state.focusKey) {
    const target = [...container.querySelectorAll("[data-focus-key]")]
      .find(node => node.dataset.focusKey === state.focusKey);
    target?.focus?.({ preventScroll: true });
  }
}

function appendLinkedText(container, text) {
  const pattern = /\[([^\]\n]+)\]\((https?:\/\/[^\s)]+)\)|(https?:\/\/[^\s<]+)/g;
  let cursor = 0;
  for (const match of text.matchAll(pattern)) {
    container.append(document.createTextNode(text.slice(cursor, match.index)));
    let label = match[1];
    let url = match[2] || match[3];
    let trailing = "";
    if (!match[2]) {
      const cleaned = url.replace(/[.,;:!?]+$/, "");
      trailing = url.slice(cleaned.length);
      url = cleaned; label = cleaned;
    }
    const link = element("a", "source-link", label);
    link.href = url; link.target = "_blank"; link.rel = "noopener noreferrer";
    container.append(link, document.createTextNode(trailing));
    cursor = match.index + match[0].length;
  }
  container.append(document.createTextNode(text.slice(cursor)));
}

function ingressRetryNotice(retrying, onRetry) {
  const notice = element("section", "ingress-retry-notice");
  notice.setAttribute("aria-label", "Retry failed history ingress");
  const copy = element("div");
  copy.append(
    element("span", "eyebrow", "ACTION REQUIRED"),
    element("strong", "", "History ingress failed"),
    element("p", "", "The conversation is preserved. Restart Kennedy’s memory update with a fresh model context."),
  );
  const retry = element("button", "primary", retrying ? "Scheduling retry…" : "Retry history ingress");
  retry.type = "button";
  retry.disabled = retrying;
  retry.addEventListener("click", onRetry);
  notice.append(copy, retry);
  return notice;
}

export function renderTranscript(container, transcript, ingressActivity = null, viewKey = "transcript", retryAction = null) {
  const viewState = captureViewState(container, viewKey);
  container.replaceChildren();
  if (!transcript.length && !ingressActivity?.diagnostic) {
    const empty = element("div", "empty-state");
    empty.append(element("p", "empty-title", "What are we working on?"), element("p", "", "Kennedy can help directly and draw on your local memory when it matters."));
    container.append(empty);
    restoreViewState(container, viewKey, viewState);
    return;
  }
  if (ingressActivity?.failed && typeof retryAction?.onRetry === "function") {
    container.append(ingressRetryNotice(Boolean(retryAction.retrying), retryAction.onRetry));
  }
  for (const item of transcript) {
    const message = element("article", `message ${item.role === "kennedy" ? "assistant" : "user"}`);
    const body = element("div", "body"); appendLinkedText(body, item.content);
    message.append(element("span", "role", item.role === "kennedy" ? "Kennedy" : "You"));
    if (item.inputKind === "voice") message.append(element("span", "voice-note-badge", "Voice note · paid transcription"));
    if (Array.isArray(item.attachments) && item.attachments.length) {
      message.append(element("span", "voice-note-badge", `${item.attachments.length} document${item.attachments.length === 1 ? "" : "s"} · ${item.attachments.map(attachment => attachment.fileName).join(", ")}`));
    }
    message.append(body);
    container.append(message);
  }
  if (ingressActivity?.diagnostic) {
    renderIngressActivity(
      container,
      ingressActivity.diagnostic,
      ingressActivity.active,
      ingressActivity.failed,
      ingressActivity.failures,
      { namespace: `${viewKey}:history-ingress` },
    );
  }
  restoreViewState(container, viewKey, viewState);
}

export function conversationTitle(record, limit = 54) {
  const sessionType = record?.state?.sessionType || record?.state?.archive?.sessionType;
  if (sessionType === "free-time") {
    const freeTime = record?.state?.freeTime || record?.state?.archive?.freeTime || {};
    const customPrompt = String(freeTime.customPrompt || "").replace(/\s+/g, " ").trim();
    const suffix = ` · session ${freeTime.sliceIndex || 1}`;
    if (!customPrompt) return `Self time${suffix}`;
    const title = `${customPrompt}${suffix}`;
    if (title.length <= limit) return title;
    const promptLimit = limit - suffix.length;
    return promptLimit > 1
      ? `${customPrompt.slice(0, promptLimit - 1).trimEnd()}…${suffix}`
      : `Self time${suffix}`;
  }
  if (sessionType === "telegram-group") {
    const channel = record?.state?.channel || record?.state?.archive?.channel || {};
    const title = channel.groupContext?.groupTitle || "Telegram group";
    return channel.backgroundIngress ? `${title} · background ingress` : title;
  }
  if (sessionType === "telegram") {
    const username = record?.state?.channel?.username || record?.state?.archive?.channel?.username;
    const displayName = record?.state?.channel?.displayName || record?.state?.archive?.channel?.displayName || "Telegram user";
    return username ? `@${String(username).replace(/^@/, "")}` : displayName;
  }
  const transcript = Array.isArray(record?.state?.transcript) ? record.state.transcript : [];
  const firstUserMessage = transcript.find(item => item?.role === "user" && typeof item.content === "string")?.content;
  const normalized = (firstUserMessage || "New conversation").replace(/\s+/g, " ").trim() || "New conversation";
  return normalized.length > limit ? `${normalized.slice(0, limit - 1).trimEnd()}…` : normalized;
}

function conversationPhaseRank(phase) {
  if (phase === "active") return 0;
  if (phase === "complete") return 2;
  return 1;
}

export function sortConversationHistory(records) {
  return [...(records || [])].sort((left, right) =>
    conversationPhaseRank(left?.phase) - conversationPhaseRank(right?.phase) ||
    String(right?.updated_at || "").localeCompare(String(left?.updated_at || "")) ||
    String(right?.started_at || "").localeCompare(String(left?.started_at || "")) ||
    String(left?.id || "").localeCompare(String(right?.id || ""))
  );
}

export function reconcileConversationHistory(cachedRecords, incomingRecords) {
  const cachedById = new Map((cachedRecords || []).map(record => [record.id, record]));
  return sortConversationHistory((incomingRecords || []).map(record => {
    const cached = cachedById.get(record.id);
    const cachedVersion = Number(cached?.version);
    const incomingVersion = Number(record?.version);
    if (cachedVersion > incomingVersion) return cached;
    if (cachedVersion === incomingVersion && !cached?.summary && record?.summary) return cached;
    return record;
  }));
}

function historyDate(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return "Saved";
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" }).format(parsed);
}

export function renderConversationHistory(container, records, {
  selectedId = null,
  onSelect = () => {},
  retryingIds = new Set(),
  onRetryIngress = () => {},
  purgingIds = new Set(),
  onPurge = () => {},
  viewKey = "conversation-history",
} = {}) {
  const viewState = captureViewState(container, viewKey);
  container.replaceChildren();
  if (!records.length) {
    container.append(element("p", "history-empty", "Past conversations will appear here after you begin chatting."));
    restoreViewState(container, viewKey, viewState);
    return;
  }
  for (const record of records) {
    const row = element("div", `history-item-row${record.id === selectedId ? " selected" : ""}`);
    const button = element("button", `history-item${record.id === selectedId ? " selected" : ""}`);
    button.type = "button";
    button.dataset.focusKey = `${viewKey}:record:${record.id}`;
    button.setAttribute("aria-pressed", String(record.id === selectedId));
    const meta = element("span", "history-item-meta");
    const phases = {
      active: "Live · Continue",
      ingress_pending: "Closed · Memory queued",
      ingress_in_progress: "Closed · Updating memory",
      ingress_failed: "Closed · Memory failed",
      complete: "Saved · Read only",
    };
    const basePhase = phases[record.phase] || record.phase.replaceAll("_", " ");
    const recordSessionType = record?.state?.sessionType || record?.state?.archive?.sessionType;
    const phase = recordSessionType === "free-time" && record.phase === "active" ? "Live · Self time" : basePhase;
    const status = element("span", `history-phase ${record.phase === "active" ? "live" : "closed"}`, phase);
    status.setAttribute("aria-label", phase);
    meta.append(status, element("time", "", historyDate(record.started_at)));
    button.append(element("span", "history-item-title", conversationTitle(record)), meta);
    button.addEventListener("click", () => onSelect(record.id));
    row.append(button);
    const actions = element("div", "history-item-actions");
    if (record.phase === "ingress_failed") {
      const retry = element("button", "quiet history-item-retry", retryingIds.has(record.id) ? "Retrying…" : "Retry");
      retry.type = "button";
      retry.disabled = retryingIds.has(record.id);
      retry.setAttribute("aria-label", `Retry history ingress for ${conversationTitle(record, 100)}`);
      retry.addEventListener("click", () => onRetryIngress(record));
      actions.append(retry);
    }
    if (record.id === selectedId) {
      const purging = purgingIds.has(record.id);
      const purge = element("button", "quiet history-item-purge", purging ? "Purging…" : "Purge");
      purge.type = "button";
      purge.disabled = purging;
      purge.setAttribute("aria-label", `Permanently purge ${conversationTitle(record, 100)}`);
      purge.addEventListener("click", () => onPurge(record));
      actions.append(purge);
    }
    if (actions.childNodes.length) row.append(actions);
    container.append(row);
  }
  restoreViewState(container, viewKey, viewState);
}

const AUDIO_STATUS_LABELS = {
  uploaded: "Uploaded · queued",
  chunking: "Preparing audio",
  transcribing: "Gemini transcription",
  reconciling: "Reconciling transcript",
  ready_for_ingress: "Transcript · queued",
  ingressing: "Updating memory",
  ingress_failed: "Memory ingress failed",
  complete: "Complete",
  failed: "Processing failed",
};

export function audioRecordingTitle(record, limit = 54) {
  const title = String(record?.original_filename || "Audio recording").trim() || "Audio recording";
  return title.length > limit ? `${title.slice(0, limit - 1).trimEnd()}…` : title;
}

export function renderAudioHistory(container, records, {
  selectedId = null,
  onSelect = () => {},
  retryingIds = new Set(),
  onRetryIngress = () => {},
  viewKey = "audio-history",
} = {}) {
  const viewState = captureViewState(container, viewKey);
  container.replaceChildren();
  if (!records.length) {
    container.append(element("p", "history-empty", "Uploaded vnotes will appear here as soon as Kennedy accepts them."));
    restoreViewState(container, viewKey, viewState);
    return;
  }
  for (const record of records) {
    const selected = record.id === selectedId;
    const row = element("div", `history-item-row${selected ? " selected" : ""}`);
    const button = element("button", `history-item${selected ? " selected" : ""}`);
    button.type = "button";
    button.dataset.focusKey = `${viewKey}:record:${record.id}`;
    button.setAttribute("aria-pressed", String(selected));
    const meta = element("span", "history-item-meta");
    const label = AUDIO_STATUS_LABELS[record.status] || String(record.status || "unknown").replaceAll("_", " ");
    const active = !["complete", "failed", "ingress_failed"].includes(record.status);
    const status = element("span", `history-phase ${active ? "live" : "closed"}`, label);
    status.setAttribute("aria-label", label);
    meta.append(status, element("time", "", historyDate(record.source_created_at)));
    button.append(element("span", "history-item-title", audioRecordingTitle(record)), meta);
    button.addEventListener("click", () => onSelect(record.id));
    row.append(button);
    if (record.status === "ingress_failed") {
      const retry = element("button", "quiet history-item-retry", retryingIds.has(record.id) ? "Retrying…" : "Retry");
      retry.type = "button";
      retry.disabled = retryingIds.has(record.id);
      retry.setAttribute("aria-label", `Retry failed memory ingress for ${audioRecordingTitle(record, 100)}`);
      retry.addEventListener("click", () => onRetryIngress(record));
      row.append(retry);
    }
    container.append(row);
  }
  restoreViewState(container, viewKey, viewState);
}

function fullDate(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return value || "Unavailable";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" }).format(parsed);
}

function fileSize(bytes) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value < 0) return "Unknown";
  if (value < 1024) return `${value} B`;
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1024 ** 2).toFixed(1)} MiB`;
}

function audioDetail(label, value) {
  const item = element("div", "audio-detail");
  item.append(element("dt", "", label), element("dd", "", String(value)));
  return item;
}

function audioDisclosure(label, content, key, open = false) {
  const disclosure = element("details", "audio-disclosure");
  disclosure.dataset.viewKey = key;
  disclosure.open = open;
  const summary = element("summary", "audio-disclosure-summary");
  summary.dataset.focusKey = `${key}:summary`;
  summary.append(
    element("span", "audio-disclosure-label", label),
    element("span", "audio-disclosure-size", characterLabel(content)),
  );
  const body = element("pre", "audio-disclosure-body", content);
  body.dataset.scrollKey = `${key}:body`;
  disclosure.append(summary, body);
  return disclosure;
}

function audioRetryButton(piece, retryingPieceIds, onRetryPiece, viewKey) {
  const retry = element(
    "button",
    "secondary audio-retry-button",
    retryingPieceIds.has(piece.id) ? "Scheduling retry…" : "Retry Kennedy ingress",
  );
  retry.type = "button";
  retry.dataset.focusKey = `${viewKey}:piece:${piece.id}:retry`;
  retry.disabled = retryingPieceIds.has(piece.id);
  retry.addEventListener("click", () => onRetryPiece(piece));
  return retry;
}

export function renderAudioRecording(container, detail, {
  loading = false,
  error = null,
  retryingPieceIds = new Set(),
  onRetryPiece = () => {},
  ingressActivities = new Map(),
  viewKey = "audio-recording",
} = {}) {
  const viewState = captureViewState(container, viewKey);
  container.replaceChildren();
  if (loading) {
    container.append(element("div", "audio-empty", "Loading the durable audio-ingress history…"));
    restoreViewState(container, viewKey, viewState);
    return;
  }
  if (error) {
    container.append(element("div", "audio-empty", `Could not load this recording: ${error}`));
    restoreViewState(container, viewKey, viewState);
    return;
  }
  if (!detail?.recording) {
    container.append(element("div", "audio-empty", "Upload a vnote to see its complete processing and memory-ingress history here."));
    restoreViewState(container, viewKey, viewState);
    return;
  }

  const record = detail.recording;
  const header = element("header", "audio-recording-header");
  header.append(
    element("span", "eyebrow", "DURABLE VNOTE"),
    element("h2", "", audioRecordingTitle(record, 100)),
    element("span", `audio-status audio-status-${record.status}`, AUDIO_STATUS_LABELS[record.status] || record.status),
  );
  const metadata = element("dl", "audio-metadata");
  metadata.append(
    audioDetail("Recorded", fullDate(record.source_created_at)),
    audioDetail("Kennedy received", fullDate(record.received_at)),
    audioDetail("File size", fileSize(record.size_bytes)),
    audioDetail("SHA-256", record.sha256),
    audioDetail("Gemini model", record.gemini_model),
    audioDetail("Reconciliation", `${record.reconciliation_model} · ${record.reconciliation_reasoning}`),
    audioDetail("Kennedy pieces", `${record.completed_piece_count}/${record.transcript_piece_count} complete`),
  );
  container.append(header, metadata);
  if (record.last_error) container.append(element("pre", "audio-error", record.last_error));

  const failedPieces = (detail.pieces || []).filter(piece => piece.phase === "ingress_failed");
  if (failedPieces.length) {
    const retryPanel = element("section", "audio-retry-panel");
    retryPanel.setAttribute("aria-label", "Failed Kennedy audio ingress");
    retryPanel.append(
      element("span", "eyebrow", "ACTION REQUIRED"),
      element("h3", "", failedPieces.length === 1
        ? "Kennedy memory ingress failed"
        : `${failedPieces.length} Kennedy memory ingress pieces failed`),
      element("p", "", "The transcript is preserved. Retry each failed piece when Kennedy is ready to continue updating the Kmap."),
    );
    for (const piece of failedPieces) {
      const action = element("div", "audio-retry-action");
      action.append(
        element("span", "", `Transcript piece ${piece.piece_index + 1}/${piece.piece_count}`),
        audioRetryButton(piece, retryingPieceIds, onRetryPiece, viewKey),
      );
      retryPanel.append(action);
    }
    container.append(retryPanel);
  }

  container.append(element("p", "audio-history-note", "History ingress appears below the transcript pieces as the same continuous memory-update stream used for conversations. The right-hand Full History inspector retains every piece and context reset."));

  const finalTranscript = typeof detail.final_transcript === "string" && detail.final_transcript.trim()
    ? detail.final_transcript
    : "The reconciled transcript has not been produced yet.";
  container.append(audioDisclosure(
    "Final reconciled transcript",
    finalTranscript,
    `${viewKey}:final-transcript`,
  ));

  const chunks = element("section", "audio-history-section");
  chunks.append(element("h3", "", `Gemini chunk transcripts (${detail.chunks?.length || 0})`));
  if (!detail.chunks?.length) {
    chunks.append(element("p", "audio-section-empty", "Audio chunks have not been created yet."));
  } else {
    for (const chunk of detail.chunks) {
      const start = (Number(chunk.audio_start_ms) / 1000).toFixed(1);
      const end = (Number(chunk.audio_end_ms) / 1000).toFixed(1);
      const transcript = chunk.transcript
        ? JSON.stringify(chunk.transcript, null, 2)
        : "Gemini has not transcribed this chunk yet.";
      chunks.append(audioDisclosure(
        `Chunk ${chunk.chunk_index + 1} · ${start}s–${end}s`,
        transcript,
        `${viewKey}:chunk:${chunk.chunk_index}`,
      ));
    }
  }
  container.append(chunks);

  const pieces = element("section", "audio-history-section");
  pieces.append(element("h3", "", `Kennedy transcript pieces (${detail.pieces?.length || 0})`));
  if (!detail.pieces?.length) {
    pieces.append(element("p", "audio-section-empty", "The final transcript has not been divided for Kennedy yet."));
  } else {
    for (const piece of detail.pieces) {
      const archived = piece.state?.historyIngress?.format === "kennedy-chatend";
      const label = `Piece ${piece.piece_index + 1}/${piece.piece_count} · ${piece.phase.replaceAll("_", " ")} · ~${Number(piece.estimated_tokens).toLocaleString()} tokens${archived ? " · Kennedy history saved" : ""}`;
      const disclosure = audioDisclosure(
        label,
        piece.transcript_text,
        `${viewKey}:piece:${piece.id}`,
      );
      pieces.append(disclosure);
    }
  }
  container.append(pieces);

  const ingress = element("section", "audio-history-section audio-memory-history");
  ingress.append(element("h3", "", `History ingress (${detail.pieces?.length || 0})`));
  if (!detail.pieces?.length) {
    ingress.append(element("p", "audio-section-empty", "Kennedy memory ingress has not started because no transcript pieces are ready."));
  } else {
    for (const piece of detail.pieces) {
      const activity = ingressActivities.get(piece.id);
      if (!activity?.diagnostic) continue;
      renderIngressActivity(
        ingress,
        activity.diagnostic,
        activity.active,
        activity.failed,
        activity.failures,
        {
          namespace: `${viewKey}:piece:${piece.id}:history-ingress`,
          sourceLabel: `Transcript piece ${piece.piece_index + 1}/${piece.piece_count}`,
        },
      );
    }
  }
  container.append(ingress);
  restoreViewState(container, viewKey, viewState);
}

export function conversationControlState({ hasSession, sessionBusy, transitionBusy, pendingTurn, viewingHistory, transcriptLength }) {
  return {
    composerHidden: viewingHistory,
    inputDisabled: viewingHistory || !hasSession,
    sendDisabled: sessionBusy || transitionBusy || pendingTurn || viewingHistory || !hasSession,
    endDisabled: sessionBusy || transitionBusy || viewingHistory || !hasSession || (!pendingTurn && !transcriptLength),
    stopHidden: !sessionBusy || viewingHistory || !hasSession,
    newDisabled: transitionBusy,
  };
}

export function conversationIngressActivity({ record, liveRecordId = null, liveDiagnostic = null, dismissedId = null }) {
  if (!record || record.id === dismissedId) return null;
  const archive = record.state?.historyIngress;
  const saved = archive?.format === "kennedy-chatend" && archive?.sessionType === "history-ingress"
    ? { chatend: { messages: archive.messages || [] }, usage: { snapshot: () => archive.usage || null }, toolLog: archive.tools?.log || [] }
    : null;
  const failed = record.phase === "ingress_failed";
  const failures = Array.isArray(record.ingress_failures) ? record.ingress_failures : [];
  const diagnostic = record.id === liveRecordId && liveDiagnostic
    ? liveDiagnostic
    : saved || (failed ? { chatend: { messages: [] }, usage: { snapshot: () => null }, toolLog: [] } : null);
  if (!diagnostic) return null;
  return {
    diagnostic,
    active: record.phase === "ingress_pending" || record.phase === "ingress_in_progress",
    failed,
    failures,
  };
}

export function ingressMutationSummary(diagnostic) {
  const toolLog = diagnostic?.executor?.toolLog || diagnostic?.toolLog || [];
  return toolLog.reduce((summary, entry) => {
    if (entry?.ok !== true) return summary;
    if (entry.name === "CreateNode") summary.nodesAdded += 1;
    else if (entry.name === "UpdateNode") summary.nodesUpdated += 1;
    else if (entry.name === "ConnectNodes") summary.connectCalls += 1;
    return summary;
  }, { nodesAdded: 0, nodesUpdated: 0, connectCalls: 0 });
}

export function ingressEntryPresentation(message) {
  const content = typeof message?.content === "string" ? message.content.trim() : "";
  if (message?.role === "assistant" && content.startsWith("KENNEDY_TOOL_CALLS")) {
    return { collapsed: true, label: "Kennedy tool call" };
  }
  if (message?.display_role === "Memory tool result") {
    return { collapsed: true, label: "Memory tool result" };
  }
  if (message?.display_role === "Coding tool result") {
    return { collapsed: true, label: "Coding tool result" };
  }
  if (message?.display_role === "Tool protocol error") {
    return { collapsed: true, label: "Tool protocol error" };
  }
  return { collapsed: false, label: message?.display_role || "Kennedy" };
}

export function renderInspector(container, diagnostic, view = "main", viewKey = `inspector:${view}`) {
  const viewState = captureViewState(container, viewKey);
  if (view === "history") {
    renderFullHistory(container, diagnostic);
    restoreViewState(container, viewKey, viewState);
    return;
  }
  if (view === "main") {
    renderMainView(container, diagnostic);
    restoreViewState(container, viewKey, viewState);
    return;
  }
  container.replaceChildren();
  if (view === "memory") {
    renderMemoryTree(container, diagnostic.memory);
    restoreViewState(container, viewKey, viewState);
    return;
  }
  const text = element("pre", "inspector-text", inspectorText(diagnostic, view));
  text.dataset.scrollKey = `${viewKey}:text`;
  container.append(text);
  restoreViewState(container, viewKey, viewState);
}

function parseToolRequest(content) {
  if (typeof content !== "string") return [];
  const trimmed = content.trim();
  if (!trimmed.startsWith("KENNEDY_TOOL_CALLS\n")) return [];
  try {
    const envelope = JSON.parse(trimmed.slice("KENNEDY_TOOL_CALLS".length).trim());
    return Array.isArray(envelope?.calls) ? envelope.calls.filter(call => call && typeof call.name === "string") : [];
  } catch {
    return [];
  }
}

function isToolResult(message) {
  return message?.display_role === "Memory tool result" ||
    message?.display_role === "Web tool result" ||
    message?.display_role === "Coding tool result" ||
    message?.display_role === "Tool protocol error" ||
    (typeof message?.content === "string" && message.content.startsWith("Kennedy tool result"));
}

function readableMessageContent(content) {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content.map(block => {
      if (typeof block === "string") return block;
      if (typeof block?.text === "string") return block.text;
      if (block?.image_url || block?.type?.includes?.("image")) return "[Image content]";
      try { return JSON.stringify(block, null, 2); } catch { return "[Structured content]"; }
    }).join("\n\n");
  }
  if (content === null || content === undefined) return "";
  try { return JSON.stringify(content, null, 2); } catch { return String(content); }
}

function loadedNodesForResult(message, call, memory) {
  if ((message?.tool_name || call?.name) !== "LoadNode" || message?.tool_result?.ok === false) return [];
  const result = message?.tool_result?.result;
  if (result?.requestedNode) return [{ node: result.requestedNode, relation: "direct" }];
  const requested = (memory?.nodes || []).find(node => node.identifier === call?.arguments?.identifier);
  if (!requested) return [];
  return [{ node: requested, relation: "direct" }];
}

function responsePreview(content) {
  const characters = [...content];
  return characters.length > RESPONSE_PREVIEW_CHARACTERS
    ? characters.slice(0, RESPONSE_PREVIEW_CHARACTERS).join("")
    : null;
}

function ingressProvenanceLabel(message, content) {
  if (message?.context_kind === "provenance") {
    return message.display_role || "Conversation provenance";
  }
  if (content.startsWith("Conversation provenance\n")) return "Conversation provenance";
  if (content.startsWith("Audio transcript provenance\n")) return "Audio transcript provenance";
  return null;
}

function addEntryTiming(entry, timing) {
  if (!entry || typeof timing !== "string" || !timing.trim()) return;
  if (!Array.isArray(entry.timing)) entry.timing = [];
  entry.timing.push(timing.trim());
}

function toolResultPresentation(message) {
  const content = readableMessageContent(message?.content);
  const [firstLine = "", ...followingLines] = content.split("\n");
  const match = firstLine.match(/^Kennedy tool result · (.+?) · (.+)$/);
  if (!match) return { content, timing: null };
  return {
    content: followingLines.join("\n").replace(/^\s+/, ""),
    timing: `${match[1]} ${match[2]}`,
  };
}

export function mainViewEntries(diagnostic) {
  const messages = Array.isArray(diagnostic?.chatend) ? diagnostic.chatend : [];
  const memory = diagnostic?.memory || { directlyLoadedIdentifiers: [], nodes: [] };
  const entries = [];
  const instructions = messages.filter(message => message?.context_kind === "instructions");
  const fallbackInstructions = instructions.length ? [] : messages.filter((message, index) => message?.role === "system" && index === 0);
  for (const message of [...instructions, ...fallbackInstructions]) {
    entries.push({ kind: "context", label: "System prompt", content: readableMessageContent(message.content) });
  }
  entries.push({ kind: "memory", memory });

  const pendingCalls = [];
  let pendingToolTiming = [];
  let lastVisibleEntry = null;
  for (const [messageIndex, message] of messages.entries()) {
    if (instructions.includes(message) || fallbackInstructions.includes(message) || message?.context_kind === "memory") continue;
    if (message?.context_kind === "timing") {
      const timing = readableMessageContent(message.content);
      if (pendingCalls.length || lastVisibleEntry?.kind === "tool-call") pendingToolTiming.push(timing);
      else addEntryTiming(lastVisibleEntry, timing);
      continue;
    }
    const calls = message?.role === "assistant" ? parseToolRequest(message.content) : [];
    if (calls.length) {
      for (const [callIndex, call] of calls.entries()) {
        const entry = {
          kind: "tool-call",
          label: `Tool call · ${call.name}`,
          content: JSON.stringify(call.arguments || {}, null, 2),
          key: `tool-call:${messageIndex}:${callIndex}`,
        };
        entries.push(entry);
        lastVisibleEntry = entry;
        pendingCalls.push(call);
      }
      continue;
    }
    if (message?.role === "assistant" && typeof message.content === "string" && message.content.trim().startsWith("KENNEDY_TOOL_CALLS")) {
      const entry = { kind: "tool-call", label: "Malformed tool call", content: message.content, key: `tool-call:${messageIndex}:malformed` };
      entries.push(entry);
      lastVisibleEntry = entry;
      continue;
    }
    if (isToolResult(message)) {
      const call = pendingCalls.shift() || (message.tool_name ? { name: message.tool_name, arguments: {} } : null);
      const loadedNodes = loadedNodesForResult(message, call, memory);
      const presentation = toolResultPresentation(message);
      if (loadedNodes.length) {
        let lastLoadedEntry = null;
        for (const [nodeIndex, loaded] of loadedNodes.entries()) {
          lastLoadedEntry = {
            kind: "loaded-node",
            label: `${loaded.relation === "active" ? "Active connection loaded" : "Node loaded"} · ${loaded.node.identifier}: ${loaded.node.shortName}`,
            node: loaded.node,
            relation: loaded.relation,
            key: `tool-result:${messageIndex}:node:${nodeIndex}`,
          };
          entries.push(lastLoadedEntry);
        }
        for (const timing of [...pendingToolTiming, presentation.timing]) addEntryTiming(lastLoadedEntry, timing);
        lastVisibleEntry = lastLoadedEntry;
      } else {
        const failed = message?.tool_result?.ok === false;
        const name = message?.tool_name || call?.name || message.display_role || "Tool";
        const entry = {
          kind: "tool-result",
          label: `${failed ? "Tool failed" : "Tool result"} · ${name}`,
          content: presentation.content,
          key: `tool-result:${messageIndex}`,
        };
        for (const timing of [...pendingToolTiming, presentation.timing]) addEntryTiming(entry, timing);
        entries.push(entry);
        lastVisibleEntry = entry;
      }
      pendingToolTiming = [];
      continue;
    }
    const provenanceLabel = ingressProvenanceLabel(message, readableMessageContent(message?.content));
    if (provenanceLabel) {
      const entry = {
        kind: "provenance",
        label: provenanceLabel,
        content: readableMessageContent(message.content),
        key: `provenance:${messageIndex}`,
      };
      entries.push(entry);
      lastVisibleEntry = entry;
      continue;
    }
    if ((message?.role === "user" || message?.role === "assistant") && !message?.context_kind) {
      const content = readableMessageContent(message.content);
      const preview = message.role === "assistant" ? responsePreview(content) : null;
      const entry = {
        kind: "conversation",
        role: message.role,
        label: message.display_role || (message.role === "assistant" ? "Kennedy" : "David"),
        content,
        preview,
        hiddenCharacters: preview === null ? 0 : characterCount(content) - characterCount(preview),
        key: `conversation:${messageIndex}`,
      };
      entries.push(entry);
      lastVisibleEntry = entry;
      continue;
    }
    const entry = {
      kind: "context",
      label: message?.display_role || (message?.role === "assistant" ? "Kennedy context" : "System context"),
      content: readableMessageContent(message?.content),
      key: `context:${messageIndex}`,
    };
    entries.push(entry);
    lastVisibleEntry = entry;
  }
  return entries;
}

export function inspectorText(diagnostic, view = "full") {
  if (view === "history") return fullHistoryText(diagnostic);
  if (view === "memory") return formatKmapContext(diagnostic.memory || { directlyLoadedIdentifiers: [], nodes: [] });
  let messages = diagnostic.chatend || [];
  if (view === "system") {
    const explicit = messages.filter(message => message.context_kind === "instructions");
    messages = explicit.length ? explicit : messages.filter((message, index) => message.role === "system" && index === 0);
  }
  if (view === "tools") {
    messages = messages.filter(message => {
      const content = typeof message.content === "string" ? message.content.trim() : "";
      const isRequest = message.role === "assistant" && content.startsWith("KENNEDY_TOOL_CALLS");
      const isResult = message.role === "user" && (
        message.display_role === "Memory tool result" ||
        message.display_role === "Web tool result" ||
        message.display_role === "Coding tool result" ||
        content.startsWith("Kennedy tool result")
      );
      return isRequest || isResult;
    });
    if (!messages.length) return "No tool calls are currently in the Chatend.";
  }
  return formatChatend(messages, view === "full" || view === "main" ? diagnostic.usage : null);
}

function tokenCount(value) {
  if (value === null || value === undefined) return "Unknown";
  return new Intl.NumberFormat("en-US", { notation: value >= 100000 ? "compact" : "standard", maximumFractionDigits: 1 }).format(value);
}

function exactTokenCount(value) {
  return value === null || value === undefined ? "Unknown" : new Intl.NumberFormat("en-US").format(value);
}

export function renderUsage(container, diagnostic) {
  const usage = diagnostic.usage;
  container.replaceChildren();
  if (!usage) {
    container.append(element("span", "context-usage-primary", "Usage unavailable"));
    return;
  }
  const { contextKnown, contextTokens, contextWindowTokens, contextRemaining } = contextUsageMeasurement(usage);
  const contextPercent = contextWindowTokens ? 100 * contextTokens / contextWindowTokens : 0;
  const cachePercent = usage.cacheReadPercent || 0;
  const primary = contextWindowTokens && !contextKnown
    ? `Current unknown / ${exactTokenCount(contextWindowTokens)}`
    : contextWindowTokens
    ? `${exactTokenCount(contextTokens)} / ${exactTokenCount(contextWindowTokens)}`
    : `${exactTokenCount(contextTokens)} used`;
  const remaining = !contextKnown
    ? "no successful LLM usage yet"
    : contextRemaining === null ? "window unknown" : `${exactTokenCount(contextRemaining)} remaining`;
  const text = element("div", "context-usage-text");
  text.append(
    element("strong", "context-usage-primary", primary),
    element("span", "context-usage-secondary", `${remaining} · ${cachePercent.toFixed(1)}% cache reads`),
  );
  const track = element("div", "context-usage-track");
  const fill = element("div", "context-usage-fill");
  fill.style.width = `${Math.max(0, Math.min(100, contextPercent))}%`;
  track.append(fill);
  container.title = [
    !contextKnown
      ? "Context occupancy is not available before the first successful LLM usage report"
      : `${exactTokenCount(contextTokens)} tokens in the latest successful LLM call`,
    `${remaining}`,
    `${exactTokenCount(usage.totalInputTokens)} cumulative input tokens`,
    `${exactTokenCount(usage.totalOutputTokens)} cumulative output tokens`,
    `${exactTokenCount(usage.totalCachedTokens)} cache-read tokens`,
    `${exactTokenCount(usage.totalCacheWriteTokens)} cache-write tokens`,
  ].join(" · ");
  container.append(text, track);
}

function badge(text, kind) { return element("span", `memory-badge ${kind}`, text); }

function connectionLeaf(connection, kind, nodeByIdentifier, directlyLoaded) {
  const row = element("div", "memory-connection");
  const target = nodeByIdentifier.get(connection.identifier);
  row.append(
    badge(String(connection.identifier), "identifier"),
    element("span", "memory-connection-name", connection.shortName),
    badge(kind === "fanout" ? "summary only" : target ? "full context" : "summary only", kind === "fanout" || !target ? "summary" : "expanded"),
  );
  if (kind === "fixed") row.append(badge(`slot ${connection.slot}`, "task"));
  if (directlyLoaded.has(connection.identifier)) row.append(badge("also directly loaded", "direct"));
  if (connection.shortDescription) row.append(element("span", "memory-connection-description", connection.shortDescription));
  return row;
}

function connectionGroup(title, connections, kind, nodeByIdentifier, directlyLoaded, path, depth, openKeys = new Set(), keyPrefix = "memory") {
  const group = element("div", `memory-branch ${kind}`);
  const heading = element("div", "memory-branch-title");
  heading.append(element("span", "memory-branch-line"), element("strong", "", title), badge(String(connections.length), "count"));
  group.append(heading);
  if (!connections.length) {
    group.append(element("p", "memory-none", "None"));
    return group;
  }
  for (const connection of connections) {
    const target = nodeByIdentifier.get(connection.identifier);
    const canExpand = kind === "active" && target && !path.has(connection.identifier) && depth < 1;
    if (canExpand) {
      group.append(memoryNode(target, "expanded", nodeByIdentifier, directlyLoaded, new Set([...path, connection.identifier]), depth + 1, openKeys, `${keyPrefix}:active:${connection.identifier}`));
    } else {
      group.append(connectionLeaf(connection, kind, nodeByIdentifier, directlyLoaded));
    }
  }
  return group;
}

function memoryNode(node, relation, nodeByIdentifier, directlyLoaded, path, depth = 0, openKeys = new Set(), key = `memory-node:${node.identifier}:${depth}`) {
  const details = element("details", `memory-node ${relation}`);
  details.dataset.mainKey = key;
  details.open = openKeys.has(key);
  const summary = element("summary", "memory-node-summary");
  summary.dataset.focusKey = `${key}:summary`;
  const sourceLabel = relation === "direct"
    ? "directly loaded"
    : node.contextSources?.includes("active") ? "pulled by active connection" : "full context";
  summary.append(
    badge(String(node.identifier), "identifier"),
    element("strong", "memory-node-name", node.shortName),
    badge(sourceLabel, relation),
    badge(characterLabel(nodeContentCharacters(node)), "summary"),
  );
  if (relation !== "direct" && directlyLoaded.has(node.identifier)) summary.append(badge("also directly loaded", "direct"));
  details.append(summary);
  const body = element("div", "memory-node-body");
  if (node.shortDescription) body.append(element("p", "memory-node-short", node.shortDescription));
  body.append(element("p", "memory-node-attribution", `Last modified by: ${node.lastModifiedBy || "legacy-unknown"}`));
  body.append(element("p", "memory-node-attribution", `Owner: ${Number.isInteger(node.ownerIdentifier) ? `Node ${node.ownerIdentifier}` : "unowned"}`));
  body.append(element("p", "memory-node-long", node.longDescription || "No detailed description."));
  body.append(
    connectionGroup("Fixed connections", node.fixedConnections || [], "fixed", nodeByIdentifier, directlyLoaded, path, depth, openKeys, key),
    connectionGroup("Active connections", node.activeConnections || [], "active", nodeByIdentifier, directlyLoaded, path, depth, openKeys, key),
    connectionGroup("Fanout references", node.fanoutConnections || [], "fanout", nodeByIdentifier, directlyLoaded, path, depth, openKeys, key),
  );
  details.append(body);
  return details;
}

function keyedDetails(className, key, openKeys) {
  const details = element("details", className);
  details.dataset.mainKey = key;
  details.open = openKeys.has(key);
  return details;
}

function nodeContentCharacters(node) {
  return characterCount([
    node?.shortName,
    node?.shortDescription,
    node?.longDescription,
    ...(node?.fixedConnections || []).flatMap(connection => [connection.shortName, connection.shortDescription]),
    ...(node?.activeConnections || []).flatMap(connection => [connection.shortName, connection.shortDescription]),
    ...(node?.fanoutConnections || []).flatMap(connection => [connection.shortName, connection.shortDescription]),
  ].filter(Boolean).join("\n"));
}

function disclosureSummary(label, kind = "context", characters = null) {
  const summary = element("summary", "main-entry-summary");
  summary.append(badge(kind, kind), element("strong", "main-entry-label", label));
  if (characters !== null) summary.append(badge(characterLabel(characters), "summary"));
  summary.append(element("span", "main-entry-toggle"));
  return summary;
}

function mainMemorySet(entry, openKeys) {
  const memory = entry.memory || { directlyLoadedIdentifiers: [], nodes: [] };
  const directlyLoaded = new Set(memory.directlyLoadedIdentifiers || []);
  const nodeByIdentifier = new Map((memory.nodes || []).map(node => [node.identifier, node]));
  const activeExpanded = [...nodeByIdentifier.values()].filter(node => !directlyLoaded.has(node.identifier) && node.contextSources?.includes("active")).length;
  const details = keyedDetails("main-entry main-memory-set", "memory-set", openKeys);
  const summary = element("summary", "main-entry-summary");
  summary.dataset.focusKey = "memory-set:summary";
  summary.append(
    badge("memory", "memory"),
    element("strong", "main-entry-label", "Loaded nodes"),
    badge(`${directlyLoaded.size} direct`, "direct"),
    badge(`${activeExpanded} active`, "expanded"),
    badge(characterLabel([...nodeByIdentifier.values()].reduce((total, node) => total + nodeContentCharacters(node), 0)), "summary"),
    element("span", "main-entry-toggle"),
  );
  details.append(summary);
  const body = element("div", "main-memory-body");
  const directNodes = [...directlyLoaded].map(identifier => nodeByIdentifier.get(identifier)).filter(Boolean);
  if (!directNodes.length) {
    body.append(element("p", "memory-tree-empty", "No memory nodes are currently loaded."));
  } else {
    for (const node of directNodes) body.append(memoryNode(node, "direct", nodeByIdentifier, directlyLoaded, new Set([node.identifier]), 0, openKeys, `memory-set:node:${node.identifier}`));
  }
  const other = [...nodeByIdentifier.values()].filter(node =>
    !directlyLoaded.has(node.identifier) && !node.contextSources?.includes("active")
  );
  if (other.length) {
    const section = element("section", "memory-other");
    section.append(element("h3", "", "Other full-context nodes"));
    for (const node of other) section.append(memoryNode(node, "expanded", nodeByIdentifier, directlyLoaded, new Set([node.identifier]), 1, openKeys, `memory-set:other:${node.identifier}`));
    body.append(section);
  }
  details.append(body);
  return details;
}

function mainCollapsedEntry(entry, openKeys, index) {
  const key = entry.key || `${entry.kind}:${index}`;
  const details = keyedDetails(`main-entry main-${entry.kind}`, key, openKeys);
  const summary = disclosureSummary(
    entry.label,
    entry.kind === "tool-call" ? "call" : entry.kind === "tool-result" ? "result" : "context",
    characterCount(entry.content),
  );
  summary.dataset.focusKey = `${key}:summary`;
  details.append(summary);
  const body = element("pre", "main-entry-body", entry.content);
  body.dataset.scrollKey = `${key}:body`;
  details.append(body);
  if (entry.timing?.length) details.append(element("p", "main-entry-timing", entry.timing.join(" · ")));
  return details;
}

function mainLoadedNode(entry, memory, openKeys, index) {
  const directlyLoaded = new Set(memory?.directlyLoadedIdentifiers || []);
  const nodes = [...(memory?.nodes || [])];
  if (!nodes.some(node => node.identifier === entry.node.identifier)) nodes.push(entry.node);
  const nodeByIdentifier = new Map(nodes.map(node => [node.identifier, node]));
  const relation = entry.relation === "active" ? "expanded" : "direct";
  const key = entry.key || `loaded-node:${index}`;
  const details = memoryNode(entry.node, relation, nodeByIdentifier, directlyLoaded, new Set([entry.node.identifier]), entry.relation === "active" ? 1 : 0, openKeys, key);
  details.classList.add("main-entry", "main-loaded-node");
  if (entry.timing?.length) details.querySelector(".memory-node-body")?.append(element("p", "main-entry-timing", entry.timing.join(" · ")));
  return details;
}

function mainConversationEntry(entry, openKeys, index) {
  const article = element("article", `main-conversation-message ${entry.role === "assistant" ? "assistant" : "user"}`);
  const heading = element("div", "main-conversation-heading");
  heading.append(element("span", "role", entry.label));
  if (entry.timing?.length) heading.append(element("span", "main-conversation-timing", entry.timing.join(" · ")));
  article.append(heading);
  const body = element("div", "body");
  appendLinkedText(body, entry.content);
  if (entry.preview === null || entry.preview === undefined) {
    article.append(body);
    return article;
  }
  const key = entry.key || `conversation:${index}`;
  const disclosure = keyedDetails("main-response-disclosure", key, openKeys);
  const summary = element("summary", "main-response-summary");
  summary.dataset.focusKey = `${key}:summary`;
  summary.append(
    element("span", "main-response-preview", entry.preview),
    element("span", "main-response-expand", ` [Show ${characterLabel(entry.hiddenCharacters)} more]`),
    element("span", "main-response-collapse", "Show less"),
  );
  disclosure.append(summary, body);
  article.append(disclosure);
  return article;
}

function renderMainView(container, diagnostic) {
  const openKeys = new Set([...container.querySelectorAll("details[open][data-main-key]")].map(details => details.dataset.mainKey));
  container.replaceChildren();
  const stream = element("div", "main-inspector-stream");
  const entries = mainViewEntries(diagnostic);
  for (const [index, entry] of entries.entries()) {
    if (entry.kind === "memory") stream.append(mainMemorySet(entry, openKeys));
    else if (entry.kind === "conversation") stream.append(mainConversationEntry(entry, openKeys, index));
    else if (entry.kind === "loaded-node") stream.append(mainLoadedNode(entry, diagnostic?.memory, openKeys, index));
    else stream.append(mainCollapsedEntry(entry, openKeys, index));
  }
  container.append(stream);
}

function fullHistoryPhases(diagnostic) {
  const phases = diagnostic?.fullHistory?.phases;
  if (Array.isArray(phases) && phases.length) return phases;
  return [{
    label: diagnostic?.mode === "history ingress" ? "History ingress" : "Conversation",
    status: diagnostic?.ingressStatus || "current",
    segments: diagnostic?.historySegments || [],
    current: { messages: diagnostic?.chatend || [], memory: diagnostic?.memory || null, usage: diagnostic?.usage || null },
  }];
}

function historyContext(segment) {
  return {
    chatend: segment?.messages || [],
    memory: segment?.memory || { directlyLoadedIdentifiers: [], nodes: [] },
    usage: segment?.usage || null,
  };
}

function historyBarrier(label, kind = "reset") {
  const barrier = element("div", `full-history-barrier ${kind}`);
  barrier.setAttribute("role", "separator");
  barrier.append(element("span", "full-history-line"), element("strong", "", label), element("span", "full-history-line"));
  return barrier;
}

function renderHistoryContext(segment, namespace, openKeys) {
  const holder = element("section", "full-history-context");
  holder.append(element("span", "full-history-context-label", namespace.split(":").at(-1)));
  const content = element("div", "full-history-main");
  renderMainView(content, historyContext(segment));
  for (const details of content.querySelectorAll("details[data-main-key]")) {
    const previousKey = details.dataset.mainKey;
    const key = `${namespace}:${details.dataset.mainKey}`;
    details.dataset.mainKey = key;
    details.open = openKeys.has(key);
    const summary = details.querySelector(":scope > summary");
    if (summary) summary.dataset.focusKey = `${key}:summary`;
    for (const scroller of details.querySelectorAll(`[data-scroll-key^="${previousKey}:"]`)) {
      scroller.dataset.scrollKey = `${namespace}:${scroller.dataset.scrollKey}`;
    }
  }
  holder.append(content);
  return holder;
}

function renderFullHistory(container, diagnostic) {
  const openKeys = new Set([...container.querySelectorAll("details[open][data-main-key]")].map(details => details.dataset.mainKey));
  container.replaceChildren();
  const history = element("div", "full-history");
  const phases = fullHistoryPhases(diagnostic);
  for (const [phaseIndex, phase] of phases.entries()) {
    if (phaseIndex) history.append(historyBarrier(`${phase.label} began`, "phase"));
    const heading = element("header", "full-history-phase-heading");
    heading.append(element("strong", "", phase.label), badge(phase.status || "saved", phase.status === "failed" ? "summary" : "expanded"));
    history.append(heading);
    const contexts = [...(phase.segments || []), ...(phase.current ? [{ ...phase.current, reason: null }] : [])];
    if (!contexts.length) {
      history.append(element("p", "full-history-empty", `${phase.label} is ${phase.status || "not started"}. No Chatend has been checkpointed yet.`));
      continue;
    }
    for (const [contextIndex, context] of contexts.entries()) {
      history.append(renderHistoryContext(context, `history:${phaseIndex}:Context ${contextIndex + 1}`, openKeys));
      if (contextIndex < contexts.length - 1) {
        history.append(historyBarrier(`${context.reason || "ResetContext"} · context reset`));
      }
    }
  }
  container.append(history);
}

function fullHistoryText(diagnostic) {
  const output = [];
  const phases = fullHistoryPhases(diagnostic);
  for (const [phaseIndex, phase] of phases.entries()) {
    if (phaseIndex) output.push(`════════ ${phase.label} began ════════`);
    output.push(`${phase.label} · ${phase.status || "saved"}`);
    const contexts = [...(phase.segments || []), ...(phase.current ? [{ ...phase.current, reason: null }] : [])];
    if (!contexts.length) {
      output.push(`No ${phase.label.toLowerCase()} Chatend has been checkpointed yet.`);
      continue;
    }
    for (const [contextIndex, context] of contexts.entries()) {
      output.push(`Context ${contextIndex + 1}`, formatChatend(context.messages || [], context.usage || null));
      if (contextIndex < contexts.length - 1) output.push(`════════ ${context.reason || "ResetContext"} · context reset ════════`);
    }
  }
  return output.join("\n\n");
}

function renderMemoryTree(container, snapshot) {
  const memory = snapshot || { directlyLoadedIdentifiers: [], nodes: [] };
  const directlyLoaded = new Set(memory.directlyLoadedIdentifiers || []);
  const nodeByIdentifier = new Map((memory.nodes || []).map(node => [node.identifier, node]));
  const activeExpanded = [...nodeByIdentifier.values()].filter(node => !directlyLoaded.has(node.identifier) && node.contextSources?.includes("active")).length;
  const intro = element("div", "memory-tree-intro");
  intro.append(
    element("div", "", "This is the Kmap material currently visible to Kennedy."),
    badge(`${directlyLoaded.size} directly loaded`, "direct"),
    badge(`${activeExpanded} pulled through active connections`, "expanded"),
    badge(`${Math.max(0, nodeByIdentifier.size - directlyLoaded.size - activeExpanded)} other full-context`, "summary"),
  );
  container.append(intro);
  if (!nodeByIdentifier.size) {
    container.append(element("p", "memory-tree-empty", "No memory nodes are currently in context."));
    return;
  }
  const roots = [...directlyLoaded].map(identifier => nodeByIdentifier.get(identifier)).filter(Boolean);
  for (const root of roots) container.append(memoryNode(root, "direct", nodeByIdentifier, directlyLoaded, new Set([root.identifier])));
  const other = [...nodeByIdentifier.values()].filter(node =>
    !directlyLoaded.has(node.identifier) && !node.contextSources?.includes("active")
  );
  if (other.length) {
    const section = element("section", "memory-other");
    section.append(element("h3", "", "Other full-context nodes"));
    for (const node of other) section.append(memoryNode(node, "expanded", nodeByIdentifier, directlyLoaded, new Set([node.identifier])));
    container.append(section);
  }
}

export function renderIngressActivity(
  container,
  diagnostic,
  active,
  failed = false,
  failures = [],
  { namespace = "history-ingress", sourceLabel = null } = {},
) {
  const continuation = element("section", "ingress-continuation");
  continuation.setAttribute("aria-label", failed ? "Failed history ingress" : active ? "History ingress in progress" : "Completed history ingress");
  const heading = element("div", "ingress-heading");
  const headingText = element("div");
  headingText.append(
    element("span", "eyebrow", "MEMORY UPDATE"),
    element("strong", "", [
      sourceLabel,
      failed ? "History ingress · failed" : active ? "History ingress · live" : "History ingress · complete",
    ].filter(Boolean).join(" · ")),
  );
  heading.append(headingText);
  continuation.append(heading);
  if (failures.length) {
    const failureSection = element("section", "ingress-failures");
    failureSection.append(
      element("span", "eyebrow", failed ? "FAILURE LOG" : "RETRY LOG"),
      element("strong", "", failed
        ? `Stopped after ${failures.length} failed attempt${failures.length === 1 ? "" : "s"}`
        : `${failures.length} failed attempt${failures.length === 1 ? "" : "s"} recorded`),
    );
    for (const failure of failures) {
      const context = Number.isFinite(failure?.context_tokens) && Number.isFinite(failure?.context_window_tokens)
        ? ` · context ${exactTokenCount(failure.context_tokens)}/${exactTokenCount(failure.context_window_tokens)}`
        : "";
      const rounds = Number.isFinite(failure?.rounds_used) ? ` · round ${failure.rounds_used}` : "";
      failureSection.append(element(
        "pre",
        "ingress-failure-entry",
        `Attempt ${failure?.attempt || "?"} · ${failure?.stage || "unknown"}${rounds}${context}\n${failure?.code || "ingress_error"}: ${failure?.message || "No error detail was recorded."}\n${failure?.occurred_at || "Time unavailable"}`,
      ));
    }
    continuation.append(failureSection);
  }
  const summary = ingressMutationSummary(diagnostic);
  const review = element("section", "ingress-summary");
  review.setAttribute("aria-label", "History ingress memory changes");
  review.append(element("span", "eyebrow", "MEMORY CHANGES"));
  const counts = element("div", "ingress-summary-counts");
  for (const [value, label] of [
    [summary.nodesAdded, "Nodes added"],
    [summary.nodesUpdated, "Nodes updated"],
    [summary.connectCalls, "ConnectNodes calls"],
  ]) {
    const item = element("div", "ingress-summary-item");
    item.append(element("strong", "", String(value)), element("span", "", label));
    counts.append(item);
  }
  review.append(counts);
  continuation.append(review);
  const usage = diagnostic?.usage?.snapshot?.();
  if (usage?.requests) {
    continuation.append(element(
      "p",
      "ingress-usage",
      `${usage.requests} request${usage.requests === 1 ? "" : "s"} · ${tokenCount(usage.totalInputTokens)} input · ${tokenCount(usage.totalCachedTokens)} cache reads · ${tokenCount(usage.totalCacheWriteTokens)} cache writes`,
    ));
  }
  const visible = (diagnostic?.chatend?.messages || []).filter(message =>
    message.role === "assistant" || message.display_role === "Memory tool result" || message.display_role === "Tool protocol error"
  );
  if (!visible.length) {
    continuation.append(element("p", "ingress-empty", active ? "Kennedy is preparing the history-ingress context…" : "No ingress activity was recorded."));
    container.append(continuation);
    return;
  }
  for (const [messageIndex, message] of visible.entries()) {
    const presentation = ingressEntryPresentation(message);
    const item = element(presentation.collapsed ? "details" : "article", `ingress-entry${presentation.collapsed ? " ingress-entry-collapsible" : ""}`);
    if (presentation.collapsed) {
      const key = `${namespace}:entry:${messageIndex}`;
      item.dataset.viewKey = key;
      const summary = element("summary", "ingress-entry-summary");
      summary.dataset.focusKey = `${key}:summary`;
      summary.append(
        element("span", "role", presentation.label),
        element("span", "ingress-entry-size", characterLabel(message.content)),
        element("span", "ingress-entry-toggle"),
      );
      item.append(summary);
    } else {
      item.append(element("span", "role", presentation.label));
    }
    item.append(element("pre", "ingress-body", message.content));
    continuation.append(item);
  }
  container.append(continuation);
}

export function showError(log, message) {
  if (!log) return;
  const previousTop = log.scrollTop;
  const wasAtBottom = log.scrollHeight - log.clientHeight - log.scrollTop <= 1;
  const text = String(message || "An unknown Kennedy error occurred.").trim();
  const previous = log.lastElementChild;
  if (previous?.dataset.message === text) {
    const count = Number(previous.dataset.count || 1) + 1;
    previous.dataset.count = String(count);
    previous.querySelector(".user-log-message").textContent = `${text} · repeated ${count} times`;
  } else {
    const entry = element("article", "user-log-entry");
    entry.dataset.message = text;
    entry.dataset.count = "1";
    entry.append(
      element("time", "", new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit", second: "2-digit" }).format(new Date())),
      element("div", "user-log-message", text),
    );
    log.append(entry);
  }
  log.parentElement?.classList.remove("hidden");
  log.scrollTop = wasAtBottom ? log.scrollHeight : previousTop;
}

export function clearError(log) {
  if (!log) return;
  log.replaceChildren();
  log.parentElement?.classList.add("hidden");
}

export { element };
