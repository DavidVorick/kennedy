// Pure UI projections over the canonical append-ordered session log.

const EMPTY_MEMORY = { directlyLoadedIdentifiers: [], nodes: [] };

const MEMORY_TOOLS = new Set([
  "LoadNode",
  "ConnectNodes",
  "ConsolidateFanout",
  "SetFixedConnection",
  "CreateNode",
  "UpdateNode",
]);

const CONTROL_TOOLS = new Set([
  "EndSession",
  "DehydrateBox",
  "SummarizeBox",
  "HydrateBox",
  "HydrateEvent",
  "DehydrateEvent",
]);

const WEB_TOOLS = new Set(["WebSearch", "WebFetch"]);

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function numericValue(value) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, number) : null;
}

function firstDefined(source, ...keys) {
  for (const key of keys) {
    if (source?.[key] !== undefined && source[key] !== null) return source[key];
  }
  return null;
}

function pretty(value) {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch {
    return String(value ?? "");
  }
}

export function isSessionLogArchive(value) {
  return typeof value?.header?.formatVersion === "string"
    && typeof value?.header?.sessionId === "string"
    && Array.isArray(value?.events);
}

function isSessionEventArray(events) {
  return Array.isArray(events) && events.every(event =>
    typeof event?.role === "string" && typeof event?.text === "string"
  );
}

export function sessionLogForRecord(record) {
  const state = objectValue(record?.state);
  if (isSessionLogArchive(state.archive)) return state.archive;
  if (!isSessionEventArray(state.events)) return null;
  return {
    header: {
      formatVersion: "materialized",
      sessionId: String(state.sessionId || record?.id || ""),
      createdAt: String(state.startedAt || record?.started_at || ""),
    },
    events: state.events,
  };
}

export function decodeSessionEvent(event, position = 0) {
  let decoded = null;
  if (typeof event?.text === "string") {
    try {
      const value = JSON.parse(event.text);
      if (value && typeof value === "object" && !Array.isArray(value)) decoded = value;
    } catch {
      // Older and externally-authored role/text events may intentionally be plain text.
    }
  }
  const kind = objectValue(decoded?.kind);
  return {
    position,
    event,
    decoded,
    kind,
    type: typeof kind.type === "string" ? kind.type : null,
  };
}

function displayText(entry) {
  if (entry.type === "box_created") {
    return typeof entry.kind?.content?.text === "string" ? entry.kind.content.text : "";
  }
  return typeof entry.event?.text === "string" ? entry.event.text : "";
}

function eventRole(role) {
  if (role === "user-message") return "user";
  if (role === "kennedy-message" || role === "kennedy-tool-call") return "assistant";
  return "system";
}

function isToolPresentationBox(entry) {
  if (entry.type !== "box_created") return false;
  const name = String(entry.kind?.name || "");
  return name.startsWith("Kennedy tool call:")
    || name === "Kennedy tool result";
}

function transcriptEntry(entry) {
  const role = entry.event?.role;
  const box = entry.type === "box_created";
  const content = objectValue(entry.kind?.content);
  const metadata = objectValue(content.metadata);
  if (box && metadata.boxKind === "attachmentText") return null;
  if (box && isToolPresentationBox(entry)) return null;
  if (entry.type && !box) return null;

  let transcriptRole = null;
  if (role === "user-message") transcriptRole = "user";
  else if (role === "kennedy-message") transcriptRole = "kennedy";
  else if (role === "system-error") transcriptRole = "system";
  else if (role === "system-message" && metadata.transcriptRole === "system") {
    transcriptRole = "system";
  }
  if (!transcriptRole) return null;

  const item = {
    role: transcriptRole,
    content: box ? displayText(entry) : String(entry.event?.text || ""),
    boxId: entry.position + 1,
  };
  if (box && Array.isArray(content.objects) && content.objects.length) {
    item.objects = content.objects;
  }
  for (const key of ["inputKind", "attachments", "externalEventId"]) {
    if (metadata[key] !== undefined) item[key] = metadata[key];
  }
  if (!item.attachments && metadata.media && typeof metadata.media === "object") {
    item.attachments = [metadata.media];
  }
  return item;
}

function boxMessage(entry) {
  if (isToolPresentationBox(entry)) return null;
  const owner = objectValue(entry.kind?.owner);
  const name = String(entry.kind?.name || `Event ${entry.position + 1}`);
  const role = eventRole(entry.event?.role);
  const ordinaryConversation =
    (owner.kind === "user" && name === "User message")
    || (owner.kind === "kennedy" && name === "Kennedy message");
  return {
    role,
    content: displayText(entry),
    display_role: name,
    context_kind: ordinaryConversation ? null
      : owner.kind === "system" ? "instructions"
      : owner.kind === "tool" && owner.tool_instance === "kweb" ? "kweb-box"
      : "box",
  };
}

function genericEventMessage(entry) {
  if (entry.type === "history_ingress_started") return null;
  if (entry.type === "note" && entry.kind?.label === "provider_input") return null;
  if (!entry.type) {
    return {
      role: eventRole(entry.event?.role),
      content: String(entry.event?.text || ""),
      display_role: `Event ${entry.position + 1} · ${entry.event?.role || "unknown"}`,
      context_kind: entry.event?.role === "user-message"
        || entry.event?.role === "kennedy-message" ? null : "box",
    };
  }
  return {
    role: "system",
    content: pretty(entry.kind),
    display_role: `Event ${entry.position + 1} · ${entry.type.replaceAll("_", " ")}`,
    context_kind: "box",
  };
}

function toolResultLabel(name, protocolError) {
  if (protocolError) return "Tool protocol error";
  if (MEMORY_TOOLS.has(name)) return "Memory tool result";
  if (WEB_TOOLS.has(name)) return "Web tool result";
  if (CONTROL_TOOLS.has(name)) return "Control tool result";
  return "Coding tool result";
}

function semanticMessages(entries) {
  const messages = [];
  const toolLog = [];
  const pendingTools = [];
  for (const entry of entries) {
    if (entry.type === "tool_invoked") {
      const name = String(firstDefined(entry.kind, "tool_name", "toolName") || "unknown");
      const argumentsValue = objectValue(entry.kind.arguments);
      pendingTools.push({ name, arguments: argumentsValue });
      messages.push({
        role: "assistant",
        content: pretty(argumentsValue),
        display_role: `Tool call · ${name}`,
        context_kind: "tool-call",
        tool_name: name,
        tool_arguments: argumentsValue,
      });
      continue;
    }
    if (entry.type === "tool_completed") {
      const paired = pendingTools.shift() || null;
      const storedName = String(firstDefined(entry.kind, "tool_name", "toolName") || "");
      const protocolError = !paired && (!storedName || storedName === "call_ktool");
      const name = paired?.name || storedName || "unknown";
      const outcome = objectValue(entry.kind.outcome);
      const ok = outcome.ok === true;
      const result = outcome.result ?? outcome;
      messages.push({
        role: "user",
        content: pretty(result),
        display_role: toolResultLabel(name, protocolError),
        context_kind: "tool-result",
        tool_name: name,
        tool_arguments: paired?.arguments || {},
        tool_result: outcome,
        tool_call_count: 1,
      });
      toolLog.push({ name, ok, result });
      continue;
    }
    const message = entry.type === "box_created"
      ? boxMessage(entry)
      : genericEventMessage(entry);
    if (message) messages.push(message);
  }
  return { messages, toolLog };
}

function configuredContextWindow(entries) {
  let contextWindowTokens = 0;
  for (const entry of entries) {
    if (entry.type !== "session_configured") continue;
    contextWindowTokens = numericValue(firstDefined(
      entry.kind,
      "effective_context_tokens",
      "effectiveContextTokens",
    )) || contextWindowTokens;
  }
  return contextWindowTokens;
}

function usageFromEntries(entries, fallbackContextWindowTokens = 0) {
  let contextWindowTokens = fallbackContextWindowTokens;
  const receipts = [];
  for (const entry of entries) {
    if (entry.type === "session_configured") {
      contextWindowTokens = numericValue(firstDefined(
        entry.kind,
        "effective_context_tokens",
        "effectiveContextTokens",
      )) || contextWindowTokens;
    }
    if (entry.type !== "provider_receipt") continue;
    const providerData = objectValue(firstDefined(entry.kind, "provider_data", "providerData"));
    receipts.push({
      inputTokens: numericValue(firstDefined(entry.kind, "input_tokens", "inputTokens")),
      outputTokens: numericValue(firstDefined(entry.kind, "output_tokens", "outputTokens")),
      cachedTokens: numericValue(firstDefined(
        providerData,
        "cachedInputTokens",
        "cached_input_tokens",
      )) || 0,
      cacheWriteTokens: numericValue(firstDefined(
        providerData,
        "cacheWriteTokens",
        "cache_write_tokens",
      )) || 0,
    });
  }
  if (!receipts.length) return null;
  const totalInputTokens = receipts.reduce((total, receipt) => total + (receipt.inputTokens || 0), 0);
  const totalOutputTokens = receipts.reduce((total, receipt) => total + (receipt.outputTokens || 0), 0);
  const totalCachedTokens = receipts.reduce((total, receipt) => total + receipt.cachedTokens, 0);
  const totalCacheWriteTokens = receipts.reduce((total, receipt) => total + receipt.cacheWriteTokens, 0);
  const latest = receipts.at(-1);
  const contextKnown = latest.inputTokens !== null && latest.outputTokens !== null;
  const contextTokens = contextKnown ? latest.inputTokens + latest.outputTokens : 0;
  return {
    requests: receipts.length,
    totalInputTokens,
    totalOutputTokens,
    totalCachedTokens,
    totalCacheWriteTokens,
    cacheReadPercent: totalInputTokens ? 100 * totalCachedTokens / totalInputTokens : 0,
    contextKnown,
    contextTokens,
    contextWindowTokens,
    lastContext: contextKnown
      ? { inputTokens: latest.inputTokens, outputTokens: latest.outputTokens }
      : null,
  };
}

function latestProviderInput(entries) {
  return entries.reduce((latest, entry) =>
    entry.type === "note"
      && entry.kind?.label === "provider_input"
      && typeof entry.kind?.value === "string"
      ? entry.kind.value
      : latest
  , null);
}

function activeBoxCount(entries) {
  const active = new Set();
  for (const entry of entries) {
    const id = firstDefined(entry.kind, "box_id", "boxId");
    if (entry.type === "box_created" && id !== null) active.add(String(id));
    if (entry.type === "box_retired" && id !== null) active.delete(String(id));
  }
  return active.size;
}

function diagnosticFromEntries(
  entries,
  mode,
  { provider = null, model = null, contextWindowTokens = 0 } = {},
) {
  const { messages, toolLog } = semanticMessages(entries);
  const exactInput = latestProviderInput(entries);
  return {
    mode,
    provider,
    model,
    chatend: messages,
    chatendText: exactInput || messages
      .map(message => `[${message.display_role || message.role}]\n${message.content}`)
      .join("\n\n"),
    context: {
      boxCount: activeBoxCount(entries),
      eventCount: entries.length,
      staleBoxes: [],
    },
    loadCalls: toolLog.filter(entry => entry.name === "LoadNode").length,
    loadLimit: 0,
    toolLog,
    usage: usageFromEntries(entries, contextWindowTokens),
    memory: EMPTY_MEMORY,
    historySegments: [],
    events: entries.map(entry => entry.event),
    boxes: [],
  };
}

function phaseBoundaries(entries) {
  const historyStart = entries.findIndex(entry => entry.type === "history_ingress_started");
  if (historyStart < 0) {
    return { sourceEnd: entries.length, transitionStart: -1, historyStart: -1 };
  }
  let transitionStart = historyStart;
  for (let index = historyStart - 1; index >= 0; index -= 1) {
    if (entries[index].type === "source_terminated") {
      transitionStart = index;
      break;
    }
  }
  return { sourceEnd: transitionStart, transitionStart, historyStart };
}

export function projectSessionLog(
  sessionLog,
  { provider = null, model = null } = {},
) {
  if (!sessionLog || !isSessionEventArray(sessionLog.events)) return null;
  const entries = sessionLog.events.map(decodeSessionEvent);
  const boundaries = phaseBoundaries(entries);
  const contextWindowTokens = configuredContextWindow(entries);
  const sourceEntries = entries.slice(0, boundaries.sourceEnd);
  const transitionEntries = boundaries.historyStart >= 0
    ? entries.slice(boundaries.transitionStart, boundaries.historyStart + 1)
    : [];
  const ingressEntries = boundaries.historyStart >= 0
    ? entries.slice(boundaries.historyStart + 1)
    : [];
  const transcript = sourceEntries.map(transcriptEntry).filter(Boolean);
  const conversationDiagnostic = diagnosticFromEntries(
    sourceEntries,
    "session log",
    { provider, model, contextWindowTokens },
  );
  let ingressDiagnostic = null;
  if (boundaries.historyStart >= 0) {
    ingressDiagnostic = diagnosticFromEntries(
      ingressEntries,
      "history ingress",
      { provider, model, contextWindowTokens },
    );
    if (transitionEntries.length) {
      const transition = diagnosticFromEntries(
        transitionEntries,
        "history ingress preparation",
        { provider, model, contextWindowTokens },
      );
      ingressDiagnostic.historySegments = [{
        reason: "History ingress started",
        messages: transition.chatend,
        chatendText: transition.chatendText,
        memory: transition.memory,
        usage: transition.usage,
      }];
    }
  }
  const firstUserMessage = transcript.find(item =>
    item.role === "user" && typeof item.content === "string"
  )?.content || null;
  return {
    sessionLog,
    transcript,
    firstUserMessage,
    conversationDiagnostic,
    ingressDiagnostic,
    historyIngressStarted: boundaries.historyStart >= 0,
    boundaries,
  };
}

export function projectSessionRecord(record, options = {}) {
  const sessionLog = sessionLogForRecord(record);
  return sessionLog ? projectSessionLog(sessionLog, options) : null;
}

export function firstSourceUserMessage(record) {
  const sessionLog = sessionLogForRecord(record);
  if (!sessionLog) return null;
  for (const [position, event] of sessionLog.events.entries()) {
    const entry = decodeSessionEvent(event, position);
    if (entry.type === "history_ingress_started") break;
    const transcript = transcriptEntry(entry);
    if (transcript?.role === "user" && typeof transcript.content === "string") {
      return transcript.content;
    }
  }
  return null;
}
