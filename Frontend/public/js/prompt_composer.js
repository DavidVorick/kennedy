export const PROMPT_FILES = {
  identity: "KennedyIdentity.txt",
  conversationSession: "ConversationSession.txt",
  historyIngressSession: "HistoryIngressSession.txt",
  audioIngressSession: "AudioIngressSession.txt",
  codexHarness: "CodexHarness.txt",
  kmapBasics: "KmapBasics.txt",
  readTools: "ReadTools.txt",
  writeTools: "WriteTools.txt",
};

export function requiredPromptKeys(mode, { sourceSessionType = "conversation", providerKind = null } = {}) {
  const sessionKey = mode === "conversation"
    ? "conversationSession"
    : sourceSessionType === "audio" ? "audioIngressSession" : "historyIngressSession";
  const keys = [
    "identity",
    sessionKey,
    "kmapBasics",
    "readTools",
    ...(mode === "conversation" ? [] : ["writeTools"]),
  ];
  if (providerKind === "codex") keys.push("codexHarness");
  return keys;
}

export function promptsReady(manuals, mode, options = {}) {
  return requiredPromptKeys(mode, options).every(key => typeof manuals?.[key] === "string" && manuals[key].trim());
}

export async function loadPromptManuals(base = "") {
  const results = await Promise.all(Object.entries(PROMPT_FILES).map(async ([key, file]) => {
    try {
      const response = await fetch(`${base}/system-prompts/${file}`, { cache: "no-store" });
      if (!response.ok) throw new Error(`Could not load system prompt ${file}.`);
      return { key, text: (await response.text()).trim() };
    } catch (error) {
      return { key, error: error?.message || `Could not load system prompt ${file}.` };
    }
  }));
  return {
    manuals: Object.fromEntries(results.filter(result => result.text !== undefined).map(result => [result.key, result.text])),
    errors: Object.fromEntries(results.filter(result => result.error).map(result => [result.key, result.error])),
  };
}

function runtimeValue(value, fallback) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

export function formatModelAttribution(model, reasoningEffort) {
  return `${runtimeValue(model, "unknown-model")}-${runtimeValue(reasoningEffort, "unknown-thinking")}`;
}

function sessionDetail(mode, sessionType, sourceSessionType) {
  if (mode === "conversation") {
    if (sessionType === "telegram-group") {
      return "Channel: Telegram group. This is a persistent session scoped to one participant and one group. Only that participant's invocations accumulate in this session; other participants have separate sessions. Other participant roots are references that you may load if useful.";
    }
    if (sessionType === "telegram") {
      return "Channel: Telegram private message. The final conversational response is relayed to the user; the visible Chatend and tool loop still run in Kennedy's browser UI.";
    }
    return "Channel: Kennedy's browser UI.";
  }
  if (sourceSessionType === "audio") return "Source: one chronologically placed piece of a vnote transcript.";
  if (sourceSessionType === "telegram-group") return "Source: an archived Telegram group invocation or background group-chat batch.";
  if (sourceSessionType === "telegram") return "Source: an archived Telegram conversation (private message).";
  return "Source: an archived browser conversation.";
}

function participantName(participant) {
  const handle = typeof participant?.username === "string" && participant.username
    ? `@${participant.username.replace(/^@/, "")}`
    : null;
  return [participant?.displayName || handle || "Unknown participant", handle].filter((value, index, values) => value && values.indexOf(value) === index).join(" · ");
}

export function formatTelegramGroupContext(groupContext, context) {
  if (!groupContext || !Array.isArray(groupContext.participants) || !Array.isArray(groupContext.messages)) return "";
  const invokerId = groupContext.invokingTelegramUserId;
  const groupRootId = groupContext.groupRootNodeId;
  const groupRootIdentifier = typeof groupRootId === "string" && groupRootId
    ? context.registerReference(groupRootId)
    : null;
  const invoker = groupContext.participants.find(participant => String(participant.telegramUserId) === String(invokerId));
  const invokerRootIdentifier = invoker?.rootNodeId ? context.registerReference(invoker.rootNodeId) : null;
  const rootIdentifiers = context.rootNodeIds.map(rootNodeId => context.shortId(rootNodeId));
  const kennedyRootIdentifier = rootIdentifiers.at(-1);
  const additionalCapacity = Math.max(0, 10 - context.rootNodeIds.length);
  const participants = groupContext.participants.map(participant => {
    const identifier = context.registerReference(participant.rootNodeId);
    const core = String(participant.telegramUserId) === String(invokerId) ? " · user for this persistent group session" : "";
    return `- ${participantName(participant)} · Telegram user ID ${participant.telegramUserId} · root node identifier ${identifier}${core}`;
  });
  const messages = groupContext.messages.map(message => {
    const sender = message.sentByKennedy ? "Kennedy" : participantName(message);
    const reply = message.replyToMessageId == null ? "" : ` · replying to message ${message.replyToMessageId}`;
    return `- message ${message.messageId} · ${sender}${reply}: ${message.text}`;
  });
  return [
    `Group: ${groupContext.groupTitle || "Telegram group"} (chat ID ${groupContext.chatId})`,
    groupRootIdentifier == null
      ? "This archived session predates group roots; use the always-loaded roots shown in the Kmap context."
      : invokerId == null
        ? `This is background group-chat ingress. No participant is designated as the core user. The group root (${groupRootIdentifier}) and Kennedy's root (${kennedyRootIdentifier}) are loaded automatically, leaving room for ${additionalCapacity} additional directly loaded nodes.`
        : `The invoking Telegram user ID is ${invokerId}. The invoking participant's root (${invokerRootIdentifier}), the group root (${groupRootIdentifier}), and Kennedy's root (${kennedyRootIdentifier}) are loaded automatically in that order, leaving room for ${additionalCapacity} additional directly loaded nodes.`,
    "Participant root identifiers are registered in this session. The session participant's root is loaded; other participant roots are not automatically loaded:",
    ...participants,
    "",
    `Telegram messages supplied as context (${messages.length}):`,
    ...messages,
  ].join("\n");
}

function section(title, content) {
  return `${title}\n\n${content}`;
}

export function composePrompt(manuals, mode, { providerKind = null, model, reasoningEffort, sessionType = "conversation", sourceSessionType = "conversation", sessionContext = "" } = {}) {
  const audioIngress = mode !== "conversation" && sourceSessionType === "audio";
  const required = requiredPromptKeys(mode, { sourceSessionType, providerKind });
  const missing = required.filter(key => typeof manuals?.[key] !== "string" || !manuals[key].trim());
  if (missing.length) throw new Error(`Missing system prompt sections: ${missing.join(", ")}.`);
  const session = mode === "conversation"
    ? manuals.conversationSession
    : audioIngress ? manuals.audioIngressSession : manuals.historyIngressSession;
  const currentModel = runtimeValue(model, "unknown-model");
  const currentThinkingMode = runtimeValue(reasoningEffort, "unknown-thinking");
  const sections = [
    section("Kennedy's identity", manuals.identity),
    section("Session type", `${session}\n\n${sessionDetail(mode, sessionType, sourceSessionType)}`),
    section("Kmap basics", manuals.kmapBasics),
    section("Read-only tools", manuals.readTools),
  ];
  if (mode !== "conversation") sections.push(section("Write tools", manuals.writeTools));
  if (providerKind === "codex") sections.push(section("Codex harness", manuals.codexHarness));
  if (typeof sessionContext === "string" && sessionContext.trim()) sections.push(section("Telegram group context", sessionContext.trim()));
  sections.push(section("Current runtime", `You are currently running on ${currentModel} with ${currentThinkingMode} thinking mode.`));
  return sections.join("\n\n");
}
