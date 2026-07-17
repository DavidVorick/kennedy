export const PROMPT_FILES = {
  identity: "KennedyIdentity.txt",
  conversationSession: "ConversationSession.txt",
  historyIngressSession: "HistoryIngressSession.txt",
  audioIngressSession: "AudioIngressSession.txt",
  kmapBasics: "KmapBasics.txt",
  readTools: "ReadTools.txt",
  writeTools: "WriteTools.txt",
};

export function requiredPromptKeys(mode, { sourceSessionType = "conversation" } = {}) {
  const sessionKey = mode === "conversation"
    ? "conversationSession"
    : sourceSessionType === "audio" ? "audioIngressSession" : "historyIngressSession";
  return [
    "identity",
    sessionKey,
    "kmapBasics",
    "readTools",
    ...(mode === "conversation" ? [] : ["writeTools"]),
  ];
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
    return sessionType === "telegram"
      ? "Channel: Telegram. The final conversational response is relayed to the user; the visible Chatend and tool loop still run in Kennedy's browser UI."
      : "Channel: Kennedy's browser UI.";
  }
  if (sourceSessionType === "audio") return "Source: one chronologically placed piece of a vnote transcript.";
  return sourceSessionType === "telegram"
    ? "Source: an archived Telegram conversation."
    : "Source: an archived browser conversation.";
}

function section(title, content) {
  return `${title}\n\n${content}`;
}

export function composePrompt(manuals, mode, { model, reasoningEffort, sessionType = "conversation", sourceSessionType = "conversation" } = {}) {
  const audioIngress = mode !== "conversation" && sourceSessionType === "audio";
  const required = requiredPromptKeys(mode, { sourceSessionType });
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
  sections.push(section("Current runtime", `You are currently running on ${currentModel} with ${currentThinkingMode} thinking mode.`));
  return sections.join("\n\n");
}
