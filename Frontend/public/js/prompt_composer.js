const FILES = {
  identity: "KennedyIdentity.txt",
  conversation: "ConversationManual.txt",
  ingress: "HistoryIngress.txt",
  audioIngress: "AudioIngress.txt",
};

export async function loadPromptManuals(base = "") {
  const results = await Promise.all(Object.entries(FILES).map(async ([key, file]) => {
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

export function composePrompt(manuals, mode, { model, reasoningEffort, sessionType = "conversation", sourceSessionType = "conversation" } = {}) {
  const audioIngress = mode !== "conversation" && sourceSessionType === "audio";
  const session = mode === "conversation"
    ? manuals.conversation
    : audioIngress
      ? [manuals.ingress, manuals.audioIngress].filter(Boolean).join("\n\n")
      : manuals.ingress;
  const sessionTitle = mode === "conversation"
    ? "Conversation session instructions"
    : audioIngress ? "Audio-ingress session instructions" : "History-ingress session instructions";
  const currentModel = runtimeValue(model, "unknown-model");
  const currentThinkingMode = runtimeValue(reasoningEffort, "unknown-thinking");
  const sessionDescription = mode === "conversation"
    ? sessionType === "telegram"
      ? "This is a telegram session. The user is talking to you through Kennedy's Telegram bot. Your final conversational output will be relayed to Telegram; the visible Chatend and tool loop still run in Kennedy's browser UI."
      : "This is a conversation session in Kennedy's browser UI."
    : audioIngress
      ? "This is an audio-ingress session. You are ingressing one chronologically placed piece of a fallible vnote transcript."
      : sourceSessionType === "telegram"
      ? "This is a history-ingress session. You are ingressing an archived telegram session."
      : "This is a history-ingress session. You are ingressing an archived UI conversation session.";
  return [
    "Kennedy's identity",
    "",
    manuals.identity ?? manuals.shared,
    "",
    sessionTitle,
    "",
    session,
    "",
    "Current session",
    "",
    sessionDescription,
    "",
    "Current runtime",
    "",
    `You are currently running on ${currentModel} with ${currentThinkingMode} thinking mode.`,
  ].join("\n");
}
