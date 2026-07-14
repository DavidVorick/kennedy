const FILES = {
  identity: "KennedyIdentity.txt",
  conversation: "ConversationManual.txt",
  ingress: "HistoryIngress.txt",
};

export async function loadPromptManuals(base = "") {
  const entries = await Promise.all(Object.entries(FILES).map(async ([key, file]) => {
    const response = await fetch(`${base}/system-prompts/${file}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`Could not load system prompt ${file}.`);
    return [key, (await response.text()).trim()];
  }));
  return Object.fromEntries(entries);
}

function runtimeValue(value, fallback) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

export function formatModelAttribution(model, reasoningEffort) {
  return `${runtimeValue(model, "unknown-model")}-${runtimeValue(reasoningEffort, "unknown-thinking")}`;
}

export function composePrompt(manuals, mode, { model, reasoningEffort } = {}) {
  const session = mode === "conversation" ? manuals.conversation : manuals.ingress;
  const sessionTitle = mode === "conversation" ? "Conversation session instructions" : "History-ingress session instructions";
  const currentModel = runtimeValue(model, "unknown-model");
  const currentThinkingMode = runtimeValue(reasoningEffort, "unknown-thinking");
  return [
    "Kennedy's identity",
    "",
    manuals.identity ?? manuals.shared,
    "",
    sessionTitle,
    "",
    session,
    "",
    "Current runtime",
    "",
    `You are currently running on ${currentModel} with ${currentThinkingMode} thinking mode.`,
  ].join("\n");
}
