const FILES = {
  shared: "KmapAgentManual.txt",
  conversation: "ConversationAgentManual.txt",
  ingress: "HistoryIngressAgentManual.txt",
};

export async function loadPromptManuals(base = "") {
  const entries = await Promise.all(Object.entries(FILES).map(async ([key, file]) => {
    const response = await fetch(`${base}/system-prompts/${file}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`Could not load system prompt ${file}.`);
    return [key, (await response.text()).trim()];
  }));
  return Object.fromEntries(entries);
}

export function composePrompt(manuals, mode) {
  const session = mode === "conversation" ? manuals.conversation : manuals.ingress;
  const sessionTitle = mode === "conversation" ? "Conversation session instructions" : "History-ingress session instructions";
  return [
    "Kennedy's shared instructions",
    "",
    manuals.shared,
    "",
    sessionTitle,
    "",
    session,
  ].join("\n");
}
