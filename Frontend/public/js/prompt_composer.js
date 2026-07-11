const FILES = {
  shared: "SystemPromptKmapAgentManual.txt",
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
  return [
    "<kennedy_shared_manual>", manuals.shared, "</kennedy_shared_manual>",
    "", `<kennedy_${mode}_manual>`, session, `</kennedy_${mode}_manual>`,
  ].join("\n");
}

