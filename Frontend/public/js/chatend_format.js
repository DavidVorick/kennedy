// Browser-only Chatend rendering helpers.
export const CHATEND_SEPARATOR = "\n\n────────────────────────\n\n";

const ROLE_LABELS = { system: "System context", user: "David", assistant: "Kennedy" };

function exactTokens(value) {
  return new Intl.NumberFormat("en-US").format(Math.max(0, Number(value) || 0));
}

export function contextUsageMeasurement(usage) {
  const contextWindowTokens = Math.max(0, Number(usage?.contextWindowTokens) || 0);
  const storedContextTokens = Number(usage?.contextTokens);
  const storedKnown = usage?.contextKnown === true && Number.isFinite(storedContextTokens);
  const tokenPair = value => {
    const inputTokens = Number(value?.inputTokens);
    const outputTokens = Number(value?.outputTokens);
    return Number.isFinite(inputTokens) && Number.isFinite(outputTokens)
      ? { inputTokens: Math.max(0, inputTokens), outputTokens: Math.max(0, outputTokens) }
      : null;
  };
  const previousCall = tokenPair(usage?.lastContext) || tokenPair(usage?.last);
  const previousKnown = Boolean(previousCall);
  const contextKnown = storedKnown || previousKnown;
  const contextTokens = previousKnown
    ? previousCall.inputTokens + previousCall.outputTokens
    : storedKnown ? Math.max(0, storedContextTokens) : 0;
  return {
    contextKnown,
    contextTokens,
    contextWindowTokens,
    contextRemaining: contextKnown && contextWindowTokens
      ? Math.max(0, contextWindowTokens - contextTokens)
      : null,
  };
}

export function formatContextWindowProgress(usage) {
  const { contextKnown, contextTokens, contextWindowTokens } = contextUsageMeasurement(usage);
  if (contextWindowTokens <= 0) return "context window usage: unknown";
  if (!contextKnown) {
    return `context window usage: unknown / ${exactTokens(contextWindowTokens)}`;
  }
  return `context window usage: ${exactTokens(contextTokens)} / ${exactTokens(contextWindowTokens)}`;
}

export function formatChatend(messages, usage = null) {
  const formatted = (Array.isArray(messages) ? messages : [])
    .filter(message => typeof message?.content === "string" && message.content.trim())
    .map(message => `${message.display_role || ROLE_LABELS[message.role] || "Context"}\n\n${message.content.trim()}`)
    .join(CHATEND_SEPARATOR);
  return usage ? [formatted, formatContextWindowProgress(usage)].filter(Boolean).join(CHATEND_SEPARATOR) : formatted;
}
