export const CHATEND_SEPARATOR = "\n\n────────────────────────\n\n";

const ROLE_LABELS = { system: "System context", user: "David", assistant: "Kennedy" };

function exactTokens(value) {
  return new Intl.NumberFormat("en-US").format(Math.max(0, Number(value) || 0));
}

export function formatContextWindowProgress(usage) {
  const contextWindowTokens = Number(usage?.contextWindowTokens) || 0;
  if (contextWindowTokens <= 0) return "context window usage: unknown";
  const contextKnown = usage?.contextKnown === true || Boolean(usage?.last);
  if (!contextKnown) {
    return `context window usage: unknown / ${exactTokens(contextWindowTokens)}`;
  }
  const contextTokens = Math.max(0, Number(usage?.contextTokens) || 0);
  return `context window usage: ${exactTokens(contextTokens)} / ${exactTokens(contextWindowTokens)}`;
}

export function formatChatend(messages, usage = null) {
  const formatted = (Array.isArray(messages) ? messages : [])
    .filter(message => typeof message?.content === "string" && message.content.trim())
    .map(message => `${message.display_role || ROLE_LABELS[message.role] || "Context"}\n\n${message.content.trim()}`)
    .join(CHATEND_SEPARATOR);
  return usage ? [formatted, formatContextWindowProgress(usage)].filter(Boolean).join(CHATEND_SEPARATOR) : formatted;
}
