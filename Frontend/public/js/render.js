function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

export function renderTranscript(container, transcript) {
  container.replaceChildren();
  if (!transcript.length) {
    const empty = element("div", "empty-state");
    empty.append(element("p", "empty-title", "What are we working on?"), element("p", "", "Kennedy can help directly and draw on your local memory when it matters."));
    container.append(empty); return;
  }
  for (const item of transcript) {
    const message = element("article", `message ${item.role === "kennedy" ? "assistant" : "user"}`);
    message.append(element("span", "role", item.role === "kennedy" ? "Kennedy" : "You"), element("div", "body", item.content));
    container.append(message);
  }
  container.scrollTop = container.scrollHeight;
}

export function renderInspector(pre, diagnostic) {
  pre.textContent = inspectorText(diagnostic);
}

export function inspectorText(diagnostic) {
  const labels = { system: "System context", user: "David", assistant: "Kennedy" };
  return (diagnostic.chatend || [])
    .filter(message => typeof message.content === "string" && message.content.trim())
    .map(message => `${message.display_role || labels[message.role] || "Context"}\n\n${message.content.trim()}`)
    .join("\n\n────────────────────────\n\n");
}

function tokenCount(value) {
  if (value === null || value === undefined) return "Unknown";
  return new Intl.NumberFormat("en-US", { notation: value >= 100000 ? "compact" : "standard", maximumFractionDigits: 1 }).format(value);
}

function metric(label, value, detail, progress = null) {
  const card = element("div", "usage-card");
  card.append(element("span", "usage-label", label), element("strong", "usage-value", value), element("span", "usage-detail", detail));
  if (progress !== null) {
    const track = element("div", "usage-track");
    const fill = element("div", "usage-fill");
    fill.style.width = `${Math.max(0, Math.min(100, progress))}%`;
    track.append(fill); card.append(track);
  }
  return card;
}

export function renderUsage(container, diagnostic) {
  const usage = diagnostic.usage;
  container.replaceChildren();
  if (!usage) {
    container.append(metric("Context", "Waiting", "Usage appears after the first response."));
    return;
  }
  const contextPercent = usage.contextWindowTokens ? 100 * usage.contextTokens / usage.contextWindowTokens : null;
  const cachePercent = usage.cacheReadPercent || 0;
  const remaining = usage.contextRemaining === null
    ? "Window size unknown"
    : `${tokenCount(usage.contextRemaining)} available of ${tokenCount(usage.contextWindowTokens)} · max input ${tokenCount(usage.maxInputTokens)}`;
  container.append(
    metric("Current context", `${tokenCount(usage.contextTokens)} tokens`, remaining, contextPercent),
    metric("Cache reads", `${tokenCount(usage.totalCachedTokens)} tokens`, `${cachePercent.toFixed(1)}% of session input`, cachePercent),
    metric("Cache writes", `${tokenCount(usage.totalCacheWriteTokens)} tokens`, "GPT-5.6 cache-prefill writes"),
    metric("Session usage", `${usage.requests} request${usage.requests === 1 ? "" : "s"}`, `${tokenCount(usage.totalInputTokens)} input · ${tokenCount(usage.totalOutputTokens)} output · ${tokenCount(usage.totalReasoningTokens)} reasoning`),
  );
}

export function renderIngressActivity(container, diagnostic, active) {
  container.replaceChildren();
  const usage = diagnostic?.usage?.snapshot?.();
  if (usage?.requests) {
    container.append(element(
      "p",
      "ingress-usage",
      `${usage.requests} request${usage.requests === 1 ? "" : "s"} · ${tokenCount(usage.totalInputTokens)} input · ${tokenCount(usage.totalCachedTokens)} cache reads · ${tokenCount(usage.totalCacheWriteTokens)} cache writes`,
    ));
  }
  const visible = (diagnostic?.chatend?.messages || []).filter(message =>
    message.role === "assistant" || message.display_role === "Memory tool result" || message.display_role === "Tool protocol error"
  );
  if (!visible.length) {
    container.append(element("p", "ingress-empty", active ? "Kennedy is preparing the history-ingress context…" : "No ingress activity was recorded."));
    return;
  }
  for (const message of visible) {
    const item = element("article", "ingress-entry");
    item.append(element("span", "role", message.display_role || "Kennedy"), element("pre", "ingress-body", message.content));
    container.append(item);
  }
  container.scrollTop = container.scrollHeight;
}

export function showError(banner, message) { banner.textContent = message; banner.classList.remove("hidden"); }
export function clearError(banner) { banner.textContent = ""; banner.classList.add("hidden"); }

export { element };
