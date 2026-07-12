import { formatKmapContext } from "./human_format.js?v=20260712.1";

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

export function renderInspector(container, diagnostic, view = "full") {
  container.replaceChildren();
  if (view === "memory") {
    renderMemoryTree(container, diagnostic.memory);
    return;
  }
  container.append(element("pre", "inspector-text", inspectorText(diagnostic, view)));
}

export function inspectorText(diagnostic, view = "full") {
  if (view === "memory") return formatKmapContext(diagnostic.memory || { directlyLoadedIdentifiers: [], nodes: [] });
  const labels = { system: "System context", user: "David", assistant: "Kennedy" };
  let messages = diagnostic.chatend || [];
  if (view === "system") {
    const explicit = messages.filter(message => message.context_kind === "instructions");
    messages = explicit.length ? explicit : messages.filter((message, index) => message.role === "system" && index === 0);
  }
  return messages
    .filter(message => typeof message.content === "string" && message.content.trim())
    .map(message => `${message.display_role || labels[message.role] || "Context"}\n\n${message.content.trim()}`)
    .join("\n\n────────────────────────\n\n");
}

function tokenCount(value) {
  if (value === null || value === undefined) return "Unknown";
  return new Intl.NumberFormat("en-US", { notation: value >= 100000 ? "compact" : "standard", maximumFractionDigits: 1 }).format(value);
}

function exactTokenCount(value) {
  return value === null || value === undefined ? "Unknown" : new Intl.NumberFormat("en-US").format(value);
}

export function renderUsage(container, diagnostic) {
  const usage = diagnostic.usage;
  container.replaceChildren();
  if (!usage) {
    container.append(element("span", "context-usage-primary", "Usage unavailable"));
    return;
  }
  const contextPercent = usage.contextWindowTokens ? 100 * usage.contextTokens / usage.contextWindowTokens : 0;
  const cachePercent = usage.cacheReadPercent || 0;
  const primary = usage.contextWindowTokens
    ? `${exactTokenCount(usage.contextTokens)} / ${exactTokenCount(usage.contextWindowTokens)}`
    : `${exactTokenCount(usage.contextTokens)} used`;
  const remaining = usage.contextRemaining === null ? "window unknown" : `${exactTokenCount(usage.contextRemaining)} remaining`;
  const text = element("div", "context-usage-text");
  text.append(
    element("strong", "context-usage-primary", primary),
    element("span", "context-usage-secondary", `${remaining} · ${cachePercent.toFixed(1)}% cache reads`),
  );
  const track = element("div", "context-usage-track");
  const fill = element("div", "context-usage-fill");
  fill.style.width = `${Math.max(0, Math.min(100, contextPercent))}%`;
  track.append(fill);
  container.title = [
    `${exactTokenCount(usage.contextTokens)} tokens currently in context`,
    `${remaining}`,
    `${exactTokenCount(usage.totalInputTokens)} cumulative input tokens`,
    `${exactTokenCount(usage.totalOutputTokens)} cumulative output tokens`,
    `${exactTokenCount(usage.totalCachedTokens)} cache-read tokens`,
    `${exactTokenCount(usage.totalCacheWriteTokens)} cache-write tokens`,
  ].join(" · ");
  container.append(text, track);
}

function badge(text, kind) { return element("span", `memory-badge ${kind}`, text); }

function connectionLeaf(connection, kind, nodeByIdentifier, directlyLoaded) {
  const row = element("div", "memory-connection");
  const target = nodeByIdentifier.get(connection.identifier);
  row.append(
    badge(String(connection.identifier), "identifier"),
    element("span", "memory-connection-name", connection.shortName),
    badge(kind === "fanout" ? "summary only" : target ? "full context" : "summary only", kind === "fanout" || !target ? "summary" : "expanded"),
  );
  if (directlyLoaded.has(connection.identifier)) row.append(badge("also directly loaded", "direct"));
  if (connection.shortDescription) row.append(element("span", "memory-connection-description", connection.shortDescription));
  return row;
}

function connectionGroup(title, connections, kind, nodeByIdentifier, directlyLoaded, path, depth) {
  const group = element("div", `memory-branch ${kind}`);
  const heading = element("div", "memory-branch-title");
  heading.append(element("span", "memory-branch-line"), element("strong", "", title), badge(String(connections.length), "count"));
  group.append(heading);
  if (!connections.length) {
    group.append(element("p", "memory-none", "None"));
    return group;
  }
  for (const connection of connections) {
    const target = nodeByIdentifier.get(connection.identifier);
    const canExpand = kind === "active" && target && !path.has(connection.identifier) && depth < 2;
    if (canExpand) {
      group.append(memoryNode(target, "expanded", nodeByIdentifier, directlyLoaded, new Set([...path, connection.identifier]), depth + 1));
    } else {
      group.append(connectionLeaf(connection, kind, nodeByIdentifier, directlyLoaded));
    }
  }
  return group;
}

function memoryNode(node, relation, nodeByIdentifier, directlyLoaded, path, depth = 0) {
  const details = element("details", `memory-node ${relation}`);
  if (relation === "direct") details.open = true;
  const summary = element("summary", "memory-node-summary");
  const sourceLabel = relation === "direct"
    ? "directly loaded"
    : node.contextSources?.includes("active") ? "pulled by active connection" : "full context";
  summary.append(
    badge(String(node.identifier), "identifier"),
    element("strong", "memory-node-name", node.shortName),
    badge(sourceLabel, relation),
  );
  if (relation !== "direct" && directlyLoaded.has(node.identifier)) summary.append(badge("also directly loaded", "direct"));
  details.append(summary);
  const body = element("div", "memory-node-body");
  if (node.shortDescription) body.append(element("p", "memory-node-short", node.shortDescription));
  body.append(element("p", "memory-node-long", node.longDescription || "No detailed description."));
  body.append(
    connectionGroup("Active connections", node.activeConnections || [], "active", nodeByIdentifier, directlyLoaded, path, depth),
    connectionGroup("Fanout references", node.fanoutConnections || [], "fanout", nodeByIdentifier, directlyLoaded, path, depth),
  );
  details.append(body);
  return details;
}

function renderMemoryTree(container, snapshot) {
  const memory = snapshot || { directlyLoadedIdentifiers: [], nodes: [] };
  const directlyLoaded = new Set(memory.directlyLoadedIdentifiers || []);
  const nodeByIdentifier = new Map((memory.nodes || []).map(node => [node.identifier, node]));
  const activeExpanded = [...nodeByIdentifier.values()].filter(node => !directlyLoaded.has(node.identifier) && node.contextSources?.includes("active")).length;
  const intro = element("div", "memory-tree-intro");
  intro.append(
    element("div", "", "This is the Kmap material currently visible to Kennedy."),
    badge(`${directlyLoaded.size} directly loaded`, "direct"),
    badge(`${activeExpanded} pulled through active connections`, "expanded"),
    badge(`${Math.max(0, nodeByIdentifier.size - directlyLoaded.size - activeExpanded)} other full-context`, "summary"),
  );
  container.append(intro);
  if (!nodeByIdentifier.size) {
    container.append(element("p", "memory-tree-empty", "No memory nodes are currently in context."));
    return;
  }
  const roots = [...directlyLoaded].map(identifier => nodeByIdentifier.get(identifier)).filter(Boolean);
  for (const root of roots) container.append(memoryNode(root, "direct", nodeByIdentifier, directlyLoaded, new Set([root.identifier])));
  const other = [...nodeByIdentifier.values()].filter(node =>
    !directlyLoaded.has(node.identifier) && !node.contextSources?.includes("active")
  );
  if (other.length) {
    const section = element("section", "memory-other");
    section.append(element("h3", "", "Other full-context nodes"));
    for (const node of other) section.append(memoryNode(node, "expanded", nodeByIdentifier, directlyLoaded, new Set([node.identifier])));
    container.append(section);
  }
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
