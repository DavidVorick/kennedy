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

export function renderInspector(pre, summary, diagnostic) {
  pre.textContent = JSON.stringify(diagnostic, null, 2);
  summary.replaceChildren();
  const metrics = [
    `${diagnostic.mode || "conversation"}`,
    `${diagnostic.context?.loadedNodeIds?.length || 0}/7 directly loaded`,
    `${diagnostic.loadCalls || 0}/${diagnostic.loadLimit || 0} loads`,
    `${diagnostic.toolLog?.length || 0} tool calls`,
    diagnostic.model || "no model",
  ];
  for (const metric of metrics) summary.append(element("span", "metric", metric));
}

export function showError(banner, message) { banner.textContent = message; banner.classList.remove("hidden"); }
export function clearError(banner) { banner.textContent = ""; banner.classList.add("hidden"); }

export { element };

