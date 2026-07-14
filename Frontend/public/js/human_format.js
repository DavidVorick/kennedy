function text(value, fallback = "(none)") {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value);
}

function indented(value, spaces = 2) {
  const prefix = " ".repeat(spaces);
  return text(value).split("\n").map(line => `${prefix}${line}`).join("\n");
}

function connectionLines(title, connections) {
  if (!connections?.length) return [`${title}: none`];
  return [
    `${title}:`,
    ...connections.flatMap(connection => [
      `  - ${connection.identifier}: ${text(connection.shortName)}`,
      `    Summary: ${text(connection.shortDescription)}`,
    ]),
  ];
}

function taskConnectionLines(connections) {
  if (!connections?.length) return ["Task connections: none"];
  return [
    "Task connections:",
    ...connections.flatMap(connection => [
      `  - ${connection.priority}: ${connection.identifier}: ${text(connection.shortName)}`,
      `    Summary: ${text(connection.shortDescription)}`,
    ]),
  ];
}

export function formatContextNode(node) {
  return [
    `Node ${node.identifier}: ${text(node.shortName)}`,
    `Summary: ${text(node.shortDescription)}`,
    `Last modified by: ${text(node.lastModifiedBy, "legacy-unknown")}`,
    "Details:",
    indented(node.longDescription),
    ...taskConnectionLines(node.taskConnections),
    ...connectionLines("Active connections", node.activeConnections),
    ...connectionLines("Fanout connections", node.fanoutConnections),
  ].join("\n");
}

export function formatKmapContext(snapshot) {
  const roots = snapshot.rootIdentifiers?.length
    ? snapshot.rootIdentifiers.join(", ")
    : "none";
  const identifiers = snapshot.directlyLoadedIdentifiers?.length
    ? snapshot.directlyLoadedIdentifiers.join(", ")
    : "none";
  const nodes = snapshot.nodes?.length
    ? snapshot.nodes.map(formatContextNode).join("\n\n")
    : "No memory nodes are currently loaded.";
  return [
    "Current Kmap context",
    "",
    `Always-loaded root identifiers: ${roots}`,
    `Directly loaded node limit: 10`,
    `Directly loaded memory identifiers: ${identifiers}`,
    "",
    nodes,
  ].join("\n");
}

function formatNodes(title, nodes) {
  if (!nodes?.length) return `${title}\n\nNone.`;
  return `${title}\n\n${nodes.map(formatContextNode).join("\n\n")}`;
}

function formatWebSources(sources) {
  if (!sources?.length) return "Sources: none returned.";
  return [
    "Sources:",
    ...sources.flatMap((source, index) => [
      `  ${index + 1}. ${text(source.title, source.url)}`,
      `     URL: ${text(source.url)}`,
    ]),
  ].join("\n");
}

export function formatToolResult(toolName, content) {
  if (!content?.ok) {
    return [
      `${toolName} could not be completed.`,
      "",
      `Reason: ${text(content?.error?.message, "The local operation failed.")}`,
    ].join("\n");
  }

  const result = content.result || {};
  switch (toolName) {
    case "LoadNode":
      return [
        "Memory load completed.",
        "",
        formatNodes("Requested node", result.requestedNode ? [result.requestedNode] : []),
        "",
        formatNodes("Newly available active-connection nodes", result.activeConnectionNodes),
      ].join("\n");
    case "ResetContext":
      return ["Memory context reset completed.", "", formatKmapContext(result.context || {})].join("\n");
    case "ConnectNodes":
      return ["Memory connections updated.", "", formatNodes("Affected nodes", result.nodes)].join("\n");
    case "ConsolidateFanout":
      return ["Fanout connections consolidated.", "", formatNodes("Affected nodes", result.nodes)].join("\n");
    case "AssignTask":
      return [
        result.cleared ? "Task slot cleared." : "Task connection assigned.",
        "",
        formatNodes("Updated parent node", result.node ? [result.node] : []),
        ...(result.replacedTask ? ["", `Replaced task: ${result.replacedTask.priority} · ${result.replacedTask.identifier}: ${text(result.replacedTask.shortName)}`] : []),
      ].join("\n");
    case "CreateNode":
      return ["Memory node created.", "", formatNodes("Created node", result.node ? [result.node] : [])].join("\n");
    case "UpdateNode":
      return ["Memory node updated.", "", formatNodes("Updated node", result.node ? [result.node] : [])].join("\n");
    case "WebSearch":
      return [
        "Web research completed.",
        "",
        "Research answer:",
        indented(result.answer),
        "",
        formatWebSources(result.sources),
      ].join("\n");
    case "WebFetch":
      return [
        "Web page fetched.",
        "",
        `URL: ${text(result.url)}`,
        `Title: ${text(result.title)}`,
        `Retrieved: ${text(result.retrieved_at)}`,
        `Content type: ${text(result.content_type)}`,
        `Truncated: ${result.truncated ? "yes" : "no"}`,
        "",
        "Readable page content:",
        indented(result.content),
      ].join("\n");
    default:
      return `${toolName} completed successfully.`;
  }
}
