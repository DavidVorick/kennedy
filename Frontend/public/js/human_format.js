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

export function formatContextNode(node) {
  return [
    `Node ${node.identifier}: ${text(node.shortName)}`,
    `Summary: ${text(node.shortDescription)}`,
    "Details:",
    indented(node.longDescription),
    ...connectionLines("Active connections", node.activeConnections),
    ...connectionLines("Fanout connections", node.fanoutConnections),
  ].join("\n");
}

export function formatKmapContext(snapshot) {
  const identifiers = snapshot.directlyLoadedIdentifiers?.length
    ? snapshot.directlyLoadedIdentifiers.join(", ")
    : "none";
  const nodes = snapshot.nodes?.length
    ? snapshot.nodes.map(formatContextNode).join("\n\n")
    : "No memory nodes are currently loaded.";
  return [
    "Current Kmap context",
    "",
    `Directly loaded memory identifiers: ${identifiers}`,
    "",
    nodes,
  ].join("\n");
}

function formatNodes(title, nodes) {
  if (!nodes?.length) return `${title}\n\nNone.`;
  return `${title}\n\n${nodes.map(formatContextNode).join("\n\n")}`;
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
    case "CreateNode":
      return ["Memory node created.", "", formatNodes("Created node", result.node ? [result.node] : [])].join("\n");
    case "UpdateNode":
      return ["Memory node updated.", "", formatNodes("Updated node", result.node ? [result.node] : [])].join("\n");
    default:
      return `${toolName} completed successfully.`;
  }
}
