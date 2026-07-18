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

function fixedConnectionLines(connections) {
  if (!connections?.length) return ["Fixed connections: none"];
  return [
    "Fixed connections:",
    ...connections.flatMap(connection => [
      `  - slot ${connection.slot}: ${connection.identifier}: ${text(connection.shortName)}`,
      `    Summary: ${text(connection.shortDescription)}`,
    ]),
  ];
}

export function formatContextNode(node) {
  const owner = Number.isInteger(node.ownerIdentifier) ? `Node ${node.ownerIdentifier}` : "unowned";
  return [
    `Node ${node.identifier}: ${text(node.shortName)}`,
    `Summary: ${text(node.shortDescription)}`,
    `Last modified by: ${text(node.lastModifiedBy, "legacy-unknown")}`,
    `Last modified at: ${text(node.lastModifiedAt, "unknown")}`,
    `Owner: ${owner}`,
    "Details:",
    indented(node.longDescription),
    ...fixedConnectionLines(node.fixedConnections),
    ...connectionLines("Active connections", node.activeConnections),
    ...connectionLines("Fanout connections", node.fanoutConnections),
  ].join("\n");
}

function connectionIdentifiers(connections) {
  return connections?.length ? connections.map(connection => connection.identifier).join(", ") : "none";
}

function fixedConnectionIdentifiers(connections) {
  return connections?.length
    ? connections.map(connection => `slot ${connection.slot}: ${connection.identifier}`).join(", ")
    : "none";
}

function formatCompactFullNode(node, includeShortDescription) {
  const owner = Number.isInteger(node.ownerIdentifier) ? `Node ${node.ownerIdentifier}` : "unowned";
  return [
    `Node ${node.identifier}: ${text(node.shortName)}`,
    ...(includeShortDescription ? [`Summary: ${text(node.shortDescription)}`] : []),
    `Last modified by: ${text(node.lastModifiedBy, "legacy-unknown")}`,
    `Last modified at: ${text(node.lastModifiedAt, "unknown")}`,
    `Owner: ${owner}`,
    "Details:",
    indented(node.longDescription),
    `Fixed connection identifiers: ${fixedConnectionIdentifiers(node.fixedConnections)}`,
    `Active connection identifiers: ${connectionIdentifiers(node.activeConnections)}`,
    `Fanout connection identifiers: ${connectionIdentifiers(node.fanoutConnections)}`,
  ].join("\n");
}

function uniqueConnections(nodes, select, excluded = new Set()) {
  const unique = new Map();
  for (const node of nodes || []) {
    for (const connection of select(node) || []) {
      if (!excluded.has(connection.identifier) && !unique.has(connection.identifier)) unique.set(connection.identifier, connection);
    }
  }
  return [...unique.values()];
}

function fanoutReferenceGroups(directNodes, activeNodes) {
  const fullIdentifiers = new Set([...directNodes, ...activeNodes].map(node => node.identifier));
  const directConnectionIdentifiers = new Set(directNodes.flatMap(node => [
    ...(node.fixedConnections || []),
    ...(node.activeConnections || []),
    ...(node.fanoutConnections || []),
  ]).map(connection => connection.identifier));
  const directFanoutNodes = uniqueConnections(directNodes, node => node.fanoutConnections, fullIdentifiers);
  const indirectExcluded = new Set([...fullIdentifiers, ...directConnectionIdentifiers]);
  const indirectFanoutNodes = uniqueConnections(activeNodes, node => node.fanoutConnections, indirectExcluded);
  return { directFanoutNodes, indirectFanoutNodes };
}

function formatCompactMemorySections({ directNodes = [], activeNodes = [], directFanoutNodes, indirectFanoutNodes }) {
  const fallback = fanoutReferenceGroups(directNodes, activeNodes);
  const directFanouts = Array.isArray(directFanoutNodes) ? directFanoutNodes : fallback.directFanoutNodes;
  const indirectFanouts = Array.isArray(indirectFanoutNodes) ? indirectFanoutNodes : fallback.indirectFanoutNodes;
  const sections = [];
  if (directNodes.length) {
    sections.push(`Directly loaded nodes\n\n${directNodes.map(node => formatCompactFullNode(node, true)).join("\n\n")}`);
  }
  if (activeNodes.length) {
    sections.push(`Full active-connection nodes\n\n${activeNodes.map(node => formatCompactFullNode(node, false)).join("\n\n")}`);
  }
  if (directFanouts.length) {
    sections.push([
      "Fanout nodes of directly loaded nodes",
      "",
      ...directFanouts.flatMap(connection => [
        `${connection.identifier}: ${text(connection.shortName)}`,
        `  Summary: ${text(connection.shortDescription)}`,
      ]),
    ].join("\n"));
  }
  if (indirectFanouts.length) {
    sections.push([
      "Fanout nodes only of active-connection nodes",
      "",
      ...indirectFanouts.map(connection => `${connection.identifier}: ${text(connection.shortName)}`),
    ].join("\n"));
  }
  return sections.join("\n\n");
}

export function formatKmapContext(snapshot) {
  const roots = snapshot.rootIdentifiers?.length
    ? snapshot.rootIdentifiers.join(", ")
    : "none";
  const identifiers = snapshot.directlyLoadedIdentifiers?.length
    ? snapshot.directlyLoadedIdentifiers.join(", ")
    : "none";
  const directIdentifiers = new Set(snapshot.directlyLoadedIdentifiers || []);
  const nodeByIdentifier = new Map((snapshot.nodes || []).map(node => [node.identifier, node]));
  const directNodes = (snapshot.directlyLoadedIdentifiers || []).map(identifier => nodeByIdentifier.get(identifier)).filter(Boolean);
  const activeNodes = (snapshot.nodes || []).filter(node => !directIdentifiers.has(node.identifier));
  const nodes = formatCompactMemorySections({ directNodes, activeNodes }) || "No memory nodes are currently loaded.";
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
        result.requestedNodeAlreadyLoaded
          ? `Requested node: Node ${result.requestedNodeIdentifier} was already available in full context and is now directly loaded.`
          : null,
        formatCompactMemorySections({
          directNodes: result.requestedNode ? [result.requestedNode] : [],
          activeNodes: result.activeConnectionNodes || [],
          directFanoutNodes: result.directFanoutNodes,
          indirectFanoutNodes: result.indirectFanoutNodes,
        }),
      ].filter(Boolean).join("\n\n");
    case "ResetContext":
      return "Memory context reset completed. The rebuilt Kmap context above contains the newly loaded nodes.";
    case "ConnectNodes":
      return ["Memory connections updated.", "", formatNodes("Affected nodes", result.nodes)].join("\n");
    case "ConsolidateFanout":
      return ["Fanout connections consolidated.", "", formatNodes("Affected nodes", result.nodes)].join("\n");
    case "SetFixedConnection":
      return [
        result.cleared ? "Fixed connection slot cleared." : "Fixed connection assigned.",
        "",
        formatNodes("Updated parent node", result.node ? [result.node] : []),
        ...(result.replacedFixedConnection ? ["", `Replaced fixed connection: slot ${result.replacedFixedConnection.slot} · ${result.replacedFixedConnection.identifier}: ${text(result.replacedFixedConnection.shortName)}`] : []),
      ].join("\n");
    case "CreateNode":
      return ["Memory node created.", "", formatNodes("Created node", result.node ? [result.node] : [])].join("\n");
    case "UpdateNode":
      return ["Memory node updated.", "", formatNodes("Updated node", result.node ? [result.node] : [])].join("\n");
    case "EndSelfTimeSession":
    case "EndFreeTimeSession":
      return [
        "The current self-time session is ending with the total time unchanged.",
        `Remaining self time: ${text(result.remaining)}`,
        result.messageForwarded ? "The message was saved for the next self-time session." : null,
        text(result.next),
      ].filter(Boolean).join("\n");
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
