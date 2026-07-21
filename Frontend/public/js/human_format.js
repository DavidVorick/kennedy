// Browser-only formatting helpers.
function text(value, fallback = "(none)") {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value);
}

function indented(value, spaces = 2) {
  const prefix = " ".repeat(spaces);
  return text(value).split("\n").map(line => `${prefix}${line}`).join("\n");
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
