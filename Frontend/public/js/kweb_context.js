export const MAX_DIRECTLY_LOADED_NODES = 10;

function rawConnectionIds(nodesById, nodeIds, fields) {
  const identifiers = new Set();
  for (const nodeId of nodeIds) {
    const node = nodesById.get(nodeId);
    if (!node) continue;
    for (const field of fields) {
      for (const connection of node[field] || []) identifiers.add(connection.id);
    }
  }
  return identifiers;
}

function uniqueRawConnections(connections, include) {
  const unique = new Map();
  for (const connection of connections || []) {
    if (include(connection) && !unique.has(connection.id)) unique.set(connection.id, connection);
  }
  return [...unique.values()];
}

export class KwebContext {
  constructor(api, rootNodeIds) {
    this.api = api;
    this.rootNodeIds = Array.isArray(rootNodeIds) ? [...rootNodeIds] : [rootNodeIds];
    if (!this.rootNodeIds.length || this.rootNodeIds.some(id => typeof id !== "string" || !id) || new Set(this.rootNodeIds).size !== this.rootNodeIds.length) {
      throw new Error("Kmap root node identifiers must be distinct non-empty strings.");
    }
    this.rootNodeId = this.rootNodeIds[0];
    this.clear();
  }

  clear() {
    this.loadedNodeIds = [];
    this.fullNodeIds = new Set();
    this.nodeOrigins = new Map();
    this.nodesById = new Map();
    this.shortToDurable = new Map();
    this.durableToShort = new Map();
    this.nextShortId = 1;
  }

  shortId(durableId) {
    if (!this.durableToShort.has(durableId)) {
      const short = this.nextShortId++;
      this.durableToShort.set(durableId, short);
      this.shortToDurable.set(short, durableId);
    }
    return this.durableToShort.get(durableId);
  }

  registerReference(durableId) {
    if (typeof durableId !== "string" || !durableId) throw new Error("A referenced Kmap node identifier must be a non-empty string.");
    return this.shortId(durableId);
  }

  resolve(identifier) {
    if (!Number.isInteger(identifier) || identifier < 1 || !this.shortToDurable.has(identifier)) throw Object.assign(new Error(`Unknown memory identifier ${identifier}.`), { code: "unknown_identifier" });
    return this.shortToDurable.get(identifier);
  }

  ingestNode(node, full = true, origin = "context") {
    this.shortId(node.id);
    for (const connection of [...(node.task_connections || []), ...(node.active_connections || []), ...(node.fanout_connections || [])]) this.shortId(connection.id);
    if (full) {
      this.nodesById.set(node.id, node); this.fullNodeIds.add(node.id);
      if (!this.nodeOrigins.has(node.id)) this.nodeOrigins.set(node.id, new Set());
      this.nodeOrigins.get(node.id).add(origin);
    }
    else if (!this.nodesById.has(node.id)) this.nodesById.set(node.id, node);
  }

  async loadDurable(durableId) {
    if (this.loadedNodeIds.includes(durableId)) throw Object.assign(new Error("That node is already directly loaded."), { code: "already_loaded" });
    if (this.loadedNodeIds.length >= MAX_DIRECTLY_LOADED_NODES) throw Object.assign(new Error("Ten nodes are already directly loaded. Reset the context to continue."), { code: "loaded_node_limit" });
    const previouslyFullNodeIds = new Set(this.fullNodeIds);
    const previouslyDirectNodeIds = new Set(this.loadedNodeIds);
    const previousDirectConnectionIds = rawConnectionIds(this.nodesById, previouslyDirectNodeIds, ["task_connections", "active_connections", "fanout_connections"]);
    const previousDirectFanoutIds = rawConnectionIds(this.nodesById, previouslyDirectNodeIds, ["fanout_connections"]);
    const previouslyActiveNodeIds = [...previouslyFullNodeIds].filter(id => !previouslyDirectNodeIds.has(id));
    const previousIndirectFanoutIds = rawConnectionIds(this.nodesById, previouslyActiveNodeIds, ["fanout_connections"]);
    for (const id of [...previousIndirectFanoutIds]) {
      if (previouslyFullNodeIds.has(id) || previousDirectConnectionIds.has(id)) previousIndirectFanoutIds.delete(id);
    }
    const payload = await this.api.context(durableId);
    this.ingestNode(payload.requested_node, true, "direct");
    for (const node of payload.active_connection_nodes) this.ingestNode(node, true, "active");
    this.loadedNodeIds.push(durableId);
    const requestedNodeAlreadyLoaded = previouslyFullNodeIds.has(payload.requested_node.id);
    const newlyFullActiveNodes = payload.active_connection_nodes.filter(node => !previouslyFullNodeIds.has(node.id));
    const currentDirectConnectionIds = rawConnectionIds(this.nodesById, this.loadedNodeIds, ["task_connections", "active_connections", "fanout_connections"]);
    const directFanoutNodes = uniqueRawConnections(
      payload.requested_node.fanout_connections,
      connection => !this.fullNodeIds.has(connection.id) && !previousDirectFanoutIds.has(connection.id),
    );
    const indirectFanoutNodes = uniqueRawConnections(
      newlyFullActiveNodes.flatMap(node => node.fanout_connections || []),
      connection => !this.fullNodeIds.has(connection.id)
        && !currentDirectConnectionIds.has(connection.id)
        && !previousIndirectFanoutIds.has(connection.id),
    );
    return {
      requestedNode: requestedNodeAlreadyLoaded ? null : this.toContextNode(payload.requested_node),
      requestedNodeIdentifier: this.shortId(payload.requested_node.id),
      requestedNodeAlreadyLoaded,
      activeConnectionNodes: newlyFullActiveNodes.map(node => this.toContextNode(node)),
      directFanoutNodes: directFanoutNodes.map(node => this.summary(node)),
      indirectFanoutNodes: indirectFanoutNodes.map(node => this.summary(node)),
    };
  }

  async ensureRootsLoaded() {
    const loads = [];
    for (const rootNodeId of this.rootNodeIds) this.shortId(rootNodeId);
    for (const rootNodeId of this.rootNodeIds) {
      if (!this.loadedNodeIds.includes(rootNodeId)) loads.push(await this.loadDurable(rootNodeId));
    }
    return loads;
  }

  async initialize() {
    this.clear();
    const loads = await this.ensureRootsLoaded();
    return { loads, context: this.snapshot() };
  }

  async reset(durableIds) {
    if (durableIds.some(id => this.rootNodeIds.includes(id))) throw Object.assign(new Error("Root nodes are loaded automatically and must not be listed."), { code: "root_in_reset" });
    if (new Set(durableIds).size !== durableIds.length) throw Object.assign(new Error("Reset identifiers must be distinct."), { code: "duplicate_identifier" });
    if (durableIds.length + this.rootNodeIds.length > MAX_DIRECTLY_LOADED_NODES) throw Object.assign(new Error("Reset would exceed the ten directly loaded node limit."), { code: "loaded_node_limit" });
    this.clear();
    const loads = await this.ensureRootsLoaded();
    for (const id of durableIds) loads.push(await this.loadDurable(id));
    return { loads, context: this.snapshot() };
  }

  refresh(nodes) { for (const node of nodes) this.ingestNode(node, true, "operation"); }

  recordModelAttribution(durableIds, modelAttribution) {
    for (const durableId of durableIds) {
      const node = this.nodesById.get(durableId);
      if (node && this.fullNodeIds.has(durableId)) node.last_modified_by = modelAttribution;
    }
  }

  summary(connection) { return { identifier: this.shortId(connection.id), shortName: connection.short_name, shortDescription: connection.short_description }; }

  toContextNode(node) {
    return {
      identifier: this.shortId(node.id),
      shortName: node.short_name,
      shortDescription: node.short_description,
      longDescription: node.long_description,
      lastModifiedBy: node.last_modified_by || "legacy-unknown",
      taskConnections: (node.task_connections || []).map(c => ({ ...this.summary(c), priority: c.priority })),
      activeConnections: (node.active_connections || []).map(c => this.summary(c)),
      fanoutConnections: (node.fanout_connections || []).map(c => this.summary(c)),
    };
  }

  snapshot() {
    return {
      rootIdentifiers: this.rootNodeIds.map(id => this.shortId(id)),
      directlyLoadedIdentifiers: this.loadedNodeIds.map(id => this.shortId(id)),
      nodes: [...this.fullNodeIds].map(id => ({ ...this.toContextNode(this.nodesById.get(id)), contextSources: [...(this.nodeOrigins.get(id) || [])] })),
    };
  }

  archive() {
    return {
      loadedNodeIds: [...this.loadedNodeIds],
      fullNodeIds: [...this.fullNodeIds],
      nodesById: [...this.nodesById].map(([id, node]) => [id, JSON.parse(JSON.stringify(node))]),
      nodeOrigins: [...this.nodeOrigins].map(([id, origins]) => [id, [...origins]]),
      shortToDurable: [...this.shortToDurable],
      nextShortId: this.nextShortId,
    };
  }

  restore(archive) {
    if (!archive || !Array.isArray(archive.loadedNodeIds) || !Array.isArray(archive.nodesById) || !Array.isArray(archive.shortToDurable)) {
      throw new Error("The saved Kmap context archive is invalid.");
    }
    if (archive.loadedNodeIds.length > MAX_DIRECTLY_LOADED_NODES || new Set(archive.loadedNodeIds).size !== archive.loadedNodeIds.length) {
      throw new Error("The saved Kmap context exceeds the directly loaded node limit or contains duplicates.");
    }
    this.loadedNodeIds = [...archive.loadedNodeIds];
    this.fullNodeIds = new Set(archive.fullNodeIds || []);
    this.nodesById = new Map(archive.nodesById.map(([id, node]) => [id, JSON.parse(JSON.stringify(node))]));
    this.nodeOrigins = new Map((archive.nodeOrigins || []).map(([id, origins]) => [id, new Set(origins)]));
    this.shortToDurable = new Map(archive.shortToDurable);
    this.durableToShort = new Map([...this.shortToDurable].map(([short, durable]) => [durable, short]));
    this.nextShortId = Number.isInteger(archive.nextShortId) ? archive.nextShortId : this.shortToDurable.size + 1;
  }

  diagnostics() {
    return {
      loadedNodeIds: [...this.loadedNodeIds],
      fullNodeIds: [...this.fullNodeIds],
      nodeOrigins: Object.fromEntries([...this.nodeOrigins].map(([id, origins]) => [id, [...origins]])),
      shortToDurable: Object.fromEntries(this.shortToDurable),
      nextShortId: this.nextShortId,
    };
  }
}
