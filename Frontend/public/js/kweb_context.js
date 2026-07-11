export class KwebContext {
  constructor(api, rootNodeId) { this.api = api; this.rootNodeId = rootNodeId; this.clear(); }

  clear() {
    this.loadedNodeIds = [];
    this.fullNodeIds = new Set();
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

  resolve(identifier) {
    if (!Number.isInteger(identifier) || identifier < 1 || !this.shortToDurable.has(identifier)) throw Object.assign(new Error(`Unknown memory identifier ${identifier}.`), { code: "unknown_identifier" });
    return this.shortToDurable.get(identifier);
  }

  ingestNode(node, full = true) {
    this.shortId(node.id);
    for (const connection of [...(node.active_connections || []), ...(node.fanout_connections || [])]) this.shortId(connection.id);
    if (full) { this.nodesById.set(node.id, node); this.fullNodeIds.add(node.id); }
    else if (!this.nodesById.has(node.id)) this.nodesById.set(node.id, node);
  }

  async loadDurable(durableId, { internal = false } = {}) {
    if (!internal && this.loadedNodeIds.length >= 7) throw Object.assign(new Error("Seven nodes are already directly loaded. Reset the context to continue."), { code: "loaded_node_limit" });
    if (this.loadedNodeIds.includes(durableId)) throw Object.assign(new Error("That node is already directly loaded."), { code: "already_loaded" });
    const payload = await this.api.context(durableId);
    this.ingestNode(payload.requested_node, true);
    for (const node of payload.active_connection_nodes) this.ingestNode(node, true);
    this.loadedNodeIds.push(durableId);
    return { requestedNode: this.toContextNode(payload.requested_node), activeConnectionNodes: payload.active_connection_nodes.map(node => this.toContextNode(node)) };
  }

  async initialize() { this.clear(); return this.loadDurable(this.rootNodeId, { internal: true }); }

  async reset(durableIds) {
    if (durableIds.includes(this.rootNodeId)) throw Object.assign(new Error("The root node is loaded automatically and must not be listed."), { code: "root_in_reset" });
    if (new Set(durableIds).size !== durableIds.length) throw Object.assign(new Error("Reset identifiers must be distinct."), { code: "duplicate_identifier" });
    if (durableIds.length + 1 > 7) throw Object.assign(new Error("Reset would exceed the seven directly loaded node limit."), { code: "loaded_node_limit" });
    this.clear();
    const loads = [await this.loadDurable(this.rootNodeId, { internal: true })];
    for (const id of durableIds) loads.push(await this.loadDurable(id, { internal: true }));
    return { loads, context: this.snapshot() };
  }

  refresh(nodes) { for (const node of nodes) this.ingestNode(node, true); }

  summary(connection) { return { identifier: this.shortId(connection.id), shortName: connection.short_name, shortDescription: connection.short_description }; }

  toContextNode(node) {
    return {
      identifier: this.shortId(node.id),
      shortName: node.short_name,
      shortDescription: node.short_description,
      longDescription: node.long_description,
      activeConnections: (node.active_connections || []).map(c => this.summary(c)),
      fanoutConnections: (node.fanout_connections || []).map(c => this.summary(c)),
    };
  }

  snapshot() {
    return {
      directlyLoadedIdentifiers: this.loadedNodeIds.map(id => this.shortId(id)),
      nodes: [...this.fullNodeIds].map(id => this.toContextNode(this.nodesById.get(id))),
    };
  }

  diagnostics() {
    return {
      loadedNodeIds: [...this.loadedNodeIds],
      fullNodeIds: [...this.fullNodeIds],
      shortToDurable: Object.fromEntries(this.shortToDurable),
      nextShortId: this.nextShortId,
    };
  }
}

