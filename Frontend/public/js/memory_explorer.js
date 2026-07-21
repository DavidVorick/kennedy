import { element } from "./render.js?v=20260721.1";
import { hydrateStoredNodeConnections } from "./api.js?v=20260720.2";

export class MemoryExplorer {
  constructor({ api, rootNodeIds, content, backButton, forwardButton }) {
    if (!Array.isArray(rootNodeIds) || rootNodeIds.length !== 2 || rootNodeIds.some(id => typeof id !== "string" || !id) || new Set(rootNodeIds).size !== 2) {
      throw new Error("The memory explorer requires distinct user and Kennedy root node identifiers.");
    }
    this.api = api; [this.userRootNodeId, this.kennedyRootNodeId] = rootNodeIds; this.content = content; this.backButton = backButton; this.forwardButton = forwardButton;
    this.currentNodeId = null; this.back = []; this.forward = [];
  }

  async home() { return this.open(this.userRootNodeId); }
  async kennedyHome() { return this.open(this.kennedyRootNodeId); }
  async open(id, navigation = true) {
    if (navigation && this.currentNodeId && this.currentNodeId !== id) { this.back.push(this.currentNodeId); this.forward = []; }
    this.currentNodeId = id; this.updateButtons();
    this.content.replaceChildren(element("p", "", "Loading memory…"));
    try {
      const [node, history] = await Promise.all([this.api.node(id), this.api.history(id)]);
      this.renderNode(node, history.provenance_ids || []);
    } catch (error) { this.content.replaceChildren(element("p", "error-banner", error.message)); }
  }

  async goBack() { if (!this.back.length) return; this.forward.push(this.currentNodeId); const id = this.back.pop(); this.currentNodeId = null; await this.open(id, false); }
  async goForward() { if (!this.forward.length) return; this.back.push(this.currentNodeId); const id = this.forward.pop(); this.currentNodeId = null; await this.open(id, false); }
  updateButtons() { this.backButton.disabled = !this.back.length; this.forwardButton.disabled = !this.forward.length; }

  renderNode(node, history) {
    const { fixedConnections, recentConnections } = hydrateStoredNodeConnections(node);
    const root = document.createDocumentFragment();
    root.append(
      element("h2", "", node.short_name),
      element("p", "node-description", node.short_description || "No short description."),
      element("p", "node-attribution", `Last modified by: ${node.last_modified_by || "legacy-unknown"}`),
      element("p", "node-attribution", `Last modified at: ${node.last_modified_at || "unknown"}`),
      element("p", "node-attribution", `Owner: ${node.owner_node_id || node.owner_root_node_id || "unowned"}`),
      element("div", "long-description", node.long_description || "No long description."),
    );
    const grid = element("div", "connection-grid");
    grid.append(
      this.connectionList("Fixed connections", fixedConnections, true),
      this.connectionList("Active connections", recentConnections.slice(0, 8)),
      this.connectionList("Fanout connections", recentConnections.slice(8)),
    );
    root.append(grid);
    const historySection = element("section", "history"); historySection.append(element("h3", "", "Source history"));
    if (!history.length) historySection.append(element("p", "", "No history entries."));
    history.forEach((provenanceId, index) => {
      const row = element("div", "history-entry");
      row.append(element("span", "", `Revision ${history.length - index}`));
      const button = element("button", "quiet", "View source"); button.type = "button";
      button.addEventListener("click", () => this.showSource(provenanceId, row)); row.append(button); historySection.append(row);
    });
    root.append(historySection); this.content.replaceChildren(root);
  }

  connectionList(title, connections, showPriority = false) {
    const section = element("section", "connection-list"); section.append(element("h3", "", title));
    if (!connections.length) section.append(element("p", "", "None yet."));
    for (const connection of connections) {
      const button = element("button", "connection"); button.type = "button";
      const slot = connection.slot || ({ high: 1, medium: 2, low: 3 })[connection.priority] || "?";
      const name = connection.short_name || "Unloaded node";
      button.append(element("strong", "", showPriority ? `Slot ${slot} · ${name}` : name), element("small", "", connection.short_description || `ID: ${connection.id}`));
      button.addEventListener("click", () => this.open(connection.id)); section.append(button);
    }
    return section;
  }

  async showSource(provenanceId, row) {
    const existing = row.nextElementSibling;
    if (existing?.classList.contains("source-detail")) { existing.remove(); return; }
    try {
      const source = await this.api.provenance(provenanceId);
      const detail = element("div", "source-detail", `${source.source} · ${source.source_created_at}\n\n${source.data}`);
      row.insertAdjacentElement("afterend", detail);
    } catch (error) { row.insertAdjacentElement("afterend", element("div", "source-detail", error.message)); }
  }
}
