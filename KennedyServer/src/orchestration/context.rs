use std::collections::{HashMap, HashSet};

use anyhow::Context as _;
use kcode_kweb_db::NodeId;
use serde_json::{Value, json};

use super::Api;

struct LoadReceipt {
    requested_identifier: String,
    fixed_identifiers: Vec<String>,
    active_identifiers: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct KmapContext {
    api: Api,
    pub root_node_ids: Vec<String>,
    pub loaded_node_ids: Vec<String>,
    pub fixed_node_ids: Vec<String>,
    pub active_node_ids: Vec<String>,
    pub full_node_ids: HashSet<String>,
    pub nodes_by_id: HashMap<String, Value>,
}

pub(crate) struct FullNodeBox {
    pub identifier: String,
    pub role: &'static str,
    pub node: Value,
}

pub(crate) struct LoadedFanoutBox {
    pub parent_identifier: String,
    pub parent_name: String,
    pub connections: Vec<Value>,
}

pub(crate) struct KmapBoxLayout {
    pub full_nodes: Vec<FullNodeBox>,
    pub loaded_fanouts: Vec<LoadedFanoutBox>,
    pub fixed_neighbors: Vec<Value>,
    pub active_neighbors: Vec<Value>,
    pub connection_fanouts: Vec<Value>,
}

impl KmapContext {
    pub(crate) fn new(api: Api, root_node_ids: Vec<String>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !root_node_ids.is_empty()
                && root_node_ids.iter().all(|id| id.parse::<NodeId>().is_ok())
                && root_node_ids.iter().collect::<HashSet<_>>().len() == root_node_ids.len(),
            "Kmap root node identifiers must be distinct canonical node IDs"
        );
        Ok(Self {
            api,
            root_node_ids,
            loaded_node_ids: Vec::new(),
            fixed_node_ids: Vec::new(),
            active_node_ids: Vec::new(),
            full_node_ids: HashSet::new(),
            nodes_by_id: HashMap::new(),
        })
    }

    #[cfg(test)]
    pub(crate) async fn initialize(&mut self) -> anyhow::Result<()> {
        self.clear();
        self.ensure_roots_loaded().await
    }

    #[cfg(test)]
    pub(crate) async fn ensure_roots_loaded(&mut self) -> anyhow::Result<()> {
        let roots = self.root_node_ids.clone();
        let missing = roots
            .into_iter()
            .filter(|id| !self.loaded_node_ids.contains(id))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            self.load_durable_batch(&missing).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.loaded_node_ids.clear();
        self.fixed_node_ids.clear();
        self.active_node_ids.clear();
        self.full_node_ids.clear();
        self.nodes_by_id.clear();
    }

    pub(crate) fn register_reference(&mut self, durable_id: &str) -> anyhow::Result<String> {
        durable_id
            .parse::<NodeId>()
            .with_context(|| format!("invalid canonical Kmap node ID {durable_id:?}"))?;
        Ok(durable_id.to_owned())
    }

    pub(crate) async fn load_durable(&mut self, durable_id: &str) -> anyhow::Result<Value> {
        self.load_durable_batch(&[durable_id.to_owned()]).await
    }

    pub(crate) async fn load_durable_batch(
        &mut self,
        durable_ids: &[String],
    ) -> anyhow::Result<Value> {
        let before = self.snapshot()?;
        let mut requested_identifiers = Vec::new();
        let mut fixed_identifiers = Vec::new();
        let mut active_identifiers = Vec::new();
        for durable_id in durable_ids {
            let receipt = self.load_durable_one(durable_id).await?;
            requested_identifiers.push(receipt.requested_identifier);
            fixed_identifiers.extend(receipt.fixed_identifiers);
            active_identifiers.extend(receipt.active_identifiers);
        }
        let after = self.snapshot()?;
        Ok(project_load_batch(
            &before,
            &after,
            &requested_identifiers,
            &fixed_identifiers,
            &active_identifiers,
        ))
    }

    async fn load_durable_one(&mut self, durable_id: &str) -> anyhow::Result<LoadReceipt> {
        let previously_full = self.full_node_ids.clone();
        let payload = self.api.kmap_context(durable_id).await?;
        let requested = payload
            .get("requested_node")
            .cloned()
            .context("Kmap context response omitted requested_node")?;
        let requested_id = string_field(&requested, "id")?.to_owned();
        self.ingest_node(requested.clone(), true)?;
        let fixed = payload
            .get("fixed_connection_nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for node in &fixed {
            self.ingest_node(node.clone(), true)?;
        }
        let active = payload
            .get("active_connection_nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for node in &active {
            self.ingest_node(node.clone(), true)?;
        }
        if !self.loaded_node_ids.iter().any(|id| id == durable_id) {
            self.loaded_node_ids.push(durable_id.to_owned());
        }
        self.rebuild_connection_roles();
        let requested_identifier = requested_id;
        let mut fixed_identifiers = Vec::new();
        for node in &fixed {
            let id = string_field(node, "id")?;
            if !previously_full.contains(id) {
                fixed_identifiers.push(id.to_owned());
            }
        }
        let mut active_identifiers = Vec::new();
        for node in &active {
            let id = string_field(node, "id")?;
            if !previously_full.contains(id) {
                active_identifiers.push(id.to_owned());
            }
        }
        Ok(LoadReceipt {
            requested_identifier,
            fixed_identifiers,
            active_identifiers,
        })
    }

    pub(crate) fn refresh(&mut self, nodes: Vec<Value>) -> anyhow::Result<()> {
        for node in nodes {
            self.ingest_node(node, true)?;
        }
        Ok(())
    }

    pub(crate) fn ordered_full_node_ids(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.loaded_node_ids
            .iter()
            .chain(self.fixed_node_ids.iter())
            .chain(self.active_node_ids.iter())
            .filter(|id| self.full_node_ids.contains(*id) && seen.insert((*id).clone()))
            .cloned()
            .collect()
    }

    pub(crate) fn role_for(&self, id: &str) -> Option<&'static str> {
        if self.loaded_node_ids.iter().any(|candidate| candidate == id) {
            Some("direct")
        } else if self.fixed_node_ids.iter().any(|candidate| candidate == id) {
            Some("fixed")
        } else if self.active_node_ids.iter().any(|candidate| candidate == id) {
            Some("active")
        } else {
            None
        }
    }

    pub(crate) fn restore_roles(
        &mut self,
        direct: Vec<String>,
        fixed: Vec<String>,
        active: Vec<String>,
    ) -> anyhow::Result<()> {
        let mut seen = HashSet::new();
        self.loaded_node_ids = direct
            .into_iter()
            .filter(|id| self.nodes_by_id.contains_key(id) && seen.insert(id.clone()))
            .collect();
        self.fixed_node_ids = fixed
            .into_iter()
            .filter(|id| self.nodes_by_id.contains_key(id) && seen.insert(id.clone()))
            .collect();
        self.active_node_ids = active
            .into_iter()
            .filter(|id| self.nodes_by_id.contains_key(id) && seen.insert(id.clone()))
            .collect();
        anyhow::ensure!(
            !self.loaded_node_ids.is_empty(),
            "restored Kweb context has no directly loaded nodes"
        );
        self.full_node_ids = seen;
        Ok(())
    }

    fn rebuild_connection_roles(&mut self) {
        let direct = self.loaded_node_ids.iter().cloned().collect::<HashSet<_>>();
        let mut fixed_seen = direct.clone();
        let mut fixed = Vec::new();
        for identifier in &self.loaded_node_ids {
            let Some(node) = self.nodes_by_id.get(identifier) else {
                continue;
            };
            for connection in stored_fixed_ids(node) {
                if self.nodes_by_id.contains_key(&connection)
                    && fixed_seen.insert(connection.clone())
                {
                    fixed.push(connection);
                }
            }
        }
        let mut active_seen = fixed_seen;
        let mut active = Vec::new();
        for identifier in &self.loaded_node_ids {
            let Some(node) = self.nodes_by_id.get(identifier) else {
                continue;
            };
            for connection in stored_active_ids(node) {
                if self.nodes_by_id.contains_key(&connection)
                    && active_seen.insert(connection.clone())
                {
                    active.push(connection);
                }
            }
        }
        self.fixed_node_ids = fixed;
        self.active_node_ids = active;
        self.full_node_ids = direct
            .into_iter()
            .chain(self.fixed_node_ids.iter().cloned())
            .chain(self.active_node_ids.iter().cloned())
            .collect();
    }

    pub(crate) fn box_layout(&self) -> anyhow::Result<KmapBoxLayout> {
        let full_nodes = self
            .ordered_full_node_ids()
            .into_iter()
            .map(|identifier| {
                let stored = self
                    .nodes_by_id
                    .get(&identifier)
                    .with_context(|| format!("missing full Kmap node {identifier}"))?;
                Ok(FullNodeBox {
                    role: self.role_for(&identifier).unwrap_or("active"),
                    identifier,
                    node: self.context_node(stored)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let full_ids = full_nodes
            .iter()
            .map(|entry| entry.identifier.clone())
            .collect::<HashSet<_>>();
        let projected = full_nodes
            .iter()
            .map(|entry| (entry.identifier.as_str(), &entry.node))
            .collect::<HashMap<_, _>>();
        let mut seen = full_ids;
        let mut loaded_fanouts = Vec::with_capacity(self.loaded_node_ids.len());
        for parent_identifier in &self.loaded_node_ids {
            let parent = projected
                .get(parent_identifier.as_str())
                .with_context(|| format!("missing loaded Kmap node {parent_identifier}"))?;
            loaded_fanouts.push(LoadedFanoutBox {
                parent_identifier: parent_identifier.clone(),
                parent_name: text(parent.get("shortName"), "(none)"),
                connections: take_unique_connections(
                    parent.get("fanoutConnections"),
                    &mut seen,
                    true,
                ),
            });
        }
        let mut fixed_neighbors = Vec::new();
        for identifier in self
            .fixed_node_ids
            .iter()
            .filter(|identifier| self.role_for(identifier) == Some("fixed"))
        {
            let node = projected
                .get(identifier.as_str())
                .with_context(|| format!("missing fixed Kmap node {identifier}"))?;
            fixed_neighbors.extend(take_unique_connections(
                node.get("fixedConnections"),
                &mut seen,
                true,
            ));
            fixed_neighbors.extend(take_unique_connections(
                node.get("activeConnections"),
                &mut seen,
                true,
            ));
        }
        let mut active_neighbors = Vec::new();
        for identifier in self
            .active_node_ids
            .iter()
            .filter(|identifier| self.role_for(identifier) == Some("active"))
        {
            let node = projected
                .get(identifier.as_str())
                .with_context(|| format!("missing active Kmap node {identifier}"))?;
            active_neighbors.extend(take_unique_connections(
                node.get("fixedConnections"),
                &mut seen,
                true,
            ));
            active_neighbors.extend(take_unique_connections(
                node.get("activeConnections"),
                &mut seen,
                true,
            ));
        }
        let mut connection_fanouts = Vec::new();
        for entry in full_nodes
            .iter()
            .filter(|entry| matches!(entry.role, "fixed" | "active"))
        {
            connection_fanouts.extend(take_unique_connections(
                entry.node.get("fanoutConnections"),
                &mut seen,
                false,
            ));
        }
        Ok(KmapBoxLayout {
            full_nodes,
            loaded_fanouts,
            fixed_neighbors,
            active_neighbors,
            connection_fanouts,
        })
    }

    pub(crate) fn snapshot(&self) -> anyhow::Result<Value> {
        let roots = self.root_node_ids.clone();
        let loaded = self.loaded_node_ids.clone();
        let mut nodes = Vec::new();
        for id in self.ordered_full_node_ids() {
            let node = self
                .nodes_by_id
                .get(&id)
                .cloned()
                .with_context(|| format!("missing full Kmap node {id}"))?;
            let mut projected = self.context_node(&node)?;
            projected["contextSources"] = json!([self.role_for(&id).unwrap_or("operation")]);
            nodes.push(projected);
        }
        Ok(json!({
            "rootIdentifiers": roots,
            "directlyLoadedIdentifiers": loaded,
            "nodes": nodes,
        }))
    }

    pub(crate) fn context_node(&self, node: &Value) -> anyhow::Result<Value> {
        let id = string_field(node, "id")?;
        let owner = node
            .get("owner_root_node_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Ok(json!({
            "identifier": id,
            "shortName": node.get("short_name").and_then(Value::as_str).unwrap_or(""),
            "shortDescription": node.get("short_description").and_then(Value::as_str).unwrap_or(""),
            "longDescription": node.get("long_description").and_then(Value::as_str).unwrap_or(""),
            "lastModifiedBy": node.get("last_modified_by").and_then(Value::as_str).unwrap_or("legacy-unknown"),
            "lastModifiedAt": node.get("last_modified_at").cloned().unwrap_or(Value::Null),
            "ownerIdentifier": owner.unwrap_or_else(|| "unowned".into()),
            "fixedConnections": self.connection_summaries(node.get("fixed_connections"), true),
            "activeConnections": self.connection_summaries(node.get("active_connections"), false),
            "fanoutConnections": self.connection_summaries(node.get("fanout_connections"), false),
        }))
    }

    pub(crate) fn stored_node(&self, id: &str) -> anyhow::Result<Value> {
        self.nodes_by_id
            .get(id)
            .cloned()
            .with_context(|| format!("Kmap context does not contain node {id}"))
    }

    fn ingest_node(&mut self, node: Value, full: bool) -> anyhow::Result<()> {
        let id = string_field(&node, "id")?.to_owned();
        self.register_reference(&id)?;
        if full {
            self.nodes_by_id.insert(id.clone(), node);
            self.full_node_ids.insert(id.clone());
        } else {
            self.nodes_by_id.entry(id).or_insert(node);
        }
        Ok(())
    }

    fn connection_summaries(&self, value: Option<&Value>, fixed: bool) -> Vec<Value> {
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
            .filter_map(|(index, connection)| {
                let id = connection.get("id").and_then(Value::as_str)?;
                let full_name = self
                    .nodes_by_id
                    .get(id)
                    .and_then(|node| node.get("short_name"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let full_description = self
                    .nodes_by_id
                    .get(id)
                    .and_then(|node| node.get("short_description"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let mut value = json!({
                    "identifier": id,
                    "shortName": connection.get("short_name").and_then(Value::as_str)
                        .or(full_name.as_deref())
                        .unwrap_or("Unloaded node"),
                    "shortDescription": connection.get("short_description").and_then(Value::as_str)
                        .or(full_description.as_deref())
                        .unwrap_or(""),
                });
                if fixed {
                    value["slot"] = json!(
                        connection
                            .get("slot")
                            .and_then(Value::as_u64)
                            .unwrap_or(index as u64 + 1)
                    );
                }
                Some(value)
            })
            .collect()
    }
}

fn take_unique_connections(
    value: Option<&Value>,
    seen: &mut HashSet<String>,
    include_summary: bool,
) -> Vec<Value> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|connection| {
            let identifier = connection.get("identifier")?.as_str()?.to_owned();
            if !seen.insert(identifier.clone()) {
                return None;
            }
            let mut projected = json!({
                "identifier":identifier,
                "shortName":connection.get("shortName").and_then(Value::as_str).unwrap_or("Unloaded node"),
            });
            if include_summary {
                projected["shortDescription"] = json!(
                    connection
                        .get("shortDescription")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                );
            }
            Some(projected)
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn format_kmap_context(snapshot: &Value) -> String {
    format_projected_kmap_context(snapshot, &project_snapshot(snapshot))
}

#[cfg(test)]
pub(crate) fn format_projected_kmap_context(snapshot: &Value, projection: &Value) -> String {
    let roots = identifier_list(snapshot.get("rootIdentifiers"));
    let loaded = identifier_list(snapshot.get("directlyLoadedIdentifiers"));
    let nodes = format_compact_memory_sections(projection);
    format!(
        "Current Kmap context\n\nAlways-loaded root identifiers: {roots}\nDirectly loaded memory identifiers: {loaded}\n\n{}",
        if nodes.is_empty() {
            "No memory nodes are currently loaded.".to_owned()
        } else {
            nodes
        }
    )
}

pub(crate) fn project_load_batch(
    before: &Value,
    after: &Value,
    requested_identifiers: &[String],
    fixed_identifiers: &[String],
    active_identifiers: &[String],
) -> Value {
    let before_projection = project_snapshot(before);
    let before_roles = projection_roles(&before_projection);
    let after_nodes = nodes_by_identifier(after);

    let mut direct_nodes = Vec::new();
    let mut direct_node_promotions = Vec::new();
    let mut seen_direct = HashSet::new();
    for identifier in requested_identifiers {
        if !seen_direct.insert(identifier.clone()) {
            continue;
        }
        let previous_role = before_roles.get(identifier).copied().unwrap_or_default();
        if previous_role < 3
            && let Some(node) = after_nodes.get(identifier)
        {
            direct_nodes.push((*node).clone());
        } else if matches!(previous_role, 3 | 4) {
            direct_node_promotions.push(identifier.clone());
        }
    }

    let direct_identifiers = requested_identifiers
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let mut fixed_nodes = Vec::new();
    let mut fixed_node_promotions = Vec::new();
    let mut seen_fixed = HashSet::new();
    for identifier in fixed_identifiers {
        if direct_identifiers.contains(identifier) || !seen_fixed.insert(identifier.clone()) {
            continue;
        }
        let previous_role = before_roles.get(identifier).copied().unwrap_or_default();
        if previous_role < 3
            && let Some(node) = after_nodes.get(identifier)
        {
            fixed_nodes.push((*node).clone());
        } else if previous_role == 3 {
            fixed_node_promotions.push(identifier.clone());
        }
    }
    let fixed_identifiers = fixed_identifiers.iter().cloned().collect::<HashSet<_>>();
    let mut active_nodes = Vec::new();
    let mut seen_active = HashSet::new();
    for identifier in active_identifiers {
        if !direct_identifiers.contains(identifier)
            && !fixed_identifiers.contains(identifier)
            && seen_active.insert(identifier.clone())
            && before_roles.get(identifier).copied().unwrap_or_default() < 3
            && let Some(node) = after_nodes.get(identifier)
        {
            active_nodes.push((*node).clone());
        }
    }

    let full_identifiers = full_identifiers(after);
    let batch_direct = requested_identifiers
        .iter()
        .filter_map(|identifier| after_nodes.get(identifier).copied())
        .collect::<Vec<_>>();
    let batch_active = active_identifiers
        .iter()
        .filter(|identifier| !direct_identifiers.contains(identifier.as_str()))
        .filter_map(|identifier| after_nodes.get(identifier).copied())
        .collect::<Vec<_>>();
    let direct_fanout_candidates =
        unique_connections(&batch_direct, "fanoutConnections", &full_identifiers);
    let direct_fanout_identifiers = direct_fanout_candidates
        .iter()
        .filter_map(|node| node_identifier(node).map(str::to_owned))
        .collect::<HashSet<_>>();
    let mut indirect_excluded = full_identifiers;
    indirect_excluded.extend(direct_fanout_identifiers);
    let indirect_fanout_candidates =
        unique_connections(&batch_active, "fanoutConnections", &indirect_excluded);
    let direct_fanout_nodes = direct_fanout_candidates
        .into_iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| {
                before_roles.get(identifier).copied().unwrap_or_default() < 2
            })
        })
        .collect::<Vec<_>>();
    let indirect_fanout_nodes = indirect_fanout_candidates
        .into_iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| {
                before_roles.get(identifier).copied().unwrap_or_default() < 1
            })
        })
        .collect::<Vec<_>>();

    json!({
        "directNodes": direct_nodes,
        "directNodePromotions": direct_node_promotions,
        "fixedConnectionNodes": fixed_nodes,
        "fixedNodePromotions": fixed_node_promotions,
        "activeConnectionNodes": active_nodes,
        "directFanoutNodes": direct_fanout_nodes,
        "indirectFanoutNodes": indirect_fanout_nodes,
    })
}

#[cfg(test)]
pub(crate) fn format_compact_memory_sections(projection: &Value) -> String {
    let direct_nodes = array(projection.get("directNodes"));
    let direct_promotions = projection
        .get("directNodePromotions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let fixed_nodes = array(projection.get("fixedConnectionNodes"));
    let fixed_promotions = projection
        .get("fixedNodePromotions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let active_nodes = array(projection.get("activeConnectionNodes"));
    let direct_fanout = array(projection.get("directFanoutNodes"));
    let indirect_fanout = array(projection.get("indirectFanoutNodes"));
    let mut sections = Vec::new();
    if !direct_nodes.is_empty() {
        sections.push(format!(
            "Directly loaded nodes\n\n{}",
            direct_nodes
                .iter()
                .map(|node| format_context_node(node, true))
                .collect::<Vec<_>>()
                .join("\n\n")
        ));
    }
    if !direct_promotions.is_empty() {
        sections.push(format!(
            "Now directly loaded; full text already present: {}",
            direct_promotions.join(", ")
        ));
    }
    if !fixed_nodes.is_empty() {
        sections.push(format!(
            "Full fixed-connection nodes\n\n{}",
            fixed_nodes
                .iter()
                .map(|node| format_context_node(node, true))
                .collect::<Vec<_>>()
                .join("\n\n")
        ));
    }
    if !fixed_promotions.is_empty() {
        sections.push(format!(
            "Now fixed connections; full text already present: {}",
            fixed_promotions.join(", ")
        ));
    }
    if !active_nodes.is_empty() {
        sections.push(format!(
            "Full active-connection nodes\n\n{}",
            active_nodes
                .iter()
                .map(|node| format_context_node(node, false))
                .collect::<Vec<_>>()
                .join("\n\n")
        ));
    }
    if !direct_fanout.is_empty() {
        sections.push(format!(
            "Fanout nodes of directly loaded nodes\n\n{}",
            direct_fanout
                .iter()
                .map(|node| format!(
                    "{}: {}\n  Summary: {}",
                    node.get("identifier")
                        .and_then(Value::as_str)
                        .unwrap_or("invalid"),
                    text(node.get("shortName"), "(none)"),
                    text(node.get("shortDescription"), "(none)")
                ))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    if !indirect_fanout.is_empty() {
        sections.push(format!(
            "Fanout nodes only of full active-connection nodes\n\n{}",
            indirect_fanout
                .iter()
                .map(|node| format!(
                    "{}: {}",
                    node.get("identifier")
                        .and_then(Value::as_str)
                        .unwrap_or("invalid"),
                    text(node.get("shortName"), "(none)")
                ))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    sections.join("\n\n")
}

fn project_snapshot(snapshot: &Value) -> Value {
    let direct = snapshot
        .get("directlyLoadedIdentifiers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<HashSet<_>>();
    let node_map = nodes_by_identifier(snapshot);
    let mut direct_nodes = Vec::new();
    for identifier in snapshot
        .get("directlyLoadedIdentifiers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        if let Some(node) = node_map.get(identifier) {
            direct_nodes.push((*node).clone());
        }
    }
    let fixed_identifiers = array(snapshot.get("nodes"))
        .iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| !direct.contains(identifier))
                && node
                    .get("contextSources")
                    .and_then(Value::as_array)
                    .is_some_and(|sources| {
                        sources
                            .iter()
                            .any(|source| source.as_str() == Some("fixed"))
                    })
        })
        .filter_map(|node| node_identifier(node).map(str::to_owned))
        .collect::<HashSet<_>>();
    let active_nodes = array(snapshot.get("nodes"))
        .iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| !direct.contains(identifier))
                && node_identifier(node)
                    .is_some_and(|identifier| !fixed_identifiers.contains(identifier))
        })
        .cloned()
        .collect::<Vec<_>>();
    let fixed_nodes = array(snapshot.get("nodes"))
        .iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| fixed_identifiers.contains(identifier))
        })
        .cloned()
        .collect::<Vec<_>>();
    let full = full_identifiers(snapshot);
    let direct_refs = direct_nodes.iter().collect::<Vec<_>>();
    let direct_fanout_nodes = unique_connections(&direct_refs, "fanoutConnections", &full);
    let mut indirect_excluded = full;
    indirect_excluded.extend(
        direct_fanout_nodes
            .iter()
            .filter_map(|node| node_identifier(node).map(str::to_owned)),
    );
    let active_refs = active_nodes.iter().collect::<Vec<_>>();
    let indirect_fanout_nodes =
        unique_connections(&active_refs, "fanoutConnections", &indirect_excluded);
    json!({
        "directNodes": direct_nodes,
        "directNodePromotions": [],
        "fixedConnectionNodes": fixed_nodes,
        "fixedNodePromotions": [],
        "activeConnectionNodes": active_nodes,
        "directFanoutNodes": direct_fanout_nodes,
        "indirectFanoutNodes": indirect_fanout_nodes,
    })
}

fn nodes_by_identifier(snapshot: &Value) -> HashMap<String, &Value> {
    array(snapshot.get("nodes"))
        .iter()
        .filter_map(|node| Some((node_identifier(node)?.to_owned(), node)))
        .collect()
}

fn full_identifiers(snapshot: &Value) -> HashSet<String> {
    array(snapshot.get("nodes"))
        .iter()
        .filter_map(|node| node_identifier(node).map(str::to_owned))
        .collect()
}

fn projection_roles(projection: &Value) -> HashMap<String, u8> {
    let mut roles = HashMap::new();
    for (key, role) in [
        ("indirectFanoutNodes", 1),
        ("directFanoutNodes", 2),
        ("activeConnectionNodes", 3),
        ("fixedConnectionNodes", 4),
        ("directNodes", 5),
    ] {
        for node in array(projection.get(key)) {
            if let Some(identifier) = node_identifier(node) {
                roles.insert(identifier.to_owned(), role);
            }
        }
    }
    roles
}

fn unique_connections(nodes: &[&Value], field: &str, excluded: &HashSet<String>) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for node in nodes {
        for connection in array(node.get(field)) {
            let Some(identifier) = node_identifier(connection) else {
                continue;
            };
            if !excluded.contains(identifier) && seen.insert(identifier.to_owned()) {
                result.push(connection.clone());
            }
        }
    }
    result
}

fn node_identifier(node: &Value) -> Option<&str> {
    node.get("identifier").and_then(Value::as_str)
}

fn array(value: Option<&Value>) -> &[Value] {
    value
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

pub(crate) fn format_context_node(node: &Value, _include_summary: bool) -> String {
    [
        format!(
            "Node ID: {}",
            node.get("identifier")
                .and_then(Value::as_str)
                .unwrap_or("invalid")
        ),
        format!("Node name: {}", text(node.get("shortName"), "(none)")),
        format!(
            "Node summary: {}",
            text(node.get("shortDescription"), "(none)")
        ),
        "Node long description:".into(),
        indent(&text(node.get("longDescription"), "(none)")),
        format!(
            "Fixed connection IDs: {}",
            connection_ids(node.get("fixedConnections"), false)
        ),
        format!(
            "Active connection IDs: {}",
            connection_ids(node.get("activeConnections"), false)
        ),
        format!(
            "Fanout connection IDs: {}",
            connection_ids(node.get("fanoutConnections"), false)
        ),
    ]
    .join("\n")
}

pub(crate) fn stored_fixed_ids(node: &Value) -> Vec<String> {
    stored_connection_ids(node, "fixed_connections")
}

pub(crate) fn stored_recent_ids(node: &Value) -> Vec<String> {
    let mut values = stored_connection_ids(node, "active_connections");
    values.extend(stored_connection_ids(node, "fanout_connections"));
    if values.is_empty() {
        values = stored_connection_ids(node, "recent_connections");
    }
    values
}

pub(crate) fn stored_active_ids(node: &Value) -> Vec<String> {
    let values = stored_connection_ids(node, "active_connections");
    if values.is_empty() {
        stored_connection_ids(node, "recent_connections")
            .into_iter()
            .take(8)
            .collect()
    } else {
        values
    }
}

fn stored_connection_ids(node: &Value, field: &str) -> Vec<String> {
    node.get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            entry
                .as_str()
                .or_else(|| entry.get("id").and_then(Value::as_str))
                .map(str::to_owned)
        })
        .collect()
}

fn string_field<'a>(value: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("Kmap node is missing {key}"))
}

#[cfg(test)]
fn identifier_list(value: Option<&Value>) -> String {
    let values = value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if values.is_empty() {
        "none".into()
    } else {
        values.join(", ")
    }
}

fn connection_ids(value: Option<&Value>, fixed: bool) -> String {
    let values = value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|connection| {
            let identifier = connection.get("identifier").and_then(Value::as_str)?;
            Some(if fixed {
                format!(
                    "slot {}: {identifier}",
                    connection
                        .get("slot")
                        .and_then(Value::as_u64)
                        .unwrap_or_default()
                )
            } else {
                identifier.to_owned()
            })
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        "none".into()
    } else {
        values.join(", ")
    }
}

fn text(value: Option<&Value>, fallback: &str) -> String {
    match value {
        Some(Value::String(value)) if !value.is_empty() => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        _ => fallback.to_owned(),
    }
}

fn indent(value: &str) -> String {
    value
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::Path, routing::get};

    fn position(text: &str, needle: &str) -> usize {
        text.find(needle)
            .unwrap_or_else(|| panic!("missing {needle:?} in:\n{text}"))
    }

    fn occurrences(text: &str, needle: &str) -> usize {
        text.match_indices(needle).count()
    }

    fn nid(value: u8) -> String {
        NodeId::from_bytes([0, 0, 0, 0, 0, value])
            .unwrap()
            .to_string()
    }

    fn canonical_ids(mut value: Value) -> Value {
        fn visit(value: &mut Value, key: Option<&str>) {
            match value {
                Value::Number(number) if matches!(key, Some("identifier" | "ownerIdentifier")) => {
                    *value = json!(nid(number.as_u64().unwrap() as u8));
                }
                Value::Array(values) => {
                    for value in values {
                        if matches!(key, Some("rootIdentifiers" | "directlyLoadedIdentifiers"))
                            && value.is_number()
                        {
                            *value = json!(nid(value.as_u64().unwrap() as u8));
                        } else {
                            visit(value, key);
                        }
                    }
                }
                Value::Object(values) => {
                    for (key, value) in values {
                        visit(value, Some(key));
                    }
                }
                _ => {}
            }
        }
        visit(&mut value, None);
        value
    }

    #[test]
    fn compact_context_keeps_root_and_connection_identifiers() {
        let rendered = format_kmap_context(&canonical_ids(json!({
            "rootIdentifiers": [1, 2],
            "directlyLoadedIdentifiers": [1],
            "nodes": [{
                "identifier": 1,
                "shortName": "Root",
                "shortDescription": "A root",
                "longDescription": "Details",
                "ownerIdentifier": "unowned",
                "activeConnections": [{"identifier": 3}],
                "fixedConnections": [],
                "fanoutConnections": [],
            }],
        })));
        assert!(rendered.contains("Always-loaded root identifiers: AAAAAAAB, AAAAAAAC"));
        assert!(rendered.contains("Active connection IDs: AAAAAAAD"));
    }

    #[test]
    fn full_node_box_contains_exactly_the_requested_node_fields() {
        let rendered = format_context_node(
            &canonical_ids(json!({
                "identifier": 1,
                "shortName": "Node name",
                "shortDescription": "Node summary",
                "longDescription": "Node details",
                "ownerIdentifier": 2,
                "lastModifiedBy": "writer",
                "lastModifiedAt": "today",
                "fixedConnections": [{"identifier": 3, "slot": 1}],
                "activeConnections": [{"identifier": 4}],
                "fanoutConnections": [{"identifier": 5}],
            })),
            true,
        );
        assert_eq!(
            rendered,
            concat!(
                "Node ID: AAAAAAAB\n",
                "Node name: Node name\n",
                "Node summary: Node summary\n",
                "Node long description:\n",
                "  Node details\n",
                "Fixed connection IDs: AAAAAAAD\n",
                "Active connection IDs: AAAAAAAE\n",
                "Fanout connection IDs: AAAAAAAF"
            )
        );
        assert!(!rendered.contains("owner"));
        assert!(!rendered.contains("writer"));
        assert!(!rendered.contains("today"));
    }

    #[test]
    fn box_layout_uses_role_precedence_and_one_global_summary_deduplication_pass() {
        fn connection(id: &str) -> Value {
            json!({
                "id":id,
                "short_name":format!("Name {id}"),
                "short_description":format!("Summary {id}"),
            })
        }
        fn node(id: &str, fixed: &[&str], active: &[&str], fanout: &[&str]) -> Value {
            json!({
                "id":id,
                "short_name":format!("Name {id}"),
                "short_description":format!("Summary {id}"),
                "long_description":format!("Long {id}"),
                "fixed_connections":fixed.iter().map(|id| connection(id)).collect::<Vec<_>>(),
                "active_connections":active.iter().map(|id| connection(id)).collect::<Vec<_>>(),
                "fanout_connections":fanout.iter().map(|id| connection(id)).collect::<Vec<_>>(),
            })
        }
        let api = Api::new(&super::super::Config {
            system_prompts_directory: std::path::PathBuf::new(),
            kweb_base: "http://127.0.0.1:1".into(),
            intelligence_base: "http://127.0.0.1:1".into(),
            session_history_base: "http://127.0.0.1:1".into(),
            telegram_relay_base: "http://127.0.0.1:1".into(),
            telegram_max_media_bytes: 1024,
            telegram_web_user_handle: "@test".into(),
            runtime_model: super::super::RuntimeModel::testing(),
        })
        .unwrap();
        let direct_a = nid(1);
        let direct_b = nid(2);
        let fixed = nid(3);
        let active = nid(4);
        let mut context = KmapContext::new(api, vec![direct_a.clone()]).unwrap();
        context.nodes_by_id = [
            (
                direct_a.clone(),
                node(
                    &direct_a,
                    &[&fixed],
                    &[&active],
                    &[&active, &nid(5), &nid(6)],
                ),
            ),
            (
                direct_b.clone(),
                node(&direct_b, &[], &[], &[&nid(6), &nid(7)]),
            ),
            (
                fixed.clone(),
                node(&fixed, &[&nid(8)], &[&nid(9)], &[&nid(11)]),
            ),
            (
                active.clone(),
                node(&active, &[&nid(9)], &[&nid(10)], &[&nid(11), &nid(12)]),
            ),
        ]
        .into_iter()
        .collect();
        context
            .restore_roles(
                vec![direct_a.clone(), direct_b.clone()],
                vec![fixed.clone()],
                vec![active.clone()],
            )
            .unwrap();
        let layout = context.box_layout().unwrap();
        assert_eq!(
            layout
                .full_nodes
                .iter()
                .map(|entry| (entry.identifier.clone(), entry.role))
                .collect::<Vec<_>>(),
            vec![
                (direct_a, "direct"),
                (direct_b, "direct"),
                (fixed, "fixed"),
                (active, "active"),
            ]
        );
        let ids = |values: &[Value]| {
            values
                .iter()
                .filter_map(|value| value["identifier"].as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(&layout.loaded_fanouts[0].connections),
            vec![nid(5), nid(6)]
        );
        assert_eq!(ids(&layout.loaded_fanouts[1].connections), vec![nid(7)]);
        assert_eq!(ids(&layout.fixed_neighbors), vec![nid(8), nid(9)]);
        assert_eq!(ids(&layout.active_neighbors), vec![nid(10)]);
        assert_eq!(ids(&layout.connection_fanouts), vec![nid(11), nid(12)]);
    }

    #[test]
    fn complete_context_is_classified_globally_before_rendering() {
        let rendered = format_kmap_context(&canonical_ids(json!({
            "rootIdentifiers": [1],
            "directlyLoadedIdentifiers": [1, 3],
            "nodes": [
                {
                    "identifier": 1,
                    "shortName": "Direct One",
                    "shortDescription": "Direct one summary",
                    "longDescription": "Direct one details",
                    "ownerIdentifier": 1,
                    "fixedConnections": [],
                    "activeConnections": [{"identifier": 2, "shortName": "Active Two", "shortDescription": "Active two summary"}],
                    "fanoutConnections": [
                        {"identifier": 4, "shortName": "Direct Fanout Four", "shortDescription": "Four summary"},
                        {"identifier": 2, "shortName": "Active Two", "shortDescription": "Must not become a fanout"}
                    ]
                },
                {
                    "identifier": 2,
                    "shortName": "Active Two",
                    "shortDescription": "ACTIVE SUMMARY MUST BE OMITTED",
                    "longDescription": "Active two details",
                    "ownerIdentifier": 1,
                    "fixedConnections": [],
                    "activeConnections": [],
                    "fanoutConnections": [
                        {"identifier": 4, "shortName": "Direct Fanout Four", "shortDescription": "Four summary"},
                        {"identifier": 5, "shortName": "Indirect Fanout Five", "shortDescription": "INDIRECT SUMMARY MUST BE OMITTED"},
                        {"identifier": 3, "shortName": "Direct Three", "shortDescription": "Must not become a fanout"}
                    ]
                },
                {
                    "identifier": 3,
                    "shortName": "Direct Three",
                    "shortDescription": "Direct three summary",
                    "longDescription": "Direct three details",
                    "ownerIdentifier": 1,
                    "fixedConnections": [],
                    "activeConnections": [{"identifier": 2, "shortName": "Active Two"}],
                    "fanoutConnections": [{"identifier": 6, "shortName": "Direct Fanout Six", "shortDescription": "Six summary"}]
                }
            ]
        })));

        let direct_heading = position(&rendered, "Directly loaded nodes");
        let active_heading = position(&rendered, "Full active-connection nodes");
        let direct_fanout_heading = position(&rendered, "Fanout nodes of directly loaded nodes");
        let indirect_heading = position(
            &rendered,
            "Fanout nodes only of full active-connection nodes",
        );
        assert!(direct_heading < active_heading);
        assert!(active_heading < direct_fanout_heading);
        assert!(direct_fanout_heading < indirect_heading);
        assert!(
            position(&rendered, "Node ID: AAAAAAAB") < position(&rendered, "Node ID: AAAAAAAD")
        );
        assert!(
            position(&rendered, "Node ID: AAAAAAAD") < position(&rendered, "Node ID: AAAAAAAC")
        );
        assert_eq!(occurrences(&rendered, "Node ID: AAAAAAAC"), 1);
        assert_eq!(occurrences(&rendered, "Node ID: AAAAAAAD"), 1);
        assert_eq!(occurrences(&rendered, "AAAAAAAE: Direct Fanout Four"), 1);
        assert!(rendered.contains("AAAAAAAG: Direct Fanout Six\n  Summary: Six summary"));
        assert!(rendered.contains("AAAAAAAF: Indirect Fanout Five"));
        assert!(rendered.contains("ACTIVE SUMMARY MUST BE OMITTED"));
        assert!(!rendered.contains("INDIRECT SUMMARY MUST BE OMITTED"));
        assert!(!rendered.contains('{'));
    }

    #[test]
    fn load_batch_upgrades_then_renders_each_final_role_once() {
        let before = canonical_ids(json!({
            "directlyLoadedIdentifiers": [1],
            "nodes": [
                {
                    "identifier": 1, "shortName": "Root", "shortDescription": "Root summary", "longDescription": "Root details",
                    "fixedConnections": [], "activeConnections": [{"identifier": 2}],
                    "fanoutConnections": [{"identifier": 5, "shortName": "Known Direct Fanout", "shortDescription": "Known summary"}]
                },
                {
                    "identifier": 2, "shortName": "Previously Active", "shortDescription": "Hidden active summary", "longDescription": "Previously active details",
                    "fixedConnections": [], "activeConnections": [],
                    "fanoutConnections": [{"identifier": 6, "shortName": "Previously Indirect", "shortDescription": "Previously hidden summary"}]
                }
            ]
        }));
        let after = canonical_ids(json!({
            "directlyLoadedIdentifiers": [1, 3, 2],
            "nodes": [
                {
                    "identifier": 1, "shortName": "Root", "shortDescription": "Root summary", "longDescription": "Root details",
                    "fixedConnections": [], "activeConnections": [{"identifier": 2}],
                    "fanoutConnections": [{"identifier": 5, "shortName": "Known Direct Fanout", "shortDescription": "Known summary"}]
                },
                {
                    "identifier": 2, "shortName": "Previously Active", "shortDescription": "Now directly visible", "longDescription": "Previously active details",
                    "fixedConnections": [], "activeConnections": [],
                    "fanoutConnections": [{"identifier": 6, "shortName": "Previously Indirect", "shortDescription": "Now direct summary"}]
                },
                {
                    "identifier": 3, "shortName": "New Direct", "shortDescription": "New direct summary", "longDescription": "New direct details",
                    "fixedConnections": [], "activeConnections": [{"identifier": 4}],
                    "fanoutConnections": [
                        {"identifier": 6, "shortName": "Previously Indirect", "shortDescription": "Now direct summary"},
                        {"identifier": 8, "shortName": "New Direct Fanout", "shortDescription": "Eight summary"}
                    ]
                },
                {
                    "identifier": 4, "shortName": "New Active", "shortDescription": "Active summary omitted", "longDescription": "New active details",
                    "fixedConnections": [], "activeConnections": [],
                    "fanoutConnections": [
                        {"identifier": 8, "shortName": "New Direct Fanout", "shortDescription": "Eight summary"},
                        {"identifier": 9, "shortName": "New Indirect", "shortDescription": "Nine summary omitted"}
                    ]
                }
            ]
        }));

        let projection = project_load_batch(
            &before,
            &after,
            &[nid(3), nid(2)],
            &[],
            &[nid(2), nid(4), nid(3)],
        );
        let rendered = format_compact_memory_sections(&projection);
        assert_eq!(
            array(projection.get("directNodes"))
                .iter()
                .filter_map(node_identifier)
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            vec![nid(3)]
        );
        assert_eq!(
            projection.get("directNodePromotions"),
            Some(&json!([nid(2)]))
        );
        assert_eq!(
            array(projection.get("activeConnectionNodes"))
                .iter()
                .filter_map(node_identifier)
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            vec![nid(4)]
        );
        assert_eq!(
            array(projection.get("directFanoutNodes"))
                .iter()
                .filter_map(node_identifier)
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            vec![nid(6), nid(8)]
        );
        assert_eq!(
            array(projection.get("indirectFanoutNodes"))
                .iter()
                .filter_map(node_identifier)
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            vec![nid(9)]
        );
        assert!(
            position(&rendered, "Node ID: AAAAAAAD")
                < position(
                    &rendered,
                    "Now directly loaded; full text already present: AAAAAAAC"
                )
        );
        assert!(
            position(
                &rendered,
                "Now directly loaded; full text already present: AAAAAAAC"
            ) < position(&rendered, "Node ID: AAAAAAAE")
        );
        assert!(
            position(&rendered, "Node ID: AAAAAAAE")
                < position(&rendered, "AAAAAAAG: Previously Indirect")
        );
        assert!(
            position(&rendered, "AAAAAAAG: Previously Indirect")
                < position(&rendered, "AAAAAAAJ: New Indirect")
        );
        assert_eq!(occurrences(&rendered, "AAAAAAAI: New Direct Fanout"), 1);
        assert!(!rendered.contains("Node ID: AAAAAAAC"));
        assert!(!rendered.contains("Previously active details"));
        assert!(!rendered.contains("Known Direct Fanout"));
        assert!(!rendered.contains("Nine summary omitted"));
    }

    #[tokio::test]
    async fn repeated_loads_refresh_nodes_without_reset_context() {
        let root_id = nid(1);
        let node_a_id = nid(2);
        let node_d_id = nid(4);
        let app = Router::new().route(
            "/api/v1/kmap/nodes/{id}",
            get(|Path(id): Path<String>| async move {
                let (name, fixed, recent, summaries) = match id.as_str() {
                    "AAAAAAAB" => (
                        "Root",
                        vec!["AAAAAAAC", "AAAAAAAD"],
                        Vec::new(),
                        vec![
                            json!({"id":"AAAAAAAC","short_name":"Node A","short_description":"A summary"}),
                            json!({"id":"AAAAAAAD","short_name":"Node C","short_description":"C summary"}),
                        ],
                    ),
                    "AAAAAAAC" => (
                        "Node A",
                        Vec::new(),
                        vec!["AAAAAAAD", "AAAAAAAE"],
                        vec![
                            json!({"id":"AAAAAAAD","short_name":"Node C","short_description":"C summary"}),
                            json!({"id":"AAAAAAAE","short_name":"Node D","short_description":"D summary"}),
                        ],
                    ),
                    "AAAAAAAD" => ("Node C", Vec::new(), Vec::new(), Vec::new()),
                    "AAAAAAAE" => ("Node D", Vec::new(), Vec::new(), Vec::new()),
                    _ => ("Unknown", Vec::new(), Vec::new(), Vec::new()),
                };
                Json(json!({
                    "id":id,
                    "owner_node_id":"AAAAAAAB",
                    "short_name":name,
                    "short_description":format!("{name} summary"),
                    "long_description":format!("{name} details"),
                    "last_modified_by":"test-model-high",
                    "last_modified_at":"2026-07-20T00:00:00Z",
                    "fixed_connections":fixed,
                    "recent_connections":recent,
                    "connection_summaries":summaries,
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = super::super::Config {
            system_prompts_directory: std::path::PathBuf::new(),
            kweb_base: base.clone(),
            intelligence_base: base.clone(),
            session_history_base: base.clone(),
            telegram_relay_base: base.clone(),
            telegram_max_media_bytes: 1024,
            telegram_web_user_handle: "@test".into(),
            runtime_model: super::super::RuntimeModel::testing(),
        };
        let api = Api::new(&config).unwrap();
        let mut context = KmapContext::new(api, vec![root_id.clone()]).unwrap();
        context.initialize().await.unwrap();

        let projection = context.load_durable(&node_a_id).await.unwrap();
        assert!(array(projection.get("directNodes")).is_empty());
        assert_eq!(
            projection.get("directNodePromotions"),
            Some(&json!([node_a_id.clone()]))
        );
        assert_eq!(
            array(projection.get("activeConnectionNodes"))
                .iter()
                .filter_map(node_identifier)
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            vec![node_d_id.clone()]
        );
        assert!(projection.get("loads").is_none());
        assert!(projection.get("context").is_none());

        let snapshot = context.snapshot().unwrap();
        assert_eq!(
            snapshot["directlyLoadedIdentifiers"],
            json!([root_id, node_a_id])
        );
        let rendered = format_projected_kmap_context(&snapshot, &projection);
        assert!(rendered.contains("Always-loaded root identifiers: AAAAAAAB"));
        assert!(!rendered.contains("Node ID: AAAAAAAB"));
        assert!(
            position(
                &rendered,
                "Now directly loaded; full text already present: AAAAAAAC"
            ) < position(&rendered, "Node ID: AAAAAAAE")
        );
        assert!(!rendered.contains("Node ID: AAAAAAAD"));
        assert_eq!(occurrences(&rendered, "Node ID: AAAAAAAE"), 1);
        server.abort();
    }

    #[test]
    fn compact_projection_is_materially_smaller_than_structured_context() {
        let connections = (100..164)
            .map(|identifier| json!({
                "identifier": identifier,
                "shortName": format!("Fanout {identifier}"),
                "shortDescription": "A repeated connection description that serialized structures duplicate many times."
            }))
            .collect::<Vec<_>>();
        let nodes = (1..=8)
            .map(|identifier| {
                json!({
                    "identifier": identifier,
                    "shortName": format!("Dense node {identifier}"),
                    "shortDescription": format!("Dense node {identifier} summary"),
                    "longDescription": format!("Important durable details for node {identifier}."),
                    "ownerIdentifier": 1,
                    "fixedConnections": [],
                    "activeConnections": [],
                    "fanoutConnections": connections.clone(),
                })
            })
            .collect::<Vec<_>>();
        let snapshot = canonical_ids(json!({
            "rootIdentifiers": [1],
            "directlyLoadedIdentifiers": [1, 2, 3, 4, 5, 6, 7, 8],
            "nodes": nodes,
        }));
        let rendered = format_kmap_context(&snapshot);
        let structured = snapshot.to_string();
        assert!(
            rendered.len() * 2 < structured.len(),
            "{} compact bytes versus {} structured bytes",
            rendered.len(),
            structured.len()
        );
        assert!(!rendered.contains("shortDescription"));
        assert!(!rendered.contains('{'));
    }
}
