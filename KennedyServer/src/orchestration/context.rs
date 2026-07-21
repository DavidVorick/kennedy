use std::collections::{HashMap, HashSet};

use anyhow::Context as _;
use serde_json::{Value, json};

use super::Api;

pub(crate) const MAX_DIRECTLY_LOADED_NODES: usize = 10;

#[derive(Clone)]
pub(crate) struct KmapContext {
    api: Api,
    pub root_node_ids: Vec<String>,
    pub loaded_node_ids: Vec<String>,
    pub full_node_ids: HashSet<String>,
    pub nodes_by_id: HashMap<String, Value>,
    node_origins: HashMap<String, HashSet<String>>,
    short_to_durable: HashMap<u64, String>,
    durable_to_short: HashMap<String, u64>,
    next_short_id: u64,
}

impl KmapContext {
    pub(crate) fn new(api: Api, root_node_ids: Vec<String>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !root_node_ids.is_empty()
                && root_node_ids.iter().all(|id| !id.is_empty())
                && root_node_ids.iter().collect::<HashSet<_>>().len() == root_node_ids.len(),
            "Kmap root node identifiers must be distinct non-empty strings"
        );
        Ok(Self {
            api,
            root_node_ids,
            loaded_node_ids: Vec::new(),
            full_node_ids: HashSet::new(),
            nodes_by_id: HashMap::new(),
            node_origins: HashMap::new(),
            short_to_durable: HashMap::new(),
            durable_to_short: HashMap::new(),
            next_short_id: 1,
        })
    }

    pub(crate) async fn initialize(&mut self) -> anyhow::Result<()> {
        self.clear(false);
        self.ensure_roots_loaded().await
    }

    pub(crate) async fn ensure_roots_loaded(&mut self) -> anyhow::Result<()> {
        let roots = self.root_node_ids.clone();
        for id in &roots {
            self.short_id(id);
        }
        for id in roots {
            if !self.loaded_node_ids.contains(&id) {
                self.load_durable(&id).await?;
            }
        }
        Ok(())
    }

    fn clear(&mut self, preserve_identifiers: bool) {
        self.loaded_node_ids.clear();
        self.full_node_ids.clear();
        self.nodes_by_id.clear();
        self.node_origins.clear();
        if !preserve_identifiers {
            self.short_to_durable.clear();
            self.durable_to_short.clear();
            self.next_short_id = 1;
        }
    }

    pub(crate) fn short_id(&mut self, durable_id: &str) -> u64 {
        if let Some(identifier) = self.durable_to_short.get(durable_id) {
            return *identifier;
        }
        let identifier = self.next_short_id;
        self.next_short_id += 1;
        self.short_to_durable
            .insert(identifier, durable_id.to_owned());
        self.durable_to_short
            .insert(durable_id.to_owned(), identifier);
        identifier
    }

    pub(crate) fn register_reference(&mut self, durable_id: &str) -> anyhow::Result<u64> {
        anyhow::ensure!(!durable_id.is_empty(), "a referenced Kmap node ID is empty");
        Ok(self.short_id(durable_id))
    }

    pub(crate) fn resolve(&self, identifier: u64) -> anyhow::Result<String> {
        self.short_to_durable
            .get(&identifier)
            .cloned()
            .with_context(|| format!("Unknown memory identifier {identifier}."))
    }

    pub(crate) fn full_durable(&self, identifier: u64) -> anyhow::Result<String> {
        let id = self.resolve(identifier)?;
        anyhow::ensure!(
            self.full_node_ids.contains(&id),
            "Identifier {identifier} is only a connection summary; load it before using this tool."
        );
        Ok(id)
    }

    pub(crate) async fn load_durable(&mut self, durable_id: &str) -> anyhow::Result<Value> {
        anyhow::ensure!(
            !self.loaded_node_ids.iter().any(|id| id == durable_id),
            "That node is already directly loaded."
        );
        anyhow::ensure!(
            self.loaded_node_ids.len() < MAX_DIRECTLY_LOADED_NODES,
            "Ten nodes are already directly loaded. Reset the context to continue."
        );
        let previously_full = self.full_node_ids.clone();
        let payload = self.api.kmap_context(durable_id).await?;
        let requested = payload
            .get("requested_node")
            .cloned()
            .context("Kmap context response omitted requested_node")?;
        let requested_id = string_field(&requested, "id")?.to_owned();
        self.ingest_node(requested.clone(), true, "direct")?;
        let active = payload
            .get("active_connection_nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for node in &active {
            self.ingest_node(node.clone(), true, "active")?;
        }
        self.loaded_node_ids.push(durable_id.to_owned());
        let requested_already_loaded = previously_full.contains(&requested_id);
        let requested_context = if requested_already_loaded {
            Value::Null
        } else {
            self.context_node(&requested)?
        };
        let mut active_context = Vec::new();
        for node in &active {
            let id = string_field(node, "id")?;
            if !previously_full.contains(id) {
                active_context.push(self.context_node(node)?);
            }
        }
        let full = self.full_node_ids.clone();
        let direct_fanout = requested
            .get("fanout_connections")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|entry| {
                entry
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| !full.contains(id))
            })
            .cloned()
            .collect::<Vec<_>>();
        let direct_fanout = self.summaries(&direct_fanout);
        Ok(json!({
            "requestedNode": requested_context,
            "requestedNodeIdentifier": self.short_id(&requested_id),
            "requestedNodeAlreadyLoaded": requested_already_loaded,
            "activeConnectionNodes": active_context,
            "directFanoutNodes": direct_fanout,
            "indirectFanoutNodes": [],
        }))
    }

    pub(crate) async fn reset(&mut self, durable_ids: &[String]) -> anyhow::Result<Value> {
        anyhow::ensure!(
            durable_ids
                .iter()
                .all(|id| !self.root_node_ids.contains(id)),
            "Root nodes are loaded automatically and must not be listed."
        );
        anyhow::ensure!(
            durable_ids.iter().collect::<HashSet<_>>().len() == durable_ids.len(),
            "Reset identifiers must be distinct."
        );
        anyhow::ensure!(
            durable_ids.len() + self.root_node_ids.len() <= MAX_DIRECTLY_LOADED_NODES,
            "Reset would exceed the ten directly loaded node limit."
        );
        self.clear(true);
        self.ensure_roots_loaded().await?;
        let mut loads = Vec::new();
        for id in durable_ids {
            loads.push(self.load_durable(id).await?);
        }
        Ok(json!({"loads": loads, "context": self.snapshot()?}))
    }

    pub(crate) fn refresh(&mut self, nodes: Vec<Value>) -> anyhow::Result<()> {
        for node in nodes {
            self.ingest_node(node, true, "operation")?;
        }
        Ok(())
    }

    pub(crate) fn snapshot(&mut self) -> anyhow::Result<Value> {
        let roots = self.root_node_ids.clone();
        let loaded = self.loaded_node_ids.clone();
        let mut full = self.full_node_ids.iter().cloned().collect::<Vec<_>>();
        full.sort_by_key(|id| self.durable_to_short.get(id).copied().unwrap_or(u64::MAX));
        let root_identifiers = roots.iter().map(|id| self.short_id(id)).collect::<Vec<_>>();
        let directly_loaded_identifiers = loaded
            .iter()
            .map(|id| self.short_id(id))
            .collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for id in full {
            let node = self
                .nodes_by_id
                .get(&id)
                .cloned()
                .with_context(|| format!("missing full Kmap node {id}"))?;
            let mut projected = self.context_node(&node)?;
            projected["contextSources"] = json!(
                self.node_origins
                    .get(&id)
                    .map(|origins| origins.iter().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            );
            nodes.push(projected);
        }
        Ok(json!({
            "rootIdentifiers": root_identifiers,
            "directlyLoadedIdentifiers": directly_loaded_identifiers,
            "nodes": nodes,
        }))
    }

    pub(crate) fn archive(&self) -> Value {
        let mut mappings = self
            .short_to_durable
            .iter()
            .map(|(short, durable)| json!([short, durable]))
            .collect::<Vec<_>>();
        mappings.sort_by_key(|entry| entry[0].as_u64().unwrap_or_default());
        json!({
            "loadedNodeIds": self.loaded_node_ids,
            "fullNodeIds": self.full_node_ids,
            "nodesById": self.nodes_by_id.iter().map(|(id, node)| json!([id, node])).collect::<Vec<_>>(),
            "nodeOrigins": self.node_origins.iter().map(|(id, origins)| json!([id, origins])).collect::<Vec<_>>(),
            "shortToDurable": mappings,
            "nextShortId": self.next_short_id,
        })
    }

    pub(crate) fn diagnostics(&self) -> Value {
        json!({
            "loadedNodeIds": self.loaded_node_ids,
            "fullNodeIds": self.full_node_ids,
            "nodeOrigins": self.node_origins,
            "shortToDurable": self.short_to_durable,
            "nextShortId": self.next_short_id,
        })
    }

    pub(crate) fn restore(&mut self, archive: &Value) -> anyhow::Result<()> {
        let loaded = string_array(archive.get("loadedNodeIds"))?;
        anyhow::ensure!(
            loaded.len() <= MAX_DIRECTLY_LOADED_NODES
                && loaded.iter().collect::<HashSet<_>>().len() == loaded.len(),
            "The saved Kmap context exceeds the directly loaded node limit or contains duplicates."
        );
        self.loaded_node_ids = loaded;
        self.full_node_ids = string_array(archive.get("fullNodeIds"))?
            .into_iter()
            .collect();
        self.nodes_by_id = pairs(archive.get("nodesById"))?.into_iter().collect();
        self.node_origins = pairs(archive.get("nodeOrigins"))?
            .into_iter()
            .map(|(key, value)| {
                let values = string_array(Some(&value))
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                (key, values)
            })
            .collect();
        self.short_to_durable.clear();
        for entry in archive
            .get("shortToDurable")
            .and_then(Value::as_array)
            .context("saved Kmap context omitted shortToDurable")?
        {
            let pair = entry.as_array().context("invalid short ID mapping")?;
            let short = pair
                .first()
                .and_then(Value::as_u64)
                .context("invalid short ID")?;
            let durable = pair
                .get(1)
                .and_then(Value::as_str)
                .context("invalid durable ID")?;
            self.short_to_durable.insert(short, durable.to_owned());
        }
        self.durable_to_short = self
            .short_to_durable
            .iter()
            .map(|(short, durable)| (durable.clone(), *short))
            .collect();
        self.next_short_id = archive
            .get("nextShortId")
            .and_then(Value::as_u64)
            .unwrap_or(self.short_to_durable.len() as u64 + 1);
        Ok(())
    }

    pub(crate) fn context_node(&mut self, node: &Value) -> anyhow::Result<Value> {
        let id = string_field(node, "id")?;
        let owner = node
            .get("owner_root_node_id")
            .and_then(Value::as_str)
            .map(|owner| self.short_id(owner));
        Ok(json!({
            "identifier": self.short_id(id),
            "shortName": node.get("short_name").and_then(Value::as_str).unwrap_or(""),
            "shortDescription": node.get("short_description").and_then(Value::as_str).unwrap_or(""),
            "longDescription": node.get("long_description").and_then(Value::as_str).unwrap_or(""),
            "lastModifiedBy": node.get("last_modified_by").and_then(Value::as_str).unwrap_or("legacy-unknown"),
            "lastModifiedAt": node.get("last_modified_at").cloned().unwrap_or(Value::Null),
            "ownerIdentifier": owner.map(Value::from).unwrap_or_else(|| Value::String("unowned".into())),
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

    fn ingest_node(&mut self, node: Value, full: bool, origin: &str) -> anyhow::Result<()> {
        let id = string_field(&node, "id")?.to_owned();
        self.short_id(&id);
        if let Some(owner) = node.get("owner_root_node_id").and_then(Value::as_str) {
            self.short_id(owner);
        }
        for field in [
            "fixed_connections",
            "active_connections",
            "fanout_connections",
        ] {
            for connection in node
                .get(field)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(id) = connection.get("id").and_then(Value::as_str) {
                    self.short_id(id);
                }
            }
        }
        if full {
            self.nodes_by_id.insert(id.clone(), node);
            self.full_node_ids.insert(id.clone());
            self.node_origins
                .entry(id)
                .or_default()
                .insert(origin.to_owned());
        } else {
            self.nodes_by_id.entry(id).or_insert(node);
        }
        Ok(())
    }

    fn connection_summaries(&mut self, value: Option<&Value>, fixed: bool) -> Vec<Value> {
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
                let identifier = self.short_id(id);
                let mut value = json!({
                    "identifier": identifier,
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

    fn summaries(&mut self, connections: &[Value]) -> Vec<Value> {
        connections
            .iter()
            .filter_map(|connection| {
                let id = connection.get("id").and_then(Value::as_str)?;
                Some(json!({
                    "identifier": self.short_id(id),
                    "shortName": connection.get("short_name").and_then(Value::as_str).unwrap_or("Unloaded node"),
                    "shortDescription": connection.get("short_description").and_then(Value::as_str).unwrap_or(""),
                }))
            })
            .collect()
    }
}

pub(crate) fn format_kmap_context(snapshot: &Value) -> String {
    let roots = number_list(snapshot.get("rootIdentifiers"));
    let loaded = number_list(snapshot.get("directlyLoadedIdentifiers"));
    let projection = project_snapshot(snapshot);
    let nodes = format_compact_memory_sections(&projection);
    format!(
        "Current Kmap context\n\nAlways-loaded root identifiers: {roots}\nDirectly loaded node limit: 10\nDirectly loaded memory identifiers: {loaded}\n\n{}",
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
    requested_identifiers: &[u64],
    active_identifiers: &[u64],
) -> Value {
    let before_projection = project_snapshot(before);
    let before_roles = projection_roles(&before_projection);
    let after_nodes = nodes_by_identifier(after);

    let mut direct_nodes = Vec::new();
    let mut direct_node_promotions = Vec::new();
    let mut seen_direct = HashSet::new();
    for identifier in requested_identifiers {
        if !seen_direct.insert(*identifier) {
            continue;
        }
        let previous_role = before_roles.get(identifier).copied().unwrap_or_default();
        if previous_role < 3
            && let Some(node) = after_nodes.get(identifier)
        {
            direct_nodes.push((*node).clone());
        } else if previous_role == 3 {
            direct_node_promotions.push(*identifier);
        }
    }

    let direct_identifiers = requested_identifiers
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let mut active_nodes = Vec::new();
    let mut seen_active = HashSet::new();
    for identifier in active_identifiers {
        if !direct_identifiers.contains(identifier)
            && seen_active.insert(*identifier)
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
        .filter(|identifier| !direct_identifiers.contains(identifier))
        .filter_map(|identifier| after_nodes.get(identifier).copied())
        .collect::<Vec<_>>();
    let direct_fanout_candidates =
        unique_connections(&batch_direct, "fanoutConnections", &full_identifiers);
    let direct_fanout_identifiers = direct_fanout_candidates
        .iter()
        .filter_map(node_identifier)
        .collect::<HashSet<_>>();
    let mut indirect_excluded = full_identifiers;
    indirect_excluded.extend(direct_fanout_identifiers);
    let indirect_fanout_candidates =
        unique_connections(&batch_active, "fanoutConnections", &indirect_excluded);
    let direct_fanout_nodes = direct_fanout_candidates
        .into_iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| {
                before_roles.get(&identifier).copied().unwrap_or_default() < 2
            })
        })
        .collect::<Vec<_>>();
    let indirect_fanout_nodes = indirect_fanout_candidates
        .into_iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| {
                before_roles.get(&identifier).copied().unwrap_or_default() < 1
            })
        })
        .collect::<Vec<_>>();

    json!({
        "directNodes": direct_nodes,
        "directNodePromotions": direct_node_promotions,
        "activeConnectionNodes": active_nodes,
        "directFanoutNodes": direct_fanout_nodes,
        "indirectFanoutNodes": indirect_fanout_nodes,
    })
}

pub(crate) fn format_compact_memory_sections(projection: &Value) -> String {
    let direct_nodes = array(projection.get("directNodes"));
    let direct_promotions = projection
        .get("directNodePromotions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
        .map(|identifier| identifier.to_string())
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
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
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
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
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
        .filter_map(Value::as_u64)
        .collect::<HashSet<_>>();
    let node_map = nodes_by_identifier(snapshot);
    let mut direct_nodes = Vec::new();
    for identifier in snapshot
        .get("directlyLoadedIdentifiers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
    {
        if let Some(node) = node_map.get(&identifier) {
            direct_nodes.push((*node).clone());
        }
    }
    let active_nodes = array(snapshot.get("nodes"))
        .iter()
        .filter(|node| {
            node_identifier(node).is_some_and(|identifier| !direct.contains(&identifier))
        })
        .cloned()
        .collect::<Vec<_>>();
    let full = full_identifiers(snapshot);
    let direct_refs = direct_nodes.iter().collect::<Vec<_>>();
    let direct_fanout_nodes = unique_connections(&direct_refs, "fanoutConnections", &full);
    let mut indirect_excluded = full;
    indirect_excluded.extend(direct_fanout_nodes.iter().filter_map(node_identifier));
    let active_refs = active_nodes.iter().collect::<Vec<_>>();
    let indirect_fanout_nodes =
        unique_connections(&active_refs, "fanoutConnections", &indirect_excluded);
    json!({
        "directNodes": direct_nodes,
        "directNodePromotions": [],
        "activeConnectionNodes": active_nodes,
        "directFanoutNodes": direct_fanout_nodes,
        "indirectFanoutNodes": indirect_fanout_nodes,
    })
}

fn nodes_by_identifier(snapshot: &Value) -> HashMap<u64, &Value> {
    array(snapshot.get("nodes"))
        .iter()
        .filter_map(|node| Some((node_identifier(node)?, node)))
        .collect()
}

fn full_identifiers(snapshot: &Value) -> HashSet<u64> {
    array(snapshot.get("nodes"))
        .iter()
        .filter_map(node_identifier)
        .collect()
}

fn projection_roles(projection: &Value) -> HashMap<u64, u8> {
    let mut roles = HashMap::new();
    for (key, role) in [
        ("indirectFanoutNodes", 1),
        ("directFanoutNodes", 2),
        ("activeConnectionNodes", 3),
        ("directNodes", 4),
    ] {
        for node in array(projection.get(key)) {
            if let Some(identifier) = node_identifier(node) {
                roles.insert(identifier, role);
            }
        }
    }
    roles
}

fn unique_connections(nodes: &[&Value], field: &str, excluded: &HashSet<u64>) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for node in nodes {
        for connection in array(node.get(field)) {
            let Some(identifier) = node_identifier(connection) else {
                continue;
            };
            if !excluded.contains(&identifier) && seen.insert(identifier) {
                result.push(connection.clone());
            }
        }
    }
    result
}

fn node_identifier(node: &Value) -> Option<u64> {
    node.get("identifier").and_then(Value::as_u64)
}

fn array(value: Option<&Value>) -> &[Value] {
    value
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

pub(crate) fn format_context_node(node: &Value, include_summary: bool) -> String {
    let owner = node
        .get("ownerIdentifier")
        .and_then(Value::as_u64)
        .map(|value| format!("Node {value}"))
        .unwrap_or_else(|| "unowned".into());
    let mut lines = vec![format!(
        "Node {}: {}",
        node.get("identifier")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        text(node.get("shortName"), "(none)")
    )];
    if include_summary {
        lines.push(format!(
            "Summary: {}",
            text(node.get("shortDescription"), "(none)")
        ));
    }
    lines.extend([
        format!(
            "Last modified by: {}",
            text(node.get("lastModifiedBy"), "legacy-unknown")
        ),
        format!(
            "Last modified at: {}",
            text(node.get("lastModifiedAt"), "unknown")
        ),
        format!("Owner: {owner}"),
        "Details:".into(),
        indent(&text(node.get("longDescription"), "(none)")),
        format!(
            "Fixed connection identifiers: {}",
            connection_ids(node.get("fixedConnections"), true)
        ),
        format!(
            "Active connection identifiers: {}",
            connection_ids(node.get("activeConnections"), false)
        ),
        format!(
            "Fanout connection identifiers: {}",
            connection_ids(node.get("fanoutConnections"), false)
        ),
    ]);
    lines.join("\n")
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

fn string_array(value: Option<&Value>) -> anyhow::Result<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .context("expected an array")?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::to_owned)
                .context("expected string ID")
        })
        .collect()
}

fn pairs(value: Option<&Value>) -> anyhow::Result<Vec<(String, Value)>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .context("expected saved key/value pairs")?
        .iter()
        .map(|entry| {
            let pair = entry.as_array().context("invalid saved key/value pair")?;
            Ok((
                pair.first()
                    .and_then(Value::as_str)
                    .context("invalid saved key")?
                    .to_owned(),
                pair.get(1).cloned().context("missing saved value")?,
            ))
        })
        .collect()
}

fn number_list(value: Option<&Value>) -> String {
    let values = value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
        .map(|value| value.to_string())
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
            let identifier = connection.get("identifier").and_then(Value::as_u64)?;
            Some(if fixed {
                format!(
                    "slot {}: {identifier}",
                    connection
                        .get("slot")
                        .and_then(Value::as_u64)
                        .unwrap_or_default()
                )
            } else {
                identifier.to_string()
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

    fn position(text: &str, needle: &str) -> usize {
        text.find(needle)
            .unwrap_or_else(|| panic!("missing {needle:?} in:\n{text}"))
    }

    fn occurrences(text: &str, needle: &str) -> usize {
        text.match_indices(needle).count()
    }

    #[test]
    fn compact_context_keeps_root_and_connection_identifiers() {
        let rendered = format_kmap_context(&json!({
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
        }));
        assert!(rendered.contains("Always-loaded root identifiers: 1, 2"));
        assert!(rendered.contains("Active connection identifiers: 3"));
    }

    #[test]
    fn complete_context_is_classified_globally_before_rendering() {
        let rendered = format_kmap_context(&json!({
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
        }));

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
            position(&rendered, "Node 1: Direct One") < position(&rendered, "Node 3: Direct Three")
        );
        assert!(
            position(&rendered, "Node 3: Direct Three") < position(&rendered, "Node 2: Active Two")
        );
        assert_eq!(occurrences(&rendered, "2: Active Two"), 1);
        assert_eq!(occurrences(&rendered, "3: Direct Three"), 1);
        assert_eq!(occurrences(&rendered, "4: Direct Fanout Four"), 1);
        assert!(rendered.contains("6: Direct Fanout Six\n  Summary: Six summary"));
        assert!(rendered.contains("5: Indirect Fanout Five"));
        assert!(!rendered.contains("ACTIVE SUMMARY MUST BE OMITTED"));
        assert!(!rendered.contains("INDIRECT SUMMARY MUST BE OMITTED"));
        assert!(!rendered.contains('{'));
    }

    #[test]
    fn load_batch_upgrades_then_renders_each_final_role_once() {
        let before = json!({
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
        });
        let after = json!({
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
        });

        let projection = project_load_batch(&before, &after, &[3, 2], &[2, 4, 3]);
        let rendered = format_compact_memory_sections(&projection);
        assert_eq!(
            array(projection.get("directNodes"))
                .iter()
                .filter_map(node_identifier)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(projection.get("directNodePromotions"), Some(&json!([2])));
        assert_eq!(
            array(projection.get("activeConnectionNodes"))
                .iter()
                .filter_map(node_identifier)
                .collect::<Vec<_>>(),
            vec![4]
        );
        assert_eq!(
            array(projection.get("directFanoutNodes"))
                .iter()
                .filter_map(node_identifier)
                .collect::<Vec<_>>(),
            vec![6, 8]
        );
        assert_eq!(
            array(projection.get("indirectFanoutNodes"))
                .iter()
                .filter_map(node_identifier)
                .collect::<Vec<_>>(),
            vec![9]
        );
        assert!(
            position(&rendered, "Node 3: New Direct")
                < position(
                    &rendered,
                    "Now directly loaded; full text already present: 2"
                )
        );
        assert!(
            position(
                &rendered,
                "Now directly loaded; full text already present: 2"
            ) < position(&rendered, "Node 4: New Active")
        );
        assert!(
            position(&rendered, "Node 4: New Active")
                < position(&rendered, "6: Previously Indirect")
        );
        assert!(
            position(&rendered, "6: Previously Indirect") < position(&rendered, "9: New Indirect")
        );
        assert_eq!(occurrences(&rendered, "8: New Direct Fanout"), 1);
        assert!(!rendered.contains("Node 2: Previously Active"));
        assert!(!rendered.contains("Previously active details"));
        assert!(!rendered.contains("Known Direct Fanout"));
        assert!(!rendered.contains("Nine summary omitted"));
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
        let snapshot = json!({
            "rootIdentifiers": [1],
            "directlyLoadedIdentifiers": [1, 2, 3, 4, 5, 6, 7, 8],
            "nodes": nodes,
        });
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
