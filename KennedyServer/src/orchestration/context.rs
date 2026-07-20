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
        let full = self.full_node_ids.iter().cloned().collect::<Vec<_>>();
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
    let direct = snapshot
        .get("directlyLoadedIdentifiers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
        .collect::<HashSet<_>>();
    let mut direct_nodes = Vec::new();
    let mut active_nodes = Vec::new();
    for node in snapshot
        .get("nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if node
            .get("identifier")
            .and_then(Value::as_u64)
            .is_some_and(|identifier| direct.contains(&identifier))
        {
            direct_nodes.push(format_context_node(node, true));
        } else {
            active_nodes.push(format_context_node(node, false));
        }
    }
    let mut sections = Vec::new();
    if !direct_nodes.is_empty() {
        sections.push(format!(
            "Directly loaded nodes\n\n{}",
            direct_nodes.join("\n\n")
        ));
    }
    if !active_nodes.is_empty() {
        sections.push(format!(
            "Full active-connection nodes\n\n{}",
            active_nodes.join("\n\n")
        ));
    }
    format!(
        "Current Kmap context\n\nAlways-loaded root identifiers: {roots}\nDirectly loaded node limit: 10\nDirectly loaded memory identifiers: {loaded}\n\n{}",
        if sections.is_empty() {
            "No memory nodes are currently loaded.".to_owned()
        } else {
            sections.join("\n\n")
        }
    )
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
}
