use kcode_kweb_context::{Connection, Context, LoadReport, Node};
use kcode_kweb_db::{Node as KwebNode, NodeId, Owner};
use serde_json::{Value, json};

use super::Api;

pub(crate) fn load_durable_batch(
    api: &Api,
    context: &mut Context,
    durable_ids: &[String],
) -> anyhow::Result<Value> {
    let mut preview = context.clone();
    let mut reports = Vec::with_capacity(durable_ids.len());
    for durable_id in durable_ids {
        reports.push(load_one(api, &mut preview, durable_id)?);
    }
    *context = preview;
    Ok(json!({"loads":reports}))
}

fn load_one(api: &Api, context: &mut Context, durable_id: &str) -> anyhow::Result<LoadReport> {
    let requested = api.kmap_node(durable_id)?;
    let fixed_ids = requested.data.fixed_connections.clone();
    let requested = node_from_kweb(api, requested)?;
    anyhow::ensure!(
        requested.id == durable_id,
        "Kweb returned node {} when {durable_id} was requested",
        requested.id
    );
    let fixed = fixed_ids
        .into_iter()
        .filter(|id| id.to_string() != durable_id)
        .map(|id| api.kmap_node(&id.to_string()))
        .map(|node| node.map_err(anyhow::Error::new))
        .map(|node| node.and_then(|node| node_from_kweb(api, node)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    context
        .apply_load(requested, fixed)
        .map_err(anyhow::Error::new)
}

fn node_from_kweb(api: &Api, node: KwebNode) -> anyhow::Result<Node> {
    let fixed_connections = connections(api, &node.data.fixed_connections)?;
    let recent_connections = connections(api, &node.data.recent_connections)?;
    let owner = match node.data.owner {
        Owner::Unowned => "unowned".into(),
        Owner::SelfNode => node.id.to_string(),
        Owner::Node(id) => id.to_string(),
    };
    Ok(Node {
        id: node.id.to_string(),
        short_name: node.data.short_name,
        short_description: node.data.short_description,
        long_description: node.data.long_description,
        owner,
        fixed_connections,
        recent_connections,
        objects: node.data.objects.iter().map(ToString::to_string).collect(),
        last_modified_by: node.last_author,
        last_modified_at: Some(node.committed_at.to_rfc3339()),
    })
}

fn connections(api: &Api, ids: &[NodeId]) -> anyhow::Result<Vec<Connection>> {
    ids.iter()
        .map(|id| api.kmap_node(&id.to_string()))
        .map(|node| node.map_err(anyhow::Error::new))
        .map(|node| {
            node.map(|node| Connection {
                id: node.id.to_string(),
                short_name: node.data.short_name,
                short_description: node.data.short_description,
            })
        })
        .collect()
}

pub(crate) fn node_from_value(value: &Value) -> anyhow::Result<Node> {
    Node::from_kweb_value(value).map_err(anyhow::Error::new)
}
