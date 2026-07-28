use anyhow::Context as _;
use kcode_kweb_context::{Context, LoadReport, Node};
use serde_json::{Value, json};

use super::Api;

pub(crate) async fn load_durable(
    api: &Api,
    context: &mut Context,
    durable_id: &str,
) -> anyhow::Result<Value> {
    load_durable_batch(api, context, &[durable_id.to_owned()]).await
}

pub(crate) async fn load_durable_batch(
    api: &Api,
    context: &mut Context,
    durable_ids: &[String],
) -> anyhow::Result<Value> {
    let mut reports = Vec::with_capacity(durable_ids.len());
    for durable_id in durable_ids {
        reports.push(load_one(api, context, durable_id).await?);
    }
    Ok(json!({"loads":reports}))
}

async fn load_one(
    api: &Api,
    context: &mut Context,
    durable_id: &str,
) -> anyhow::Result<LoadReport> {
    let payload = api.kmap_context(durable_id).await?;
    let requested = payload
        .get("requested_node")
        .context("Kweb context response omitted requested_node")
        .and_then(node_from_value)?;
    anyhow::ensure!(
        requested.id == durable_id,
        "Kweb returned node {} when {durable_id} was requested",
        requested.id
    );
    let fixed = payload
        .get("fixed_connection_nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(node_from_value)
        .collect::<anyhow::Result<Vec<_>>>()?;
    context
        .apply_load(requested, fixed)
        .map_err(anyhow::Error::new)
}

pub(crate) fn node_from_value(value: &Value) -> anyhow::Result<Node> {
    Node::from_kweb_value(value).map_err(anyhow::Error::new)
}
