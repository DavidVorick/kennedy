use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    path::PathBuf,
    time::Duration,
};

use super::chatend::{
    BoxContent, BoxId, BoxOwner, BoxRepresentation, EventId, EventKind, PendingId, Representation,
    SessionJournal, SessionKind, SessionMetadata, ToolSlotInput,
};
use anyhow::Context as _;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use kcode_kweb_db::NodeId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    kmap_http::{
        SessionCommit, SessionCommitResult, SessionNodeCreate, SessionNodeData, SessionNodeUpdate,
        SessionObject,
    },
    rust_lib_tools::{
        LibrarySnapshot, PREVIEW_WRITE_FILE_RUST_LIB_TOOL, RUST_LIB_TOOLS,
        WRITE_FILE_FREEFORM_RUST_LIB_TOOL, WRITE_RUST_LIB_TOOL, proposed_write_snapshot,
    },
};

use super::{
    Api, Manuals, RuntimeModel,
    context::{
        KmapContext, format_context_node, stored_active_ids, stored_fixed_ids, stored_recent_ids,
    },
};

const AGENT_LOOP_ROUND_LIMIT: u64 = 100;
const BROWSER_CONVERSATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const HISTORY_INGRESS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const INLINE_TOOL_INVOCATION_CHARACTERS: usize = 1_000;
const KWEB_TOOL_INSTANCE: &str = "kweb";
const HISTORY_TOOL_INSTANCE: &str = "history";
const RUST_LIB_TOOL_INSTANCE: &str = "managed-rust-libraries";
const CAPACITY_ERROR_BOX_NAME: &str = "Context capacity error";
const INGRESS_FORCE_COMMIT_NOTE: &str = "ingress_force_commit";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AgentMode {
    Conversation,
    FreeTime,
    Ingress { record_id: Option<String> },
}

#[derive(Clone, Debug)]
pub(crate) struct SessionOptions {
    pub session_type: String,
    pub root_node_ids: Vec<String>,
    pub reference_root_node_ids: Vec<String>,
    pub channel: Value,
    pub free_time: Value,
    pub orchestration: Value,
    pub provenance_id: Option<String>,
    pub mode: AgentMode,
    pub source_session_type: Option<String>,
    pub group_context: Value,
    pub rust_lib_session_id: Option<String>,
}

impl SessionOptions {
    pub(crate) fn conversation(session_type: impl Into<String>, roots: Vec<String>) -> Self {
        Self {
            session_type: session_type.into(),
            root_node_ids: roots,
            reference_root_node_ids: Vec::new(),
            channel: Value::Null,
            free_time: Value::Null,
            orchestration: json!({"owner":"backend","status":"idle"}),
            provenance_id: None,
            mode: AgentMode::Conversation,
            source_session_type: None,
            group_context: Value::Null,
            rust_lib_session_id: None,
        }
    }
}

fn restore_session_type(options: &mut SessionOptions, state: &Value) {
    if !matches!(&options.mode, AgentMode::Ingress { .. }) {
        options.session_type = state
            .get("sessionType")
            .and_then(Value::as_str)
            .unwrap_or(&options.session_type)
            .to_owned();
    }
}

fn restore_commit_receipt(restored: Option<&Value>) -> anyhow::Result<Option<SessionCommitResult>> {
    restored
        .and_then(|state| state.get("commitReceipt"))
        .filter(|receipt| !receipt.is_null())
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("decoding the stored session commit receipt")
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct KwebPlan {
    creates: Vec<SessionNodeCreate>,
    updates: BTreeMap<String, SessionNodeData>,
}

impl KwebPlan {
    fn restore(restored: Option<&Value>, journal: &SessionJournal) -> anyhow::Result<Self> {
        if let Some(plan) = restored.and_then(|state| state.get("kwebPlan")) {
            return serde_json::from_value(plan.clone()).context("decoding the staged Kweb plan");
        }
        // Transitional compatibility for journals written before Kweb plans
        // moved into KennedyServer lifecycle state.
        let latest = journal.state().events.iter().rev().find_map(|event| {
            let EventKind::KwebPlanChanged { operation } = &event.kind else {
                return None;
            };
            operation.get("plan")
        });
        latest
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .context("decoding the staged Kweb plan")
            .map(Option::unwrap_or_default)
    }

    fn created(&self, id: &str) -> Option<&SessionNodeData> {
        self.creates
            .iter()
            .find(|create| create.pending_id == id)
            .map(|create| &create.data)
    }

    fn created_mut(&mut self, id: &str) -> Option<&mut SessionNodeData> {
        self.creates
            .iter_mut()
            .find(|create| create.pending_id == id)
            .map(|create| &mut create.data)
    }
}

pub(crate) struct Session {
    api: Api,
    runtime: RuntimeModel,
    journal: SessionJournal,
    plan: KwebPlan,
    pub session_type: String,
    pub channel: Value,
    pub free_time: Value,
    pub orchestration: Value,
    pub provenance_id: Option<String>,
    pub rust_lib_session_id: String,
    pub root_node_ids: Vec<String>,
    pub reference_root_node_ids: Vec<String>,
    pub started_at: String,
    pub transcript: Vec<Value>,
    pub pending_turn: bool,
    pub pending_external_event_id: Option<String>,
    pub completed: bool,
    pub rounds_used: u64,
    commit_receipt: Option<SessionCommitResult>,
    commit_author: String,
    mode: AgentMode,
    source_session_type: Option<String>,
    group_context: Value,
    context: KmapContext,
    free_time_end_reason: Option<String>,
    fatal_persistence_error: Option<String>,
}

struct DesiredKwebBox {
    logical_slot: String,
    name: String,
    content: BoxContent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputStage {
    Accepted,
    RejectedForCapacity,
}

#[derive(Clone, Copy)]
enum SummaryDetail {
    Summary,
    NameAndSummary,
    Name,
}

fn format_summary_entries(heading: &str, entries: &[Value], detail: SummaryDetail) -> String {
    let mut lines = vec![heading.to_owned()];
    if entries.is_empty() {
        lines.push("None.".into());
        return lines.join("\n");
    }
    for entry in entries {
        let identifier = entry
            .get("identifier")
            .and_then(Value::as_str)
            .unwrap_or("invalid");
        let name = entry
            .get("shortName")
            .and_then(Value::as_str)
            .unwrap_or("Unloaded node");
        let summary = entry
            .get("shortDescription")
            .and_then(Value::as_str)
            .unwrap_or("(none)");
        lines.push(match detail {
            SummaryDetail::Summary => format!("{identifier}: {summary}"),
            SummaryDetail::NameAndSummary => format!("{identifier} · {name}: {summary}"),
            SummaryDetail::Name => format!("{identifier}: {name}"),
        });
    }
    lines.join("\n")
}

fn mark_kweb_content(content: &mut BoxContent, logical_slot: &str, role: &str) {
    if !content.metadata.is_object() {
        content.metadata = json!({});
    }
    content.metadata["kwebLogicalSlot"] = json!(logical_slot);
    content.metadata["kwebRole"] = json!(role);
}

fn kweb_logical_slot(state: &super::chatend::BoxState, actual_slot: &str) -> String {
    state
        .canonical
        .content
        .metadata
        .get("kwebLogicalSlot")
        .and_then(Value::as_str)
        .unwrap_or(actual_slot)
        .to_owned()
}

fn kweb_node_identifier(state: &super::chatend::BoxState) -> Option<&str> {
    state
        .canonical
        .content
        .metadata
        .get("canonicalNodeId")
        .and_then(Value::as_str)
        .or_else(|| {
            state
                .canonical
                .content
                .metadata
                .get("identifier")
                .and_then(Value::as_str)
        })
}

type KwebBoxVersions = BTreeMap<BoxId, (String, EventId)>;

fn kweb_box_versions(journal: &SessionJournal) -> KwebBoxVersions {
    journal
        .state()
        .tool_layouts
        .get(KWEB_TOOL_INSTANCE)
        .into_iter()
        .flatten()
        .filter_map(|box_id| {
            let state = journal.state().box_state(*box_id)?;
            state
                .active
                .then(|| (*box_id, (state.name.clone(), state.canonical.event_id)))
        })
        .collect()
}

fn changed_kweb_box_ids(journal: &SessionJournal, previous: &KwebBoxVersions) -> Vec<BoxId> {
    journal
        .state()
        .tool_layouts
        .get(KWEB_TOOL_INSTANCE)
        .into_iter()
        .flatten()
        .filter_map(|box_id| {
            let state = journal.state().box_state(*box_id)?;
            let current = (state.name.as_str(), state.canonical.event_id);
            let changed = previous
                .get(box_id)
                .map(|(name, revision)| (name.as_str(), *revision) != current)
                .unwrap_or(true);
            (state.active && changed).then_some(*box_id)
        })
        .collect()
}

fn render_load_node_result(
    journal: &SessionJournal,
    changed_box_ids: &[BoxId],
) -> anyhow::Result<String> {
    if changed_box_ids.is_empty() {
        return Ok("LoadNode completed. The shared Kweb boxes were already current.".into());
    }
    let rendered = journal
        .state()
        .projection()
        .items
        .into_iter()
        .filter(|item| !item.marker)
        .map(|item| (item.box_id, item.text))
        .collect::<BTreeMap<_, _>>();
    changed_box_ids
        .iter()
        .map(|box_id| {
            rendered
                .get(box_id)
                .cloned()
                .with_context(|| format!("updated Kweb box {box_id} is absent from the projection"))
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|boxes| boxes.join("\n\n"))
}

fn render_web_search_result(result: &Value) -> anyhow::Result<String> {
    let answer = result
        .get("answer")
        .and_then(Value::as_str)
        .context("web search response has no answer text")?;
    let mut text = answer.to_owned();
    let sources = result
        .get("sources")
        .and_then(Value::as_array)
        .context("web search response has no sources")?;
    if !sources.is_empty() {
        text.push_str("\n\nSources:");
        for source in sources {
            let url = source
                .get("url")
                .and_then(Value::as_str)
                .context("web search source has no URL")?;
            let title = source
                .get("title")
                .and_then(Value::as_str)
                .filter(|title| !title.trim().is_empty())
                .unwrap_or(url);
            text.push_str("\n- ");
            text.push_str(title);
            if title != url {
                text.push_str(": ");
                text.push_str(url);
            }
        }
    }
    Ok(text)
}

fn render_web_fetch_result(result: &Value) -> anyhow::Result<String> {
    let url = result
        .get("url")
        .and_then(Value::as_str)
        .context("web fetch response has no URL")?;
    let content = result
        .get("content")
        .and_then(Value::as_str)
        .context("web fetch response has no page text")?;
    let mut text = format!("Source URL: {url}");
    if let Some(title) = result
        .get("title")
        .and_then(Value::as_str)
        .filter(|title| !title.trim().is_empty())
    {
        text.push_str("\nTitle: ");
        text.push_str(title);
    }
    if let Some(content_type) = result.get("content_type").and_then(Value::as_str) {
        text.push_str("\nContent type: ");
        text.push_str(content_type);
    }
    if result
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        text.push_str("\nThe returned page text was truncated.");
    }
    text.push_str("\n\n");
    text.push_str(content);
    Ok(text)
}

struct HistoryIngressRepresentationPlan {
    desired: BTreeMap<BoxId, BoxRepresentation>,
    fits: bool,
}

fn history_ingress_representation_plan(
    state: &super::chatend::Chatend,
) -> anyhow::Result<HistoryIngressRepresentationPlan> {
    let mut desired = state
        .active_boxes()
        .map(|box_state| {
            let representation = if matches!(box_state.owner, BoxOwner::System) {
                BoxRepresentation::Hydrated
            } else if let Representation::Summarized { text, .. } = &box_state.representation {
                BoxRepresentation::Summarized(text.clone())
            } else {
                BoxRepresentation::Hydrated
            };
            (box_state.id, representation)
        })
        .collect::<BTreeMap<_, _>>();
    let projection = state.projection_with_box_representations(&desired)?;
    let limit = state.ingress_initial_context_limit();
    if projection.estimated_tokens <= limit {
        return Ok(HistoryIngressRepresentationPlan {
            desired,
            fits: true,
        });
    }

    let mut unprotected = state
        .active_boxes()
        .filter(|box_state| !history_ingress_box_is_protected(box_state))
        .map(|box_state| {
            let target = if matches!(
                desired.get(&box_state.id),
                Some(BoxRepresentation::Summarized(_))
            ) {
                BoxRepresentation::Dehydrated
            } else if let Some((tool_name, characters)) = tool_invocation(box_state)
                && characters > INLINE_TOOL_INVOCATION_CHARACTERS
            {
                BoxRepresentation::Summarized(format!(
                    "Tool invocation: {tool_name} {{arguments dehydrated: {} characters}}.",
                    decimal_with_commas(characters)
                ))
            } else {
                BoxRepresentation::Dehydrated
            };
            (box_state.id, target)
        })
        .collect::<Vec<_>>();
    if reduce_history_ingress_boxes(state, &mut desired, &mut unprotected, limit)? {
        return Ok(HistoryIngressRepresentationPlan {
            desired,
            fits: true,
        });
    }

    let mut protected = state
        .active_boxes()
        .filter(|box_state| history_ingress_box_is_protected(box_state))
        .map(|box_state| (box_state.id, BoxRepresentation::Dehydrated))
        .collect::<Vec<_>>();
    if reduce_history_ingress_boxes(state, &mut desired, &mut protected, limit)? {
        return Ok(HistoryIngressRepresentationPlan {
            desired,
            fits: true,
        });
    }

    let mut remaining = state
        .active_boxes()
        .map(|box_state| (box_state.id, BoxRepresentation::Dehydrated))
        .collect::<Vec<_>>();
    let fits = reduce_history_ingress_boxes(state, &mut desired, &mut remaining, limit)?;
    Ok(HistoryIngressRepresentationPlan { desired, fits })
}

fn reduce_history_ingress_boxes(
    state: &super::chatend::Chatend,
    desired: &mut BTreeMap<BoxId, BoxRepresentation>,
    candidates: &mut Vec<(BoxId, BoxRepresentation)>,
    limit: u64,
) -> anyhow::Result<bool> {
    let projection = state.projection_with_box_representations(desired)?;
    let rendered_tokens = projection
        .items
        .iter()
        .filter(|item| !item.marker)
        .map(|item| (item.box_id, item.approximate_tokens))
        .collect::<BTreeMap<_, _>>();
    candidates.retain(|(box_id, target)| desired.get(box_id) != Some(target));
    candidates.sort_by(|left, right| {
        rendered_tokens
            .get(&right.0)
            .copied()
            .unwrap_or_default()
            .cmp(&rendered_tokens.get(&left.0).copied().unwrap_or_default())
            .then_with(|| left.0.cmp(&right.0))
    });
    for (box_id, target) in candidates.drain(..) {
        desired.insert(box_id, target);
        if state
            .projection_with_box_representations(desired)?
            .estimated_tokens
            <= limit
        {
            return Ok(true);
        }
    }
    Ok(state
        .projection_with_box_representations(desired)?
        .estimated_tokens
        <= limit)
}

fn history_ingress_box_is_protected(state: &super::chatend::BoxState) -> bool {
    if matches!(state.owner, BoxOwner::System)
        || matches!(state.owner, BoxOwner::User) && state.name == "User message"
        || matches!(state.owner, BoxOwner::Kennedy) && state.name == "Kennedy message"
    {
        return true;
    }
    if state
        .canonical
        .content
        .metadata
        .get("kwebRole")
        .and_then(Value::as_str)
        .is_some_and(|role| matches!(role, "direct" | "fixed" | "active"))
    {
        return true;
    }
    tool_invocation(state)
        .is_some_and(|(_, characters)| characters <= INLINE_TOOL_INVOCATION_CHARACTERS)
}

fn tool_invocation(state: &super::chatend::BoxState) -> Option<(&str, usize)> {
    let BoxOwner::Kennedy = &state.owner else {
        return None;
    };
    let tool_name = state.name.strip_prefix("Kennedy tool call: ")?;
    Some((tool_name, state.canonical.content.text.chars().count()))
}

fn decimal_with_commas(value: usize) -> String {
    let digits = value.to_string();
    let first = digits.len() % 3;
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    if first > 0 {
        output.push_str(&digits[..first]);
    }
    for chunk in digits.as_bytes()[first..].chunks(3) {
        if !output.is_empty() {
            output.push(',');
        }
        output.push_str(std::str::from_utf8(chunk).expect("decimal digits are UTF-8"));
    }
    output
}

fn unique_kweb_slot(logical: &str, used: &mut HashSet<String>) -> String {
    if used.insert(logical.to_owned()) {
        return logical.to_owned();
    }
    let mut generation = 2_u64;
    loop {
        let candidate = format!("{logical}#generation-{generation}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        generation += 1;
    }
}

struct ToolCall {
    name: String,
    arguments: Value,
}

#[derive(Clone)]
struct FreeformWriteRequest {
    name: String,
    path: String,
    update_description: String,
}

struct PendingFreeformWrite {
    request: FreeformWriteRequest,
    call_box_id: BoxId,
}

fn freeform_write_request(arguments: &Value) -> anyhow::Result<FreeformWriteRequest> {
    validate_arguments(arguments, &["name", "path", "updateDescription"], &[])?;
    let name = nonempty_string(arguments, "name", 255)?;
    let path = nonempty_string(arguments, "path", 4_096)?;
    let update_description = nonempty_string(arguments, "updateDescription", 4_000)?;
    anyhow::ensure!(
        !path.contains(['\r', '\n']),
        "path must contain exactly one line"
    );
    anyhow::ensure!(
        !update_description.contains(['\r', '\n']),
        "updateDescription must contain exactly one line"
    );
    Ok(FreeformWriteRequest {
        name,
        path,
        update_description,
    })
}

fn captured_write_box_content(request: &FreeformWriteRequest, contents: String) -> BoxContent {
    BoxContent {
        text: contents,
        objects: Vec::new(),
        metadata: json!({
            "capturedFreeformOutput":true,
            "toolName":WRITE_FILE_FREEFORM_RUST_LIB_TOOL,
            "arguments":{
                "name":request.name,
                "path":request.path,
                "updateDescription":request.update_description,
            },
        }),
    }
}

fn ensure_final_newline(mut contents: String) -> String {
    if !contents.ends_with('\n') {
        contents.push('\n');
    }
    contents
}

fn captured_write_summary(request: &FreeformWriteRequest) -> String {
    format!(
        "Kennedy called write-file on {} in {}, and she describes the update as: {}",
        request.path, request.name, request.update_description
    )
}

fn tool_call_box_content(call: &ToolCall) -> anyhow::Result<BoxContent> {
    if call.name == WRITE_RUST_LIB_TOOL {
        let name = call
            .arguments
            .get("name")
            .and_then(Value::as_str)
            .map(|name| name.chars().take(255).collect::<String>());
        let file_count = call
            .arguments
            .get("files")
            .and_then(Value::as_array)
            .map(Vec::len);
        return Ok(BoxContent {
            text: serde_json::to_string_pretty(&json!({
                "name":call.name,
                "arguments":{
                    "name":name,
                    "fileCount":file_count,
                    "completeFileContents":"omitted from active context; retained in the durable tool invocation"
                }
            }))?,
            objects: Vec::new(),
            metadata: json!({
                "compactedToolInvocation":true,
                "toolName":call.name,
            }),
        });
    }
    Ok(BoxContent::text(serde_json::to_string_pretty(
        &json!({"name":call.name,"arguments":call.arguments}),
    )?))
}

fn rust_lib_box_content(snapshot: &LibrarySnapshot) -> BoxContent {
    BoxContent {
        text: snapshot.text.clone(),
        objects: Vec::new(),
        metadata: json!({"managedRustLibrary":snapshot.name}),
    }
}

fn rust_lib_logical_name(state: &super::chatend::BoxState, fallback: &str) -> String {
    state
        .canonical
        .content
        .metadata
        .get("managedRustLibrary")
        .and_then(Value::as_str)
        .unwrap_or(fallback)
        .to_owned()
}

fn rust_lib_box_id(journal: &SessionJournal, name: &str) -> Option<BoxId> {
    journal
        .state()
        .tools
        .get(RUST_LIB_TOOL_INSTANCE)?
        .slots
        .iter()
        .find_map(|slot| {
            if slot.retired {
                return None;
            }
            let state = journal.state().box_state(slot.box_id)?;
            (rust_lib_logical_name(state, &slot.slot) == name).then_some(slot.box_id)
        })
}

fn prospective_rust_lib_box_updates(
    journal: &SessionJournal,
    call: &ToolCall,
) -> BTreeMap<BoxId, BoxContent> {
    if call.name != WRITE_RUST_LIB_TOOL {
        return BTreeMap::new();
    }
    let Some(snapshot) = proposed_write_snapshot(&call.arguments) else {
        return BTreeMap::new();
    };
    let Some(box_id) = rust_lib_box_id(journal, &snapshot.name) else {
        return BTreeMap::new();
    };
    BTreeMap::from([(box_id, rust_lib_box_content(&snapshot))])
}

fn apply_rust_lib_snapshot(
    journal: &mut SessionJournal,
    snapshot: LibrarySnapshot,
) -> anyhow::Result<BoxId> {
    let current = journal
        .state()
        .tools
        .get(RUST_LIB_TOOL_INSTANCE)
        .cloned()
        .unwrap_or_default();
    let mut selected_slot = None;
    let mut slots = Vec::with_capacity(current.slots.len() + 1);
    let mut used_slots = current
        .slots
        .iter()
        .map(|slot| slot.slot.clone())
        .collect::<HashSet<_>>();
    for slot in &current.slots {
        let state = journal
            .state()
            .box_state(slot.box_id)
            .context("managed Rust library slot box is missing")?;
        let selected = selected_slot.is_none()
            && !slot.retired
            && rust_lib_logical_name(state, &slot.slot) == snapshot.name;
        if selected {
            selected_slot = Some(slot.slot.clone());
            slots.push(ToolSlotInput {
                slot: slot.slot.clone(),
                name: format!("Managed Rust library {}", snapshot.name),
                content: rust_lib_box_content(&snapshot),
                retired: false,
            });
        } else {
            slots.push(ToolSlotInput {
                slot: slot.slot.clone(),
                name: state.name.clone(),
                content: state.canonical.content.clone(),
                retired: slot.retired,
            });
        }
    }
    let selected_slot = selected_slot.unwrap_or_else(|| {
        let slot = unique_kweb_slot(&snapshot.name, &mut used_slots);
        slots.push(ToolSlotInput {
            slot: slot.clone(),
            name: format!("Managed Rust library {}", snapshot.name),
            content: rust_lib_box_content(&snapshot),
            retired: false,
        });
        slot
    });
    journal.apply_tool_slots(now(), RUST_LIB_TOOL_INSTANCE, slots)?;
    journal
        .state()
        .tools
        .get(RUST_LIB_TOOL_INSTANCE)
        .and_then(|tool| {
            tool.slots
                .iter()
                .find(|slot| slot.slot == selected_slot && !slot.retired)
        })
        .map(|slot| slot.box_id)
        .context("managed Rust library box was not installed")
}

struct ToolOutcome {
    text: String,
    store_result: bool,
    ok: bool,
    end_session: bool,
    freeform_write: Option<FreeformWriteRequest>,
}

#[derive(Debug)]
struct AgentLoopRoundLimitError;

impl std::fmt::Display for AgentLoopRoundLimitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Kennedy exceeded the {AGENT_LOOP_ROUND_LIMIT}-round tool-loop safety limit"
        )
    }
}

impl std::error::Error for AgentLoopRoundLimitError {}

pub(crate) fn is_agent_loop_round_limit(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AgentLoopRoundLimitError>().is_some()
}

impl Session {
    pub(crate) async fn new(
        api: Api,
        manuals: Manuals,
        runtime: RuntimeModel,
        mut options: SessionOptions,
        restored: Option<&Value>,
    ) -> anyhow::Result<Self> {
        if let Some(state) = restored {
            restore_session_type(&mut options, state);
            options.channel = state.get("channel").cloned().unwrap_or(options.channel);
            options.free_time = state.get("freeTime").cloned().unwrap_or(options.free_time);
            options.orchestration = state
                .get("orchestration")
                .cloned()
                .unwrap_or(options.orchestration);
        }
        options
            .reference_root_node_ids
            .retain(|id| !options.root_node_ids.contains(id));
        options.reference_root_node_ids.sort();
        options.reference_root_node_ids.dedup();

        let started_at = restored
            .and_then(|state| state.get("startedAt"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let rust_lib_session_id = restored
            .and_then(|state| state.get("rustLibSessionId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(options.rust_lib_session_id.clone())
            .unwrap_or_else(|| format!("kennedy:{}", Uuid::new_v4()));
        let journal_path = restored
            .and_then(|state| state.get("journalPath"))
            .and_then(Value::as_str)
            .map(PathBuf::from);
        let source_session_type = options.source_session_type.clone().or_else(|| {
            restored
                .and_then(|state| state.get("sourceSessionType"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        let session_context = if options.session_type == "telegram-group" {
            format_telegram_group_context(&options.channel)
        } else if options.session_type == "free-time" {
            free_time_schedule(&options.free_time)
        } else {
            String::new()
        };
        let system_prompt = if matches!(options.mode, AgentMode::Ingress { .. }) {
            manuals.compose_ingress(
                &runtime,
                source_session_type.as_deref().unwrap_or("conversation"),
                &session_context,
            )?
        } else {
            manuals.compose_conversation(&runtime, &options.session_type, &session_context)?
        };

        let (mut journal, _new_journal) = if let Some(path) = journal_path {
            let session_id = path
                .file_stem()
                .and_then(|value| value.to_str())
                .context("session-log filename is not valid UTF-8")?
                .to_owned();
            (
                SessionJournal::open_with_metadata(
                    &path,
                    SessionMetadata {
                        session_id,
                        kind: session_kind(&options.session_type, &options.mode),
                        created_at: started_at.clone(),
                        effective_context_tokens: runtime.context_window_tokens,
                        channel: options.channel.clone(),
                    },
                )
                .with_context(|| {
                    format!(
                        "opening authoritative session log {} (legacy snapshots are intentionally unsupported)",
                        path.display()
                    )
                })?,
                false,
            )
        } else {
            let session_id = Uuid::new_v4().to_string();
            #[cfg(test)]
            let root = std::env::temp_dir().join("kennedy-session-log-tests");
            #[cfg(not(test))]
            let root = PathBuf::from("./data/sessions/in-progress");
            let path = root.join(format!("{session_id}.session-log"));
            (
                SessionJournal::create(
                    &path,
                    SessionMetadata {
                        session_id,
                        kind: session_kind(&options.session_type, &options.mode),
                        created_at: started_at.clone(),
                        effective_context_tokens: runtime.context_window_tokens,
                        channel: options.channel.clone(),
                    },
                )?,
                true,
            )
        };
        let mut context = KmapContext::new(api.clone(), options.root_node_ids.clone())?;
        restore_kweb_context(&journal, &mut context)?;
        let plan = KwebPlan::restore(restored, &journal)?;
        let transcript = transcript_from_journal(&journal);
        let journal_pending_external = transcript.iter().rev().find_map(|entry| {
            if entry.get("role").and_then(Value::as_str) != Some("user") {
                return None;
            }
            let id = entry.get("externalEventId").and_then(Value::as_str)?;
            let answered = transcript.iter().any(|candidate| {
                matches!(
                    candidate.get("role").and_then(Value::as_str),
                    Some("kennedy" | "system")
                ) && candidate.get("externalEventId").and_then(Value::as_str) == Some(id)
            });
            (!answered).then(|| id.to_owned())
        });
        let restored_pending = restored
            .and_then(|state| state.get("pendingTurn"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let pending_external_event_id = restored
            .and_then(|state| state.get("pendingExternalEventId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(journal_pending_external);

        let needs_initialization = !journal
            .state()
            .boxes
            .values()
            .any(|state| matches!(state.owner, BoxOwner::System));
        let commit_receipt = restore_commit_receipt(restored)?;
        let commit_author = restored
            .and_then(|state| state.get("commitAuthor"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| runtime.attribution());
        if let Some(receipt) = &commit_receipt {
            journal.mark_completed(receipt.session_object_id.clone());
        }
        let completed =
            journal.state().completed_session_object.is_some() || commit_receipt.is_some();
        let mut session = Self {
            api,
            runtime,
            journal,
            plan,
            session_type: options.session_type,
            channel: options.channel,
            free_time: options.free_time,
            orchestration: options.orchestration,
            provenance_id: options.provenance_id,
            rust_lib_session_id,
            root_node_ids: options.root_node_ids,
            reference_root_node_ids: options.reference_root_node_ids,
            started_at,
            transcript,
            pending_turn: restored_pending || pending_external_event_id.is_some(),
            pending_external_event_id,
            completed,
            rounds_used: restored
                .and_then(|state| state.get("roundsUsed"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            commit_receipt,
            commit_author,
            mode: options.mode,
            source_session_type,
            group_context: options.group_context,
            context,
            free_time_end_reason: None,
            fatal_persistence_error: None,
        };

        if session.journal.is_sealed() {
            anyhow::ensure!(
                !matches!(session.mode, AgentMode::Conversation),
                "a read-only conversation has an unexpectedly sealed session log"
            );
            if session.commit_receipt.is_none() {
                session.finalize_kweb_session()?;
            }
            session.completed = true;
            return Ok(session);
        }

        if needs_initialization {
            session.journal.create_box(
                now(),
                "Kennedy system prompt",
                BoxOwner::System,
                BoxContent::text(&system_prompt),
            )?;
            let roots = session.root_node_ids.clone();
            for root in &roots {
                session.record_tool_invocation("LoadNode", json!({"identifier":root}))?;
            }
            let result = session.context.load_durable_batch(&roots).await?;
            session.sync_kweb_boxes()?;
            session.record_tool_completion(
                "LoadNode",
                json!({"ok":true,"automatic":true,"identifiers":roots,"result":result}),
            )?;
        } else {
            session.sync_kweb_boxes()?;
        }
        if matches!(session.mode, AgentMode::Ingress { .. })
            && !session.completed
            && !session.journal.state().history_ingress_started
            && !needs_initialization
        {
            session.prepare_history_ingress(&system_prompt).await?;
        }
        Ok(session)
    }

    async fn prepare_history_ingress(&mut self, prompt: &str) -> anyhow::Result<()> {
        if !self.journal.state().source_terminated {
            self.journal.record(
                now(),
                EventKind::SourceTerminated {
                    reason: "history_ingress".into(),
                },
            )?;
        }
        let system_box = self
            .journal
            .state()
            .boxes
            .values()
            .find(|state| matches!(state.owner, BoxOwner::System))
            .map(|state| state.id)
            .context("session has no system-prompt box")?;
        self.journal
            .update_box(now(), system_box, BoxContent::text(prompt))?;
        let ingress_kind = session_kind(&self.session_type, &self.mode);
        if self.journal.state().metadata.effective_context_tokens
            != self.runtime.context_window_tokens
            || self.journal.state().metadata.kind != ingress_kind
        {
            self.journal
                .configure_context(ingress_kind, self.runtime.context_window_tokens);
        }
        self.revalidate_loaded_nodes().await?;
        let plan = history_ingress_representation_plan(self.journal.state())?;
        self.journal
            .apply_box_representations(now(), &plan.desired)?;
        if !plan.fits {
            self.journal.record(
                now(),
                EventKind::Note {
                    label: INGRESS_FORCE_COMMIT_NOTE.into(),
                    value: json!({
                        "reason":"fully_dehydrated_context_above_initial_target",
                        "estimatedTokens":self.journal.state().projection().estimated_tokens,
                        "initialTargetTokens":self.journal.state().ingress_initial_context_limit(),
                    }),
                },
            )?;
            self.pending_turn = false;
            self.finalize_kweb_session()?;
            self.completed = true;
            return Ok(());
        }
        self.journal
            .record(now(), EventKind::HistoryIngressStarted)?;
        self.pending_turn = true;
        Ok(())
    }

    async fn revalidate_loaded_nodes(&mut self) -> anyhow::Result<()> {
        let previous = self
            .context
            .ordered_full_node_ids()
            .into_iter()
            .filter_map(|id| {
                self.context
                    .nodes_by_id
                    .get(&id)
                    .cloned()
                    .map(|node| (id, node))
            })
            .collect::<BTreeMap<_, _>>();
        let direct = self.context.loaded_node_ids.clone();
        for id in &direct {
            self.context.load_durable(id).await?;
        }
        let direct = direct.into_iter().collect::<HashSet<_>>();
        for id in self.context.ordered_full_node_ids() {
            if direct.contains(&id) {
                continue;
            }
            let latest = self.api.kmap_node(&id).await?;
            self.context.refresh(vec![latest])?;
        }
        self.sync_kweb_boxes()?;
        let changed = previous
            .iter()
            .filter(|(id, before)| self.context.nodes_by_id.get(*id) != Some(*before))
            .map(|(id, _)| id.as_str())
            .collect::<HashSet<_>>();
        let affected = self
            .journal
            .state()
            .tools
            .get(KWEB_TOOL_INSTANCE)
            .into_iter()
            .flat_map(|tool| &tool.slots)
            .filter_map(|slot| {
                let state = self.journal.state().box_state(slot.box_id)?;
                let id = kweb_node_identifier(state)?;
                (state.active && changed.contains(id)).then_some(slot.box_id)
            })
            .collect::<Vec<_>>();
        for box_id in affected {
            let notice = self.journal.create_box(
                now(),
                format!("Kweb update for box {box_id}"),
                BoxOwner::Controller,
                BoxContent::text(format!(
                    "The canonical Kweb revision underlying box {box_id} changed."
                )),
            )?;
            self.journal.dehydrate_box(now(), notice)?;
        }
        Ok(())
    }

    pub(crate) async fn stage_ingress_source(
        &mut self,
        text: &str,
        metadata: &Value,
    ) -> anyhow::Result<()> {
        if self.completed {
            self.pending_turn = false;
            return Ok(());
        }
        if self.journal.state().history_ingress_started {
            self.pending_turn = true;
            return Ok(());
        }
        self.stage_user_input_inner(text.trim(), metadata, Vec::new())?;
        let prompt = self
            .journal
            .state()
            .boxes
            .values()
            .find(|state| matches!(state.owner, BoxOwner::System))
            .map(|state| state.canonical.content.text.clone())
            .context("ingress session has no system prompt")?;
        self.prepare_history_ingress(&prompt).await?;
        self.pending_turn = !self.completed;
        Ok(())
    }

    fn stage_user_input(&mut self, text: &str, metadata: &Value) -> Option<InputStage> {
        let text = text.trim();
        let attachments = metadata
            .get("attachments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if text.is_empty() && attachments.is_empty() && metadata.get("media").is_none() {
            return None;
        }
        let result = self.stage_user_input_inner(text, metadata, attachments);
        match result {
            Ok(stage) => Some(stage),
            Err(error) => {
                self.fatal_persistence_error = Some(error.to_string());
                tracing::error!(error=%error, "Could not durably stage session input");
                Some(InputStage::Accepted)
            }
        }
    }

    fn stage_user_input_inner(
        &mut self,
        text: &str,
        metadata: &Value,
        attachments: Vec<Value>,
    ) -> anyhow::Result<InputStage> {
        let mut content = BoxContent::text(text);
        content.metadata = message_metadata_without_attachment_payloads(metadata);
        let mut attachment_boxes = Vec::new();
        let mut attachment_names = Vec::new();
        for attachment in &attachments {
            let file_name = attachment
                .get("fileName")
                .and_then(Value::as_str)
                .unwrap_or("document");
            attachment_names.push(file_name.to_owned());
            if let Some(pending_id) = attachment.get("pendingId").and_then(Value::as_str) {
                let pending_id = PendingId::parse(pending_id.to_owned())?;
                anyhow::ensure!(
                    self.journal.objects().contains_key(&pending_id),
                    "attached object {pending_id} is not staged in this session"
                );
                content.objects.push(pending_id.to_string());
            } else if let Some(data_url) = attachment.get("dataUrl").and_then(Value::as_str) {
                let (media_type, bytes) = decode_data_url(data_url)?;
                let id = self.journal.stage_object(
                    now(),
                    media_type,
                    Some(file_name.to_owned()),
                    attachment_metadata_without_payload(attachment),
                    &bytes,
                )?;
                content.objects.push(id.to_string());
            }
            if let Some(extracted) = attachment
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                attachment_boxes.push((
                    format!("User attachment text: {file_name}"),
                    BoxContent {
                        text: extracted.into(),
                        objects: Vec::new(),
                        metadata: json!({
                            "boxKind":"attachmentText",
                            "attachment":attachment_metadata_without_payload(attachment),
                        }),
                    },
                ));
            }
        }
        if let Some(media) = metadata.get("media") {
            if let Some(pending_id) = media.get("pendingId").and_then(Value::as_str) {
                let pending_id = PendingId::parse(pending_id.to_owned())?;
                anyhow::ensure!(
                    self.journal.objects().contains_key(&pending_id),
                    "voice object {pending_id} is not staged in this session"
                );
                content.objects.push(pending_id.to_string());
            } else if let Some(data_url) = media.get("dataUrl").and_then(Value::as_str) {
                let (media_type, bytes) = decode_data_url(data_url)?;
                let id = self.journal.stage_object(
                    now(),
                    media_type,
                    media
                        .get("fileName")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    attachment_metadata_without_payload(media),
                    &bytes,
                )?;
                content.objects.push(id.to_string());
            }
        }
        if content.text.trim().is_empty()
            && content.objects.is_empty()
            && !attachment_names.is_empty()
        {
            content.text = attachment_names
                .iter()
                .map(|name| format!("Attachment provided: {name}"))
                .collect::<Vec<_>>()
                .join("\n");
        }
        let visible = if content.text.trim().is_empty() {
            content
                .objects
                .iter()
                .map(|id| format!("Object provided: {id}"))
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            content.text.clone()
        };
        let mut prospective_boxes =
            vec![("User message".to_owned(), BoxOwner::User, content.clone())];
        prospective_boxes.extend(
            attachment_boxes
                .iter()
                .map(|(name, content)| (name.clone(), BoxOwner::User, content.clone())),
        );
        if !matches!(self.mode, AgentMode::Ingress { .. }) {
            let projection = self
                .journal
                .state()
                .projection_with_new_boxes(&prospective_boxes)?;
            let limit = self.journal.state().live_context_limit();
            if projection.estimated_tokens > limit {
                self.record_live_capacity_error(
                    "Your message",
                    projection.estimated_tokens,
                    limit,
                    metadata.get("externalEventId").and_then(Value::as_str),
                )?;
                return Ok(InputStage::RejectedForCapacity);
            }
        }
        for (name, owner, content) in prospective_boxes {
            self.journal.create_box(now(), name, owner, content)?;
        }
        let mut transcript = json!({"role":"user","content":visible});
        if let Some(id) = metadata.get("externalEventId").and_then(Value::as_str) {
            transcript["externalEventId"] = json!(id);
        }
        self.transcript.push(transcript);
        Ok(InputStage::Accepted)
    }

    pub(crate) fn append_final_user_message(&mut self, text: &str, metadata: &Value) -> bool {
        self.stage_user_input(text, metadata).is_some()
    }

    pub(crate) fn stage_source_message(
        &mut self,
        kennedy: bool,
        text: &str,
        metadata: Value,
    ) -> anyhow::Result<()> {
        let owner = if kennedy {
            BoxOwner::Kennedy
        } else {
            BoxOwner::User
        };
        let name = if kennedy {
            "Kennedy message"
        } else {
            "User message"
        };
        self.journal.create_box(
            now(),
            name,
            owner,
            BoxContent {
                text: text.into(),
                objects: Vec::new(),
                metadata: metadata.clone(),
            },
        )?;
        self.transcript.push(json!({
            "role":if kennedy {"kennedy"} else {"user"},
            "content":text,
            "metadata":metadata,
        }));
        Ok(())
    }

    pub(crate) fn answer_for_external_event(&self, id: &str) -> Option<&Value> {
        self.transcript.iter().rev().find(|entry| {
            matches!(
                entry.get("role").and_then(Value::as_str),
                Some("kennedy" | "system")
            ) && entry.get("externalEventId").and_then(Value::as_str) == Some(id)
        })
    }

    fn record_live_capacity_error(
        &mut self,
        attempted_operation: &str,
        projected_tokens: u64,
        limit_tokens: u64,
        external_event_id: Option<&str>,
    ) -> anyhow::Result<String> {
        if let Some(id) = external_event_id
            && let Some(existing) = self.answer_for_external_event(id)
            && existing.get("role").and_then(Value::as_str) == Some("system")
            && let Some(text) = existing.get("content").and_then(Value::as_str)
        {
            let text = text.to_owned();
            if self.journal.state().projection().estimated_tokens
                > self.journal.state().forced_ingress_context_limit()
                && !self.journal.state().source_terminated
            {
                self.journal.record(
                    now(),
                    EventKind::SourceTerminated {
                        reason: "context_capacity_limit".into(),
                    },
                )?;
            }
            return Ok(text);
        }
        self.journal.record(
            now(),
            EventKind::CapacityError {
                attempted_operation: attempted_operation.into(),
                projected_tokens,
                limit_tokens,
            },
        )?;
        let text = format!(
            "{attempted_operation} was not added because it would use approximately \
             {projected_tokens} context tokens, above the 70% limit of {limit_tokens}. \
             Reduce the size of the request or dehydrate existing context and try again."
        );
        let mut metadata = json!({
            "transcriptRole":"system",
            "capacityError":true,
            "projectedTokens":projected_tokens,
            "limitTokens":limit_tokens,
        });
        if let Some(id) = external_event_id {
            metadata["externalEventId"] = json!(id);
        }
        self.journal.create_box(
            now(),
            CAPACITY_ERROR_BOX_NAME,
            BoxOwner::Controller,
            BoxContent {
                text: text.clone(),
                objects: Vec::new(),
                metadata,
            },
        )?;
        let mut transcript = json!({"role":"system","content":text});
        if let Some(id) = external_event_id {
            transcript["externalEventId"] = json!(id);
        }
        self.transcript.push(transcript);
        if self.journal.state().projection().estimated_tokens
            > self.journal.state().forced_ingress_context_limit()
            && !self.journal.state().source_terminated
        {
            self.journal.record(
                now(),
                EventKind::SourceTerminated {
                    reason: "context_capacity_limit".into(),
                },
            )?;
        }
        Ok(text)
    }

    fn request_ingress_force_commit(
        &mut self,
        reason: &str,
        projected_tokens: u64,
    ) -> anyhow::Result<()> {
        if self.ingress_force_commit_requested() {
            return Ok(());
        }
        self.journal.record(
            now(),
            EventKind::Note {
                label: INGRESS_FORCE_COMMIT_NOTE.into(),
                value: json!({
                    "reason":reason,
                    "projectedTokens":projected_tokens,
                    "limitTokens":self.journal.state().ingress_context_limit(),
                }),
            },
        )?;
        Ok(())
    }

    fn ingress_force_commit_requested(&self) -> bool {
        self.journal.state().events.iter().rev().any(|event| {
            matches!(
                &event.kind,
                EventKind::Note { label, .. } if label == INGRESS_FORCE_COMMIT_NOTE
            )
        })
    }

    fn pending_capacity_error(&self) -> bool {
        self.pending_external_event_id
            .as_deref()
            .and_then(|id| self.answer_for_external_event(id))
            .is_some_and(|entry| entry.get("role").and_then(Value::as_str) == Some("system"))
    }

    fn has_live_capacity_error(&self) -> bool {
        self.journal.state().active_boxes().any(|box_state| {
            box_state
                .canonical
                .content
                .metadata
                .get("capacityError")
                .and_then(Value::as_bool)
                == Some(true)
        })
    }

    fn current_live_capacity_error(&self) -> bool {
        match self.mode {
            AgentMode::Conversation => self.pending_capacity_error(),
            AgentMode::FreeTime => self.has_live_capacity_error(),
            AgentMode::Ingress { .. } => false,
        }
    }

    pub(crate) fn requires_history_ingress(&self) -> bool {
        matches!(self.mode, AgentMode::Conversation) && self.journal.state().source_terminated
    }

    pub(crate) fn stage_free_time_opening(&mut self) -> bool {
        if self.pending_turn {
            return false;
        }
        let mut blocks = vec![free_time_opening(&self.free_time)];
        if let Some(message) = self
            .free_time
            .get("handoffMessage")
            .and_then(Value::as_str)
            .filter(|message| !message.trim().is_empty())
        {
            blocks.push(format!(
                "Message from the previous self-time session:\n\n{message}"
            ));
        }
        let Some(stage) = self.stage_user_input(&blocks.join("\n\n"), &json!({"kind":"self-time"}))
        else {
            return false;
        };
        self.pending_turn = matches!(stage, InputStage::Accepted);
        true
    }

    pub(crate) fn begin_user_turn(&mut self, text: &str, metadata: &Value) -> bool {
        if self.pending_turn {
            return false;
        }
        let Some(stage) = self.stage_user_input(text, metadata) else {
            return false;
        };
        if matches!(stage, InputStage::RejectedForCapacity) {
            self.pending_external_event_id = None;
            return true;
        }
        self.rounds_used = 0;
        self.pending_turn = true;
        self.pending_external_event_id = metadata
            .get("externalEventId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        true
    }

    pub(crate) fn reset_exhausted_turn_rounds_for_retry(&mut self) {
        if matches!(self.mode, AgentMode::Conversation)
            && self.rounds_used >= AGENT_LOOP_ROUND_LIMIT
        {
            self.rounds_used = 0;
        }
    }

    pub(crate) async fn run_pending_turn<C, F>(
        &mut self,
        operation_id: Uuid,
        mut checkpoint: C,
    ) -> anyhow::Result<Option<String>>
    where
        C: FnMut(Value) -> F,
        F: Future<Output = anyhow::Result<()>>,
    {
        if let Some(error) = self.fatal_persistence_error.take() {
            anyhow::bail!("session journal write failed: {error}");
        }
        if !self.pending_turn {
            return Ok(None);
        }
        let result = self.run_agent_loop(operation_id, &mut checkpoint).await?;
        match self.mode {
            AgentMode::Conversation => {
                if self.journal.state().source_terminated {
                    self.pending_turn = false;
                    self.pending_external_event_id = None;
                    checkpoint(self.snapshot()?).await?;
                    return Ok(None);
                }
                let Some(answer) = result else {
                    if self
                        .pending_external_event_id
                        .as_deref()
                        .and_then(|id| self.answer_for_external_event(id))
                        .is_some_and(|entry| {
                            entry.get("role").and_then(Value::as_str) == Some("system")
                        })
                    {
                        self.pending_turn = false;
                        self.pending_external_event_id = None;
                        checkpoint(self.snapshot()?).await?;
                        return Ok(None);
                    }
                    anyhow::bail!(
                        "Kennedy ended a conversational turn without an assistant response"
                    );
                };
                let mut response = json!({"role":"kennedy","content":answer});
                if let Some(id) = &self.pending_external_event_id {
                    response["externalEventId"] = json!(id);
                }
                self.transcript.push(response);
                self.pending_turn = false;
                self.pending_external_event_id = None;
                checkpoint(self.snapshot()?).await?;
                Ok(Some(answer))
            }
            AgentMode::FreeTime | AgentMode::Ingress { .. } => {
                self.pending_turn = false;
                self.pending_external_event_id = None;
                if matches!(self.mode, AgentMode::FreeTime | AgentMode::Ingress { .. }) {
                    self.finalize_kweb_session()?;
                    self.completed = true;
                }
                checkpoint(self.snapshot()?).await?;
                Ok(None)
            }
        }
    }

    async fn run_agent_loop<C, F>(
        &mut self,
        operation_id: Uuid,
        checkpoint: &mut C,
    ) -> anyhow::Result<Option<String>>
    where
        C: FnMut(Value) -> F,
        F: Future<Output = anyhow::Result<()>>,
    {
        for round in self.rounds_used..AGENT_LOOP_ROUND_LIMIT {
            self.rounds_used = round + 1;
            let deadline_after_response = self.prepare_free_time_round()?;
            let input = self.journal.state().render();
            let projection = self.journal.state().projection();
            if matches!(self.mode, AgentMode::Ingress { .. }) {
                if projection.estimated_tokens > self.journal.state().ingress_context_limit()
                    || self.ingress_force_commit_requested()
                {
                    return Ok(None);
                }
            } else if projection.estimated_tokens > self.journal.state().live_context_limit() {
                let external_event_id = self.pending_external_event_id.clone();
                self.record_live_capacity_error(
                    "The pending turn",
                    projection.estimated_tokens,
                    self.journal.state().live_context_limit(),
                    external_event_id.as_deref(),
                )?;
                return Ok(None);
            }
            let manifest_hash = hex::encode(Sha256::digest(input.as_bytes()));
            self.journal.record(
                now(),
                EventKind::InferenceSubmitted {
                    manifest_hash: manifest_hash.clone(),
                    estimated_input_tokens: projection.estimated_tokens,
                    raw_estimated_input_tokens: Some(projection.raw_estimated_tokens),
                },
            )?;
            checkpoint(self.snapshot()?).await?;
            let mut request =
                kcode_codex_runtime_v2::AgentRequest::new(input, self.runtime.model.clone());
            request.reasoning_effort = codex_reasoning_effort(&self.runtime.reasoning_effort)?;
            request.previous_thread_id = None;
            request.tools = vec![call_ktool_definition()];
            if let Some(timeout) = self.agent_request_timeout() {
                request.timeout = timeout;
            }
            let mut turn = self.api.start_agent_turn(operation_id, request).await?;
            let mut end_session = false;
            let mut used_tool = false;
            let mut pending_freeform_write = None;
            let completed = loop {
                let event = turn
                    .next_event()
                    .await
                    .context("provider ended without a terminal turn event")??;
                match event {
                    kcode_codex_runtime_v2::AgentEvent::ProviderInput(exact) => {
                        self.journal.record(
                            now(),
                            EventKind::Note {
                                label: "provider_input".into(),
                                value: Value::String(exact),
                            },
                        )?;
                        checkpoint(self.snapshot()?).await?;
                    }
                    kcode_codex_runtime_v2::AgentEvent::ToolCall(native) => {
                        used_tool = true;
                        if pending_freeform_write.is_some() {
                            let text = format!(
                                "{WRITE_FILE_FREEFORM_RUST_LIB_TOOL} is awaiting the complete file contents; no other Ktool can run before that output."
                            );
                            self.record_tool_completion(
                                "call_ktool",
                                json!({"ok":false,"result":text}),
                            )?;
                            checkpoint(self.snapshot()?).await?;
                            turn.respond(
                                &native.call_id,
                                kcode_codex_runtime_v2::ToolResult::failure(text),
                            )
                            .await?;
                            continue;
                        }
                        let mut created_call_box_id = None;
                        let call = native_ktool_call(&native);
                        let mut outcome = match call {
                            Ok(call) => {
                                let call_name = format!("Kennedy tool call: {}", call.name);
                                let call_content = tool_call_box_content(&call)?;
                                self.record_tool_invocation(&call.name, call.arguments.clone())?;
                                let prospective_updates =
                                    prospective_rust_lib_box_updates(&self.journal, &call);
                                let prospective_projection =
                                    self.journal.state().projection_with_new_boxes_and_updates(
                                        &[(
                                            call_name.clone(),
                                            BoxOwner::Kennedy,
                                            call_content.clone(),
                                        )],
                                        &prospective_updates,
                                    )?;
                                let prospective_tokens = prospective_projection.estimated_tokens;
                                let rejected = if !matches!(self.mode, AgentMode::Ingress { .. }) {
                                    if self.current_live_capacity_error() {
                                        let external_event_id =
                                            self.pending_external_event_id.clone();
                                        Some(self.record_live_capacity_error(
                                            &format!("Kennedy's {} tool call", call.name),
                                            self.journal.state().projection().estimated_tokens,
                                            self.journal.state().live_context_limit(),
                                            external_event_id.as_deref(),
                                        )?)
                                    } else {
                                        let limit = self.journal.state().live_context_limit();
                                        if prospective_tokens > limit {
                                            let external_event_id =
                                                self.pending_external_event_id.clone();
                                            Some(self.record_live_capacity_error(
                                                &format!("Kennedy's {} tool call", call.name),
                                                prospective_tokens,
                                                limit,
                                                external_event_id.as_deref(),
                                            )?)
                                        } else {
                                            None
                                        }
                                    }
                                } else {
                                    None
                                };
                                if let Some(text) = rejected {
                                    ToolOutcome {
                                        text,
                                        store_result: false,
                                        ok: false,
                                        end_session: false,
                                        freeform_write: None,
                                    }
                                } else {
                                    created_call_box_id = Some(self.journal.create_box(
                                        now(),
                                        call_name,
                                        BoxOwner::Kennedy,
                                        call_content,
                                    )?);
                                    if matches!(self.mode, AgentMode::Ingress { .. })
                                        && prospective_tokens
                                            > self.journal.state().ingress_context_limit()
                                    {
                                        self.request_ingress_force_commit(
                                            "tool_call_exceeded_full_window",
                                            prospective_tokens,
                                        )?;
                                        ToolOutcome {
                                            text: "The tool was not run because history ingress exceeded the full context window; the staged transaction will now be committed.".into(),
                                            store_result: false,
                                            ok: false,
                                            end_session: false,
                                            freeform_write: None,
                                        }
                                    } else {
                                        match self.execute_tool(&call, operation_id).await {
                                            Ok(outcome) => outcome,
                                            Err(error) => ToolOutcome {
                                                text: format!("{} failed: {error}", call.name),
                                                store_result: call.name != "LoadNode",
                                                ok: false,
                                                end_session: false,
                                                freeform_write: None,
                                            },
                                        }
                                    }
                                }
                            }
                            Err(error) => ToolOutcome {
                                text: format!("Invalid Ktool call: {error}"),
                                store_result: true,
                                ok: false,
                                end_session: false,
                                freeform_write: None,
                            },
                        };
                        if !matches!(self.mode, AgentMode::Ingress { .. }) {
                            let projection = if outcome.store_result {
                                self.journal.state().projection_with_new_boxes(&[(
                                    "Kennedy tool result".into(),
                                    BoxOwner::Controller,
                                    BoxContent::text(&outcome.text),
                                )])?
                            } else {
                                self.journal.state().projection()
                            };
                            let limit = self.journal.state().live_context_limit();
                            if projection.estimated_tokens > limit {
                                let external_event_id = self.pending_external_event_id.clone();
                                outcome = ToolOutcome {
                                    text: self.record_live_capacity_error(
                                        "Kennedy's tool result",
                                        projection.estimated_tokens,
                                        limit,
                                        external_event_id.as_deref(),
                                    )?,
                                    store_result: false,
                                    ok: false,
                                    end_session: false,
                                    freeform_write: None,
                                };
                            }
                        }
                        if let Some(request) = outcome.freeform_write.take() {
                            pending_freeform_write = Some(PendingFreeformWrite {
                                request,
                                call_box_id: created_call_box_id
                                    .context("freeform write call box was not created")?,
                            });
                        }
                        if outcome.store_result {
                            self.journal.create_box(
                                now(),
                                "Kennedy tool result",
                                BoxOwner::Controller,
                                BoxContent::text(&outcome.text),
                            )?;
                        }
                        let provider_result = outcome.text.clone();
                        self.record_tool_completion(
                            "call_ktool",
                            json!({"ok":outcome.ok,"result":outcome.text}),
                        )?;
                        end_session |= outcome.ok && outcome.end_session;
                        if matches!(self.mode, AgentMode::Ingress { .. })
                            && self.journal.state().projection().estimated_tokens
                                > self.journal.state().ingress_context_limit()
                        {
                            self.request_ingress_force_commit(
                                "tool_context_exceeded_full_window",
                                self.journal.state().projection().estimated_tokens,
                            )?;
                        }
                        checkpoint(self.snapshot()?).await?;
                        if matches!(self.mode, AgentMode::Ingress { .. })
                            && self.ingress_force_commit_requested()
                        {
                            return Ok(None);
                        }
                        turn.respond(
                            &native.call_id,
                            if outcome.ok {
                                kcode_codex_runtime_v2::ToolResult::success(provider_result)
                            } else {
                                kcode_codex_runtime_v2::ToolResult::failure(provider_result)
                            },
                        )
                        .await?;
                        if !matches!(self.mode, AgentMode::Ingress { .. })
                            && (self.current_live_capacity_error()
                                || self.journal.state().source_terminated)
                        {
                            return Ok(None);
                        }
                    }
                    kcode_codex_runtime_v2::AgentEvent::Completed(completed) => break completed,
                }
            };
            let usage = completed.usage.as_ref();
            let raw_context_tokens = self.journal.state().projection().raw_estimated_tokens;
            self.journal.record(
                now(),
                EventKind::ProviderReceipt {
                    manifest_hash,
                    input_tokens: usage
                        .map(|usage| usage.last_input_tokens.unwrap_or(usage.input_tokens)),
                    output_tokens: usage
                        .map(|usage| usage.last_output_tokens.unwrap_or(usage.output_tokens)),
                    raw_context_tokens: Some(raw_context_tokens),
                    provider_data: usage
                        .map(|usage| {
                            json!({
                                "inputTokens":usage.input_tokens,
                                "outputTokens":usage.output_tokens,
                                "cachedInputTokens":usage.cached_input_tokens,
                                "reasoningOutputTokens":usage.reasoning_output_tokens,
                            })
                        })
                        .unwrap_or(Value::Null),
                },
            )?;
            let mut answer = if let Some(pending) = pending_freeform_write {
                let result_metadata = pending.request.clone();
                let outcome = self
                    .complete_freeform_write(pending, completed.answer)
                    .await?;
                if outcome.store_result {
                    self.journal.create_box(
                        now(),
                        "Kennedy tool result",
                        BoxOwner::Controller,
                        BoxContent::text(&outcome.text),
                    )?;
                }
                self.journal.record(
                    now(),
                    EventKind::Note {
                        label: "write_file_freeform_result".into(),
                        value: json!({
                            "tool":WRITE_FILE_FREEFORM_RUST_LIB_TOOL,
                            "name":result_metadata.name,
                            "path":result_metadata.path,
                            "updateDescription":result_metadata.update_description,
                            "ok":outcome.ok,
                            "result":outcome.text,
                        }),
                    },
                )?;
                String::new()
            } else {
                completed.answer.trim().to_owned()
            };
            if !matches!(self.mode, AgentMode::Ingress { .. }) && self.current_live_capacity_error()
            {
                answer.clear();
            }
            if !answer.is_empty() {
                let mut content = BoxContent::text(answer.clone());
                if let Some(id) = &self.pending_external_event_id {
                    content.metadata["externalEventId"] = json!(id);
                }
                let rejected = if !matches!(self.mode, AgentMode::Ingress { .. }) {
                    let projection = self.journal.state().projection_with_new_boxes(&[(
                        "Kennedy message".into(),
                        BoxOwner::Kennedy,
                        content.clone(),
                    )])?;
                    let limit = self.journal.state().live_context_limit();
                    if projection.estimated_tokens > limit {
                        let external_event_id = self.pending_external_event_id.clone();
                        self.record_live_capacity_error(
                            "Kennedy's response",
                            projection.estimated_tokens,
                            limit,
                            external_event_id.as_deref(),
                        )?;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if rejected {
                    answer.clear();
                } else {
                    self.journal.create_box(
                        now(),
                        "Kennedy message",
                        BoxOwner::Kennedy,
                        content,
                    )?;
                    if !matches!(self.mode, AgentMode::Conversation) {
                        self.transcript
                            .push(json!({"role":"kennedy","content":answer}));
                    }
                }
            }
            if matches!(self.mode, AgentMode::Ingress { .. })
                && self.journal.state().projection().estimated_tokens
                    > self.journal.state().ingress_context_limit()
            {
                self.request_ingress_force_commit(
                    "kennedy_output_exceeded_full_window",
                    self.journal.state().projection().estimated_tokens,
                )?;
            }
            checkpoint(self.snapshot()?).await?;
            if matches!(self.mode, AgentMode::Ingress { .. })
                && self.ingress_force_commit_requested()
            {
                return Ok(None);
            }
            if !matches!(self.mode, AgentMode::Ingress { .. })
                && (self.current_live_capacity_error() || self.journal.state().source_terminated)
            {
                return Ok(None);
            }
            if end_session || deadline_after_response {
                return Ok((!answer.is_empty()).then_some(answer));
            }
            if matches!(self.mode, AgentMode::Conversation) && !answer.is_empty() {
                return Ok(Some(answer));
            }
            let solo_ingress_response =
                matches!(self.mode, AgentMode::Ingress { .. }) && !answer.is_empty();
            anyhow::ensure!(
                used_tool || solo_ingress_response,
                "provider completed without a response or tool call"
            );
            self.journal.create_box(
                now(),
                controller_box_name(&self.mode),
                BoxOwner::Controller,
                BoxContent::text(controller_message(&self.mode, &self.free_time)),
            )?;
        }
        if matches!(self.mode, AgentMode::Ingress { .. }) {
            self.request_ingress_force_commit(
                "agent_loop_round_limit",
                self.journal.state().projection().estimated_tokens,
            )?;
            checkpoint(self.snapshot()?).await?;
            return Ok(None);
        }
        Err(AgentLoopRoundLimitError.into())
    }

    async fn complete_freeform_write(
        &mut self,
        pending: PendingFreeformWrite,
        contents: String,
    ) -> anyhow::Result<ToolOutcome> {
        let request = pending.request;
        let contents = ensure_final_newline(contents);
        self.journal.update_box(
            now(),
            pending.call_box_id,
            captured_write_box_content(&request, contents.clone()),
        )?;
        self.journal
            .summarize_box(now(), pending.call_box_id, captured_write_summary(&request))?;

        let backend_arguments = json!({
            "name":request.name,
            "path":request.path,
            "contents":contents,
        });
        let preview = match self
            .api
            .rust_lib_execute(
                &self.rust_lib_session_id,
                PREVIEW_WRITE_FILE_RUST_LIB_TOOL,
                backend_arguments.clone(),
            )
            .await
        {
            Ok(preview) => preview,
            Err(error) => {
                return Ok(ToolOutcome {
                    text: format!("{WRITE_FILE_FREEFORM_RUST_LIB_TOOL} failed: {error}"),
                    store_result: true,
                    ok: false,
                    end_session: false,
                    freeform_write: None,
                });
            }
        };
        let preview = preview
            .snapshot
            .context("freeform write preview omitted the resulting library snapshot")?;
        let source_box_id = rust_lib_box_id(&self.journal, &request.name)
            .context("the managed Rust library box disappeared during freeform capture")?;
        let prospective = self.journal.state().projection_with_new_boxes_and_updates(
            &[],
            &BTreeMap::from([(source_box_id, rust_lib_box_content(&preview))]),
        )?;
        let prospective_tokens = prospective.estimated_tokens;
        let limit = if matches!(self.mode, AgentMode::Ingress { .. }) {
            self.journal.state().ingress_context_limit()
        } else {
            self.journal.state().live_context_limit()
        };
        if prospective_tokens > limit {
            if matches!(self.mode, AgentMode::Ingress { .. }) {
                self.request_ingress_force_commit(
                    "write_file_freeform_exceeded_full_window",
                    prospective_tokens,
                )?;
                return Ok(ToolOutcome {
                    text: "The file was not written because its resulting managed-library snapshot exceeded the full context window; the staged transaction will now be committed.".into(),
                    store_result: false,
                    ok: false,
                    end_session: false,
                    freeform_write: None,
                });
            }
            let external_event_id = self.pending_external_event_id.clone();
            let text = self.record_live_capacity_error(
                &format!(
                    "Kennedy's {WRITE_FILE_FREEFORM_RUST_LIB_TOOL} output for {}",
                    request.path
                ),
                prospective_tokens,
                limit,
                external_event_id.as_deref(),
            )?;
            return Ok(ToolOutcome {
                text,
                store_result: false,
                ok: false,
                end_session: false,
                freeform_write: None,
            });
        }

        let execution = match self
            .api
            .rust_lib_execute(
                &self.rust_lib_session_id,
                WRITE_FILE_FREEFORM_RUST_LIB_TOOL,
                backend_arguments,
            )
            .await
        {
            Ok(execution) => execution,
            Err(error) => {
                return Ok(ToolOutcome {
                    text: format!("{WRITE_FILE_FREEFORM_RUST_LIB_TOOL} failed: {error}"),
                    store_result: true,
                    ok: false,
                    end_session: false,
                    freeform_write: None,
                });
            }
        };
        let snapshot = execution
            .snapshot
            .context("freeform write omitted the resulting library snapshot")?;
        apply_rust_lib_snapshot(&mut self.journal, snapshot)?;
        Ok(ToolOutcome {
            text: execution.text,
            store_result: false,
            ok: true,
            end_session: false,
            freeform_write: None,
        })
    }

    async fn execute_tool(
        &mut self,
        call: &ToolCall,
        operation_id: Uuid,
    ) -> anyhow::Result<ToolOutcome> {
        self.assert_tool_allowed(&call.name)?;
        let mut end_session = false;
        let mut store_result = true;
        let mut freeform_write = None;
        let text = match call.name.as_str() {
            "EndSession" => {
                validate_arguments(&call.arguments, &[], &["message"])?;
                anyhow::ensure!(
                    !matches!(self.mode, AgentMode::Conversation),
                    "EndSession is only available during history ingress or self time"
                );
                end_session = true;
                if matches!(self.mode, AgentMode::FreeTime)
                    && let Some(message) = call
                        .arguments
                        .get("message")
                        .and_then(Value::as_str)
                        .filter(|message| !message.trim().is_empty())
                {
                    self.free_time["nextSessionMessage"] = json!(message);
                }
                "Session ending.".into()
            }
            "DehydrateBox" => {
                validate_arguments(&call.arguments, &["boxId"], &[])?;
                let id = box_id(&call.arguments, "boxId")?;
                self.journal.dehydrate_box(now(), id)?;
                format!("Dehydrated box {id}.")
            }
            "SummarizeBox" => {
                validate_arguments(&call.arguments, &["boxId", "summary"], &[])?;
                let id = box_id(&call.arguments, "boxId")?;
                let summary = nonempty_string(&call.arguments, "summary", 1_000_000)?;
                self.journal.summarize_box(now(), id, summary)?;
                format!("Summarized box {id}.")
            }
            "HydrateBox" => {
                validate_arguments(&call.arguments, &["boxId"], &[])?;
                let id = box_id(&call.arguments, "boxId")?;
                self.preflight_hydration(id)?;
                self.journal.rehydrate_box(now(), id)?;
                format!("Hydrated box {id}.")
            }
            "HydrateEvent" => {
                anyhow::ensure!(
                    matches!(self.mode, AgentMode::Ingress { .. }),
                    "HydrateEvent is only available during history ingress"
                );
                validate_arguments(&call.arguments, &["eventId"], &[])?;
                let event_id = event_id(&call.arguments, "eventId")?;
                let event = self
                    .journal
                    .state()
                    .event(event_id)
                    .cloned()
                    .with_context(|| format!("event {event_id} does not exist"))?;
                let content = BoxContent::text(serde_json::to_string_pretty(&event)?);
                self.apply_history_inspection(event_id, content)?;
                self.journal.record(
                    now(),
                    EventKind::HistoryEventInspected {
                        source_event: event_id,
                    },
                )?;
                format!("Hydrated history event {event_id}.")
            }
            "DehydrateEvent" => {
                anyhow::ensure!(
                    matches!(self.mode, AgentMode::Ingress { .. }),
                    "DehydrateEvent is only available during history ingress"
                );
                validate_arguments(&call.arguments, &["eventId"], &[])?;
                let event_id = event_id(&call.arguments, "eventId")?;
                if let Some(tool) = self.journal.state().tools.get(HISTORY_TOOL_INSTANCE)
                    && let Some(slot) = tool
                        .slots
                        .iter()
                        .find(|slot| slot.slot == event_id.to_string() && !slot.retired)
                {
                    self.journal.retire_box(now(), slot.box_id)?;
                }
                self.journal.record(
                    now(),
                    EventKind::HistoryEventReleased {
                        source_event: event_id,
                    },
                )?;
                format!("Released history event {event_id} from the active context.")
            }
            "LoadNode" => {
                validate_arguments(&call.arguments, &["identifier"], &[])?;
                let id = canonical_node_id(&call.arguments, "identifier")?;
                self.context.load_durable(&id).await?;
                let changed = self.sync_kweb_boxes()?;
                store_result = false;
                render_load_node_result(&self.journal, &changed)?
            }
            "WebSearch" => {
                validate_arguments(&call.arguments, &["question", "mode"], &[])?;
                let mode = nonempty_string(&call.arguments, "mode", 20)?;
                anyhow::ensure!(
                    matches!(mode.as_str(), "quality" | "balanced" | "fast"),
                    "mode must be quality, balanced, or fast"
                );
                let result = self
                    .api
                    .intelligence_post(
                        "/api/v1/web/search",
                        json!({
                            "provider":self.runtime.provider,
                            "model":self.runtime.model,
                            "question":nonempty_string(&call.arguments,"question",4_000)?,
                            "mode":mode,
                            "parent_operation_id":operation_id,
                        }),
                    )
                    .await?;
                render_web_search_result(&result)?
            }
            "WebFetch" => {
                validate_arguments(&call.arguments, &["url"], &[])?;
                let result = self
                    .api
                    .intelligence_post(
                        "/api/v1/web/fetch",
                        json!({
                            "url":nonempty_string(&call.arguments,"url",4_096)?,
                            "parent_operation_id":operation_id,
                        }),
                    )
                    .await?;
                render_web_fetch_result(&result)?
            }
            "ConnectNodes" => self.connect_nodes(&call.arguments)?,
            "ConsolidateFanout" => self.consolidate_fanout(&call.arguments)?,
            "SetFixedConnection" => self.set_fixed_connection(&call.arguments)?,
            "CreateNode" => self.create_node(&call.arguments)?,
            "UpdateNode" => self.update_node(&call.arguments)?,
            WRITE_FILE_FREEFORM_RUST_LIB_TOOL => {
                let request = freeform_write_request(&call.arguments)?;
                anyhow::ensure!(
                    rust_lib_box_id(&self.journal, &request.name).is_some(),
                    "Rust library {:?} is not open in this Kennedy session. Call {} first.",
                    request.name,
                    crate::rust_lib_tools::OPEN_RUST_LIB_TOOL
                );
                store_result = false;
                let acknowledgement = format!(
                    "Ready. Output the complete contents of {} only, with no Markdown fences or commentary.",
                    request.path
                );
                freeform_write = Some(request);
                acknowledgement
            }
            name if RUST_LIB_TOOLS.contains(&name) => {
                let execution = self
                    .api
                    .rust_lib_execute(&self.rust_lib_session_id, name, call.arguments.clone())
                    .await?;
                if let Some(snapshot) = execution.snapshot {
                    apply_rust_lib_snapshot(&mut self.journal, snapshot)?;
                    store_result = false;
                }
                execution.text
            }
            _ => anyhow::bail!("Tool {} is not available", call.name),
        };
        Ok(ToolOutcome {
            text,
            store_result,
            ok: true,
            end_session,
            freeform_write,
        })
    }

    fn assert_tool_allowed(&self, name: &str) -> anyhow::Result<()> {
        let write = matches!(
            name,
            "ConnectNodes"
                | "ConsolidateFanout"
                | "SetFixedConnection"
                | "CreateNode"
                | "UpdateNode"
        );
        anyhow::ensure!(
            !write || !matches!(self.mode, AgentMode::Conversation),
            "{name} requires the global Kweb write lane and is unavailable in a read-only conversation"
        );
        if name == "EndSession" {
            anyhow::ensure!(
                !matches!(self.mode, AgentMode::Conversation),
                "EndSession is unavailable in a conversation"
            );
        }
        Ok(())
    }

    fn preflight_hydration(&mut self, id: BoxId) -> anyhow::Result<()> {
        self.journal
            .state()
            .box_state(id)
            .with_context(|| format!("box {id} does not exist"))?;
        let projected = self
            .journal
            .state()
            .projection_with_box_representations(&BTreeMap::from([(
                id,
                BoxRepresentation::Hydrated,
            )]))?
            .estimated_tokens;
        let ingress = matches!(self.mode, AgentMode::Ingress { .. });
        let limit = if ingress {
            self.journal.state().ingress_context_limit()
        } else {
            self.journal.state().live_context_limit()
        };
        if projected > limit {
            if !matches!(self.mode, AgentMode::Ingress { .. }) {
                let external_event_id = self.pending_external_event_id.clone();
                let message = self.record_live_capacity_error(
                    &format!("Hydrating box {id}"),
                    projected,
                    limit,
                    external_event_id.as_deref(),
                )?;
                anyhow::bail!(message);
            }
            self.journal.record(
                now(),
                EventKind::CapacityError {
                    attempted_operation: format!("HydrateBox({id})"),
                    projected_tokens: projected,
                    limit_tokens: limit,
                },
            )?;
            if ingress {
                self.request_ingress_force_commit("hydration_exceeded_full_window", projected)?;
            }
            anyhow::bail!(
                "hydrating box {id} would use approximately {projected} tokens, over the {limit} token limit"
            );
        }
        Ok(())
    }

    fn apply_history_inspection(
        &mut self,
        event_id: EventId,
        content: BoxContent,
    ) -> anyhow::Result<()> {
        let current = self
            .journal
            .state()
            .tools
            .get(HISTORY_TOOL_INSTANCE)
            .cloned()
            .unwrap_or_default();
        let mut slots = current
            .slots
            .iter()
            .map(|slot| {
                let state = self
                    .journal
                    .state()
                    .box_state(slot.box_id)
                    .context("history slot box is missing")?;
                Ok(ToolSlotInput {
                    slot: slot.slot.clone(),
                    name: state.name.clone(),
                    content: state.canonical.content.clone(),
                    retired: slot.retired,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if let Some(existing) = slots
            .iter_mut()
            .find(|slot| slot.slot == event_id.to_string())
        {
            anyhow::ensure!(
                !existing.retired,
                "that history event inspection was retired"
            );
            existing.content = content;
        } else {
            slots.push(ToolSlotInput {
                slot: event_id.to_string(),
                name: format!("History event {event_id}"),
                content,
                retired: false,
            });
        }
        self.journal
            .apply_tool_slots(now(), HISTORY_TOOL_INSTANCE, slots)?;
        Ok(())
    }

    fn sync_kweb_boxes(&mut self) -> anyhow::Result<Vec<BoxId>> {
        let previous = kweb_box_versions(&self.journal);
        let layout = self.context.box_layout()?;
        let mut desired = Vec::new();
        for role in ["direct", "fixed", "active"] {
            for full in layout.full_nodes.iter().filter(|entry| entry.role == role) {
                let staged = self.plan.updates.get(&full.identifier);
                let mut content = if let Some(data) = staged {
                    self.staged_kweb_box_content(&full.identifier, data)?
                } else {
                    let stored = self
                        .context
                        .nodes_by_id
                        .get(&full.identifier)
                        .context("full Kweb node is missing")?;
                    self.kweb_box_content(stored)?
                };
                mark_kweb_content(&mut content, &full.identifier, role);
                desired.push(DesiredKwebBox {
                    logical_slot: full.identifier.clone(),
                    name: format!(
                        "Kweb {role} node {} · {}",
                        full.identifier,
                        full.node
                            .get("shortName")
                            .and_then(Value::as_str)
                            .unwrap_or("Unnamed")
                    ),
                    content,
                });
            }
            if role == "direct" {
                for create in &self.plan.creates {
                    let mut content =
                        self.staged_kweb_box_content(&create.pending_id, &create.data)?;
                    mark_kweb_content(&mut content, &create.pending_id, "direct");
                    desired.push(DesiredKwebBox {
                        logical_slot: create.pending_id.clone(),
                        name: format!(
                            "Kweb staged node {} · {}",
                            create.pending_id, create.data.short_name
                        ),
                        content,
                    });
                }
            }
        }
        for fanout in layout.loaded_fanouts {
            let logical_slot = format!("loaded-fanout:{}", fanout.parent_identifier);
            let mut content = BoxContent {
                text: format_summary_entries(
                    &format!(
                        "Fanout connections of loaded node {} · {}",
                        fanout.parent_identifier, fanout.parent_name
                    ),
                    &fanout.connections,
                    SummaryDetail::Summary,
                ),
                ..BoxContent::default()
            };
            mark_kweb_content(&mut content, &logical_slot, "loaded-fanout");
            desired.push(DesiredKwebBox {
                logical_slot,
                name: format!("Kweb fanout · {}", fanout.parent_identifier),
                content,
            });
        }
        for (logical_slot, name, heading, entries, detail) in [
            (
                "fixed-node-connections",
                "Fixed-node fixed and active connections",
                "Fixed and active connections of fixed nodes",
                &layout.fixed_neighbors,
                SummaryDetail::NameAndSummary,
            ),
            (
                "active-node-connections",
                "Active-node fixed and active connections",
                "Fixed and active connections of active nodes",
                &layout.active_neighbors,
                SummaryDetail::NameAndSummary,
            ),
            (
                "connection-node-fanout",
                "Fanout of fixed and active nodes",
                "Fanout connections of fixed and active nodes",
                &layout.connection_fanouts,
                SummaryDetail::Name,
            ),
        ] {
            let mut content = BoxContent {
                text: format_summary_entries(heading, entries, detail),
                ..BoxContent::default()
            };
            mark_kweb_content(&mut content, logical_slot, "aggregate");
            desired.push(DesiredKwebBox {
                logical_slot: logical_slot.into(),
                name: name.into(),
                content,
            });
        }
        self.reconcile_kweb_slots(desired)?;
        Ok(changed_kweb_box_ids(&self.journal, &previous))
    }

    fn reconcile_kweb_slots(&mut self, desired: Vec<DesiredKwebBox>) -> anyhow::Result<()> {
        let current = self
            .journal
            .state()
            .tools
            .get(KWEB_TOOL_INSTANCE)
            .cloned()
            .unwrap_or_default();
        let desired_by_logical = desired
            .iter()
            .enumerate()
            .map(|(index, entry)| (entry.logical_slot.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        anyhow::ensure!(
            desired_by_logical.len() == desired.len(),
            "Kweb box layout contains duplicate logical slots"
        );
        let mut claimed = HashSet::new();
        let mut actual_by_desired = BTreeMap::new();
        let mut slots = Vec::with_capacity(current.slots.len() + desired.len());
        let mut used_actual = current
            .slots
            .iter()
            .map(|slot| slot.slot.clone())
            .collect::<HashSet<_>>();
        for slot in &current.slots {
            let state = self
                .journal
                .state()
                .box_state(slot.box_id)
                .context("Kweb tool slot box is missing")?;
            let logical = kweb_logical_slot(state, &slot.slot);
            let selected = !slot.retired
                && desired_by_logical.contains_key(logical.as_str())
                && claimed.insert(logical.clone());
            if selected {
                let entry = &desired[desired_by_logical[logical.as_str()]];
                slots.push(ToolSlotInput {
                    slot: slot.slot.clone(),
                    name: entry.name.clone(),
                    content: entry.content.clone(),
                    retired: false,
                });
                actual_by_desired.insert(entry.logical_slot.clone(), slot.slot.clone());
            } else {
                slots.push(ToolSlotInput {
                    slot: slot.slot.clone(),
                    name: state.name.clone(),
                    content: state.canonical.content.clone(),
                    retired: slot.retired || !selected,
                });
            }
        }
        for entry in &desired {
            if actual_by_desired.contains_key(&entry.logical_slot) {
                continue;
            }
            let actual = unique_kweb_slot(&entry.logical_slot, &mut used_actual);
            slots.push(ToolSlotInput {
                slot: actual.clone(),
                name: entry.name.clone(),
                content: entry.content.clone(),
                retired: false,
            });
            actual_by_desired.insert(entry.logical_slot.clone(), actual);
        }
        let layout_slots = desired
            .iter()
            .map(|entry| actual_by_desired[&entry.logical_slot].clone())
            .collect::<Vec<_>>();
        self.journal.apply_tool_slots_with_layout(
            now(),
            KWEB_TOOL_INSTANCE,
            slots,
            &layout_slots,
        )?;
        Ok(())
    }

    fn staged_kweb_box_content(
        &self,
        identifier: &str,
        data: &SessionNodeData,
    ) -> anyhow::Result<BoxContent> {
        let active = data
            .recent_connections
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>();
        let fanout = data
            .recent_connections
            .iter()
            .skip(8)
            .cloned()
            .collect::<Vec<_>>();
        let text = format!(
            concat!(
                "Node ID: {identifier}\n",
                "Node name: {short_name}\n",
                "Node summary: {short_description}\n",
                "Node long description:\n  {long_description}\n",
                "Fixed connection IDs: {fixed}\n",
                "Active connection IDs: {active}\n",
                "Fanout connection IDs: {fanout}"
            ),
            identifier = identifier,
            short_name = data.short_name,
            short_description = data.short_description,
            long_description = data.long_description,
            fixed = data.fixed_connections.join(", "),
            active = active.join(", "),
            fanout = fanout.join(", "),
        );
        Ok(BoxContent {
            text,
            objects: Vec::new(),
            metadata: json!({
                "staged":true,
                "identifier":identifier,
                "nodeData":data,
                "revisionHash":hex::encode(Sha256::digest(serde_json::to_vec(data)?)),
            }),
        })
    }

    fn kweb_box_content(&self, node: &Value) -> anyhow::Result<BoxContent> {
        let projected = self.context.context_node(node)?;
        Ok(BoxContent {
            text: format_context_node(&projected, true),
            objects: Vec::new(),
            metadata: json!({
                "storedNode":node,
                "canonicalNodeId":node.get("id"),
                "revisionHash":hex::encode(Sha256::digest(serde_json::to_vec(node)?)),
            }),
        })
    }

    fn record_tool_invocation(&mut self, name: &str, arguments: Value) -> anyhow::Result<EventId> {
        self.journal.record(
            now(),
            EventKind::ToolInvoked {
                tool_instance: tool_instance(name),
                tool_name: name.into(),
                arguments,
            },
        )
    }

    fn record_tool_completion(&mut self, name: &str, outcome: Value) -> anyhow::Result<EventId> {
        self.journal.record(
            now(),
            EventKind::ToolCompleted {
                tool_instance: name.into(),
                tool_name: name.into(),
                outcome,
            },
        )
    }

    fn stage_plan(&mut self) -> anyhow::Result<()> {
        self.sync_kweb_boxes()?;
        Ok(())
    }

    fn node_data(&self, id: &str) -> anyhow::Result<SessionNodeData> {
        if let Some(data) = self.plan.created(id) {
            return Ok(data.clone());
        }
        if let Some(data) = self.plan.updates.get(id) {
            return Ok(data.clone());
        }
        let node = self.context.stored_node(id)?;
        Ok(session_node_data(&node))
    }

    fn put_node_data(&mut self, id: &str, data: SessionNodeData) -> anyhow::Result<()> {
        if let Some(created) = self.plan.created_mut(id) {
            *created = data;
        } else {
            canonical_id(id)?;
            self.plan.updates.insert(id.to_owned(), data);
        }
        Ok(())
    }

    fn connect_nodes(&mut self, args: &Value) -> anyhow::Result<String> {
        validate_arguments(args, &["identifiers"], &[])?;
        let ids = resource_id_array(args, "identifiers", 2)?;
        for id in &ids {
            self.ensure_known_node(id)?;
        }
        for id in &ids {
            let mut data = self.node_data(id)?;
            let mut recent = ids
                .iter()
                .filter(|other| *other != id)
                .cloned()
                .collect::<Vec<_>>();
            for other in data.recent_connections {
                if &other != id && !recent.contains(&other) {
                    recent.push(other);
                }
            }
            data.recent_connections = recent;
            self.put_node_data(id, data)?;
        }
        self.stage_plan()?;
        Ok(format!(
            "Staged connections among nodes {}.",
            ids.join(", ")
        ))
    }

    fn consolidate_fanout(&mut self, args: &Value) -> anyhow::Result<String> {
        validate_arguments(
            args,
            &[
                "parentIdentifier",
                "fanoutIdentifiers",
                "aggregatorIdentifier",
            ],
            &[],
        )?;
        let parent = resource_id(args, "parentIdentifier")?;
        let aggregator = resource_id(args, "aggregatorIdentifier")?;
        let fanout = resource_id_array(args, "fanoutIdentifiers", 1)?;
        for id in std::iter::once(&parent)
            .chain(std::iter::once(&aggregator))
            .chain(fanout.iter())
        {
            self.ensure_known_node(id)?;
        }
        let mut parent_data = self.node_data(&parent)?;
        parent_data
            .recent_connections
            .retain(|id| !fanout.contains(id));
        if !parent_data.recent_connections.contains(&aggregator) {
            parent_data.recent_connections.push(aggregator.clone());
        }
        let mut aggregator_data = self.node_data(&aggregator)?;
        for id in fanout {
            if !aggregator_data.recent_connections.contains(&id) {
                aggregator_data.recent_connections.push(id);
            }
        }
        self.put_node_data(&parent, parent_data)?;
        self.put_node_data(&aggregator, aggregator_data)?;
        self.stage_plan()?;
        Ok(format!(
            "Staged fanout consolidation from node {parent} into node {aggregator}."
        ))
    }

    fn set_fixed_connection(&mut self, args: &Value) -> anyhow::Result<String> {
        validate_arguments(args, &["parentIdentifier", "childIdentifier", "slot"], &[])?;
        let parent = resource_id(args, "parentIdentifier")?;
        self.ensure_known_node(&parent)?;
        let child = args
            .get("childIdentifier")
            .and_then(Value::as_str)
            .filter(|value| *value != "blank")
            .map(parse_resource_id)
            .transpose()?;
        if let Some(child) = &child {
            self.ensure_known_node(child)?;
            anyhow::ensure!(child != &parent, "a node cannot connect to itself");
        }
        let slot = positive_integer(args, "slot")? as usize;
        let mut data = self.node_data(&parent)?;
        if let Some(child) = child.clone() {
            anyhow::ensure!(
                slot <= data.fixed_connections.len() + 1,
                "fixed connection positions must remain contiguous"
            );
            data.fixed_connections.retain(|id| id != &child);
            if slot - 1 < data.fixed_connections.len() {
                data.fixed_connections[slot - 1] = child;
            } else {
                data.fixed_connections.push(child);
            }
        } else if slot > 0 && slot - 1 < data.fixed_connections.len() {
            data.fixed_connections.remove(slot - 1);
        }
        self.put_node_data(&parent, data)?;
        self.stage_plan()?;
        Ok(match child {
            Some(child) => {
                format!("Staged node {child} in fixed slot {slot} of node {parent}.")
            }
            None => format!("Cleared fixed slot {slot} of node {parent} in the staged plan."),
        })
    }

    fn create_node(&mut self, args: &Value) -> anyhow::Result<String> {
        validate_arguments(
            args,
            &[
                "parentIdentifiers",
                "ownerIdentifier",
                "shortName",
                "shortDescription",
                "longDescription",
            ],
            &[],
        )?;
        let parents = resource_id_array(args, "parentIdentifiers", 1)?;
        let owner = resource_id(args, "ownerIdentifier")?;
        for id in parents.iter().chain(std::iter::once(&owner)) {
            if id != "self" && id != "unowned" {
                self.ensure_known_node(id)?;
            }
        }
        let pending = self.journal.allocate_pending_node(now())?.to_string();
        self.plan.creates.push(SessionNodeCreate {
            pending_id: pending.clone(),
            data: SessionNodeData {
                short_name: string_value(args, "shortName")?,
                short_description: string_value(args, "shortDescription")?,
                long_description: string_value(args, "longDescription")?,
                owner,
                fixed_connections: Vec::new(),
                recent_connections: parents.clone(),
                objects: Vec::new(),
                include_session_object: true,
            },
        });
        for parent in parents {
            let mut data = self.node_data(&parent)?;
            data.recent_connections.retain(|id| id != &pending);
            data.recent_connections.insert(0, pending.clone());
            self.put_node_data(&parent, data)?;
        }
        self.stage_plan()?;
        Ok(format!("Created staged node {pending}."))
    }

    fn update_node(&mut self, args: &Value) -> anyhow::Result<String> {
        validate_arguments(
            args,
            &[
                "identifier",
                "ownerIdentifier",
                "newShortName",
                "newShortDescription",
                "newLongDescription",
            ],
            &[],
        )?;
        let id = resource_id(args, "identifier")?;
        let owner = resource_id(args, "ownerIdentifier")?;
        self.ensure_known_node(&id)?;
        if owner != "self" && owner != "unowned" {
            self.ensure_known_node(&owner)?;
        }
        let mut data = self.node_data(&id)?;
        data.owner = owner;
        data.short_name = string_value(args, "newShortName")?;
        data.short_description = string_value(args, "newShortDescription")?;
        data.long_description = string_value(args, "newLongDescription")?;
        data.include_session_object = true;
        self.put_node_data(&id, data)?;
        self.stage_plan()?;
        Ok(format!("Staged the update to node {id}."))
    }

    fn ensure_known_node(&self, id: &str) -> anyhow::Result<()> {
        if id.starts_with("pending:") {
            anyhow::ensure!(
                self.plan.created(id).is_some(),
                "pending node {id} is not part of this session"
            );
        } else {
            canonical_id(id)?;
            anyhow::ensure!(
                self.context.full_node_ids.contains(id) || self.plan.updates.contains_key(id),
                "node {id} is not loaded; call LoadNode first"
            );
        }
        Ok(())
    }

    fn finalize_kweb_session(&mut self) -> anyhow::Result<()> {
        if self.commit_receipt.is_some() {
            return Ok(());
        }
        self.journal.seal()?;
        let archive = serde_json::to_vec(&self.journal.session_log())?;
        let object_locations = self
            .journal
            .objects()
            .iter()
            .map(|(id, location)| (id.clone(), location.clone()))
            .collect::<Vec<_>>();
        let mut objects = Vec::with_capacity(object_locations.len());
        for (id, _) in object_locations {
            objects.push(SessionObject {
                pending_id: id.to_string(),
                bytes: self.journal.read_object(&id)?,
            });
        }
        let result = self.api.commit_kweb_session(SessionCommit {
            session_id: self.journal.state().metadata.session_id.clone(),
            author: self.commit_author.clone(),
            source_created_at: DateTime::parse_from_rfc3339(&self.started_at)
                .context("session start timestamp is invalid")?
                .with_timezone(&Utc),
            archive,
            objects,
            creates: self.plan.creates.clone(),
            updates: self
                .plan
                .updates
                .iter()
                .map(|(node_id, data)| SessionNodeUpdate {
                    node_id: node_id.clone(),
                    data: data.clone(),
                })
                .collect(),
        })?;
        self.journal
            .mark_completed(result.session_object_id.clone());
        self.commit_receipt = Some(result);
        Ok(())
    }

    fn prepare_free_time_round(&mut self) -> anyhow::Result<bool> {
        if !matches!(self.mode, AgentMode::FreeTime) {
            return Ok(false);
        }
        let Some(deadline) = deadline(&self.free_time) else {
            return Ok(false);
        };
        if Utc::now() >= deadline {
            self.free_time_end_reason = Some("deadline".into());
            self.journal.create_box(
                now(),
                "Self-time timer",
                BoxOwner::Controller,
                BoxContent::text(
                    "The self-time deadline has arrived. Finish without starting more tool work.",
                ),
            )?;
            return Ok(true);
        }
        Ok(false)
    }

    fn agent_request_timeout(&self) -> Option<Duration> {
        if matches!(self.mode, AgentMode::Conversation) && self.session_type == "conversation" {
            return Some(BROWSER_CONVERSATION_REQUEST_TIMEOUT);
        }
        if matches!(self.mode, AgentMode::Ingress { .. }) {
            return Some(HISTORY_INGRESS_REQUEST_TIMEOUT);
        }
        if matches!(self.mode, AgentMode::FreeTime) {
            let deadline = deadline(&self.free_time)?;
            return Some(Duration::from_secs(
                (deadline - Utc::now()).num_seconds().max(1) as u64 + 120,
            ));
        }
        None
    }

    pub(crate) fn refresh_telegram_group_context(
        &mut self,
        group_context: &Value,
        _current_message_id: Option<&str>,
    ) -> anyhow::Result<()> {
        if self.session_type != "telegram-group" {
            return Ok(());
        }
        self.channel["groupContext"] = group_context.clone();
        self.group_context = group_context.clone();
        self.journal.create_box(
            now(),
            "Telegram group update",
            BoxOwner::Controller,
            BoxContent::text(format_telegram_group_context(group_context)),
        )?;
        Ok(())
    }

    pub(crate) fn finalize_free_time(&mut self, reason: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(reason, "tool" | "deadline" | "hard-stop"),
            "invalid self-time completion reason"
        );
        self.free_time["sliceEndedReason"] = json!(reason);
        self.free_time["sliceEndedAt"] = json!(now());
        self.pending_turn = false;
        self.pending_external_event_id = None;
        Ok(())
    }

    pub(crate) fn commit_current_write_session(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(self.mode, AgentMode::FreeTime | AgentMode::Ingress { .. }),
            "a read-only conversation cannot be committed as a Kweb write session"
        );
        self.finalize_kweb_session()?;
        self.completed = true;
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> anyhow::Result<Value> {
        Ok(json!({
            "format":"kennedy-chatend",
            "version":1,
            "stateVersion":3,
            "sessionId":self.journal.state().metadata.session_id,
            "journalPath":self.journal.path(),
            "sessionType":self.session_type,
            "sourceSessionType":self.source_session_type,
            "channel":self.channel,
            "freeTime":self.free_time,
            "orchestration":self.orchestration,
            "provenanceId":self.provenance_id,
            "rustLibSessionId":self.rust_lib_session_id,
            "rootNodeIds":self.root_node_ids,
            "referenceRootNodeIds":self.reference_root_node_ids,
            "startedAt":self.started_at,
            "transcript":self.transcript,
            "pendingTurn":self.pending_turn,
            "pendingExternalEventId":self.pending_external_event_id,
            "roundsUsed":self.rounds_used,
            "completed":self.completed,
            "sessionObjectId":self.journal.state().completed_session_object,
            "commitReceipt":self.commit_receipt,
            "commitAuthor":self.commit_author,
            "kwebPlan":self.plan,
            "boxCount":self.journal.state().boxes.len(),
            "eventCount":self.journal.state().events.len(),
            "boxes":self.journal.state().boxes,
            "events":self.journal.state().events,
            "context":self.journal.state().projection(),
            "chatendText":self.journal.state().render(),
        }))
    }

    pub(crate) async fn release_rust_libs(&self) {
        self.api.release_rust_libs(&self.rust_lib_session_id).await;
    }
}

fn restore_kweb_context(journal: &SessionJournal, context: &mut KmapContext) -> anyhow::Result<()> {
    let Some(tool) = journal.state().tools.get(KWEB_TOOL_INSTANCE) else {
        return Ok(());
    };
    let mut nodes = Vec::new();
    let mut fixed = Vec::new();
    let mut active = Vec::new();
    for slot in &tool.slots {
        let state = journal
            .state()
            .box_state(slot.box_id)
            .context("Kweb slot references a missing box")?;
        if let Some(node) = state.canonical.content.metadata.get("storedNode") {
            nodes.push(node.clone());
            let identifier = node
                .get("id")
                .and_then(Value::as_str)
                .context("stored Kweb node has no identifier")?
                .to_owned();
            match state
                .canonical
                .content
                .metadata
                .get("kwebRole")
                .and_then(Value::as_str)
            {
                Some("fixed") => fixed.push(identifier),
                Some("active") => active.push(identifier),
                _ => {}
            }
        }
    }
    context.refresh(nodes)?;
    let mut direct = journal
        .state()
        .events
        .iter()
        .filter_map(|event| {
            let EventKind::ToolInvoked {
                tool_name,
                arguments,
                ..
            } = &event.kind
            else {
                return None;
            };
            (tool_name == "LoadNode")
                .then(|| arguments.get("identifier")?.as_str().map(str::to_owned))
                .flatten()
        })
        .collect::<Vec<_>>();
    if direct.is_empty() {
        direct = context.root_node_ids.clone();
    }
    for identifier in &direct {
        let Some(node) = context.nodes_by_id.get(identifier) else {
            continue;
        };
        fixed.extend(stored_fixed_ids(node));
        active.extend(stored_active_ids(node));
    }
    context.restore_roles(direct, fixed, active)
}

fn transcript_from_journal(journal: &SessionJournal) -> Vec<Value> {
    journal
        .state()
        .boxes
        .values()
        .filter_map(|state| {
            let role = match state.owner {
                BoxOwner::User if state.name == "User message" => "user",
                BoxOwner::Kennedy if state.name == "Kennedy message" => "kennedy",
                BoxOwner::Controller
                    if state
                        .canonical
                        .content
                        .metadata
                        .get("transcriptRole")
                        .and_then(Value::as_str)
                        == Some("system") =>
                {
                    "system"
                }
                _ => return None,
            };
            let mut entry = json!({
                "role":role,
                "content":state.canonical.content.text,
            });
            if let Some(id) = state.canonical.content.metadata.get("externalEventId") {
                entry["externalEventId"] = id.clone();
            }
            Some(entry)
        })
        .collect()
}

fn session_node_data(node: &Value) -> SessionNodeData {
    SessionNodeData {
        short_name: node
            .get("short_name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        short_description: node
            .get("short_description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        long_description: node
            .get("long_description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        owner: node
            .get("owner_node_id")
            .and_then(Value::as_str)
            .unwrap_or("unowned")
            .into(),
        fixed_connections: stored_fixed_ids(node),
        recent_connections: stored_recent_ids(node),
        objects: node
            .get("objects")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        include_session_object: true,
    }
}

fn session_kind(session_type: &str, mode: &AgentMode) -> SessionKind {
    if matches!(mode, AgentMode::Ingress { .. }) {
        return SessionKind::HistoryIngress;
    }
    match session_type {
        "conversation" => SessionKind::Conversation,
        "telegram" => SessionKind::Telegram,
        "telegram-group" => SessionKind::TelegramGroup,
        "free-time" => SessionKind::SelfTime,
        "audio" => SessionKind::AudioIngress,
        other => SessionKind::Other(other.into()),
    }
}

fn tool_instance(name: &str) -> String {
    if name == "LoadNode" {
        return KWEB_TOOL_INSTANCE.into();
    }
    format!("{name}:{}", Uuid::new_v4())
}

fn decode_data_url(value: &str) -> anyhow::Result<(String, Vec<u8>)> {
    let value = value
        .strip_prefix("data:")
        .context("object data URL must begin with data:")?;
    let (header, data) = value.split_once(',').context("invalid object data URL")?;
    let media_type = header
        .strip_suffix(";base64")
        .context("object data URL must use Base64")?;
    Ok((media_type.into(), BASE64.decode(data)?))
}

fn attachment_metadata_without_payload(value: &Value) -> Value {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("dataUrl");
        object.remove("text");
    }
    value
}

fn message_metadata_without_attachment_payloads(value: &Value) -> Value {
    let mut value = value.clone();
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    if let Some(attachments) = object.get_mut("attachments").and_then(Value::as_array_mut) {
        for attachment in attachments {
            *attachment = attachment_metadata_without_payload(attachment);
        }
    }
    if let Some(media) = object.get_mut("media") {
        *media = attachment_metadata_without_payload(media);
    }
    value
}

fn call_ktool_definition() -> kcode_codex_runtime_v2::DynamicTool {
    kcode_codex_runtime_v2::DynamicTool::new(
        "call_ktool",
        "Call one Kennedy Ktool. The provider function remains registered even if its explaining system-prompt box is dehydrated.",
        json!({
            "type":"object",
            "additionalProperties":false,
            "required":["name","arguments"],
            "properties":{
                "name":{"type":"string"},
                "arguments":{"type":"object"}
            }
        }),
    )
}

fn native_ktool_call(call: &kcode_codex_runtime_v2::DynamicToolCall) -> anyhow::Result<ToolCall> {
    anyhow::ensure!(call.tool == "call_ktool", "unknown provider tool");
    validate_arguments(&call.arguments, &["name", "arguments"], &[])?;
    Ok(ToolCall {
        name: nonempty_string(&call.arguments, "name", 100)?,
        arguments: call
            .arguments
            .get("arguments")
            .filter(|value| value.is_object())
            .context("call_ktool.arguments must be an object")?
            .clone(),
    })
}

fn validate_arguments(value: &Value, required: &[&str], optional: &[&str]) -> anyhow::Result<()> {
    let map = value
        .as_object()
        .context("arguments must be a JSON object")?;
    let allowed = required
        .iter()
        .chain(optional)
        .copied()
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        required.iter().all(|key| map.contains_key(*key))
            && map.keys().all(|key| allowed.contains(key.as_str())),
        "expected exactly: {}{}",
        required.join(", "),
        if optional.is_empty() {
            String::new()
        } else {
            format!(" (optional: {})", optional.join(", "))
        }
    );
    Ok(())
}

fn positive_integer(value: &Value, key: &str) -> anyhow::Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .with_context(|| format!("{key} must be a positive integer"))
}

fn box_id(value: &Value, key: &str) -> anyhow::Result<BoxId> {
    positive_integer(value, key).map(BoxId)
}

fn event_id(value: &Value, key: &str) -> anyhow::Result<EventId> {
    positive_integer(value, key).map(EventId)
}

fn canonical_id(value: &str) -> anyhow::Result<String> {
    value
        .parse::<NodeId>()
        .with_context(|| format!("{value:?} is not a canonical node ID"))?;
    Ok(value.into())
}

fn canonical_node_id(value: &Value, key: &str) -> anyhow::Result<String> {
    let value = string_value(value, key)?;
    canonical_id(&value)
}

fn parse_resource_id(value: &str) -> anyhow::Result<String> {
    if value.starts_with("pending:") {
        PendingId::parse(value.to_owned())?;
        Ok(value.into())
    } else if matches!(value, "self" | "unowned") {
        Ok(value.into())
    } else {
        canonical_id(value)
    }
}

fn resource_id(value: &Value, key: &str) -> anyhow::Result<String> {
    parse_resource_id(
        value
            .get(key)
            .and_then(Value::as_str)
            .with_context(|| format!("{key} must be a node identifier"))?,
    )
}

fn resource_id_array(value: &Value, key: &str, minimum: usize) -> anyhow::Result<Vec<String>> {
    let ids = value
        .get(key)
        .and_then(Value::as_array)
        .with_context(|| format!("{key} must be an array"))?
        .iter()
        .map(|value| parse_resource_id(value.as_str().context("node identifier must be a string")?))
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        ids.len() >= minimum && ids.iter().collect::<HashSet<_>>().len() == ids.len(),
        "{key} has invalid length or duplicate identifiers"
    );
    Ok(ids)
}

fn string_value(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("{key} must be a string"))
}

fn nonempty_string(value: &Value, key: &str, max: usize) -> anyhow::Result<String> {
    let value = string_value(value, key)?;
    let trimmed = value.trim();
    anyhow::ensure!(
        !trimmed.is_empty() && trimmed.chars().count() <= max,
        "{key} must contain between 1 and {max} characters"
    );
    Ok(trimmed.into())
}

fn codex_reasoning_effort(value: &str) -> anyhow::Result<kcode_codex_runtime_v2::ReasoningEffort> {
    use kcode_codex_runtime_v2::ReasoningEffort;
    Ok(match value {
        "none" => ReasoningEffort::None,
        "minimal" => ReasoningEffort::Minimal,
        "low" => ReasoningEffort::Low,
        "medium" => ReasoningEffort::Medium,
        "high" => ReasoningEffort::High,
        "xhigh" => ReasoningEffort::XHigh,
        "max" => ReasoningEffort::Max,
        other => anyhow::bail!("unsupported reasoning effort {other}"),
    })
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn deadline(value: &Value) -> Option<DateTime<Utc>> {
    value
        .get("deadlineAt")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn free_time_schedule(value: &Value) -> String {
    format!(
        "Self-time deadline: {}",
        value
            .get("deadlineAt")
            .and_then(Value::as_str)
            .unwrap_or("not supplied")
    )
}

fn free_time_opening(value: &Value) -> String {
    let custom = value
        .get("customPrompt")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if custom.trim().is_empty() {
        "Begin this self-time session.".into()
    } else {
        format!("Begin this self-time session.\n\nRequested focus:\n{custom}")
    }
}

fn format_telegram_group_context(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".into())
}

fn controller_box_name(mode: &AgentMode) -> &'static str {
    match mode {
        AgentMode::Conversation => "Turn continuation",
        AgentMode::FreeTime => "Self-time continuation",
        AgentMode::Ingress { .. } => "History-ingress continuation",
    }
}

fn controller_message(mode: &AgentMode, free_time: &Value) -> String {
    match mode {
        AgentMode::Conversation => {
            "Continue the turn. Use tools if needed, then answer the user.".into()
        }
        AgentMode::FreeTime => format!("Continue self time. {}", free_time_schedule(free_time)),
        AgentMode::Ingress { .. } => {
            "You are in a solo history-ingress session; there is no user to receive a conversational response. If you have completed all useful memory work, call EndSession now using the native call_ktool function with exactly this arguments object: {\"name\":\"EndSession\",\"arguments\":{}}. A normal response does not end this session. If work remains, continue it with tools, then call EndSession when finished.".into()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn test_journal(label: &str, effective_context_tokens: u64) -> (PathBuf, SessionJournal) {
        let path = std::env::temp_dir().join(format!(
            "kennedy-ingress-context-{label}-{}-{}.session-log",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let journal = SessionJournal::create(
            &path,
            SessionMetadata {
                session_id: label.into(),
                kind: SessionKind::Conversation,
                created_at: "2026-07-23T00:00:00Z".into(),
                effective_context_tokens,
                channel: Value::Null,
            },
        )
        .unwrap();
        let path = journal.path().to_path_buf();
        (path, journal)
    }

    #[test]
    fn resource_identifiers_accept_pending_and_canonical_but_not_fake_short_ids() {
        assert_eq!(
            parse_resource_id("pending:47").unwrap(),
            "pending:47".to_owned()
        );
        assert!(parse_resource_id("km:47").is_err());
    }

    #[test]
    fn ingress_continuation_explains_the_solo_session_and_exact_end_session_call() {
        let message = controller_message(&AgentMode::Ingress { record_id: None }, &Value::Null);
        assert!(message.contains("solo history-ingress session"));
        assert!(message.contains("there is no user"));
        assert!(message.contains(r#"{"name":"EndSession","arguments":{}}"#));
        assert!(message.contains("A normal response does not end this session"));
    }

    #[test]
    fn restoring_a_source_record_does_not_relabel_history_ingress() {
        let mut ingress = SessionOptions::conversation("history-ingress", Vec::new());
        ingress.mode = AgentMode::Ingress { record_id: None };
        ingress.source_session_type = Some("conversation".into());
        restore_session_type(&mut ingress, &json!({"sessionType":"conversation"}));
        assert_eq!(ingress.session_type, "history-ingress");
        assert_eq!(ingress.source_session_type.as_deref(), Some("conversation"));

        let mut conversation = SessionOptions::conversation("conversation", Vec::new());
        restore_session_type(&mut conversation, &json!({"sessionType":"telegram"}));
        assert_eq!(conversation.session_type, "telegram");
    }

    #[test]
    fn a_null_stored_commit_receipt_means_not_committed() {
        assert_eq!(
            restore_commit_receipt(Some(&json!({"commitReceipt":null}))).unwrap(),
            None
        );
        assert_eq!(restore_commit_receipt(Some(&json!({}))).unwrap(), None);
    }

    #[test]
    fn staged_plan_round_trips_as_additive_json() {
        let plan = KwebPlan {
            creates: vec![SessionNodeCreate {
                pending_id: "pending:3".into(),
                data: SessionNodeData {
                    short_name: "Test node".into(),
                    short_description: String::new(),
                    long_description: String::new(),
                    owner: "self".into(),
                    fixed_connections: Vec::new(),
                    recent_connections: Vec::new(),
                    objects: Vec::new(),
                    include_session_object: true,
                },
            }],
            updates: BTreeMap::new(),
        };
        assert_eq!(
            serde_json::from_value::<KwebPlan>(serde_json::to_value(&plan).unwrap())
                .unwrap()
                .creates
                .len(),
            1
        );
    }

    #[test]
    fn load_node_result_is_exact_changed_kweb_projection_in_layout_order() {
        let (path, mut journal) = test_journal("load-node-result", 10_000);
        let mut direct = BoxContent::text("Node ID: direct\nNode name: Direct");
        mark_kweb_content(&mut direct, "direct", "direct");
        let mut fanout = BoxContent::text("old fanout");
        mark_kweb_content(&mut fanout, "loaded-fanout:direct", "loaded-fanout");
        journal
            .apply_tool_slots_with_layout(
                "t1",
                KWEB_TOOL_INSTANCE,
                vec![
                    ToolSlotInput {
                        slot: "direct".into(),
                        name: "Kweb direct node direct · Direct".into(),
                        content: direct.clone(),
                        retired: false,
                    },
                    ToolSlotInput {
                        slot: "loaded-fanout:direct".into(),
                        name: "Kweb fanout · direct".into(),
                        content: fanout,
                        retired: false,
                    },
                ],
                &["direct".into(), "loaded-fanout:direct".into()],
            )
            .unwrap();
        let fanout_id = journal.state().tool_layouts[KWEB_TOOL_INSTANCE][1];
        journal
            .summarize_box("t2", fanout_id, "Kennedy's retained fanout summary")
            .unwrap();
        let previous = kweb_box_versions(&journal);

        let mut refreshed_fanout = BoxContent::text("new canonical fanout");
        mark_kweb_content(
            &mut refreshed_fanout,
            "loaded-fanout:direct",
            "loaded-fanout",
        );
        let mut active = BoxContent::text("Node ID: active\nNode name: Active");
        mark_kweb_content(&mut active, "active", "active");
        journal
            .apply_tool_slots_with_layout(
                "t3",
                KWEB_TOOL_INSTANCE,
                vec![
                    ToolSlotInput {
                        slot: "direct".into(),
                        name: "Kweb direct node direct · Direct".into(),
                        content: direct,
                        retired: false,
                    },
                    ToolSlotInput {
                        slot: "loaded-fanout:direct".into(),
                        name: "Kweb fanout · direct".into(),
                        content: refreshed_fanout,
                        retired: false,
                    },
                    ToolSlotInput {
                        slot: "active".into(),
                        name: "Kweb active node active · Active".into(),
                        content: active,
                        retired: false,
                    },
                ],
                &[
                    "direct".into(),
                    "active".into(),
                    "loaded-fanout:direct".into(),
                ],
            )
            .unwrap();

        let changed = changed_kweb_box_ids(&journal, &previous);
        let active_id = journal.state().tool_layouts[KWEB_TOOL_INSTANCE][1];
        assert_eq!(changed, vec![active_id, fanout_id]);

        let projected = journal
            .state()
            .projection()
            .items
            .into_iter()
            .filter(|item| !item.marker && changed.contains(&item.box_id))
            .map(|item| item.text)
            .collect::<Vec<_>>();
        let result = render_load_node_result(&journal, &changed).unwrap();
        assert_eq!(result, projected.join("\n\n"));
        assert!(result.find("Node ID: active").unwrap() < result.find("retained fanout").unwrap());
        assert!(result.contains("| summarized | stale]"));
        assert!(!result.contains("new canonical fanout"));
        assert!(!result.contains("\"updatedBoxIds\""));

        let current = kweb_box_versions(&journal);
        let unchanged = changed_kweb_box_ids(&journal, &current);
        assert!(unchanged.is_empty());
        assert_eq!(
            render_load_node_result(&journal, &unchanged).unwrap(),
            "LoadNode completed. The shared Kweb boxes were already current."
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn managed_rust_write_revises_one_stable_box_without_a_second_source_copy() {
        let (path, mut journal) = test_journal("managed-rust-write", 10_000);
        let initial = LibrarySnapshot {
            name: "example-lib".into(),
            text: "Rust library: example-lib\nFiles: 1\n\nFile: src/lib.rs\npub fn old_revision() {}\n"
                .into(),
        };
        let box_id = apply_rust_lib_snapshot(&mut journal, initial).unwrap();
        let call = ToolCall {
            name: WRITE_RUST_LIB_TOOL.into(),
            arguments: json!({
                "name":"example-lib",
                "files":[{
                    "path":"src/lib.rs",
                    "contents":"pub fn unique_new_revision() {}\n"
                }]
            }),
        };
        journal
            .record(
                "t2",
                EventKind::ToolInvoked {
                    tool_instance: "test-write".into(),
                    tool_name: call.name.clone(),
                    arguments: call.arguments.clone(),
                },
            )
            .unwrap();
        let compact_call = tool_call_box_content(&call).unwrap();
        assert!(!compact_call.text.contains("unique_new_revision"));
        assert!(compact_call.text.contains("completeFileContents"));
        journal
            .create_box(
                "t3",
                format!("Kennedy tool call: {}", call.name),
                BoxOwner::Kennedy,
                compact_call,
            )
            .unwrap();
        let updated = proposed_write_snapshot(&call.arguments).unwrap();
        let same_box_id = apply_rust_lib_snapshot(&mut journal, updated).unwrap();

        assert_eq!(same_box_id, box_id);
        assert_eq!(journal.state().tools[RUST_LIB_TOOL_INSTANCE].slots.len(), 1);
        let rendered = journal.state().render();
        assert!(!rendered.contains("old_revision"));
        assert_eq!(rendered.matches("unique_new_revision").count(), 1);
        assert!(journal.state().events.iter().any(|event| {
            matches!(
                &event.kind,
                EventKind::ToolInvoked { arguments, .. }
                    if arguments.to_string().contains("unique_new_revision")
            )
        }));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn managed_rust_write_capacity_previews_replacement_instead_of_duplication() {
        let (path, mut journal) = test_journal("managed-rust-capacity", 1_200);
        let initial = LibrarySnapshot {
            name: "large-lib".into(),
            text: format!(
                "Rust library: large-lib\nFiles: 1\n\nFile: src/lib.rs\n{}",
                "a".repeat(1_800)
            ),
        };
        apply_rust_lib_snapshot(&mut journal, initial).unwrap();
        let call = ToolCall {
            name: WRITE_RUST_LIB_TOOL.into(),
            arguments: json!({
                "name":"large-lib",
                "files":[{"path":"src/lib.rs","contents":"b".repeat(1_800)}]
            }),
        };
        let call_name = format!("Kennedy tool call: {}", call.name);
        let full_call = BoxContent::text(
            serde_json::to_string_pretty(&json!({"name":call.name,"arguments":call.arguments}))
                .unwrap(),
        );
        let duplicated = journal
            .state()
            .projection_with_new_boxes(&[(call_name.clone(), BoxOwner::Kennedy, full_call)])
            .unwrap();
        assert!(duplicated.estimated_tokens > journal.state().live_context_limit());

        let compact = tool_call_box_content(&call).unwrap();
        let updates = prospective_rust_lib_box_updates(&journal, &call);
        let replacement = journal
            .state()
            .projection_with_new_boxes_and_updates(
                &[(call_name, BoxOwner::Kennedy, compact)],
                &updates,
            )
            .unwrap();
        assert!(replacement.estimated_tokens <= journal.state().live_context_limit());
        assert_eq!(
            replacement
                .items
                .iter()
                .filter(|item| item.text.contains(&"b".repeat(200)))
                .count(),
            1
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn freeform_write_capture_preserves_the_exact_file_and_summarizes_its_active_box() {
        let request = freeform_write_request(&json!({
            "name":"example-lib",
            "path":"src/lib.rs",
            "updateDescription":"Preserved raw Rust source",
        }))
        .unwrap();
        let raw = "\n//! leading newline\npub fn quote() -> &'static str { \"raw\\\\text\" }\n";
        let content = captured_write_box_content(&request, raw.into());
        assert_eq!(content.text, raw);
        assert_eq!(
            content.metadata["arguments"],
            json!({
                "name":"example-lib",
                "path":"src/lib.rs",
                "updateDescription":"Preserved raw Rust source",
            })
        );

        let (path, mut journal) = test_journal("freeform-write-capture", 10_000);
        let box_id = journal
            .create_box(
                "t1",
                format!("Kennedy tool call: {WRITE_FILE_FREEFORM_RUST_LIB_TOOL}"),
                BoxOwner::Kennedy,
                BoxContent::text("initial metadata"),
            )
            .unwrap();
        journal.update_box("t2", box_id, content).unwrap();
        let summary = captured_write_summary(&request);
        journal.summarize_box("t3", box_id, &summary).unwrap();

        let state = journal.state().box_state(box_id).unwrap();
        assert_eq!(state.canonical.content.text, raw);
        assert_eq!(
            summary,
            "Kennedy called write-file on src/lib.rs in example-lib, and she describes the update as: Preserved raw Rust source"
        );
        let rendered = journal.state().render();
        assert!(rendered.contains(&summary));
        assert!(!rendered.contains("raw\\\\text"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn freeform_write_capture_adds_only_a_missing_final_newline() {
        assert_eq!(
            ensure_final_newline("pub fn example() {}".into()),
            "pub fn example() {}\n"
        );
        assert_eq!(
            ensure_final_newline("pub fn example() {}\n".into()),
            "pub fn example() {}\n"
        );
        assert_eq!(
            ensure_final_newline("pub fn example() {}\n\n".into()),
            "pub fn example() {}\n\n"
        );
        assert_eq!(ensure_final_newline(String::new()), "\n");
    }

    #[test]
    fn freeform_write_metadata_is_strict_and_single_line() {
        assert!(
            freeform_write_request(&json!({
                "name":"example-lib",
                "path":"src/lib.rs",
                "updateDescription":"valid",
                "contents":"not accepted in the Ktool call",
            }))
            .is_err()
        );
        assert!(
            freeform_write_request(&json!({
                "name":"example-lib",
                "path":"src/lib.rs\nanother",
                "updateDescription":"valid",
            }))
            .is_err()
        );
        assert!(
            freeform_write_request(&json!({
                "name":"example-lib",
                "path":"src/lib.rs",
                "updateDescription":"line one\nline two",
            }))
            .is_err()
        );
    }

    #[test]
    fn managed_rust_snapshot_updates_preserve_kennedy_representation_choices() {
        let (path, mut journal) = test_journal("managed-rust-representation", 10_000);
        let box_id = apply_rust_lib_snapshot(
            &mut journal,
            LibrarySnapshot {
                name: "summary-lib".into(),
                text: "old canonical source".into(),
            },
        )
        .unwrap();
        journal
            .summarize_box("t2", box_id, "Kennedy's retained library summary")
            .unwrap();
        let same_box_id = apply_rust_lib_snapshot(
            &mut journal,
            LibrarySnapshot {
                name: "summary-lib".into(),
                text: "new canonical source".into(),
            },
        )
        .unwrap();

        assert_eq!(same_box_id, box_id);
        let state = journal.state().box_state(box_id).unwrap();
        assert_eq!(state.canonical.content.text, "new canonical source");
        assert!(state.stale());
        assert!(matches!(
            state.representation,
            Representation::Summarized { .. }
        ));
        let rendered = journal.state().render();
        assert!(rendered.contains("Kennedy's retained library summary"));
        assert!(!rendered.contains("new canonical source"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ordinary_tool_result_boxes_store_raw_text_without_escaping() {
        let (path, mut journal) = test_journal("tool-result", 1_000);
        let raw = "File: src/lib.rs\npub fn quote() -> &'static str { \"raw\\\\text\" }\n";
        let box_id = journal
            .create_box(
                "t1",
                "Kennedy tool result",
                BoxOwner::Controller,
                BoxContent::text(raw),
            )
            .unwrap();

        assert_eq!(
            journal
                .state()
                .box_state(box_id)
                .unwrap()
                .canonical
                .content
                .text,
            raw
        );
        let projected = journal.state().projection().items[0].text.clone();
        assert!(projected.contains("\"raw\\\\text\""));
        assert!(!projected.contains("\\\"raw"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn web_tool_results_are_plain_text() {
        let search = render_web_search_result(&json!({
            "answer":"The answer uses \"quotes\" and a backslash: \\",
            "sources":[
                {"title":"Primary source","url":"https://example.test/source"},
                {"title":"","url":"https://example.test/untitled"}
            ]
        }))
        .unwrap();
        assert_eq!(
            search,
            "The answer uses \"quotes\" and a backslash: \\\n\nSources:\n- Primary source: https://example.test/source\n- https://example.test/untitled"
        );
        assert!(!search.contains("\\\"quotes\\\""));

        let fetched = render_web_fetch_result(&json!({
            "url":"https://example.test/page",
            "title":"A page",
            "content_type":"text/plain",
            "content":"fn main() {\n    println!(\"raw\");\n}\n",
            "truncated":true
        }))
        .unwrap();
        assert_eq!(
            fetched,
            "Source URL: https://example.test/page\nTitle: A page\nContent type: text/plain\nThe returned page text was truncated.\n\nfn main() {\n    println!(\"raw\");\n}\n"
        );
        assert!(!fetched.contains("\\n"));
        assert!(!fetched.contains("\\\"raw\\\""));
    }

    #[test]
    fn ingress_always_keeps_kennedy_summaries_and_otherwise_hydrates_a_small_context() {
        let (path, mut journal) = test_journal("small", 1_000);
        journal
            .create_box("t1", "system", BoxOwner::System, BoxContent::text("system"))
            .unwrap();
        let summarized = journal
            .create_box(
                "t2",
                "web result",
                BoxOwner::Controller,
                BoxContent::text("x".repeat(600)),
            )
            .unwrap();
        journal
            .summarize_box("t3", summarized, "Kennedy's important points")
            .unwrap();
        let dehydrated = journal
            .create_box(
                "t4",
                "code",
                BoxOwner::Tool {
                    tool_instance: "rust".into(),
                    slot: "lib.rs".into(),
                },
                BoxContent::text("y".repeat(300)),
            )
            .unwrap();
        journal.dehydrate_box("t5", dehydrated).unwrap();

        let plan = history_ingress_representation_plan(journal.state()).unwrap();
        assert!(plan.fits);
        assert_eq!(
            plan.desired[&summarized],
            BoxRepresentation::Summarized("Kennedy's important points".into())
        );
        assert_eq!(plan.desired[&dehydrated], BoxRepresentation::Hydrated);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ingress_reduces_largest_unprotected_box_before_protected_messages() {
        let (path, mut journal) = test_journal("largest-first", 500);
        journal
            .create_box("t1", "system", BoxOwner::System, BoxContent::text("system"))
            .unwrap();
        let message = journal
            .create_box(
                "t2",
                "User message",
                BoxOwner::User,
                BoxContent::text("m".repeat(300)),
            )
            .unwrap();
        let large = journal
            .create_box(
                "t3",
                "Kennedy tool result",
                BoxOwner::Controller,
                BoxContent::text("r".repeat(1_200)),
            )
            .unwrap();
        let small = journal
            .create_box(
                "t4",
                "small notice",
                BoxOwner::Controller,
                BoxContent::text("n".repeat(60)),
            )
            .unwrap();

        let plan = history_ingress_representation_plan(journal.state()).unwrap();
        assert!(plan.fits);
        assert_eq!(plan.desired[&message], BoxRepresentation::Hydrated);
        assert_eq!(plan.desired[&large], BoxRepresentation::Dehydrated);
        assert_eq!(plan.desired[&small], BoxRepresentation::Hydrated);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ingress_dehydrates_largest_protected_box_when_protected_context_exceeds_target() {
        let (path, mut journal) = test_journal("protected-largest-first", 500);
        let system = journal
            .create_box(
                "t1",
                "system",
                BoxOwner::System,
                BoxContent::text("s".repeat(500)),
            )
            .unwrap();
        let message = journal
            .create_box(
                "t2",
                "User message",
                BoxOwner::User,
                BoxContent::text("m".repeat(1_200)),
            )
            .unwrap();

        let plan = history_ingress_representation_plan(journal.state()).unwrap();
        assert!(plan.fits);
        assert_eq!(plan.desired[&message], BoxRepresentation::Dehydrated);
        assert_eq!(plan.desired[&system], BoxRepresentation::Hydrated);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ingress_reports_no_fit_only_after_every_box_is_dehydrated() {
        let (path, mut journal) = test_journal("fully-dehydrated", 100);
        let mut box_ids = Vec::new();
        for index in 0..40 {
            box_ids.push(
                journal
                    .create_box(
                        format!("t{index}"),
                        format!("protected message {index}"),
                        BoxOwner::User,
                        BoxContent::text("x"),
                    )
                    .unwrap(),
            );
        }

        let plan = history_ingress_representation_plan(journal.state()).unwrap();
        assert!(!plan.fits);
        assert!(
            box_ids
                .iter()
                .all(|box_id| plan.desired[box_id] == BoxRepresentation::Dehydrated)
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn long_tool_invocations_are_programmatically_summarized_only_when_needed() {
        let invocation = "i".repeat(1_284);
        let (large_path, mut large_window) = test_journal("tool-large-window", 1_000);
        large_window
            .create_box("t1", "system", BoxOwner::System, BoxContent::text("system"))
            .unwrap();
        let large_id = large_window
            .create_box(
                "t2",
                "Kennedy tool call: LoadNode",
                BoxOwner::Kennedy,
                BoxContent::text(&invocation),
            )
            .unwrap();
        let large_plan = history_ingress_representation_plan(large_window.state()).unwrap();
        assert_eq!(large_plan.desired[&large_id], BoxRepresentation::Hydrated);

        let (small_path, mut small_window) = test_journal("tool-small-window", 300);
        small_window
            .create_box("t1", "system", BoxOwner::System, BoxContent::text("system"))
            .unwrap();
        let small_id = small_window
            .create_box(
                "t2",
                "Kennedy tool call: LoadNode",
                BoxOwner::Kennedy,
                BoxContent::text(invocation),
            )
            .unwrap();
        let small_plan = history_ingress_representation_plan(small_window.state()).unwrap();
        assert_eq!(
            small_plan.desired[&small_id],
            BoxRepresentation::Summarized(
                "Tool invocation: LoadNode {arguments dehydrated: 1,284 characters}.".into()
            )
        );
        std::fs::remove_file(large_path).unwrap();
        std::fs::remove_file(small_path).unwrap();
    }

    #[test]
    fn a_large_unprotected_kennedy_summary_can_later_be_dehydrated() {
        let (path, mut journal) = test_journal("large-summary", 300);
        journal
            .create_box("t1", "system", BoxOwner::System, BoxContent::text("system"))
            .unwrap();
        let summarized = journal
            .create_box(
                "t2",
                "large fetched page",
                BoxOwner::Controller,
                BoxContent::text("canonical".repeat(300)),
            )
            .unwrap();
        journal
            .summarize_box("t3", summarized, "s".repeat(1_200))
            .unwrap();

        let plan = history_ingress_representation_plan(journal.state()).unwrap();
        assert_eq!(plan.desired[&summarized], BoxRepresentation::Dehydrated);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn attachment_payload_metadata_is_separate_and_not_a_transcript_turn() {
        let metadata = json!({
            "externalEventId":"event-1",
            "attachments":[{
                "fileName":"notes.txt",
                "text":"derived contents",
                "dataUrl":"data:text/plain;base64,ZA==",
                "sizeBytes":1
            }]
        });
        let sanitized = message_metadata_without_attachment_payloads(&metadata);
        let attachment = &sanitized["attachments"][0];
        assert!(attachment.get("text").is_none());
        assert!(attachment.get("dataUrl").is_none());
        assert_eq!(attachment["fileName"], "notes.txt");

        let (path, mut journal) = test_journal("attachment-transcript", 1_000);
        journal
            .create_box(
                "t1",
                "User message",
                BoxOwner::User,
                BoxContent::text("please inspect the attachment"),
            )
            .unwrap();
        journal
            .create_box(
                "t2",
                "User attachment text: notes.txt",
                BoxOwner::User,
                BoxContent::text("derived contents"),
            )
            .unwrap();
        let transcript = transcript_from_journal(&journal);
        assert_eq!(transcript.len(), 1);
        assert_eq!(transcript[0]["content"], "please inspect the attachment");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn capacity_errors_are_recovered_as_system_transcript_messages() {
        let (path, mut journal) = test_journal("capacity-system-message", 1_000);
        journal
            .create_box(
                "t1",
                CAPACITY_ERROR_BOX_NAME,
                BoxOwner::Controller,
                BoxContent {
                    text: "The message exceeded capacity.".into(),
                    objects: Vec::new(),
                    metadata: json!({
                        "transcriptRole":"system",
                        "externalEventId":"event-1",
                        "capacityError":true,
                    }),
                },
            )
            .unwrap();

        assert_eq!(
            transcript_from_journal(&journal),
            vec![json!({
                "role":"system",
                "content":"The message exceeded capacity.",
                "externalEventId":"event-1",
            })]
        );
        std::fs::remove_file(path).unwrap();
    }
}
