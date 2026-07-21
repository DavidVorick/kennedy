use std::{collections::HashSet, future::Future, time::Instant};

use anyhow::Context as _;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use kennedy_chatend::canonical_chatend_text as format_chatend;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    Api, ApiError, Manuals, RuntimeModel,
    context::{
        KmapContext, MAX_DIRECTLY_LOADED_NODES, format_compact_memory_sections,
        format_context_node, format_kmap_context, project_load_batch, stored_fixed_ids,
        stored_recent_ids,
    },
    http::{encode_path, idempotency_id},
};

const TOOL_PREFIX: &str = "KENNEDY_TOOL_CALLS";
const AGENT_LOOP_ROUND_LIMIT: u64 = 100;
const RUST_LIB_TOOLS: [&str; 5] = [
    "CreateRustLib",
    "OpenRustLib",
    "WriteRustLib",
    "CheckRustLib",
    "PublishRustLib",
];

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

pub(crate) struct Session {
    api: Api,
    runtime: RuntimeModel,
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
    pub media: Vec<Value>,
    pub pending_turn: bool,
    pub pending_external_event_id: Option<String>,
    pub last_context_warning_band: u64,
    pub completed: bool,
    pub rounds_used: u64,
    mode: AgentMode,
    source_session_type: Option<String>,
    group_context: Value,
    context: KmapContext,
    system_prompt: String,
    retained: Vec<Value>,
    reset_history: Vec<Value>,
    messages: Vec<Value>,
    full_history_segments: Vec<Value>,
    load_calls: u64,
    load_limit: u64,
    tool_log: Vec<Value>,
    turn_end_content: Option<String>,
    usage: Value,
    free_time_end_reason: Option<String>,
}

struct ToolCall {
    name: String,
    arguments: Value,
}

struct ToolOutcome {
    message: Value,
    duration_ms: u64,
    reset: bool,
    end_turn: bool,
    self_message: Option<String>,
    previous_context: Value,
    reset_history_entry: Value,
}

impl Session {
    pub(crate) async fn new(
        api: Api,
        manuals: Manuals,
        runtime: RuntimeModel,
        mut options: SessionOptions,
        restored: Option<&Value>,
    ) -> anyhow::Result<Self> {
        let archive = restored
            .and_then(|state| state.get("archive"))
            .filter(|archive| {
                archive.get("format").and_then(Value::as_str) == Some("kennedy-chatend")
            })
            .or_else(|| {
                restored.filter(|archive| {
                    archive.get("format").and_then(Value::as_str) == Some("kennedy-chatend")
                })
            });
        if let Some(state) = restored {
            options.session_type = string_in(state, archive, "sessionType")
                .unwrap_or(&options.session_type)
                .to_owned();
            options.channel = value_in(state, archive, "channel").unwrap_or(options.channel);
            options.free_time = value_in(state, archive, "freeTime").unwrap_or(options.free_time);
            options.orchestration =
                value_in(state, archive, "orchestration").unwrap_or(options.orchestration);
            options.provenance_id = string_in(state, archive, "provenanceId")
                .map(str::to_owned)
                .or(options.provenance_id);
            if options.reference_root_node_ids.is_empty() {
                options.reference_root_node_ids =
                    string_values(value_in_ref(state, archive, "referenceRootNodeIds"));
            }
        }
        options
            .reference_root_node_ids
            .retain(|id| !id.is_empty() && !options.root_node_ids.contains(id));
        options.reference_root_node_ids.sort();
        options.reference_root_node_ids.dedup();
        let mut context = KmapContext::new(api.clone(), options.root_node_ids.clone())?;
        if let Some(saved) = archive
            .and_then(|archive| archive.get("context"))
            .and_then(|context| context.get("state"))
        {
            context.restore(saved)?;
            context.ensure_roots_loaded().await?;
        } else {
            context.initialize().await?;
            let loaded = restored
                .and_then(|state| state.get("loadedNodeIds"))
                .or_else(|| {
                    archive
                        .and_then(|archive| archive.get("context"))
                        .and_then(|context| context.get("diagnostics"))
                        .and_then(|value| value.get("loadedNodeIds"))
                });
            for id in string_values(loaded) {
                if !options.root_node_ids.contains(&id) && !context.loaded_node_ids.contains(&id) {
                    context.load_durable(&id).await?;
                }
            }
        }
        for id in &options.reference_root_node_ids {
            context.register_reference(id)?;
        }
        let source_session_type = archive
            .and_then(|archive| archive.get("sourceSessionType"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(options.source_session_type.clone());
        let group_context = archive
            .and_then(|archive| archive.get("groupContext"))
            .cloned()
            .unwrap_or(options.group_context.clone());
        let session_context = if options.session_type == "telegram-group" {
            format_telegram_group_context(
                options
                    .channel
                    .get("groupContext")
                    .unwrap_or(&group_context),
                &mut context,
            )?
        } else if options.session_type == "free-time" {
            free_time_schedule(&options.free_time)
        } else if matches!(options.mode, AgentMode::Ingress { .. })
            && source_session_type.as_deref() == Some("telegram-group")
        {
            format_telegram_group_context(&group_context, &mut context)?
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
        let transcript = restored
            .and_then(|state| state.get("transcript"))
            .or_else(|| archive.and_then(|archive| archive.get("transcript")))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut retained = archive
            .and_then(|archive| archive.get("retained"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| retained_transcript(&transcript));
        if matches!(options.mode, AgentMode::Ingress { .. }) && archive.is_none() {
            retained.clear();
        }
        let context_message = json!({
            "role":"system",
            "display_role":"Kmap context",
            "context_kind":"memory",
            "content": format_kmap_context(&context.snapshot()?),
        });
        let mut messages = archive
            .and_then(|archive| archive.get("messages"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| {
                let mut values = vec![instruction_message(&system_prompt)];
                values.extend(retained.clone());
                values.push(context_message.clone());
                values
            });
        replace_context_message(&mut messages, &system_prompt, context_message);
        let started_at = restored
            .and_then(|state| state.get("startedAt"))
            .or_else(|| archive.and_then(|archive| archive.get("startedAt")))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let reset_history = retained
            .iter()
            .find(|message| {
                message.get("context_kind").and_then(Value::as_str) == Some("reset-history")
            })
            .and_then(|message| message.get("reset_history_entries"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let pending_turn = restored
            .and_then(|state| state.get("pendingTurn"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || (options.session_type != "free-time"
                && transcript
                    .last()
                    .and_then(|entry| entry.get("role"))
                    .and_then(Value::as_str)
                    == Some("user"));
        let usage = archive
            .and_then(|archive| archive.get("usage"))
            .cloned()
            .unwrap_or_else(|| empty_usage(&runtime));
        let rust_lib_session_id = restored
            .and_then(|state| state.get("rustLibSessionId"))
            .or_else(|| archive.and_then(|archive| archive.get("rustLibSessionId")))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(options.rust_lib_session_id)
            .unwrap_or_else(|| format!("kennedy:{}", Uuid::new_v4()));
        let load_limit = if matches!(options.mode, AgentMode::Conversation) {
            20
        } else {
            50
        };
        let mut session = Self {
            api,
            runtime,
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
            media: value_in_ref_opt(restored, archive, "media")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            pending_turn,
            pending_external_event_id: value_in_ref_opt(
                restored,
                archive,
                "pendingExternalEventId",
            )
            .and_then(Value::as_str)
            .map(str::to_owned),
            last_context_warning_band: value_in_ref_opt(
                restored,
                archive,
                "lastContextWarningBand",
            )
            .and_then(Value::as_u64)
            .unwrap_or_default(),
            completed: archive
                .and_then(|archive| archive.get("completed"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            rounds_used: archive
                .and_then(|archive| archive.get("roundsUsed"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            mode: options.mode,
            source_session_type,
            group_context,
            context,
            system_prompt,
            retained,
            reset_history,
            messages,
            full_history_segments: archive
                .and_then(|archive| archive.get("fullHistory"))
                .and_then(|history| history.get("segments"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            load_calls: archive
                .and_then(|archive| archive.get("tools"))
                .and_then(|tools| tools.get("loadCalls"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            load_limit,
            tool_log: archive
                .and_then(|archive| archive.get("tools"))
                .and_then(|tools| tools.get("log"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            turn_end_content: archive
                .and_then(|archive| archive.get("tools"))
                .and_then(|tools| tools.get("turnEndContent"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            usage,
            free_time_end_reason: None,
        };
        session.ensure_initial_tool_check();
        Ok(session)
    }

    pub(crate) fn set_ingress_provenance_message(
        &mut self,
        provenance: &Value,
    ) -> anyhow::Result<()> {
        if !matches!(self.mode, AgentMode::Ingress { .. }) {
            anyhow::bail!("only ingress sessions accept provenance context");
        }
        if self.retained.iter().any(|message| {
            message.get("context_kind").and_then(Value::as_str) == Some("provenance")
        }) {
            return Ok(());
        }
        let audio = self.source_session_type.as_deref() == Some("audio");
        let data = provenance
            .get("data")
            .and_then(Value::as_str)
            .context("ingress provenance has no data")?;
        let readable = model_readable_provenance(data)?;
        let content = [
            if audio {
                "Audio transcript provenance"
            } else {
                "Conversation provenance"
            },
            "",
            &format!(
                "Source: {}",
                provenance
                    .get("source")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
            &format!(
                "Created: {}",
                provenance
                    .get("source_created_at")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
            "",
            if audio {
                "Final transcript piece"
            } else {
                "Archived Chatend"
            },
            "",
            &readable,
        ]
        .join("\n");
        let message = json!({
            "role":"user",
            "display_role": if audio {"Audio transcript provenance"} else {"Conversation provenance"},
            "context_kind":"provenance",
            "content":content,
        });
        self.retained.push(message.clone());
        self.rebuild_chatend()?;
        Ok(())
    }

    pub(crate) fn stage_user_input(&mut self, text: &str, metadata: &Value) -> bool {
        let content = text.trim();
        let attachments = metadata
            .get("attachments")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| {
                item.get("kind").and_then(Value::as_str) == Some("document")
                    && item
                        .get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.trim().is_empty())
            })
            .cloned()
            .collect::<Vec<_>>();
        if content.is_empty() && attachments.is_empty() {
            return false;
        }
        self.turn_end_content = None;
        let external_event_id = metadata.get("externalEventId").and_then(Value::as_str);
        let input_kind = if metadata.get("inputKind").and_then(Value::as_str) == Some("voice") {
            "voice"
        } else if !attachments.is_empty() {
            "document"
        } else {
            "text"
        };
        let visible = if content.is_empty() {
            format!(
                "Attached {}.",
                attachments
                    .iter()
                    .map(|item| item
                        .get("fileName")
                        .and_then(Value::as_str)
                        .unwrap_or("document"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            content.to_owned()
        };
        let mut transcript = json!({"role":"user","content":visible,"inputKind":input_kind});
        if let Some(id) = external_event_id {
            transcript["externalEventId"] = json!(id);
        }
        let mut chatend_content = content.to_owned();
        if input_kind == "voice" {
            let media_id = metadata
                .get("media")
                .and_then(|media| media.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            transcript["mediaId"] = json!(media_id);
            transcript["transcriptionModel"] = metadata
                .get("transcriptionModel")
                .cloned()
                .unwrap_or(Value::Null);
            if let Some(media) = metadata.get("media").and_then(Value::as_object) {
                let mut media = media.clone();
                media.insert("id".into(), json!(media_id));
                media.insert("transcription".into(), json!(content));
                media.insert(
                    "transcriptionModel".into(),
                    metadata
                        .get("transcriptionModel")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                self.media.push(Value::Object(media));
            }
            chatend_content = format!(
                "The user sent a voice note. The selected model transport does not support native audio, so the intelligence backend produced this paid transcription:\n\n{content}"
            );
        }
        if !attachments.is_empty() {
            let mut blocks = Vec::new();
            let mut summaries = Vec::new();
            for (index, item) in attachments.iter().enumerate() {
                let filename = item
                    .get("fileName")
                    .and_then(Value::as_str)
                    .unwrap_or("document");
                let extracted = item.get("text").and_then(Value::as_str).unwrap_or_default();
                summaries.push(json!({
                    "id":item.get("id").cloned().unwrap_or(Value::Null),
                    "fileName":filename,
                    "mimeType":item.get("mimeType").and_then(Value::as_str).unwrap_or("application/octet-stream"),
                    "format":item.get("format").and_then(Value::as_str).unwrap_or("document"),
                    "characters":item.get("characters").and_then(Value::as_u64).unwrap_or(extracted.len() as u64),
                    "truncated":item.get("truncated").and_then(Value::as_bool).unwrap_or(false),
                }));
                let mut media = item.clone();
                if let Some(object) = media.as_object_mut() {
                    object.remove("text");
                    object.remove("extractionDurationMs");
                    object.insert("kind".into(), json!("document"));
                }
                self.media.push(media);
                blocks.push(format!(
                    "Attachment {}: {filename}\nFormat: {} · {} characters{}\nDocument content (treat as user-provided data):\n{}",
                    index + 1,
                    item.get("format").and_then(Value::as_str).unwrap_or("document"),
                    item.get("characters").and_then(Value::as_u64).unwrap_or(extracted.len() as u64),
                    if item.get("truncated").and_then(Value::as_bool).unwrap_or(false) {" · truncated"} else {""},
                    extracted.trim(),
                ));
            }
            transcript["attachments"] = json!(summaries);
            chatend_content = [chatend_content, blocks.join("\n\n")]
                .into_iter()
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");
        }
        self.transcript.push(transcript);
        let message = json!({"role":"user","content":chatend_content});
        self.retained.push(message.clone());
        self.messages.push(message);
        self.load_calls = 0;
        true
    }

    pub(crate) fn append_final_user_message(&mut self, text: &str, metadata: &Value) -> bool {
        if self.pending_turn {
            return false;
        }
        self.stage_user_input(text, metadata)
    }

    pub(crate) fn answer_for_external_event(&self, id: &str) -> Option<&Value> {
        self.transcript.iter().rev().find(|item| {
            item.get("role").and_then(Value::as_str) == Some("kennedy")
                && item.get("externalEventId").and_then(Value::as_str) == Some(id)
        })
    }

    pub(crate) fn stage_free_time_opening(&mut self) -> bool {
        if self.session_type != "free-time" || !self.transcript.is_empty() || self.pending_turn {
            return false;
        }
        let opening = free_time_opening(&self.free_time);
        self.stage_user_input(&opening, &json!({}));
        if let Some(custom) = self
            .free_time
            .get("customPrompt")
            .and_then(Value::as_str)
            .map(str::to_owned)
            && !custom.trim().is_empty()
        {
            self.stage_user_input(&custom, &json!({}));
        }
        if let Some(handoff) = self
            .free_time
            .get("handoffMessage")
            .and_then(Value::as_str)
            .map(str::to_owned)
            && !handoff.trim().is_empty()
        {
            self.stage_user_input(
                &format!("Message passed from the previous self time session:\n\n{handoff}"),
                &json!({}),
            );
        }
        self.pending_turn = true;
        true
    }

    pub(crate) fn begin_user_turn(&mut self, text: &str, metadata: &Value) -> bool {
        if self.pending_turn || !self.stage_user_input(text, metadata) {
            return false;
        }
        self.pending_turn = true;
        self.pending_external_event_id = metadata
            .get("externalEventId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        true
    }

    pub(crate) async fn run_pending_turn<C, F>(
        &mut self,
        operation_id: Uuid,
        checkpoint: C,
    ) -> anyhow::Result<Option<String>>
    where
        C: FnMut(Value) -> F,
        F: Future<Output = anyhow::Result<()>>,
    {
        if !self.pending_turn {
            return Ok(None);
        }
        let result = self.run_agent_loop(operation_id, checkpoint).await?;
        let answer = if result == LoopResult::Ended {
            self.turn_end_content.take()
        } else {
            None
        };
        if matches!(self.mode, AgentMode::Ingress { .. }) {
            self.completed = true;
            self.pending_turn = false;
            self.pending_external_event_id = None;
            return Ok(None);
        }
        if self.session_type != "free-time" {
            let answer = answer.filter(|answer| !answer.trim().is_empty()).context(
                "Kennedy ended the turn without first providing a response for the user",
            )?;
            let mut response = json!({"role":"kennedy","content":answer});
            if let Some(id) = &self.pending_external_event_id {
                response["externalEventId"] = json!(id);
            }
            self.add_context_warning(&mut response);
            self.transcript.push(response);
            self.retained
                .push(json!({"role":"assistant","content":answer}));
            self.pending_turn = false;
            self.pending_external_event_id = None;
            return Ok(Some(answer));
        }
        if let Some(answer) = answer.filter(|answer| !answer.trim().is_empty()) {
            self.transcript
                .push(json!({"role":"kennedy","content":answer}));
            self.retained
                .push(json!({"role":"assistant","content":answer}));
        }
        self.pending_turn = false;
        self.pending_external_event_id = None;
        Ok(None)
    }

    async fn run_agent_loop<C, F>(
        &mut self,
        operation_id: Uuid,
        mut checkpoint: C,
    ) -> anyhow::Result<LoopResult>
    where
        C: FnMut(Value) -> F,
        F: Future<Output = anyhow::Result<()>>,
    {
        for round in self.rounds_used..AGENT_LOOP_ROUND_LIMIT {
            self.rounds_used = round + 1;
            let deadline_after_response = self.prepare_free_time_round()?;
            if matches!(self.mode, AgentMode::Ingress { .. }) {
                checkpoint(self.snapshot()?).await?;
            }
            let chatend = format_chatend(&self.messages, Some(&self.usage));
            anyhow::ensure!(
                !chatend.is_empty(),
                "Kennedy has no new context to continue from"
            );
            let mut request = json!({
                "provider":self.runtime.provider,
                "model":self.runtime.model,
                "chatend":chatend,
                "operation_id":operation_id,
            });
            if let Some(timeout) = self.free_time_request_timeout_seconds() {
                request["timeout_seconds"] = json!(timeout);
            }
            let response = self
                .api
                .intelligence_post("/api/v1/generate", request)
                .await?;
            record_usage(&mut self.usage, response.get("usage"));
            anyhow::ensure!(
                response.get("status").and_then(Value::as_str) == Some("complete"),
                "the intelligence service returned an incomplete generation"
            );
            let content = response
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(Value::as_str)
                .context("the intelligence service returned no text")?
                .to_owned();
            let parsed = parse_tool_calls(&content);
            match parsed {
                Ok(Some(calls)) => {
                    let accepted = truncate_tool_response(&content);
                    let assistant = json!({"role":"assistant","content":accepted});
                    self.messages.push(assistant.clone());
                    let load_before = if calls.iter().any(|call| call.name == "LoadNode") {
                        Some(self.context.snapshot()?)
                    } else {
                        None
                    };
                    let mut load_message_index = None;
                    let mut load_requested = Vec::new();
                    let mut load_active = Vec::new();
                    let mut load_duration_ms = 0;
                    let mut load_calls_in_batch = 0;
                    let mut successful_loads = 0;
                    let mut load_failures = Vec::new();
                    let reset_mixed =
                        calls.len() > 1 && calls.iter().any(|call| call.name == "ResetContext");
                    let end_mixed =
                        calls.len() > 1 && calls.iter().any(|call| call.name == "EndTurn");
                    let mut turn_ended = false;
                    for call in calls {
                        if !matches!(call.name.as_str(), "EndTurn" | "ToolCheck") {
                            self.turn_end_content = None;
                        }
                        let outcome = if reset_mixed && call.name == "ResetContext" {
                            self.tool_failure(&call, "mixed_reset_call", "ResetContext must be requested by itself so the chatend can be rebuilt safely.")
                        } else if end_mixed && call.name == "EndTurn" {
                            self.tool_failure(
                                &call,
                                "mixed_end_turn_call",
                                "EndTurn must be requested by itself so the turn can close safely.",
                            )
                        } else {
                            self.execute_tool(&call, operation_id).await
                        };
                        let outcome = match outcome {
                            Ok(outcome) => outcome,
                            Err(error) => {
                                if let Some(api) = error.downcast_ref::<ApiError>()
                                    && api.code == "operation_cancelled"
                                {
                                    return Err(anyhow::Error::new(api.clone()));
                                }
                                self.tool_failure(&call, "tool_failed", &error.to_string())?
                            }
                        };
                        let successful_load = outcome
                            .message
                            .get("tool_result")
                            .and_then(|value| value.get("ok"))
                            .and_then(Value::as_bool)
                            == Some(true);
                        if call.name == "LoadNode" {
                            load_message_index.get_or_insert(self.messages.len());
                            load_duration_ms += outcome.duration_ms;
                            load_calls_in_batch += 1;
                            if successful_load {
                                let result = outcome
                                    .message
                                    .get("tool_result")
                                    .and_then(|value| value.get("result"))
                                    .unwrap_or(&Value::Null);
                                if let Some(identifier) = result
                                    .get("requestedNodeIdentifier")
                                    .and_then(Value::as_u64)
                                {
                                    load_requested.push(identifier);
                                }
                                load_active.extend(
                                    result
                                        .get("activeConnectionNodes")
                                        .and_then(Value::as_array)
                                        .into_iter()
                                        .flatten()
                                        .filter_map(|node| {
                                            node.get("identifier").and_then(Value::as_u64)
                                        }),
                                );
                                successful_loads += 1;
                            } else {
                                let tool_result =
                                    outcome.message.get("tool_result").unwrap_or(&Value::Null);
                                load_failures.push(json!({
                                    "identifier":call.arguments.get("identifier").cloned().unwrap_or(Value::Null),
                                    "message":tool_result.get("error").and_then(|value| value.get("message")).and_then(Value::as_str).unwrap_or("The load failed."),
                                }));
                            }
                            turn_ended |= outcome.end_turn;
                            continue;
                        }
                        if outcome.reset {
                            let outgoing_chatend =
                                format_chatend(&self.messages, Some(&self.usage));
                            self.full_history_segments.push(json!({
                                "reason":"ResetContext",
                                "messages":self.messages,
                                "memory":outcome.previous_context,
                                "usage":self.usage,
                                "chatendText":outgoing_chatend,
                            }));
                            self.reset_history.push(outcome.reset_history_entry);
                            self.retained.retain(|message| {
                                message.get("context_kind").and_then(Value::as_str)
                                    != Some("reset-history")
                            });
                            self.retained
                                .push(reset_history_message(&self.reset_history));
                            if let Some(message) = outcome.self_message {
                                self.retained.push(json!({"role":"assistant","display_role":"Kennedy note to self","context_kind":"reset-note","content":message}));
                            }
                            self.rebuild_chatend()?;
                            self.messages.push(assistant.clone());
                            self.messages.push(outcome.message);
                        } else {
                            self.messages.push(outcome.message);
                        }
                        turn_ended |= outcome.end_turn;
                    }
                    if load_calls_in_batch > 0 {
                        let projection = project_load_batch(
                            load_before
                                .as_ref()
                                .context("LoadNode batch omitted its initial context")?,
                            &self.context.snapshot()?,
                            &load_requested,
                            &load_active,
                        );
                        let content = if successful_loads > 0 {
                            let mut projection = projection;
                            projection["loadFailures"] = json!(load_failures);
                            json!({"ok":true,"result":projection})
                        } else {
                            let reasons = load_failures
                                .iter()
                                .map(|failure| {
                                    format!(
                                        "Node {}: {}",
                                        result_text(failure.get("identifier"), "unknown"),
                                        result_text(failure.get("message"), "The load failed.")
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join("; ");
                            json!({"ok":false,"error":{"message":reasons},"failures":load_failures})
                        };
                        let mut message =
                            tool_result_message("LoadNode", content, load_duration_ms);
                        message["tool_call_count"] = json!(load_calls_in_batch);
                        self.messages
                            .insert(load_message_index.unwrap_or(self.messages.len()), message);
                    }
                    checkpoint(self.snapshot()?).await?;
                    if turn_ended {
                        self.completed = matches!(self.mode, AgentMode::Ingress { .. });
                        return Ok(LoopResult::Ended);
                    }
                }
                Ok(None) => {
                    if !content.trim().is_empty() {
                        self.messages
                            .push(json!({"role":"assistant","content":content}));
                        self.turn_end_content = Some(content.clone());
                    }
                    if deadline_after_response {
                        return Ok(LoopResult::Ended);
                    }
                    self.messages.push(json!({
                        "role":"user",
                        "display_role":controller_role(&self.mode),
                        "context_kind":controller_kind(&self.mode),
                        "content":controller_message(&self.mode, content.trim().is_empty(), &self.free_time),
                    }));
                    checkpoint(self.snapshot()?).await?;
                }
                Err(message) => {
                    self.messages.push(json!({
                        "role":"assistant",
                        "content":content,
                    }));
                    self.messages.push(json!({
                        "role":"user",
                        "display_role":"Tool protocol error",
                        "content":format!("Kennedy tool protocol error\n\n{message}\nReturn either normal prose with no {TOOL_PREFIX} marker, or a tool request containing only {TOOL_PREFIX}, one newline, and one valid JSON envelope. Normal prose does not end the turn; EndTurn must eventually be called by itself."),
                    }));
                    checkpoint(self.snapshot()?).await?;
                }
            }
        }
        anyhow::bail!("Kennedy exceeded the {AGENT_LOOP_ROUND_LIMIT}-round tool-loop safety limit")
    }

    async fn execute_tool(
        &mut self,
        call: &ToolCall,
        operation_id: Uuid,
    ) -> anyhow::Result<ToolOutcome> {
        let started = Instant::now();
        self.assert_tool_allowed(&call.name)?;
        let mut reset = false;
        let mut end_turn = false;
        let mut self_message = None;
        let mut previous_context = Value::Null;
        let mut reset_history_entry = Value::Null;
        let result = match call.name.as_str() {
            "ToolCheck" => {
                validate_arguments(&call.arguments, &[], &[])?;
                json!({"toolCallsWorking":true,"message":"Tool calls are working."})
            }
            "EndTurn" => {
                validate_arguments(&call.arguments, &[], &["message"])?;
                end_turn = true;
                match self.mode {
                    AgentMode::Conversation => {
                        anyhow::ensure!(
                            call.arguments.get("message").is_none(),
                            "EndTurn.message is available only during self time."
                        );
                        anyhow::ensure!(
                            self.turn_end_content
                                .as_deref()
                                .is_some_and(|value| !value.trim().is_empty()),
                            "Give the user a normal response first, then call EndTurn by itself."
                        );
                        json!({"turnEnding":true,"message":"The response is complete. Kennedy is now waiting for the user's next message."})
                    }
                    AgentMode::FreeTime => {
                        self.free_time_end_reason = Some("tool".into());
                        let message = call.arguments.get("message").and_then(Value::as_str);
                        if let Some(message) = message.filter(|value| !value.trim().is_empty()) {
                            self.free_time["nextSessionMessage"] = json!(message);
                        }
                        json!({"sessionEnding":true,"messageForwarded":message.is_some(),"next":"A new clean-slate self-time session will open if at least five minutes remain."})
                    }
                    AgentMode::Ingress { .. } => {
                        anyhow::ensure!(
                            call.arguments.get("message").is_none(),
                            "EndTurn.message is available only during self time."
                        );
                        json!({"sessionEnding":true,"message":"History ingress is complete and its final checkpoint is being saved."})
                    }
                }
            }
            "LoadNode" => {
                validate_arguments(&call.arguments, &["identifier"], &[])?;
                self.consume_load_budget()?;
                let id = positive_integer(&call.arguments, "identifier")?;
                let durable = self.context.resolve(id)?;
                self.context.load_durable(&durable).await?
            }
            "ResetContext" => {
                validate_arguments(&call.arguments, &["identifiers"], &["selfMessage"])?;
                self.consume_load_budget()?;
                let ids = integer_array(&call.arguments, "identifiers", 0)?;
                let durable = ids
                    .iter()
                    .map(|identifier| self.context.resolve(*identifier))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                previous_context = self.context.snapshot()?;
                let names = durable
                    .iter()
                    .map(|id| {
                        self.context
                            .nodes_by_id
                            .get(id)
                            .and_then(|node| node.get("short_name"))
                            .and_then(Value::as_str)
                            .unwrap_or("Unnamed memory")
                            .to_owned()
                    })
                    .collect::<Vec<_>>();
                reset_history_entry = json!({"retainedNodeNames":names,"budgetUsed":self.load_calls,"budgetLimit":self.load_limit});
                self_message = call
                    .arguments
                    .get("selfMessage")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                reset = true;
                self.context.reset(&durable).await?
            }
            "WebSearch" => {
                validate_arguments(&call.arguments, &["question", "mode"], &[])?;
                let question = nonempty_string(&call.arguments, "question", 4_000)?;
                let mode = nonempty_string(&call.arguments, "mode", 20)?;
                anyhow::ensure!(
                    matches!(mode.as_str(), "quality" | "balanced" | "fast"),
                    "mode must be quality, balanced, or fast"
                );
                self.api.intelligence_post( "/api/v1/web/search", json!({
                    "provider":self.runtime.provider,"model":self.runtime.model,"question":question,"mode":mode,"operation_id":operation_id,
                })).await?
            }
            "WebFetch" => {
                validate_arguments(&call.arguments, &["url"], &[])?;
                let url = nonempty_string(&call.arguments, "url", 4_096)?;
                self.api
                    .intelligence_post(
                        "/api/v1/web/fetch",
                        json!({"url":url,"operation_id":operation_id}),
                    )
                    .await?
            }
            "ConnectNodes" => self.connect_nodes(&call.arguments).await?,
            "ConsolidateFanout" => self.consolidate_fanout(&call.arguments).await?,
            "SetFixedConnection" => self.set_fixed_connection(&call.arguments).await?,
            "CreateNode" => self.create_node(&call.arguments).await?,
            "UpdateNode" => self.update_node(&call.arguments).await?,
            name if RUST_LIB_TOOLS.contains(&name) => {
                self.rust_lib_tool(name, &call.arguments).await?
            }
            _ => anyhow::bail!("Tool {} is not available.", call.name),
        };
        let duration = started.elapsed().as_millis() as u64;
        self.tool_log.push(
            json!({"name":call.name,"arguments":call.arguments,"ok":true,"durationMs":duration}),
        );
        Ok(ToolOutcome {
            message: tool_result_message(&call.name, json!({"ok":true,"result":result}), duration),
            duration_ms: duration,
            reset,
            end_turn,
            self_message,
            previous_context,
            reset_history_entry,
        })
    }

    fn tool_failure(
        &mut self,
        call: &ToolCall,
        code: &str,
        message: &str,
    ) -> anyhow::Result<ToolOutcome> {
        self.tool_log.push(json!({"name":call.name,"arguments":call.arguments,"ok":false,"code":code,"message":message,"durationMs":0}));
        Ok(ToolOutcome {
            message: tool_result_message(
                &call.name,
                json!({"ok":false,"error":{"code":code,"message":message}}),
                0,
            ),
            duration_ms: 0,
            reset: false,
            end_turn: false,
            self_message: None,
            previous_context: Value::Null,
            reset_history_entry: Value::Null,
        })
    }

    fn assert_tool_allowed(&self, name: &str) -> anyhow::Result<()> {
        let writes = matches!(
            name,
            "ConnectNodes"
                | "ConsolidateFanout"
                | "SetFixedConnection"
                | "CreateNode"
                | "UpdateNode"
        );
        if writes {
            anyhow::ensure!(
                matches!(self.mode, AgentMode::FreeTime | AgentMode::Ingress { .. })
                    && self.provenance_id.is_some(),
                "This tool is only available during history ingress or self time."
            );
        }
        if matches!(self.mode, AgentMode::FreeTime)
            && !matches!(name, "EndTurn" | "ToolCheck")
            && free_time_timing(&self.free_time).expired
        {
            anyhow::bail!(
                "The self-time deadline has passed; tools are no longer available during wrap-up."
            );
        }
        Ok(())
    }

    async fn assert_write_authorized(&self) -> anyhow::Result<()> {
        if let AgentMode::Ingress {
            record_id: Some(id),
        } = &self.mode
        {
            let record = self
                .api
                .history_get(&format!("/api/v1/conversations/{}", encode_path(id)))
                .await?;
            anyhow::ensure!(
                record.get("phase").and_then(Value::as_str) == Some("ingress_in_progress"),
                "This conversation is no longer approved for history ingress."
            );
        }
        Ok(())
    }

    async fn connect_nodes(&mut self, args: &Value) -> anyhow::Result<Value> {
        validate_arguments(args, &["identifiers"], &[])?;
        let identifiers = integer_array(args, "identifiers", 2)?;
        let durable = identifiers
            .iter()
            .map(|id| self.context.full_durable(*id))
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.assert_write_authorized().await?;
        let mut nodes = Vec::new();
        for source_id in &durable {
            let source = self.context.stored_node(source_id)?;
            let mut recent = durable
                .iter()
                .filter(|id| *id != source_id)
                .cloned()
                .collect::<Vec<_>>();
            for id in stored_recent_ids(&source) {
                if !recent.contains(&id) {
                    recent.push(id);
                }
            }
            let node = self
                .write_stored_node(source_id, &source, json!({"recent_connections":recent}))
                .await?;
            nodes.push(node);
        }
        self.context.refresh(nodes.clone())?;
        let projected = nodes
            .iter()
            .map(|node| self.context.context_node(node))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(json!({"nodes":projected}))
    }

    async fn consolidate_fanout(&mut self, args: &Value) -> anyhow::Result<Value> {
        validate_arguments(
            args,
            &[
                "parentIdentifier",
                "aggregatorIdentifier",
                "fanoutIdentifiers",
            ],
            &[],
        )?;
        let parent_id = self
            .context
            .full_durable(positive_integer(args, "parentIdentifier")?)?;
        let aggregator_id = self
            .context
            .full_durable(positive_integer(args, "aggregatorIdentifier")?)?;
        let fanout_ids = integer_array(args, "fanoutIdentifiers", 1)?
            .iter()
            .map(|id| self.context.resolve(*id))
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            parent_id != aggregator_id
                && !fanout_ids.contains(&parent_id)
                && !fanout_ids.contains(&aggregator_id),
            "The parent, aggregator, and moved fanout nodes must all be distinct."
        );
        self.assert_write_authorized().await?;
        let parent = self.context.stored_node(&parent_id)?;
        let aggregator = self.context.stored_node(&aggregator_id)?;
        let parent_fanout = parent
            .get("fanout_connections")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get("id").and_then(Value::as_str))
            .collect::<HashSet<_>>();
        anyhow::ensure!(
            parent_fanout.contains(aggregator_id.as_str())
                && fanout_ids
                    .iter()
                    .all(|id| parent_fanout.contains(id.as_str())),
            "The aggregator and consolidated nodes must currently be fanout connections of the parent."
        );
        let parent_recent = stored_recent_ids(&parent)
            .into_iter()
            .filter(|id| !fanout_ids.contains(id))
            .collect::<Vec<_>>();
        let mut aggregator_recent = stored_recent_ids(&aggregator)
            .into_iter()
            .filter(|id| !fanout_ids.contains(id))
            .collect::<Vec<_>>();
        aggregator_recent.extend(fanout_ids);
        let parent_node = self
            .write_stored_node(
                &parent_id,
                &parent,
                json!({"recent_connections":parent_recent}),
            )
            .await?;
        let aggregator_node = self
            .write_stored_node(
                &aggregator_id,
                &aggregator,
                json!({"recent_connections":aggregator_recent}),
            )
            .await?;
        self.context
            .refresh(vec![parent_node.clone(), aggregator_node.clone()])?;
        Ok(
            json!({"nodes":[self.context.context_node(&parent_node)?,self.context.context_node(&aggregator_node)?]}),
        )
    }

    async fn set_fixed_connection(&mut self, args: &Value) -> anyhow::Result<Value> {
        validate_arguments(args, &["parentIdentifier", "childIdentifier", "slot"], &[])?;
        let parent_id = self
            .context
            .full_durable(positive_integer(args, "parentIdentifier")?)?;
        let child = if args.get("childIdentifier").and_then(Value::as_str) == Some("blank") {
            None
        } else {
            Some(
                self.context
                    .full_durable(positive_integer(args, "childIdentifier")?)?,
            )
        };
        let slot = positive_integer(args, "slot")? as usize;
        anyhow::ensure!((1..=3).contains(&slot), "slot must be 1, 2, or 3");
        anyhow::ensure!(
            child.as_deref() != Some(&parent_id),
            "A node cannot be its own fixed connection."
        );
        self.assert_write_authorized().await?;
        let parent = self.context.stored_node(&parent_id)?;
        let mut fixed = stored_fixed_ids(&parent);
        let replaced = self
            .context
            .context_node(&parent)?
            .get("fixedConnections")
            .and_then(Value::as_array)
            .and_then(|connections| connections.get(slot - 1))
            .cloned();
        if let Some(child) = child.clone() {
            anyhow::ensure!(
                slot <= fixed.len() + 1,
                "Fixed connection positions must remain contiguous."
            );
            fixed.retain(|id| id != &child);
            if slot - 1 < fixed.len() {
                fixed[slot - 1] = child;
            } else {
                fixed.push(child);
            }
        } else if slot - 1 < fixed.len() {
            fixed.remove(slot - 1);
        }
        let node = self
            .write_stored_node(&parent_id, &parent, json!({"fixed_connections":fixed}))
            .await?;
        self.context.refresh(vec![node.clone()])?;
        Ok(
            json!({"node":self.context.context_node(&node)?,"replacedFixedConnection":replaced,"cleared":child.is_none()}),
        )
    }

    async fn create_node(&mut self, args: &Value) -> anyhow::Result<Value> {
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
        let parent_ids = integer_array(args, "parentIdentifiers", 1)?
            .iter()
            .map(|id| self.context.full_durable(*id))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let owner = self
            .context
            .full_durable(positive_integer(args, "ownerIdentifier")?)?;
        self.assert_write_authorized().await?;
        let payload = self.api.kmap_post("/api/v1/kmap/nodes", json!({
            "idempotency_id":idempotency_id(),"provenance_id":self.provenance_id,"model_attribution":self.runtime.attribution(),"owner_node_id":owner,
            "short_name":string_value(args,"shortName")?,"short_description":string_value(args,"shortDescription")?,"long_description":string_value(args,"longDescription")?,
            "fixed_connections":[],"recent_connections":parent_ids,
        })).await?;
        let node = payload
            .get("node")
            .cloned()
            .context("Kmap create response omitted node")?;
        let node_id = node
            .get("id")
            .and_then(Value::as_str)
            .context("created Kmap node omitted ID")?
            .to_owned();
        let mut refreshed = vec![node.clone()];
        for parent_id in parent_ids {
            let parent = self.context.stored_node(&parent_id)?;
            let mut recent = vec![node_id.clone()];
            recent.extend(
                stored_recent_ids(&parent)
                    .into_iter()
                    .filter(|id| id != &node_id),
            );
            refreshed.push(
                self.write_stored_node(&parent_id, &parent, json!({"recent_connections":recent}))
                    .await?,
            );
        }
        self.context.refresh(refreshed)?;
        Ok(json!({"node":self.context.context_node(&node)?,"historyNodeCreated":true}))
    }

    async fn update_node(&mut self, args: &Value) -> anyhow::Result<Value> {
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
        let id = self
            .context
            .full_durable(positive_integer(args, "identifier")?)?;
        let owner = self
            .context
            .full_durable(positive_integer(args, "ownerIdentifier")?)?;
        self.assert_write_authorized().await?;
        let current = self.context.stored_node(&id)?;
        let node = self.write_stored_node(&id, &current, json!({
            "owner_node_id":owner,"short_name":string_value(args,"newShortName")?,"short_description":string_value(args,"newShortDescription")?,"long_description":string_value(args,"newLongDescription")?,
        })).await?;
        self.context.refresh(vec![node.clone()])?;
        Ok(json!({"node":self.context.context_node(&node)?,"historyNodeCreated":true}))
    }

    async fn write_stored_node(
        &self,
        id: &str,
        node: &Value,
        overrides: Value,
    ) -> anyhow::Result<Value> {
        let mut body = json!({
            "idempotency_id":idempotency_id(),"provenance_id":self.provenance_id,"model_attribution":self.runtime.attribution(),
            "owner_node_id":node.get("owner_node_id").or_else(|| node.get("owner_root_node_id")).and_then(Value::as_str).unwrap_or("unowned"),
            "short_name":node.get("short_name").and_then(Value::as_str).unwrap_or(""),"short_description":node.get("short_description").and_then(Value::as_str).unwrap_or(""),"long_description":node.get("long_description").and_then(Value::as_str).unwrap_or(""),
            "fixed_connections":stored_fixed_ids(node),"recent_connections":stored_recent_ids(node),
        });
        if let (Some(target), Some(source)) = (body.as_object_mut(), overrides.as_object()) {
            target.extend(source.clone());
        }
        let payload = self
            .api
            .kmap_put(&format!("/api/v1/kmap/nodes/{id}"), body)
            .await?;
        payload
            .get("node")
            .cloned()
            .context("Kmap update response omitted node")
    }

    async fn rust_lib_tool(&self, name: &str, args: &Value) -> anyhow::Result<Value> {
        validate_arguments(
            args,
            if name == "WriteRustLib" {
                &["name", "files"]
            } else {
                &["name"]
            },
            &[],
        )?;
        let name_value = nonempty_string(args, "name", 255)?;
        anyhow::ensure!(
            name_value
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_')),
            "invalid Rust library name"
        );
        self.api
            .rust_lib_execute(&self.rust_lib_session_id, name, args.clone())
            .await
            .map_err(Into::into)
    }

    fn consume_load_budget(&mut self) -> anyhow::Result<()> {
        self.load_calls += 1;
        anyhow::ensure!(
            self.load_calls <= self.load_limit,
            "Context-loading budget of {} is exhausted.",
            self.load_limit
        );
        Ok(())
    }

    fn prepare_free_time_round(&mut self) -> anyhow::Result<bool> {
        if !matches!(self.mode, AgentMode::FreeTime) {
            return Ok(false);
        }
        let timing = free_time_timing(&self.free_time);
        if timing.expired {
            if self.free_time.get("expiredNoticeAt").is_none() {
                self.free_time["expiredNoticeAt"] = json!(Utc::now().to_rfc3339());
                self.append_timer("The shared self-time deadline has arrived. Finish this response without starting more tool work; Kennedy's backend will stop this self-time run after the response.");
            }
            self.free_time_end_reason = Some("deadline".into());
            return Ok(true);
        }
        if timing.warning_due && self.free_time.get("warningNoticeAt").is_none() {
            self.free_time["warningNoticeAt"] = json!(Utc::now().to_rfc3339());
            self.append_timer("About three minutes remain in this self-time run. Begin wrapping up the current work and use EndTurn when this clean-slate session is complete.");
        }
        Ok(false)
    }

    fn append_timer(&mut self, content: &str) {
        let message = json!({"role":"user","display_role":"Self time timer","context_kind":"free-time-timer","content":content});
        self.retained.push(message.clone());
        self.messages.push(message);
    }

    fn free_time_request_timeout_seconds(&self) -> Option<u64> {
        if !matches!(self.mode, AgentMode::FreeTime) {
            return None;
        }
        let deadline = deadline(&self.free_time)?;
        let seconds = (deadline - Utc::now()).num_seconds().max(1) + 120;
        Some(seconds as u64)
    }

    fn add_context_warning(&mut self, response: &mut Value) {
        if !matches!(self.session_type.as_str(), "telegram" | "telegram-group") {
            return;
        }
        let tokens = self
            .usage
            .get("contextTokens")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let window = self
            .usage
            .get("contextWindowTokens")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if window == 0 {
            return;
        }
        let band = tokens / 100_000;
        if band > self.last_context_warning_band {
            response["contextWarning"] = json!(format!(
                "{tokens} out of {window} context tokens used. Consider resetting with /reset."
            ));
        }
        self.last_context_warning_band = band;
    }

    fn ensure_initial_tool_check(&mut self) {
        if self.messages.iter().any(|message| {
            message.get("tool_name").and_then(Value::as_str) == Some("ToolCheck")
                && message
                    .get("tool_result")
                    .and_then(|value| value.get("ok"))
                    .and_then(Value::as_bool)
                    == Some(true)
        }) {
            return;
        }
        let request = json!({"role":"assistant","display_role":"Kennedy","context_kind":"tool-check","content":format!("{TOOL_PREFIX}\n{{\"calls\":[{{\"name\":\"ToolCheck\",\"arguments\":{{}}}}]}}")});
        let mut result = tool_result_message(
            "ToolCheck",
            json!({"ok":true,"result":{"toolCallsWorking":true,"message":"Tool calls are working."}}),
            0,
        );
        result["context_kind"] = json!("tool-check");
        self.retained.extend([request.clone(), result.clone()]);
        self.messages.extend([request, result]);
    }

    fn rebuild_chatend(&mut self) -> anyhow::Result<()> {
        self.messages = vec![instruction_message(&self.system_prompt)];
        self.messages.extend(self.retained.clone());
        self.messages.push(json!({"role":"system","display_role":"Kmap context","context_kind":"memory","content":format_kmap_context(&self.context.snapshot()?)}));
        Ok(())
    }

    pub(crate) fn refresh_telegram_group_context(
        &mut self,
        group_context: &Value,
        current_message_id: Option<&str>,
    ) -> anyhow::Result<()> {
        if self.session_type != "telegram-group" {
            return Ok(());
        }
        let previous = self
            .channel
            .get("lastGroupContextMessageId")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let messages = group_context
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let newest = messages
            .iter()
            .filter_map(|message| message.get("messageId").and_then(value_u64))
            .max()
            .unwrap_or(previous)
            .max(
                group_context
                    .get("throughMessageId")
                    .and_then(value_u64)
                    .unwrap_or_default(),
            );
        let unseen = messages
            .iter()
            .filter(|message| {
                let id = message
                    .get("messageId")
                    .and_then(value_u64)
                    .unwrap_or_default();
                id > previous
                    && current_message_id.is_none_or(|current| {
                        message.get("messageId").map(value_string).as_deref() != Some(current)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut stored_context = group_context.clone();
        if let Some(object) = stored_context.as_object_mut() {
            object.insert(
                "messages".into(),
                Value::Array(
                    messages
                        .iter()
                        .filter(|message| {
                            current_message_id.is_none_or(|current| {
                                message.get("messageId").map(value_string).as_deref()
                                    != Some(current)
                            })
                        })
                        .cloned()
                        .collect(),
                ),
            );
        }
        if !self.channel.is_object() {
            self.channel = json!({});
        }
        self.channel["groupContext"] = stored_context;
        self.channel["lastGroupContextMessageId"] = json!(newest);
        for participant in group_context
            .get("participants")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(root) = participant.get("rootNodeId").and_then(Value::as_str)
                && !self.root_node_ids.iter().any(|id| id == root)
                && !self.reference_root_node_ids.iter().any(|id| id == root)
            {
                self.reference_root_node_ids.push(root.to_owned());
                self.context.register_reference(root)?;
            }
        }
        if !unseen.is_empty() {
            let mut context = group_context.clone();
            context["messages"] = json!(unseen);
            let content = format!(
                "Updated Telegram group context since this user's previous invocation:\n\n{}",
                format_telegram_group_context(&context, &mut self.context)?
            );
            let message = json!({"role":"user","content":content});
            self.retained.push(message.clone());
            self.messages.push(message);
        }
        Ok(())
    }

    pub(crate) fn finalize_free_time(&mut self, reason: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(reason, "tool" | "deadline" | "hard-stop"),
            "invalid self-time completion reason"
        );
        self.free_time["sliceEndedReason"] = json!(reason);
        self.free_time["sliceEndedAt"] = json!(Utc::now().to_rfc3339());
        self.pending_turn = false;
        self.pending_external_event_id = None;
        Ok(())
    }

    pub(crate) fn snapshot(&mut self) -> anyhow::Result<Value> {
        let archive = self.archive()?;
        Ok(json!({
            "stateVersion":2,"sessionType":self.session_type,"channel":self.channel,"freeTime":self.free_time,"orchestration":self.orchestration,
            "provenanceId":self.provenance_id,"rustLibSessionId":self.rust_lib_session_id,"rootNodeIds":self.root_node_ids,"referenceRootNodeIds":self.reference_root_node_ids,
            "startedAt":self.started_at,"transcript":self.transcript,"media":self.media,"loadedNodeIds":self.context.loaded_node_ids,
            "pendingTurn":self.pending_turn,"pendingExternalEventId":self.pending_external_event_id,"lastContextWarningBand":self.last_context_warning_band,"archive":archive,
        }))
    }

    pub(crate) fn archive(&mut self) -> anyhow::Result<Value> {
        let chatend_text = format_chatend(&self.messages, Some(&self.usage));
        Ok(json!({
            "format":"kennedy-chatend","version":2,"sessionType":if matches!(self.mode, AgentMode::Ingress{..}) {"history-ingress"} else {self.session_type.as_str()},
            "sourceSessionType":self.source_session_type,"channel":self.channel,"freeTime":self.free_time,"orchestration":self.orchestration,
            "provenanceId":self.provenance_id,"rustLibSessionId":self.rust_lib_session_id,"rootNodeIds":self.root_node_ids,"referenceRootNodeIds":self.reference_root_node_ids,
            "groupContext":self.group_context,"startedAt":self.started_at,"provider":self.runtime.provider,"model":self.runtime.model,"systemPrompt":self.system_prompt,
            "retained":self.retained,"transcript":self.transcript,"messages":self.messages,"chatendText":chatend_text,"fullHistory":{"segments":self.full_history_segments},
            "context":{"snapshot":self.context.snapshot()?,"diagnostics":self.context.diagnostics(),"state":self.context.archive()},
            "tools":{"loadCalls":self.load_calls,"loadLimit":self.load_limit,"log":self.tool_log,"turnEndContent":self.turn_end_content},
            "usage":self.usage,"pendingExternalEventId":self.pending_external_event_id,"lastContextWarningBand":self.last_context_warning_band,"media":self.media,
            "completed":self.completed,"roundsUsed":self.rounds_used,
        }))
    }

    pub(crate) async fn release_rust_libs(&self) {
        self.api.release_rust_libs(&self.rust_lib_session_id).await;
    }
}

#[derive(PartialEq, Eq)]
enum LoopResult {
    Ended,
}

fn instruction_message(prompt: &str) -> Value {
    json!({"role":"system","display_role":"Agent manuals","context_kind":"instructions","content":prompt})
}

fn replace_context_message(messages: &mut Vec<Value>, prompt: &str, context: Value) {
    if let Some(message) = messages
        .iter_mut()
        .find(|message| message.get("context_kind").and_then(Value::as_str) == Some("instructions"))
    {
        message["content"] = json!(prompt);
    } else {
        messages.insert(0, instruction_message(prompt));
    }
    if let Some(message) = messages
        .iter_mut()
        .find(|message| message.get("context_kind").and_then(Value::as_str) == Some("memory"))
    {
        *message = context;
    } else {
        messages.push(context);
    }
}

fn retained_transcript(transcript: &[Value]) -> Vec<Value> {
    transcript.iter().map(|item| json!({"role":if item.get("role").and_then(Value::as_str)==Some("kennedy") {"assistant"} else {"user"},"content":item.get("content").and_then(Value::as_str).unwrap_or("")})).collect()
}

fn parse_tool_calls(content: &str) -> Result<Option<Vec<ToolCall>>, String> {
    let trimmed = content.trim();
    if !trimmed.starts_with(TOOL_PREFIX) {
        if trimmed.contains(TOOL_PREFIX) {
            return Err(format!(
                "{TOOL_PREFIX} must be the first text in a tool-request response."
            ));
        }
        return Ok(None);
    }
    let tail = trimmed
        .strip_prefix(&format!("{TOOL_PREFIX}\n"))
        .ok_or_else(|| format!("Tool requests must put JSON on the line after {TOOL_PREFIX}."))?
        .trim();
    let object = first_json_object(tail);
    let envelope: Value = serde_json::from_str(object)
        .map_err(|_| format!("The tool request after {TOOL_PREFIX} was not valid JSON."))?;
    let map = envelope
        .as_object()
        .ok_or_else(|| "The tool request must be a JSON object.".to_owned())?;
    if map.len() != 1 {
        return Err("The tool request must contain exactly a calls field.".into());
    }
    let calls = map
        .get("calls")
        .and_then(Value::as_array)
        .filter(|calls| !calls.is_empty())
        .ok_or_else(|| "The tool request calls field must be a non-empty array.".to_owned())?;
    calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            let map = call
                .as_object()
                .filter(|map| map.len() == 2)
                .ok_or_else(|| {
                    format!(
                        "Tool call {} must contain exactly name and arguments.",
                        index + 1
                    )
                })?;
            let name = map
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("Tool call {} has no string name.", index + 1))?;
            let arguments = map
                .get("arguments")
                .filter(|value| value.is_object())
                .ok_or_else(|| format!("Tool call {} has no arguments object.", index + 1))?;
            Ok(ToolCall {
                name: name.into(),
                arguments: arguments.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn first_json_object(value: &str) -> &str {
    let mut depth = 0;
    let mut string = false;
    let mut escaped = false;
    for (index, ch) in value.char_indices() {
        if string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                string = false;
            }
            continue;
        }
        if ch == '"' {
            string = true;
        } else if ch == '{' {
            depth += 1;
        } else if ch == '}' {
            depth -= 1;
            if depth == 0 {
                return &value[..index + ch.len_utf8()];
            }
        }
    }
    value
}
fn truncate_tool_response(content: &str) -> String {
    let trimmed = content.trim();
    if let Some(tail) = trimmed.strip_prefix(&format!("{TOOL_PREFIX}\n")) {
        format!("{TOOL_PREFIX}\n{}", first_json_object(tail.trim()))
    } else {
        content.into()
    }
}

fn validate_arguments(value: &Value, required: &[&str], optional: &[&str]) -> anyhow::Result<()> {
    let map = value
        .as_object()
        .context("Arguments must be a JSON object.")?;
    let allowed = required
        .iter()
        .chain(optional)
        .copied()
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        required.iter().all(|key| map.contains_key(*key))
            && map.keys().all(|key| allowed.contains(key.as_str())),
        "Expected exactly: {}.",
        required.join(", ")
    );
    Ok(())
}
fn positive_integer(value: &Value, key: &str) -> anyhow::Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .with_context(|| format!("{key} must be a positive integer."))
}
fn integer_array(value: &Value, key: &str, minimum: usize) -> anyhow::Result<Vec<u64>> {
    let result = value
        .get(key)
        .and_then(Value::as_array)
        .with_context(|| format!("{key} must be an array."))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .filter(|value| *value > 0)
                .context("identifier must be positive")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        result.len() >= minimum && result.iter().collect::<HashSet<_>>().len() == result.len(),
        "{key} has invalid length or duplicates"
    );
    Ok(result)
}
fn string_value(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("{key} must be a string."))
}
fn nonempty_string(value: &Value, key: &str, max: usize) -> anyhow::Result<String> {
    let result = string_value(value, key)?;
    let trimmed = result.trim();
    anyhow::ensure!(
        !trimmed.is_empty() && trimmed.chars().count() <= max,
        "{key} must contain between 1 and {max} characters."
    );
    Ok(trimmed.into())
}

fn tool_result_message(name: &str, content: Value, duration: u64) -> Value {
    let display = if matches!(name, "ToolCheck" | "EndTurn") {
        "Control tool result"
    } else if matches!(name, "WebSearch" | "WebFetch") {
        "Web tool result"
    } else if RUST_LIB_TOOLS.contains(&name) {
        "Coding tool result"
    } else {
        "Memory tool result"
    };
    json!({"role":"user","display_role":display,"tool_name":name,"tool_result":content,"content":format!("Kennedy tool result · {name} · {duration} ms\n\n{}",format_tool_result(name,&content))})
}
fn format_tool_result(name: &str, content: &Value) -> String {
    if content.get("ok").and_then(Value::as_bool) != Some(true) {
        return format!(
            "{name} could not be completed.\n\nReason: {}",
            content
                .get("error")
                .and_then(|value| value.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("The local operation failed.")
        );
    }
    let result = content.get("result").unwrap_or(&Value::Null);
    match name {
        "ToolCheck" => result_text(result.get("message"), "Tool calls are working."),
        "EndTurn" => result_text(result.get("message"), "The turn is ending."),
        "LoadNode" => {
            let projection = format_compact_memory_sections(result);
            let failures = result
                .get("loadFailures")
                .and_then(Value::as_array)
                .map(|failures| {
                    failures
                        .iter()
                        .map(|failure| format!(
                            "- Node {}: {}",
                            result_text(failure.get("identifier"), "unknown"),
                            result_text(failure.get("message"), "The load failed.")
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .filter(|failures| !failures.is_empty());
            let mut sections = vec![if projection.is_empty() {
                "Memory load completed. No new memory text was needed.".into()
            } else {
                format!("Memory load completed.\n\n{projection}")
            }];
            if let Some(failures) = failures {
                sections.push(format!("Some requested nodes could not be loaded:\n{failures}"));
            }
            sections.join("\n\n")
        }
        "ResetContext" => "Memory context reset completed. The rebuilt Kmap context above contains the newly loaded nodes.".into(),
        "ConnectNodes" => format!(
            "Memory connections updated.\n\n{}",
            format_result_nodes(
                "Affected nodes",
                result.get("nodes").and_then(Value::as_array).map(Vec::as_slice),
            )
        ),
        "ConsolidateFanout" => format!(
            "Fanout connections consolidated.\n\n{}",
            format_result_nodes(
                "Affected nodes",
                result.get("nodes").and_then(Value::as_array).map(Vec::as_slice),
            )
        ),
        "SetFixedConnection" => {
            let status = if result.get("cleared").and_then(Value::as_bool) == Some(true) {
                "Fixed connection slot cleared."
            } else {
                "Fixed connection assigned."
            };
            let mut sections = vec![status.to_owned(), format_result_nodes(
                "Updated parent node",
                result.get("node").map(std::slice::from_ref),
            )];
            if let Some(replaced) = result.get("replacedFixedConnection").filter(|value| !value.is_null()) {
                sections.push(format!(
                    "Replaced fixed connection: slot {}: {}: {}",
                    replaced.get("slot").and_then(Value::as_u64).unwrap_or_default(),
                    replaced.get("identifier").and_then(Value::as_u64).unwrap_or_default(),
                    result_text(replaced.get("shortName"), "(none)")
                ));
            }
            sections.join("\n\n")
        }
        "CreateNode" => format_result_nodes(
            "Memory node created",
            result.get("node").map(std::slice::from_ref),
        ),
        "UpdateNode" => format_result_nodes(
            "Memory node updated",
            result.get("node").map(std::slice::from_ref),
        ),
        "WebSearch" => {
            let sources = result
                .get("sources")
                .and_then(Value::as_array)
                .map(|sources| {
                    sources
                        .iter()
                        .enumerate()
                        .map(|(index, source)| format!(
                            "  {}. {}\n     URL: {}",
                            index + 1,
                            result_text(source.get("title").or_else(|| source.get("url")), "(untitled)"),
                            result_text(source.get("url"), "(none)")
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .filter(|sources| !sources.is_empty())
                .unwrap_or_else(|| "  none returned".into());
            format!(
                "Web research completed.\n\nResearch answer:\n{}\n\nSources:\n{sources}",
                indent_result(result_text(result.get("answer"), "(none)").as_str())
            )
        }
        "WebFetch" => format!(
            "Web page fetched.\n\nURL: {}\nTitle: {}\nRetrieved: {}\nContent type: {}\nTruncated: {}\n\nReadable page content:\n{}",
            result_text(result.get("url"), "(none)"),
            result_text(result.get("title"), "(none)"),
            result_text(result.get("retrieved_at"), "(unknown)"),
            result_text(result.get("content_type"), "(unknown)"),
            if result.get("truncated").and_then(Value::as_bool) == Some(true) { "yes" } else { "no" },
            indent_result(result_text(result.get("content"), "(none)").as_str()),
        ),
        "CreateRustLib" | "OpenRustLib" => format!(
            "Managed Rust library {}.\n\nComplete library snapshot (every UTF-8 file; file bodies are exact JSON strings):\n{}",
            if name == "CreateRustLib" { "created" } else { "opened" },
            serde_json::to_string_pretty(result).unwrap_or_else(|_| "null".into()),
        ),
        "WriteRustLib" => format!(
            "Managed Rust library files written.\n\n{}",
            serde_json::to_string_pretty(result).unwrap_or_else(|_| "null".into()),
        ),
        "CheckRustLib" => format!(
            "Managed Rust library check completed.\n\n{}",
            serde_json::to_string_pretty(result).unwrap_or_else(|_| "null".into()),
        ),
        "PublishRustLib" => format!(
            "Managed Rust library publication completed.\n\n{}",
            serde_json::to_string_pretty(result).unwrap_or_else(|_| "null".into()),
        ),
        _ => format!("{name} completed successfully."),
    }
}

fn format_result_nodes(title: &str, nodes: Option<&[Value]>) -> String {
    let Some(nodes) = nodes.filter(|nodes| !nodes.is_empty()) else {
        return format!("{title}\n\nNone.");
    };
    format!(
        "{title}\n\n{}",
        nodes
            .iter()
            .map(|node| format_context_node(node, true))
            .collect::<Vec<_>>()
            .join("\n\n")
    )
}

fn result_text(value: Option<&Value>, fallback: &str) -> String {
    match value {
        Some(Value::String(value)) if !value.is_empty() => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        _ => fallback.to_owned(),
    }
}

fn indent_result(value: &str) -> String {
    value
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn empty_usage(runtime: &RuntimeModel) -> Value {
    json!({"requests":0,"contextWindowTokens":runtime.context_window_tokens,"maxInputTokens":runtime.max_input_tokens,"contextKnown":false,"contextTokens":0,"contextRemaining":Value::Null,"totalInputTokens":0,"totalOutputTokens":0,"totalCachedTokens":0,"totalCacheWriteTokens":0,"totalReasoningTokens":0,"cacheReadPercent":0,"last":Value::Null,"lastContext":Value::Null,"providerThreadTotals":Value::Null})
}
fn record_usage(target: &mut Value, usage: Option<&Value>) {
    target["requests"] = json!(
        target
            .get("requests")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            + 1
    );
    let Some(usage) = usage else {
        return;
    };
    let input = usage
        .get("last_input_tokens")
        .and_then(Value::as_u64)
        .or_else(|| usage.get("input_tokens").and_then(Value::as_u64))
        .unwrap_or_default();
    let output = usage
        .get("last_output_tokens")
        .and_then(Value::as_u64)
        .or_else(|| usage.get("output_tokens").and_then(Value::as_u64))
        .unwrap_or_default();
    target["contextKnown"] = json!(true);
    target["contextTokens"] = json!(input + output);
    target["lastContext"] = json!({"inputTokens":input,"outputTokens":output});
    for (source, dest) in [
        ("input_tokens", "totalInputTokens"),
        ("output_tokens", "totalOutputTokens"),
        ("cached_tokens", "totalCachedTokens"),
        ("cache_write_tokens", "totalCacheWriteTokens"),
        ("reasoning_tokens", "totalReasoningTokens"),
    ] {
        target[dest] = json!(
            target.get(dest).and_then(Value::as_u64).unwrap_or_default()
                + usage
                    .get(source)
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
        );
    }
}

fn controller_role(mode: &AgentMode) -> &'static str {
    match mode {
        AgentMode::Ingress { .. } => "History ingress controller",
        AgentMode::FreeTime => "Self time controller",
        AgentMode::Conversation => "Turn controller",
    }
}
fn controller_kind(mode: &AgentMode) -> &'static str {
    match mode {
        AgentMode::Ingress { .. } => "history-ingress-continuation",
        AgentMode::FreeTime => "free-time-continuation",
        AgentMode::Conversation => "turn-continuation",
    }
}
fn controller_message(mode: &AgentMode, no_answer: bool, free_time: &Value) -> String {
    match mode {
        AgentMode::Ingress { .. } => format!(
            "History-ingress controller: {} This ingress session is still active. Use KENNEDY_TOOL_CALLS to persist every useful update. When fully ingressed, call EndTurn with empty arguments by itself.",
            if no_answer {
                "no assistant answer was returned."
            } else {
                "a normal answer does not complete history ingress."
            }
        ),
        AgentMode::FreeTime => format!(
            "Self-time controller: this clean-slate session remains active. {} Continue useful autonomous work, or call EndTurn when this session is complete.",
            free_time_schedule(free_time)
        ),
        AgentMode::Conversation => format!(
            "Kennedy turn controller: {} Kennedy tool calls are available through KENNEDY_TOOL_CALLS. If more tool work is needed, continue. If the response is complete, call EndTurn with empty arguments by itself.",
            if no_answer {
                "no assistant answer was returned, so this turn is still active."
            } else {
                "the response above did not end this turn."
            }
        ),
    }
}

fn reset_history_message(entries: &[Value]) -> Value {
    let mut groups: Vec<(Vec<String>, u64)> = Vec::new();
    for entry in entries {
        let mut names = entry
            .get("retainedNodeNames")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(|name| name.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        names.sort();
        if let Some((_, count)) = groups.iter_mut().find(|(saved, _)| *saved == names) {
            *count += 1;
        } else {
            groups.push((names, 1));
        }
    }
    let latest = entries.last().cloned().unwrap_or_else(|| json!({}));
    let mut lines = vec![format!(
        "ResetContext history · {} successful call{} · shared context-load budget at latest reset: {}/{}",
        entries.len(),
        if entries.len() == 1 { "" } else { "s" },
        latest
            .get("budgetUsed")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        latest
            .get("budgetLimit")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    )];
    lines.extend(groups.into_iter().map(|(names, count)| {
        format!(
            "{count}× {}",
            if names.is_empty() {
                "roots only".to_owned()
            } else {
                names.join(" | ")
            }
        )
    }));
    json!({
        "role":"system",
        "display_role":"ResetContext history",
        "context_kind":"reset-history",
        "reset_history_entries":entries,
        "content":lines.join("\n"),
    })
}

struct FreeTimeTiming {
    expired: bool,
    warning_due: bool,
}
fn deadline(value: &Value) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.get("deadlineAt")?.as_str()?)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}
fn free_time_timing(value: &Value) -> FreeTimeTiming {
    let remaining = deadline(value)
        .map(|deadline| deadline - Utc::now())
        .unwrap_or(ChronoDuration::zero());
    FreeTimeTiming {
        expired: remaining <= ChronoDuration::zero(),
        warning_due: remaining <= ChronoDuration::minutes(3),
    }
}
fn free_time_schedule(value: &Value) -> String {
    let deadline = value
        .get("deadlineAt")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let duration = value
        .get("durationMinutes")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    format!("Self-time run duration: {duration} minutes. Shared deadline: {deadline}.")
}
fn free_time_opening(value: &Value) -> String {
    format!(
        "Kennedy self time has started. {} Work autonomously on anything useful. End this clean-slate session with EndTurn; the backend may begin another clean-slate session while time remains.",
        free_time_schedule(value)
    )
}

fn format_telegram_group_context(
    group: &Value,
    context: &mut KmapContext,
) -> anyhow::Result<String> {
    if !group.is_object() {
        return Ok(String::new());
    }
    let title = group
        .get("groupTitle")
        .and_then(Value::as_str)
        .unwrap_or("Telegram group");
    let chat = group
        .get("chatId")
        .map(value_string)
        .unwrap_or_else(|| "unknown".into());
    let invoking_user_id = group.get("invokingTelegramUserId").map(value_string);
    let group_root_identifier = group
        .get("groupRootNodeId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(|id| context.register_reference(id))
        .transpose()?;
    let invoking_root_identifier = group
        .get("participants")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|participant| {
            invoking_user_id.as_deref()
                == participant
                    .get("telegramUserId")
                    .map(value_string)
                    .as_deref()
        })
        .and_then(|participant| participant.get("rootNodeId"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(|id| context.register_reference(id))
        .transpose()?;
    let kennedy_root_identifier = context
        .root_node_ids
        .last()
        .cloned()
        .map(|id| context.short_id(&id));
    let root_description = match (
        group_root_identifier,
        invoking_user_id.as_deref(),
        invoking_root_identifier,
        kennedy_root_identifier,
    ) {
        (None, _, _, _) => "This archived session predates group roots; use the always-loaded roots shown in the Kmap context.".to_owned(),
        (Some(group_root), None, _, Some(kennedy_root)) => format!(
            "This is background group-chat ingress. No participant is designated as the core user. The group root ({group_root}) and Kennedy's root ({kennedy_root}) are loaded automatically, leaving room for {} additional directly loaded nodes.",
            MAX_DIRECTLY_LOADED_NODES.saturating_sub(context.root_node_ids.len())
        ),
        (Some(group_root), Some(invoker), Some(invoker_root), Some(kennedy_root)) => format!(
            "The invoking Telegram user ID is {invoker}. The invoking participant's root ({invoker_root}), the group root ({group_root}), and Kennedy's root ({kennedy_root}) are loaded automatically in that order, leaving room for {} additional directly loaded nodes.",
            MAX_DIRECTLY_LOADED_NODES.saturating_sub(context.root_node_ids.len())
        ),
        _ => "Use the always-loaded roots shown in the Kmap context for this group session.".to_owned(),
    };
    let mut participants = Vec::new();
    for participant in group
        .get("participants")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let root = participant
            .get("rootNodeId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let identifier = if root.is_empty() {
            0
        } else {
            context.register_reference(root)?
        };
        let participant_user_id = participant
            .get("telegramUserId")
            .map(value_string)
            .unwrap_or_default();
        let core = if invoking_user_id.as_deref() == Some(participant_user_id.as_str()) {
            " · user for this persistent group session"
        } else {
            ""
        };
        participants.push(format!(
            "- {} · Telegram user ID {} · root node identifier {}{}",
            telegram_participant_name(participant),
            participant_user_id,
            identifier,
            core,
        ));
    }
    let messages = group
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|message| {
            format!(
                "- message {} · {}{}: {}",
                message
                    .get("messageId")
                    .map(value_string)
                    .unwrap_or_default(),
                if message.get("sentByKennedy").and_then(Value::as_bool) == Some(true) {
                    "Kennedy".to_owned()
                } else {
                    telegram_participant_name(message)
                },
                message
                    .get("replyToMessageId")
                    .filter(|value| !value.is_null())
                    .map(|value| format!(" · replying to message {}", value_string(value)))
                    .unwrap_or_default(),
                message.get("text").and_then(Value::as_str).unwrap_or("")
            )
        })
        .collect::<Vec<_>>();
    Ok(format!(
        "Group: {title} (chat ID {chat})\n{root_description}\nParticipant root identifiers are registered in this session. The session participant's root is loaded; other participant roots are not automatically loaded:\n{}\n\nTelegram messages supplied as context ({}):\n{}",
        participants.join("\n"),
        messages.len(),
        messages.join("\n")
    ))
}

fn telegram_participant_name(participant: &Value) -> String {
    let handle = participant
        .get("username")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| format!("@{}", value.trim_start_matches('@')));
    let display = participant
        .get("displayName")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    match (display, handle) {
        (Some(display), Some(handle)) if display != handle => format!("{display} · {handle}"),
        (Some(display), _) => display,
        (None, Some(handle)) => handle,
        _ => "Unknown participant".into(),
    }
}

fn model_readable_provenance(data: &str) -> anyhow::Result<String> {
    let Ok(archive) = serde_json::from_str::<Value>(data) else {
        return Ok(data.trim().to_owned());
    };
    if let Some(chatend) = archive.get("chatendText").and_then(Value::as_str) {
        return Ok(chatend.to_owned());
    }
    let messages = archive
        .get("messages")
        .and_then(Value::as_array)
        .context("Ingress provenance does not contain readable source data.")?;
    Ok(format_chatend(messages, archive.get("usage")))
}
fn value_in(state: &Value, archive: Option<&Value>, key: &str) -> Option<Value> {
    state
        .get(key)
        .filter(|value| !value.is_null())
        .cloned()
        .or_else(|| {
            archive
                .and_then(|archive| archive.get(key))
                .filter(|value| !value.is_null())
                .cloned()
        })
}
fn value_in_ref<'a>(state: &'a Value, archive: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    state.get(key).filter(|value| !value.is_null()).or_else(|| {
        archive
            .and_then(|archive| archive.get(key))
            .filter(|value| !value.is_null())
    })
}
fn value_in_ref_opt<'a>(
    state: Option<&'a Value>,
    archive: Option<&'a Value>,
    key: &str,
) -> Option<&'a Value> {
    state
        .and_then(|value| value.get(key))
        .filter(|value| !value.is_null())
        .or_else(|| {
            archive
                .and_then(|value| value.get(key))
                .filter(|value| !value.is_null())
        })
}
fn string_in<'a>(state: &'a Value, archive: Option<&'a Value>, key: &str) -> Option<&'a str> {
    value_in_ref(state, archive, key).and_then(Value::as_str)
}
fn string_values(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}
fn value_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}
fn value_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Json, Router,
        extract::Path,
        routing::{get, post},
    };

    #[test]
    fn tool_protocol_requires_marker_at_the_start() {
        assert!(parse_tool_calls("hello KENNEDY_TOOL_CALLS\n{}").is_err());
        assert!(parse_tool_calls("ordinary answer").unwrap().is_none());
        let calls = parse_tool_calls(
            "KENNEDY_TOOL_CALLS\n{\"calls\":[{\"name\":\"ToolCheck\",\"arguments\":{}}]}",
        )
        .unwrap()
        .unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "ToolCheck");
    }

    #[test]
    fn free_time_deadline_is_derived_from_durable_metadata() {
        let old = json!({"deadlineAt":"2000-01-01T00:00:00Z"});
        assert!(free_time_timing(&old).expired);
        let future = json!({"deadlineAt":"2999-01-01T00:00:00Z"});
        assert!(!free_time_timing(&future).expired);
    }

    #[test]
    fn reset_history_is_compact_and_keeps_every_receipt() {
        let message = reset_history_message(&[
            json!({"retainedNodeNames":["Project", "User Root"],"budgetUsed":1,"budgetLimit":20}),
            json!({"retainedNodeNames":["User Root", "Project"],"budgetUsed":2,"budgetLimit":20}),
        ]);
        assert_eq!(
            message["reset_history_entries"].as_array().unwrap().len(),
            2
        );
        assert!(
            message["content"]
                .as_str()
                .unwrap()
                .contains("2× Project | User Root")
        );
        assert!(message["content"].as_str().unwrap().contains("2/20"));
    }

    #[test]
    fn model_facing_tool_results_are_readable_text_not_serialized_state() {
        let node = json!({
            "identifier": 3,
            "shortName": "Project",
            "shortDescription": "Project summary",
            "longDescription": "Project details",
            "ownerIdentifier": 1,
            "fixedConnections": [],
            "activeConnections": [{"identifier": 4, "shortName": "Related", "shortDescription": "Repeated summary"}],
            "fanoutConnections": [{"identifier": 5, "shortName": "Later", "shortDescription": "Repeated summary"}],
        });
        let load = format_tool_result(
            "LoadNode",
            &json!({
                "ok": true,
                "result": {
                    "directNodes": [node],
                    "activeConnectionNodes": [{
                        "identifier": 4, "shortName": "Related", "shortDescription": "OMIT ME", "longDescription": "Related details",
                        "ownerIdentifier": 1, "fixedConnections": [], "activeConnections": [], "fanoutConnections": []
                    }],
                    "directFanoutNodes": [{"identifier": 5, "shortName": "Later", "shortDescription": "Later summary"}],
                    "indirectFanoutNodes": [{"identifier": 6, "shortName": "Distant", "shortDescription": "OMIT ME TOO"}],
                    "loadFailures": [{"identifier": 9, "message": "Unknown memory identifier 9."}],
                }
            }),
        );
        assert!(load.contains("Memory load completed."));
        assert!(
            load.find("Directly loaded nodes").unwrap()
                < load.find("Full active-connection nodes").unwrap()
        );
        assert!(
            load.find("Full active-connection nodes").unwrap()
                < load.find("Fanout nodes of directly loaded nodes").unwrap()
        );
        assert!(
            load.find("Fanout nodes of directly loaded nodes").unwrap()
                < load
                    .find("Fanout nodes only of full active-connection nodes")
                    .unwrap()
        );
        assert!(!load.contains('{'));
        assert!(!load.contains("shortName") && !load.contains("OMIT ME"));
        assert!(load.contains("Node 9: Unknown memory identifier 9."));

        let web = format_tool_result(
            "WebSearch",
            &json!({
                "ok": true,
                "result": {"answer":"Readable answer","sources":[{"title":"Example","url":"https://example.com"}]}
            }),
        );
        assert!(web.contains("1. Example\n     URL: https://example.com"));
        assert!(!web.contains('{') && !web.contains("\"title\""));

        let mutation = format_tool_result(
            "CreateNode",
            &json!({
                "ok": true,
                "result": {"node": {
                    "identifier": 7, "shortName": "Created", "shortDescription": "Summary", "longDescription": "Details",
                    "ownerIdentifier": 1, "fixedConnections": [], "activeConnections": [], "fanoutConnections": []
                }}
            }),
        );
        assert!(mutation.contains("Node 7: Created"));
        assert!(!mutation.contains('{') && !mutation.contains("shortDescription"));
    }

    #[test]
    fn open_rust_lib_exposes_the_complete_snapshot_to_the_model() {
        let result = json!({
            "name": "complete-lib",
            "version": "0.3.0",
            "documentation": "Complete docs\n",
            "files": [
                {"path":"Cargo.toml","contents":"[package]\nname = \"complete-lib\"\nversion = \"0.3.0\"\n"},
                {"path":"Documentation.md","contents":"Complete docs\n"},
                {"path":"src/internal/mod.rs","contents":"pub fn nested() -> &'static str { \"nested\" }\n"},
                {"path":"src/lib.rs","contents":"mod internal;\npub use internal::nested;\n"},
                {"path":"tests/integration.rs","contents":"#[test]\nfn it_works() { assert_eq!(complete_lib::nested(), \"nested\"); }\n"},
            ],
        });
        let payload = json!({"ok":true,"result":result});
        let exact_snapshot = serde_json::to_string_pretty(&payload["result"]).unwrap();
        let readable = format_tool_result("OpenRustLib", &payload);
        assert!(readable.ends_with(&exact_snapshot));
        assert_eq!(readable.matches("\"contents\":").count(), 5);

        let message = tool_result_message("OpenRustLib", payload, 1);
        let chatend = format_chatend(std::slice::from_ref(&message), None);
        assert!(chatend.contains(&exact_snapshot));
    }

    #[test]
    fn provenance_uses_the_backend_owned_chatend_text_without_reformatting() {
        let readable = model_readable_provenance(
            r#"{"chatendText":"Exact backend text\n  with spacing  ","messages":[{"role":"user","content":"MUST NOT BE REFORMATTED"}]}"#,
        )
        .unwrap();
        assert_eq!(readable, "Exact backend text\n  with spacing  ");
    }

    #[tokio::test]
    async fn native_session_runs_a_complete_read_only_conversation_turn() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let generations = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/v1/kmap/nodes/{id}",
                get(|| async {
                    Json(json!({
                        "id":"root",
                        "owner_node_id":"root",
                        "short_name":"User Root",
                        "short_description":"",
                        "long_description":"",
                        "fixed_connections":[],
                        "recent_connections":[],
                        "connection_summaries":[],
                    }))
                }),
            )
            .route(
                "/api/v1/generate",
                post({
                    let generations = generations.clone();
                    move || {
                        let call = generations.fetch_add(1, Ordering::SeqCst);
                        async move {
                            let content = if call == 0 {
                                "A native Rust response."
                            } else {
                                "KENNEDY_TOOL_CALLS\n{\"calls\":[{\"name\":\"EndTurn\",\"arguments\":{}}]}"
                            };
                            Json(json!({
                                "status":"complete",
                                "message":{"content":content},
                                "usage":{"input_tokens":10,"output_tokens":2,"cached_tokens":0,"cache_write_tokens":0,"reasoning_tokens":0},
                            }))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = super::super::Config {
            system_prompts_directory: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../Frontend/SystemPrompts"),
            kweb_base: base.clone(),
            intelligence_base: base.clone(),
            conversation_history_base: base.clone(),
            telegram_relay_base: base.clone(),
            audio_ingress_base: base,
            telegram_web_user_handle: "@test".into(),
        };
        let api = Api::new(&config).unwrap();
        let manuals = Manuals::load(&config.system_prompts_directory).unwrap();
        let runtime = RuntimeModel {
            provider: "test".into(),
            provider_kind: "test".into(),
            model: "test-model".into(),
            reasoning_effort: "high".into(),
            context_window_tokens: 100_000,
            max_input_tokens: 90_000,
        };
        let mut session = Session::new(
            api,
            manuals,
            runtime,
            SessionOptions::conversation("conversation", vec!["root".into()]),
            None,
        )
        .await
        .unwrap();
        assert!(session.begin_user_turn("Hello", &json!({})));
        let checkpoints = Arc::new(AtomicUsize::new(0));
        let saved = checkpoints.clone();
        let answer = session
            .run_pending_turn(Uuid::new_v4(), move |_| {
                let saved = saved.clone();
                async move {
                    saved.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
            .unwrap();
        assert_eq!(answer.as_deref(), Some("A native Rust response."));
        assert!(!session.pending_turn);
        assert_eq!(generations.load(Ordering::SeqCst), 2);
        assert!(checkpoints.load(Ordering::SeqCst) >= 2);
        let archive = session.archive().unwrap();
        assert_eq!(
            archive.get("chatendText").and_then(Value::as_str),
            Some(
                format_chatend(
                    archive.get("messages").and_then(Value::as_array).unwrap(),
                    archive.get("usage"),
                )
                .as_str()
            )
        );
        server.abort();
    }

    #[tokio::test]
    async fn native_session_combines_loadnode_calls_before_rendering_the_batch() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let generations = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let app = Router::new()
            .route(
                "/api/v1/kmap/nodes/{id}",
                get(|Path(id): Path<String>| async move {
                    let (name, fixed, recent, summaries) = match id.as_str() {
                        "root" => (
                            "Root",
                            vec!["a", "c"],
                            Vec::new(),
                            vec![
                                json!({"id":"a","short_name":"Node A","short_description":"A summary"}),
                                json!({"id":"c","short_name":"Node C","short_description":"C summary"}),
                            ],
                        ),
                        "a" => (
                            "Node A",
                            Vec::new(),
                            vec!["c", "d"],
                            vec![
                                json!({"id":"c","short_name":"Node C","short_description":"C summary"}),
                                json!({"id":"d","short_name":"Node D","short_description":"D summary"}),
                            ],
                        ),
                        "c" => ("Node C", Vec::new(), Vec::new(), Vec::new()),
                        "d" => ("Node D", Vec::new(), Vec::new(), Vec::new()),
                        _ => ("Unknown", Vec::new(), Vec::new(), Vec::new()),
                    };
                    Json(json!({
                        "id":id,
                        "owner_node_id":"root",
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
            )
            .route(
                "/api/v1/generate",
                post({
                    let generations = generations.clone();
                    let requests = requests.clone();
                    move |Json(request): Json<Value>| {
                        let call = generations.fetch_add(1, Ordering::SeqCst);
                        requests.lock().unwrap().push(request);
                        async move {
                            let content = match call {
                                0 => "KENNEDY_TOOL_CALLS\n{\"calls\":[{\"name\":\"LoadNode\",\"arguments\":{\"identifier\":2}},{\"name\":\"LoadNode\",\"arguments\":{\"identifier\":3}},{\"name\":\"LoadNode\",\"arguments\":{\"identifier\":999}}]}",
                                1 => "The batch is loaded.",
                                _ => "KENNEDY_TOOL_CALLS\n{\"calls\":[{\"name\":\"EndTurn\",\"arguments\":{}}]}",
                            };
                            Json(json!({
                                "status":"complete",
                                "message":{"content":content},
                                "usage":{"input_tokens":100,"output_tokens":10,"cached_tokens":0,"cache_write_tokens":0,"reasoning_tokens":0},
                            }))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = super::super::Config {
            system_prompts_directory: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../Frontend/SystemPrompts"),
            kweb_base: base.clone(),
            intelligence_base: base.clone(),
            conversation_history_base: base.clone(),
            telegram_relay_base: base.clone(),
            audio_ingress_base: base,
            telegram_web_user_handle: "@test".into(),
        };
        let api = Api::new(&config).unwrap();
        let manuals = Manuals::load(&config.system_prompts_directory).unwrap();
        let runtime = RuntimeModel {
            provider: "test".into(),
            provider_kind: "test".into(),
            model: "test-model".into(),
            reasoning_effort: "high".into(),
            context_window_tokens: 100_000,
            max_input_tokens: 90_000,
        };
        let mut session = Session::new(
            api,
            manuals,
            runtime,
            SessionOptions::conversation("conversation", vec!["root".into()]),
            None,
        )
        .await
        .unwrap();
        assert!(session.begin_user_turn("Load both nodes", &json!({})));
        let answer = session
            .run_pending_turn(Uuid::new_v4(), |_| async { Ok(()) })
            .await
            .unwrap();
        assert_eq!(answer.as_deref(), Some("The batch is loaded."));

        let archive = session.archive().unwrap();
        let load_results = archive
            .get("messages")
            .and_then(Value::as_array)
            .unwrap()
            .iter()
            .filter(|message| message.get("tool_name").and_then(Value::as_str) == Some("LoadNode"))
            .collect::<Vec<_>>();
        assert_eq!(load_results.len(), 1);
        assert_eq!(
            load_results[0]
                .get("tool_call_count")
                .and_then(Value::as_u64),
            Some(3)
        );
        let rendered = load_results[0]
            .get("content")
            .and_then(Value::as_str)
            .unwrap();
        assert!(
            rendered.find("Node 2: Node A").unwrap() < rendered.find("Node 3: Node C").unwrap()
        );
        assert!(
            rendered.find("Node 3: Node C").unwrap() < rendered.find("Node 4: Node D").unwrap()
        );
        assert!(
            rendered.find("Node 4: Node D").unwrap()
                > rendered.find("Full active-connection nodes").unwrap()
        );
        assert_eq!(rendered.match_indices("Node 3: Node C").count(), 1);
        assert!(!rendered.contains("\"shortName\"") && !rendered.contains("requestedNode"));
        assert!(rendered.contains("Node 999: Unknown memory identifier 999."));

        let sent = requests.lock().unwrap();
        let second_chatend = sent[1].get("chatend").and_then(Value::as_str).unwrap();
        assert!(second_chatend.contains(rendered));
        assert_eq!(second_chatend.match_indices("Node 3: Node C").count(), 1);
        server.abort();
    }
}
