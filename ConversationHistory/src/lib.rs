use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use kennedy_chatend::hydrate_state_chatend_text;
use kennedy_memory_ingress::{
    Failure as QueueFailure, Job as QueueJob, LegacySubmission, Queue, SourceKind, Submission,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const MULTIPLE_LIVE_MIGRATION: &str =
    include_str!("../migrations/002_multiple_live_conversations.sql");
const INGRESS_FAILURES_MIGRATION: &str = include_str!("../migrations/003_ingress_failures.sql");
const INGRESS_RETRY_SCHEDULE_MIGRATION: &str =
    include_str!("../migrations/004_ingress_retry_schedule.sql");
const SELF_TIME_COMPLETION_MIGRATION: &str =
    include_str!("../migrations/005_self_time_completes_directly.sql");
const CONVERSATION_SUMMARIES_MIGRATION: &str =
    include_str!("../migrations/006_conversation_summaries.sql");
const BACKEND_COMMANDS_MIGRATION: &str = include_str!("../migrations/007_backend_commands.sql");
#[cfg(test)]
const INGRESS_FAILURE_LIMIT: i64 = 5;
#[cfg(test)]
const INGRESS_RETRY_DELAY_SECONDS: i64 = 15;
const SUMMARY_TEXT_LIMIT: usize = 512;

#[derive(Clone, Debug)]
pub struct Config {
    pub database: PathBuf,
    pub max_request_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    queue: Queue,
}

#[derive(Clone)]
pub struct Service {
    state: AppState,
    max_request_bytes: usize,
}

#[derive(Debug)]
pub struct ServiceError {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ServiceError {}

impl From<ApiError> for ServiceError {
    fn from(error: ApiError) -> Self {
        Self {
            status: error.status.as_u16(),
            code: error.code,
            message: error.message,
        }
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
    fn bad(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }
    fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "Conversation not found.",
        )
    }
    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "state_conflict", message)
    }
    fn free_time_active() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "free_time_already_active",
            "A free-time run is already active.",
        )
    }
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error=%error, "conversation history request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected conversation database error occurred.",
        )
    }

    fn queue(error: kennedy_memory_ingress::Error) -> Self {
        use kennedy_memory_ingress::ErrorKind;
        match error.kind {
            ErrorKind::Invalid => Self::bad(error.message),
            ErrorKind::NotFound => Self::not_found(),
            ErrorKind::Conflict => Self::conflict(error.message),
            ErrorKind::Internal => Self::internal(error),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error":{"code":self.code,"message":self.message}})),
        )
            .into_response()
    }
}

#[derive(Clone, Debug, Serialize)]
struct ConversationRecord {
    id: String,
    phase: String,
    started_at: String,
    updated_at: String,
    state: Value,
    provenance_id: Option<String>,
    version: i64,
    last_user_message_at: Option<String>,
    ended_at: Option<String>,
    ingress_failure_count: i64,
    ingress_failures: Value,
    ingress_next_attempt_at: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    summary: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Deserialize)]
struct CreateConversation {
    started_at: String,
    state: Value,
}

#[derive(Deserialize)]
struct StartManagedConversation {
    idempotency_id: String,
    started_at: String,
    session_type: String,
    #[serde(default)]
    duration_minutes: Option<f64>,
    #[serde(default)]
    custom_prompt: Option<String>,
}

#[derive(Deserialize)]
struct QueueConversationCommand {
    idempotency_id: String,
    kind: String,
    #[serde(default = "empty_json_object")]
    payload: Value,
}

fn empty_json_object() -> Value {
    json!({})
}

#[derive(Deserialize)]
struct CompleteConversationCommand {
    #[serde(default = "empty_json_object")]
    outcome: Value,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConversationCommand {
    id: String,
    conversation_id: String,
    sequence: i64,
    kind: String,
    payload: Value,
    status: String,
    cancel_requested: bool,
    outcome: Option<Value>,
    created_at: String,
    processing_started_at: Option<String>,
    completed_at: Option<String>,
}

#[derive(Deserialize)]
struct CheckpointConversation {
    expected_version: i64,
    state: Value,
    #[serde(default)]
    user_activity: bool,
}

#[derive(Deserialize)]
struct VersionedTransition {
    expected_version: i64,
}

#[derive(Deserialize)]
struct RetryIngress {
    expected_version: i64,
    state: Value,
}

#[derive(Deserialize)]
struct StartIngress {
    expected_version: i64,
    provenance_id: String,
    completion_protocol: Option<String>,
}

#[derive(Deserialize)]
struct RecordIngressFailure {
    expected_version: i64,
    stage: String,
    #[serde(default)]
    code: Option<String>,
    message: String,
    #[serde(default)]
    rounds_used: Option<u64>,
    #[serde(default)]
    context_tokens: Option<u64>,
    #[serde(default)]
    context_window_tokens: Option<u64>,
}

pub fn open(config: Config, queue: Queue) -> anyhow::Result<Service> {
    let connection = Connection::open(&config.database)
        .with_context(|| format!("opening {}", config.database.display()))?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;",
    )?;
    apply_migrations(&connection).context("applying conversation history migrations")?;
    import_legacy_ingress(&connection, &queue)
        .context("moving conversation history ingress into the shared queue")?;
    let state = AppState {
        db: Arc::new(Mutex::new(connection)),
        queue,
    };
    Ok(Service {
        state,
        max_request_bytes: config.max_request_bytes,
    })
}

pub fn router(service: Service) -> Router {
    Router::new()
        .route("/api/v1/conversations/health", get(health))
        .route(
            "/api/v1/conversations",
            get(list_conversations).post(create_conversation),
        )
        .route(
            "/api/v1/conversations/start",
            post(start_managed_conversation),
        )
        .route(
            "/api/v1/conversation-commands",
            get(list_conversation_command_heads),
        )
        .route(
            "/api/v1/conversation-commands/{command_id}/claim",
            post(claim_conversation_command),
        )
        .route(
            "/api/v1/conversation-commands/{command_id}/complete",
            post(complete_conversation_command),
        )
        .route(
            "/api/v1/conversations/summaries",
            get(list_conversation_summaries),
        )
        .route("/api/v1/conversations/current", get(current_conversation))
        .route("/api/v1/conversations/ingress/next", get(next_ingress))
        .route(
            "/api/v1/conversations/ingress/repairs/release",
            post(release_ingress_repairs),
        )
        .route(
            "/api/v1/conversations/unstarted",
            delete(discard_unstarted_conversations),
        )
        .route(
            "/api/v1/conversations/{conversation_id}",
            get(get_conversation).delete(purge_conversation),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/checkpoint",
            put(checkpoint_conversation),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/commands",
            post(queue_conversation_command),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/stop",
            post(request_conversation_stop),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/request-ingress",
            post(request_ingress),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/complete",
            post(complete_conversation),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/ingress-started",
            post(ingress_started),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/ingress-checkpoint",
            put(checkpoint_ingress),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/ingress-completed",
            post(ingress_completed),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/ingress-failure",
            post(ingress_failure),
        )
        .route(
            "/api/v1/conversations/{conversation_id}/retry-ingress",
            post(retry_ingress),
        )
        .layer(DefaultBodyLimit::max(service.max_request_bytes))
        .with_state(service.state)
}

impl Service {
    pub fn health(&self) -> Result<Value, ServiceError> {
        let db = self.state.db.lock().map_err(ApiError::internal)?;
        db.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
            .map_err(ApiError::internal)?;
        Ok(json!({"service":"conversation-history","status":"ok"}))
    }

    pub async fn get_json(&self, path: &str) -> Result<Value, ServiceError> {
        let state = State(self.state.clone());
        match path {
            "/api/v1/conversations/health" => self.health(),
            "/api/v1/conversations/summaries" => {
                let Json(value) = list_conversation_summaries(state).await?;
                Ok(value)
            }
            "/api/v1/conversation-commands" => {
                let Json(value) = list_conversation_command_heads(state).await?;
                Ok(value)
            }
            "/api/v1/conversations/current" => {
                let Json(value) = current_conversation(state).await?;
                Ok(value)
            }
            _ => {
                let id = path
                    .strip_prefix("/api/v1/conversations/")
                    .filter(|id| !id.contains('/'))
                    .ok_or_else(ApiError::not_found)?;
                let Json(record) = get_conversation(state, Path(id.to_owned())).await?;
                serde_json::to_value(record)
                    .map_err(ApiError::internal)
                    .map_err(Into::into)
            }
        }
    }

    pub async fn post_json(&self, path: &str, body: Value) -> Result<Value, ServiceError> {
        let state = State(self.state.clone());
        if path == "/api/v1/conversations" {
            let (_, Json(record)) = create_conversation(state, Json(parse_body(body)?)).await?;
            return json_value(record);
        }
        if path == "/api/v1/conversations/start" {
            let (_, Json(record)) =
                start_managed_conversation(state, Json(parse_body(body)?)).await?;
            return json_value(record);
        }
        if path == "/api/v1/conversations/ingress/repairs/release" {
            let Json(value) = release_ingress_repairs(state).await?;
            return Ok(value);
        }
        if let Some(command_id) = path
            .strip_prefix("/api/v1/conversation-commands/")
            .and_then(|tail| tail.strip_suffix("/claim"))
        {
            let Json(command) = claim_conversation_command(state, Path(command_id.into())).await?;
            return json_value(command);
        }
        if let Some(command_id) = path
            .strip_prefix("/api/v1/conversation-commands/")
            .and_then(|tail| tail.strip_suffix("/complete"))
        {
            let Json(command) = complete_conversation_command(
                state,
                Path(command_id.into()),
                Json(parse_body(body)?),
            )
            .await?;
            return json_value(command);
        }
        let tail = path
            .strip_prefix("/api/v1/conversations/")
            .ok_or_else(ApiError::not_found)?;
        let (id, action) = tail.split_once('/').ok_or_else(ApiError::not_found)?;
        match action {
            "commands" => {
                let (_, Json(command)) =
                    queue_conversation_command(state, Path(id.into()), Json(parse_body(body)?))
                        .await?;
                json_value(command)
            }
            "stop" => {
                let Json(value) = request_conversation_stop(state, Path(id.into())).await?;
                Ok(value)
            }
            "request-ingress" => {
                let Json(record) =
                    request_ingress(state, Path(id.into()), Json(parse_body(body)?)).await?;
                json_value(record)
            }
            "complete" => {
                let Json(record) =
                    complete_conversation(state, Path(id.into()), Json(parse_body(body)?)).await?;
                json_value(record)
            }
            "ingress-started" => {
                let Json(record) =
                    ingress_started(state, Path(id.into()), Json(parse_body(body)?)).await?;
                json_value(record)
            }
            "ingress-completed" => {
                let Json(record) =
                    ingress_completed(state, Path(id.into()), Json(parse_body(body)?)).await?;
                json_value(record)
            }
            "ingress-failure" => {
                let Json(record) =
                    ingress_failure(state, Path(id.into()), Json(parse_body(body)?)).await?;
                json_value(record)
            }
            _ => Err(ApiError::not_found().into()),
        }
    }

    pub async fn put_json(&self, path: &str, body: Value) -> Result<Value, ServiceError> {
        let tail = path
            .strip_prefix("/api/v1/conversations/")
            .ok_or_else(ApiError::not_found)?;
        let (id, action) = tail.split_once('/').ok_or_else(ApiError::not_found)?;
        let state = State(self.state.clone());
        let Json(record) = match action {
            "checkpoint" => {
                checkpoint_conversation(state, Path(id.into()), Json(parse_body(body)?)).await?
            }
            "ingress-checkpoint" => {
                checkpoint_ingress(state, Path(id.into()), Json(parse_body(body)?)).await?
            }
            _ => return Err(ApiError::not_found().into()),
        };
        json_value(record)
    }

    pub async fn delete_json(
        &self,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, ServiceError> {
        let state = State(self.state.clone());
        if path == "/api/v1/conversations/unstarted" {
            let Json(value) = discard_unstarted_conversations(state).await?;
            return Ok(value);
        }
        let id = path
            .strip_prefix("/api/v1/conversations/")
            .filter(|id| !id.contains('/'))
            .ok_or_else(ApiError::not_found)?;
        let Json(value) = purge_conversation(
            state,
            Path(id.into()),
            Json(parse_body(body.unwrap_or(Value::Null))?),
        )
        .await?;
        Ok(value)
    }
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: Value) -> Result<T, ServiceError> {
    serde_json::from_value(body)
        .map_err(|error| ApiError::bad(format!("Invalid internal request: {error}")))
        .map_err(Into::into)
}

fn json_value(value: impl Serialize) -> Result<Value, ServiceError> {
    serde_json::to_value(value)
        .map_err(ApiError::internal)
        .map_err(Into::into)
}

async fn health(State(state): State<AppState>) -> Response {
    match state.db.lock().ok().and_then(|db| {
        db.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
            .ok()
    }) {
        Some(_) => Json(json!({"service":"conversation-history","status":"ok"})).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"service":"conversation-history","status":"unavailable"})),
        )
            .into_response(),
    }
}

fn apply_migrations(connection: &Connection) -> rusqlite::Result<()> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version < 1 {
        connection.execute_batch(INITIAL_MIGRATION)?;
    }
    if version < 2 {
        connection.execute_batch(MULTIPLE_LIVE_MIGRATION)?;
    }
    if version < 3 {
        connection.execute_batch(INGRESS_FAILURES_MIGRATION)?;
    }
    if version < 4 {
        connection.execute_batch(INGRESS_RETRY_SCHEDULE_MIGRATION)?;
    }
    if version < 5 {
        connection.execute_batch(SELF_TIME_COMPLETION_MIGRATION)?;
    }
    if version < 6 {
        connection.execute_batch(CONVERSATION_SUMMARIES_MIGRATION)?;
    }
    if version < 7 {
        connection.execute_batch(BACKEND_COMMANDS_MIGRATION)?;
    }
    // An early v2 build re-ran the v1 migration on every launch. That could recreate
    // this legacy singleton index after user_version had already advanced to 2, at
    // which point the normal v2 migration no longer ran. Repair that state
    // idempotently for every database opened by current builds.
    connection.execute_batch("DROP INDEX IF EXISTS one_unfinished_conversation;")?;
    backfill_missing_summaries(connection)?;
    Ok(())
}

fn import_legacy_ingress(connection: &Connection, queue: &Queue) -> anyhow::Result<()> {
    let mut statement = connection.prepare(&format!(
        "{} WHERE phase IN ('ingress_pending','ingress_in_progress','ingress_failed') ORDER BY datetime(COALESCE(last_user_message_at,started_at)),id",
        conversation_select()
    ))?;
    let records = statement
        .query_map([], row_record)?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for record in records {
        if is_free_time_session(&record.state) && !requires_history_ingress_repair(&record.state) {
            continue;
        }
        let job = queue.import_legacy(LegacySubmission {
            source_kind: SourceKind::Conversation,
            source_id: record.id.clone(),
            source_created_at: record
                .last_user_message_at
                .clone()
                .unwrap_or_else(|| record.started_at.clone()),
            source_position: 0,
            phase: record.phase.clone(),
            provenance_id: record.provenance_id.clone(),
            state: record.state.clone(),
            version: record.version,
            failure_count: record.ingress_failure_count,
            failures: record.ingress_failures.clone(),
            next_attempt_at: record.ingress_next_attempt_at.clone(),
        })?;
        mirror_queue_job(connection, &job).map_err(|error| anyhow::anyhow!(error.message))?;
    }
    Ok(())
}

fn mirror_queue_job(db: &Connection, job: &QueueJob) -> Result<ConversationRecord, ApiError> {
    let (state_json, summary_state_json) = serialize_state(&job.state)?;
    let failures_json = serde_json::to_string(&job.failures).map_err(ApiError::internal)?;
    let changed = db
        .execute(
            "UPDATE conversations SET phase=?1,provenance_id=?2,state_json=?3,summary_state_json=?4,version=?5,ingress_failure_count=?6,ingress_failures_json=?7,ingress_next_attempt_at=?8,updated_at=?9 WHERE id=?10",
            params![job.phase,job.provenance_id,state_json,summary_state_json,job.version,job.failure_count,failures_json,job.next_attempt_at,job.updated_at,job.source_id],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::not_found());
    }
    fetch_record(db, &job.source_id)
}

fn validate_started_at(value: &str) -> Result<(), ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| ApiError::bad("started_at must be an RFC 3339 timestamp."))?;
    Ok(())
}

fn validate_version(value: i64) -> Result<(), ApiError> {
    if value < 1 {
        return Err(ApiError::bad("expected_version must be positive."));
    }
    Ok(())
}

fn row_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConversationRecord> {
    let state_json: String = row.get(4)?;
    let mut state = serde_json::from_str(&state_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;
    hydrate_state_chatend_text(&mut state);
    let failures_json: String = row.get(10)?;
    let ingress_failures = serde_json::from_str(&failures_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(10, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(ConversationRecord {
        id: row.get(0)?,
        phase: row.get(1)?,
        started_at: row.get(2)?,
        updated_at: row.get(3)?,
        state,
        provenance_id: row.get(5)?,
        version: row.get(6)?,
        last_user_message_at: row.get(7)?,
        ended_at: row.get(8)?,
        ingress_failure_count: row.get(9)?,
        ingress_failures,
        ingress_next_attempt_at: row.get(11)?,
        summary: false,
    })
}

fn row_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConversationRecord> {
    let mut record = row_record(row)?;
    record.summary = true;
    Ok(record)
}

fn bounded_summary_value(value: &Value) -> Option<Value> {
    match value {
        Value::String(text) => Some(Value::String(
            text.chars().take(SUMMARY_TEXT_LIMIT).collect(),
        )),
        Value::Bool(_) | Value::Number(_) => Some(value.clone()),
        _ => None,
    }
}

fn copy_summary_fields(
    source: Option<&Value>,
    target: &mut serde_json::Map<String, Value>,
    fields: &[&str],
) {
    let Some(source) = source.and_then(Value::as_object) else {
        return;
    };
    for field in fields {
        if let Some(value) = source.get(*field).and_then(bounded_summary_value) {
            target.insert((*field).to_owned(), value);
        }
    }
}

fn first_user_message(state: &Value) -> Option<String> {
    [
        state.get("transcript"),
        state.pointer("/archive/transcript"),
        state.pointer("/archive/retained"),
        state.pointer("/archive/messages"),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_array)
    .flat_map(|messages| messages.iter())
    .find_map(|message| {
        let role = message.get("role")?.as_str()?;
        if !matches!(role, "user" | "david") {
            return None;
        }
        let content = message
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Some(content.chars().take(SUMMARY_TEXT_LIMIT).collect())
    })
}

fn summarize_state(state: &Value) -> Value {
    let session_type = state
        .get("sessionType")
        .or_else(|| state.pointer("/archive/sessionType"))
        .and_then(Value::as_str)
        .unwrap_or("conversation")
        .chars()
        .take(64)
        .collect::<String>();
    let mut summary = serde_json::Map::new();
    summary.insert("historySummary".into(), Value::Bool(true));
    summary.insert("sessionType".into(), Value::String(session_type));
    if let Some(pending_turn) = state
        .get("pendingTurn")
        .or_else(|| state.pointer("/archive/pendingTurn"))
        .and_then(bounded_summary_value)
    {
        summary.insert("pendingTurn".into(), pending_turn);
    }
    summary.insert(
        "transcript".into(),
        Value::Array(
            first_user_message(state)
                .map(|content| vec![json!({"role":"user","content":content})])
                .unwrap_or_default(),
        ),
    );

    let free_time = state
        .get("freeTime")
        .or_else(|| state.pointer("/archive/freeTime"))
        .or_else(|| state.get("selfTimeIntent"));
    if free_time.is_some() {
        let mut compact = serde_json::Map::new();
        copy_summary_fields(
            free_time,
            &mut compact,
            &[
                "runId",
                "sliceIndex",
                "customPrompt",
                "durationMinutes",
                "deadlineAt",
                "sliceEndedAt",
                "sliceEndedReason",
            ],
        );
        if !compact.contains_key("sliceIndex") {
            compact.insert("sliceIndex".into(), Value::Number(1.into()));
        }
        summary.insert("freeTime".into(), Value::Object(compact));
    }

    let orchestration = state.get("orchestration");
    if orchestration.is_some() {
        let mut compact = serde_json::Map::new();
        copy_summary_fields(
            orchestration,
            &mut compact,
            &["owner", "status", "lastError"],
        );
        summary.insert("orchestration".into(), Value::Object(compact));
    }

    let channel = state
        .get("channel")
        .or_else(|| state.pointer("/archive/channel"));
    if channel.is_some() {
        let mut compact = serde_json::Map::new();
        copy_summary_fields(
            channel,
            &mut compact,
            &[
                "kind",
                "telegramUserId",
                "chatId",
                "groupId",
                "username",
                "displayName",
                "groupRootNodeId",
                "groupIngressBatchId",
                "backgroundIngress",
                "lastGroupContextMessageId",
            ],
        );
        let group_context = channel.and_then(|value| value.get("groupContext"));
        if group_context.is_some() {
            let mut compact_group = serde_json::Map::new();
            copy_summary_fields(
                group_context,
                &mut compact_group,
                &["groupTitle", "groupRootNodeId", "chatId"],
            );
            compact.insert("groupContext".into(), Value::Object(compact_group));
        }
        summary.insert("channel".into(), Value::Object(compact));
    }
    Value::Object(summary)
}

fn serialize_state(state: &Value) -> Result<(String, String), ApiError> {
    Ok((
        serde_json::to_string(state).map_err(ApiError::internal)?,
        serde_json::to_string(&summarize_state(state)).map_err(ApiError::internal)?,
    ))
}

fn backfill_missing_summaries(db: &Connection) -> rusqlite::Result<()> {
    let ids = {
        let mut statement = db
            .prepare("SELECT id FROM conversations WHERE summary_state_json IS NULL ORDER BY id")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for id in ids {
        let state_json = db.query_row(
            "SELECT state_json FROM conversations WHERE id=?1",
            [&id],
            |row| row.get::<_, String>(0),
        )?;
        let state = serde_json::from_str::<Value>(&state_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        let summary_json = serde_json::to_string(&summarize_state(&state))
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        db.execute(
            "UPDATE conversations SET summary_state_json=?1 WHERE id=?2 AND summary_state_json IS NULL",
            params![summary_json, id],
        )?;
    }
    Ok(())
}

fn conversation_select() -> &'static str {
    "SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at,ingress_failure_count,ingress_failures_json,ingress_next_attempt_at FROM conversations"
}

fn conversation_summary_select() -> &'static str {
    "SELECT id,phase,started_at,updated_at,summary_state_json,provenance_id,version,last_user_message_at,ended_at,ingress_failure_count,ingress_failures_json,ingress_next_attempt_at FROM conversations"
}

fn fetch_record(db: &Connection, id: &str) -> Result<ConversationRecord, ApiError> {
    db.query_row(
        &format!("{} WHERE id=?1", conversation_select()),
        [id],
        row_record,
    )
    .optional()
    .map_err(ApiError::internal)?
    .ok_or_else(ApiError::not_found)
}

fn validate_idempotency_id(value: &str) -> Result<(), ApiError> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::bad(
            "idempotency_id must be exactly 32 hexadecimal characters.",
        ));
    }
    Ok(())
}

fn row_command(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConversationCommand> {
    let payload_json: String = row.get(4)?;
    let payload = serde_json::from_str(&payload_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let outcome_json: Option<String> = row.get(7)?;
    let outcome = outcome_json
        .map(|encoded| {
            serde_json::from_str(&encoded).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .transpose()?;
    Ok(ConversationCommand {
        id: row.get(0)?,
        conversation_id: row.get(1)?,
        sequence: row.get(2)?,
        kind: row.get(3)?,
        payload,
        status: row.get(5)?,
        cancel_requested: row.get::<_, i64>(6)? != 0,
        outcome,
        created_at: row.get(8)?,
        processing_started_at: row.get(9)?,
        completed_at: row.get(10)?,
    })
}

fn command_select() -> &'static str {
    "SELECT id,conversation_id,sequence,kind,payload_json,status,cancel_requested,outcome_json,created_at,processing_started_at,completed_at FROM conversation_commands"
}

fn fetch_command(db: &Connection, id: &str) -> Result<ConversationCommand, ApiError> {
    db.query_row(
        &format!("{} WHERE id=?1", command_select()),
        [id],
        row_command,
    )
    .optional()
    .map_err(ApiError::internal)?
    .ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "command_not_found",
            "Conversation command not found.",
        )
    })
}

async fn create_conversation(
    State(state): State<AppState>,
    Json(input): Json<CreateConversation>,
) -> Result<(StatusCode, Json<ConversationRecord>), ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok((StatusCode::CREATED, Json(insert_conversation(&db, input)?)))
}

fn insert_conversation(
    db: &Connection,
    input: CreateConversation,
) -> Result<ConversationRecord, ApiError> {
    validate_started_at(&input.started_at)?;
    if is_free_time_session(&input.state) && active_free_time(db)?.is_some() {
        return Err(ApiError::free_time_active());
    }
    let (state_json, summary_state_json) = serialize_state(&input.state)?;
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().to_rfc3339();
    db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,summary_state_json,version) VALUES(?1,'active',?2,?3,?4,?5,1)", params![id,input.started_at,now,state_json,summary_state_json]).map_err(ApiError::internal)?;
    fetch_record(db, &id)
}

async fn start_managed_conversation(
    State(state): State<AppState>,
    Json(input): Json<StartManagedConversation>,
) -> Result<(StatusCode, Json<ConversationRecord>), ApiError> {
    validate_idempotency_id(&input.idempotency_id)?;
    validate_started_at(&input.started_at)?;
    if !matches!(input.session_type.as_str(), "conversation" | "free-time") {
        return Err(ApiError::bad(
            "session_type must be conversation or free-time.",
        ));
    }
    let custom_prompt = input.custom_prompt.unwrap_or_default().trim().to_owned();
    if custom_prompt.chars().count() > 20_000 {
        return Err(ApiError::bad(
            "custom_prompt must be at most 20000 characters.",
        ));
    }
    let duration_minutes = if input.session_type == "free-time" {
        let duration = input
            .duration_minutes
            .ok_or_else(|| ApiError::bad("duration_minutes is required for free-time."))?;
        if !duration.is_finite() || !(0.1..=10_080.0).contains(&duration) {
            return Err(ApiError::bad(
                "duration_minutes must be between 0.1 and 10080.",
            ));
        }
        Some(duration)
    } else {
        if input.duration_minutes.is_some() || !custom_prompt.is_empty() {
            return Err(ApiError::bad(
                "duration_minutes and custom_prompt are available only for free-time.",
            ));
        }
        None
    };

    let mut db = state.db.lock().map_err(ApiError::internal)?;
    if let Some(existing) = db
        .query_row(
            &format!("{} WHERE start_request_id=?1", conversation_select()),
            [&input.idempotency_id],
            row_record,
        )
        .optional()
        .map_err(ApiError::internal)?
    {
        let same_request = session_type_for_command(&existing.state) == input.session_type
            && if input.session_type == "free-time" {
                existing
                    .state
                    .pointer("/selfTimeIntent/durationMinutes")
                    .and_then(Value::as_f64)
                    == duration_minutes
                    && existing
                        .state
                        .pointer("/selfTimeIntent/customPrompt")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        == custom_prompt
            } else {
                true
            };
        if !same_request {
            return Err(ApiError::conflict(
                "This start idempotency ID was already used for a different session request.",
            ));
        }
        return Ok((StatusCode::OK, Json(existing)));
    }
    if input.session_type == "free-time" && active_free_time(&db)?.is_some() {
        return Err(ApiError::free_time_active());
    }
    let intent = if let Some(duration_minutes) = duration_minutes {
        json!({
            "stateVersion": 2,
            "sessionType": "free-time",
            "selfTimeIntent": {
                "durationMinutes": duration_minutes,
                "customPrompt": custom_prompt,
                "requestedAt": input.started_at,
                "provenanceIdempotencyId": input.idempotency_id,
            },
            "orchestration": {"owner":"backend","status":"queued"},
            "transcript": [],
        })
    } else {
        json!({
            "stateVersion": 2,
            "sessionType": "conversation",
            "orchestration": {"owner":"backend","status":"queued"},
            "transcript": [],
        })
    };
    let (state_json, summary_state_json) = serialize_state(&intent)?;
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().to_rfc3339();
    let tx = db.transaction().map_err(ApiError::internal)?;
    tx.execute(
        "INSERT INTO conversations(id,phase,started_at,updated_at,state_json,summary_state_json,version,start_request_id)
         VALUES(?1,'active',?2,?3,?4,?5,1,?6)",
        params![id, input.started_at, now, state_json, summary_state_json, input.idempotency_id],
    )
    .map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
            if input.session_type == "free-time" {
                ApiError::free_time_active()
            } else {
                ApiError::conflict("This conversation start request already exists.")
            }
        } else {
            ApiError::internal(error)
        }
    })?;
    tx.commit().map_err(ApiError::internal)?;
    Ok((StatusCode::CREATED, Json(fetch_record(&db, &id)?)))
}

async fn queue_conversation_command(
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
    Json(input): Json<QueueConversationCommand>,
) -> Result<(StatusCode, Json<ConversationCommand>), ApiError> {
    validate_idempotency_id(&input.idempotency_id)?;
    if !matches!(
        input.kind.as_str(),
        "message" | "retry" | "end" | "send-and-end"
    ) {
        return Err(ApiError::bad(
            "kind must be message, retry, end, or send-and-end.",
        ));
    }
    if !input.payload.is_object() {
        return Err(ApiError::bad("payload must be a JSON object."));
    }
    if matches!(input.kind.as_str(), "message" | "send-and-end") {
        let text_present = input
            .payload
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty());
        let attachments_present = input
            .payload
            .pointer("/metadata/attachments")
            .and_then(Value::as_array)
            .is_some_and(|attachments| !attachments.is_empty());
        if !text_present && !attachments_present {
            return Err(ApiError::bad(
                "A message command must contain text or at least one attachment.",
            ));
        }
    } else if input
        .payload
        .as_object()
        .is_some_and(|payload| !payload.is_empty())
    {
        return Err(ApiError::bad(
            "retry and end commands do not accept a payload.",
        ));
    }

    let mut db = state.db.lock().map_err(ApiError::internal)?;
    match fetch_command(&db, &input.idempotency_id) {
        Ok(existing) => {
            if existing.conversation_id != conversation_id
                || existing.kind != input.kind
                || existing.payload != input.payload
            {
                return Err(ApiError::conflict(
                    "This command idempotency ID was already used for a different command.",
                ));
            }
            return Ok((StatusCode::OK, Json(existing)));
        }
        Err(error) if error.code == "command_not_found" => {}
        Err(error) => return Err(error),
    }
    let conversation = fetch_record(&db, &conversation_id)?;
    if conversation.phase != "active" || !is_browser_conversation(&conversation.state) {
        return Err(ApiError::conflict(
            "Commands can be queued only for an active browser conversation.",
        ));
    }
    let payload_json = serde_json::to_string(&input.payload).map_err(ApiError::internal)?;
    let now = Utc::now().to_rfc3339();
    let tx = db.transaction().map_err(ApiError::internal)?;
    let sequence = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM conversation_commands WHERE conversation_id=?1",
            [&conversation_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(ApiError::internal)?;
    if input.kind == "end" {
        tx.execute(
            "UPDATE conversation_commands SET cancel_requested=1
             WHERE conversation_id=?1
               AND status IN ('pending','processing')
               AND kind IN ('message','retry')",
            [&conversation_id],
        )
        .map_err(ApiError::internal)?;
    }
    tx.execute(
        "INSERT INTO conversation_commands(id,conversation_id,sequence,kind,payload_json,created_at)
         VALUES(?1,?2,?3,?4,?5,?6)",
        params![input.idempotency_id, conversation_id, sequence, input.kind, payload_json, now],
    )
    .map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    Ok((
        StatusCode::CREATED,
        Json(fetch_command(&db, &input.idempotency_id)?),
    ))
}

fn session_type_for_command(state: &Value) -> &str {
    state
        .get("sessionType")
        .or_else(|| state.pointer("/archive/sessionType"))
        .and_then(Value::as_str)
        .unwrap_or("conversation")
}

fn is_browser_conversation(state: &Value) -> bool {
    session_type_for_command(state) == "conversation"
}

async fn list_conversation_command_heads(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let mut statement = db
        .prepare(&format!(
            "{} c
             WHERE c.status IN ('pending','processing')
               AND NOT EXISTS (
                 SELECT 1 FROM conversation_commands earlier
                 WHERE earlier.conversation_id=c.conversation_id
                   AND earlier.status<>'complete'
                   AND earlier.sequence<c.sequence
               )
             ORDER BY datetime(c.created_at),c.conversation_id,c.sequence",
            command_select()
        ))
        .map_err(ApiError::internal)?;
    let commands = statement
        .query_map([], row_command)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"commands":commands})))
}

async fn claim_conversation_command(
    State(state): State<AppState>,
    Path(command_id): Path<String>,
) -> Result<Json<ConversationCommand>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_command(&db, &command_id)?;
    if existing.status == "processing" {
        return Ok(Json(existing));
    }
    if existing.status != "pending" {
        return Err(ApiError::conflict(
            "This conversation command is already complete.",
        ));
    }
    let changed = db
        .execute(
            "UPDATE conversation_commands
             SET status='processing',processing_started_at=?1
             WHERE id=?2 AND status='pending'
               AND NOT EXISTS (
                 SELECT 1 FROM conversation_commands earlier
                 WHERE earlier.conversation_id=conversation_commands.conversation_id
                   AND earlier.status<>'complete'
                   AND earlier.sequence<conversation_commands.sequence
               )",
            params![Utc::now().to_rfc3339(), command_id],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "An earlier command must complete before this command can start.",
        ));
    }
    Ok(Json(fetch_command(&db, &command_id)?))
}

async fn complete_conversation_command(
    State(state): State<AppState>,
    Path(command_id): Path<String>,
    Json(input): Json<CompleteConversationCommand>,
) -> Result<Json<ConversationCommand>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_command(&db, &command_id)?;
    if existing.status == "complete" {
        return Ok(Json(existing));
    }
    if existing.status != "processing" {
        return Err(ApiError::conflict(
            "This conversation command has not been claimed.",
        ));
    }
    let outcome_json = serde_json::to_string(&input.outcome).map_err(ApiError::internal)?;
    let changed = db
        .execute(
            "UPDATE conversation_commands
             SET status='complete',outcome_json=?1,completed_at=?2
             WHERE id=?3 AND status='processing'",
            params![outcome_json, Utc::now().to_rfc3339(), command_id],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "This conversation command changed before completion.",
        ));
    }
    Ok(Json(fetch_command(&db, &command_id)?))
}

async fn request_conversation_stop(
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let conversation = fetch_record(&db, &conversation_id)?;
    if conversation.phase != "active" || !is_browser_conversation(&conversation.state) {
        return Err(ApiError::conflict(
            "Only an active browser conversation can be stopped.",
        ));
    }
    let changed = db
        .execute(
            "UPDATE conversation_commands SET cancel_requested=1
             WHERE id=(
               SELECT id FROM conversation_commands
               WHERE conversation_id=?1 AND status='processing' AND kind IN ('message','retry')
               ORDER BY sequence LIMIT 1
             )",
            [&conversation_id],
        )
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"stop_requested":changed > 0})))
}

async fn list_conversations(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let mut statement = db
        .prepare(&format!(
            "{} ORDER BY updated_at DESC",
            conversation_select()
        ))
        .map_err(ApiError::internal)?;
    let records = statement
        .query_map([], row_record)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"conversations":records})))
}

async fn list_conversation_summaries(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    backfill_missing_summaries(&db).map_err(ApiError::internal)?;
    let mut statement = db
        .prepare(&format!(
            "{} ORDER BY updated_at DESC",
            conversation_summary_select()
        ))
        .map_err(ApiError::internal)?;
    let records = statement
        .query_map([], row_summary)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"conversations":records})))
}

fn contains_user_message(messages: Option<&Value>) -> bool {
    messages.and_then(Value::as_array).is_some_and(|messages| {
        messages.iter().any(|message| {
            message
                .get("role")
                .and_then(Value::as_str)
                .is_some_and(|role| matches!(role, "user" | "david"))
        })
    })
}

fn state_contains_user_message(state: &Value) -> bool {
    contains_user_message(state.get("transcript"))
        || contains_user_message(state.pointer("/archive/transcript"))
        || contains_user_message(state.pointer("/archive/retained"))
        || contains_user_message(state.pointer("/archive/messages"))
}

fn is_telegram_session(state: &Value) -> bool {
    let telegram = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .is_some_and(|session_type| session_type.starts_with("telegram"))
    };
    telegram(state.get("sessionType")) || telegram(state.pointer("/archive/sessionType"))
}

fn is_free_time_session(state: &Value) -> bool {
    let free_time = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .is_some_and(|session_type| session_type == "free-time")
    };
    free_time(state.get("sessionType")) || free_time(state.pointer("/archive/sessionType"))
}

fn is_backend_owned_session(state: &Value) -> bool {
    state
        .get("orchestration")
        .or_else(|| state.pointer("/archive/orchestration"))
        .and_then(|value| value.get("owner"))
        .and_then(Value::as_str)
        == Some("backend")
}

fn requires_history_ingress_repair(state: &Value) -> bool {
    state
        .get("historyIngressRepairRequired")
        .and_then(Value::as_bool)
        == Some(true)
}

fn tool_log_has_success(state: &Value, pointer: &str, name: &str) -> bool {
    state
        .pointer(pointer)
        .and_then(Value::as_array)
        .is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry.get("name").and_then(Value::as_str) == Some(name)
                    && entry.get("ok").and_then(Value::as_bool) == Some(true)
            })
        })
}

#[cfg(test)]
fn history_ingress_was_explicitly_ended(state: &Value) -> bool {
    tool_log_has_success(state, "/historyIngress/tools/log", "EndTurn")
}

fn is_idle_protected_session(state: &Value) -> bool {
    is_telegram_session(state) || is_free_time_session(state)
}

fn active_free_time(db: &Connection) -> Result<Option<ConversationRecord>, ApiError> {
    backfill_missing_summaries(db).map_err(ApiError::internal)?;
    let mut statement = db
        .prepare(&format!(
            "{} WHERE phase='active' ORDER BY updated_at DESC",
            conversation_summary_select()
        ))
        .map_err(ApiError::internal)?;
    let records = statement
        .query_map([], row_summary)
        .map_err(ApiError::internal)?;
    for record in records {
        let record = record.map_err(ApiError::internal)?;
        if is_free_time_session(&record.state) {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

fn discard_unstarted(db: &mut Connection) -> Result<Vec<String>, ApiError> {
    backfill_missing_summaries(db).map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let mut statement = tx
        .prepare(
            "SELECT id,summary_state_json FROM conversations WHERE last_user_message_at IS NULL ORDER BY id",
        )
        .map_err(ApiError::internal)?;
    let candidates = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(statement);

    let mut discarded = Vec::new();
    for (id, summary_state_json) in candidates {
        let state =
            serde_json::from_str::<Value>(&summary_state_json).map_err(ApiError::internal)?;
        if state_contains_user_message(&state)
            || is_idle_protected_session(&state)
            || is_backend_owned_session(&state)
        {
            continue;
        }
        let changed = tx
            .execute(
                "DELETE FROM conversations WHERE id=?1 AND last_user_message_at IS NULL",
                [&id],
            )
            .map_err(ApiError::internal)?;
        if changed == 1 {
            discarded.push(id);
        }
    }
    tx.commit().map_err(ApiError::internal)?;
    Ok(discarded)
}

async fn discard_unstarted_conversations(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let discarded_ids = discard_unstarted(&mut db)?;
    Ok(Json(json!({
        "discarded": discarded_ids.len(),
        "discarded_ids": discarded_ids,
    })))
}

async fn current_conversation(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let record = db
        .query_row(
            &format!(
                "{} WHERE phase='active' ORDER BY updated_at DESC LIMIT 1",
                conversation_select()
            ),
            [],
            row_record,
        )
        .optional()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"conversation":record})))
}

async fn next_ingress(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let record = state
        .queue
        .next_for(SourceKind::Conversation)
        .map_err(ApiError::queue)?
        .map(|job| mirror_queue_job(&db, &job))
        .transpose()?;
    Ok(Json(json!({"conversation":record})))
}

async fn release_ingress_repairs(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let released = state
        .queue
        .release_repairs_for(SourceKind::Conversation)
        .map_err(ApiError::queue)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let ids = {
        let mut statement = db
            .prepare(
                "SELECT id FROM conversations WHERE phase IN ('ingress_pending','ingress_failed')",
            )
            .map_err(ApiError::internal)?;
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(ApiError::internal)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(ApiError::internal)?
    };
    for id in ids {
        if let Some(job) = state
            .queue
            .get(SourceKind::Conversation, &id)
            .map_err(ApiError::queue)?
        {
            mirror_queue_job(&db, &job)?;
        }
    }
    Ok(Json(json!({"released":released})))
}

#[cfg(test)]
fn fetch_next_ingress(db: &Connection) -> Result<Option<ConversationRecord>, ApiError> {
    let mut statement = db
        .prepare(&format!("{} WHERE phase IN ('ingress_in_progress','ingress_pending') AND (ingress_next_attempt_at IS NULL OR datetime(ingress_next_attempt_at)<=datetime('now')) ORDER BY CASE phase WHEN 'ingress_in_progress' THEN 0 ELSE 1 END, datetime(COALESCE(last_user_message_at,started_at)), datetime(started_at), id", conversation_select()))
        .map_err(ApiError::internal)?;
    let records = statement
        .query_map([], row_record)
        .map_err(ApiError::internal)?;
    for record in records {
        let record = record.map_err(ApiError::internal)?;
        if !is_free_time_session(&record.state) || requires_history_ingress_repair(&record.state) {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

async fn get_conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(fetch_record(&db, &id)?))
}

fn purge_record(db: &Connection, id: &str, expected_version: i64) -> Result<(), ApiError> {
    validate_version(expected_version)?;
    let changed = db
        .execute(
            "DELETE FROM conversations WHERE id=?1 AND version=?2",
            params![id, expected_version],
        )
        .map_err(ApiError::internal)?;
    if changed == 1 {
        return Ok(());
    }
    let exists = db
        .query_row("SELECT 1 FROM conversations WHERE id=?1", [id], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .map_err(ApiError::internal)?
        .is_some();
    if exists {
        Err(ApiError::conflict(
            "Conversation changed in another session before it could be purged.",
        ))
    } else {
        Err(ApiError::not_found())
    }
}

async fn purge_conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<VersionedTransition>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    purge_record(&db, &id, input.expected_version)?;
    state
        .queue
        .remove(SourceKind::Conversation, &id)
        .map_err(ApiError::queue)?;
    tracing::info!(conversation_id = %id, "Conversation permanently purged");
    Ok(Json(json!({"purged":true,"conversation_id":id})))
}

fn update_active(
    db: &Connection,
    id: &str,
    expected_version: i64,
    state: &Value,
    phase: &str,
) -> Result<ConversationRecord, ApiError> {
    validate_version(expected_version)?;
    let (state_json, summary_state_json) = serialize_state(state)?;
    let now = Utc::now().to_rfc3339();
    let ended_at = (phase != "active").then_some(now.as_str());
    let changed = db.execute("UPDATE conversations SET state_json=?1,summary_state_json=?2,phase=?3,updated_at=?4,ended_at=COALESCE(?5,ended_at),version=version+1 WHERE id=?6 AND phase='active' AND version=?7", params![state_json,summary_state_json,phase,now,ended_at,id,expected_version]).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation changed in another session or is no longer active.",
        ));
    }
    fetch_record(db, id)
}

async fn checkpoint_conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<CheckpointConversation>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    if input.user_activity {
        Ok(Json(checkpoint_user_activity(
            &mut db,
            &state.queue,
            &id,
            input.expected_version,
            &input.state,
        )?))
    } else {
        Ok(Json(update_active(
            &db,
            &id,
            input.expected_version,
            &input.state,
            "active",
        )?))
    }
}

fn checkpoint_user_activity(
    db: &mut Connection,
    queue: &Queue,
    id: &str,
    expected_version: i64,
    state: &Value,
) -> Result<ConversationRecord, ApiError> {
    validate_version(expected_version)?;
    let (state_json, summary_state_json) = serialize_state(state)?;
    let now = Utc::now();
    let now_text = now.to_rfc3339();
    let cutoff = (now - ChronoDuration::hours(24)).to_rfc3339();
    backfill_missing_summaries(db).map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx.execute(
        "UPDATE conversations SET state_json=?1,summary_state_json=?2,updated_at=?3,last_user_message_at=?3,version=version+1 WHERE id=?4 AND phase='active' AND version=?5",
        params![state_json,summary_state_json,now_text,id,expected_version],
    ).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation changed in another session or is no longer active.",
        ));
    }

    let mut statement = tx.prepare(
        "SELECT id,summary_state_json FROM conversations WHERE phase='active' AND id<>?1 AND datetime(COALESCE(last_user_message_at,started_at))<datetime(?2)",
    ).map_err(ApiError::internal)?;
    let candidates = statement
        .query_map(params![id, cutoff], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(statement);
    let mut queued = Vec::new();
    for (stale_id, stale_summary) in candidates {
        let state = serde_json::from_str::<Value>(&stale_summary).unwrap_or(Value::Null);
        let pending_turn = state
            .get("pendingTurn")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !pending_turn && !is_idle_protected_session(&state) {
            let changed = tx.execute(
                "UPDATE conversations SET phase='ingress_pending',updated_at=?1,ended_at=?1,version=version+1 WHERE id=?2 AND phase='active'",
                params![now_text,stale_id],
            ).map_err(ApiError::internal)?;
            if changed == 1 {
                queued.push(stale_id);
            }
        }
    }
    tx.commit().map_err(ApiError::internal)?;
    for stale_id in queued {
        let record = fetch_record(db, &stale_id)?;
        let job = queue
            .submit(Submission {
                source_kind: SourceKind::Conversation,
                source_id: record.id.clone(),
                source_created_at: record
                    .last_user_message_at
                    .clone()
                    .unwrap_or_else(|| record.started_at.clone()),
                source_position: 0,
                state: record.state.clone(),
                version: record.version,
            })
            .map_err(ApiError::queue)?;
        mirror_queue_job(db, &job)?;
    }
    fetch_record(db, id)
}

async fn request_ingress(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<CheckpointConversation>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    if is_free_time_session(&input.state) {
        return Ok(Json(complete_self_time(
            &db,
            &id,
            input.expected_version,
            &input.state,
        )?));
    }
    let record = update_active(
        &db,
        &id,
        input.expected_version,
        &input.state,
        "ingress_pending",
    )?;
    let job = state
        .queue
        .submit(Submission {
            source_kind: SourceKind::Conversation,
            source_id: record.id.clone(),
            source_created_at: record
                .last_user_message_at
                .clone()
                .unwrap_or_else(|| record.started_at.clone()),
            source_position: 0,
            state: record.state.clone(),
            version: record.version,
        })
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

fn complete_self_time(
    db: &Connection,
    id: &str,
    expected_version: i64,
    state: &Value,
) -> Result<ConversationRecord, ApiError> {
    let existing = fetch_record(db, id)?;
    if !is_free_time_session(&existing.state) || !is_free_time_session(state) {
        return Err(ApiError::bad(
            "Only self-time records can complete without history ingress.",
        ));
    }
    let ended_reason = state
        .pointer("/freeTime/sliceEndedReason")
        .and_then(Value::as_str);
    if !matches!(ended_reason, Some("tool" | "deadline" | "hard-stop")) {
        return Err(ApiError::bad(
            "Self time can complete only after EndTurn or the shared deadline.",
        ));
    }
    if ended_reason == Some("tool") && !tool_log_has_success(state, "/archive/tools/log", "EndTurn")
    {
        return Err(ApiError::bad(
            "Self time cannot complete from a tool ending without a successful EndTurn receipt.",
        ));
    }
    update_active(db, id, expected_version, state, "complete")
}

async fn complete_conversation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<CheckpointConversation>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(complete_self_time(
        &db,
        &id,
        input.expected_version,
        &input.state,
    )?))
}

async fn ingress_started(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<StartIngress>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_record(&db, &id)?;
    if is_free_time_session(&existing.state) && !requires_history_ingress_repair(&existing.state) {
        return Err(ApiError::conflict(
            "Self-time records complete directly and do not undergo history ingress.",
        ));
    }
    let job = state
        .queue
        .start(
            SourceKind::Conversation,
            &id,
            input.expected_version,
            &input.provenance_id,
            input.completion_protocol.as_deref(),
        )
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

async fn checkpoint_ingress(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<CheckpointConversation>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let job = state
        .queue
        .checkpoint(
            SourceKind::Conversation,
            &id,
            input.expected_version,
            &input.state,
        )
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

#[cfg(test)]
fn update_ingress(
    db: &Connection,
    id: &str,
    expected_version: i64,
    state: &Value,
) -> Result<ConversationRecord, ApiError> {
    validate_version(expected_version)?;
    let (state_json, summary_state_json) = serialize_state(state)?;
    let changed = db.execute(
        "UPDATE conversations SET state_json=?1,summary_state_json=?2,updated_at=?3,version=version+1 WHERE id=?4 AND phase='ingress_in_progress' AND version=?5",
        params![state_json,summary_state_json,Utc::now().to_rfc3339(),id,expected_version],
    ).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation ingress changed in another session or is no longer in progress.",
        ));
    }
    fetch_record(db, id)
}

#[cfg(test)]
fn concise_failure_text(value: &str, limit: usize, fallback: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let bounded = normalized.chars().take(limit).collect::<String>();
    if bounded.is_empty() {
        fallback.to_owned()
    } else {
        bounded
    }
}

#[cfg(test)]
fn record_ingress_failure(
    db: &mut Connection,
    id: &str,
    input: &RecordIngressFailure,
) -> Result<ConversationRecord, ApiError> {
    validate_version(input.expected_version)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let existing = fetch_record(&tx, id)?;
    if !matches!(
        existing.phase.as_str(),
        "ingress_pending" | "ingress_in_progress"
    ) || existing.version != input.expected_version
    {
        return Err(ApiError::conflict(
            "Conversation is no longer in the expected history-ingress attempt.",
        ));
    }

    let attempt = existing.ingress_failure_count + 1;
    let terminal =
        input.code.as_deref() == Some("input_too_large") || attempt >= INGRESS_FAILURE_LIMIT;
    let stage = concise_failure_text(&input.stage, 80, "unknown");
    let code = input
        .code
        .as_deref()
        .map(|value| concise_failure_text(value, 80, "unknown_error"));
    let message = concise_failure_text(
        &input.message,
        2_000,
        "History ingress failed without an error message.",
    );
    let mut failures = existing
        .ingress_failures
        .as_array()
        .cloned()
        .unwrap_or_default();
    failures.push(json!({
        "attempt": attempt,
        "occurred_at": Utc::now().to_rfc3339(),
        "stage": stage,
        "code": code,
        "message": message,
        "rounds_used": input.rounds_used,
        "context_tokens": input.context_tokens,
        "context_window_tokens": input.context_window_tokens,
    }));
    if failures.len() > INGRESS_FAILURE_LIMIT as usize {
        failures.drain(..failures.len() - INGRESS_FAILURE_LIMIT as usize);
    }
    let failures_json = serde_json::to_string(&failures).map_err(ApiError::internal)?;
    let next_phase = if terminal {
        "ingress_failed"
    } else {
        "ingress_pending"
    };
    let now_time = Utc::now();
    let now = now_time.to_rfc3339();
    let next_attempt_at = (!terminal)
        .then(|| (now_time + ChronoDuration::seconds(INGRESS_RETRY_DELAY_SECONDS)).to_rfc3339());
    let changed = tx
        .execute(
            "UPDATE conversations SET phase=?1,ingress_next_attempt_at=?2,updated_at=?3,ingress_failure_count=?4,ingress_failures_json=?5,version=version+1 WHERE id=?6 AND phase IN ('ingress_pending','ingress_in_progress') AND version=?7",
            params![next_phase, next_attempt_at, now, attempt, failures_json, id, input.expected_version],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation history ingress changed while recording a failure.",
        ));
    }
    tx.commit().map_err(ApiError::internal)?;
    tracing::warn!(
        conversation_id = id,
        attempt,
        limit = INGRESS_FAILURE_LIMIT,
        terminal,
        stage,
        code = code.as_deref().unwrap_or("unknown_error"),
        message,
        rounds_used = input.rounds_used,
        context_tokens = input.context_tokens,
        context_window_tokens = input.context_window_tokens,
        "History ingress attempt failed"
    );
    fetch_record(db, id)
}

async fn ingress_failure(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<RecordIngressFailure>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let job = state
        .queue
        .fail(
            SourceKind::Conversation,
            &id,
            input.expected_version,
            &QueueFailure {
                stage: input.stage,
                code: input.code,
                message: input.message,
                rounds_used: input.rounds_used,
                context_tokens: input.context_tokens,
                context_window_tokens: input.context_window_tokens,
            },
        )
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

#[cfg(test)]
fn retry_failed_ingress(
    db: &mut Connection,
    id: &str,
    input: &RetryIngress,
) -> Result<ConversationRecord, ApiError> {
    validate_version(input.expected_version)?;
    let existing = fetch_record(db, id)?;
    if existing.phase != "ingress_failed" || existing.version != input.expected_version {
        return Err(ApiError::conflict(
            "Conversation is not in the expected failed history-ingress state.",
        ));
    }
    let (state_json, summary_state_json) = serialize_state(&input.state)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx
        .execute(
            "UPDATE conversations SET phase='ingress_pending',state_json=?1,summary_state_json=?2,ingress_next_attempt_at=NULL,updated_at=?3,ingress_failure_count=0,version=version+1 WHERE id=?4 AND phase='ingress_failed' AND version=?5",
            params![state_json, summary_state_json, Utc::now().to_rfc3339(), id, input.expected_version],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation history ingress changed before it could be retried.",
        ));
    }
    tx.commit().map_err(ApiError::internal)?;
    fetch_record(db, id)
}

async fn retry_ingress(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let job = state
        .queue
        .retry(
            SourceKind::Conversation,
            &id,
            input.expected_version,
            &input.state,
        )
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

async fn ingress_completed(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<VersionedTransition>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let job = state
        .queue
        .complete(SourceKind::Conversation, &id, input.expected_version)
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&db).unwrap();
        db
    }

    fn app_state() -> AppState {
        AppState {
            db: Arc::new(Mutex::new(database())),
            queue: Queue::in_memory().unwrap(),
        }
    }

    #[tokio::test]
    async fn managed_starts_are_idempotent_and_survive_unstarted_cleanup() {
        let state = app_state();
        let request_id = "11111111111111111111111111111111";
        let (status, Json(first)) = start_managed_conversation(
            State(state.clone()),
            Json(StartManagedConversation {
                idempotency_id: request_id.into(),
                started_at: "2026-07-20T12:00:00Z".into(),
                session_type: "conversation".into(),
                duration_minutes: None,
                custom_prompt: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(first.state["orchestration"]["owner"], "backend");
        assert_eq!(first.state["orchestration"]["status"], "queued");

        let (status, Json(replayed)) = start_managed_conversation(
            State(state.clone()),
            Json(StartManagedConversation {
                idempotency_id: request_id.into(),
                started_at: "2026-07-20T12:00:00Z".into(),
                session_type: "conversation".into(),
                duration_minutes: None,
                custom_prompt: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed.id, first.id);

        let mismatch = start_managed_conversation(
            State(state.clone()),
            Json(StartManagedConversation {
                idempotency_id: request_id.into(),
                started_at: "2026-07-20T12:00:00Z".into(),
                session_type: "free-time".into(),
                duration_minutes: Some(30.0),
                custom_prompt: Some("Explore".into()),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch.code, "state_conflict");

        let mut db = state.db.lock().unwrap();
        assert!(discard_unstarted(&mut db).unwrap().is_empty());
        assert_eq!(fetch_record(&db, &first.id).unwrap().phase, "active");
    }

    #[tokio::test]
    async fn ending_a_conversation_submits_its_exact_archive_to_the_shared_queue() {
        let state = app_state();
        let record = {
            let db = state.db.lock().unwrap();
            insert_conversation(
                &db,
                CreateConversation {
                    started_at: "2026-07-20T12:00:00Z".into(),
                    state: json!({"transcript":[]}),
                },
            )
            .unwrap()
        };
        let final_state = json!({
            "archive": {
                "format":"kennedy-chatend",
                "messages":[{"role":"user","content":"Remember this exactly."}],
                "chatendText":"Remember this exactly."
            }
        });
        let Json(closed) = request_ingress(
            State(state.clone()),
            Path(record.id.clone()),
            Json(CheckpointConversation {
                expected_version: record.version,
                state: final_state.clone(),
                user_activity: false,
            }),
        )
        .await
        .unwrap();

        let job = state.queue.next().unwrap().unwrap();
        assert_eq!(job.source_kind, SourceKind::Conversation);
        assert_eq!(job.source_id, record.id);
        assert_eq!(job.state, final_state);
        assert_eq!(closed.version, job.version);
        assert_eq!(
            closed.state["archive"]["chatendText"],
            "Remember this exactly."
        );
    }

    #[tokio::test]
    async fn managed_self_time_has_one_active_run_and_preserves_its_intent() {
        let state = app_state();
        let first_id = "22222222222222222222222222222222";
        let request = || StartManagedConversation {
            idempotency_id: first_id.into(),
            started_at: "2026-07-20T12:00:00Z".into(),
            session_type: "free-time".into(),
            duration_minutes: Some(45.5),
            custom_prompt: Some("  Review current plans.  ".into()),
        };
        let (_, Json(first)) = start_managed_conversation(State(state.clone()), Json(request()))
            .await
            .unwrap();
        assert_eq!(first.state["selfTimeIntent"]["durationMinutes"], 45.5);
        assert_eq!(
            first.state["selfTimeIntent"]["customPrompt"],
            "Review current plans."
        );

        let (status, Json(replayed)) =
            start_managed_conversation(State(state.clone()), Json(request()))
                .await
                .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed.id, first.id);

        let conflict = start_managed_conversation(
            State(state),
            Json(StartManagedConversation {
                idempotency_id: "33333333333333333333333333333333".into(),
                started_at: "2026-07-20T12:01:00Z".into(),
                session_type: "free-time".into(),
                duration_minutes: Some(10.0),
                custom_prompt: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(conflict.code, "free_time_already_active");
    }

    #[tokio::test]
    async fn command_heads_are_parallel_between_conversations_and_ordered_within_each_one() {
        let state = app_state();
        let start = |id: &'static str| StartManagedConversation {
            idempotency_id: id.into(),
            started_at: "2026-07-20T12:00:00Z".into(),
            session_type: "conversation".into(),
            duration_minutes: None,
            custom_prompt: None,
        };
        let (_, Json(first)) = start_managed_conversation(
            State(state.clone()),
            Json(start("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
        )
        .await
        .unwrap();
        let (_, Json(second)) = start_managed_conversation(
            State(state.clone()),
            Json(start("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
        )
        .await
        .unwrap();
        let message = |id: &'static str, text: &'static str| QueueConversationCommand {
            idempotency_id: id.into(),
            kind: "message".into(),
            payload: json!({"text":text,"metadata":{}}),
        };
        let (_, Json(first_head)) = queue_conversation_command(
            State(state.clone()),
            Path(first.id.clone()),
            Json(message("cccccccccccccccccccccccccccccccc", "First A")),
        )
        .await
        .unwrap();
        let (_, Json(first_tail)) = queue_conversation_command(
            State(state.clone()),
            Path(first.id.clone()),
            Json(message("dddddddddddddddddddddddddddddddd", "Second A")),
        )
        .await
        .unwrap();
        let (_, Json(second_head)) = queue_conversation_command(
            State(state.clone()),
            Path(second.id.clone()),
            Json(message("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "First B")),
        )
        .await
        .unwrap();
        assert_eq!(first_head.sequence, 1);
        assert_eq!(first_tail.sequence, 2);
        assert_eq!(second_head.sequence, 1);

        let Json(heads) = list_conversation_command_heads(State(state.clone()))
            .await
            .unwrap();
        let head_ids = heads["commands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|command| command["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(head_ids.len(), 2);
        assert!(head_ids.contains(&first_head.id.as_str()));
        assert!(head_ids.contains(&second_head.id.as_str()));
        assert!(!head_ids.contains(&first_tail.id.as_str()));

        let Json(claimed_a) =
            claim_conversation_command(State(state.clone()), Path(first_head.id.clone()))
                .await
                .unwrap();
        let Json(claimed_b) =
            claim_conversation_command(State(state.clone()), Path(second_head.id.clone()))
                .await
                .unwrap();
        assert_eq!(claimed_a.status, "processing");
        assert_eq!(claimed_b.status, "processing");

        let _ = complete_conversation_command(
            State(state.clone()),
            Path(first_head.id.clone()),
            Json(CompleteConversationCommand {
                outcome: json!({"status":"answered"}),
            }),
        )
        .await
        .unwrap();
        let Json(heads) = list_conversation_command_heads(State(state.clone()))
            .await
            .unwrap();
        let head_ids = heads["commands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|command| command["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(head_ids.contains(&first_tail.id.as_str()));
        assert!(head_ids.contains(&second_head.id.as_str()));

        let _ = claim_conversation_command(State(state.clone()), Path(first_tail.id.clone()))
            .await
            .unwrap();
        let Json(stopped) = request_conversation_stop(State(state.clone()), Path(first.id.clone()))
            .await
            .unwrap();
        assert_eq!(stopped["stop_requested"], true);
        assert!(
            fetch_command(&state.db.lock().unwrap(), &first_tail.id)
                .unwrap()
                .cancel_requested
        );

        let (status, Json(replayed)) = queue_conversation_command(
            State(state.clone()),
            Path(second.id.clone()),
            Json(message("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "First B")),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed.id, second_head.id);
        let mismatch = queue_conversation_command(
            State(state),
            Path(second.id),
            Json(message(
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "Changed payload",
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch.code, "state_conflict");
    }

    #[tokio::test]
    async fn ending_a_legacy_conversation_cancels_its_failed_turn_and_exposes_end_next() {
        let state = app_state();
        let legacy = {
            let db = state.db.lock().unwrap();
            insert_conversation(
                &db,
                CreateConversation {
                    started_at: "2026-07-20T12:00:00Z".into(),
                    state: json!({
                        "sessionType":"conversation",
                        "orchestration":{"owner":"frontend","status":"stopped"},
                        "pendingTurn":true,
                        "transcript":[{"role":"user","content":"Unanswered query"}]
                    }),
                },
            )
            .unwrap()
        };
        let (_, Json(failed_turn)) = queue_conversation_command(
            State(state.clone()),
            Path(legacy.id.clone()),
            Json(QueueConversationCommand {
                idempotency_id: "11111111111111111111111111111111".into(),
                kind: "retry".into(),
                payload: json!({}),
            }),
        )
        .await
        .unwrap();
        let Json(failed_turn) =
            claim_conversation_command(State(state.clone()), Path(failed_turn.id.clone()))
                .await
                .unwrap();
        assert_eq!(failed_turn.status, "processing");

        let (_, Json(end)) = queue_conversation_command(
            State(state.clone()),
            Path(legacy.id.clone()),
            Json(QueueConversationCommand {
                idempotency_id: "22222222222222222222222222222222".into(),
                kind: "end".into(),
                payload: json!({}),
            }),
        )
        .await
        .unwrap();
        assert!(
            fetch_command(&state.db.lock().unwrap(), &failed_turn.id)
                .unwrap()
                .cancel_requested
        );

        let _ = complete_conversation_command(
            State(state.clone()),
            Path(failed_turn.id),
            Json(CompleteConversationCommand {
                outcome: json!({"status":"stopped"}),
            }),
        )
        .await
        .unwrap();
        let Json(heads) = list_conversation_command_heads(State(state)).await.unwrap();
        assert_eq!(heads["commands"][0]["id"], end.id);
        assert_eq!(heads["commands"][0]["kind"], "end");
    }

    #[test]
    fn conversation_summaries_exclude_large_archives_and_keep_sidebar_metadata() {
        let db = database();
        let large_payload = "x".repeat(1_000_000);
        let record = insert_conversation(
            &db,
            CreateConversation {
                started_at: "2026-07-18T12:00:00Z".into(),
                state: json!({
                    "sessionType":"telegram-group",
                    "channel":{
                        "kind":"telegram-group",
                        "telegramUserId":42,
                        "chatId":-100,
                        "groupId":"group-opaque",
                        "groupRootNodeId":"group-root",
                        "groupContext":{
                            "groupTitle":"Kennedy workshop",
                            "messages":[{"text":large_payload.clone()}]
                        }
                    },
                    "transcript":[
                        {"role":"kennedy","content":"Welcome"},
                        {"role":"user","content":"Plan the next workshop"}
                    ],
                    "archive":{
                        "messages":[{"role":"user","content":"large archive","payload":large_payload.clone()}],
                        "media":[{"dataUrl":format!("data:audio/ogg;base64,{large_payload}")}]
                    }
                }),
            },
        )
        .unwrap();

        let summary = db
            .query_row(
                &format!("{} WHERE id=?1", conversation_summary_select()),
                [&record.id],
                row_summary,
            )
            .unwrap();
        let encoded = serde_json::to_string(&summary).unwrap();
        assert!(summary.summary);
        assert_eq!(summary.state["sessionType"], "telegram-group");
        assert_eq!(
            summary.state["transcript"][0]["content"],
            "Plan the next workshop"
        );
        assert_eq!(
            summary.state["channel"]["groupContext"]["groupTitle"],
            "Kennedy workshop"
        );
        assert_eq!(summary.state["channel"]["groupId"], "group-opaque");
        assert!(encoded.len() < 4_000);
        assert!(!encoded.contains("data:audio"));
        assert!(!encoded.contains(&"x".repeat(1_000)));
    }

    #[test]
    fn self_time_summary_keeps_only_compact_restart_metadata() {
        let summary = summarize_state(&json!({
            "sessionType":"free-time",
            "freeTime":{
                "runId":"run",
                "sliceIndex":3,
                "deadlineAt":"2026-07-20T06:00:00Z",
                "sliceEndedAt":"2026-07-20T05:30:00Z",
                "sliceEndedReason":"tool",
                "nextSessionMessage":"large handoff intentionally omitted"
            }
        }));
        assert_eq!(summary["freeTime"]["sliceIndex"], 3);
        assert_eq!(summary["freeTime"]["deadlineAt"], "2026-07-20T06:00:00Z");
        assert_eq!(summary["freeTime"]["sliceEndedReason"], "tool");
        assert!(summary["freeTime"].get("nextSessionMessage").is_none());
    }

    #[test]
    fn migration_releases_a_failed_legacy_conversation_claim() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(INITIAL_MIGRATION).unwrap();
        db.execute_batch(MULTIPLE_LIVE_MIGRATION).unwrap();
        db.execute_batch(INGRESS_FAILURES_MIGRATION).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,ingress_failure_count,ingress_failures_json) VALUES('stranded','ingress_in_progress','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',7,2,'[]')", []).unwrap();

        apply_migrations(&db).unwrap();

        let record = fetch_record(&db, "stranded").unwrap();
        assert_eq!(record.phase, "ingress_pending");
        assert_eq!(record.version, 8);
        assert!(record.ingress_next_attempt_at.is_some());
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            7
        );
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('next','ingress_in_progress','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',1)", []).unwrap();
    }

    #[test]
    fn migration_removes_existing_self_time_records_from_the_ingress_queue() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(INITIAL_MIGRATION).unwrap();
        db.execute_batch(MULTIPLE_LIVE_MIGRATION).unwrap();
        db.execute_batch(INGRESS_FAILURES_MIGRATION).unwrap();
        db.execute_batch(INGRESS_RETRY_SCHEDULE_MIGRATION).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('self-time-claimed','ingress_in_progress','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"sessionType\":\"free-time\"}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('self-time-failed','ingress_failed','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{\"archive\":{\"sessionType\":\"free-time\"}}',3)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('conversation','ingress_pending','2026-01-03T00:00:00Z','2026-01-03T00:00:00Z','{}',1)", []).unwrap();

        apply_migrations(&db).unwrap();

        let claimed = fetch_record(&db, "self-time-claimed").unwrap();
        assert_eq!(claimed.phase, "complete");
        assert_eq!(claimed.version, 2);
        assert!(claimed.ended_at.is_some());
        assert_eq!(
            fetch_record(&db, "self-time-failed").unwrap().phase,
            "complete"
        );
        assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "conversation");
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            7
        );
    }

    #[test]
    fn state_machine_requires_ingress_before_completion() {
        let db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('c','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        let state = json!({
            "transcript":[],
            "archive":{
                "format":"kennedy-chatend",
                "messages":[{"role":"user","content":[
                    {"type":"input_text","text":"Look"},
                    {"type":"input_image","image_url":"data:image/png;base64,AAAA"}
                ]}]
            }
        });
        let record = update_active(&db, "c", 1, &state, "ingress_pending").unwrap();
        assert_eq!(record.phase, "ingress_pending");
        assert_eq!(record.version, 2);
        assert_eq!(record.state["transcript"], json!([]));
        assert_eq!(record.state, state);
    }

    #[test]
    fn self_time_completes_directly_without_entering_the_ingress_queue() {
        let db = database();
        let active_state = json!({
            "sessionType":"free-time",
            "archive":{"format":"kennedy-chatend","sessionType":"free-time"}
        });
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('self-time','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',?1,1)", [serde_json::to_string(&active_state).unwrap()]).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('conversation','active','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{\"sessionType\":\"conversation\"}',1)", []).unwrap();

        let unfinished = complete_self_time(&db, "self-time", 1, &active_state).unwrap_err();
        assert_eq!(unfinished.code, "invalid_request");
        assert!(unfinished.message.contains("EndTurn"));
        assert_eq!(fetch_record(&db, "self-time").unwrap().phase, "active");

        let state = json!({
            "sessionType":"free-time",
            "freeTime":{"sliceEndedReason":"tool"},
            "archive":{"format":"kennedy-chatend","sessionType":"free-time","tools":{"log":[{"name":"EndTurn","ok":true}]}}
        });
        let completed = complete_self_time(&db, "self-time", 1, &state).unwrap();
        assert_eq!(completed.phase, "complete");
        assert_eq!(completed.version, 2);
        assert!(completed.ended_at.is_some());
        assert!(fetch_next_ingress(&db).unwrap().is_none());

        let error = complete_self_time(
            &db,
            "conversation",
            1,
            &json!({"sessionType":"conversation"}),
        )
        .unwrap_err();
        assert_eq!(error.code, "invalid_request");
        assert_eq!(fetch_record(&db, "conversation").unwrap().phase, "active");
    }

    #[test]
    fn ingress_queue_defensively_skips_self_time_records() {
        let db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('self-time','ingress_pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"sessionType\":\"free-time\"}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('conversation','ingress_pending','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',1)", []).unwrap();

        assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "conversation");

        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('self-time-repair','ingress_pending','2025-12-31T00:00:00Z','2025-12-31T00:00:00Z','{\"sessionType\":\"free-time\",\"historyIngressRepairRequired\":true}',1)", []).unwrap();
        assert_eq!(
            fetch_next_ingress(&db).unwrap().unwrap().id,
            "self-time-repair"
        );
    }

    #[test]
    fn stale_checkpoint_is_rejected() {
        let db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('c','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',2)", []).unwrap();
        assert_eq!(
            update_active(&db, "c", 1, &json!({}), "active")
                .unwrap_err()
                .code,
            "state_conflict"
        );
    }

    #[test]
    fn purge_permanently_removes_any_phase_without_queueing_ingress() {
        let db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('stuck','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"pendingTurn\":true}',4)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('claimed','ingress_in_progress','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',2)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('next','ingress_pending','2026-01-03T00:00:00Z','2026-01-03T00:00:00Z','{}',1)", []).unwrap();

        assert_eq!(
            purge_record(&db, "stuck", 3).unwrap_err().code,
            "state_conflict"
        );
        assert_eq!(fetch_record(&db, "stuck").unwrap().phase, "active");
        purge_record(&db, "stuck", 4).unwrap();
        purge_record(&db, "claimed", 2).unwrap();

        assert_eq!(fetch_record(&db, "stuck").unwrap_err().code, "not_found");
        assert_eq!(fetch_record(&db, "claimed").unwrap_err().code, "not_found");
        assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "next");
    }

    #[test]
    fn database_allows_multiple_live_conversations_but_one_ingress_worker() {
        let db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('a','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('b','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        db.execute(
            "UPDATE conversations SET phase='ingress_in_progress' WHERE id='a'",
            [],
        )
        .unwrap();
        assert!(
            db.execute(
                "UPDATE conversations SET phase='ingress_in_progress' WHERE id='b'",
                []
            )
            .is_err()
        );
    }

    #[test]
    fn create_allows_only_one_active_free_time_record() {
        let db = database();
        let free_time = || CreateConversation {
            started_at: "2026-07-17T12:00:00Z".into(),
            state: json!({"sessionType":"free-time","freeTime":{"runId":Uuid::new_v4().to_string()}}),
        };
        let first = insert_conversation(&db, free_time()).unwrap();
        let error = insert_conversation(&db, free_time()).unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, "free_time_already_active");

        insert_conversation(
            &db,
            CreateConversation {
                started_at: "2026-07-17T12:00:01Z".into(),
                state: json!({"sessionType":"conversation"}),
            },
        )
        .unwrap();
        update_active(
            &db,
            &first.id,
            first.version,
            &first.state,
            "ingress_pending",
        )
        .unwrap();
        insert_conversation(&db, free_time()).unwrap();
    }

    #[test]
    fn user_activity_expires_only_idle_conversations_without_pending_turns() {
        let mut db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('current','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1,'2026-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('stale','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"pendingTurn\":false}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('pending','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"pendingTurn\":true}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('telegram','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"sessionType\":\"telegram\",\"pendingTurn\":false}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('telegram-group','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"sessionType\":\"telegram-group\",\"pendingTurn\":false}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('free-time','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"sessionType\":\"free-time\",\"pendingTurn\":false}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        checkpoint_user_activity(
            &mut db,
            &Queue::in_memory().unwrap(),
            "current",
            1,
            &json!({"pendingTurn":true}),
        )
        .unwrap();
        assert_eq!(fetch_record(&db, "stale").unwrap().phase, "ingress_pending");
        assert_eq!(fetch_record(&db, "pending").unwrap().phase, "active");
        assert_eq!(fetch_record(&db, "telegram").unwrap().phase, "active");
        assert_eq!(fetch_record(&db, "telegram-group").unwrap().phase, "active");
        assert_eq!(fetch_record(&db, "free-time").unwrap().phase, "active");
    }

    #[test]
    fn current_migrations_do_not_recreate_the_old_unfinished_index() {
        let db = database();
        apply_migrations(&db).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('a','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('b','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
    }

    #[test]
    fn current_migrations_repair_a_v2_database_with_the_legacy_index() {
        let db = database();
        db.execute_batch(
            "CREATE UNIQUE INDEX one_unfinished_conversation
             ON conversations ((1))
             WHERE phase IN ('active','ingress_pending','ingress_in_progress');",
        )
        .unwrap();
        apply_migrations(&db).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('a','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('b','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
    }

    #[test]
    fn startup_cleanup_discards_only_conversations_without_user_messages() {
        let mut db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('empty','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"transcript\":[]}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('started','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"transcript\":[{\"role\":\"user\",\"content\":\"Hello\"}]}',1,'2026-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('legacy-started','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"archive\":{\"transcript\":[{\"role\":\"user\",\"content\":\"Hello\"}]}}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('queued','ingress_pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"transcript\":[]}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('complete-empty','complete','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"transcript\":[]}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('telegram-bound','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"sessionType\":\"telegram\",\"transcript\":[]}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('telegram-group-bound','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"sessionType\":\"telegram-group\",\"transcript\":[]}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('free-time-bound','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{\"sessionType\":\"free-time\",\"transcript\":[]}',1)", []).unwrap();

        assert_eq!(
            discard_unstarted(&mut db).unwrap(),
            vec!["complete-empty", "empty", "queued"]
        );
        assert_eq!(
            fetch_record(&db, "complete-empty").unwrap_err().code,
            "not_found"
        );
        assert_eq!(fetch_record(&db, "empty").unwrap_err().code, "not_found");
        assert_eq!(fetch_record(&db, "queued").unwrap_err().code, "not_found");
        assert_eq!(fetch_record(&db, "started").unwrap().phase, "active");
        assert_eq!(fetch_record(&db, "legacy-started").unwrap().phase, "active");
        assert_eq!(fetch_record(&db, "telegram-bound").unwrap().phase, "active");
        assert_eq!(
            fetch_record(&db, "telegram-group-bound").unwrap().phase,
            "active"
        );
        assert_eq!(
            fetch_record(&db, "free-time-bound").unwrap().phase,
            "active"
        );
        assert!(discard_unstarted(&mut db).unwrap().is_empty());
    }

    #[test]
    fn legacy_single_conversation_database_migrates_in_place() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(INITIAL_MIGRATION).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('legacy','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        apply_migrations(&db).unwrap();
        let record = fetch_record(&db, "legacy").unwrap();
        assert_eq!(record.phase, "active");
        assert!(record.last_user_message_at.is_none());
        assert_eq!(record.ingress_failure_count, 0);
        assert_eq!(record.ingress_failures, json!([]));
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            7
        );
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('new','active','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',1)", []).unwrap();
    }

    #[test]
    fn legacy_checkpoint_reads_supply_canonical_chatend_text() {
        let db = database();
        let state = json!({
            "archive": {
                "format":"kennedy-chatend",
                "messages":[{"role":"user","content":"Legacy conversation"}],
                "fullHistory":{"segments":[{
                    "messages":[{"role":"assistant","content":"Before reset"}]
                }]}
            },
            "historyIngress": {
                "format":"kennedy-chatend",
                "messages":[{"role":"user","content":"Legacy ingress"}]
            }
        });
        db.execute(
            "INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('legacy-chatend','complete','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',?1,1)",
            [serde_json::to_string(&state).unwrap()],
        )
        .unwrap();

        let record = fetch_record(&db, "legacy-chatend").unwrap();
        assert_eq!(
            record
                .state
                .pointer("/archive/chatendText")
                .and_then(Value::as_str),
            Some("David\n\nLegacy conversation")
        );
        assert_eq!(
            record
                .state
                .pointer("/archive/fullHistory/segments/0/chatendText")
                .and_then(Value::as_str),
            Some("Kennedy\n\nBefore reset")
        );
        assert_eq!(
            record
                .state
                .pointer("/historyIngress/chatendText")
                .and_then(Value::as_str),
            Some("David\n\nLegacy ingress")
        );
    }

    #[test]
    fn ingress_queue_is_oldest_activity_first_and_resumes_claimed_work() {
        let db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('newer','ingress_pending','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',1,'2026-01-02T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('older','ingress_pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1,'2026-01-01T00:00:00Z')", []).unwrap();
        assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "older");
        db.execute(
            "UPDATE conversations SET phase='ingress_in_progress' WHERE id='newer'",
            [],
        )
        .unwrap();
        assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "newer");
    }

    #[test]
    fn ingress_checkpoints_preserve_the_complete_ingress_chatend() {
        let db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('c','ingress_in_progress','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',4)", []).unwrap();
        let state = json!({"archive":{"format":"kennedy-chatend"},"historyIngress":{
            "format":"kennedy-chatend",
            "sessionType":"history-ingress",
            "messages":[{"role":"assistant","content":"Memory updated."}]
        }});
        let record = update_ingress(&db, "c", 4, &state).unwrap();
        assert_eq!(record.phase, "ingress_in_progress");
        assert_eq!(record.version, 5);
        assert_eq!(
            record
                .state
                .pointer("/historyIngress/chatendText")
                .and_then(Value::as_str),
            Some("Kennedy\n\nMemory updated.")
        );
        let stored: String = db
            .query_row(
                "SELECT state_json FROM conversations WHERE id='c'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(&stored).unwrap(), state);
        assert_eq!(
            update_ingress(&db, "c", 4, &json!({})).unwrap_err().code,
            "state_conflict"
        );
    }

    #[test]
    fn ingress_completion_requires_a_successful_end_tool_receipt() {
        assert!(!history_ingress_was_explicitly_ended(&json!({})));
        assert!(!history_ingress_was_explicitly_ended(&json!({
            "historyIngress":{"tools":{"log":[{
                "name":"EndHistoryIngress",
                "ok":true
            }]}}
        })));
        assert!(!history_ingress_was_explicitly_ended(&json!({
            "historyIngress":{"tools":{"log":[{
                "name":"EndTurn",
                "ok":false
            }]}}
        })));
        assert!(history_ingress_was_explicitly_ended(&json!({
            "historyIngress":{"tools":{"log":[{
                "name":"EndTurn",
                "ok":true
            }]}}
        })));
    }

    #[test]
    fn fifth_ingress_failure_is_durable_and_terminal() {
        let mut db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('poisoned','ingress_in_progress','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('next','ingress_pending','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',1)", []).unwrap();

        let mut record = fetch_record(&db, "poisoned").unwrap();
        for attempt in 1..=INGRESS_FAILURE_LIMIT {
            record = record_ingress_failure(
                &mut db,
                "poisoned",
                &RecordIngressFailure {
                    expected_version: record.version,
                    stage: "generation".into(),
                    code: Some("provider_error".into()),
                    message: format!("Attempt {attempt} failed\nwith details"),
                    rounds_used: Some(attempt as u64),
                    context_tokens: Some(250_000),
                    context_window_tokens: Some(258_400),
                },
            )
            .unwrap();
            assert_eq!(record.ingress_failure_count, attempt);
            assert_eq!(
                record.ingress_failures.as_array().unwrap().len(),
                attempt as usize
            );
            if attempt < INGRESS_FAILURE_LIMIT {
                assert_eq!(record.phase, "ingress_pending");
                assert!(record.ingress_next_attempt_at.is_some());
                assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "next");
                db.execute(
                    "UPDATE conversations SET phase='ingress_in_progress',ingress_next_attempt_at=NULL WHERE id='poisoned'",
                    [],
                )
                .unwrap();
            }
        }

        assert_eq!(record.phase, "ingress_failed");
        assert_eq!(record.ingress_failures[4]["stage"], "generation");
        assert_eq!(record.ingress_failures[4]["rounds_used"], 5);
        assert_eq!(
            record.ingress_failures[4]["message"],
            "Attempt 5 failed with details"
        );
        assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "next");
    }

    #[test]
    fn oversized_conversation_ingress_is_terminal_without_repeating_the_same_request() {
        let mut db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('oversized','ingress_in_progress','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('next','ingress_pending','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',1)", []).unwrap();

        let failed = record_ingress_failure(
            &mut db,
            "oversized",
            &RecordIngressFailure {
                expected_version: 1,
                stage: "generation".into(),
                code: Some("input_too_large".into()),
                message: "input exceeds the provider limit".into(),
                rounds_used: Some(1),
                context_tokens: None,
                context_window_tokens: None,
            },
        )
        .unwrap();

        assert_eq!(failed.phase, "ingress_failed");
        assert_eq!(failed.ingress_failure_count, 1);
        assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "next");
    }

    #[test]
    fn terminal_ingress_can_be_requeued_with_a_fresh_frontend_checkpoint() {
        let mut db = database();
        db.execute(
            "INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,ingress_failure_count,ingress_failures_json) VALUES('failed','ingress_failed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',?1,3,5,?2)",
            params![
                r#"{"archive":{"format":"kennedy-chatend"},"historyIngress":{"completed":false}}"#,
                r#"[{"attempt":5,"message":"context exhausted"}]"#,
            ],
        )
        .unwrap();

        let retried = retry_failed_ingress(
            &mut db,
            "failed",
            &RetryIngress {
                expected_version: 3,
                state: json!({"archive":{"format":"kennedy-chatend"}}),
            },
        )
        .unwrap();

        assert_eq!(retried.phase, "ingress_pending");
        assert_eq!(retried.ingress_failure_count, 0);
        assert!(retried.ingress_next_attempt_at.is_none());
        assert_eq!(retried.state.get("historyIngress"), None);
        assert_eq!(retried.ingress_failures[0]["message"], "context exhausted");
    }
}
