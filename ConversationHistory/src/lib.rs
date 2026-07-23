//! Session lifecycle and local Session History.
//!
//! In-progress lifecycle and command records are sideband frames in the same
//! append-only Chatend journal as the session itself. Successfully committed
//! sessions leave only one local line containing their immutable Kweb object
//! ID; their details are loaded from Kweb on demand.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path as FilePath, PathBuf},
    sync::{Arc, Mutex, Weak},
};

use anyhow::Context as _;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use kennedy_chatend::{BoxOwner, SessionJournal, SessionKind, SessionMetadata};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

const LIFECYCLE_SIDEBAND: &str = "session_lifecycle";
const COMMAND_SIDEBAND: &str = "session_command";

#[derive(Clone, Debug)]
pub struct Config {
    pub directory: PathBuf,
    pub completed_list: PathBuf,
    pub max_request_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    config: Config,
    catalog_mutation: Arc<Mutex<()>>,
    session_mutations: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
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
        Self::new(StatusCode::NOT_FOUND, "not_found", "Session not found.")
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "state_conflict", message)
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error=%error, "Session History request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected Session History storage error occurred.",
        )
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SessionRecord {
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
    #[serde(default, skip_serializing_if = "is_false")]
    summary: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Deserialize)]
struct CreateSession {
    started_at: String,
    state: Value,
}

#[derive(Deserialize)]
struct StartManagedSession {
    idempotency_id: String,
    started_at: String,
    session_type: String,
    #[serde(default)]
    duration_minutes: Option<f64>,
    #[serde(default)]
    custom_prompt: Option<String>,
}

#[derive(Deserialize)]
struct QueueSessionCommand {
    idempotency_id: String,
    kind: String,
    #[serde(default = "empty_object")]
    payload: Value,
}

fn empty_object() -> Value {
    json!({})
}

#[derive(Deserialize)]
struct CompleteSessionCommand {
    #[serde(default = "empty_object")]
    outcome: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionCommand {
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
    idempotency_id: String,
}

#[derive(Deserialize)]
struct CheckpointSession {
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

#[derive(Deserialize)]
struct RecordCompletedSession {
    session_object_id: String,
    #[serde(default)]
    journal_path: Option<PathBuf>,
}

pub fn open(config: Config) -> anyhow::Result<Service> {
    create_private_directory(&config.directory)?;
    if let Some(parent) = config
        .completed_list
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        create_private_directory(parent)?;
    }
    if !config.completed_list.exists() {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&config.completed_list)
            .with_context(|| format!("creating {}", config.completed_list.display()))?;
        file.sync_all()?;
        sync_directory(
            config
                .completed_list
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| FilePath::new(".")),
        )?;
    }
    let state = AppState {
        config: config.clone(),
        catalog_mutation: Arc::new(Mutex::new(())),
        session_mutations: Arc::new(Mutex::new(HashMap::new())),
    };
    Ok(Service {
        state,
        max_request_bytes: config.max_request_bytes,
    })
}

pub fn router(service: Service) -> Router {
    Router::new()
        .route("/api/v1/conversations/health", get(health))
        .route("/api/v1/session-history", post(record_completed_session))
        .route("/api/v1/conversations/start", post(start_managed_session))
        .route("/api/v1/conversation-commands", get(list_command_heads))
        .route(
            "/api/v1/conversations/summaries",
            get(list_session_summaries),
        )
        .route(
            "/api/v1/conversations/{session_id}",
            get(get_session).delete(purge_session),
        )
        .route(
            "/api/v1/conversations/{session_id}/commands",
            post(queue_session_command),
        )
        .route(
            "/api/v1/conversations/{session_id}/objects",
            post(stage_session_object),
        )
        .route(
            "/api/v1/conversations/{session_id}/stop",
            post(request_session_stop),
        )
        .route(
            "/api/v1/conversations/{session_id}/retry-ingress",
            post(retry_ingress),
        )
        .layer(DefaultBodyLimit::max(service.max_request_bytes))
        .with_state(service.state)
}

impl Service {
    pub fn health(&self) -> Result<Value, ServiceError> {
        read_completed_ids(&self.state.config.completed_list).map_err(ApiError::internal)?;
        Ok(json!({"service":"session-history","status":"ok"}))
    }

    pub async fn get_json(&self, path: &str) -> Result<Value, ServiceError> {
        let state = State(self.state.clone());
        match path {
            "/api/v1/conversations/health" => self.health(),
            "/api/v1/conversations/summaries" => {
                let Json(value) = list_session_summaries(state).await?;
                Ok(value)
            }
            "/api/v1/conversation-commands" => {
                let Json(value) = list_command_heads(state).await?;
                Ok(value)
            }
            _ => {
                let id = path
                    .strip_prefix("/api/v1/conversations/")
                    .filter(|id| !id.contains('/'))
                    .ok_or_else(ApiError::not_found)?;
                let Json(record) = get_session(state, Path(id.to_owned())).await?;
                serde_json::to_value(record)
                    .map_err(ApiError::internal)
                    .map_err(Into::into)
            }
        }
    }

    pub async fn post_json(&self, path: &str, body: Value) -> Result<Value, ServiceError> {
        let state = State(self.state.clone());
        if path == "/api/v1/conversations" {
            let (_, Json(record)) = create_session(state, Json(parse_body(body)?)).await?;
            return json_value(record);
        }
        if path == "/api/v1/session-history" {
            let Json(value) = record_completed_session(state, Json(parse_body(body)?)).await?;
            return Ok(value);
        }
        if path == "/api/v1/conversations/start" {
            let (_, Json(record)) = start_managed_session(state, Json(parse_body(body)?)).await?;
            return json_value(record);
        }
        if path == "/api/v1/conversations/ingress/repairs/release" {
            return Ok(json!({"released":[]}));
        }
        if let Some(command_id) = path
            .strip_prefix("/api/v1/conversation-commands/")
            .and_then(|tail| tail.strip_suffix("/claim"))
        {
            let Json(command) = claim_command(state, Path(command_id.into())).await?;
            return json_value(command);
        }
        if let Some(command_id) = path
            .strip_prefix("/api/v1/conversation-commands/")
            .and_then(|tail| tail.strip_suffix("/complete"))
        {
            let Json(command) =
                complete_command(state, Path(command_id.into()), Json(parse_body(body)?)).await?;
            return json_value(command);
        }
        let tail = path
            .strip_prefix("/api/v1/conversations/")
            .ok_or_else(ApiError::not_found)?;
        let (id, action) = tail.split_once('/').ok_or_else(ApiError::not_found)?;
        match action {
            "commands" => {
                let (_, Json(command)) =
                    queue_session_command(state, Path(id.into()), Json(parse_body(body)?)).await?;
                json_value(command)
            }
            "stop" => {
                let Json(value) = request_session_stop(state, Path(id.into())).await?;
                Ok(value)
            }
            "request-ingress" => {
                let Json(record) =
                    transition_with_checkpoint(state, id, parse_body(body)?, "ingress_pending")
                        .await?;
                json_value(record)
            }
            "complete" => {
                let input: CheckpointSession = parse_body(body)?;
                let Json(record) =
                    complete_session(state, id, input.expected_version, input.state).await?;
                json_value(record)
            }
            "ingress-started" => {
                let input: StartIngress = parse_body(body)?;
                let _protocol = input.completion_protocol;
                let Json(record) = transition(
                    state,
                    id,
                    input.expected_version,
                    "ingress_in_progress",
                    Some(input.provenance_id),
                )
                .await?;
                json_value(record)
            }
            "ingress-completed" => {
                let input: VersionedTransition = parse_body(body)?;
                let current = fetch_active(&state.0, id)?;
                let Json(record) =
                    complete_session(state, id, input.expected_version, current.state).await?;
                json_value(record)
            }
            "ingress-failure" => {
                let input: RecordIngressFailure = parse_body(body)?;
                let Json(record) = record_ingress_failure(state, id, input).await?;
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
        let input: CheckpointSession = parse_body(body)?;
        if !matches!(action, "checkpoint" | "ingress-checkpoint") {
            return Err(ApiError::not_found().into());
        }
        let Json(record) = checkpoint(
            State(self.state.clone()),
            id,
            input.expected_version,
            input.state,
            input.user_activity,
        )
        .await?;
        json_value(record)
    }

    pub async fn delete_json(
        &self,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, ServiceError> {
        if path == "/api/v1/conversations/unstarted" {
            return Ok(json!({"discarded":[]}));
        }
        let id = path
            .strip_prefix("/api/v1/conversations/")
            .filter(|id| !id.contains('/'))
            .ok_or_else(ApiError::not_found)?;
        let expected = body
            .as_ref()
            .and_then(|value| value.get("expected_version"))
            .and_then(Value::as_i64);
        let Json(value) =
            purge_session(State(self.state.clone()), Path(id.into()), Json(expected)).await?;
        Ok(value)
    }
}

async fn record_completed_session(
    State(state): State<AppState>,
    Json(input): Json<RecordCompletedSession>,
) -> Result<Json<Value>, ApiError> {
    let session_guard = input
        .journal_path
        .as_ref()
        .and_then(|path| path.file_stem())
        .and_then(|value| value.to_str())
        .map(|id| session_mutation(&state, id))
        .transpose()?;
    let _session_guard = session_guard
        .as_ref()
        .map(|guard| guard.lock().map_err(ApiError::internal))
        .transpose()?;
    let _catalog_guard = state.catalog_mutation.lock().map_err(ApiError::internal)?;
    append_completed_id(&state.config.completed_list, &input.session_object_id)
        .map_err(ApiError::internal)?;
    if let Some(path) = input.journal_path {
        ensure_inside(&path, &state.config.directory)?;
        if path.exists() {
            std::fs::remove_file(&path)
                .with_context(|| format!("removing committed journal {}", path.display()))
                .map_err(ApiError::internal)?;
        }
    }
    Ok(Json(json!({
        "sessionObjectId":input.session_object_id,
        "recorded":true
    })))
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
    match read_completed_ids(&state.config.completed_list) {
        Ok(_) => Json(json!({"service":"session-history","status":"ok"})).into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"service":"session-history","status":"unavailable"})),
        )
            .into_response(),
    }
}

async fn create_session(
    State(state): State<AppState>,
    Json(input): Json<CreateSession>,
) -> Result<(StatusCode, Json<SessionRecord>), ApiError> {
    validate_started_at(&input.started_at)?;
    let path = input
        .state
        .get("journalPath")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| {
            ApiError::bad("A session created by the backend must provide its journalPath.")
        })?;
    ensure_inside(&path, &state.config.directory)?;
    let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
    let id = journal.state().metadata.session_id.clone();
    drop(journal);
    let session_guard = session_mutation(&state, &id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
    if latest_lifecycle(&journal).is_some() {
        return Err(ApiError::conflict("Session is already registered."));
    }
    let record = SessionRecord {
        id,
        phase: "active".into(),
        started_at: input.started_at.clone(),
        updated_at: input.started_at,
        state: control_state(&input.state),
        provenance_id: None,
        version: 1,
        last_user_message_at: None,
        ended_at: None,
        ingress_failure_count: 0,
        ingress_failures: json!([]),
        ingress_next_attempt_at: None,
        summary: false,
    };
    append_lifecycle(&mut journal, &record)?;
    Ok((StatusCode::CREATED, Json(materialize(record, &journal))))
}

async fn start_managed_session(
    State(state): State<AppState>,
    Json(input): Json<StartManagedSession>,
) -> Result<(StatusCode, Json<SessionRecord>), ApiError> {
    validate_started_at(&input.started_at)?;
    validate_idempotency(&input.idempotency_id)?;
    let _catalog_guard = state.catalog_mutation.lock().map_err(ApiError::internal)?;
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
        if let Some(record) = latest_lifecycle(&journal)
            && record
                .state
                .get("startIdempotencyId")
                .and_then(Value::as_str)
                == Some(&input.idempotency_id)
        {
            return Ok((StatusCode::OK, Json(materialize(record, &journal))));
        }
    }
    let id = Uuid::new_v4().to_string();
    let path = state.config.directory.join(format!("{id}.chatend"));
    let kind = match input.session_type.as_str() {
        "free-time" => SessionKind::SelfTime,
        "telegram" => SessionKind::Telegram,
        "telegram-group" => SessionKind::TelegramGroup,
        other => SessionKind::Other(other.into()),
    };
    let mut journal = SessionJournal::create(
        &path,
        SessionMetadata {
            session_id: id.clone(),
            kind,
            created_at: input.started_at.clone(),
            // The orchestration runtime records the exact selected-model
            // value before it creates the first box.
            effective_context_tokens: 1,
            channel: Value::Null,
        },
    )
    .map_err(ApiError::internal)?;
    let mut session_state = json!({
        "stateVersion":3,
        "sessionId":id,
        "journalPath":path,
        "sessionType":input.session_type,
        "startedAt":input.started_at,
        "startIdempotencyId":input.idempotency_id,
        "orchestration":{"owner":"backend","status":"idle"},
    });
    if input.session_type == "free-time" {
        session_state["selfTimeIntent"] = json!({
            "requestedAt":input.started_at,
            "durationMinutes":input.duration_minutes,
            "customPrompt":input.custom_prompt.unwrap_or_default(),
        });
    }
    let record = SessionRecord {
        id,
        phase: "active".into(),
        started_at: input.started_at.clone(),
        updated_at: input.started_at,
        state: session_state,
        provenance_id: None,
        version: 1,
        last_user_message_at: None,
        ended_at: None,
        ingress_failure_count: 0,
        ingress_failures: json!([]),
        ingress_next_attempt_at: None,
        summary: false,
    };
    append_lifecycle(&mut journal, &record)?;
    Ok((StatusCode::CREATED, Json(materialize(record, &journal))))
}

async fn list_session_summaries(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let mut sessions = Vec::new();
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
        if let Some(mut record) = latest_lifecycle(&journal) {
            record.summary = true;
            record.state = summary_state(&record.state, &journal);
            sessions.push(record);
        }
    }
    for object_id in read_completed_ids(&state.config.completed_list).map_err(ApiError::internal)? {
        sessions.push(SessionRecord {
            id: object_id.clone(),
            phase: "complete".into(),
            started_at: String::new(),
            updated_at: String::new(),
            state: json!({"sessionObjectId":object_id}),
            provenance_id: None,
            version: 1,
            last_user_message_at: None,
            ended_at: None,
            ingress_failure_count: 0,
            ingress_failures: json!([]),
            ingress_next_attempt_at: None,
            summary: true,
        });
    }
    sessions.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    Ok(Json(json!({"conversations":sessions})))
}

async fn get_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionRecord>, ApiError> {
    if read_completed_ids(&state.config.completed_list)
        .map_err(ApiError::internal)?
        .contains(&id)
    {
        return Ok(Json(SessionRecord {
            id: id.clone(),
            phase: "complete".into(),
            started_at: String::new(),
            updated_at: String::new(),
            state: json!({"sessionObjectId":id}),
            provenance_id: None,
            version: 1,
            last_user_message_at: None,
            ended_at: None,
            ingress_failure_count: 0,
            ingress_failures: json!([]),
            ingress_next_attempt_at: None,
            summary: false,
        }));
    }
    let journal = open_by_id(&state, &id)?;
    let record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    Ok(Json(materialize(record, &journal)))
}

async fn queue_session_command(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<QueueSessionCommand>,
) -> Result<(StatusCode, Json<SessionCommand>), ApiError> {
    validate_idempotency(&input.idempotency_id)?;
    let session_guard = session_mutation(&state, &id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, &id)?;
    let record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    if record.phase != "active" {
        return Err(ApiError::conflict("Session is no longer active."));
    }
    let commands = commands(&journal);
    if let Some(command) = commands
        .values()
        .find(|command| command.idempotency_id == input.idempotency_id)
    {
        return Ok((StatusCode::OK, Json(command.clone())));
    }
    let command = SessionCommand {
        id: Uuid::new_v4().to_string(),
        conversation_id: id,
        sequence: commands
            .values()
            .map(|command| command.sequence)
            .max()
            .unwrap_or(0)
            + 1,
        kind: input.kind,
        payload: input.payload,
        status: "pending".into(),
        cancel_requested: false,
        outcome: None,
        created_at: now(),
        processing_started_at: None,
        completed_at: None,
        idempotency_id: input.idempotency_id,
    };
    append_command(&mut journal, &command)?;
    Ok((StatusCode::CREATED, Json(command)))
}

async fn stage_session_object(
    State(state): State<AppState>,
    Path(id): Path<String>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut body = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad(error.to_string()))?
    {
        if field.name() != Some("file") {
            continue;
        }
        let file_name = field.file_name().map(str::to_owned);
        let media_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_owned();
        let bytes = field
            .bytes()
            .await
            .map_err(|error| ApiError::bad(error.to_string()))?;
        if body.replace((file_name, media_type, bytes)).is_some() {
            return Err(ApiError::bad("Upload exactly one object per request."));
        }
    }
    let (file_name, media_type, bytes) =
        body.ok_or_else(|| ApiError::bad("Multipart upload omitted the file field."))?;
    let session_guard = session_mutation(&state, &id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, &id)?;
    let record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    if record.phase != "active" {
        return Err(ApiError::conflict(
            "Objects can only be supplied to an active session.",
        ));
    }
    if commands(&journal)
        .values()
        .any(|command| matches!(command.status.as_str(), "pending" | "processing"))
    {
        return Err(ApiError::conflict(
            "Objects cannot be supplied while the session is processing a command.",
        ));
    }
    let pending_id = journal
        .stage_object(
            now(),
            media_type,
            file_name,
            json!({"adapter":"browser"}),
            &bytes,
        )
        .map_err(|error| ApiError::bad(error.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"pendingId":pending_id,"bytes":bytes.len()})),
    ))
}

async fn list_command_heads(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let mut heads = Vec::new();
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(path).map_err(ApiError::internal)?;
        let mut active = commands(&journal)
            .into_values()
            .filter(|command| matches!(command.status.as_str(), "pending" | "processing"))
            .collect::<Vec<_>>();
        active.sort_by_key(|command| command.sequence);
        if let Some(head) = active.into_iter().next() {
            heads.push(head);
        }
    }
    heads.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    Ok(Json(json!({"commands":heads})))
}

async fn claim_command(
    State(state): State<AppState>,
    Path(command_id): Path<String>,
) -> Result<Json<SessionCommand>, ApiError> {
    mutate_command(&state, &command_id, |command| {
        if command.status == "pending" {
            command.status = "processing".into();
            command.processing_started_at = Some(now());
        } else if command.status != "processing" {
            return Err(ApiError::conflict("Command is already complete."));
        }
        Ok(())
    })
    .map(Json)
}

async fn complete_command(
    State(state): State<AppState>,
    Path(command_id): Path<String>,
    Json(input): Json<CompleteSessionCommand>,
) -> Result<Json<SessionCommand>, ApiError> {
    mutate_command(&state, &command_id, |command| {
        if command.status == "complete" {
            return Ok(());
        }
        if command.status != "processing" {
            return Err(ApiError::conflict("Command was not claimed."));
        }
        command.status = "complete".into();
        command.outcome = Some(input.outcome);
        command.completed_at = Some(now());
        Ok(())
    })
    .map(Json)
}

fn mutate_command(
    state: &AppState,
    command_id: &str,
    mutation: impl FnOnce(&mut SessionCommand) -> Result<(), ApiError>,
) -> Result<SessionCommand, ApiError> {
    let mut target = None;
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
        if let Some(command) = commands(&journal).remove(command_id) {
            target = Some((path, command.conversation_id));
            break;
        }
    }
    let (path, conversation_id) = target.ok_or_else(ApiError::not_found)?;
    let session_guard = session_mutation(state, &conversation_id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = SessionJournal::open(path).map_err(ApiError::internal)?;
    let mut command = commands(&journal)
        .remove(command_id)
        .ok_or_else(ApiError::not_found)?;
    mutation(&mut command)?;
    append_command(&mut journal, &command)?;
    Ok(command)
}

async fn request_session_stop(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let current = fetch_active(&state, &id)?;
    let Json(record) =
        transition(State(state), &id, current.version, "ingress_pending", None).await?;
    Ok(Json(
        json!({"id":id,"phase":record.phase,"stopRequested":true}),
    ))
}

async fn checkpoint(
    State(state): State<AppState>,
    id: &str,
    expected_version: i64,
    new_state: Value,
    user_activity: bool,
) -> Result<Json<SessionRecord>, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, expected_version)?;
    record.state = control_state(&new_state);
    record.version += 1;
    record.updated_at = now();
    if user_activity {
        record.last_user_message_at = Some(record.updated_at.clone());
    }
    append_lifecycle(&mut journal, &record)?;
    Ok(Json(materialize(record, &journal)))
}

async fn transition_with_checkpoint(
    State(state): State<AppState>,
    id: &str,
    input: CheckpointSession,
    phase: &str,
) -> Result<Json<SessionRecord>, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, input.expected_version)?;
    record.state = control_state(&input.state);
    record.phase = phase.into();
    record.version += 1;
    record.updated_at = now();
    append_lifecycle(&mut journal, &record)?;
    Ok(Json(materialize(record, &journal)))
}

async fn transition(
    State(state): State<AppState>,
    id: &str,
    expected_version: i64,
    phase: &str,
    provenance_id: Option<String>,
) -> Result<Json<SessionRecord>, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, expected_version)?;
    record.phase = phase.into();
    record.provenance_id = provenance_id;
    record.version += 1;
    record.updated_at = now();
    append_lifecycle(&mut journal, &record)?;
    Ok(Json(materialize(record, &journal)))
}

async fn record_ingress_failure(
    State(state): State<AppState>,
    id: &str,
    input: RecordIngressFailure,
) -> Result<Json<SessionRecord>, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, input.expected_version)?;
    let mut failures = record
        .ingress_failures
        .as_array()
        .cloned()
        .unwrap_or_default();
    failures.push(json!({
        "at":now(),
        "stage":input.stage,
        "code":input.code,
        "message":input.message,
        "roundsUsed":input.rounds_used,
        "contextTokens":input.context_tokens,
        "contextWindowTokens":input.context_window_tokens,
    }));
    record.ingress_failures = Value::Array(failures);
    record.ingress_failure_count += 1;
    record.version += 1;
    record.updated_at = now();
    append_lifecycle(&mut journal, &record)?;
    Ok(Json(materialize(record, &journal)))
}

async fn retry_ingress(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<SessionRecord>, ApiError> {
    transition_with_checkpoint(
        State(state),
        &id,
        CheckpointSession {
            expected_version: input.expected_version,
            state: input.state,
            user_activity: false,
        },
        "ingress_pending",
    )
    .await
}

async fn complete_session(
    State(state): State<AppState>,
    id: &str,
    expected_version: i64,
    new_state: Value,
) -> Result<Json<SessionRecord>, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let path = state.config.directory.join(format!("{id}.chatend"));
    let journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, expected_version)?;
    record.state = control_state(&new_state);
    let object_id = record
        .state
        .get("sessionObjectId")
        .and_then(Value::as_str)
        .or_else(|| journal.state().completed_session_object.as_deref())
        .ok_or_else(|| {
            ApiError::conflict("completed session has no permanent Kweb session object")
        })?
        .to_owned();
    let _catalog_guard = state.catalog_mutation.lock().map_err(ApiError::internal)?;
    append_completed_id(&state.config.completed_list, &object_id).map_err(ApiError::internal)?;
    record.phase = "complete".into();
    record.version += 1;
    record.updated_at = now();
    record.ended_at = Some(record.updated_at.clone());
    record.state["sessionObjectId"] = json!(object_id);
    let output = materialize(record, &journal);
    drop(journal);
    std::fs::remove_file(&path)
        .with_context(|| format!("removing committed journal {}", path.display()))
        .map_err(ApiError::internal)?;
    Ok(Json(output))
}

async fn purge_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(expected_version): Json<Option<i64>>,
) -> Result<Json<Value>, ApiError> {
    if read_completed_ids(&state.config.completed_list)
        .map_err(ApiError::internal)?
        .contains(&id)
    {
        return Err(ApiError::conflict(
            "Committed Session History is permanent and cannot be purged.",
        ));
    }
    let record = fetch_active(&state, &id)?;
    if let Some(expected) = expected_version {
        require_version(&record, expected)?;
    }
    Err(ApiError::conflict(
        "An in-progress session must be ended and committed, not purged.",
    ))
}

fn fetch_active(state: &AppState, id: &str) -> Result<SessionRecord, ApiError> {
    let journal = open_by_id(state, id)?;
    latest_lifecycle(&journal).ok_or_else(ApiError::not_found)
}

fn session_mutation(state: &AppState, id: &str) -> Result<Arc<Mutex<()>>, ApiError> {
    let mut sessions = state.session_mutations.lock().map_err(ApiError::internal)?;
    sessions.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = sessions.get(id).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(Mutex::new(()));
    sessions.insert(id.to_owned(), Arc::downgrade(&lock));
    Ok(lock)
}

fn open_by_id(state: &AppState, id: &str) -> Result<SessionJournal, ApiError> {
    validate_session_id(id)?;
    let path = state.config.directory.join(format!("{id}.chatend"));
    SessionJournal::open(&path).map_err(|error| {
        if !path.exists() {
            ApiError::not_found()
        } else {
            ApiError::internal(error)
        }
    })
}

fn latest_lifecycle(journal: &SessionJournal) -> Option<SessionRecord> {
    journal
        .sidebands()
        .iter()
        .rev()
        .find(|record| record.kind == LIFECYCLE_SIDEBAND)
        .and_then(|record| serde_json::from_value(record.value.clone()).ok())
}

fn append_lifecycle(journal: &mut SessionJournal, record: &SessionRecord) -> Result<(), ApiError> {
    journal
        .append_sideband(
            LIFECYCLE_SIDEBAND,
            now(),
            serde_json::to_value(record).map_err(ApiError::internal)?,
        )
        .map_err(ApiError::internal)
}

fn commands(journal: &SessionJournal) -> BTreeMap<String, SessionCommand> {
    let mut commands = BTreeMap::new();
    for record in journal
        .sidebands()
        .iter()
        .filter(|record| record.kind == COMMAND_SIDEBAND)
    {
        if let Ok(command) = serde_json::from_value::<SessionCommand>(record.value.clone()) {
            commands.insert(command.id.clone(), command);
        }
    }
    commands
}

fn append_command(journal: &mut SessionJournal, command: &SessionCommand) -> Result<(), ApiError> {
    journal
        .append_sideband(
            COMMAND_SIDEBAND,
            now(),
            serde_json::to_value(command).map_err(ApiError::internal)?,
        )
        .map_err(ApiError::internal)
}

fn materialize(mut record: SessionRecord, journal: &SessionJournal) -> SessionRecord {
    record.state["sessionId"] = json!(journal.state().metadata.session_id);
    record.state["journalPath"] = json!(journal.path());
    record.state["transcript"] = Value::Array(
        journal
            .state()
            .boxes
            .values()
            .filter_map(|state| {
                let role = match state.owner {
                    BoxOwner::User => "user",
                    BoxOwner::Kennedy if state.name == "Kennedy message" => "kennedy",
                    _ => return None,
                };
                Some(json!({
                    "role":role,
                    "content":state.canonical.content.text,
                    "boxId":state.id.0,
                }))
            })
            .collect(),
    );
    record.state["boxes"] = serde_json::to_value(&journal.state().boxes).unwrap_or(Value::Null);
    record.state["events"] = serde_json::to_value(&journal.state().events).unwrap_or(Value::Null);
    record.state["context"] =
        serde_json::to_value(journal.state().projection()).unwrap_or(Value::Null);
    record.state["chatendText"] = json!(journal.state().render());
    record.state["sessionObjectId"] = journal
        .state()
        .completed_session_object
        .as_ref()
        .map_or(Value::Null, |id| json!(id));
    record
}

fn summary_state(control: &Value, journal: &SessionJournal) -> Value {
    let first_user = journal
        .state()
        .boxes
        .values()
        .find(|state| matches!(state.owner, BoxOwner::User))
        .map(|state| {
            state
                .canonical
                .content
                .text
                .chars()
                .take(512)
                .collect::<String>()
        });
    json!({
        "sessionType":control.get("sessionType"),
        "channel":control.get("channel"),
        "freeTime":control.get("freeTime"),
        "orchestration":control.get("orchestration"),
        "journalPath":journal.path(),
        "firstUserMessage":first_user,
        "boxCount":journal.state().boxes.len(),
        "eventCount":journal.state().events.len(),
        "pendingTurn":control.get("pendingTurn").cloned().unwrap_or(Value::Bool(false)),
    })
}

fn control_state(value: &Value) -> Value {
    const KEYS: &[&str] = &[
        "stateVersion",
        "sessionId",
        "journalPath",
        "sessionType",
        "sourceSessionType",
        "channel",
        "freeTime",
        "selfTimeIntent",
        "orchestration",
        "provenanceId",
        "rustLibSessionId",
        "rootNodeIds",
        "referenceRootNodeIds",
        "startedAt",
        "pendingTurn",
        "pendingExternalEventId",
        "roundsUsed",
        "completed",
        "sessionObjectId",
        "startIdempotencyId",
    ];
    let mut output = Map::new();
    for key in KEYS {
        if let Some(item) = value.get(*key) {
            output.insert((*key).into(), item.clone());
        }
    }
    Value::Object(output)
}

fn journal_paths(directory: &FilePath) -> Result<Vec<PathBuf>, ApiError> {
    let mut paths = std::fs::read_dir(directory)
        .map_err(ApiError::internal)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("chatend"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn read_completed_ids(path: &FilePath) -> anyhow::Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        let id = line.trim();
        if !id.is_empty() && seen.insert(id.to_owned()) {
            ids.push(id.to_owned());
        }
    }
    Ok(ids)
}

fn append_completed_id(path: &FilePath, id: &str) -> anyhow::Result<()> {
    if read_completed_ids(path)?
        .iter()
        .any(|existing| existing == id)
    {
        return Ok(());
    }
    let mut file = OpenOptions::new().append(true).open(path)?;
    writeln!(file, "{id}")?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn create_private_directory(path: &FilePath) -> anyhow::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .with_context(|| format!("creating {}", path.display()))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| FilePath::new("."));
    sync_directory(parent)
}

fn sync_directory(path: &FilePath) -> anyhow::Result<()> {
    File::open(path)
        .with_context(|| format!("opening directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing directory {}", path.display()))
}

fn ensure_inside(path: &FilePath, directory: &FilePath) -> Result<(), ApiError> {
    let parent = path
        .parent()
        .ok_or_else(|| ApiError::bad("invalid journal path"))?;
    let parent = std::fs::canonicalize(parent).map_err(ApiError::internal)?;
    let directory = std::fs::canonicalize(directory).map_err(ApiError::internal)?;
    if parent != directory {
        return Err(ApiError::bad(
            "session journal must be inside the configured in-progress directory",
        ));
    }
    Ok(())
}

fn validate_started_at(value: &str) -> Result<(), ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map(|_| ())
        .map_err(|_| ApiError::bad("started_at must be an RFC 3339 timestamp"))
}

fn validate_idempotency(value: &str) -> Result<(), ApiError> {
    if value.is_empty() || value.len() > 255 {
        return Err(ApiError::bad(
            "idempotency_id must contain between 1 and 255 bytes",
        ));
    }
    Ok(())
}

fn validate_session_id(value: &str) -> Result<(), ApiError> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| ApiError::bad("invalid session ID"))
}

fn require_version(record: &SessionRecord, expected: i64) -> Result<(), ApiError> {
    if record.version != expected {
        return Err(ApiError::conflict(format!(
            "Expected session version {expected}, found {}.",
            record.version
        )));
    }
    Ok(())
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "kennedy-session-history-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn service(label: &str) -> Service {
        let root = root(label);
        std::fs::create_dir_all(&root).unwrap();
        open(Config {
            directory: root.join("sessions"),
            completed_list: root.join("session-history.txt"),
            max_request_bytes: 1024 * 1024,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn managed_session_and_commands_live_in_one_journal() {
        let service = service("commands");
        let record = service
            .post_json(
                "/api/v1/conversations/start",
                json!({
                    "idempotency_id":"start-1",
                    "started_at":"2026-07-23T00:00:00Z",
                    "session_type":"conversation"
                }),
            )
            .await
            .unwrap();
        let id = record["id"].as_str().unwrap();
        let command = service
            .post_json(
                &format!("/api/v1/conversations/{id}/commands"),
                json!({"idempotency_id":"message-1","kind":"message","payload":{"text":"hello"}}),
            )
            .await
            .unwrap();
        assert_eq!(command["status"], "pending");
        let materialized = service
            .get_json(&format!("/api/v1/conversations/{id}"))
            .await
            .unwrap();
        assert!(
            materialized["state"]["chatendText"]
                .as_str()
                .is_some_and(|text| text.contains("[context budget"))
        );
        let listed = service
            .get_json("/api/v1/conversation-commands")
            .await
            .unwrap();
        assert_eq!(listed["commands"].as_array().unwrap().len(), 1);
        assert_eq!(
            std::fs::read_dir(&service.state.config.directory)
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn committed_history_is_only_an_object_id_list() {
        let service = service("completed");
        append_completed_id(&service.state.config.completed_list, "A1234567").unwrap();
        let listed = service
            .get_json("/api/v1/conversations/summaries")
            .await
            .unwrap();
        assert_eq!(
            listed["conversations"][0]["state"]["sessionObjectId"],
            "A1234567"
        );
        assert_eq!(
            std::fs::read_to_string(&service.state.config.completed_list).unwrap(),
            "A1234567\n"
        );
    }
}
