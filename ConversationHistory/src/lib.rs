use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const MULTIPLE_LIVE_MIGRATION: &str =
    include_str!("../migrations/002_multiple_live_conversations.sql");
const INGRESS_FAILURES_MIGRATION: &str = include_str!("../migrations/003_ingress_failures.sql");
const INGRESS_FAILURE_LIMIT: i64 = 5;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: String,
    pub database: PathBuf,
    pub allowed_origins: Vec<String>,
    pub max_request_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
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
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error=%error, "conversation history request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected conversation database error occurred.",
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
}

#[derive(Deserialize)]
struct CreateConversation {
    started_at: String,
    state: Value,
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

pub async fn serve(config: Config) -> anyhow::Result<()> {
    let connection = Connection::open(&config.database)
        .with_context(|| format!("opening {}", config.database.display()))?;
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
    apply_migrations(&connection).context("applying conversation history migrations")?;
    let origins = config
        .allowed_origins
        .iter()
        .map(|origin| {
            origin
                .parse::<HeaderValue>()
                .with_context(|| format!("invalid allowed origin {origin}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([HeaderName::from_static("content-type")]);
    let state = AppState {
        db: Arc::new(Mutex::new(connection)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/api/v1/conversations",
            get(list_conversations).post(create_conversation),
        )
        .route("/api/v1/conversations/current", get(current_conversation))
        .route("/api/v1/conversations/ingress/next", get(next_ingress))
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
            "/api/v1/conversations/{conversation_id}/request-ingress",
            post(request_ingress),
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
        .layer(DefaultBodyLimit::max(config.max_request_bytes))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(address=%config.bind, "History ready");
    axum::serve(listener, app).await?;
    Ok(())
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
    // An early v2 build re-ran the v1 migration on every launch. That could recreate
    // this legacy singleton index after user_version had already advanced to 2, at
    // which point the normal v2 migration no longer ran. Repair that state
    // idempotently for every database opened by current builds.
    connection.execute_batch("DROP INDEX IF EXISTS one_unfinished_conversation;")?;
    Ok(())
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
    let state = serde_json::from_str(&state_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;
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
    })
}

fn fetch_record(db: &Connection, id: &str) -> Result<ConversationRecord, ApiError> {
    db.query_row("SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at,ingress_failure_count,ingress_failures_json FROM conversations WHERE id=?1", [id], row_record)
        .optional().map_err(ApiError::internal)?.ok_or_else(ApiError::not_found)
}

async fn create_conversation(
    State(state): State<AppState>,
    Json(input): Json<CreateConversation>,
) -> Result<(StatusCode, Json<ConversationRecord>), ApiError> {
    validate_started_at(&input.started_at)?;
    let state_json = serde_json::to_string(&input.state).map_err(ApiError::internal)?;
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().to_rfc3339();
    let db = state.db.lock().map_err(ApiError::internal)?;
    db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES(?1,'active',?2,?3,?4,1)", params![id,input.started_at,now,state_json]).map_err(ApiError::internal)?;
    let record = fetch_record(&db, &id)?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn list_conversations(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let mut statement = db.prepare("SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at,ingress_failure_count,ingress_failures_json FROM conversations ORDER BY updated_at DESC").map_err(ApiError::internal)?;
    let records = statement
        .query_map([], row_record)
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
    state.get("sessionType").and_then(Value::as_str) == Some("telegram")
        || state
            .pointer("/archive/sessionType")
            .and_then(Value::as_str)
            == Some("telegram")
}

fn discard_unstarted(db: &mut Connection) -> Result<Vec<String>, ApiError> {
    let tx = db.transaction().map_err(ApiError::internal)?;
    let mut statement = tx
        .prepare(
            "SELECT id,state_json FROM conversations WHERE last_user_message_at IS NULL ORDER BY id",
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
    for (id, state_json) in candidates {
        let state = serde_json::from_str::<Value>(&state_json).map_err(ApiError::internal)?;
        if state_contains_user_message(&state) || is_telegram_session(&state) {
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
    let record = db.query_row("SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at,ingress_failure_count,ingress_failures_json FROM conversations WHERE phase='active' ORDER BY updated_at DESC LIMIT 1", [], row_record).optional().map_err(ApiError::internal)?;
    Ok(Json(json!({"conversation":record})))
}

async fn next_ingress(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(json!({"conversation":fetch_next_ingress(&db)?})))
}

fn fetch_next_ingress(db: &Connection) -> Result<Option<ConversationRecord>, ApiError> {
    db.query_row(
        "SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at,ingress_failure_count,ingress_failures_json FROM conversations WHERE phase='ingress_in_progress' OR phase='ingress_pending' ORDER BY CASE phase WHEN 'ingress_in_progress' THEN 0 ELSE 1 END, datetime(COALESCE(last_user_message_at,started_at)), datetime(started_at), id LIMIT 1",
        [],
        row_record,
    ).optional().map_err(ApiError::internal)
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
    let state_json = serde_json::to_string(state).map_err(ApiError::internal)?;
    let now = Utc::now().to_rfc3339();
    let ended_at = (phase == "ingress_pending").then_some(now.as_str());
    let changed = db.execute("UPDATE conversations SET state_json=?1,phase=?2,updated_at=?3,ended_at=COALESCE(?4,ended_at),version=version+1 WHERE id=?5 AND phase='active' AND version=?6", params![state_json,phase,now,ended_at,id,expected_version]).map_err(ApiError::internal)?;
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
    id: &str,
    expected_version: i64,
    state: &Value,
) -> Result<ConversationRecord, ApiError> {
    validate_version(expected_version)?;
    let state_json = serde_json::to_string(state).map_err(ApiError::internal)?;
    let now = Utc::now();
    let now_text = now.to_rfc3339();
    let cutoff = (now - ChronoDuration::hours(24)).to_rfc3339();
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx.execute(
        "UPDATE conversations SET state_json=?1,updated_at=?2,last_user_message_at=?2,version=version+1 WHERE id=?3 AND phase='active' AND version=?4",
        params![state_json,now_text,id,expected_version],
    ).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation changed in another session or is no longer active.",
        ));
    }

    let mut statement = tx.prepare(
        "SELECT id,state_json FROM conversations WHERE phase='active' AND id<>?1 AND datetime(COALESCE(last_user_message_at,started_at))<datetime(?2)",
    ).map_err(ApiError::internal)?;
    let candidates = statement
        .query_map(params![id, cutoff], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(statement);
    for (stale_id, stale_state) in candidates {
        let state = serde_json::from_str::<Value>(&stale_state).unwrap_or(Value::Null);
        let pending_turn = state
            .get("pendingTurn")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !pending_turn && !is_telegram_session(&state) {
            tx.execute(
                "UPDATE conversations SET phase='ingress_pending',updated_at=?1,ended_at=?1,version=version+1 WHERE id=?2 AND phase='active'",
                params![now_text,stale_id],
            ).map_err(ApiError::internal)?;
        }
    }
    tx.commit().map_err(ApiError::internal)?;
    fetch_record(db, id)
}

async fn request_ingress(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<CheckpointConversation>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(update_active(
        &db,
        &id,
        input.expected_version,
        &input.state,
        "ingress_pending",
    )?))
}

async fn ingress_started(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<StartIngress>,
) -> Result<Json<ConversationRecord>, ApiError> {
    validate_version(input.expected_version)?;
    if input.provenance_id.trim().is_empty() {
        return Err(ApiError::bad("provenance_id must not be empty."));
    }
    let db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_record(&db, &id)?;
    if existing.phase == "ingress_in_progress"
        && existing.provenance_id.as_deref() == Some(input.provenance_id.as_str())
    {
        return Ok(Json(existing));
    }
    let claimed_by: Option<String> = db
        .query_row(
            "SELECT id FROM conversations WHERE phase='ingress_in_progress' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if claimed_by.as_deref().is_some_and(|claimed| claimed != id) {
        return Err(ApiError::conflict(
            "Another conversation is already undergoing history ingress.",
        ));
    }
    let changed = db.execute("UPDATE conversations SET phase='ingress_in_progress',provenance_id=?1,updated_at=?2,version=version+1 WHERE id=?3 AND phase='ingress_pending' AND version=?4", params![input.provenance_id,Utc::now().to_rfc3339(),id,input.expected_version]).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation is not ready to start history ingress.",
        ));
    }
    Ok(Json(fetch_record(&db, &id)?))
}

async fn checkpoint_ingress(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<CheckpointConversation>,
) -> Result<Json<ConversationRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(update_ingress(
        &db,
        &id,
        input.expected_version,
        &input.state,
    )?))
}

fn update_ingress(
    db: &Connection,
    id: &str,
    expected_version: i64,
    state: &Value,
) -> Result<ConversationRecord, ApiError> {
    validate_version(expected_version)?;
    let state_json = serde_json::to_string(state).map_err(ApiError::internal)?;
    let changed = db.execute(
        "UPDATE conversations SET state_json=?1,updated_at=?2,version=version+1 WHERE id=?3 AND phase='ingress_in_progress' AND version=?4",
        params![state_json,Utc::now().to_rfc3339(),id,expected_version],
    ).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation ingress changed in another session or is no longer in progress.",
        ));
    }
    fetch_record(db, id)
}

fn concise_failure_text(value: &str, limit: usize, fallback: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let bounded = normalized.chars().take(limit).collect::<String>();
    if bounded.is_empty() {
        fallback.to_owned()
    } else {
        bounded
    }
}

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
    let terminal = attempt >= INGRESS_FAILURE_LIMIT;
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
        existing.phase.as_str()
    };
    let now = Utc::now().to_rfc3339();
    let changed = tx
        .execute(
            "UPDATE conversations SET phase=?1,updated_at=?2,ingress_failure_count=?3,ingress_failures_json=?4,version=version+1 WHERE id=?5 AND phase IN ('ingress_pending','ingress_in_progress') AND version=?6",
            params![next_phase, now, attempt, failures_json, id, input.expected_version],
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
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(record_ingress_failure(&mut db, &id, &input)?))
}

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
    let state_json = serde_json::to_string(&input.state).map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx
        .execute(
            "UPDATE conversations SET phase='ingress_pending',state_json=?1,updated_at=?2,ingress_failure_count=0,version=version+1 WHERE id=?3 AND phase='ingress_failed' AND version=?4",
            params![state_json, Utc::now().to_rfc3339(), id, input.expected_version],
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
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(retry_failed_ingress(&mut db, &id, &input)?))
}

async fn ingress_completed(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<VersionedTransition>,
) -> Result<Json<ConversationRecord>, ApiError> {
    validate_version(input.expected_version)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_record(&db, &id)?;
    if existing.phase == "complete" {
        return Ok(Json(existing));
    }
    let changed = db.execute("UPDATE conversations SET phase='complete',updated_at=?1,version=version+1 WHERE id=?2 AND phase='ingress_in_progress' AND version=?3", params![Utc::now().to_rfc3339(),id,input.expected_version]).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Conversation is not in the expected ingress state.",
        ));
    }
    Ok(Json(fetch_record(&db, &id)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        apply_migrations(&db).unwrap();
        db
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
    fn user_activity_expires_only_idle_conversations_without_pending_turns() {
        let mut db = database();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('current','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1,'2026-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('stale','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"pendingTurn\":false}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('pending','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"pendingTurn\":true}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version,last_user_message_at) VALUES('telegram','active','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','{\"sessionType\":\"telegram\",\"pendingTurn\":false}',1,'2020-01-01T00:00:00Z')", []).unwrap();
        checkpoint_user_activity(&mut db, "current", 1, &json!({"pendingTurn":true})).unwrap();
        assert_eq!(fetch_record(&db, "stale").unwrap().phase, "ingress_pending");
        assert_eq!(fetch_record(&db, "pending").unwrap().phase, "active");
        assert_eq!(fetch_record(&db, "telegram").unwrap().phase, "active");
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
            3
        );
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('new','active','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z','{}',1)", []).unwrap();
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
        assert_eq!(record.state, state);
        assert_eq!(
            update_ingress(&db, "c", 4, &json!({})).unwrap_err().code,
            "state_conflict"
        );
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
                assert_eq!(record.phase, "ingress_in_progress");
                assert_eq!(fetch_next_ingress(&db).unwrap().unwrap().id, "poisoned");
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
        assert_eq!(retried.state.get("historyIngress"), None);
        assert_eq!(retried.ingress_failures[0]["message"], "context exhausted");
    }
}
