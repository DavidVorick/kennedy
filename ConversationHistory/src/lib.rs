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
    routing::{get, post, put},
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
struct StartIngress {
    expected_version: i64,
    provenance_id: String,
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
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::OPTIONS])
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
            "/api/v1/conversations/{conversation_id}",
            get(get_conversation),
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
        .layer(DefaultBodyLimit::max(config.max_request_bytes))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(address=%config.bind, "Kennedy conversation history listening");
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
    })
}

fn fetch_record(db: &Connection, id: &str) -> Result<ConversationRecord, ApiError> {
    db.query_row("SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at FROM conversations WHERE id=?1", [id], row_record)
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
    let mut statement = db.prepare("SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at FROM conversations ORDER BY updated_at DESC").map_err(ApiError::internal)?;
    let records = statement
        .query_map([], row_record)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"conversations":records})))
}

async fn current_conversation(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let record = db.query_row("SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at FROM conversations WHERE phase='active' ORDER BY updated_at DESC LIMIT 1", [], row_record).optional().map_err(ApiError::internal)?;
    Ok(Json(json!({"conversation":record})))
}

async fn next_ingress(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(json!({"conversation":fetch_next_ingress(&db)?})))
}

fn fetch_next_ingress(db: &Connection) -> Result<Option<ConversationRecord>, ApiError> {
    db.query_row(
        "SELECT id,phase,started_at,updated_at,state_json,provenance_id,version,last_user_message_at,ended_at FROM conversations WHERE phase='ingress_in_progress' OR phase='ingress_pending' ORDER BY CASE phase WHEN 'ingress_in_progress' THEN 0 ELSE 1 END, datetime(COALESCE(last_user_message_at,started_at)), datetime(started_at), id LIMIT 1",
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
        let pending_turn = serde_json::from_str::<Value>(&stale_state)
            .ok()
            .and_then(|value| value.get("pendingTurn").and_then(Value::as_bool))
            .unwrap_or(false);
        if !pending_turn {
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
        checkpoint_user_activity(&mut db, "current", 1, &json!({"pendingTurn":true})).unwrap();
        assert_eq!(fetch_record(&db, "stale").unwrap().phase, "ingress_pending");
        assert_eq!(fetch_record(&db, "pending").unwrap().phase, "active");
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
    fn legacy_single_conversation_database_migrates_in_place() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(INITIAL_MIGRATION).unwrap();
        db.execute("INSERT INTO conversations(id,phase,started_at,updated_at,state_json,version) VALUES('legacy','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','{}',1)", []).unwrap();
        apply_migrations(&db).unwrap();
        let record = fetch_record(&db, "legacy").unwrap();
        assert_eq!(record.phase, "active");
        assert!(record.last_user_message_at.is_none());
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
}
