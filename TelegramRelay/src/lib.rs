use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderName, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use futures::StreamExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use teloxide::{
    net::Download,
    payloads::SendMessageSetters,
    prelude::*,
    requests::Request,
    types::{AllowedUpdate, ChatMemberKind, Message, MessageEntityKind, Update, UpdateKind},
};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const UPDATE_ORDER_MIGRATION: &str = include_str!("../migrations/002_update_order.sql");
const USERS_MIGRATION: &str = include_str!("../migrations/003_users.sql");
const GROUP_EVENTS_MIGRATION: &str = include_str!("../migrations/003_group_events.sql");
const UNAUTHORIZED_MESSAGE: &str =
    "Sorry, this Kennedy bot is private and your Telegram handle is not whitelisted.";
const TELEGRAM_MESSAGE_LIMIT: usize = 4_000;
const TELEGRAM_POLL_TIMEOUT_SECONDS: u32 = 30;
const TELEGRAM_HTTP_TIMEOUT_SECONDS: u64 = 40;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: String,
    pub database: PathBuf,
    pub user_database: PathBuf,
    pub allowed_origins: Vec<String>,
    pub bot_token: Option<String>,
    pub bootstrap_usernames: Vec<String>,
    pub max_voice_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    users: Arc<Mutex<Connection>>,
    bot: Option<Bot>,
    max_voice_bytes: usize,
    bot_user_id: Option<i64>,
    bot_username: Option<String>,
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
            "Telegram event not found.",
        )
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "state_conflict", message)
    }

    fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "telegram_unavailable",
            "The Telegram bot token is not configured.",
        )
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error=%error, "telegram relay request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected Telegram relay error occurred.",
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
#[serde(rename_all = "camelCase")]
struct RelayEvent {
    id: String,
    message_id: i64,
    telegram_user_id: i64,
    chat_id: i64,
    username: Option<String>,
    display_name: String,
    kind: String,
    text: Option<String>,
    mime_type: Option<String>,
    file_name: Option<String>,
    duration_seconds: Option<i64>,
    status: String,
    conversation_id: Option<String>,
    transcription: Option<String>,
    transcription_model: Option<String>,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_root_node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_root_node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_context: Option<Value>,
    session_kind: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DirectoryUser {
    handle: String,
    telegram_user_id: Option<i64>,
    current_username: Option<String>,
    display_name: Option<String>,
    root_node_id: String,
    root_ready: bool,
    can_add_users: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DirectoryGroup {
    chat_id: i64,
    title: String,
    root_node_id: String,
    root_ready: bool,
    state: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompleteRoot {
    root_node_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BindEvent {
    conversation_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SaveTranscription {
    text: String,
    transcription_model: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplyEvent {
    conversation_id: String,
    text: String,
    context_warning: Option<String>,
}

#[derive(Deserialize)]
struct CompleteReset {
    message: Option<String>,
}

pub async fn serve(config: Config) -> anyhow::Result<()> {
    if config.max_voice_bytes == 0 {
        anyhow::bail!("telegram max_voice_bytes must be greater than zero");
    }
    let connection = Connection::open(&config.database)
        .with_context(|| format!("opening {}", config.database.display()))?;
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
    apply_migrations(&connection).context("applying Telegram relay migrations")?;
    let user_connection = Connection::open(&config.user_database)
        .with_context(|| format!("opening {}", config.user_database.display()))?;
    user_connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;",
    )?;
    apply_user_migrations(&user_connection).context("applying user-directory migrations")?;
    seed_bootstrap_users(&connection, &user_connection, &config.bootstrap_usernames)?;

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
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([HeaderName::from_static("content-type")]);
    // Teloxide's default 17-second HTTP timeout is shorter than Kennedy's
    // 30-second Telegram long poll. A quiet, healthy bot would therefore time
    // out locally before Telegram completed the request.
    let bot = match config.bot_token.as_ref() {
        Some(token) => {
            let client = teloxide::net::default_reqwest_settings()
                .timeout(Duration::from_secs(TELEGRAM_HTTP_TIMEOUT_SECONDS))
                .build()
                .context("building Telegram HTTP client")?;
            Some(Bot::with_client(token, client))
        }
        None => None,
    };
    let (bot_user_id, bot_username) = if let Some(bot) = bot.as_ref() {
        let me = bot
            .get_me()
            .send()
            .await
            .context("validating Telegram bot token")?;
        (
            Some(i64::try_from(me.id.0).context("Telegram bot ID exceeds SQLite range")?),
            me.username.clone(),
        )
    } else {
        (None, None)
    };
    let state = AppState {
        db: Arc::new(Mutex::new(connection)),
        users: Arc::new(Mutex::new(user_connection)),
        bot: bot.clone(),
        max_voice_bytes: config.max_voice_bytes,
        bot_user_id,
        bot_username,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/events", get(list_events))
        .route("/api/v1/events/{event_id}/media", get(event_media))
        .route("/api/v1/events/{event_id}/bind", post(bind_event))
        .route(
            "/api/v1/events/{event_id}/transcription",
            post(save_transcription),
        )
        .route("/api/v1/events/{event_id}/reply", post(reply_event))
        .route(
            "/api/v1/events/{event_id}/reset-completed",
            post(complete_reset),
        )
        .route("/api/v1/users/provisioning", get(list_provisioning_users))
        .route("/api/v1/users/by-handle/{handle}", get(user_by_handle))
        .route(
            "/api/v1/users/by-handle/{handle}/root-ready",
            post(complete_handle_root),
        )
        .route("/api/v1/users/{telegram_user_id}", get(user_by_id))
        .route(
            "/api/v1/users/{telegram_user_id}/root-ready",
            post(complete_user_root),
        )
        .route("/api/v1/groups/provisioning", get(list_provisioning_groups))
        .route("/api/v1/groups/{chat_id}", get(group_by_id))
        .route(
            "/api/v1/groups/{chat_id}/root-ready",
            post(complete_group_root),
        )
        .route("/api/v1/group-ingress", get(list_group_ingress))
        .route(
            "/api/v1/group-ingress/{batch_id}/complete",
            post(complete_group_ingress),
        )
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(address=%config.bind, enabled=bot.is_some(), "Telegram ready");

    if let Some(bot) = bot {
        tokio::try_join!(
            async {
                axum::serve(listener, app)
                    .await
                    .context("serving Telegram relay API")
            },
            async { poll_telegram(bot, state).await.context("polling Telegram") },
        )?;
    } else {
        axum::serve(listener, app).await?;
    }
    Ok(())
}

fn apply_migrations(db: &Connection) -> anyhow::Result<()> {
    db.execute_batch(INITIAL_MIGRATION)?;
    db.execute_batch(UPDATE_ORDER_MIGRATION)?;
    migrate_document_events(db)?;
    db.execute_batch(GROUP_EVENTS_MIGRATION)?;
    migrate_event_context(db)?;
    Ok(())
}

fn apply_user_migrations(db: &Connection) -> anyhow::Result<()> {
    db.execute_batch(USERS_MIGRATION)?;
    let columns = db
        .prepare("PRAGMA table_info(telegram_groups)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|name| name == "root_node_id") {
        db.execute_batch("ALTER TABLE telegram_groups ADD COLUMN root_node_id TEXT;")?;
    }
    if !columns.iter().any(|name| name == "root_ready") {
        db.execute_batch(
            "ALTER TABLE telegram_groups ADD COLUMN root_ready INTEGER NOT NULL DEFAULT 0 CHECK(root_ready IN (0, 1));",
        )?;
    }
    let missing = db
        .prepare(
            "SELECT chat_id FROM telegram_groups WHERE root_node_id IS NULL OR length(root_node_id)<>40 ORDER BY chat_id",
        )?
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for chat_id in missing {
        let root_node_id = random_unassigned_node_id(db)?;
        db.execute(
            "UPDATE telegram_groups SET root_node_id=?1,root_ready=0 WHERE chat_id=?2",
            params![root_node_id, chat_id],
        )?;
    }
    db.execute_batch("PRAGMA user_version=2;")?;
    Ok(())
}

fn migrate_event_context(db: &Connection) -> anyhow::Result<()> {
    let columns = db
        .prepare("PRAGMA table_info(telegram_events)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|name| name == "session_kind") {
        db.execute_batch(
            "ALTER TABLE telegram_events ADD COLUMN session_kind TEXT NOT NULL DEFAULT 'private';",
        )?;
    }
    if !columns.iter().any(|name| name == "group_context_json") {
        db.execute_batch("ALTER TABLE telegram_events ADD COLUMN group_context_json TEXT;")?;
    }
    Ok(())
}

fn migrate_document_events(db: &Connection) -> anyhow::Result<()> {
    let schema = db.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='telegram_events'",
        [],
        |row| row.get::<_, String>(0),
    )?;
    let mut columns = db.prepare("PRAGMA table_info(telegram_events)")?;
    let has_file_name = columns
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|name| name == "file_name");
    if schema.contains("'document'") && has_file_name {
        return Ok(());
    }
    let file_name_source = if has_file_name { "file_name" } else { "NULL" };
    db.execute_batch(&format!(
        r#"
        BEGIN IMMEDIATE;
        DROP INDEX IF EXISTS telegram_events_work_queue;
        DROP INDEX IF EXISTS telegram_events_user_queue;
        CREATE TABLE telegram_events_new (
            id TEXT PRIMARY KEY,
            update_id INTEGER NOT NULL UNIQUE,
            message_id INTEGER NOT NULL,
            telegram_user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            username TEXT,
            display_name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('text', 'voice', 'document', 'reset')),
            text TEXT,
            voice_bytes BLOB,
            mime_type TEXT,
            file_name TEXT,
            duration_seconds INTEGER,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'complete')),
            conversation_id TEXT,
            transcription TEXT,
            transcription_model TEXT,
            created_at TEXT NOT NULL,
            completed_at TEXT
        );
        INSERT INTO telegram_events_new (
            id,update_id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,
            voice_bytes,mime_type,file_name,duration_seconds,status,conversation_id,transcription,
            transcription_model,created_at,completed_at
        )
        SELECT
            id,update_id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,
            voice_bytes,mime_type,{file_name_source},duration_seconds,status,conversation_id,
            transcription,transcription_model,created_at,completed_at
        FROM telegram_events;
        DROP TABLE telegram_events;
        ALTER TABLE telegram_events_new RENAME TO telegram_events;
        CREATE INDEX telegram_events_work_queue ON telegram_events(status, update_id);
        CREATE INDEX telegram_events_user_queue ON telegram_events(telegram_user_id, status, update_id);
        COMMIT;
        "#
    ))?;
    Ok(())
}

fn normalize_username(value: &str) -> String {
    value.trim().trim_start_matches('@').to_ascii_lowercase()
}

fn random_node_id() -> String {
    hex::encode(rand::random::<[u8; 20]>())
}

fn random_unassigned_node_id(db: &Connection) -> anyhow::Result<String> {
    loop {
        let candidate = random_node_id();
        let assigned = db.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM whitelist_entries WHERE root_node_id=?1
                 UNION ALL
                 SELECT 1 FROM telegram_groups WHERE root_node_id=?1
             )",
            [&candidate],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if !assigned {
            return Ok(candidate);
        }
    }
}

fn seed_bootstrap_users(
    relay: &Connection,
    users: &Connection,
    usernames: &[String],
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    for username in usernames {
        let normalized = normalize_username(username);
        if normalized.is_empty() {
            anyhow::bail!("telegram bootstrap usernames must not be empty");
        }
        // Import a binding from pre-directory Kennedy installations when one
        // exists, but do not seed handles into the transport database. The
        // user directory is the sole identity authority going forward.
        let legacy = relay
            .query_row(
                "SELECT telegram_user_id,username,display_name FROM authorized_users WHERE bootstrap_username=?1",
                [&normalized],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .unwrap_or((None, None, None));
        let root_node_id = random_unassigned_node_id(users)?;
        users.execute(
            "INSERT INTO whitelist_entries(handle,telegram_user_id,current_username,display_name,root_node_id,can_add_users,whitelisted_at,resolved_at,updated_at)
             VALUES(?1,?2,?3,?4,?5,1,?6,CASE WHEN ?2 IS NULL THEN NULL ELSE ?6 END,?6)
             ON CONFLICT(handle) DO UPDATE SET can_add_users=1,
                 telegram_user_id=COALESCE(whitelist_entries.telegram_user_id,excluded.telegram_user_id),
                 current_username=COALESCE(excluded.current_username,whitelist_entries.current_username),
                 display_name=COALESCE(excluded.display_name,whitelist_entries.display_name),updated_at=excluded.updated_at",
            params![normalized, legacy.0, legacy.1, legacy.2, root_node_id, now],
        )?;
    }
    Ok(())
}

fn directory_user_by_clause(
    db: &Connection,
    clause: &str,
    value: &dyn rusqlite::ToSql,
) -> Result<Option<DirectoryUser>, rusqlite::Error> {
    db.query_row(
        &format!(
            "SELECT handle,telegram_user_id,current_username,display_name,root_node_id,root_ready,can_add_users FROM whitelist_entries WHERE {clause}"
        ),
        [value],
        |row| {
            Ok(DirectoryUser {
                handle: row.get(0)?,
                telegram_user_id: row.get(1)?,
                current_username: row.get(2)?,
                display_name: row.get(3)?,
                root_node_id: row.get(4)?,
                root_ready: row.get::<_, i64>(5)? != 0,
                can_add_users: row.get::<_, i64>(6)? != 0,
            })
        },
    )
    .optional()
}

fn directory_user_by_id(db: &Connection, id: i64) -> anyhow::Result<Option<DirectoryUser>> {
    Ok(directory_user_by_clause(db, "telegram_user_id=?1", &id)?)
}

fn directory_user_by_handle(
    db: &Connection,
    handle: &str,
) -> anyhow::Result<Option<DirectoryUser>> {
    Ok(directory_user_by_clause(db, "handle=?1", &handle)?)
}

fn directory_group_by_id(db: &Connection, chat_id: i64) -> anyhow::Result<Option<DirectoryGroup>> {
    Ok(db
        .query_row(
            "SELECT chat_id,title,root_node_id,root_ready,state FROM telegram_groups WHERE chat_id=?1",
            [chat_id],
            |row| {
                Ok(DirectoryGroup {
                    chat_id: row.get(0)?,
                    title: row.get(1)?,
                    root_node_id: row.get(2)?,
                    root_ready: row.get::<_, i64>(3)? != 0,
                    state: row.get(4)?,
                })
            },
        )
        .optional()?)
}

#[derive(Debug, PartialEq, Eq)]
enum TofuOutcome {
    Whitelisted,
    UnresolvedHandleClaimed,
    HandleConflict,
    NotWhitelisted,
}

fn observe_identity(
    db: &Connection,
    telegram_user_id: i64,
    username: Option<&str>,
    display_name: &str,
) -> anyhow::Result<TofuOutcome> {
    let now = Utc::now().to_rfc3339();
    let normalized = username
        .map(normalize_username)
        .filter(|value| !value.is_empty());
    let first_seen = db
        .query_row(
            "SELECT 1 FROM observed_identities WHERE telegram_user_id=?1",
            [telegram_user_id],
            |_| Ok(()),
        )
        .optional()?
        .is_none();
    db.execute(
        "INSERT INTO observed_identities(telegram_user_id,current_username,display_name,first_seen_at,last_seen_at)
         VALUES(?1,?2,?3,?4,?4)
         ON CONFLICT(telegram_user_id) DO UPDATE SET current_username=excluded.current_username,display_name=excluded.display_name,last_seen_at=excluded.last_seen_at",
        params![telegram_user_id, normalized, display_name, now],
    )?;
    if first_seen {
        tracing::info!(telegram_user_id, username=?username, display_name, "Observed Telegram identity for the first time");
    }
    if directory_user_by_id(db, telegram_user_id)?.is_some() {
        db.execute(
            "UPDATE whitelist_entries SET current_username=?1,display_name=?2,updated_at=?3 WHERE telegram_user_id=?4",
            params![normalized, display_name, now, telegram_user_id],
        )?;
        return Ok(TofuOutcome::Whitelisted);
    }
    let Some(handle) = normalized else {
        return Ok(TofuOutcome::NotWhitelisted);
    };
    let Some(entry) = directory_user_by_handle(db, &handle)? else {
        return Ok(TofuOutcome::NotWhitelisted);
    };
    if let Some(expected) = entry.telegram_user_id {
        tracing::warn!(
            handle,
            expected_telegram_user_id = expected,
            observed_telegram_user_id = telegram_user_id,
            "Telegram TOFU handle conflict"
        );
        return Ok(TofuOutcome::HandleConflict);
    }
    let changed = db.execute(
        "UPDATE whitelist_entries SET telegram_user_id=?1,current_username=?2,display_name=?3,resolved_at=?4,updated_at=?4 WHERE handle=?2 AND telegram_user_id IS NULL",
        params![telegram_user_id, handle, display_name, now],
    )?;
    Ok(if changed == 1 {
        tracing::info!(
            handle,
            telegram_user_id,
            "Resolved whitelisted Telegram handle by TOFU"
        );
        TofuOutcome::UnresolvedHandleClaimed
    } else {
        TofuOutcome::HandleConflict
    })
}

fn whitelist_handle(db: &Connection, handle: &str, added_by: i64) -> anyhow::Result<DirectoryUser> {
    let handle = normalize_username(handle.trim_matches(['\'', '"']));
    anyhow::ensure!(!handle.is_empty(), "the Telegram handle must not be empty");
    let now = Utc::now().to_rfc3339();
    let root_node_id = random_unassigned_node_id(db)?;
    db.execute(
        "INSERT INTO whitelist_entries(handle,current_username,root_node_id,added_by_telegram_user_id,whitelisted_at,updated_at)
         VALUES(?1,?1,?2,?3,?4,?4)
         ON CONFLICT(handle) DO UPDATE SET updated_at=excluded.updated_at",
        params![handle, root_node_id, added_by, now],
    )?;
    directory_user_by_handle(db, &handle)?.context("reading the whitelisted Telegram handle")
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "service":"telegram-relay",
        "status":"ok",
        "telegram": if state.bot.is_some() { "ready" } else { "disabled" },
    }))
}

async fn list_provisioning_users(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.users.lock().map_err(ApiError::internal)?;
    let mut statement = db
        .prepare("SELECT handle,telegram_user_id,current_username,display_name,root_node_id,root_ready,can_add_users FROM whitelist_entries WHERE root_ready=0 ORDER BY whitelisted_at,handle")
        .map_err(ApiError::internal)?;
    let users = statement
        .query_map([], |row| {
            Ok(DirectoryUser {
                handle: row.get(0)?,
                telegram_user_id: row.get(1)?,
                current_username: row.get(2)?,
                display_name: row.get(3)?,
                root_node_id: row.get(4)?,
                root_ready: row.get::<_, i64>(5)? != 0,
                can_add_users: row.get::<_, i64>(6)? != 0,
            })
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"users":users})))
}

async fn user_by_handle(
    State(state): State<AppState>,
    Path(handle): Path<String>,
) -> Result<Json<DirectoryUser>, ApiError> {
    let handle = normalize_username(&handle);
    let db = state.users.lock().map_err(ApiError::internal)?;
    directory_user_by_handle(&db, &handle)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn user_by_id(
    State(state): State<AppState>,
    Path(telegram_user_id): Path<i64>,
) -> Result<Json<DirectoryUser>, ApiError> {
    let db = state.users.lock().map_err(ApiError::internal)?;
    directory_user_by_id(&db, telegram_user_id)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn complete_user_root(
    State(state): State<AppState>,
    Path(telegram_user_id): Path<i64>,
    Json(input): Json<CompleteRoot>,
) -> Result<Json<DirectoryUser>, ApiError> {
    if input.root_node_id.len() != 40 || hex::decode(&input.root_node_id).is_err() {
        return Err(ApiError::bad(
            "rootNodeId must be a 40-character hexadecimal Kmap identifier.",
        ));
    }
    let db = state.users.lock().map_err(ApiError::internal)?;
    let current = directory_user_by_id(&db, telegram_user_id)
        .map_err(ApiError::internal)?
        .ok_or_else(ApiError::not_found)?;
    if current.root_ready && current.root_node_id != input.root_node_id {
        return Err(ApiError::conflict(
            "This Telegram identity already has a different root node.",
        ));
    }
    db.execute(
        "UPDATE whitelist_entries SET root_node_id=?1,root_ready=1,updated_at=?2 WHERE telegram_user_id=?3",
        params![input.root_node_id, Utc::now().to_rfc3339(), telegram_user_id],
    )
    .map_err(ApiError::internal)?;
    Ok(Json(
        directory_user_by_id(&db, telegram_user_id)
            .map_err(ApiError::internal)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

async fn complete_handle_root(
    State(state): State<AppState>,
    Path(handle): Path<String>,
    Json(input): Json<CompleteRoot>,
) -> Result<Json<DirectoryUser>, ApiError> {
    if input.root_node_id.len() != 40 || hex::decode(&input.root_node_id).is_err() {
        return Err(ApiError::bad(
            "rootNodeId must be a 40-character hexadecimal Kmap identifier.",
        ));
    }
    let handle = normalize_username(&handle);
    let db = state.users.lock().map_err(ApiError::internal)?;
    let current = directory_user_by_handle(&db, &handle)
        .map_err(ApiError::internal)?
        .ok_or_else(ApiError::not_found)?;
    if current.root_ready && current.root_node_id != input.root_node_id {
        return Err(ApiError::conflict(
            "This whitelisted handle already has a different root node.",
        ));
    }
    db.execute(
        "UPDATE whitelist_entries SET root_node_id=?1,root_ready=1,updated_at=?2 WHERE handle=?3",
        params![input.root_node_id, Utc::now().to_rfc3339(), handle],
    )
    .map_err(ApiError::internal)?;
    Ok(Json(
        directory_user_by_handle(&db, &handle)
            .map_err(ApiError::internal)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

async fn list_provisioning_groups(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.users.lock().map_err(ApiError::internal)?;
    let mut statement = db
        .prepare(
            "SELECT chat_id,title,root_node_id,root_ready,state FROM telegram_groups WHERE root_ready=0 ORDER BY datetime(created_at),chat_id",
        )
        .map_err(ApiError::internal)?;
    let groups = statement
        .query_map([], |row| {
            Ok(DirectoryGroup {
                chat_id: row.get(0)?,
                title: row.get(1)?,
                root_node_id: row.get(2)?,
                root_ready: row.get::<_, i64>(3)? != 0,
                state: row.get(4)?,
            })
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"groups":groups})))
}

async fn group_by_id(
    State(state): State<AppState>,
    Path(chat_id): Path<i64>,
) -> Result<Json<DirectoryGroup>, ApiError> {
    let db = state.users.lock().map_err(ApiError::internal)?;
    directory_group_by_id(&db, chat_id)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn complete_group_root(
    State(state): State<AppState>,
    Path(chat_id): Path<i64>,
    Json(input): Json<CompleteRoot>,
) -> Result<Json<DirectoryGroup>, ApiError> {
    if input.root_node_id.len() != 40 || hex::decode(&input.root_node_id).is_err() {
        return Err(ApiError::bad(
            "rootNodeId must be a 40-character hexadecimal Kmap identifier.",
        ));
    }
    let db = state.users.lock().map_err(ApiError::internal)?;
    let current = directory_group_by_id(&db, chat_id)
        .map_err(ApiError::internal)?
        .ok_or_else(ApiError::not_found)?;
    if current.root_node_id != input.root_node_id {
        return Err(ApiError::conflict(
            "This Telegram group has a different reserved root node.",
        ));
    }
    db.execute(
        "UPDATE telegram_groups SET root_ready=1,updated_at=?1 WHERE chat_id=?2",
        params![Utc::now().to_rfc3339(), chat_id],
    )
    .map_err(ApiError::internal)?;
    Ok(Json(
        directory_group_by_id(&db, chat_id)
            .map_err(ApiError::internal)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

async fn list_group_ingress(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let mut batches = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        let mut statement = db.prepare(
            "SELECT id,chat_id,first_message_id,last_message_id,messages_json,participants_json,created_at FROM telegram_group_ingress WHERE status IN ('pending','processing') ORDER BY datetime(created_at),id",
        ).map_err(ApiError::internal)?;
        statement.query_map([], |row| {
            let messages: String = row.get(4)?;
            let participants: String = row.get(5)?;
            Ok(json!({
                "id":row.get::<_,String>(0)?, "chatId":row.get::<_,i64>(1)?,
                "firstMessageId":row.get::<_,i64>(2)?, "lastMessageId":row.get::<_,i64>(3)?,
                "messages":serde_json::from_str::<Value>(&messages).unwrap_or(Value::Array(vec![])),
                "participants":serde_json::from_str::<Value>(&participants).unwrap_or(Value::Array(vec![])),
                "createdAt":row.get::<_,String>(6)?,
            }))
        }).map_err(ApiError::internal)?.collect::<Result<Vec<_>,_>>().map_err(ApiError::internal)?
    };
    let users = state.users.lock().map_err(ApiError::internal)?;
    for batch in &mut batches {
        let Some(chat_id) = batch["chatId"].as_i64() else {
            continue;
        };
        if let Some(group) = directory_group_by_id(&users, chat_id).map_err(ApiError::internal)? {
            batch["groupTitle"] = Value::String(group.title);
            batch["groupRootNodeId"] = Value::String(group.root_node_id);
            batch["groupRootReady"] = Value::Bool(group.root_ready);
        }
    }
    Ok(Json(json!({"batches":batches})))
}

async fn complete_group_ingress(
    State(state): State<AppState>,
    Path(batch_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let changed = db.execute(
        "UPDATE telegram_group_ingress SET status='complete',completed_at=?1 WHERE id=?2 AND status<>'complete'",
        params![Utc::now().to_rfc3339(), batch_id],
    ).map_err(ApiError::internal)?;
    if changed == 0 {
        let exists = db
            .query_row(
                "SELECT 1 FROM telegram_group_ingress WHERE id=?1",
                [&batch_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(ApiError::internal)?
            .is_some();
        if !exists {
            return Err(ApiError::not_found());
        }
    }
    Ok(Json(json!({"id":batch_id,"status":"complete"})))
}

fn row_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<RelayEvent> {
    Ok(RelayEvent {
        id: row.get(0)?,
        message_id: row.get(1)?,
        telegram_user_id: row.get(2)?,
        chat_id: row.get(3)?,
        username: row.get(4)?,
        display_name: row.get(5)?,
        kind: row.get(6)?,
        text: row.get(7)?,
        mime_type: row.get(8)?,
        file_name: row.get(9)?,
        duration_seconds: row.get(10)?,
        status: row.get(11)?,
        conversation_id: row.get(12)?,
        transcription: row.get(13)?,
        transcription_model: row.get(14)?,
        created_at: row.get(15)?,
        user_root_node_id: None,
        group_root_node_id: None,
        group_context: row
            .get::<_, Option<String>>(17)?
            .and_then(|value| serde_json::from_str(&value).ok()),
        session_kind: row.get(16)?,
    })
}

fn fetch_event(db: &Connection, id: &str) -> Result<RelayEvent, ApiError> {
    db.query_row(
        "SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at,session_kind,group_context_json FROM telegram_events WHERE id=?1",
        [id], row_event,
    ).optional().map_err(ApiError::internal)?.ok_or_else(ApiError::not_found)
}

async fn list_events(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let mut statement = db.prepare(
        "SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at,session_kind,group_context_json FROM telegram_events WHERE status IN ('pending','processing') ORDER BY update_id",
    ).map_err(ApiError::internal)?;
    let queued = statement
        .query_map([], row_event)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    let mut seen = HashSet::new();
    let mut events = queued
        .into_iter()
        .filter(|event| {
            seen.insert(if event.session_kind == "group" {
                event.chat_id
            } else {
                event.telegram_user_id
            })
        })
        .collect::<Vec<_>>();
    let users = state.users.lock().map_err(ApiError::internal)?;
    for event in &mut events {
        event.user_root_node_id = directory_user_by_id(&users, event.telegram_user_id)
            .map_err(ApiError::internal)?
            .filter(|user| user.root_ready)
            .map(|user| user.root_node_id);
        if event.session_kind == "group"
            && let Some(group) =
                directory_group_by_id(&users, event.chat_id).map_err(ApiError::internal)?
        {
            event.group_root_node_id = Some(group.root_node_id.clone());
            if let Some(context) = event.group_context.as_mut().and_then(Value::as_object_mut) {
                context.insert("groupRootNodeId".into(), Value::String(group.root_node_id));
                context.insert("groupRootReady".into(), Value::Bool(group.root_ready));
            }
        }
    }
    Ok(Json(json!({"events":events})))
}

async fn event_media(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let media = db
        .query_row(
            "SELECT voice_bytes,mime_type,kind FROM telegram_events WHERE id=?1 AND kind IN ('voice','document')",
            [&id],
            |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?
        .ok_or_else(ApiError::not_found)?;
    let bytes = media.0.ok_or_else(ApiError::not_found)?;
    let mime = media.1.unwrap_or_else(|| {
        if media.2 == "document" {
            "application/octet-stream".into()
        } else {
            "audio/ogg".into()
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(bytes))
        .map_err(ApiError::internal)
}

fn validate_conversation_id(value: &str) -> Result<(), ApiError> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| ApiError::bad("conversationId must be a UUID."))
}

async fn bind_event(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<BindEvent>,
) -> Result<Json<RelayEvent>, ApiError> {
    validate_conversation_id(&input.conversation_id)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let event = fetch_event(&db, &id)?;
    if event.status == "complete" {
        return Err(ApiError::conflict(
            "The Telegram event is already complete.",
        ));
    }
    if let Some(existing) = event.conversation_id.as_deref()
        && existing != input.conversation_id
        && event.status != "pending"
    {
        return Err(ApiError::conflict(
            "The Telegram event is already bound to another conversation.",
        ));
    }
    let now = Utc::now().to_rfc3339();
    db.execute(
        "UPDATE telegram_events SET status='processing',conversation_id=?1 WHERE id=?2 AND status<>'complete'",
        params![input.conversation_id, id],
    ).map_err(ApiError::internal)?;
    if event.session_kind == "private" {
        db.execute(
            "UPDATE authorized_users SET current_conversation_id=?1,updated_at=?2 WHERE telegram_user_id=?3",
            params![input.conversation_id, now, event.telegram_user_id],
        ).map_err(ApiError::internal)?;
    }
    Ok(Json(fetch_event(&db, &id)?))
}

async fn save_transcription(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<SaveTranscription>,
) -> Result<Json<RelayEvent>, ApiError> {
    let text = input.text.trim();
    let transcription_model = input.transcription_model.trim();
    if text.is_empty() || transcription_model.is_empty() {
        return Err(ApiError::bad(
            "text and transcriptionModel must not be empty.",
        ));
    }
    let db = state.db.lock().map_err(ApiError::internal)?;
    let event = fetch_event(&db, &id)?;
    if event.kind != "voice" || event.status == "complete" {
        return Err(ApiError::conflict(
            "This event cannot accept a transcription.",
        ));
    }
    if let Some(existing) = event.transcription.as_deref()
        && existing != text
    {
        return Err(ApiError::conflict(
            "This voice note already has a different transcription.",
        ));
    }
    db.execute(
        "UPDATE telegram_events SET transcription=?1,transcription_model=?2 WHERE id=?3 AND status<>'complete'",
        params![text, transcription_model, id],
    ).map_err(ApiError::internal)?;
    Ok(Json(fetch_event(&db, &id)?))
}

async fn send_telegram_text(
    bot: &Bot,
    chat_id: i64,
    text: &str,
    reply_to_message_id: Option<i64>,
) -> Result<Vec<Message>, ApiError> {
    let mut sent = Vec::new();
    for (index, chunk) in telegram_chunks(text, TELEGRAM_MESSAGE_LIMIT)
        .into_iter()
        .enumerate()
    {
        let mut request = bot.send_message(ChatId(chat_id), chunk);
        if index == 0
            && let Some(message_id) =
                reply_to_message_id.and_then(|value| i32::try_from(value).ok())
        {
            request = request.reply_parameters(teloxide::types::ReplyParameters::new(
                teloxide::types::MessageId(message_id),
            ));
        }
        let message = request.send().await.map_err(|error| {
            tracing::warn!(%chat_id, error=%error, "Telegram reply failed");
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "telegram_send_failed",
                "Telegram did not accept the reply.",
            )
        })?;
        sent.push(message);
    }
    Ok(sent)
}

async fn reply_event(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<ReplyEvent>,
) -> Result<Json<RelayEvent>, ApiError> {
    let started = Instant::now();
    validate_conversation_id(&input.conversation_id)?;
    if input.text.trim().is_empty() {
        return Err(ApiError::bad("text must not be empty."));
    }
    let event = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        let event = fetch_event(&db, &id)?;
        if event.status == "complete" {
            return Ok(Json(event));
        }
        if event.conversation_id.as_deref() != Some(input.conversation_id.as_str()) {
            return Err(ApiError::conflict(
                "The event is not bound to this conversation.",
            ));
        }
        event
    };
    let bot = state.bot.as_ref().ok_or_else(ApiError::unavailable)?;
    let group_reply = (event.session_kind == "group").then_some(event.message_id);
    let mut sent = send_telegram_text(bot, event.chat_id, input.text.trim(), group_reply).await?;
    if let Some(warning) = input
        .context_warning
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        sent.extend(send_telegram_text(bot, event.chat_id, warning, None).await?);
    }
    let db = state.db.lock().map_err(ApiError::internal)?;
    if event.session_kind == "group" {
        for message in sent {
            db.execute(
                "INSERT INTO telegram_group_messages(chat_id,message_id,update_id,display_name,text,reply_to_message_id,sent_by_kennedy,created_at)
                 VALUES(?1,?2,0,'Kennedy',?3,?4,1,?5) ON CONFLICT(chat_id,message_id) DO NOTHING",
                params![event.chat_id, i64::from(message.id.0), message.text().unwrap_or(""), event.message_id, message.date.to_rfc3339()],
            ).map_err(ApiError::internal)?;
        }
    }
    db.execute(
        "UPDATE telegram_events SET status='complete',completed_at=?1 WHERE id=?2",
        params![Utc::now().to_rfc3339(), id],
    )
    .map_err(ApiError::internal)?;
    tracing::info!(event_id=%id, duration_ms=started.elapsed().as_millis(), "Telegram reply");
    Ok(Json(fetch_event(&db, &id)?))
}

async fn complete_reset(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<CompleteReset>,
) -> Result<Json<RelayEvent>, ApiError> {
    let event = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        let event = fetch_event(&db, &id)?;
        if event.kind != "reset" {
            return Err(ApiError::conflict("This event is not a reset."));
        }
        if event.status == "complete" {
            return Ok(Json(event));
        }
        event
    };
    let bot = state.bot.as_ref().ok_or_else(ApiError::unavailable)?;
    let message = input.message.as_deref().map(str::trim).filter(|value| !value.is_empty())
        .unwrap_or("Conversation reset. Your previous Telegram session has been queued for memory ingress.");
    send_telegram_text(bot, event.chat_id, message, None).await?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let now = Utc::now().to_rfc3339();
    db.execute(
        "UPDATE telegram_events SET status='complete',completed_at=?1 WHERE id=?2",
        params![now, id],
    )
    .map_err(ApiError::internal)?;
    db.execute(
        "UPDATE authorized_users SET current_conversation_id=NULL,updated_at=?1 WHERE telegram_user_id=?2",
        params![now, event.telegram_user_id],
    ).map_err(ApiError::internal)?;
    Ok(Json(fetch_event(&db, &id)?))
}

fn telegram_chunks(text: &str, max_utf16_units: usize) -> Vec<String> {
    if text.encode_utf16().count() <= max_utf16_units {
        return vec![text.to_owned()];
    }
    let mut chunks = Vec::new();
    let mut remaining = text.trim();
    while remaining.encode_utf16().count() > max_utf16_units {
        let mut units = 0;
        let boundary = remaining
            .char_indices()
            .find_map(|(index, character)| {
                units += character.len_utf16();
                (units > max_utf16_units).then_some(index)
            })
            .unwrap_or(remaining.len());
        let prefix = &remaining[..boundary];
        let split = prefix
            .rfind('\n')
            .or_else(|| prefix.rfind(char::is_whitespace))
            .filter(|index| prefix[..*index].encode_utf16().count() > max_utf16_units / 2)
            .unwrap_or(boundary);
        let (chunk, tail) = remaining.split_at(split);
        chunks.push(chunk.trim().to_owned());
        remaining = tail.trim_start();
    }
    if !remaining.is_empty() {
        chunks.push(remaining.to_owned());
    }
    chunks
}

async fn poll_telegram(bot: Bot, state: AppState) -> anyhow::Result<()> {
    let mut offset = 0;
    loop {
        let updates = match bot
            .get_updates()
            .offset(offset)
            .timeout(TELEGRAM_POLL_TIMEOUT_SECONDS)
            .allowed_updates(vec![
                AllowedUpdate::Message,
                AllowedUpdate::EditedMessage,
                AllowedUpdate::MyChatMember,
                AllowedUpdate::ChatMember,
            ])
            .send()
            .await
        {
            Ok(updates) => updates,
            Err(error) => {
                tracing::debug!(%error, "Telegram poll retry");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        for update in updates {
            offset = i32::try_from(update.id.0.saturating_add(1)).unwrap_or(i32::MAX);
            if let Err(error) = process_update(&bot, &state, update).await {
                tracing::warn!(%error, "Telegram ingress failed");
            }
        }
    }
}

fn supported_document(file_name: Option<&str>, mime_type: Option<&str>) -> bool {
    let supported_extension = file_name
        .and_then(|name| name.rsplit_once('.').map(|(_, extension)| extension))
        .map(|extension| extension.to_ascii_lowercase())
        .is_some_and(|extension| {
            matches!(
                extension.as_str(),
                "pdf"
                    | "docx"
                    | "xlsx"
                    | "xls"
                    | "xlsb"
                    | "ods"
                    | "csv"
                    | "tsv"
                    | "txt"
                    | "md"
                    | "json"
                    | "yaml"
                    | "yml"
                    | "xml"
            )
        });
    supported_extension
        || matches!(
            mime_type.unwrap_or("").to_ascii_lowercase().as_str(),
            "application/pdf"
                | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                | "application/vnd.ms-excel"
                | "application/vnd.ms-excel.sheet.binary.macroenabled.12"
                | "application/vnd.oasis.opendocument.spreadsheet"
                | "application/json"
                | "application/xml"
                | "application/yaml"
                | "application/x-yaml"
                | "text/plain"
                | "text/csv"
                | "text/tab-separated-values"
        )
}

async fn process_update(bot: &Bot, state: &AppState, update: Update) -> anyhow::Result<()> {
    let update_id = i64::from(update.id.0);
    match update.kind {
        UpdateKind::Message(message) => {
            if message.chat.is_private() {
                process_private_message(bot, state, update_id, message).await
            } else if message.chat.is_group() || message.chat.is_supergroup() {
                process_group_message(bot, state, update_id, message, false).await
            } else {
                Ok(())
            }
        }
        UpdateKind::EditedMessage(message)
            if message.chat.is_group() || message.chat.is_supergroup() =>
        {
            process_group_message(bot, state, update_id, message, true).await
        }
        UpdateKind::ChatMember(change) | UpdateKind::MyChatMember(change) => {
            process_group_membership(state, change)
        }
        _ => Ok(()),
    }
}

fn ensure_transport_user(
    db: &Connection,
    telegram_user_id: i64,
    username: Option<&str>,
    display_name: &str,
    chat_id: i64,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    let bootstrap = format!("id:{telegram_user_id}");
    db.execute(
        "INSERT INTO authorized_users(bootstrap_username,telegram_user_id,username,display_name,chat_id,paired_at,updated_at)
         VALUES(?1,?2,?3,?4,?5,?6,?6)
         ON CONFLICT(telegram_user_id) DO UPDATE SET username=excluded.username,display_name=excluded.display_name,chat_id=excluded.chat_id,updated_at=excluded.updated_at",
        params![bootstrap, telegram_user_id, username, display_name, chat_id, now],
    )?;
    Ok(())
}

async fn process_private_message(
    bot: &Bot,
    state: &AppState,
    update_id: i64,
    message: Message,
) -> anyhow::Result<()> {
    let Some(user) = message.from.as_ref() else {
        return Ok(());
    };
    let telegram_user_id =
        i64::try_from(user.id.0).context("Telegram user ID exceeds SQLite range")?;
    let username = user.username.clone();
    let display_name = user.full_name();
    let chat_id = message.chat.id.0;
    let outcome = {
        let db = state
            .users
            .lock()
            .map_err(|_| anyhow::anyhow!("locking user directory"))?;
        observe_identity(&db, telegram_user_id, username.as_deref(), &display_name)?
    };
    let authorized = matches!(
        outcome,
        TofuOutcome::Whitelisted | TofuOutcome::UnresolvedHandleClaimed
    );
    if !authorized {
        bot.send_message(message.chat.id, UNAUTHORIZED_MESSAGE)
            .send()
            .await?;
        return Ok(());
    }

    if let Some(text) = message.text()
        && text.split_whitespace().next().is_some_and(|command| {
            command.eq_ignore_ascii_case("/adduser")
                || command.to_ascii_lowercase().starts_with("/adduser@")
        })
    {
        let can_add = {
            let db = state
                .users
                .lock()
                .map_err(|_| anyhow::anyhow!("locking user directory"))?;
            directory_user_by_id(&db, telegram_user_id)?.is_some_and(|entry| entry.can_add_users)
        };
        if !can_add {
            bot.send_message(
                message.chat.id,
                "Only the initial Kennedy administrator can use /adduser.",
            )
            .send()
            .await?;
            return Ok(());
        }
        let Some(handle) = text.split_whitespace().nth(1) else {
            bot.send_message(message.chat.id, "Usage: /adduser @theirHandle")
                .send()
                .await?;
            return Ok(());
        };
        let entry = {
            let db = state
                .users
                .lock()
                .map_err(|_| anyhow::anyhow!("locking user directory"))?;
            whitelist_handle(&db, handle, telegram_user_id)?
        };
        let status = if let Some(id) = entry.telegram_user_id {
            format!(
                "Whitelisted @{} and pinned Telegram user ID {}. Their blank Kmap root is being prepared.",
                entry.handle, id
            )
        } else {
            format!(
                "Whitelisted @{}. Kennedy will pin its numeric Telegram user ID by TOFU the first time that handle is observed.",
                entry.handle
            )
        };
        bot.send_message(message.chat.id, status).send().await?;
        return Ok(());
    }

    {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("locking Telegram database"))?;
        ensure_transport_user(
            &db,
            telegram_user_id,
            username.as_deref(),
            &display_name,
            chat_id,
        )?;
    }

    let (kind, text, media_bytes, mime_type, file_name, duration_seconds) = if let Some(text) =
        message.text()
    {
        let reset = text.split_whitespace().next().is_some_and(|command| {
            command.eq_ignore_ascii_case("/reset")
                || command.to_ascii_lowercase().starts_with("/reset@")
        });
        if reset {
            ("reset", None, None, None, None, None)
        } else {
            ("text", Some(text.to_owned()), None, None, None, None)
        }
    } else if let Some(voice) = message.voice() {
        if voice.file.size > state.max_voice_bytes as u32 {
            bot.send_message(
                message.chat.id,
                "That voice note is too large for Kennedy to process.",
            )
            .send()
            .await?;
            return Ok(());
        }
        let file = bot.get_file(voice.file.id.clone()).send().await?;
        let mut stream = bot.download_file_stream(&file.path);
        let mut bytes = Vec::with_capacity(voice.file.size as usize);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if bytes.len().saturating_add(chunk.len()) > state.max_voice_bytes {
                bot.send_message(
                    message.chat.id,
                    "That voice note is too large for Kennedy to process.",
                )
                .send()
                .await?;
                return Ok(());
            }
            bytes.extend_from_slice(&chunk);
        }
        let mime_type = voice
            .mime_type
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "audio/ogg".into());
        (
            "voice",
            None,
            Some(bytes),
            Some(mime_type),
            None,
            Some(i64::from(voice.duration.seconds())),
        )
    } else if let Some(document) = message.document() {
        let mime_type = document.mime_type.as_ref().map(ToString::to_string);
        if !supported_document(document.file_name.as_deref(), mime_type.as_deref()) {
            bot.send_message(
                message.chat.id,
                "Kennedy can read PDF, DOCX, spreadsheet, CSV, and text documents.",
            )
            .send()
            .await?;
            return Ok(());
        }
        if document.file.size > state.max_voice_bytes as u32 {
            bot.send_message(
                message.chat.id,
                "That document is too large for Kennedy to process.",
            )
            .send()
            .await?;
            return Ok(());
        }
        let file = bot.get_file(document.file.id.clone()).send().await?;
        let mut stream = bot.download_file_stream(&file.path);
        let mut bytes = Vec::with_capacity(document.file.size as usize);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if bytes.len().saturating_add(chunk.len()) > state.max_voice_bytes {
                bot.send_message(
                    message.chat.id,
                    "That document is too large for Kennedy to process.",
                )
                .send()
                .await?;
                return Ok(());
            }
            bytes.extend_from_slice(&chunk);
        }
        (
            "document",
            message.caption().map(ToOwned::to_owned),
            Some(bytes),
            mime_type,
            Some(
                document
                    .file_name
                    .clone()
                    .unwrap_or_else(|| "telegram-document".into()),
            ),
            None,
        )
    } else {
        bot.send_message(message.chat.id, "Kennedy accepts text, voice notes, and PDF, DOCX, spreadsheet, CSV, or text documents here. Use /reset to end this Telegram session.").send().await?;
        return Ok(());
    };
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("locking Telegram database"))?;
    insert_event(
        &db,
        update_id,
        &message,
        telegram_user_id,
        username.as_deref(),
        &display_name,
        kind,
        text.as_deref(),
        media_bytes.as_deref(),
        mime_type.as_deref(),
        file_name.as_deref(),
        duration_seconds,
    )?;
    Ok(())
}

fn ensure_group(db: &Connection, chat_id: i64, title: &str) -> anyhow::Result<DirectoryGroup> {
    let now = Utc::now().to_rfc3339();
    if directory_group_by_id(db, chat_id)?.is_none() {
        let root_node_id = random_unassigned_node_id(db)?;
        db.execute(
            "INSERT INTO telegram_groups(chat_id,title,root_node_id,created_at,updated_at) VALUES(?1,?2,?3,?4,?4)",
            params![chat_id, title, root_node_id, now],
        )?;
    } else {
        db.execute(
            "UPDATE telegram_groups SET title=?1,updated_at=?2 WHERE chat_id=?3",
            params![title, now, chat_id],
        )?;
    }
    directory_group_by_id(db, chat_id)?.context("reading Telegram group after assignment")
}

fn migrate_group_identity(
    db: &Connection,
    old_chat_id: i64,
    new_chat_id: i64,
    title: &str,
) -> anyhow::Result<DirectoryGroup> {
    let old = ensure_group(db, old_chat_id, title)?;
    let now = Utc::now().to_rfc3339();
    let blacklist_reason: Option<String> = db.query_row(
        "SELECT blacklist_reason FROM telegram_groups WHERE chat_id=?1",
        [old_chat_id],
        |row| row.get(0),
    )?;
    let blacklisted_at: Option<String> = db.query_row(
        "SELECT blacklisted_at FROM telegram_groups WHERE chat_id=?1",
        [old_chat_id],
        |row| row.get(0),
    )?;
    let (last_invocation, background_cursor): (Option<i64>, Option<i64>) = db.query_row(
        "SELECT last_invocation_message_id,background_cursor_message_id FROM telegram_groups WHERE chat_id=?1",
        [old_chat_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    db.execute(
        "INSERT INTO telegram_groups(
             chat_id,title,root_node_id,root_ready,state,blacklist_reason,blacklisted_at,
             last_invocation_message_id,background_cursor_message_id,created_at,updated_at
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10)
         ON CONFLICT(chat_id) DO UPDATE SET
             title=excluded.title,
             root_node_id=excluded.root_node_id,
             root_ready=excluded.root_ready,
             state=CASE WHEN telegram_groups.state='blacklisted' OR excluded.state='blacklisted'
                        THEN 'blacklisted' ELSE excluded.state END,
             blacklist_reason=COALESCE(telegram_groups.blacklist_reason,excluded.blacklist_reason),
             blacklisted_at=COALESCE(telegram_groups.blacklisted_at,excluded.blacklisted_at),
             last_invocation_message_id=MAX(COALESCE(telegram_groups.last_invocation_message_id,0),COALESCE(excluded.last_invocation_message_id,0)),
             background_cursor_message_id=MAX(COALESCE(telegram_groups.background_cursor_message_id,0),COALESCE(excluded.background_cursor_message_id,0)),
             updated_at=excluded.updated_at",
        params![
            new_chat_id,
            title,
            old.root_node_id,
            old.root_ready,
            old.state,
            blacklist_reason,
            blacklisted_at,
            last_invocation,
            background_cursor,
            now,
        ],
    )?;
    db.execute(
        "INSERT OR REPLACE INTO telegram_group_members(
             chat_id,telegram_user_id,username,display_name,membership,first_seen_at,updated_at
         ) SELECT ?1,telegram_user_id,username,display_name,membership,first_seen_at,updated_at
           FROM telegram_group_members WHERE chat_id=?2",
        params![new_chat_id, old_chat_id],
    )?;
    db.execute(
        "INSERT INTO telegram_group_aliases(old_chat_id,current_chat_id) VALUES(?1,?2)
         ON CONFLICT(old_chat_id) DO UPDATE SET current_chat_id=excluded.current_chat_id",
        params![old_chat_id, new_chat_id],
    )?;
    directory_group_by_id(db, new_chat_id)?.context("reading migrated Telegram group")
}

fn blacklist_group(db: &Connection, chat_id: i64, reason: &str) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    db.execute(
        "UPDATE telegram_groups SET state='blacklisted',blacklist_reason=COALESCE(blacklist_reason,?1),blacklisted_at=COALESCE(blacklisted_at,?2),updated_at=?2 WHERE chat_id=?3",
        params![reason, now, chat_id],
    )?;
    tracing::warn!(chat_id, reason, "Permanently blacklisted Telegram group");
    Ok(())
}

fn group_state(db: &Connection, chat_id: i64) -> anyhow::Result<Option<String>> {
    Ok(db
        .query_row(
            "SELECT state FROM telegram_groups WHERE chat_id=?1",
            [chat_id],
            |row| row.get(0),
        )
        .optional()?)
}

fn member_status(kind: &ChatMemberKind) -> (&'static str, bool) {
    match kind {
        ChatMemberKind::Owner(_) => ("creator", true),
        ChatMemberKind::Administrator(_) => ("administrator", true),
        ChatMemberKind::Member(_) => ("member", true),
        ChatMemberKind::Restricted(member) if member.is_member => ("member", true),
        ChatMemberKind::Restricted(_) | ChatMemberKind::Left => ("left", false),
        ChatMemberKind::Banned(_) => ("kicked", false),
    }
}

fn upsert_group_member(
    db: &Connection,
    chat_id: i64,
    user_id: i64,
    username: Option<&str>,
    display_name: &str,
    membership: &str,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    db.execute(
        "INSERT INTO telegram_group_members(chat_id,telegram_user_id,username,display_name,membership,first_seen_at,updated_at)
         VALUES(?1,?2,?3,?4,?5,?6,?6)
         ON CONFLICT(chat_id,telegram_user_id) DO UPDATE SET username=excluded.username,display_name=excluded.display_name,membership=excluded.membership,updated_at=excluded.updated_at",
        params![chat_id, user_id, username, display_name, membership, now],
    )?;
    Ok(())
}

fn process_group_membership(
    state: &AppState,
    change: teloxide::types::ChatMemberUpdated,
) -> anyhow::Result<()> {
    if !(change.chat.is_group() || change.chat.is_supergroup()) {
        return Ok(());
    }
    let chat_id = change.chat.id.0;
    let title = change.chat.title().unwrap_or("Telegram group");
    let target = &change.new_chat_member.user;
    let target_id = i64::try_from(target.id.0).context("Telegram user ID exceeds SQLite range")?;
    let (membership, active) = member_status(&change.new_chat_member.kind);
    let is_kennedy = state.bot_user_id == Some(target_id);
    let outcome = if is_kennedy || !active {
        None
    } else {
        let db = state
            .users
            .lock()
            .map_err(|_| anyhow::anyhow!("locking user directory"))?;
        Some(observe_identity(
            &db,
            target_id,
            target.username.as_deref(),
            &target.full_name(),
        )?)
    };
    let db = state
        .users
        .lock()
        .map_err(|_| anyhow::anyhow!("locking user directory"))?;
    ensure_group(&db, chat_id, title)?;
    if group_state(&db, chat_id)?.as_deref() == Some("blacklisted") {
        return Ok(());
    }
    if is_kennedy {
        let was_admin = matches!(
            change.old_chat_member.kind,
            ChatMemberKind::Owner(_) | ChatMemberKind::Administrator(_)
        );
        let is_admin = matches!(
            change.new_chat_member.kind,
            ChatMemberKind::Owner(_) | ChatMemberKind::Administrator(_)
        );
        if was_admin && !is_admin {
            blacklist_group(
                &db,
                chat_id,
                "Kennedy lost group-administrator status, interrupting complete membership monitoring.",
            )?;
        } else if !is_admin {
            tracing::info!(
                chat_id,
                "Telegram group is waiting for Kennedy to be promoted to administrator"
            );
        }
        return Ok(());
    }
    upsert_group_member(
        &db,
        chat_id,
        target_id,
        target.username.as_deref(),
        &target.full_name(),
        membership,
    )?;
    if active
        && !matches!(
            outcome,
            Some(TofuOutcome::Whitelisted | TofuOutcome::UnresolvedHandleClaimed)
        )
    {
        blacklist_group(
            &db,
            chat_id,
            "A non-whitelisted or TOFU-conflicting member joined the group.",
        )?;
    }
    Ok(())
}

async fn validate_group_membership(
    bot: &Bot,
    state: &AppState,
    chat_id: i64,
) -> anyhow::Result<bool> {
    {
        let db = state
            .users
            .lock()
            .map_err(|_| anyhow::anyhow!("locking user directory"))?;
        if group_state(&db, chat_id)?.as_deref() == Some("blacklisted") {
            return Ok(false);
        }
    }
    let Some(bot_user_id) = state.bot_user_id else {
        return Ok(false);
    };
    let bot_member = bot
        .get_chat_member(
            teloxide::types::ChatId(chat_id),
            teloxide::types::UserId(u64::try_from(bot_user_id).context("negative bot user ID")?),
        )
        .send()
        .await?;
    if !matches!(
        bot_member.kind,
        ChatMemberKind::Owner(_) | ChatMemberKind::Administrator(_)
    ) {
        let db = state
            .users
            .lock()
            .map_err(|_| anyhow::anyhow!("locking user directory"))?;
        if group_state(&db, chat_id)?.as_deref() == Some("allowed") {
            blacklist_group(
                &db,
                chat_id,
                "Kennedy lost group-administrator status, interrupting complete membership monitoring.",
            )?;
        }
        return Ok(false);
    }
    let member_count = i64::from(
        bot.get_chat_member_count(teloxide::types::ChatId(chat_id))
            .send()
            .await?,
    );
    let db = state
        .users
        .lock()
        .map_err(|_| anyhow::anyhow!("locking user directory"))?;
    let known_active: i64 = db.query_row(
        "SELECT COUNT(*) FROM telegram_group_members WHERE chat_id=?1 AND membership IN ('member','administrator','creator')",
        [chat_id],
        |row| row.get(0),
    )?;
    let unknown: i64 = db.query_row(
        "SELECT COUNT(*) FROM telegram_group_members m LEFT JOIN whitelist_entries w ON w.telegram_user_id=m.telegram_user_id
         WHERE m.chat_id=?1 AND m.membership IN ('member','administrator','creator') AND w.telegram_user_id IS NULL",
        [chat_id],
        |row| row.get(0),
    )?;
    if unknown > 0 || known_active + 1 != member_count {
        blacklist_group(
            &db,
            chat_id,
            "Telegram group membership could not be completely matched to ready whitelisted identities.",
        )?;
        return Ok(false);
    }
    db.execute(
        "UPDATE telegram_groups SET state='allowed',updated_at=?1 WHERE chat_id=?2 AND state<>'blacklisted'",
        params![Utc::now().to_rfc3339(), chat_id],
    )?;
    Ok(true)
}

fn group_invokes_kennedy(message: &Message, bot_user_id: i64, bot_username: Option<&str>) -> bool {
    if message
        .reply_to_message()
        .and_then(|reply| reply.from.as_ref())
        .and_then(|user| i64::try_from(user.id.0).ok())
        == Some(bot_user_id)
    {
        return true;
    }
    let expected = bot_username.map(normalize_username);
    message
        .parse_entities()
        .into_iter()
        .flatten()
        .chain(message.parse_caption_entities().into_iter().flatten())
        .any(|entity| {
            matches!(
                entity.kind(),
                MessageEntityKind::Mention | MessageEntityKind::BotCommand
            ) && expected.as_deref().is_some_and(|name| {
                normalize_username(entity.text().rsplit('@').next().unwrap_or("")) == name
            })
        })
}

fn group_participants(db: &Connection, chat_id: i64) -> anyhow::Result<Value> {
    let mut statement = db.prepare(
        "SELECT m.telegram_user_id,m.username,m.display_name,w.root_node_id,w.root_ready
         FROM telegram_group_members m JOIN whitelist_entries w ON w.telegram_user_id=m.telegram_user_id
         WHERE m.chat_id=?1 AND m.membership IN ('member','administrator','creator') ORDER BY m.telegram_user_id",
    )?;
    let users = statement
        .query_map([chat_id], |row| {
            Ok(json!({
                "telegramUserId":row.get::<_,i64>(0)?, "username":row.get::<_,Option<String>>(1)?,
                "displayName":row.get::<_,String>(2)?, "rootNodeId":row.get::<_,String>(3)?,
                "rootReady":row.get::<_,i64>(4)? != 0,
            }))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(users))
}

fn recent_group_messages(
    db: &Connection,
    chat_id: i64,
    through_message_id: i64,
    limit: usize,
) -> anyhow::Result<Vec<Value>> {
    let mut statement = db.prepare(
        "SELECT message_id,telegram_user_id,username,display_name,text,reply_to_message_id,sent_by_kennedy,created_at
         FROM telegram_group_messages WHERE chat_id=?1 AND message_id<=?2 ORDER BY message_id DESC LIMIT ?3",
    )?;
    let mut messages = statement
        .query_map(params![chat_id, through_message_id, limit as i64], |row| {
            Ok(json!({
                "messageId":row.get::<_,i64>(0)?, "telegramUserId":row.get::<_,Option<i64>>(1)?,
                "username":row.get::<_,Option<String>>(2)?, "displayName":row.get::<_,String>(3)?,
                "text":row.get::<_,String>(4)?, "replyToMessageId":row.get::<_,Option<i64>>(5)?,
                "sentByKennedy":row.get::<_,i64>(6)? != 0, "createdAt":row.get::<_,String>(7)?,
            }))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    messages.reverse();
    Ok(messages)
}

fn maybe_queue_group_ingress(
    db: &Connection,
    chat_id: i64,
    cursor: i64,
    participants: &Value,
) -> anyhow::Result<Option<i64>> {
    let backlog: i64 = db.query_row(
        "SELECT COUNT(*) FROM telegram_group_messages WHERE chat_id=?1 AND message_id>?2 AND sent_by_kennedy=0",
        params![chat_id, cursor],
        |row| row.get(0),
    )?;
    if backlog <= 100 {
        return Ok(None);
    }
    let mut statement = db.prepare(
        "SELECT message_id,telegram_user_id,username,display_name,text,reply_to_message_id,created_at
         FROM telegram_group_messages WHERE chat_id=?1 AND message_id>?2 AND sent_by_kennedy=0
         ORDER BY message_id LIMIT 80",
    )?;
    let messages = statement
        .query_map(params![chat_id, cursor], |row| {
            Ok(json!({
                "messageId":row.get::<_,i64>(0)?, "telegramUserId":row.get::<_,Option<i64>>(1)?,
                "username":row.get::<_,Option<String>>(2)?, "displayName":row.get::<_,String>(3)?,
                "text":row.get::<_,String>(4)?, "replyToMessageId":row.get::<_,Option<i64>>(5)?,
                "sentByKennedy":false, "createdAt":row.get::<_,String>(6)?,
            }))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let Some(first) = messages
        .first()
        .and_then(|message| message["messageId"].as_i64())
    else {
        return Ok(None);
    };
    let last = messages
        .last()
        .and_then(|message| message["messageId"].as_i64())
        .expect("an ingress batch has a first and last message");
    db.execute(
        "INSERT INTO telegram_group_ingress(id,chat_id,first_message_id,last_message_id,messages_json,participants_json,created_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(chat_id,first_message_id,last_message_id) DO NOTHING",
        params![Uuid::new_v4().to_string(), chat_id, first, last, serde_json::to_string(&messages)?, serde_json::to_string(participants)?, Utc::now().to_rfc3339()],
    )?;
    Ok(Some(last))
}

async fn process_group_message(
    bot: &Bot,
    state: &AppState,
    update_id: i64,
    message: Message,
    edited: bool,
) -> anyhow::Result<()> {
    let chat_id = message.chat.id.0;
    let title = message.chat.title().unwrap_or("Telegram group");
    if let Some(new_chat_id) = message.migrate_to_chat_id() {
        let db = state
            .users
            .lock()
            .map_err(|_| anyhow::anyhow!("locking user directory"))?;
        migrate_group_identity(&db, chat_id, new_chat_id.0, title)?;
        return Ok(());
    }
    let Some(user) = message.from.as_ref() else {
        return Ok(());
    };
    let telegram_user_id =
        i64::try_from(user.id.0).context("Telegram user ID exceeds SQLite range")?;
    let outcome = {
        let db = state
            .users
            .lock()
            .map_err(|_| anyhow::anyhow!("locking user directory"))?;
        ensure_group(&db, chat_id, title)?;
        let outcome = observe_identity(
            &db,
            telegram_user_id,
            user.username.as_deref(),
            &user.full_name(),
        )?;
        upsert_group_member(
            &db,
            chat_id,
            telegram_user_id,
            user.username.as_deref(),
            &user.full_name(),
            "member",
        )?;
        if !matches!(
            outcome,
            TofuOutcome::Whitelisted | TofuOutcome::UnresolvedHandleClaimed
        ) {
            blacklist_group(
                &db,
                chat_id,
                "A group message came from a non-whitelisted or TOFU-conflicting identity.",
            )?;
        }
        for member in message.new_chat_members().unwrap_or_default() {
            let member_id =
                i64::try_from(member.id.0).context("Telegram user ID exceeds SQLite range")?;
            if state.bot_user_id == Some(member_id) {
                continue;
            }
            let member_outcome = observe_identity(
                &db,
                member_id,
                member.username.as_deref(),
                &member.full_name(),
            )?;
            upsert_group_member(
                &db,
                chat_id,
                member_id,
                member.username.as_deref(),
                &member.full_name(),
                "member",
            )?;
            if !matches!(
                member_outcome,
                TofuOutcome::Whitelisted | TofuOutcome::UnresolvedHandleClaimed
            ) {
                blacklist_group(
                    &db,
                    chat_id,
                    "A non-whitelisted or TOFU-conflicting member was added to the group.",
                )?;
            }
        }
        if let Some(member) = message.left_chat_member() {
            let member_id =
                i64::try_from(member.id.0).context("Telegram user ID exceeds SQLite range")?;
            if state.bot_user_id != Some(member_id) {
                upsert_group_member(
                    &db,
                    chat_id,
                    member_id,
                    member.username.as_deref(),
                    &member.full_name(),
                    "left",
                )?;
            }
        }
        outcome
    };
    if !matches!(
        outcome,
        TofuOutcome::Whitelisted | TofuOutcome::UnresolvedHandleClaimed
    ) || !validate_group_membership(bot, state, chat_id).await?
    {
        return Ok(());
    }
    let text = message
        .text()
        .or_else(|| message.caption())
        .unwrap_or("[non-text Telegram message]");
    let reply_to = message
        .reply_to_message()
        .map(|reply| i64::from(reply.id.0));
    {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("locking Telegram database"))?;
        db.execute(
            "INSERT INTO telegram_group_messages(chat_id,message_id,update_id,telegram_user_id,username,display_name,text,reply_to_message_id,created_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(chat_id,message_id) DO UPDATE SET text=excluded.text,username=excluded.username,display_name=excluded.display_name,reply_to_message_id=excluded.reply_to_message_id",
            params![chat_id, i64::from(message.id.0), update_id, telegram_user_id, user.username, user.full_name(), text, reply_to, message.date.to_rfc3339()],
        )?;
    }
    if edited {
        return Ok(());
    }
    let Some(bot_user_id) = state.bot_user_id else {
        return Ok(());
    };
    let invoked = group_invokes_kennedy(&message, bot_user_id, state.bot_username.as_deref());
    let users = state
        .users
        .lock()
        .map_err(|_| anyhow::anyhow!("locking user directory"))?;
    let participants = group_participants(&users, chat_id)?;
    let group = directory_group_by_id(&users, chat_id)?
        .context("reading Telegram group root for invocation")?;
    let cursor: i64 = users.query_row(
        "SELECT MAX(COALESCE(last_invocation_message_id,0),COALESCE(background_cursor_message_id,0)) FROM telegram_groups WHERE chat_id=?1",
        [chat_id], |row| row.get(0),
    )?;
    drop(users);
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("locking Telegram database"))?;
    if invoked {
        let messages = recent_group_messages(&db, chat_id, i64::from(message.id.0), 50)?;
        let context = json!({
            "groupTitle":title, "chatId":chat_id, "invokingTelegramUserId":telegram_user_id,
            "groupRootNodeId":group.root_node_id, "groupRootReady":group.root_ready,
            "participants":participants, "messages":messages,
        });
        db.execute(
            "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,status,created_at,session_kind,group_context_json)
             VALUES(?1,?2,?3,?4,?5,?6,?7,'text',?8,'pending',?9,'group',?10) ON CONFLICT(update_id) DO NOTHING",
            params![Uuid::new_v4().to_string(), update_id, i64::from(message.id.0), telegram_user_id, chat_id, user.username, user.full_name(), text, message.date.to_rfc3339(), serde_json::to_string(&context)?],
        )?;
        drop(db);
        let users = state
            .users
            .lock()
            .map_err(|_| anyhow::anyhow!("locking user directory"))?;
        users.execute(
            "UPDATE telegram_groups SET last_invocation_message_id=?1,updated_at=?2 WHERE chat_id=?3",
            params![i64::from(message.id.0), Utc::now().to_rfc3339(), chat_id],
        )?;
    } else {
        let last = maybe_queue_group_ingress(&db, chat_id, cursor, &participants)?;
        drop(db);
        if let Some(last) = last {
            let users = state
                .users
                .lock()
                .map_err(|_| anyhow::anyhow!("locking user directory"))?;
            users.execute(
                "UPDATE telegram_groups SET background_cursor_message_id=?1,updated_at=?2 WHERE chat_id=?3",
                params![last, Utc::now().to_rfc3339(), chat_id],
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_event(
    db: &Connection,
    update_id: i64,
    message: &Message,
    telegram_user_id: i64,
    username: Option<&str>,
    display_name: &str,
    kind: &str,
    text: Option<&str>,
    voice_bytes: Option<&[u8]>,
    mime_type: Option<&str>,
    file_name: Option<&str>,
    duration_seconds: Option<i64>,
) -> anyhow::Result<()> {
    let conversation_id = db
        .query_row(
            "SELECT current_conversation_id FROM authorized_users WHERE telegram_user_id=?1",
            [telegram_user_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    db.execute(
        "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,voice_bytes,mime_type,file_name,duration_seconds,conversation_id,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15) ON CONFLICT(update_id) DO NOTHING",
        params![
            Uuid::new_v4().to_string(), update_id, i64::from(message.id.0), telegram_user_id,
            message.chat.id.0, username, display_name, kind, text, voice_bytes, mime_type,
            file_name, duration_seconds, conversation_id, Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn databases() -> (Connection, Connection) {
        let relay = Connection::open_in_memory().unwrap();
        apply_migrations(&relay).unwrap();
        let users = Connection::open_in_memory().unwrap();
        apply_user_migrations(&users).unwrap();
        seed_bootstrap_users(&relay, &users, &["@taek42".into()]).unwrap();
        (relay, users)
    }

    #[test]
    fn handle_is_tofu_pinned_once_then_numeric_id_is_authoritative() {
        let (_, users) = databases();
        assert_eq!(
            observe_identity(&users, 7, Some("intruder"), "No").unwrap(),
            TofuOutcome::NotWhitelisted
        );
        assert_eq!(
            observe_identity(&users, 42, Some("TaEk42"), "David").unwrap(),
            TofuOutcome::UnresolvedHandleClaimed
        );
        assert_eq!(
            observe_identity(&users, 42, Some("renamed"), "David").unwrap(),
            TofuOutcome::Whitelisted
        );
        assert_eq!(
            observe_identity(&users, 43, Some("taek42"), "Other").unwrap(),
            TofuOutcome::HandleConflict
        );
        let david = directory_user_by_id(&users, 42).unwrap().unwrap();
        assert!(david.can_add_users);
        assert_eq!(david.handle, "taek42");
    }

    #[test]
    fn adduser_reserves_an_unresolved_blank_root_until_a_future_tofu_observation() {
        let (_, users) = databases();
        observe_identity(&users, 42, Some("taek42"), "David").unwrap();
        let friend = whitelist_handle(&users, "'@Friend'", 42).unwrap();
        assert_eq!(friend.handle, "friend");
        assert_eq!(friend.telegram_user_id, None);
        assert!(!friend.root_ready);
        assert_eq!(friend.root_node_id.len(), 40);
        assert_eq!(
            observe_identity(&users, 77, Some("friend"), "Trusted Friend").unwrap(),
            TofuOutcome::UnresolvedHandleClaimed
        );
        assert_eq!(
            directory_user_by_id(&users, 77)
                .unwrap()
                .unwrap()
                .root_node_id,
            friend.root_node_id
        );
    }

    #[test]
    fn a_blacklisted_group_never_returns_to_allowed() {
        let (_, users) = databases();
        let assigned = ensure_group(&users, -100, "Friends").unwrap();
        blacklist_group(&users, -100, "unknown member").unwrap();
        users
            .execute(
                "UPDATE telegram_groups SET state='allowed' WHERE chat_id=?1 AND state<>'blacklisted'",
                [-100],
            )
            .unwrap();
        assert_eq!(
            group_state(&users, -100).unwrap().as_deref(),
            Some("blacklisted")
        );
        let rediscovered = ensure_group(&users, -100, "Renamed Friends").unwrap();
        assert_eq!(rediscovered.root_node_id, assigned.root_node_id);
        assert_eq!(rediscovered.state, "blacklisted");
    }

    #[test]
    fn existing_groups_receive_stable_roots_during_directory_migration() {
        let users = Connection::open_in_memory().unwrap();
        users
            .execute_batch(
                "CREATE TABLE telegram_groups (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'validating',
                blacklist_reason TEXT,
                blacklisted_at TEXT,
                last_invocation_message_id INTEGER,
                background_cursor_message_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO telegram_groups(chat_id,title,created_at,updated_at)
            VALUES(-100,'Existing group','2026-01-01','2026-01-01');",
            )
            .unwrap();
        apply_user_migrations(&users).unwrap();
        let first = directory_group_by_id(&users, -100).unwrap().unwrap();
        assert_eq!(first.root_node_id.len(), 40);
        assert!(!first.root_ready);
        apply_user_migrations(&users).unwrap();
        let second = directory_group_by_id(&users, -100).unwrap().unwrap();
        assert_eq!(second.root_node_id, first.root_node_id);
    }

    #[test]
    fn telegram_group_migration_preserves_root_blacklist_and_members() {
        let (_, users) = databases();
        let old = ensure_group(&users, -100, "Friends").unwrap();
        users
            .execute(
                "UPDATE telegram_groups SET root_ready=1 WHERE chat_id=-100",
                [],
            )
            .unwrap();
        upsert_group_member(&users, -100, 42, Some("taek42"), "David", "creator").unwrap();
        blacklist_group(&users, -100, "unknown member").unwrap();
        let migrated = migrate_group_identity(&users, -100, -200, "Friends").unwrap();
        assert_eq!(migrated.root_node_id, old.root_node_id);
        assert!(migrated.root_ready);
        assert_eq!(migrated.state, "blacklisted");
        let members: i64 = users
            .query_row(
                "SELECT COUNT(*) FROM telegram_group_members WHERE chat_id=-200 AND telegram_user_id=42",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(members, 1);
    }

    #[tokio::test]
    async fn group_root_completion_accepts_only_the_reserved_identifier() {
        let (relay, users) = databases();
        let group = ensure_group(&users, -100, "Friends").unwrap();
        let state = AppState {
            db: Arc::new(Mutex::new(relay)),
            users: Arc::new(Mutex::new(users)),
            bot: None,
            max_voice_bytes: 1024,
            bot_user_id: None,
            bot_username: None,
        };
        let mismatch = complete_group_root(
            State(state.clone()),
            Path(-100),
            Json(CompleteRoot {
                root_node_id: hex::encode([0xff; 20]),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch.code, "state_conflict");
        let ready = complete_group_root(
            State(state),
            Path(-100),
            Json(CompleteRoot {
                root_node_id: group.root_node_id.clone(),
            }),
        )
        .await
        .unwrap();
        assert!(ready.0.root_ready);
        assert_eq!(ready.0.root_node_id, group.root_node_id);
    }

    #[tokio::test]
    async fn group_background_ingress_queues_oldest_eighty_and_exposes_the_group_root() {
        let (relay, users) = databases();
        let group = ensure_group(&users, -100, "Friends").unwrap();
        let now = Utc::now().to_rfc3339();
        for message_id in 1..=101 {
            relay.execute(
                "INSERT INTO telegram_group_messages(chat_id,message_id,update_id,telegram_user_id,display_name,text,created_at)
                 VALUES(-100,?1,?1,42,'David',?2,?3)",
                params![message_id, format!("message {message_id}"), now],
            ).unwrap();
        }
        let cursor = maybe_queue_group_ingress(&relay, -100, 0, &json!([]))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, 80);
        let messages: String = relay
            .query_row(
                "SELECT messages_json FROM telegram_group_ingress WHERE chat_id=-100",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let messages: Vec<Value> = serde_json::from_str(&messages).unwrap();
        assert_eq!(messages.len(), 80);
        assert_eq!(messages.first().unwrap()["messageId"], 1);
        assert_eq!(messages.last().unwrap()["messageId"], 80);
        assert_eq!(
            maybe_queue_group_ingress(&relay, -100, cursor, &json!([])).unwrap(),
            None
        );
        let state = AppState {
            db: Arc::new(Mutex::new(relay)),
            users: Arc::new(Mutex::new(users)),
            bot: None,
            max_voice_bytes: 1024,
            bot_user_id: None,
            bot_username: None,
        };
        let Json(payload) = list_group_ingress(State(state)).await.unwrap();
        assert_eq!(payload["batches"][0]["groupTitle"], "Friends");
        assert_eq!(payload["batches"][0]["groupRootNodeId"], group.root_node_id);
        assert_eq!(payload["batches"][0]["groupRootReady"], false);
    }

    #[tokio::test]
    async fn queued_group_events_are_decorated_with_the_current_group_root() {
        let (relay, users) = databases();
        observe_identity(&users, 42, Some("taek42"), "David").unwrap();
        let group = ensure_group(&users, -100, "Friends").unwrap();
        relay
            .execute(
                "INSERT INTO telegram_events(
                id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,text,
                created_at,session_kind,group_context_json
             ) VALUES('group-event',1,1,42,-100,'David','text','@kennedy hi',?1,'group','{}')",
                [Utc::now().to_rfc3339()],
            )
            .unwrap();
        let state = AppState {
            db: Arc::new(Mutex::new(relay)),
            users: Arc::new(Mutex::new(users)),
            bot: None,
            max_voice_bytes: 1024,
            bot_user_id: None,
            bot_username: None,
        };
        let Json(payload) = list_events(State(state)).await.unwrap();
        assert_eq!(payload["events"][0]["groupRootNodeId"], group.root_node_id);
        assert_eq!(
            payload["events"][0]["groupContext"]["groupRootNodeId"],
            group.root_node_id
        );
    }

    #[test]
    fn telegram_chunks_are_unicode_safe_and_bounded() {
        let text = format!("{} middle {}", "🧠".repeat(30), "z".repeat(80));
        let chunks = telegram_chunks(&text, 50);
        assert!(chunks.len() >= 2);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 50)
        );
        assert_eq!(
            chunks
                .concat()
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>(),
            text.chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>()
        );
    }

    #[test]
    fn telegram_http_timeout_exceeds_long_poll_timeout() {
        assert!(
            TELEGRAM_HTTP_TIMEOUT_SECONDS > u64::from(TELEGRAM_POLL_TIMEOUT_SECONDS),
            "the HTTP client must outlive each Telegram long poll"
        );
    }

    #[test]
    fn queue_returns_only_each_users_head_event() {
        let (db, _) = databases();
        let now = Utc::now().to_rfc3339();
        for (id, update, user) in [("a", 1, 42), ("b", 2, 42), ("c", 3, 77)] {
            db.execute(
                "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,text,created_at) VALUES(?1,?2,?2,?3,?3,'User','text','Hi',?4)",
                params![id, update, user, now],
            ).unwrap();
        }
        let mut statement = db.prepare("SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at,session_kind,group_context_json FROM telegram_events WHERE status<>'complete' ORDER BY update_id").unwrap();
        let queued = statement
            .query_map([], row_event)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut seen = HashSet::new();
        let heads = queued
            .into_iter()
            .filter(|event| seen.insert(event.telegram_user_id))
            .collect::<Vec<_>>();
        assert_eq!(
            heads
                .iter()
                .map(|event| event.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );
    }

    #[tokio::test]
    async fn pending_events_can_rebind_after_a_queued_reset_but_processing_events_cannot() {
        let (db, users) = databases();
        observe_identity(&users, 42, Some("taek42"), "David").unwrap();
        ensure_transport_user(&db, 42, Some("taek42"), "David", 42).unwrap();
        db.execute(
            "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,text,status,conversation_id,created_at) VALUES('event',1,1,42,42,'David','text','Hi','pending',?1,?2)",
            params!["019f5ca7-020f-7b63-be2f-82785fb68c03", Utc::now().to_rfc3339()],
        ).unwrap();
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            users: Arc::new(Mutex::new(users)),
            bot: None,
            max_voice_bytes: 1024,
            bot_user_id: None,
            bot_username: None,
        };
        let new_id = "029f5ca7-020f-7b63-be2f-82785fb68c03";
        let rebound = bind_event(
            State(state.clone()),
            Path("event".into()),
            Json(BindEvent {
                conversation_id: new_id.into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(rebound.0.status, "processing");
        assert_eq!(rebound.0.conversation_id.as_deref(), Some(new_id));
        let error = bind_event(
            State(state),
            Path("event".into()),
            Json(BindEvent {
                conversation_id: "039f5ca7-020f-7b63-be2f-82785fb68c03".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "state_conflict");
    }

    #[tokio::test]
    async fn binding_a_group_event_does_not_replace_the_private_session_pointer() {
        let (db, users) = databases();
        observe_identity(&users, 42, Some("taek42"), "David").unwrap();
        ensure_transport_user(&db, 42, Some("taek42"), "David", 42).unwrap();
        db.execute(
            "UPDATE authorized_users SET current_conversation_id=?1 WHERE telegram_user_id=42",
            ["019f5ca7-020f-7b63-be2f-82785fb68c03"],
        )
        .unwrap();
        db.execute(
            "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,text,created_at,session_kind) VALUES('group-event',1,1,42,-100,'David','text','@kennedy hi',?1,'group')",
            [Utc::now().to_rfc3339()],
        ).unwrap();
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            users: Arc::new(Mutex::new(users)),
            bot: None,
            max_voice_bytes: 1024,
            bot_user_id: None,
            bot_username: None,
        };
        let _ = bind_event(
            State(state.clone()),
            Path("group-event".into()),
            Json(BindEvent {
                conversation_id: "029f5ca7-020f-7b63-be2f-82785fb68c03".into(),
            }),
        )
        .await
        .unwrap();
        let private = state
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT current_conversation_id FROM authorized_users WHERE telegram_user_id=42",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap();
        assert_eq!(
            private.as_deref(),
            Some("019f5ca7-020f-7b63-be2f-82785fb68c03")
        );
    }

    #[test]
    fn existing_relay_schema_migrates_to_document_events() {
        let db = Connection::open_in_memory().unwrap();
        let legacy = INITIAL_MIGRATION
            .replace(
                "'text', 'voice', 'document', 'reset'",
                "'text', 'voice', 'reset'",
            )
            .replace("    file_name TEXT,\n", "");
        db.execute_batch(&legacy).unwrap();
        db.execute(
            "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,text,status,conversation_id,transcription,transcription_model,created_at) VALUES('queued',1,1,42,42,'David','voice','Hello','processing',?1,'Transcript','gpt-4o-transcribe',?2)",
            params![
                "019f5ca7-020f-7b63-be2f-82785fb68c03",
                Utc::now().to_rfc3339()
            ],
        )
        .unwrap();
        apply_migrations(&db).unwrap();
        let queued = fetch_event(&db, "queued").unwrap();
        assert_eq!(queued.status, "processing");
        assert_eq!(
            queued.conversation_id.as_deref(),
            Some("019f5ca7-020f-7b63-be2f-82785fb68c03")
        );
        assert_eq!(queued.transcription.as_deref(), Some("Transcript"));
        db.execute(
            "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,voice_bytes,mime_type,file_name,created_at) VALUES('doc',2,2,42,42,'David','document',X'01','application/pdf','notes.pdf',?1)",
            [Utc::now().to_rfc3339()],
        ).unwrap();
        let event = fetch_event(&db, "doc").unwrap();
        assert_eq!(event.kind, "document");
        assert_eq!(event.file_name.as_deref(), Some("notes.pdf"));
    }

    #[test]
    fn document_allowlist_accepts_supported_names_and_types() {
        assert!(supported_document(Some("report.PDF"), None));
        assert!(supported_document(None, Some("application/pdf")));
        assert!(supported_document(
            Some("sheet.xlsx"),
            Some("application/octet-stream")
        ));
        assert!(!supported_document(
            Some("archive.zip"),
            Some("application/zip")
        ));
    }
}
