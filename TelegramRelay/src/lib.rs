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
    prelude::*,
    requests::Request,
    types::{Message, Update, UpdateKind},
};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const UPDATE_ORDER_MIGRATION: &str = include_str!("../migrations/002_update_order.sql");
const UNAUTHORIZED_MESSAGE: &str =
    "Sorry, this Kennedy bot is private and your Telegram account is not authorized.";
const TELEGRAM_MESSAGE_LIMIT: usize = 4_000;
const TELEGRAM_POLL_TIMEOUT_SECONDS: u32 = 30;
const TELEGRAM_HTTP_TIMEOUT_SECONDS: u64 = 40;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: String,
    pub database: PathBuf,
    pub allowed_origins: Vec<String>,
    pub bot_token: Option<String>,
    pub bootstrap_usernames: Vec<String>,
    pub max_voice_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    bot: Option<Bot>,
    max_voice_bytes: usize,
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
    seed_bootstrap_users(&connection, &config.bootstrap_usernames)?;

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
    let state = AppState {
        db: Arc::new(Mutex::new(connection)),
        bot: bot.clone(),
        max_voice_bytes: config.max_voice_bytes,
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
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(address=%config.bind, enabled=bot.is_some(), "Telegram ready");

    if let Some(bot) = bot {
        bot.get_me()
            .send()
            .await
            .context("validating Telegram bot token")?;
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

fn seed_bootstrap_users(db: &Connection, usernames: &[String]) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    for username in usernames {
        let normalized = normalize_username(username);
        if normalized.is_empty() {
            anyhow::bail!("telegram bootstrap usernames must not be empty");
        }
        db.execute(
            "INSERT INTO authorized_users(bootstrap_username,updated_at) VALUES(?1,?2) ON CONFLICT(bootstrap_username) DO NOTHING",
            params![normalized, now],
        )?;
    }
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "service":"telegram-relay",
        "status":"ok",
        "telegram": if state.bot.is_some() { "ready" } else { "disabled" },
    }))
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
    })
}

fn fetch_event(db: &Connection, id: &str) -> Result<RelayEvent, ApiError> {
    db.query_row(
        "SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at FROM telegram_events WHERE id=?1",
        [id], row_event,
    ).optional().map_err(ApiError::internal)?.ok_or_else(ApiError::not_found)
}

async fn list_events(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let mut statement = db.prepare(
        "SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at FROM telegram_events WHERE status IN ('pending','processing') ORDER BY update_id",
    ).map_err(ApiError::internal)?;
    let queued = statement
        .query_map([], row_event)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    let mut seen = HashSet::new();
    let events = queued
        .into_iter()
        .filter(|event| seen.insert(event.telegram_user_id))
        .collect::<Vec<_>>();
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
    db.execute(
        "UPDATE authorized_users SET current_conversation_id=?1,updated_at=?2 WHERE telegram_user_id=?3",
        params![input.conversation_id, now, event.telegram_user_id],
    ).map_err(ApiError::internal)?;
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

async fn send_telegram_text(bot: &Bot, chat_id: i64, text: &str) -> Result<(), ApiError> {
    for chunk in telegram_chunks(text, TELEGRAM_MESSAGE_LIMIT) {
        bot.send_message(ChatId(chat_id), chunk)
            .send()
            .await
            .map_err(|error| {
                tracing::warn!(%chat_id, error=%error, "Telegram reply failed");
                ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    "telegram_send_failed",
                    "Telegram did not accept the reply.",
                )
            })?;
    }
    Ok(())
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
    send_telegram_text(bot, event.chat_id, input.text.trim()).await?;
    if let Some(warning) = input
        .context_warning
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        send_telegram_text(bot, event.chat_id, warning).await?;
    }
    let db = state.db.lock().map_err(ApiError::internal)?;
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
    send_telegram_text(bot, event.chat_id, message).await?;
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
    let UpdateKind::Message(message) = update.kind else {
        return Ok(());
    };
    if !message.chat.is_private() {
        return Ok(());
    }
    let Some(user) = message.from.as_ref() else {
        return Ok(());
    };
    let telegram_user_id =
        i64::try_from(user.id.0).context("Telegram user ID exceeds SQLite range")?;
    let username = user.username.clone();
    let display_name = user.full_name();
    let chat_id = message.chat.id.0;
    let authorized = {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("locking Telegram database"))?;
        authorize_user(
            &db,
            telegram_user_id,
            username.as_deref(),
            &display_name,
            chat_id,
        )?
    };
    if !authorized {
        bot.send_message(message.chat.id, UNAUTHORIZED_MESSAGE)
            .send()
            .await?;
        return Ok(());
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
        i64::from(update.id.0),
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

fn authorize_user(
    db: &Connection,
    telegram_user_id: i64,
    username: Option<&str>,
    display_name: &str,
    chat_id: i64,
) -> anyhow::Result<bool> {
    let now = Utc::now().to_rfc3339();
    let already_bound = db
        .query_row(
            "SELECT 1 FROM authorized_users WHERE telegram_user_id=?1",
            [telegram_user_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if already_bound {
        db.execute(
            "UPDATE authorized_users SET username=?1,display_name=?2,chat_id=?3,updated_at=?4 WHERE telegram_user_id=?5",
            params![username, display_name, chat_id, now, telegram_user_id],
        )?;
        return Ok(true);
    }
    let Some(username) = username
        .map(normalize_username)
        .filter(|value| !value.is_empty())
    else {
        return Ok(false);
    };
    let changed = db.execute(
        "UPDATE authorized_users SET telegram_user_id=?1,username=?2,display_name=?3,chat_id=?4,paired_at=?5,updated_at=?5 WHERE bootstrap_username=?6 AND telegram_user_id IS NULL",
        params![telegram_user_id, username, display_name, chat_id, now, username],
    )?;
    Ok(changed == 1)
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

    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        apply_migrations(&db).unwrap();
        seed_bootstrap_users(&db, &["@taek42".into()]).unwrap();
        db
    }

    #[test]
    fn username_bootstraps_once_then_numeric_id_is_authoritative() {
        let db = database();
        assert!(!authorize_user(&db, 7, Some("intruder"), "No", 7).unwrap());
        assert!(authorize_user(&db, 42, Some("TaEk42"), "David", 42).unwrap());
        assert!(authorize_user(&db, 42, Some("renamed"), "David", 42).unwrap());
        assert!(!authorize_user(&db, 43, Some("taek42"), "Other", 43).unwrap());
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
        let db = database();
        let now = Utc::now().to_rfc3339();
        for (id, update, user) in [("a", 1, 42), ("b", 2, 42), ("c", 3, 77)] {
            db.execute(
                "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,text,created_at) VALUES(?1,?2,?2,?3,?3,'User','text','Hi',?4)",
                params![id, update, user, now],
            ).unwrap();
        }
        let mut statement = db.prepare("SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at FROM telegram_events WHERE status<>'complete' ORDER BY update_id").unwrap();
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
        let db = database();
        authorize_user(&db, 42, Some("taek42"), "David", 42).unwrap();
        db.execute(
            "INSERT INTO telegram_events(id,update_id,message_id,telegram_user_id,chat_id,display_name,kind,text,status,conversation_id,created_at) VALUES('event',1,1,42,42,'David','text','Hi','pending',?1,?2)",
            params!["019f5ca7-020f-7b63-be2f-82785fb68c03", Utc::now().to_rfc3339()],
        ).unwrap();
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            bot: None,
            max_voice_bytes: 1024,
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
