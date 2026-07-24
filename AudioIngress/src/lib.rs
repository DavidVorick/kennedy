use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use kcode_audio_transcribe::{
    AudioTranscriber, JobState, RECONCILIATION_MODEL, RECONCILIATION_REASONING, Step, StepState,
    TRANSCRIPTION_MODEL, TranscriptionJob, TranscriptionStatus,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const RELEASE_DEFERRED_INGRESS_MIGRATION: &str =
    include_str!("../migrations/002_release_deferred_ingress.sql");
const TRANSCRIPTION_STATUS_MIGRATION: &str =
    include_str!("../migrations/003_transcription_status.sql");
const RETRY_ROUNDED_WAV_INTERVALS_MIGRATION: &str =
    include_str!("../migrations/004_retry_rounded_wav_intervals.sql");
const UNIFIED_INGRESS_QUEUE_MIGRATION: &str =
    include_str!("../migrations/005_unified_ingress_queue.sql");
pub const COMPLETION_PROTOCOL: &str = "end-session-v2";
const FAILURE_LIMIT: i64 = 5;
const INGRESS_RETRY_DELAY_SECONDS: i64 = 15;
const MAX_INGRESS_TOKENS: u64 = 50_000;
const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 4;

#[derive(Clone, Debug)]
pub struct Config {
    pub database: PathBuf,
    pub media_directory: PathBuf,
    pub max_upload_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    db: Arc<Mutex<Connection>>,
    transcriber: Option<AudioTranscriber>,
    jobs: Arc<Mutex<HashMap<String, TranscriptionJob>>>,
}

#[derive(Clone)]
pub struct Service {
    state: AppState,
    max_upload_bytes: usize,
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

struct TemporaryUpload(PathBuf);

impl Drop for TemporaryUpload {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
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
            "Audio recording or transcript piece not found.",
        )
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "state_conflict", message)
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error=%error, "audio ingress request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected audio-ingress error occurred.",
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
struct RecordingRecord {
    id: String,
    sha256: String,
    original_filename: String,
    content_type: String,
    size_bytes: i64,
    source_created_at: String,
    received_at: String,
    updated_at: String,
    status: String,
    gemini_model: String,
    reconciliation_model: String,
    reconciliation_reasoning: String,
    transcription_status: Option<Value>,
    attempt_count: i64,
    next_attempt_at: Option<String>,
    last_error: Option<String>,
    transcript_piece_count: i64,
    completed_piece_count: i64,
}

#[derive(Clone, Debug, Serialize)]
struct IngressPieceRecord {
    id: String,
    recording_id: String,
    sha256: String,
    original_filename: String,
    source_created_at: String,
    piece_index: i64,
    piece_count: i64,
    transcript_text: String,
    estimated_tokens: i64,
    phase: String,
    provenance_id: Option<String>,
    state: Value,
    version: i64,
    ingress_failure_count: i64,
    ingress_failures: Value,
    #[serde(skip_serializing)]
    next_attempt_at: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Clone, Debug, Serialize)]
struct ChunkHistoryRecord {
    chunk_index: i64,
    audio_start_ms: i64,
    audio_end_ms: i64,
    transcript: Option<Value>,
}

#[derive(Clone, Debug, Serialize)]
struct RecordingHistory {
    recording: RecordingRecord,
    final_transcript: Option<String>,
    chunks: Vec<ChunkHistoryRecord>,
    pieces: Vec<IngressPieceRecord>,
}

#[derive(Debug)]
struct WorkRecording {
    id: String,
    sha256: String,
    original_filename: String,
    source_created_at: String,
    original_relative_path: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default = "default_list_limit")]
    limit: usize,
}

fn default_list_limit() -> usize {
    100
}

#[derive(Deserialize)]
struct StartIngress {
    expected_version: i64,
    provenance_id: String,
    completion_protocol: Option<String>,
}

#[derive(Deserialize)]
struct CheckpointIngress {
    expected_version: i64,
    state: Value,
}

#[derive(Deserialize)]
struct VersionedTransition {
    expected_version: i64,
}

#[derive(Deserialize)]
struct RetryIngress {
    expected_version: i64,
    #[serde(default)]
    state: Option<Value>,
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

pub async fn open(
    config: Config,
    transcriber: Option<AudioTranscriber>,
) -> anyhow::Result<Service> {
    ensure!(
        config.max_upload_bytes > 0,
        "audio upload limit must be positive"
    );
    ensure_private_directory(&config.media_directory)?;
    let connection = Connection::open(&config.database)
        .with_context(|| format!("opening {}", config.database.display()))?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
    )?;
    apply_migrations(&connection).context("applying audio-ingress migrations")?;
    let completed_recording_ids = {
        let mut statement = connection
            .prepare("SELECT id FROM audio_recordings WHERE status='complete'")
            .context("selecting completed recordings for shard cleanup")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for recording_id in completed_recording_ids {
        if let Err(error) =
            remove_completed_chunk_files(&config.media_directory, &recording_id).await
        {
            tracing::warn!(%recording_id, %error, "Could not remove completed audio shards");
        }
    }

    let state = AppState {
        config: Arc::new(config),
        db: Arc::new(Mutex::new(connection)),
        transcriber,
        jobs: Arc::new(Mutex::new(HashMap::new())),
    };
    let max_upload_bytes = state.config.max_upload_bytes;
    tracing::info!(media=%state.config.media_directory.display(), "Audio ingress initialized");
    tokio::spawn(worker_loop(state.clone()));
    Ok(Service {
        state,
        max_upload_bytes,
    })
}

pub fn router(service: Service) -> Router {
    Router::new()
        .route("/api/v1/audio-ingress/health", get(health))
        .route(
            "/api/v1/audio-ingress",
            get(list_recordings).post(upload_recording),
        )
        .route(
            "/api/v1/audio-ingress/by-sha256/{sha256}",
            get(recording_by_sha256),
        )
        .route(
            "/api/v1/audio-ingress/{recording_id}/history",
            get(get_recording_history),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/retry-ingress",
            post(retry_ingress),
        )
        .layer(DefaultBodyLimit::max(service.max_upload_bytes))
        .with_state(service.state)
}

impl Service {
    pub fn health(&self) -> Result<Value, ServiceError> {
        let db = self.state.db.lock().map_err(ApiError::internal)?;
        db.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
            .map_err(ApiError::internal)?;
        Ok(json!({"service":"audio-ingress","status":"ok"}))
    }

    pub fn next_ingress_piece(&self) -> Result<Option<Value>, ServiceError> {
        let db = self.state.db.lock().map_err(ApiError::internal)?;
        select_next_ingress_piece(&db)?.map(json_value).transpose()
    }

    pub async fn get_json(&self, path: &str) -> Result<Value, ServiceError> {
        if path == "/api/v1/audio-ingress/health" {
            return self.health();
        }
        let id = path
            .strip_prefix("/api/v1/audio-ingress/pieces/")
            .filter(|id| !id.contains('/'))
            .ok_or_else(ApiError::not_found)?;
        let Json(piece) = get_ingress_piece(State(self.state.clone()), AxumPath(id.into())).await?;
        json_value(piece)
    }

    pub async fn post_json(&self, path: &str, body: Value) -> Result<Value, ServiceError> {
        if path == "/api/v1/audio-ingress/ingress/repairs/release" {
            let Json(value) = release_ingress_repairs(State(self.state.clone())).await?;
            return Ok(value);
        }
        let tail = path
            .strip_prefix("/api/v1/audio-ingress/pieces/")
            .ok_or_else(ApiError::not_found)?;
        let (id, action) = tail.split_once('/').ok_or_else(ApiError::not_found)?;
        let state = State(self.state.clone());
        let Json(piece) = match action {
            "ingress-started" => {
                ingress_started(state, AxumPath(id.into()), Json(parse_body(body)?)).await?
            }
            "ingress-completed" => {
                ingress_completed(state, AxumPath(id.into()), Json(parse_body(body)?)).await?
            }
            "ingress-failure" => {
                ingress_failure(state, AxumPath(id.into()), Json(parse_body(body)?)).await?
            }
            "retry-ingress" => {
                retry_ingress(state, AxumPath(id.into()), Json(parse_body(body)?)).await?
            }
            _ => return Err(ApiError::not_found().into()),
        };
        json_value(piece)
    }

    pub async fn put_json(&self, path: &str, body: Value) -> Result<Value, ServiceError> {
        let id = path
            .strip_prefix("/api/v1/audio-ingress/pieces/")
            .and_then(|tail| tail.strip_suffix("/ingress-checkpoint"))
            .ok_or_else(ApiError::not_found)?;
        let Json(piece) = ingress_checkpoint(
            State(self.state.clone()),
            AxumPath(id.into()),
            Json(parse_body(body)?),
        )
        .await?;
        json_value(piece)
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
    let database_ready = state
        .db
        .lock()
        .ok()
        .and_then(|db| {
            db.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
                .ok()
        })
        .is_some();
    let status = if database_ready { "ok" } else { "unavailable" };
    let http = if database_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        http,
        Json(json!({
            "service":"audio-ingress",
            "status":status,
            "transcriber":if state.transcriber.is_some() { "ready" } else { "unconfigured" },
            "gemini_model":TRANSCRIPTION_MODEL,
            "reconciliation_model":RECONCILIATION_MODEL,
            "input":"bytes",
        })),
    )
        .into_response()
}

fn apply_migrations(connection: &Connection) -> rusqlite::Result<()> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version < 1 {
        connection.execute_batch(INITIAL_MIGRATION)?;
    }
    if version < 2 {
        connection.execute_batch(RELEASE_DEFERRED_INGRESS_MIGRATION)?;
    }
    if version < 3 {
        connection.execute_batch(TRANSCRIPTION_STATUS_MIGRATION)?;
    }
    if version < 4 {
        connection.execute_batch(RETRY_ROUNDED_WAV_INTERVALS_MIGRATION)?;
    }
    if version < 5 {
        connection.execute_batch(UNIFIED_INGRESS_QUEUE_MIGRATION)?;
    }
    Ok(())
}

fn ensure_private_directory(path: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("setting private permissions on {}", path.display()))?;
    }
    Ok(())
}

async fn remove_completed_chunk_files(
    media_directory: &Path,
    recording_id: &str,
) -> anyhow::Result<()> {
    let canonical_id = Uuid::parse_str(recording_id)
        .with_context(|| format!("invalid audio recording identifier {recording_id:?}"))?
        .to_string();
    ensure!(
        canonical_id == recording_id,
        "audio recording identifier is not canonical"
    );
    let directory = media_directory.join("chunks").join(canonical_id);
    match tokio::fs::remove_dir_all(&directory).await {
        Ok(()) => {
            tracing::info!(%recording_id, path=%directory.display(), "Removed completed audio shards");
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", directory.display())),
    }
}

fn set_private_file(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("setting private permissions on {}", path.display()))?;
    }
    Ok(())
}

fn sync_file(path: &Path) -> anyhow::Result<()> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing {}", path.display()))
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    fs::File::open(path)
        .with_context(|| format!("opening directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing directory {}", path.display()))?;
    Ok(())
}

async fn upload_recording(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let upload_id = Uuid::new_v4();
    let temporary = TemporaryUpload(
        state
            .config
            .media_directory
            .join(format!(".upload-{upload_id}.tmp")),
    );
    let temporary_path = temporary.0.clone();
    let mut recorded_at = None;
    let mut original_filename = None;
    let mut content_type = "application/octet-stream".to_owned();
    let mut size_bytes = 0_u64;
    let mut sha = Sha256::new();
    let mut uploaded = false;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|_| ApiError::bad("The multipart upload could not be read."))?
    {
        match field.name() {
            Some("recorded_at") => {
                let value = field
                    .text()
                    .await
                    .map_err(|_| ApiError::bad("recorded_at could not be read."))?;
                recorded_at = Some(canonical_timestamp(&value)?);
            }
            Some("file") => {
                if uploaded {
                    let _ = tokio::fs::remove_file(&temporary_path).await;
                    return Err(ApiError::bad("Exactly one audio file is required."));
                }
                original_filename = Some(safe_filename(field.file_name()));
                content_type = field
                    .content_type()
                    .map(str::to_owned)
                    .unwrap_or_else(|| "application/octet-stream".to_owned());
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary_path)
                    .await
                    .map_err(ApiError::internal)?;
                set_private_file(&temporary_path).map_err(ApiError::internal)?;
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|_| ApiError::bad("The audio file upload was interrupted."))?
                {
                    size_bytes = size_bytes.saturating_add(chunk.len() as u64);
                    if size_bytes > state.config.max_upload_bytes as u64 {
                        drop(file);
                        let _ = tokio::fs::remove_file(&temporary_path).await;
                        return Err(ApiError::new(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "audio_too_large",
                            "The audio recording exceeds Kennedy's configured upload limit.",
                        ));
                    }
                    sha.update(&chunk);
                    file.write_all(&chunk).await.map_err(ApiError::internal)?;
                }
                file.sync_all().await.map_err(ApiError::internal)?;
                uploaded = true;
            }
            _ => {}
        }
    }

    if !uploaded || size_bytes == 0 {
        let _ = tokio::fs::remove_file(&temporary_path).await;
        return Err(ApiError::bad(
            "A non-empty multipart field named file is required.",
        ));
    }
    let recorded_at = recorded_at.ok_or_else(|| {
        let _ = fs::remove_file(&temporary_path);
        ApiError::bad("recorded_at is required and must describe when recording began.")
    })?;
    let original_filename = original_filename.unwrap_or_else(|| "vnote.wav".to_owned());
    let sha256 = format!("{:x}", sha.finalize());

    if let Some(existing) = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        fetch_recording_by_sha(&db, &sha256)?
    } {
        let _ = tokio::fs::remove_file(&temporary_path).await;
        return Ok((
            StatusCode::OK,
            Json(json!({"recording":existing,"deduplicated":true})),
        ));
    }

    let relative_path = format!("originals/{sha256}.wav");
    let final_path = state.config.media_directory.join(&relative_path);
    if let Some(parent) = final_path.parent() {
        ensure_private_directory(parent).map_err(ApiError::internal)?;
    }
    if final_path.exists() {
        tokio::fs::remove_file(&temporary_path)
            .await
            .map_err(ApiError::internal)?;
    } else {
        tokio::fs::rename(&temporary_path, &final_path)
            .await
            .map_err(ApiError::internal)?;
    }
    set_private_file(&final_path).map_err(ApiError::internal)?;
    sync_file(&final_path).map_err(ApiError::internal)?;
    if let Some(parent) = final_path.parent() {
        sync_directory(parent).map_err(ApiError::internal)?;
    }
    sync_directory(&state.config.media_directory).map_err(ApiError::internal)?;

    let id = Uuid::new_v4().to_string();
    let now = Utc::now().to_rfc3339();
    let insert = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        db.execute(
            "INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?8,'uploaded',?9,?10,?11)",
            params![id,sha256,original_filename,content_type,size_bytes as i64,recorded_at,now,relative_path,TRANSCRIPTION_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING],
        )
    };
    if let Err(error) = insert {
        if let Some(existing) = {
            let db = state.db.lock().map_err(ApiError::internal)?;
            fetch_recording_by_sha(&db, &sha256)?
        } {
            return Ok((
                StatusCode::OK,
                Json(json!({"recording":existing,"deduplicated":true})),
            ));
        }
        return Err(ApiError::internal(error));
    }
    let recording = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        fetch_recording(&db, &id)?
    };
    tracing::info!(recording_id=%id, %sha256, bytes=size_bytes, source_created_at=%recorded_at, "Durably accepted audio recording");
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"recording":recording,"deduplicated":false})),
    ))
}

fn canonical_timestamp(value: &str) -> Result<String, ApiError> {
    DateTime::parse_from_rfc3339(value.trim())
        .map(|value| value.to_rfc3339())
        .map_err(|_| ApiError::bad("recorded_at must be an RFC 3339 timestamp with an offset."))
}

fn safe_filename(value: Option<&str>) -> String {
    let name = value
        .and_then(|value| Path::new(value).file_name())
        .and_then(|value| value.to_str())
        .unwrap_or("vnote.wav");
    let clean = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .take(200)
        .collect::<String>();
    if clean.is_empty() {
        "vnote.wav".to_owned()
    } else {
        clean
    }
}

fn recording_select() -> &'static str {
    "SELECT r.id,r.sha256,r.original_filename,r.content_type,r.size_bytes,r.source_created_at,r.received_at,r.updated_at,r.status,r.gemini_model,r.reconciliation_model,r.reconciliation_reasoning,r.transcription_status_json,r.attempt_count,r.next_attempt_at,r.last_error,(SELECT COUNT(*) FROM audio_ingress_pieces p WHERE p.recording_id=r.id),(SELECT COUNT(*) FROM audio_ingress_pieces p WHERE p.recording_id=r.id AND p.phase='complete') FROM audio_recordings r"
}

fn row_recording(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordingRecord> {
    Ok(RecordingRecord {
        id: row.get(0)?,
        sha256: row.get(1)?,
        original_filename: row.get(2)?,
        content_type: row.get(3)?,
        size_bytes: row.get(4)?,
        source_created_at: row.get(5)?,
        received_at: row.get(6)?,
        updated_at: row.get(7)?,
        status: row.get(8)?,
        gemini_model: row.get(9)?,
        reconciliation_model: row.get(10)?,
        reconciliation_reasoning: row.get(11)?,
        transcription_status: row
            .get::<_, Option<String>>(12)?
            .and_then(|value| serde_json::from_str(&value).ok()),
        attempt_count: row.get(13)?,
        next_attempt_at: row.get(14)?,
        last_error: row.get(15)?,
        transcript_piece_count: row.get(16)?,
        completed_piece_count: row.get(17)?,
    })
}

fn fetch_recording(db: &Connection, id: &str) -> Result<RecordingRecord, ApiError> {
    db.query_row(
        &format!("{} WHERE r.id=?1", recording_select()),
        [id],
        row_recording,
    )
    .optional()
    .map_err(ApiError::internal)?
    .ok_or_else(ApiError::not_found)
}

fn fetch_recording_by_sha(
    db: &Connection,
    sha256: &str,
) -> Result<Option<RecordingRecord>, ApiError> {
    db.query_row(
        &format!("{} WHERE r.sha256=?1", recording_select()),
        [sha256],
        row_recording,
    )
    .optional()
    .map_err(ApiError::internal)
}

fn fetch_recording_history(db: &Connection, id: &str) -> Result<RecordingHistory, ApiError> {
    let recording = fetch_recording(db, id)?;
    let final_transcript = db
        .query_row(
            "SELECT final_transcript FROM audio_recordings WHERE id=?1",
            [id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let mut chunk_statement = db
        .prepare(
            "SELECT chunk_index,audio_start_ms,audio_end_ms,transcript_json \
             FROM audio_chunks WHERE recording_id=?1 ORDER BY chunk_index",
        )
        .map_err(ApiError::internal)?;
    let chunks = chunk_statement
        .query_map([id], |row| {
            let transcript_json: Option<String> = row.get(3)?;
            let transcript = transcript_json
                .map(|value| {
                    serde_json::from_str(&value).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })
                })
                .transpose()?;
            Ok(ChunkHistoryRecord {
                chunk_index: row.get(0)?,
                audio_start_ms: row.get(1)?,
                audio_end_ms: row.get(2)?,
                transcript,
            })
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;

    let mut piece_statement = db
        .prepare(&format!(
            "{} WHERE p.recording_id=?1 ORDER BY p.piece_index",
            piece_select()
        ))
        .map_err(ApiError::internal)?;
    let pieces = piece_statement
        .query_map([id], row_piece)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;

    Ok(RecordingHistory {
        recording,
        final_transcript,
        chunks,
        pieces,
    })
}

async fn get_recording_history(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<RecordingHistory>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(fetch_recording_history(&db, &id)?))
}

async fn recording_by_sha256(
    State(state): State<AppState>,
    AxumPath(sha256): AxumPath<String>,
) -> Result<Json<RecordingRecord>, ApiError> {
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::bad(
            "sha256 must contain 64 hexadecimal characters.",
        ));
    }
    let db = state.db.lock().map_err(ApiError::internal)?;
    fetch_recording_by_sha(&db, &sha256.to_ascii_lowercase())?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn list_recordings(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 50_000) as i64;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let recordings = fetch_recordings(&db, limit)?;
    Ok(Json(json!({"recordings":recordings})))
}

fn fetch_recordings(db: &Connection, limit: i64) -> Result<Vec<RecordingRecord>, ApiError> {
    let mut statement = db
        .prepare(&format!(
            "{} ORDER BY datetime(r.source_created_at) DESC,datetime(r.received_at) DESC,r.id DESC LIMIT ?1",
            recording_select()
        ))
        .map_err(ApiError::internal)?;
    statement
        .query_map([limit], row_recording)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)
}

async fn worker_loop(state: AppState) {
    loop {
        let worked = match process_next_recording(&state).await {
            Ok(worked) => worked,
            Err(error) => {
                tracing::error!(error=%error, "Audio-ingress worker iteration failed");
                false
            }
        };
        tokio::time::sleep(if worked {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(5)
        })
        .await;
    }
}

async fn process_next_recording(state: &AppState) -> anyhow::Result<bool> {
    let recording = {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
        fetch_work_recording(&db)?
    };
    let Some(recording) = recording else {
        return Ok(false);
    };
    let recording_id = recording.id.clone();
    let stage = recording.status.clone();
    let result = match recording.status.as_str() {
        "uploaded" | "chunking" | "transcribing" | "reconciling" => {
            poll_transcription(state, &recording).await
        }
        _ => Ok(()),
    };
    match result {
        Ok(()) => Ok(true),
        Err(error) => {
            if terminal_processing_failure(&stage, &error) {
                record_terminal_processing_failure(
                    state,
                    &recording_id,
                    &stage,
                    &error.to_string(),
                )?;
                tracing::error!(%recording_id, %stage, error=%error, "Audio processing failed permanently");
            } else {
                record_processing_failure(state, &recording_id, &stage, &error.to_string())?;
                tracing::warn!(%recording_id, %stage, error=%error, "Audio processing will retry");
            }
            Ok(true)
        }
    }
}

fn terminal_processing_failure(stage: &str, error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .to_string()
            .starts_with("terminal audio transcription:")
    }) || (matches!(stage, "uploaded" | "chunking")
        && error
            .chain()
            .any(|cause| cause.to_string().starts_with("invalid WAV recording")))
}

fn record_terminal_processing_failure(
    state: &AppState,
    recording_id: &str,
    stage: &str,
    error: &str,
) -> anyhow::Result<()> {
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
    let concise = format!(
        "{stage}: {}",
        concise_text(error, 2_000, "invalid WAV recording")
    );
    db.execute(
        "UPDATE audio_recordings SET status='failed',attempt_count=attempt_count+1,next_attempt_at=NULL,last_error=?1,updated_at=?2 WHERE id=?3",
        params![concise,Utc::now().to_rfc3339(),recording_id],
    )?;
    Ok(())
}

fn fetch_work_recording(db: &Connection) -> anyhow::Result<Option<WorkRecording>> {
    db.query_row(
        "SELECT id,sha256,original_filename,source_created_at,original_relative_path,status FROM audio_recordings WHERE status IN ('uploaded','chunking','transcribing','reconciling') AND (next_attempt_at IS NULL OR datetime(next_attempt_at)<=datetime('now')) ORDER BY datetime(received_at),id LIMIT 1",
        [],
        |row| Ok(WorkRecording {
            id: row.get(0)?,
            sha256: row.get(1)?,
            original_filename: row.get(2)?,
            source_created_at: row.get(3)?,
            original_relative_path: row.get(4)?,
            status: row.get(5)?,
        }),
    )
    .optional()
    .context("selecting audio-ingress work")
}

fn record_processing_failure(
    state: &AppState,
    recording_id: &str,
    stage: &str,
    error: &str,
) -> anyhow::Result<()> {
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
    let attempts: i64 = db.query_row(
        "SELECT attempt_count FROM audio_recordings WHERE id=?1",
        [recording_id],
        |row| row.get(0),
    )?;
    let delay_minutes = 1_i64
        .checked_shl(attempts.min(6) as u32)
        .unwrap_or(60)
        .min(60);
    let next = (Utc::now() + ChronoDuration::minutes(delay_minutes)).to_rfc3339();
    let concise = format!("{stage}: {}", concise_text(error, 2_000, "unknown error"));
    db.execute(
        "UPDATE audio_recordings SET attempt_count=attempt_count+1,next_attempt_at=?1,last_error=?2,updated_at=?3 WHERE id=?4",
        params![next,concise,Utc::now().to_rfc3339(),recording_id],
    )?;
    Ok(())
}

async fn poll_transcription(state: &AppState, recording: &WorkRecording) -> anyhow::Result<()> {
    let existing = state
        .jobs
        .lock()
        .map_err(|_| anyhow::anyhow!("audio transcription job lock was poisoned"))?
        .get(&recording.id)
        .cloned();
    let job = if let Some(job) = existing {
        job
    } else {
        let transcriber = state.transcriber.as_ref().context(
            "Audio transcription is not configured; store gemini-api-key in Kennedy's vault",
        )?;
        let source = state
            .config
            .media_directory
            .join(&recording.original_relative_path);
        let audio = tokio::fs::read(&source)
            .await
            .with_context(|| format!("reading retained audio original {}", source.display()))?;
        let job = transcriber.transcribe(audio);
        state
            .jobs
            .lock()
            .map_err(|_| anyhow::anyhow!("audio transcription job lock was poisoned"))?
            .insert(recording.id.clone(), job.clone());
        tracing::info!(
            recording_id = %recording.id,
            model = TRANSCRIPTION_MODEL,
            reconciliation_model = RECONCILIATION_MODEL,
            "Started byte-only audio transcription job"
        );
        job
    };

    let snapshot = job.status();
    persist_transcription_status(state, &recording.id, &snapshot)?;
    match snapshot.state {
        JobState::Queued | JobState::Running => Ok(()),
        JobState::Completed => {
            remove_transcription_job(state, &recording.id)?;
            let transcript = snapshot
                .transcript
                .as_deref()
                .context("completed audio transcription omitted its transcript")?;
            prepare_transcript_ingress(state, recording, transcript, &snapshot)
        }
        JobState::Failed => {
            remove_transcription_job(state, &recording.id)?;
            let error = snapshot
                .steps
                .iter()
                .find(|step| step.state == StepState::Failed)
                .and_then(|step| step.error.as_ref());
            let message = error
                .map(|error| error.message.as_str())
                .unwrap_or("audio transcription failed without detail");
            if error.is_some_and(|error| !error.retryable) {
                anyhow::bail!("terminal audio transcription: {message}");
            }
            anyhow::bail!("audio transcription job failed: {message}");
        }
    }
}

fn remove_transcription_job(state: &AppState, recording_id: &str) -> anyhow::Result<()> {
    state
        .jobs
        .lock()
        .map_err(|_| anyhow::anyhow!("audio transcription job lock was poisoned"))?
        .remove(recording_id);
    Ok(())
}

fn persist_transcription_status(
    state: &AppState,
    recording_id: &str,
    snapshot: &TranscriptionStatus,
) -> anyhow::Result<()> {
    let stage = transcription_stage(snapshot);
    let serialized = serde_json::to_string(snapshot)?;
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
    db.execute(
        "UPDATE audio_recordings SET status=?1,transcription_status_json=?2,updated_at=?3,next_attempt_at=NULL,last_error=NULL WHERE id=?4 AND (status<>?1 OR COALESCE(transcription_status_json,'')<>?2 OR next_attempt_at IS NOT NULL OR last_error IS NOT NULL)",
        params![stage,serialized,Utc::now().to_rfc3339(),recording_id],
    )?;
    Ok(())
}

fn transcription_stage(snapshot: &TranscriptionStatus) -> &'static str {
    let plan_complete = snapshot
        .steps
        .iter()
        .any(|entry| entry.step == Step::PlanChunks && entry.state == StepState::Completed);
    if !plan_complete {
        return "chunking";
    }
    let chunks_complete = snapshot
        .steps
        .iter()
        .filter(|entry| matches!(entry.step, Step::TranscribeChunk { .. }))
        .all(|entry| entry.state == StepState::Completed);
    if !chunks_complete {
        "transcribing"
    } else {
        "reconciling"
    }
}

fn prepare_transcript_ingress(
    state: &AppState,
    recording: &WorkRecording,
    transcript: &str,
    snapshot: &TranscriptionStatus,
) -> anyhow::Result<()> {
    ensure!(
        !transcript.trim().is_empty(),
        "completed transcription is empty"
    );
    let body_pieces = split_transcript_body(recording, transcript)?;
    let piece_count = body_pieces.len();
    let pieces = body_pieces
        .into_iter()
        .enumerate()
        .map(|(index, body)| {
            format!(
                "{}\n\n{}",
                transcript_header(recording, Some((index, piece_count))),
                body
            )
        })
        .collect::<Vec<_>>();
    ensure!(
        pieces
            .iter()
            .all(|piece| estimate_tokens(piece) <= MAX_INGRESS_TOKENS),
        "prepared transcript piece exceeds Kennedy's ingress limit"
    );
    let final_transcript = format!(
        "{}\n\n{}",
        transcript_header(recording, None),
        transcript.trim()
    );
    let status_json = serde_json::to_string(snapshot)?;
    let now = Utc::now().to_rfc3339();
    let mut db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
    let tx = db.transaction()?;
    tx.execute(
        "DELETE FROM audio_ingress_pieces WHERE recording_id=?1",
        [&recording.id],
    )?;
    tx.execute(
        "DELETE FROM audio_chunks WHERE recording_id=?1",
        [&recording.id],
    )?;
    for (index, piece) in pieces.iter().enumerate() {
        let piece_id = Uuid::new_v4().to_string();
        tx.execute(
            "INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,'ingress_pending',?6,?6)",
            params![piece_id,recording.id,index as i64,piece,estimate_tokens(piece) as i64,now],
        )?;
    }
    tx.execute(
        "UPDATE audio_recordings SET status='ready_for_ingress',final_transcript=?1,transcription_status_json=?2,attempt_count=0,next_attempt_at=NULL,last_error=NULL,updated_at=?3 WHERE id=?4",
        params![final_transcript,status_json,now,recording.id],
    )?;
    tx.commit()?;
    tracing::info!(
        recording_id = %recording.id,
        pieces = pieces.len(),
        model = RECONCILIATION_MODEL,
        reasoning = RECONCILIATION_REASONING,
        "Prepared library transcript for Kennedy ingress"
    );
    Ok(())
}

fn transcript_header(recording: &WorkRecording, piece: Option<(usize, usize)>) -> String {
    let mut header = format!(
        "# Audio transcript\n\n- Began: {}\n- SHA-256: {}\n- Original filename: {}\n- Transcription model: {}\n- Reconciliation model: {} ({})",
        recording.source_created_at,
        recording.sha256,
        recording.original_filename,
        TRANSCRIPTION_MODEL,
        RECONCILIATION_MODEL,
        RECONCILIATION_REASONING,
    );
    if let Some((index, total)) = piece {
        header.push_str(&format!(
            "\n- Transcript piece: {} of {}",
            index.saturating_add(1),
            total
        ));
    }
    header
}

fn split_transcript_body(
    recording: &WorkRecording,
    transcript: &str,
) -> anyhow::Result<Vec<String>> {
    let maximum_characters = usize::try_from(MAX_INGRESS_TOKENS * ESTIMATED_CHARACTERS_PER_TOKEN)?;
    let largest_header = transcript_header(recording, Some((usize::MAX, usize::MAX)));
    let body_limit = maximum_characters
        .checked_sub(largest_header.chars().count() + 2)
        .context("audio transcript metadata exceeds the ingress limit")?;
    ensure!(
        body_limit > 0,
        "audio transcript has no room after metadata"
    );
    let mut remaining = transcript.trim();
    let mut pieces = Vec::new();
    while remaining.chars().count() > body_limit {
        let cutoff = remaining
            .char_indices()
            .nth(body_limit)
            .map(|(index, _)| index)
            .unwrap_or(remaining.len());
        let prefix = &remaining[..cutoff];
        let minimum_boundary = prefix
            .char_indices()
            .nth(body_limit / 2)
            .map(|(index, _)| index)
            .unwrap_or(0);
        let boundary = prefix
            .rfind("\n\n")
            .filter(|index| *index >= minimum_boundary)
            .or_else(|| {
                prefix
                    .rfind('\n')
                    .filter(|index| *index >= minimum_boundary)
            })
            .unwrap_or(cutoff);
        let piece = remaining[..boundary].trim();
        ensure!(
            !piece.is_empty(),
            "could not split oversized audio transcript"
        );
        pieces.push(piece.to_owned());
        remaining = remaining[boundary..].trim();
    }
    if !remaining.is_empty() {
        pieces.push(remaining.to_owned());
    }
    ensure!(!pieces.is_empty(), "audio transcript is empty");
    Ok(pieces)
}

fn estimate_tokens(value: &str) -> u64 {
    (value.chars().count() as u64).div_ceil(ESTIMATED_CHARACTERS_PER_TOKEN)
}

fn concise_text(value: &str, limit: usize, fallback: &str) -> String {
    let clean = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let bounded = clean.chars().take(limit).collect::<String>();
    if bounded.is_empty() {
        fallback.to_owned()
    } else {
        bounded
    }
}

fn piece_select() -> &'static str {
    "SELECT p.id,p.recording_id,r.sha256,r.original_filename,r.source_created_at,p.piece_index,(SELECT COUNT(*) FROM audio_ingress_pieces count_p WHERE count_p.recording_id=p.recording_id),p.transcript_text,p.estimated_tokens,p.phase,p.provenance_id,p.state_json,p.version,p.ingress_failure_count,p.ingress_failures_json,p.next_attempt_at,p.created_at,p.updated_at FROM audio_ingress_pieces p JOIN audio_recordings r ON r.id=p.recording_id"
}

fn row_piece(row: &rusqlite::Row<'_>) -> rusqlite::Result<IngressPieceRecord> {
    let state_json: String = row.get(11)?;
    let failures_json: String = row.get(14)?;
    let piece_state = serde_json::from_str(&state_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(IngressPieceRecord {
        id: row.get(0)?,
        recording_id: row.get(1)?,
        sha256: row.get(2)?,
        original_filename: row.get(3)?,
        source_created_at: row.get(4)?,
        piece_index: row.get(5)?,
        piece_count: row.get(6)?,
        transcript_text: row.get(7)?,
        estimated_tokens: row.get(8)?,
        phase: row.get(9)?,
        provenance_id: row.get(10)?,
        state: piece_state,
        version: row.get(12)?,
        ingress_failure_count: row.get(13)?,
        ingress_failures: serde_json::from_str(&failures_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                14,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        next_attempt_at: row.get(15)?,
        created_at: row.get(16)?,
        updated_at: row.get(17)?,
    })
}

fn fetch_piece(db: &Connection, id: &str) -> Result<IngressPieceRecord, ApiError> {
    db.query_row(
        &format!("{} WHERE p.id=?1", piece_select()),
        [id],
        row_piece,
    )
    .optional()
    .map_err(ApiError::internal)?
    .ok_or_else(ApiError::not_found)
}

fn select_next_ingress_piece(db: &Connection) -> Result<Option<IngressPieceRecord>, ApiError> {
    db.query_row(
        &format!(
            "{} WHERE p.phase IN ('ingress_in_progress','ingress_pending')
             AND (p.next_attempt_at IS NULL OR datetime(p.next_attempt_at)<=datetime('now'))
             ORDER BY CASE p.phase WHEN 'ingress_in_progress' THEN 0 ELSE 1 END,
                      datetime(r.source_created_at),p.piece_index,p.id
             LIMIT 1",
            piece_select()
        ),
        [],
        row_piece,
    )
    .optional()
    .map_err(ApiError::internal)
}

fn validate_expected_version(version: i64) -> Result<(), ApiError> {
    if version < 1 {
        Err(ApiError::bad("expected_version must be positive."))
    } else {
        Ok(())
    }
}

fn project_piece_transition(db: &Connection, id: &str) -> Result<IngressPieceRecord, ApiError> {
    let piece = fetch_piece(db, id)?;
    let remaining = db
        .query_row(
            "SELECT COUNT(*) FROM audio_ingress_pieces WHERE recording_id=?1 AND phase<>'complete'",
            [&piece.recording_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(ApiError::internal)?;
    let (status, next_attempt_at, last_error) = match piece.phase.as_str() {
        "ingress_in_progress" => ("ingressing", None, None),
        "ingress_failed" => (
            "ingress_failed",
            None,
            Some(format!(
                "Transcript piece {} requires manual ingress retry",
                piece.piece_index + 1
            )),
        ),
        "complete" if remaining == 0 => ("complete", None, None),
        _ => ("ready_for_ingress", piece.next_attempt_at.clone(), None),
    };
    db.execute(
        "UPDATE audio_recordings SET status=?1,next_attempt_at=?2,last_error=?3,updated_at=?4 WHERE id=?5",
        params![
            status,
            next_attempt_at,
            last_error,
            piece.updated_at,
            piece.recording_id
        ],
    )
    .map_err(ApiError::internal)?;
    fetch_piece(db, id)
}

fn start_ingress_transition(
    db: &mut Connection,
    id: &str,
    input: &StartIngress,
) -> Result<IngressPieceRecord, ApiError> {
    validate_expected_version(input.expected_version)?;
    if input.provenance_id.trim().is_empty() {
        return Err(ApiError::bad("provenance_id must not be empty."));
    }
    if input.completion_protocol.as_deref() != Some(COMPLETION_PROTOCOL) {
        return Err(ApiError::conflict(
            "This client does not support the required explicit history-ingress completion protocol.",
        ));
    }
    let existing = fetch_piece(db, id)?;
    if existing.phase == "ingress_in_progress"
        && existing.provenance_id.as_deref() == Some(input.provenance_id.as_str())
    {
        return project_piece_transition(db, id);
    }
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx
        .execute(
            "UPDATE audio_ingress_pieces
             SET phase='ingress_in_progress',provenance_id=?1,next_attempt_at=NULL,updated_at=?2,version=version+1
             WHERE id=?3 AND phase='ingress_pending' AND version=?4
               AND NOT EXISTS(SELECT 1 FROM audio_ingress_pieces WHERE phase='ingress_in_progress')",
            params![
                input.provenance_id,
                Utc::now().to_rfc3339(),
                id,
                input.expected_version
            ],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Another job is active or this job is not ready to start memory ingress.",
        ));
    }
    let piece = project_piece_transition(&tx, id)?;
    tx.commit().map_err(ApiError::internal)?;
    Ok(piece)
}

fn checkpoint_ingress_transition(
    db: &mut Connection,
    id: &str,
    input: &CheckpointIngress,
) -> Result<IngressPieceRecord, ApiError> {
    validate_expected_version(input.expected_version)?;
    let state_json = serde_json::to_string(&input.state).map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx
        .execute(
            "UPDATE audio_ingress_pieces SET state_json=?1,updated_at=?2,version=version+1
             WHERE id=?3 AND phase='ingress_in_progress' AND version=?4",
            params![
                state_json,
                Utc::now().to_rfc3339(),
                id,
                input.expected_version
            ],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Memory ingress changed in another session or is no longer in progress.",
        ));
    }
    let piece = project_piece_transition(&tx, id)?;
    tx.commit().map_err(ApiError::internal)?;
    Ok(piece)
}

fn complete_ingress_transition(
    db: &mut Connection,
    id: &str,
    input: &VersionedTransition,
) -> Result<IngressPieceRecord, ApiError> {
    validate_expected_version(input.expected_version)?;
    let existing = fetch_piece(db, id)?;
    if existing.phase == "complete" {
        return project_piece_transition(db, id);
    }
    if !has_successful_final_commit(&existing.state) {
        return Err(ApiError::conflict(
            "Memory ingress cannot complete without a successful final Chatend/Kweb commit.",
        ));
    }
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx
        .execute(
            "UPDATE audio_ingress_pieces
             SET phase='complete',state_json=json_remove(state_json,'$.historyIngressRepairRequired'),next_attempt_at=NULL,updated_at=?1,version=version+1
             WHERE id=?2 AND phase='ingress_in_progress' AND version=?3",
            params![Utc::now().to_rfc3339(), id, input.expected_version],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Memory-ingress job is not in the expected state.",
        ));
    }
    let piece = project_piece_transition(&tx, id)?;
    tx.commit().map_err(ApiError::internal)?;
    Ok(piece)
}

fn fail_ingress_transition(
    db: &mut Connection,
    id: &str,
    input: &RecordIngressFailure,
) -> Result<IngressPieceRecord, ApiError> {
    validate_expected_version(input.expected_version)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let existing = fetch_piece(&tx, id)?;
    if !matches!(
        existing.phase.as_str(),
        "ingress_pending" | "ingress_in_progress"
    ) || existing.version != input.expected_version
    {
        return Err(ApiError::conflict(
            "Memory-ingress job is no longer in the expected attempt.",
        ));
    }
    let attempt = existing.ingress_failure_count + 1;
    let terminal = input.code.as_deref() == Some("input_too_large") || attempt >= FAILURE_LIMIT;
    let mut failures = existing
        .ingress_failures
        .as_array()
        .cloned()
        .unwrap_or_default();
    failures.push(json!({
        "attempt":attempt,
        "occurred_at":Utc::now().to_rfc3339(),
        "stage":concise_text(&input.stage,80,"unknown"),
        "code":input.code.as_deref().map(|value|concise_text(value,80,"unknown_error")),
        "message":concise_text(&input.message,2000,"Memory ingress failed without an error message."),
        "rounds_used":input.rounds_used,
        "context_tokens":input.context_tokens,
        "context_window_tokens":input.context_window_tokens,
    }));
    if failures.len() > FAILURE_LIMIT as usize {
        failures.drain(..failures.len() - FAILURE_LIMIT as usize);
    }
    let now = Utc::now();
    let phase = if terminal {
        "ingress_failed"
    } else {
        "ingress_pending"
    };
    let next_attempt_at = (!terminal)
        .then(|| (now + ChronoDuration::seconds(INGRESS_RETRY_DELAY_SECONDS)).to_rfc3339());
    let changed = tx
        .execute(
            "UPDATE audio_ingress_pieces
             SET phase=?1,ingress_failure_count=?2,ingress_failures_json=?3,next_attempt_at=?4,updated_at=?5,version=version+1
             WHERE id=?6 AND phase IN ('ingress_pending','ingress_in_progress') AND version=?7",
            params![
                phase,
                attempt,
                serde_json::to_string(&failures).map_err(ApiError::internal)?,
                next_attempt_at,
                now.to_rfc3339(),
                id,
                input.expected_version
            ],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Memory ingress changed while recording a failure.",
        ));
    }
    let piece = project_piece_transition(&tx, id)?;
    tx.commit().map_err(ApiError::internal)?;
    Ok(piece)
}

fn retry_ingress_transition(
    db: &mut Connection,
    id: &str,
    expected_version: i64,
    state: &Value,
) -> Result<IngressPieceRecord, ApiError> {
    validate_expected_version(expected_version)?;
    let state_json = serde_json::to_string(state).map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    let changed = tx
        .execute(
            "UPDATE audio_ingress_pieces
             SET phase='ingress_pending',state_json=?1,ingress_failure_count=0,next_attempt_at=NULL,updated_at=?2,version=version+1
             WHERE id=?3 AND phase='ingress_failed' AND version=?4",
            params![
                state_json,
                Utc::now().to_rfc3339(),
                id,
                expected_version
            ],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Memory-ingress job is not in the expected failed state.",
        ));
    }
    let piece = project_piece_transition(&tx, id)?;
    tx.commit().map_err(ApiError::internal)?;
    Ok(piece)
}

fn release_ingress_repairs_transition(db: &mut Connection) -> Result<usize, ApiError> {
    let tx = db.transaction().map_err(ApiError::internal)?;
    let ids = {
        let mut statement = tx
            .prepare(
                "SELECT id FROM audio_ingress_pieces
                 WHERE phase='ingress_failed'
                   AND json_extract(state_json,'$.historyIngressRepairRequired')=1
                   AND json_extract(state_json,'$.historyIngressRepairReleasePending')=1",
            )
            .map_err(ApiError::internal)?;
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(ApiError::internal)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(ApiError::internal)?
    };
    let changed = tx
        .execute(
            "UPDATE audio_ingress_pieces
             SET phase='ingress_pending',state_json=json_remove(state_json,'$.historyIngress','$.historyIngressRepairReleasePending'),next_attempt_at=NULL,ingress_failure_count=0,updated_at=?1,version=version+1
             WHERE phase='ingress_failed'
               AND json_extract(state_json,'$.historyIngressRepairRequired')=1
               AND json_extract(state_json,'$.historyIngressRepairReleasePending')=1",
            [Utc::now().to_rfc3339()],
        )
        .map_err(ApiError::internal)?;
    for id in ids {
        project_piece_transition(&tx, &id)?;
    }
    tx.commit().map_err(ApiError::internal)?;
    Ok(changed)
}

fn has_successful_final_commit(state: &Value) -> bool {
    let current_snapshot = state.get("historyIngress").is_some_and(|history| {
        history.get("completed").and_then(Value::as_bool) == Some(true)
            && history
                .get("sessionObjectId")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.trim().is_empty())
    });
    let legacy_end_session = state
        .pointer("/historyIngress/tools/log")
        .and_then(Value::as_array)
        .is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry.get("name").and_then(Value::as_str) == Some("EndSession")
                    && entry.get("ok").and_then(Value::as_bool) == Some(true)
            })
        });
    current_snapshot || legacy_end_session
}

async fn get_ingress_piece(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(fetch_piece(&db, &id)?))
}

async fn release_ingress_repairs(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let released = release_ingress_repairs_transition(&mut db)?;
    Ok(Json(json!({"released":released})))
}

async fn ingress_started(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<StartIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(start_ingress_transition(&mut db, &id, &input)?))
}

async fn ingress_checkpoint(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<CheckpointIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(checkpoint_ingress_transition(&mut db, &id, &input)?))
}

async fn ingress_completed(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<VersionedTransition>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let (completed_piece, recording_id, recording_complete) = {
        let mut db = state.db.lock().map_err(ApiError::internal)?;
        let completed = complete_ingress_transition(&mut db, &id, &input)?;
        let recording_id = completed.recording_id.clone();
        let remaining = db.query_row(
            "SELECT COUNT(*) FROM audio_ingress_pieces WHERE recording_id=?1 AND phase<>'complete'",
            [&recording_id],
            |row| row.get::<_, i64>(0),
        ).map_err(ApiError::internal)?;
        (completed, recording_id, remaining == 0)
    };
    if recording_complete
        && let Err(error) =
            remove_completed_chunk_files(&state.config.media_directory, &recording_id).await
    {
        tracing::warn!(%recording_id, %error, "Could not remove completed audio shards");
    }
    Ok(Json(completed_piece))
}

async fn ingress_failure(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<RecordIngressFailure>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(fail_ingress_transition(&mut db, &id, &input)?))
}

async fn retry_ingress(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_piece(&db, &id)?;
    Ok(Json(retry_ingress_transition(
        &mut db,
        &id,
        input.expected_version,
        input.state.as_ref().unwrap_or(&existing.state),
    )?))
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

    fn insert_ready_piece(
        db: &Connection,
        recording_id: &str,
        piece_id: &str,
        sha_character: char,
        source_created_at: &str,
        piece_index: i64,
    ) {
        db.execute(
            "INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning)
             VALUES(?1,?2,?3,'audio/wav',10,?4,?4,?4,?5,'ready_for_ingress',?6,?7,?8)",
            params![
                recording_id,
                sha_character.to_string().repeat(64),
                format!("{recording_id}.wav"),
                source_created_at,
                format!("originals/{recording_id}.wav"),
                TRANSCRIPTION_MODEL,
                RECONCILIATION_MODEL,
                RECONCILIATION_REASONING
            ],
        )
        .unwrap();
        db.execute(
            "INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at)
             VALUES(?1,?2,?3,?4,1,'ingress_pending',?5,?5)",
            params![
                piece_id,
                recording_id,
                piece_index,
                format!("piece {piece_index}"),
                source_created_at
            ],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn completed_audio_cleanup_removes_shards_and_keeps_originals() {
        let root = std::env::temp_dir().join(format!("kennedy-audio-cleanup-{}", Uuid::new_v4()));
        let recording_id = Uuid::new_v4().to_string();
        let chunk_directory = root.join("chunks").join(&recording_id);
        let original = root.join("originals").join("recording.wav");
        fs::create_dir_all(&chunk_directory).unwrap();
        fs::create_dir_all(original.parent().unwrap()).unwrap();
        fs::write(chunk_directory.join("chunk-00000.wav"), b"temporary shard").unwrap();
        fs::write(&original, b"raw original").unwrap();

        remove_completed_chunk_files(&root, &recording_id)
            .await
            .unwrap();

        assert!(!chunk_directory.exists());
        assert!(original.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn migration_releases_a_deferred_legacy_audio_claim() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(INITIAL_MIGRATION).unwrap();
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning,next_attempt_at) VALUES('r',?1,'note.wav','audio/wav',10,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','originals/x.wav','ingressing',?2,?3,?4,'2099-01-01T00:00:00Z')",params!["a".repeat(64),TRANSCRIPTION_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,version,created_at,updated_at) VALUES('stranded','r',0,'text',1,'ingress_in_progress',7,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')", []).unwrap();

        apply_migrations(&db).unwrap();

        let (phase, version): (String, i64) = db
            .query_row(
                "SELECT phase,version FROM audio_ingress_pieces WHERE id='stranded'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(phase, "ingress_pending");
        assert_eq!(version, 8);
        assert_eq!(
            db.query_row(
                "SELECT datetime(next_attempt_at)<=datetime('now','+16 seconds') FROM audio_recordings WHERE id='r'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            1
        );
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            5
        );
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES('next','r',1,'text',1,'ingress_in_progress','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')", []).unwrap();
    }

    #[test]
    fn migration_retries_recordings_failed_by_rounded_wav_intervals() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(INITIAL_MIGRATION).unwrap();
        db.execute_batch(TRANSCRIPTION_STATUS_MIGRATION).unwrap();
        let insert = |id: &str, sha_character: char, error: &str| {
            db.execute(
                "INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning,attempt_count,last_error,transcription_status_json) VALUES(?1,?2,'note.wav','audio/wav',10,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',?3,'failed',?4,?5,?6,1,?7,'{\"state\":\"failed\"}')",
                params![
                    id,
                    sha_character.to_string().repeat(64),
                    format!("originals/{id}.wav"),
                    TRANSCRIPTION_MODEL,
                    RECONCILIATION_MODEL,
                    RECONCILIATION_REASONING,
                    error,
                ],
            )
            .unwrap();
        };
        insert(
            "rounded",
            'a',
            "transcribing: terminal audio transcription: chunk 7 could not be prepared: WAV audio ended before the planned interval",
        );
        insert(
            "unrelated",
            'b',
            "transcribing: terminal audio transcription: unsupported WAV sample format",
        );

        apply_migrations(&db).unwrap();

        let repaired: (String, i64, Option<String>, Option<String>) = db
            .query_row(
                "SELECT status,attempt_count,last_error,transcription_status_json FROM audio_recordings WHERE id='rounded'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(repaired, ("uploaded".into(), 0, None, None));
        assert_eq!(
            db.query_row(
                "SELECT status FROM audio_recordings WHERE id='unrelated'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            "failed"
        );
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            5
        );
    }

    #[test]
    fn prepared_audio_pieces_are_selected_directly_in_piece_order() {
        let db = database();
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES('r',?1,'note.wav','audio/wav',10,'2026-01-01T00:00:00Z','2026-07-01T00:00:00Z','2026-07-01T00:00:00Z','originals/x.wav','ready_for_ingress',?2,?3,?4)",params!["a".repeat(64),TRANSCRIPTION_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
        for (id, index) in [("later", 1), ("first", 0)] {
            db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES(?1,'r',?2,?3,1,'ingress_pending','2026-07-01T00:00:00Z','2026-07-01T00:00:00Z')",params![id,index,format!("piece {index}")]).unwrap();
        }

        let next = select_next_ingress_piece(&db).unwrap().unwrap();
        assert_eq!(next.id, "first");
        assert_eq!(next.piece_index, 0);
    }

    #[test]
    fn claims_require_the_current_protocol_and_one_active_piece() {
        let mut db = database();
        insert_ready_piece(
            &db,
            "recording-a",
            "piece-a",
            'a',
            "2026-01-01T00:00:00Z",
            0,
        );
        insert_ready_piece(
            &db,
            "recording-b",
            "piece-b",
            'b',
            "2026-01-02T00:00:00Z",
            0,
        );
        let old_protocol = start_ingress_transition(
            &mut db,
            "piece-a",
            &StartIngress {
                expected_version: 1,
                provenance_id: "session:audio:piece-a".into(),
                completion_protocol: Some("one-session-transaction-v1".into()),
            },
        )
        .unwrap_err();
        assert_eq!(old_protocol.status, StatusCode::CONFLICT);

        let active = start_ingress_transition(
            &mut db,
            "piece-a",
            &StartIngress {
                expected_version: 1,
                provenance_id: "session:audio:piece-a".into(),
                completion_protocol: Some(COMPLETION_PROTOCOL.into()),
            },
        )
        .unwrap();
        assert_eq!(active.phase, "ingress_in_progress");
        let replayed = start_ingress_transition(
            &mut db,
            "piece-a",
            &StartIngress {
                expected_version: 1,
                provenance_id: "session:audio:piece-a".into(),
                completion_protocol: Some(COMPLETION_PROTOCOL.into()),
            },
        )
        .unwrap();
        assert_eq!(replayed.version, active.version);
        assert_eq!(
            select_next_ingress_piece(&db).unwrap().unwrap().id,
            "piece-a"
        );

        let conflict = start_ingress_transition(
            &mut db,
            "piece-b",
            &StartIngress {
                expected_version: 1,
                provenance_id: "session:audio:piece-b".into(),
                completion_protocol: Some(COMPLETION_PROTOCOL.into()),
            },
        )
        .unwrap_err();
        assert_eq!(conflict.status, StatusCode::CONFLICT);
    }

    #[test]
    fn checkpoint_failure_retry_and_completion_remain_one_state_machine() {
        let mut db = database();
        insert_ready_piece(&db, "recording", "piece", 'a', "2026-01-01T00:00:00Z", 0);
        let active = start_ingress_transition(
            &mut db,
            "piece",
            &StartIngress {
                expected_version: 1,
                provenance_id: "session:audio:piece".into(),
                completion_protocol: Some(COMPLETION_PROTOCOL.into()),
            },
        )
        .unwrap();
        let checkpoint = checkpoint_ingress_transition(
            &mut db,
            "piece",
            &CheckpointIngress {
                expected_version: active.version,
                state: json!({"historyIngress":{"tools":{"log":[]}}}),
            },
        )
        .unwrap();
        let pending = fail_ingress_transition(
            &mut db,
            "piece",
            &RecordIngressFailure {
                expected_version: checkpoint.version,
                stage: "generation".into(),
                code: None,
                message: "temporary".into(),
                rounds_used: None,
                context_tokens: None,
                context_window_tokens: None,
            },
        )
        .unwrap();
        assert_eq!(pending.phase, "ingress_pending");
        assert_eq!(pending.ingress_failure_count, 1);
        assert!(pending.next_attempt_at.is_some());
        assert!(select_next_ingress_piece(&db).unwrap().is_none());

        db.execute(
            "UPDATE audio_ingress_pieces SET next_attempt_at=NULL WHERE id='piece'",
            [],
        )
        .unwrap();
        let active = start_ingress_transition(
            &mut db,
            "piece",
            &StartIngress {
                expected_version: pending.version,
                provenance_id: "session:audio:piece".into(),
                completion_protocol: Some(COMPLETION_PROTOCOL.into()),
            },
        )
        .unwrap();
        let committed = checkpoint_ingress_transition(
            &mut db,
            "piece",
            &CheckpointIngress {
                expected_version: active.version,
                state: json!({"historyIngress":{"completed":true,"sessionObjectId":"AAECAwQF"}}),
            },
        )
        .unwrap();
        let complete = complete_ingress_transition(
            &mut db,
            "piece",
            &VersionedTransition {
                expected_version: committed.version,
            },
        )
        .unwrap();
        assert_eq!(complete.phase, "complete");
        assert!(select_next_ingress_piece(&db).unwrap().is_none());
        assert_eq!(
            fetch_recording(&db, "recording").unwrap().status,
            "complete"
        );
    }

    #[test]
    fn terminal_failure_retry_preserves_diagnostics_and_resets_attempts() {
        let mut db = database();
        insert_ready_piece(&db, "recording", "piece", 'a', "2026-01-01T00:00:00Z", 0);
        let failed = fail_ingress_transition(
            &mut db,
            "piece",
            &RecordIngressFailure {
                expected_version: 1,
                stage: "model_loop".into(),
                code: Some("input_too_large".into()),
                message: "too large".into(),
                rounds_used: Some(1),
                context_tokens: Some(60_000),
                context_window_tokens: Some(50_000),
            },
        )
        .unwrap();
        assert_eq!(failed.phase, "ingress_failed");
        assert_eq!(failed.ingress_failures.as_array().unwrap().len(), 1);

        let retried =
            retry_ingress_transition(&mut db, "piece", failed.version, &failed.state).unwrap();
        assert_eq!(retried.phase, "ingress_pending");
        assert_eq!(retried.ingress_failure_count, 0);
        assert_eq!(retried.ingress_failures.as_array().unwrap().len(), 1);
    }

    #[test]
    fn completion_evidence_supports_current_and_legacy_checkpoints() {
        assert!(has_successful_final_commit(
            &json!({"historyIngress":{"completed":true,"sessionObjectId":"AAECAwQF"}})
        ));
        assert!(!has_successful_final_commit(
            &json!({"historyIngress":{"completed":true,"sessionObjectId":null}})
        ));
        assert!(!has_successful_final_commit(
            &json!({"historyIngress":{"completed":false,"sessionObjectId":"AAECAwQF"}})
        ));
        assert!(has_successful_final_commit(
            &json!({"historyIngress":{"tools":{"log":[{"name":"EndSession","ok":true}]}}})
        ));
    }

    #[test]
    fn recording_list_uses_recording_time_instead_of_upload_time() {
        let db = database();
        let insert = |id: &str, sha_character: char, recorded_at: &str, received_at: &str| {
            db.execute(
                "INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES(?1,?2,?3,'audio/wav',10,?4,?5,?5,?6,'uploaded',?7,?8,?9)",
                params![
                    id,
                    sha_character.to_string().repeat(64),
                    format!("{id}.wav"),
                    recorded_at,
                    received_at,
                    format!("originals/{id}.wav"),
                    TRANSCRIPTION_MODEL,
                    RECONCILIATION_MODEL,
                    RECONCILIATION_REASONING,
                ],
            )
            .unwrap();
        };
        insert(
            "latest-recording",
            'a',
            "2026-07-20T10:00:00-04:00",
            "2026-07-20T15:00:00Z",
        );
        insert(
            "middle-recording",
            'b',
            "2026-07-20T13:00:00Z",
            "2026-07-21T15:00:00Z",
        );
        insert(
            "latest-upload",
            'c',
            "2026-07-18T13:00:00Z",
            "2026-07-22T15:00:00Z",
        );

        let ids = fetch_recordings(&db, 50)
            .unwrap()
            .into_iter()
            .map(|recording| recording.id)
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            ["latest-recording", "middle-recording", "latest-upload"]
        );
    }

    #[test]
    fn oversized_transcripts_are_split_with_repeated_metadata() {
        let recording = WorkRecording {
            id: "recording".into(),
            sha256: "a".repeat(64),
            original_filename: "note.wav".into(),
            source_created_at: "2026-07-16T10:00:00Z".into(),
            original_relative_path: "originals/note.wav".into(),
            status: "reconciling".into(),
        };
        let transcript = format!("A complete paragraph.\n\n{}", "x".repeat(210_000));
        let pieces = split_transcript_body(&recording, &transcript).unwrap();
        assert_eq!(pieces.len(), 2);
        let total = pieces.len();
        for (index, piece) in pieces.into_iter().enumerate() {
            let prepared = format!(
                "{}\n\n{}",
                transcript_header(&recording, Some((index, total))),
                piece
            );
            assert!(estimate_tokens(&prepared) <= MAX_INGRESS_TOKENS);
            assert!(prepared.contains("2026-07-16T10:00:00Z"));
            assert!(prepared.contains(&format!("Transcript piece: {} of {total}", index + 1)));
        }
        assert_eq!(estimate_tokens("12345"), 2);
    }

    #[test]
    fn sha_identity_and_ingress_completion_are_durable() {
        let mut db = database();
        let now = "2026-07-16T10:00:00Z";
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES('r',?1,'note.wav','audio/wav',10,?2,?2,?2,'originals/x.wav','ready_for_ingress',?3,?4,?5)",params!["a".repeat(64),now,TRANSCRIPTION_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES('p','r',0,'text',1,'ingress_pending',?1,?1)",[now]).unwrap();
        assert_eq!(
            fetch_recording_by_sha(&db, &"a".repeat(64))
                .unwrap()
                .unwrap()
                .status,
            "ready_for_ingress"
        );
        let tx = db.transaction().unwrap();
        tx.execute(
            "UPDATE audio_ingress_pieces SET phase='complete' WHERE id='p'",
            [],
        )
        .unwrap();
        tx.execute(
            "UPDATE audio_recordings SET status='complete' WHERE id='r'",
            [],
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(fetch_recording(&db, "r").unwrap().completed_piece_count, 1);
    }

    #[test]
    fn recording_history_contains_preparation_and_kennedy_ingress_artifacts() {
        let db = database();
        let now = "2026-07-16T10:00:00Z";
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning,final_transcript) VALUES('r',?1,'note.wav','audio/wav',10,?2,?2,?2,'originals/x.wav','complete',?3,?4,?5,'Final transcript')",params!["a".repeat(64),now,TRANSCRIPTION_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
        db.execute("INSERT INTO audio_chunks(recording_id,chunk_index,audio_start_ms,audio_end_ms,relative_path,transcript_json) VALUES('r',0,0,1000,'chunks/0.wav',?1)",[r#"{"lines":[{"speaker":"Speaker 1","text":"Hello"}]}"#]).unwrap();
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,state_json,created_at,updated_at) VALUES('p','r',0,'Final transcript',4,'complete',?1,?2,?2)",params![r#"{"historyIngress":{"format":"kennedy-chatend","completed":true,"messages":[{"role":"user","content":"Legacy audio ingress"}]}}"#,now]).unwrap();

        let history = fetch_recording_history(&db, "r").unwrap();
        assert_eq!(
            history.final_transcript.as_deref(),
            Some("Final transcript")
        );
        assert_eq!(history.chunks.len(), 1);
        assert_eq!(
            history.chunks[0].transcript.as_ref().unwrap()["lines"][0]["text"],
            "Hello"
        );
        assert_eq!(history.pieces.len(), 1);
        assert_eq!(history.pieces[0].state["historyIngress"]["completed"], true);
        assert!(history.pieces[0].state["historyIngress"]["chatendText"].is_null());
    }

    #[test]
    fn filenames_cannot_escape_the_media_directory() {
        assert_eq!(
            safe_filename(Some("../../voice note.wav")),
            "voice_note.wav"
        );
        assert_eq!(safe_filename(None), "vnote.wav");
    }
}
