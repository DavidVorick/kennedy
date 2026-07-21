use std::{
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
use futures::{StreamExt, stream};
use hound::{SampleFormat, WavReader, WavWriter};
use kcode_codex_runtime::{CatalogCache, Codex, CodexConfig, GenerationRequest, ReasoningEffort};
use kcode_gemini_api::{
    CompletionStatus, GEMINI_31_PRO, Gemini, GenerationOptions, MediaInput, MultimodalRequest,
    ServiceTier, StructuredOutput, ThinkingLevel,
};
use kennedy_chatend::hydrate_state_chatend_text;
use kennedy_memory_ingress::{
    Failure as QueueFailure, Job as QueueJob, LegacySubmission, Queue, SourceKind, Submission,
};
use ruopus::encode_ogg_opus;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const RELEASE_DEFERRED_INGRESS_MIGRATION: &str =
    include_str!("../migrations/002_release_deferred_ingress.sql");
const GEMINI_MODEL: &str = GEMINI_31_PRO;
const RECONCILIATION_MODEL: &str = "gpt-5.6-sol";
const RECONCILIATION_REASONING: &str = "xhigh";
const MAX_CHUNK_MILLISECONDS: u64 = 4 * 60 * 1_000;
const CHUNK_OVERLAP_MILLISECONDS: u64 = 15 * 1_000;
const MAX_INGRESS_TOKENS: u64 = 50_000;
const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 4;
const MAX_CONCURRENT_GEMINI_CHUNKS: usize = 4;
const OPUS_SAMPLE_RATE: u32 = 48_000;
const OPUS_MAX_CHANNELS: usize = 2;
const OPUS_BITRATE_PER_CHANNEL_BPS: u32 = 192_000;
const INGRESS_BREAK: &str = "<!-- KENNEDY_INGRESS_BREAK -->";

#[derive(Clone, Debug)]
pub struct Config {
    pub database: PathBuf,
    pub media_directory: PathBuf,
    pub max_upload_bytes: usize,
    pub gemini_api_key: Option<String>,
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    db: Arc<Mutex<Connection>>,
    gemini: Option<Gemini>,
    codex: Codex,
    queue: Queue,
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

#[derive(Debug)]
struct ChunkRecord {
    index: i64,
    audio_start_ms: i64,
    audio_end_ms: i64,
    relative_path: String,
    transcript_json: Option<String>,
}

#[derive(Deserialize)]
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
    mut config: Config,
    codex_catalog_cache: CatalogCache,
    queue: Queue,
) -> anyhow::Result<Service> {
    ensure!(
        MAX_CHUNK_MILLISECONDS > CHUNK_OVERLAP_MILLISECONDS,
        "audio chunk overlap must be smaller than the chunk limit"
    );
    ensure!(
        config.max_upload_bytes > 0,
        "audio upload limit must be positive"
    );
    let mut codex_config = CodexConfig::new(RECONCILIATION_MODEL);
    codex_config.validation_reasoning_effort = ReasoningEffort::XHigh;
    let codex = Codex::open(codex_config, codex_catalog_cache)
        .await
        .context("opening Codex audio-reconciliation runtime")?;
    ensure_private_directory(&config.media_directory)?;
    let connection = Connection::open(&config.database)
        .with_context(|| format!("opening {}", config.database.display()))?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
    )?;
    apply_migrations(&connection).context("applying audio-ingress migrations")?;
    import_legacy_ingress(&connection, &queue)
        .context("moving audio ingress into the shared queue")?;
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

    let gemini = config
        .gemini_api_key
        .take()
        .map(Gemini::open)
        .transpose()
        .context("opening Gemini audio-transcription client")?;
    let state = AppState {
        config: Arc::new(config),
        db: Arc::new(Mutex::new(connection)),
        gemini,
        codex,
        queue,
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
            "gemini":if state.gemini.is_some() { "ready" } else { "unconfigured" },
            "gemini_model":GEMINI_MODEL,
            "reconciliation_model":RECONCILIATION_MODEL,
            "chunk_seconds":MAX_CHUNK_MILLISECONDS / 1000,
            "overlap_seconds":CHUNK_OVERLAP_MILLISECONDS / 1000,
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
    Ok(())
}

fn import_legacy_ingress(connection: &Connection, queue: &Queue) -> anyhow::Result<()> {
    let mut statement = connection.prepare(&format!(
        "{} WHERE p.phase IN ('ingress_pending','ingress_in_progress','ingress_failed') ORDER BY datetime(r.source_created_at),p.piece_index,p.id",
        piece_select()
    ))?;
    let pieces = statement
        .query_map([], row_piece)?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for piece in pieces {
        let next_attempt_at = connection.query_row(
            "SELECT next_attempt_at FROM audio_recordings WHERE id=?1",
            [&piece.recording_id],
            |row| row.get::<_, Option<String>>(0),
        )?;
        let job = queue.import_legacy(LegacySubmission {
            source_kind: SourceKind::Audio,
            source_id: piece.id.clone(),
            source_created_at: piece.source_created_at.clone(),
            source_position: piece.piece_index,
            phase: piece.phase.clone(),
            provenance_id: piece.provenance_id.clone(),
            state: piece.state.clone(),
            version: piece.version,
            failure_count: piece.ingress_failure_count,
            failures: piece.ingress_failures.clone(),
            next_attempt_at,
        })?;
        mirror_queue_job(connection, &job).map_err(|error| anyhow::anyhow!(error.message))?;
    }
    Ok(())
}

fn mirror_queue_job(db: &Connection, job: &QueueJob) -> Result<IngressPieceRecord, ApiError> {
    let state_json = serde_json::to_string(&job.state).map_err(ApiError::internal)?;
    let failures_json = serde_json::to_string(&job.failures).map_err(ApiError::internal)?;
    let changed = db.execute(
        "UPDATE audio_ingress_pieces SET phase=?1,provenance_id=?2,state_json=?3,version=?4,ingress_failure_count=?5,ingress_failures_json=?6,updated_at=?7 WHERE id=?8",
        params![job.phase,job.provenance_id,state_json,job.version,job.failure_count,failures_json,job.updated_at,job.source_id],
    ).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::not_found());
    }
    let piece = fetch_piece(db, &job.source_id)?;
    let remaining = db
        .query_row(
            "SELECT COUNT(*) FROM audio_ingress_pieces WHERE recording_id=?1 AND phase<>'complete'",
            [&piece.recording_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(ApiError::internal)?;
    let (status, next_attempt_at, last_error) = match job.phase.as_str() {
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
        _ => ("ready_for_ingress", job.next_attempt_at.clone(), None),
    };
    db.execute(
        "UPDATE audio_recordings SET status=?1,next_attempt_at=?2,last_error=?3,updated_at=?4 WHERE id=?5",
        params![status,next_attempt_at,last_error,job.updated_at,piece.recording_id],
    ).map_err(ApiError::internal)?;
    fetch_piece(db, &job.source_id)
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
            params![id,sha256,original_filename,content_type,size_bytes as i64,recorded_at,now,relative_path,GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING],
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
    "SELECT r.id,r.sha256,r.original_filename,r.content_type,r.size_bytes,r.source_created_at,r.received_at,r.updated_at,r.status,r.gemini_model,r.reconciliation_model,r.reconciliation_reasoning,r.attempt_count,r.next_attempt_at,r.last_error,(SELECT COUNT(*) FROM audio_ingress_pieces p WHERE p.recording_id=r.id),(SELECT COUNT(*) FROM audio_ingress_pieces p WHERE p.recording_id=r.id AND p.phase='complete') FROM audio_recordings r"
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
        attempt_count: row.get(12)?,
        next_attempt_at: row.get(13)?,
        last_error: row.get(14)?,
        transcript_piece_count: row.get(15)?,
        completed_piece_count: row.get(16)?,
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
    let mut statement = db
        .prepare(&format!(
            "{} ORDER BY datetime(r.received_at) DESC,r.id DESC LIMIT ?1",
            recording_select()
        ))
        .map_err(ApiError::internal)?;
    let recordings = statement
        .query_map([limit], row_recording)
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"recordings":recordings})))
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
        "uploaded" | "chunking" => prepare_recording_chunks(state, &recording).await,
        "transcribing" => transcribe_pending_chunks(state, &recording).await,
        "reconciling" => reconcile_recording(state, &recording).await,
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
    matches!(stage, "uploaded" | "chunking")
        && error
            .chain()
            .any(|cause| cause.to_string().starts_with("invalid WAV recording"))
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

async fn prepare_recording_chunks(
    state: &AppState,
    recording: &WorkRecording,
) -> anyhow::Result<()> {
    {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
        db.execute(
            "UPDATE audio_recordings SET status='chunking',updated_at=?1,next_attempt_at=NULL,last_error=NULL WHERE id=?2",
            params![Utc::now().to_rfc3339(),recording.id],
        )?;
    }
    let source = state
        .config
        .media_directory
        .join(&recording.original_relative_path);
    let chunk_directory = state
        .config
        .media_directory
        .join("chunks")
        .join(&recording.id);
    let relative_prefix = format!("chunks/{}", recording.id);
    let generated =
        tokio::task::spawn_blocking(move || split_wav(&source, &chunk_directory, &relative_prefix))
            .await
            .context("audio chunk worker stopped")??;

    let mut db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
    let tx = db.transaction()?;
    tx.execute(
        "DELETE FROM audio_chunks WHERE recording_id=?1",
        [&recording.id],
    )?;
    for chunk in generated {
        tx.execute(
            "INSERT INTO audio_chunks(recording_id,chunk_index,audio_start_ms,audio_end_ms,relative_path) VALUES(?1,?2,?3,?4,?5)",
            params![recording.id,chunk.index,chunk.audio_start_ms,chunk.audio_end_ms,chunk.relative_path],
        )?;
    }
    tx.execute(
        "UPDATE audio_recordings SET status='transcribing',attempt_count=0,next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=?2",
        params![Utc::now().to_rfc3339(),recording.id],
    )?;
    tx.commit()?;
    tracing::info!(recording_id=%recording.id, "Audio recording split into durable transcription chunks");
    Ok(())
}

#[derive(Debug)]
struct GeneratedChunk {
    index: i64,
    audio_start_ms: i64,
    audio_end_ms: i64,
    relative_path: String,
}

fn chunk_boundaries(duration_ms: u64) -> Vec<(u64, u64)> {
    if duration_ms == 0 {
        return Vec::new();
    }
    if duration_ms <= MAX_CHUNK_MILLISECONDS {
        return vec![(0, duration_ms)];
    }
    let advance = MAX_CHUNK_MILLISECONDS - CHUNK_OVERLAP_MILLISECONDS;
    let chunks = (duration_ms - CHUNK_OVERLAP_MILLISECONDS).div_ceil(advance);
    let window = (duration_ms + (chunks - 1) * CHUNK_OVERLAP_MILLISECONDS).div_ceil(chunks);
    let step = window - CHUNK_OVERLAP_MILLISECONDS;
    (0..chunks)
        .map(|index| {
            let start = index * step;
            (start, (start + window).min(duration_ms))
        })
        .filter(|(start, end)| end > start)
        .collect()
}

fn split_wav(
    source: &Path,
    chunk_directory: &Path,
    relative_prefix: &str,
) -> anyhow::Result<Vec<GeneratedChunk>> {
    let reader = WavReader::open(source)
        .with_context(|| format!("invalid WAV recording: opening {}", source.display()))?;
    let spec = reader.spec();
    ensure!(
        spec.sample_rate > 0 && spec.channels > 0,
        "invalid WAV recording: audio metadata is invalid"
    );
    let complete_file_bytes = fs::metadata(source)?.len();
    let declared_audio_bytes = u64::from(reader.duration())
        .saturating_mul(u64::from(spec.channels))
        .saturating_mul(u64::from(spec.bits_per_sample).div_ceil(8));
    ensure!(
        declared_audio_bytes <= complete_file_bytes,
        "invalid WAV recording: header declares {declared_audio_bytes} audio bytes but the complete file has only {complete_file_bytes} bytes"
    );
    let duration_ms = (u64::from(reader.duration()) * 1_000).div_ceil(u64::from(spec.sample_rate));
    drop(reader);
    let boundaries = chunk_boundaries(duration_ms);
    ensure!(
        !boundaries.is_empty(),
        "invalid WAV recording: file contains no audio samples"
    );
    if chunk_directory.exists() {
        fs::remove_dir_all(chunk_directory)
            .with_context(|| format!("clearing {}", chunk_directory.display()))?;
    }
    ensure_private_directory(chunk_directory)?;
    let mut generated = Vec::with_capacity(boundaries.len());
    for (index, (start_ms, end_ms)) in boundaries.into_iter().enumerate() {
        let name = format!("chunk-{index:05}.wav");
        let path = chunk_directory.join(&name);
        copy_wav_interval(source, &path, start_ms, end_ms)
            .context("invalid WAV recording while reading declared samples")?;
        set_private_file(&path)?;
        sync_file(&path)?;
        generated.push(GeneratedChunk {
            index: index as i64,
            audio_start_ms: start_ms as i64,
            audio_end_ms: end_ms as i64,
            relative_path: format!("{relative_prefix}/{name}"),
        });
    }
    sync_directory(chunk_directory)?;
    if let Some(parent) = chunk_directory.parent() {
        sync_directory(parent)?;
    }
    Ok(generated)
}

fn copy_wav_interval(
    source: &Path,
    destination: &Path,
    start_ms: u64,
    end_ms: u64,
) -> anyhow::Result<()> {
    let mut reader = WavReader::open(source)?;
    let spec = reader.spec();
    let start_frame = (start_ms * u64::from(spec.sample_rate) / 1_000) as u32;
    let end_frame = (end_ms * u64::from(spec.sample_rate) / 1_000) as u32;
    let sample_values = u64::from(end_frame.saturating_sub(start_frame)) * u64::from(spec.channels);
    reader.seek(start_frame)?;
    let mut writer = WavWriter::create(destination, spec)?;
    match (spec.sample_format, spec.bits_per_sample) {
        (SampleFormat::Float, _) => {
            for sample in reader.samples::<f32>().take(sample_values as usize) {
                writer.write_sample(sample?)?;
            }
        }
        (SampleFormat::Int, 1..=8) => {
            for sample in reader.samples::<i8>().take(sample_values as usize) {
                writer.write_sample(sample?)?;
            }
        }
        (SampleFormat::Int, 9..=16) => {
            for sample in reader.samples::<i16>().take(sample_values as usize) {
                writer.write_sample(sample?)?;
            }
        }
        (SampleFormat::Int, _) => {
            for sample in reader.samples::<i32>().take(sample_values as usize) {
                writer.write_sample(sample?)?;
            }
        }
    }
    writer.finalize()?;
    Ok(())
}

async fn transcribe_pending_chunks(
    state: &AppState,
    recording: &WorkRecording,
) -> anyhow::Result<()> {
    let chunks = {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
        let mut statement = db.prepare(
            "SELECT chunk_index,audio_start_ms,audio_end_ms,relative_path,transcript_json \
             FROM audio_chunks WHERE recording_id=?1 AND transcript_json IS NULL ORDER BY chunk_index",
        )?;
        statement
            .query_map([&recording.id], |row| {
                Ok(ChunkRecord {
                    index: row.get(0)?,
                    audio_start_ms: row.get(1)?,
                    audio_end_ms: row.get(2)?,
                    relative_path: row.get(3)?,
                    transcript_json: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    if chunks.is_empty() {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
        db.execute(
            "UPDATE audio_recordings SET status='reconciling',attempt_count=0,next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=?2",
            params![Utc::now().to_rfc3339(),recording.id],
        )?;
        return Ok(());
    }
    let gemini = state
        .gemini
        .as_ref()
        .context(
            "Gemini audio transcription is not configured; store gemini-api-key in Kennedy's vault",
        )?
        .clone();
    tracing::info!(
        recording_id = %recording.id,
        chunks = chunks.len(),
        concurrency = MAX_CONCURRENT_GEMINI_CHUNKS.min(chunks.len()),
        model = GEMINI_MODEL,
        "Transcribing audio chunks concurrently"
    );
    let mut transcriptions = stream::iter(chunks.into_iter().map(|chunk| {
        let path = state.config.media_directory.join(&chunk.relative_path);
        let gemini = gemini.clone();
        async move {
            let index = chunk.index;
            let result = transcribe_chunk(&gemini, &path, recording, &chunk).await;
            (index, result)
        }
    }))
    .buffer_unordered(MAX_CONCURRENT_GEMINI_CHUNKS);
    let mut failures = Vec::new();
    while let Some((chunk_index, result)) = transcriptions.next().await {
        match result {
            Ok(transcript) => {
                let db = state
                    .db
                    .lock()
                    .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
                let changed = db.execute(
                    "UPDATE audio_chunks SET transcript_json=?1 WHERE recording_id=?2 AND chunk_index=?3 AND transcript_json IS NULL",
                    params![transcript,recording.id,chunk_index],
                )?;
                ensure!(
                    changed == 1,
                    "audio chunk {chunk_index} changed before its transcript could be saved"
                );
                db.execute(
                    "UPDATE audio_recordings SET attempt_count=0,next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=?2",
                    params![Utc::now().to_rfc3339(),recording.id],
                )?;
                tracing::info!(recording_id=%recording.id, chunk=chunk_index, model=GEMINI_MODEL, "Transcribed audio chunk");
            }
            Err(error) => failures.push((
                chunk_index,
                concise_text(
                    &error.to_string(),
                    800,
                    "Gemini transcription failed without detail",
                ),
            )),
        }
    }
    if !failures.is_empty() {
        failures.sort_by_key(|(chunk_index, _)| *chunk_index);
        let detail = failures
            .into_iter()
            .map(|(chunk_index, error)| format!("chunk {chunk_index}: {error}"))
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!("one or more concurrent Gemini transcriptions failed: {detail}");
    }
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
    db.execute(
        "UPDATE audio_recordings SET status='reconciling',attempt_count=0,next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=?2",
        params![Utc::now().to_rfc3339(),recording.id],
    )?;
    Ok(())
}

async fn transcribe_chunk(
    gemini: &Gemini,
    path: &Path,
    recording: &WorkRecording,
    chunk: &ChunkRecord,
) -> anyhow::Result<String> {
    let source = path.to_owned();
    let opus = tokio::task::spawn_blocking(move || wav_to_opus(&source))
        .await
        .context("joining in-memory Opus encoder")??;
    let media = MediaInput::audio("audio/ogg", opus)
        .context("preparing inline Ogg Opus audio for Gemini")?;
    let mut request = MultimodalRequest::new(transcription_prompt(recording, chunk), vec![media]);
    request.options = GenerationOptions {
        max_output_tokens: Some(32_768),
        temperature: None,
        thinking_level: Some(ThinkingLevel::High),
        service_tier: ServiceTier::Standard,
    };
    request.structured_output = Some(
        StructuredOutput::new(transcription_schema())
            .context("validating audio-transcription output schema")?,
    );
    let response = gemini
        .infer_pro_multimodal(request)
        .await
        .context("requesting Gemini audio transcription")?;
    ensure!(
        response.status == CompletionStatus::Completed,
        "Gemini transcription did not complete"
    );
    let text = response
        .text
        .context("Gemini transcription returned no structured text")?;
    let transcript: Value = serde_json::from_str(&text)
        .context("Gemini transcription returned invalid structured JSON")?;
    ensure!(
        transcript
            .get("utterances")
            .and_then(Value::as_array)
            .is_some(),
        "Gemini transcript omitted utterances"
    );
    serde_json::to_string_pretty(&transcript).context("serializing Gemini transcript")
}

fn wav_to_opus(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut reader =
        WavReader::open(path).with_context(|| format!("opening WAV audio {}", path.display()))?;
    let spec = reader.spec();
    ensure!(
        spec.sample_rate > 0 && spec.channels > 0,
        "WAV audio metadata is invalid"
    );
    let samples = match (spec.sample_format, spec.bits_per_sample) {
        (SampleFormat::Float, 32) => reader
            .samples::<f32>()
            .map(|sample| sample.context("reading 32-bit float WAV sample"))
            .collect::<anyhow::Result<Vec<_>>>()?,
        (SampleFormat::Int, 1..=8) => {
            let scale = 2.0_f32.powi(i32::from(spec.bits_per_sample) - 1);
            reader
                .samples::<i8>()
                .map(|sample| {
                    sample
                        .map(|value| f32::from(value) / scale)
                        .context("reading 8-bit WAV sample")
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        (SampleFormat::Int, 9..=16) => {
            let scale = 2.0_f32.powi(i32::from(spec.bits_per_sample) - 1);
            reader
                .samples::<i16>()
                .map(|sample| {
                    sample
                        .map(|value| f32::from(value) / scale)
                        .context("reading 16-bit WAV sample")
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        (SampleFormat::Int, 17..=32) => {
            let scale = 2.0_f64.powi(i32::from(spec.bits_per_sample) - 1) as f32;
            reader
                .samples::<i32>()
                .map(|sample| {
                    sample
                        .map(|value| value as f32 / scale)
                        .context("reading high-resolution integer WAV sample")
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        _ => anyhow::bail!(
            "unsupported WAV sample format: {:?} with {} bits",
            spec.sample_format,
            spec.bits_per_sample
        ),
    };
    let channels = usize::from(spec.channels);
    ensure!(
        (1..=OPUS_MAX_CHANNELS).contains(&channels),
        "Ogg Opus encoding supports mono or stereo WAV audio; source has {channels} channels"
    );
    ensure!(
        samples.len().is_multiple_of(channels),
        "WAV audio ended with an incomplete sample frame"
    );
    ensure!(!samples.is_empty(), "WAV audio contains no samples");
    ensure!(
        samples.iter().all(|sample| sample.is_finite()),
        "WAV audio contains a non-finite sample"
    );

    let pcm = samples
        .into_iter()
        .map(|sample| sample.clamp(-1.0, 1.0))
        .collect::<Vec<_>>();
    let pcm = resample_interleaved(&pcm, spec.sample_rate, channels)?;
    let bitrate = OPUS_BITRATE_PER_CHANNEL_BPS * u32::from(spec.channels);
    Ok(encode_ogg_opus(&pcm, channels, bitrate))
}

fn resample_interleaved(
    source: &[f32],
    source_rate: u32,
    channels: usize,
) -> anyhow::Result<Vec<f32>> {
    ensure!(source_rate > 0, "WAV sample rate must be positive");
    ensure!(
        (1..=OPUS_MAX_CHANNELS).contains(&channels),
        "Ogg Opus encoding supports mono or stereo PCM"
    );
    ensure!(
        source.len().is_multiple_of(channels),
        "PCM ended with an incomplete frame"
    );
    ensure!(!source.is_empty(), "PCM contains no samples");
    if source_rate == OPUS_SAMPLE_RATE {
        return Ok(source.to_vec());
    }
    let source_frames = source.len() / channels;
    let output_frames = usize::try_from(
        (source_frames as u128 * u128::from(OPUS_SAMPLE_RATE)).div_ceil(u128::from(source_rate)),
    )
    .context("resampled audio is too large for this platform")?;
    let output_samples = output_frames
        .checked_mul(channels)
        .context("resampled audio is too large for this platform")?;
    let mut output = Vec::with_capacity(output_samples);
    for output_frame in 0..output_frames {
        let source_position = output_frame as u128 * u128::from(source_rate);
        let lower = usize::try_from(source_position / u128::from(OPUS_SAMPLE_RATE))
            .context("resampling position is too large for this platform")?
            .min(source_frames - 1);
        let upper = (lower + 1).min(source_frames - 1);
        let fraction =
            (source_position % u128::from(OPUS_SAMPLE_RATE)) as f32 / OPUS_SAMPLE_RATE as f32;
        for channel in 0..channels {
            let lower_sample = source[lower * channels + channel];
            let upper_sample = source[upper * channels + channel];
            output.push(lower_sample + (upper_sample - lower_sample) * fraction);
        }
    }
    Ok(output)
}

fn transcription_prompt(recording: &WorkRecording, chunk: &ChunkRecord) -> String {
    format!(
        "Transcribe this vnote audio faithfully and completely. Distinguish every discernible speaker with chunk-local labels such as speaker_1. Do not guess a real identity. Preserve the original language. When an utterance is not English, also provide an accurate English translation; for English, use an empty translation string. Add concise annotations when speech is unclear, overlapping, interrupted, emotional in a materially relevant way, or accompanied by relevant non-speech audio. Timestamps are seconds relative to this audio chunk. Do not omit quiet or difficult portions.\n\nRecording began: {}\nRecording SHA-256: {}\nOriginal filename: {}\nChunk index: {}\nThis chunk covers recording offsets {:.3} through {:.3} seconds. Adjacent chunks overlap by up to 15 seconds; transcribe the entire supplied chunk even when boundary material will be repeated elsewhere.",
        recording.source_created_at,
        recording.sha256,
        recording.original_filename,
        chunk.index,
        chunk.audio_start_ms as f64 / 1_000.0,
        chunk.audio_end_ms as f64 / 1_000.0,
    )
}

fn transcription_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "utterances":{
                "type":"array",
                "items":{
                    "type":"object",
                    "properties":{
                        "start_seconds":{"type":"number"},
                        "end_seconds":{"type":"number"},
                        "speaker":{"type":"string"},
                        "language":{"type":"string"},
                        "original_text":{"type":"string"},
                        "english_translation":{"type":"string"},
                        "annotations":{"type":"array","items":{"type":"string"}},
                        "confidence":{"type":"string","enum":["high","medium","low"]}
                    },
                    "required":["start_seconds","end_seconds","speaker","language","original_text","english_translation","annotations","confidence"]
                }
            },
            "chunk_notes":{"type":"array","items":{"type":"string"}}
        },
        "required":["utterances","chunk_notes"]
    })
}

async fn reconcile_recording(state: &AppState, recording: &WorkRecording) -> anyhow::Result<()> {
    let chunks = {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("audio database lock was poisoned"))?;
        let mut statement = db.prepare("SELECT chunk_index,audio_start_ms,audio_end_ms,relative_path,transcript_json FROM audio_chunks WHERE recording_id=?1 ORDER BY chunk_index")?;
        statement
            .query_map([&recording.id], |row| {
                Ok(ChunkRecord {
                    index: row.get(0)?,
                    audio_start_ms: row.get(1)?,
                    audio_end_ms: row.get(2)?,
                    relative_path: row.get(3)?,
                    transcript_json: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    ensure!(!chunks.is_empty(), "recording has no transcription chunks");
    ensure!(
        chunks.iter().all(|chunk| chunk.transcript_json.is_some()),
        "recording has incomplete transcription chunks"
    );
    let prompt = reconciliation_prompt(recording, &chunks);
    let mut transcript = run_sol(&state.codex, &prompt).await?;
    let mut pieces = parse_ingress_pieces(&transcript);
    if pieces
        .iter()
        .any(|piece| estimate_tokens(piece) > MAX_INGRESS_TOKENS)
    {
        transcript = run_sol(&state.codex, &split_prompt(&transcript)).await?;
        pieces = parse_ingress_pieces(&transcript);
    }
    ensure!(!pieces.is_empty(), "Sol returned an empty final transcript");
    ensure!(
        pieces
            .iter()
            .all(|piece| estimate_tokens(piece) <= MAX_INGRESS_TOKENS),
        "Sol did not place transcript boundaries below the 50,000-token estimate"
    );
    let final_transcript = pieces.join("\n\n");
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
    let mut submissions = Vec::with_capacity(pieces.len());
    for (index, piece) in pieces.iter().enumerate() {
        let piece_id = Uuid::new_v4().to_string();
        tx.execute(
            "INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,'ingress_pending',?6,?6)",
            params![piece_id,recording.id,index as i64,piece,estimate_tokens(piece) as i64,now],
        )?;
        submissions.push(Submission {
            source_kind: SourceKind::Audio,
            source_id: piece_id,
            source_created_at: recording.source_created_at.clone(),
            source_position: index as i64,
            state: json!({}),
            version: 1,
        });
    }
    tx.execute(
        "UPDATE audio_recordings SET status='ready_for_ingress',final_transcript=?1,attempt_count=0,next_attempt_at=NULL,last_error=NULL,updated_at=?2 WHERE id=?3",
        params![final_transcript,now,recording.id],
    )?;
    tx.commit()?;
    for submission in submissions {
        let job = state.queue.submit(submission)?;
        mirror_queue_job(&db, &job).map_err(|error| anyhow::anyhow!(error.message))?;
    }
    tracing::info!(recording_id=%recording.id, pieces=pieces.len(), model=RECONCILIATION_MODEL, reasoning=RECONCILIATION_REASONING, "Prepared final vnote transcript for Kennedy ingress");
    Ok(())
}

fn reconciliation_prompt(recording: &WorkRecording, chunks: &[ChunkRecord]) -> String {
    let mut prompt = format!(
        "You are producing the canonical final transcript of one vnote. The chunk transcripts below are already in exact chronological order. They were independently transcribed from audio windows that overlap their neighbors by 15 seconds. Faithfully copy all spoken content into one coherent transcript, remove only duplicated boundary material, and reconcile chunk-local speaker labels across the complete conversation. Use real speaker names only when supported by the conversation; otherwise assign stable labels such as Speaker A. Preserve useful uncertainty and annotations. For every non-English utterance, show its English translation alongside it. Preserve chronological timestamps, converting chunk-relative timestamps using each chunk's supplied recording offset. Do not summarize or omit content.\n\nThe vnote began at {created}. This source timestamp is important historical context and must appear prominently at the top of the final transcript. The recording may describe plans, beliefs, or facts that were current then but are stale now.\n\nOutput only the final readable Markdown transcript. When the transcript would exceed an estimated 50,000 tokens using one token per four Unicode characters, insert the exact line `{boundary}` at sensible conversational or topical boundaries so every resulting piece stays at or below that estimate. Do not make pieces equal-sized merely for symmetry. Each piece must repeat a short metadata header containing the recording timestamp, SHA-256, and its piece context so Kennedy never receives a piece without the source date.\n\nRecording metadata\n- Began: {created}\n- SHA-256: {sha}\n- Original filename: {filename}\n- Transcription model: {gemini}\n- Reconciliation model: {sol} ({reasoning})\n\nORDERED CHUNK TRANSCRIPTS\n",
        created = recording.source_created_at,
        sha = recording.sha256,
        filename = recording.original_filename,
        gemini = GEMINI_MODEL,
        sol = RECONCILIATION_MODEL,
        reasoning = RECONCILIATION_REASONING,
        boundary = INGRESS_BREAK,
    );
    for chunk in chunks {
        prompt.push_str(&format!(
            "\n\nCHUNK {:05} | recording offsets {:.3}–{:.3} seconds\n{}",
            chunk.index,
            chunk.audio_start_ms as f64 / 1_000.0,
            chunk.audio_end_ms as f64 / 1_000.0,
            chunk.transcript_json.as_deref().unwrap_or("{}"),
        ));
    }
    prompt
}

fn split_prompt(transcript: &str) -> String {
    format!(
        "Copy the following final transcript completely and exactly, adding only the exact boundary line `{INGRESS_BREAK}` at sensible conversational or topical boundaries. Using the conservative estimate of one token per four Unicode characters, every resulting piece must be no more than 50,000 estimated tokens. Do not summarize, rewrite, reorder, or omit anything. Each piece must start with a brief copied metadata header that includes the original recording timestamp and SHA-256. Output only the complete marked transcript.\n\nFINAL TRANSCRIPT\n\n{transcript}"
    )
}

fn parse_ingress_pieces(transcript: &str) -> Vec<String> {
    transcript
        .split(INGRESS_BREAK)
        .map(str::trim)
        .filter(|piece| !piece.is_empty())
        .map(str::to_owned)
        .collect()
}

fn estimate_tokens(value: &str) -> u64 {
    (value.chars().count() as u64).div_ceil(ESTIMATED_CHARACTERS_PER_TOKEN)
}

async fn run_sol(codex: &Codex, prompt: &str) -> anyhow::Result<String> {
    let mut request = GenerationRequest::new(prompt, RECONCILIATION_MODEL);
    request.reasoning_effort = ReasoningEffort::XHigh;
    request.ephemeral = true;
    request.timeout = Duration::from_secs(30 * 60);
    codex
        .generate(request)
        .await
        .map(|response| response.answer)
        .context("Sol transcript processing failed")
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
    "SELECT p.id,p.recording_id,r.sha256,r.original_filename,r.source_created_at,p.piece_index,(SELECT COUNT(*) FROM audio_ingress_pieces count_p WHERE count_p.recording_id=p.recording_id),p.transcript_text,p.estimated_tokens,p.phase,p.provenance_id,p.state_json,p.version,p.ingress_failure_count,p.ingress_failures_json,p.created_at,p.updated_at FROM audio_ingress_pieces p JOIN audio_recordings r ON r.id=p.recording_id"
}

fn row_piece(row: &rusqlite::Row<'_>) -> rusqlite::Result<IngressPieceRecord> {
    let state_json: String = row.get(11)?;
    let failures_json: String = row.get(14)?;
    let mut piece_state = serde_json::from_str(&state_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(error))
    })?;
    hydrate_state_chatend_text(&mut piece_state);
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
        created_at: row.get(15)?,
        updated_at: row.get(16)?,
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

async fn get_ingress_piece(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(fetch_piece(&db, &id)?))
}

async fn release_ingress_repairs(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let released = state
        .queue
        .release_repairs_for(SourceKind::Audio)
        .map_err(ApiError::queue)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let ids = {
        let mut statement = db
            .prepare("SELECT id FROM audio_ingress_pieces WHERE phase IN ('ingress_pending','ingress_failed')")
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
            .get(SourceKind::Audio, &id)
            .map_err(ApiError::queue)?
        {
            mirror_queue_job(&db, &job)?;
        }
    }
    Ok(Json(json!({"released":released})))
}

async fn ingress_started(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<StartIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let job = state
        .queue
        .start(
            SourceKind::Audio,
            &id,
            input.expected_version,
            &input.provenance_id,
            input.completion_protocol.as_deref(),
        )
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

async fn ingress_checkpoint(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<CheckpointIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let job = state
        .queue
        .checkpoint(SourceKind::Audio, &id, input.expected_version, &input.state)
        .map_err(ApiError::queue)?;
    Ok(Json(mirror_queue_job(&db, &job)?))
}

async fn ingress_completed(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<VersionedTransition>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let (completed_piece, recording_id, recording_complete) = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        let job = state
            .queue
            .complete(SourceKind::Audio, &id, input.expected_version)
            .map_err(ApiError::queue)?;
        let completed = mirror_queue_job(&db, &job)?;
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
    let db = state.db.lock().map_err(ApiError::internal)?;
    let job = state
        .queue
        .fail(
            SourceKind::Audio,
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

async fn retry_ingress(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_piece(&db, &id)?;
    let job = state
        .queue
        .retry(
            SourceKind::Audio,
            &id,
            input.expected_version,
            input.state.as_ref().unwrap_or(&existing.state),
        )
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
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning,next_attempt_at) VALUES('r',?1,'note.wav','audio/wav',10,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','originals/x.wav','ingressing',?2,?3,?4,'2099-01-01T00:00:00Z')",params!["a".repeat(64),GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
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
            2
        );
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES('next','r',1,'text',1,'ingress_in_progress','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')", []).unwrap();
    }

    #[test]
    fn prepared_audio_pieces_are_adopted_by_the_shared_queue_in_piece_order() {
        let db = database();
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES('r',?1,'note.wav','audio/wav',10,'2026-01-01T00:00:00Z','2026-07-01T00:00:00Z','2026-07-01T00:00:00Z','originals/x.wav','ready_for_ingress',?2,?3,?4)",params!["a".repeat(64),GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
        for (id, index) in [("later", 1), ("first", 0)] {
            db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES(?1,'r',?2,?3,1,'ingress_pending','2026-07-01T00:00:00Z','2026-07-01T00:00:00Z')",params![id,index,format!("piece {index}")]).unwrap();
        }
        let queue = Queue::open(std::path::Path::new(":memory:")).unwrap();

        import_legacy_ingress(&db, &queue).unwrap();

        let next = queue.next().unwrap().unwrap();
        assert_eq!(next.source_kind, SourceKind::Audio);
        assert_eq!(next.source_id, "first");
        assert_eq!(next.source_position, 0);
    }

    #[test]
    fn chunk_boundaries_are_equal_overlapping_and_never_exceed_four_minutes() {
        let boundaries = chunk_boundaries(7 * 60 * 1000);
        assert_eq!(boundaries.len(), 2);
        assert_eq!(
            boundaries[0].1 - boundaries[0].0,
            boundaries[1].1 - boundaries[1].0
        );
        assert_eq!(boundaries[0].1 - boundaries[1].0, 15_000);
        assert!(boundaries.iter().all(|(start, end)| end - start <= 240_000));
        assert_eq!(boundaries.last().unwrap().1, 420_000);
    }

    #[test]
    fn wav_chunks_preserve_the_planned_duration_and_overlap() {
        let root = std::env::temp_dir().join(format!("kennedy-audio-test-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let source = root.join("source.wav");
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: 10,
            bits_per_sample: 16,
            sample_format: SampleFormat::Int,
        };
        let mut writer = WavWriter::create(&source, spec).unwrap();
        for sample in 0_i16..4_200_i16 {
            writer.write_sample(sample).unwrap();
        }
        writer.finalize().unwrap();

        let chunks = split_wav(&source, &root.join("chunks"), "chunks").unwrap();
        assert_eq!(chunks.len(), 2);
        let first = WavReader::open(root.join(&chunks[0].relative_path)).unwrap();
        let second = WavReader::open(root.join(&chunks[1].relative_path)).unwrap();
        assert_eq!(first.duration(), 2_175);
        assert_eq!(second.duration(), 2_175);
        assert_eq!(chunks[0].audio_end_ms - chunks[1].audio_start_ms, 15_000);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn wav_audio_preserves_source_channels_in_ogg_opus() {
        for channels in [1_u16, 2_u16] {
            let root = std::env::temp_dir()
                .join(format!("kennedy-opus-test-{channels}-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            let source = root.join("source.wav");
            let mut writer = WavWriter::create(
                &source,
                hound::WavSpec {
                    channels,
                    sample_rate: 44_100,
                    bits_per_sample: 16,
                    sample_format: SampleFormat::Int,
                },
            )
            .unwrap();
            for frame in 0..4_410 {
                let phase = frame as f32 * 440.0 * std::f32::consts::TAU / 44_100.0;
                for channel in 0..channels {
                    let amplitude = 8_192.0 - f32::from(channel) * 1_024.0;
                    writer
                        .write_sample((phase.sin() * amplitude) as i16)
                        .unwrap();
                }
            }
            writer.finalize().unwrap();

            let opus = wav_to_opus(&source).unwrap();
            assert_eq!(&opus[..4], b"OggS");
            let (decoded, head) = ruopus::decode_ogg_opus(&opus).unwrap();
            assert_eq!(u16::from(head.channel_count), channels);
            assert_eq!(head.input_sample_rate, OPUS_SAMPLE_RATE);
            assert!(decoded.len() >= 4_600 * usize::from(channels));
            assert!(decoded.len() <= 4_800 * usize::from(channels));

            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn transcript_breaks_are_trimmed_and_enforced_with_the_shared_estimate() {
        let transcript = format!("first\n{INGRESS_BREAK}\nsecond");
        assert_eq!(parse_ingress_pieces(&transcript), vec!["first", "second"]);
        assert_eq!(estimate_tokens("12345"), 2);
    }

    #[test]
    fn sha_identity_and_ingress_completion_are_durable() {
        let mut db = database();
        let now = "2026-07-16T10:00:00Z";
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES('r',?1,'note.wav','audio/wav',10,?2,?2,?2,'originals/x.wav','ready_for_ingress',?3,?4,?5)",params!["a".repeat(64),now,GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
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
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning,final_transcript) VALUES('r',?1,'note.wav','audio/wav',10,?2,?2,?2,'originals/x.wav','complete',?3,?4,?5,'Final transcript')",params!["a".repeat(64),now,GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
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
        assert_eq!(
            history.pieces[0].state["historyIngress"]["chatendText"],
            "David\n\nLegacy audio ingress"
        );
    }

    #[test]
    fn filenames_cannot_escape_the_media_directory() {
        assert_eq!(
            safe_filename(Some("../../voice note.wav")),
            "voice_note.wav"
        );
        assert_eq!(safe_filename(None), "vnote.wav");
    }

    #[test]
    fn truncated_wav_is_rejected_as_a_terminal_input_error() {
        let root = std::env::temp_dir().join(format!("kennedy-audio-test-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let source = root.join("truncated.wav");
        let mut writer = WavWriter::create(
            &source,
            hound::WavSpec {
                channels: 1,
                sample_rate: 8_000,
                bits_per_sample: 16,
                sample_format: SampleFormat::Int,
            },
        )
        .unwrap();
        for _ in 0..8_000 {
            writer.write_sample(0_i16).unwrap();
        }
        writer.finalize().unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&source)
            .unwrap()
            .set_len(100)
            .unwrap();

        let error = split_wav(&source, &root.join("chunks"), "chunks").unwrap_err();
        assert!(error.to_string().starts_with("invalid WAV recording"));
        assert!(terminal_processing_failure("chunking", &error));
        assert!(!terminal_processing_failure("transcribing", &error));
        fs::remove_dir_all(root).unwrap();
    }
}
