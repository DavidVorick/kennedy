use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, Query, State},
    http::{HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::{StreamExt, stream};
use hound::{SampleFormat, WavReader, WavWriter};
use kennedy_codex_runtime::{CatalogCache, DEFAULT_CODEX_EXECUTABLE, model_catalog_config};
use reqwest::{Client, header};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, process::Command};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const RELEASE_DEFERRED_INGRESS_MIGRATION: &str =
    include_str!("../migrations/002_release_deferred_ingress.sql");
const GEMINI_INTERACTIONS_URL: &str =
    "https://generativelanguage.googleapis.com/v1beta/interactions";
const GEMINI_FILES_UPLOAD_URL: &str =
    "https://generativelanguage.googleapis.com/upload/v1beta/files";
const GEMINI_FILES_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const GEMINI_MODEL: &str = "gemini-3.1-pro-preview";
const RECONCILIATION_MODEL: &str = "gpt-5.6-sol";
const RECONCILIATION_REASONING: &str = "xhigh";
const CODEX_EXECUTABLE: &str = DEFAULT_CODEX_EXECUTABLE;
const CODEX_PROMPT_BOUNDARY_SENTINEL: &str =
    "KENNEDY_AUDIO_CODEX_PROMPT_BOUNDARY_SENTINEL_4A92E1D7";
const MAX_CHUNK_MILLISECONDS: u64 = 4 * 60 * 1_000;
const CHUNK_OVERLAP_MILLISECONDS: u64 = 15 * 1_000;
const MAX_INGRESS_TOKENS: u64 = 50_000;
const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 4;
const MAX_CONCURRENT_GEMINI_CHUNKS: usize = 4;
const INGRESS_BREAK: &str = "<!-- KENNEDY_INGRESS_BREAK -->";
const INGRESS_FAILURE_LIMIT: i64 = 5;
const INGRESS_RETRY_DELAY_SECONDS: i64 = 15;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: String,
    pub database: PathBuf,
    pub media_directory: PathBuf,
    pub allowed_origins: Vec<String>,
    pub max_upload_bytes: usize,
    pub gemini_api_key: Option<String>,
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    db: Arc<Mutex<Connection>>,
    client: Client,
    codex_model_catalog: Arc<PathBuf>,
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

pub async fn serve(config: Config, codex_catalog_cache: CatalogCache) -> anyhow::Result<()> {
    ensure!(
        MAX_CHUNK_MILLISECONDS > CHUNK_OVERLAP_MILLISECONDS,
        "audio chunk overlap must be smaller than the chunk limit"
    );
    ensure!(
        config.max_upload_bytes > 0,
        "audio upload limit must be positive"
    );
    let codex_catalog = codex_catalog_cache.load().await?;
    ensure!(
        codex_catalog.executable() == CODEX_EXECUTABLE,
        "audio reconciliation uses {CODEX_EXECUTABLE} but the shared Codex catalog belongs to {}",
        codex_catalog.executable()
    );
    ensure!(
        codex_catalog.model_limits(RECONCILIATION_MODEL).is_some(),
        "audio reconciliation model {RECONCILIATION_MODEL} is absent from the Codex model catalog"
    );
    let boundary_scope = format!(
        "kennedy-audio-prompt-boundary-v1:{CODEX_EXECUTABLE}:{RECONCILIATION_MODEL}:{RECONCILIATION_REASONING}"
    );
    if codex_catalog.validation_is_cached(&boundary_scope).await? {
        tracing::info!(
            model = RECONCILIATION_MODEL,
            "Using cached audio Codex prompt-boundary validation"
        );
    } else {
        probe_codex_prompt_boundary(codex_catalog.path()).await?;
        codex_catalog.cache_validation(&boundary_scope).await?;
    }
    ensure_private_directory(&config.media_directory)?;
    let connection = Connection::open(&config.database)
        .with_context(|| format!("opening {}", config.database.display()))?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
    )?;
    apply_migrations(&connection).context("applying audio-ingress migrations")?;

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
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(20 * 60))
        .build()
        .context("building audio-ingress provider client")?;
    let state = AppState {
        config: Arc::new(config),
        db: Arc::new(Mutex::new(connection)),
        client,
        codex_model_catalog: Arc::new(codex_catalog.path().to_owned()),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/api/v1/audio-ingress",
            get(list_recordings).post(upload_recording),
        )
        .route(
            "/api/v1/audio-ingress/by-sha256/{sha256}",
            get(recording_by_sha256),
        )
        .route(
            "/api/v1/audio-ingress/ingress/next",
            get(next_ingress_piece),
        )
        .route(
            "/api/v1/audio-ingress/{recording_id}/history",
            get(get_recording_history),
        )
        .route("/api/v1/audio-ingress/{recording_id}", get(get_recording))
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}",
            get(get_ingress_piece),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/ingress-started",
            post(ingress_started),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/ingress-checkpoint",
            put(ingress_checkpoint),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/ingress-completed",
            post(ingress_completed),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/ingress-failure",
            post(ingress_failure),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/retry-ingress",
            post(retry_ingress),
        )
        .layer(DefaultBodyLimit::max(state.config.max_upload_bytes))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&state.config.bind).await?;
    tracing::info!(address=%state.config.bind, media=%state.config.media_directory.display(), "Audio ingress ready");
    let worker = tokio::spawn(worker_loop(state));
    let server_result = axum::serve(listener, app).await;
    worker.abort();
    server_result?;
    Ok(())
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
            "gemini":if state.config.gemini_api_key.is_some() { "ready" } else { "unconfigured" },
            "gemini_model":GEMINI_MODEL,
            "reconciliation_model":RECONCILIATION_MODEL,
            "chunk_seconds":MAX_CHUNK_MILLISECONDS / 1000,
            "overlap_seconds":CHUNK_OVERLAP_MILLISECONDS / 1000,
        })),
    )
        .into_response()
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

async fn get_recording(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<RecordingRecord>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(fetch_recording(&db, &id)?))
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
    let api_key = state.config.gemini_api_key.as_deref().context(
        "Gemini audio transcription is not configured; store gemini-api-key in Kennedy's vault",
    )?;
    tracing::info!(
        recording_id = %recording.id,
        chunks = chunks.len(),
        concurrency = MAX_CONCURRENT_GEMINI_CHUNKS.min(chunks.len()),
        model = GEMINI_MODEL,
        "Transcribing audio chunks concurrently"
    );
    let mut transcriptions = stream::iter(chunks.into_iter().map(|chunk| {
        let path = state.config.media_directory.join(&chunk.relative_path);
        async move {
            let index = chunk.index;
            let result = transcribe_chunk(&state.client, api_key, &path, recording, &chunk).await;
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
    client: &Client,
    api_key: &str,
    path: &Path,
    recording: &WorkRecording,
    chunk: &ChunkRecord,
) -> anyhow::Result<String> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("reading {}", path.display()))?;
    let start = client
        .post(GEMINI_FILES_UPLOAD_URL)
        .header("x-goog-api-key", api_key)
        .header("X-Goog-Upload-Protocol", "resumable")
        .header("X-Goog-Upload-Command", "start")
        .header("X-Goog-Upload-Header-Content-Length", bytes.len())
        .header("X-Goog-Upload-Header-Content-Type", "audio/wav")
        .json(&json!({"file":{"display_name":format!("kennedy-{}-{:05}.wav",recording.id,chunk.index)}}))
        .send()
        .await
        .context("starting Gemini file upload")?;
    ensure!(
        start.status().is_success(),
        "Gemini rejected file-upload initialization: HTTP {}",
        start.status()
    );
    let upload_url = start
        .headers()
        .get("x-goog-upload-url")
        .and_then(|value| value.to_str().ok())
        .context("Gemini file-upload initialization omitted its upload URL")?
        .to_owned();
    let uploaded = client
        .post(upload_url)
        .header(header::CONTENT_LENGTH, bytes.len())
        .header("X-Goog-Upload-Offset", "0")
        .header("X-Goog-Upload-Command", "upload, finalize")
        .body(bytes)
        .send()
        .await
        .context("uploading Gemini audio file")?;
    let upload_status = uploaded.status();
    let upload_body = uploaded
        .text()
        .await
        .context("reading Gemini upload response")?;
    ensure!(
        upload_status.is_success(),
        "Gemini audio upload failed with HTTP {upload_status}: {}",
        concise_text(&upload_body, 500, "no detail")
    );
    let upload: Value =
        serde_json::from_str(&upload_body).context("Gemini upload returned invalid JSON")?;
    let uri = upload
        .pointer("/file/uri")
        .and_then(Value::as_str)
        .context("Gemini upload returned no file URI")?;
    let file_name = upload
        .pointer("/file/name")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let prompt = transcription_prompt(recording, chunk);
    let request = json!({
        "model": GEMINI_MODEL,
        "input": [
            {"type":"text","text":prompt},
            {"type":"audio","uri":uri,"mime_type":"audio/wav"}
        ],
        "generation_config":{"thinking_level":"high","max_output_tokens":32768},
        "response_format":{
            "type":"text",
            "mime_type":"application/json",
            "schema":transcription_schema()
        },
        "store":false
    });
    let response = client
        .post(GEMINI_INTERACTIONS_URL)
        .header("x-goog-api-key", api_key)
        .json(&request)
        .send()
        .await
        .context("requesting Gemini audio transcription");
    if let Some(name) = file_name {
        let delete_url = format!("{GEMINI_FILES_URL}/{name}");
        let _ = client
            .delete(delete_url)
            .header("x-goog-api-key", api_key)
            .send()
            .await;
    }
    let response = response?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("reading Gemini transcription response")?;
    ensure!(
        status.is_success(),
        "Gemini transcription failed with HTTP {status}: {}",
        concise_text(&body, 800, "no detail")
    );
    let interaction: Value = serde_json::from_str(&body)
        .context("Gemini transcription returned invalid response JSON")?;
    ensure!(
        interaction.get("status").and_then(Value::as_str) == Some("completed"),
        "Gemini transcription did not complete: {}",
        interaction
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    );
    let text =
        interaction_output_text(&interaction).context("Gemini transcription returned no text")?;
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

fn interaction_output_text(interaction: &Value) -> Option<String> {
    if let Some(text) = interaction
        .get("output_text")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return Some(text.to_owned());
    }
    let mut output = Vec::new();
    for step in interaction.get("steps")?.as_array()? {
        if step.get("type").and_then(Value::as_str) != Some("model_output") {
            continue;
        }
        for content in step
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if content.get("type").and_then(Value::as_str) == Some("text")
                && let Some(text) = content.get("text").and_then(Value::as_str)
            {
                output.push(text);
            }
        }
    }
    (!output.is_empty()).then(|| output.join("\n"))
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
    let mut transcript = run_sol(&state.codex_model_catalog, &prompt).await?;
    let mut pieces = parse_ingress_pieces(&transcript);
    if pieces
        .iter()
        .any(|piece| estimate_tokens(piece) > MAX_INGRESS_TOKENS)
    {
        transcript = run_sol(&state.codex_model_catalog, &split_prompt(&transcript)).await?;
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
    for (index, piece) in pieces.iter().enumerate() {
        tx.execute(
            "INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,'ingress_pending',?6,?6)",
            params![Uuid::new_v4().to_string(),recording.id,index as i64,piece,estimate_tokens(piece) as i64,now],
        )?;
    }
    tx.execute(
        "UPDATE audio_recordings SET status='ready_for_ingress',final_transcript=?1,attempt_count=0,next_attempt_at=NULL,last_error=NULL,updated_at=?2 WHERE id=?3",
        params![final_transcript,now,recording.id],
    )?;
    tx.commit()?;
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

async fn run_sol(model_catalog: &Path, prompt: &str) -> anyhow::Result<String> {
    let mut command = Command::new(CODEX_EXECUTABLE);
    command
        .args([
            "-a",
            "never",
            "exec",
            "--json",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--model",
            RECONCILIATION_MODEL,
            "--ephemeral",
        ])
        .arg("-C")
        .arg(std::env::temp_dir())
        .args(["--sandbox", "read-only"]);
    add_codex_config(&mut command, model_catalog);
    command
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("OPENAI_API_KEY")
        .env_remove("CODEX_API_KEY")
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .context("starting codex-safe for transcript reconciliation")?;
    let mut stdin = child
        .stdin
        .take()
        .context("opening Codex transcript input")?;
    tokio::time::timeout(Duration::from_secs(60), stdin.write_all(prompt.as_bytes()))
        .await
        .context("Codex did not accept the transcript prompt in time")??;
    drop(stdin);
    let output = tokio::time::timeout(Duration::from_secs(30 * 60), child.wait_with_output())
        .await
        .context("Sol did not finish transcript processing within 30 minutes")??;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        output.status.success(),
        "Sol transcript processing failed: {}",
        codex_error(&stdout, &stderr)
    );
    parse_codex_answer(&stdout).context("Sol returned no final transcript")
}

fn add_codex_config(command: &mut Command, model_catalog: &Path) {
    command
        .arg("-c")
        .arg(format!(
            "model_reasoning_effort=\"{RECONCILIATION_REASONING}\""
        ))
        .arg("-c")
        .arg("instructions=\"\"")
        .arg("-c")
        .arg("developer_instructions=\"\"")
        .arg("-c")
        .arg("personality=\"none\"")
        .arg("-c")
        .arg("project_doc_max_bytes=0")
        .arg("-c")
        .arg("approval_policy=\"never\"")
        .arg("-c")
        .arg("sandbox_mode=\"read-only\"")
        .arg("-c")
        .arg("include_permissions_instructions=false")
        .arg("-c")
        .arg("include_apps_instructions=false")
        .arg("-c")
        .arg("include_collaboration_mode_instructions=false")
        .arg("-c")
        .arg("include_environment_context=false")
        .arg("-c")
        .arg("skills.include_instructions=false")
        .arg("-c")
        .arg("features.multi_agent=false")
        .arg("-c")
        .arg("features.multi_agent_v2=false")
        .arg("-c")
        .arg("features.apps=false")
        .arg("-c")
        .arg("features.shell_tool=false")
        .arg("-c")
        .arg("features.unified_exec=false")
        .arg("-c")
        .arg("features.code_mode=false")
        .arg("-c")
        .arg("features.code_mode_host=false")
        .arg("-c")
        .arg("features.code_mode_only=false")
        .arg("-c")
        .arg("features.current_time_reminder=false")
        .arg("-c")
        .arg("features.goals=false")
        .arg("-c")
        .arg("features.hooks=false")
        .arg("-c")
        .arg("features.plugins=false")
        .arg("-c")
        .arg("features.remote_plugin=false")
        .arg("-c")
        .arg("features.plugin_sharing=false")
        .arg("-c")
        .arg("features.personality=false")
        .arg("-c")
        .arg("features.browser_use=false")
        .arg("-c")
        .arg("features.browser_use_external=false")
        .arg("-c")
        .arg("features.browser_use_full_cdp_access=false")
        .arg("-c")
        .arg("features.computer_use=false")
        .arg("-c")
        .arg("features.in_app_browser=false")
        .arg("-c")
        .arg("features.image_generation=false")
        .arg("-c")
        .arg("features.memories=false")
        .arg("-c")
        .arg("features.mentions_v2=false")
        .arg("-c")
        .arg("features.request_permissions_tool=false")
        .arg("-c")
        .arg("features.tool_suggest=false")
        .arg("-c")
        .arg("features.workspace_dependencies=false")
        .arg("-c")
        .arg("features.shell_snapshot=false")
        .arg("-c")
        .arg("features.skill_mcp_dependency_install=false")
        .arg("-c")
        .arg("features.guardian_approval=false")
        .arg("-c")
        .arg("features.auth_elicitation=false")
        .arg("-c")
        .arg("features.tool_call_mcp_elicitation=false")
        .arg("-c")
        .arg("features.terminal_visualization_instructions=false")
        .arg("-c")
        .arg("features.use_agent_identity=false")
        .arg("-c")
        .arg("tools.experimental_request_user_input.enabled=false")
        .arg("-c")
        .arg("tools.view_image=false")
        .arg("-c")
        .arg("tools_view_image=false")
        .arg("-c")
        .arg("features.default_mode_request_user_input=false")
        .arg("-c")
        .arg("features.remote_compaction_v2=false")
        .arg("-c")
        .arg("web_search=\"disabled\"")
        .arg("-c")
        .arg(format!("model_auto_compact_token_limit={}", i64::MAX))
        .arg("-c")
        .arg(model_catalog_config(model_catalog));
}

fn verify_codex_prompt_input(output: &[u8]) -> anyhow::Result<()> {
    let inputs: Vec<Value> =
        serde_json::from_slice(output).context("Codex returned invalid prompt-input JSON")?;
    ensure!(
        inputs.len() == 1,
        "Codex reported {} model-visible prompt items instead of the supplied transcript prompt",
        inputs.len()
    );
    let input = inputs[0]
        .as_object()
        .context("Codex prompt-input item is not an object")?;
    let content = input
        .get("content")
        .and_then(Value::as_array)
        .context("Codex prompt-input item has no content array")?;
    ensure!(
        input.get("type").and_then(Value::as_str) == Some("message")
            && input.get("role").and_then(Value::as_str) == Some("user")
            && content.len() == 1
            && content[0].get("type").and_then(Value::as_str) == Some("input_text")
            && content[0].get("text").and_then(Value::as_str)
                == Some(CODEX_PROMPT_BOUNDARY_SENTINEL),
        "Codex altered the supplied transcript prompt item"
    );
    Ok(())
}

async fn probe_codex_prompt_boundary(model_catalog: &Path) -> anyhow::Result<()> {
    let mut command = Command::new(CODEX_EXECUTABLE);
    command
        .args(["debug", "prompt-input"])
        .arg("-c")
        .arg(format!(
            "model={}",
            serde_json::to_string(RECONCILIATION_MODEL)
                .expect("serializing a model name cannot fail")
        ));
    add_codex_config(&mut command, model_catalog);
    let output = command
        .arg(CODEX_PROMPT_BOUNDARY_SENTINEL)
        .current_dir(std::env::temp_dir())
        .env_remove("OPENAI_API_KEY")
        .env_remove("CODEX_API_KEY")
        .output()
        .await
        .context("starting the audio Codex prompt-boundary probe")?;
    ensure!(
        output.status.success(),
        "audio Codex prompt-boundary probe failed"
    );
    verify_codex_prompt_input(&output.stdout)
        .context("audio reconciliation exposed model-visible content outside its supplied prompt")
}

fn parse_codex_answer(stdout: &str) -> Option<String> {
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event.get("type").and_then(Value::as_str) == Some("item.completed"))
        .filter(|event| {
            event.pointer("/item/type").and_then(Value::as_str) == Some("agent_message")
        })
        .filter_map(|event| {
            event
                .pointer("/item/text")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .next_back()
        .filter(|answer| !answer.trim().is_empty())
}

fn codex_error(stdout: &str, stderr: &str) -> String {
    let detail = stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|event| {
            event
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| event.pointer("/error/message").and_then(Value::as_str))
                .map(str::to_owned)
        })
        .next_back()
        .or_else(|| {
            stderr
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "Codex returned no error detail".to_owned());
    concise_text(&detail, 500, "Codex returned no error detail")
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
        state: serde_json::from_str(&state_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
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

async fn next_ingress_piece(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(json!({"piece":fetch_next_ingress_piece(&db)?})))
}

fn fetch_next_ingress_piece(db: &Connection) -> Result<Option<IngressPieceRecord>, ApiError> {
    db.query_row(
        &format!("{} WHERE p.phase IN ('ingress_in_progress','ingress_pending') AND (r.next_attempt_at IS NULL OR datetime(r.next_attempt_at)<=datetime('now')) ORDER BY CASE p.phase WHEN 'ingress_in_progress' THEN 0 ELSE 1 END,datetime(r.source_created_at),p.recording_id,p.piece_index LIMIT 1",piece_select()),
        [], row_piece,
    )
    .optional()
    .map_err(ApiError::internal)
}

async fn ingress_started(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<StartIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    validate_version(input.expected_version)?;
    if input.provenance_id.trim().is_empty() {
        return Err(ApiError::bad("provenance_id must not be empty."));
    }
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_piece(&db, &id)?;
    if existing.phase == "ingress_in_progress"
        && existing.provenance_id.as_deref() == Some(input.provenance_id.as_str())
    {
        return Ok(Json(existing));
    }
    let tx = db.transaction().map_err(ApiError::internal)?;
    let occupied: Option<String> = tx
        .query_row(
            "SELECT id FROM audio_ingress_pieces WHERE phase='ingress_in_progress' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if occupied.as_deref().is_some_and(|piece| piece != id) {
        return Err(ApiError::conflict(
            "Another audio transcript piece is already undergoing ingress.",
        ));
    }
    let now = Utc::now().to_rfc3339();
    let changed=tx.execute("UPDATE audio_ingress_pieces SET phase='ingress_in_progress',provenance_id=?1,updated_at=?2,version=version+1 WHERE id=?3 AND phase='ingress_pending' AND version=?4",params![input.provenance_id,now,id,input.expected_version]).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Audio transcript piece is not ready to start ingress.",
        ));
    }
    tx.execute("UPDATE audio_recordings SET status='ingressing',next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=(SELECT recording_id FROM audio_ingress_pieces WHERE id=?2) AND status='ready_for_ingress'",params![now,id]).map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    Ok(Json(fetch_piece(&db, &id)?))
}

async fn ingress_checkpoint(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<CheckpointIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    validate_version(input.expected_version)?;
    let state_json = serde_json::to_string(&input.state).map_err(ApiError::internal)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let changed=db.execute("UPDATE audio_ingress_pieces SET state_json=?1,updated_at=?2,version=version+1 WHERE id=?3 AND phase='ingress_in_progress' AND version=?4",params![state_json,Utc::now().to_rfc3339(),id,input.expected_version]).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Audio transcript ingress changed in another session.",
        ));
    }
    Ok(Json(fetch_piece(&db, &id)?))
}

async fn ingress_completed(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<VersionedTransition>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    validate_version(input.expected_version)?;
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let existing = fetch_piece(&db, &id)?;
    if existing.phase == "complete" {
        return Ok(Json(existing));
    }
    let tx = db.transaction().map_err(ApiError::internal)?;
    let now = Utc::now().to_rfc3339();
    let changed=tx.execute("UPDATE audio_ingress_pieces SET phase='complete',updated_at=?1,version=version+1 WHERE id=?2 AND phase='ingress_in_progress' AND version=?3",params![now,id,input.expected_version]).map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Audio transcript piece is not in the expected ingress state.",
        ));
    }
    let recording_id = existing.recording_id;
    let remaining: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM audio_ingress_pieces WHERE recording_id=?1 AND phase<>'complete'",
            [&recording_id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    if remaining == 0 {
        tx.execute(
            "UPDATE audio_recordings SET status='complete',next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=?2",
            params![now, recording_id],
        )
        .map_err(ApiError::internal)?;
    } else {
        tx.execute(
            "UPDATE audio_recordings SET next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=?2",
            params![now, recording_id],
        )
        .map_err(ApiError::internal)?;
    }
    tx.commit().map_err(ApiError::internal)?;
    Ok(Json(fetch_piece(&db, &id)?))
}

async fn ingress_failure(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<RecordIngressFailure>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(record_piece_ingress_failure(&mut db, &id, &input)?))
}

fn record_piece_ingress_failure(
    db: &mut Connection,
    id: &str,
    input: &RecordIngressFailure,
) -> Result<IngressPieceRecord, ApiError> {
    validate_version(input.expected_version)?;
    let existing = fetch_piece(db, id)?;
    if !matches!(
        existing.phase.as_str(),
        "ingress_pending" | "ingress_in_progress"
    ) || existing.version != input.expected_version
    {
        return Err(ApiError::conflict(
            "Audio transcript piece is no longer in the expected ingress attempt.",
        ));
    }
    let consecutive_attempt = existing.ingress_failure_count + 1;
    let non_retryable = input.code.as_deref() == Some("input_too_large");
    let terminal = non_retryable || consecutive_attempt >= INGRESS_FAILURE_LIMIT;
    let mut failures = existing
        .ingress_failures
        .as_array()
        .cloned()
        .unwrap_or_default();
    let attempt = failures.len() as i64 + 1;
    failures.push(json!({
        "attempt":attempt,"occurred_at":Utc::now().to_rfc3339(),
        "stage":concise_text(&input.stage,80,"unknown"),
        "code":input.code.as_deref().map(|value|concise_text(value,80,"unknown_error")),
        "message":concise_text(&input.message,2000,"Audio ingress failed without an error message."),
        "rounds_used":input.rounds_used,"context_tokens":input.context_tokens,"context_window_tokens":input.context_window_tokens,
    }));
    let next_phase = if terminal {
        "ingress_failed"
    } else {
        "ingress_pending"
    };
    let now = Utc::now().to_rfc3339();
    let tx = db.transaction().map_err(ApiError::internal)?;
    tx.execute("UPDATE audio_ingress_pieces SET phase=?1,ingress_failure_count=?2,ingress_failures_json=?3,updated_at=?4,version=version+1 WHERE id=?5 AND version=?6",params![next_phase,consecutive_attempt,serde_json::to_string(&failures).map_err(ApiError::internal)?,now,id,input.expected_version]).map_err(ApiError::internal)?;
    if terminal {
        let failure_summary = if non_retryable {
            format!(
                "Transcript piece {} requires manual ingress retry after a non-retryable input error",
                existing.piece_index + 1
            )
        } else {
            format!(
                "Transcript piece {} exhausted its ingress attempts",
                existing.piece_index + 1
            )
        };
        tx.execute("UPDATE audio_recordings SET status='ingress_failed',next_attempt_at=NULL,last_error=?1,updated_at=?2 WHERE id=?3",params![failure_summary,now,existing.recording_id]).map_err(ApiError::internal)?;
    } else {
        let next_attempt_at = (Utc::now()
            + ChronoDuration::seconds(ingress_retry_delay_seconds(consecutive_attempt)))
        .to_rfc3339();
        tx.execute(
            "UPDATE audio_recordings SET next_attempt_at=?1,last_error=?2,updated_at=?3 WHERE id=?4",
            params![
                next_attempt_at,
                format!(
                    "Transcript piece {} ingress attempt {} failed; retry is scheduled",
                    existing.piece_index + 1,
                    consecutive_attempt
                ),
                now,
                existing.recording_id
            ],
        )
        .map_err(ApiError::internal)?;
    }
    tx.commit().map_err(ApiError::internal)?;
    fetch_piece(db, id)
}

fn ingress_retry_delay_seconds(_consecutive_attempt: i64) -> i64 {
    INGRESS_RETRY_DELAY_SECONDS
}

async fn retry_ingress(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<IngressPieceRecord>, ApiError> {
    validate_version(input.expected_version)?;
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(retry_failed_ingress(
        &mut db,
        &id,
        input.expected_version,
        input.state.as_ref(),
    )?))
}

fn retry_failed_ingress(
    db: &mut Connection,
    id: &str,
    expected_version: i64,
    replacement_state: Option<&Value>,
) -> Result<IngressPieceRecord, ApiError> {
    let existing = fetch_piece(db, id)?;
    if existing.phase != "ingress_failed" || existing.version != expected_version {
        return Err(ApiError::conflict(
            "Audio transcript piece is not in the expected failed state.",
        ));
    }
    let tx = db.transaction().map_err(ApiError::internal)?;
    let now = Utc::now().to_rfc3339();
    let state_json = serde_json::to_string(replacement_state.unwrap_or(&existing.state))
        .map_err(ApiError::internal)?;
    let changed = tx
        .execute(
            "UPDATE audio_ingress_pieces SET phase='ingress_pending',state_json=?1,ingress_failure_count=0,updated_at=?2,version=version+1 WHERE id=?3 AND phase='ingress_failed' AND version=?4",
            params![state_json, now, id, expected_version],
        )
        .map_err(ApiError::internal)?;
    if changed == 0 {
        return Err(ApiError::conflict(
            "Audio transcript piece changed before it could be retried.",
        ));
    }
    tx.execute(
        "UPDATE audio_recordings SET status='ready_for_ingress',next_attempt_at=NULL,last_error=NULL,updated_at=?1 WHERE id=?2",
        params![now, existing.recording_id],
    )
    .map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    fetch_piece(db, id)
}

fn validate_version(value: i64) -> Result<(), ApiError> {
    if value < 1 {
        Err(ApiError::bad("expected_version must be positive."))
    } else {
        Ok(())
    }
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

    #[test]
    fn audio_codex_config_removes_hidden_prompts() {
        let mut command = Command::new("codex-safe");
        let catalog = Path::new("/tmp/kennedy-codex-catalogs/audio-models.json");
        add_codex_config(&mut command, catalog);
        let arguments = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(arguments.contains(&"instructions=\"\"".into()));
        assert!(arguments.contains(&"developer_instructions=\"\"".into()));
        assert!(arguments.contains(&model_catalog_config(catalog)));
        assert!(arguments.contains(&"tools.view_image=false".into()));
        assert_eq!(
            arguments
                .iter()
                .filter(|argument| argument.starts_with("instructions="))
                .collect::<Vec<_>>(),
            vec![&"instructions=\"\"".to_owned()]
        );
    }

    #[test]
    fn audio_codex_prompt_boundary_rejects_extra_model_visible_items() {
        let exact = format!(
            r#"[{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{CODEX_PROMPT_BOUNDARY_SENTINEL}"}}]}}]"#
        );
        verify_codex_prompt_input(exact.as_bytes()).unwrap();
        let hidden = format!(
            r#"[{{"type":"message","role":"developer","content":[{{"type":"input_text","text":"hidden"}}]}},{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{CODEX_PROMPT_BOUNDARY_SENTINEL}"}}]}}]"#
        );
        assert!(verify_codex_prompt_input(hidden.as_bytes()).is_err());
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
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,state_json,created_at,updated_at) VALUES('p','r',0,'Final transcript',4,'complete',?1,?2,?2)",params![r#"{"historyIngress":{"completed":true}}"#,now]).unwrap();

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

    #[test]
    fn kennedy_ingress_retries_after_fifteen_seconds() {
        assert_eq!(ingress_retry_delay_seconds(1), 15);
        assert_eq!(ingress_retry_delay_seconds(2), 15);
        assert_eq!(ingress_retry_delay_seconds(3), 15);
        assert_eq!(ingress_retry_delay_seconds(4), 15);
    }

    #[test]
    fn failed_attempt_releases_the_audio_claim_while_its_retry_is_deferred() {
        let mut db = database();
        let now = Utc::now().to_rfc3339();
        for (recording, piece, source) in [
            ("first", "p1", "2026-01-01T00:00:00Z"),
            ("second", "p2", "2026-01-02T00:00:00Z"),
        ] {
            db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES(?1,?2,'note.wav','audio/wav',10,?3,?3,?4,'originals/x.wav','ingressing',?5,?6,?7)",params![recording,if recording == "first" { "a".repeat(64) } else { "b".repeat(64) },source,now,GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
            db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES(?1,?2,0,'text',1,?3,?4,?4)",params![piece,recording,if recording == "first" { "ingress_in_progress" } else { "ingress_pending" },now]).unwrap();
        }

        let failed = record_piece_ingress_failure(
            &mut db,
            "p1",
            &RecordIngressFailure {
                expected_version: 1,
                stage: "model_loop".into(),
                code: Some("provider_error".into()),
                message: "temporary failure".into(),
                rounds_used: Some(1),
                context_tokens: None,
                context_window_tokens: None,
            },
        )
        .unwrap();

        assert_eq!(failed.phase, "ingress_pending");
        assert_eq!(fetch_next_ingress_piece(&db).unwrap().unwrap().id, "p2");
        assert!(
            db.execute(
                "UPDATE audio_ingress_pieces SET phase='ingress_in_progress' WHERE id='p2'",
                []
            )
            .is_ok()
        );
    }

    #[test]
    fn oversized_audio_ingress_is_terminal_without_repeating_the_same_request() {
        let mut db = database();
        let now = Utc::now().to_rfc3339();
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning) VALUES('r',?1,'note.wav','audio/wav',10,?2,?2,?2,'originals/x.wav','ingressing',?3,?4,?5)",params!["a".repeat(64),now,GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING]).unwrap();
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,created_at,updated_at) VALUES('p','r',0,'text',1,'ingress_in_progress',?1,?1)",[&now]).unwrap();

        let failed = record_piece_ingress_failure(
            &mut db,
            "p",
            &RecordIngressFailure {
                expected_version: 1,
                stage: "model_loop".into(),
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
        assert!(fetch_next_ingress_piece(&db).unwrap().is_none());
        assert!(
            fetch_recording(&db, "r")
                .unwrap()
                .last_error
                .unwrap()
                .contains("manual ingress retry")
        );
    }

    #[test]
    fn deferred_ingress_is_not_selected_and_terminal_retry_preserves_diagnostics() {
        let mut db = database();
        let now = Utc::now().to_rfc3339();
        let future = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
        db.execute("INSERT INTO audio_recordings(id,sha256,original_filename,content_type,size_bytes,source_created_at,received_at,updated_at,original_relative_path,status,gemini_model,reconciliation_model,reconciliation_reasoning,next_attempt_at,last_error) VALUES('r',?1,'note.wav','audio/wav',10,?2,?2,?2,'originals/x.wav','ingress_failed',?3,?4,?5,?6,'failed')",params!["a".repeat(64),now,GEMINI_MODEL,RECONCILIATION_MODEL,RECONCILIATION_REASONING,future]).unwrap();
        db.execute("INSERT INTO audio_ingress_pieces(id,recording_id,piece_index,transcript_text,estimated_tokens,phase,version,ingress_failure_count,ingress_failures_json,created_at,updated_at) VALUES('p','r',0,'text',1,'ingress_failed',3,5,?1,?2,?2)",params![r#"[{"attempt":1,"message":"provider failed"}]"#,now]).unwrap();

        assert!(fetch_next_ingress_piece(&db).unwrap().is_none());
        let retried =
            retry_failed_ingress(&mut db, "p", 3, Some(&json!({"retry":"fresh"}))).unwrap();
        assert_eq!(retried.phase, "ingress_pending");
        assert_eq!(retried.ingress_failure_count, 0);
        assert_eq!(retried.state["retry"], "fresh");
        assert_eq!(retried.ingress_failures[0]["message"], "provider failed");
        assert_eq!(retried.version, 4);
        assert_eq!(
            fetch_recording(&db, "r").unwrap().status,
            "ready_for_ingress"
        );
        assert_eq!(fetch_next_ingress_piece(&db).unwrap().unwrap().id, "p");
    }
}
