use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Duration, Utc};
use kcode_audio_ingress::{
    AudioIngress, AudioInput, ErrorKind as LibraryErrorKind, RecordingState, RecordingStatus,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub(crate) const COMPLETION_PROTOCOL: &str = "end-session-v2";
const FAILURE_LIMIT: i64 = 5;
const RETRY_DELAY_SECONDS: i64 = 15;
const MAX_INGRESS_TOKENS: u64 = 50_000;
const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 4;

const QUEUE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS kennedy_audio_ingress_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    recording_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    source_created_at TEXT NOT NULL,
    piece_index INTEGER NOT NULL CHECK(piece_index >= 0),
    piece_count INTEGER NOT NULL CHECK(piece_count > 0),
    transcript_text TEXT NOT NULL,
    estimated_tokens INTEGER NOT NULL CHECK(estimated_tokens > 0),
    phase TEXT NOT NULL CHECK(phase IN (
        'ingress_pending', 'ingress_in_progress', 'ingress_failed', 'complete'
    )),
    provenance_id TEXT,
    state_json TEXT NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count >= 0),
    failures_json TEXT NOT NULL DEFAULT '[]',
    next_attempt_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(recording_id, piece_index)
);

CREATE INDEX IF NOT EXISTS kennedy_audio_ingress_queue
ON kennedy_audio_ingress_jobs(phase, next_attempt_at, source_created_at, piece_index, id);

CREATE UNIQUE INDEX IF NOT EXISTS one_kennedy_audio_ingress_in_progress
ON kennedy_audio_ingress_jobs((1)) WHERE phase='ingress_in_progress';
"#;

#[derive(Clone)]
pub(crate) struct Service {
    audio: AudioIngress,
    queue: Queue,
    max_upload_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct ServiceError {
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

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "Kennedy audio adapter failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected Kennedy audio error occurred.",
        )
    }
}

impl From<ApiError> for ServiceError {
    fn from(error: ApiError) -> Self {
        Self {
            status: error.status.as_u16(),
            code: error.code,
            message: error.message,
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

impl Service {
    pub(crate) fn open(
        audio: AudioIngress,
        queue_database: &Path,
        legacy_audio_database: Option<&Path>,
        max_upload_bytes: usize,
    ) -> anyhow::Result<Self> {
        ensure!(max_upload_bytes > 0, "audio upload limit must be positive");
        let queue = Queue::open(queue_database)?;
        if let Some(path) = legacy_audio_database.filter(|path| path.exists()) {
            queue.import_audio_ingress_database(path).with_context(|| {
                format!("importing audio ingress queue from {}", path.display())
            })?;
        }
        let service = Self {
            audio,
            queue,
            max_upload_bytes,
        };
        service
            .synchronize_completed_transcripts()
            .context("preparing completed audio transcripts for Kennedy")?;
        Ok(service)
    }

    pub(crate) fn next_ingress_piece(&self) -> Result<Option<Value>, ServiceError> {
        self.synchronize_completed_transcripts()
            .map_err(ApiError::internal)?;
        let job = self
            .queue
            .next()
            .map_err(ApiError::from)
            .map_err(ServiceError::from)?;
        job.map(json_value).transpose()
    }

    pub(crate) async fn get_json(&self, path: &str) -> Result<Value, ServiceError> {
        if path == "/api/v1/audio-ingress/health" {
            self.audio.status().map_err(library_error)?;
            return Ok(json!({"service":"audio-ingress","status":"ok"}));
        }
        let id = path
            .strip_prefix("/api/v1/audio-ingress/pieces/")
            .filter(|id| !id.contains('/'))
            .ok_or_else(ApiError::not_found)?;
        let id = decode_piece_id(id)?;
        let job = self
            .queue
            .get(&id)
            .map_err(ApiError::from)?
            .ok_or_else(ApiError::not_found)?;
        json_value(job)
    }

    pub(crate) async fn post_json(&self, path: &str, body: Value) -> Result<Value, ServiceError> {
        if path == "/api/v1/audio-ingress/ingress/repairs/release" {
            let released = self.queue.release_repairs().map_err(ApiError::from)?;
            return Ok(json!({"released":released}));
        }
        let tail = path
            .strip_prefix("/api/v1/audio-ingress/pieces/")
            .ok_or_else(ApiError::not_found)?;
        let (id, action) = tail.split_once('/').ok_or_else(ApiError::not_found)?;
        let id = decode_piece_id(id)?;
        let job = match action {
            "ingress-started" => {
                let input: StartIngress = parse_body(body)?;
                self.queue.start(&id, &input)
            }
            "ingress-completed" => {
                let input: VersionedTransition = parse_body(body)?;
                self.queue.complete(&id, input.expected_version)
            }
            "ingress-failure" => {
                let input: RecordIngressFailure = parse_body(body)?;
                self.queue.fail(&id, &input)
            }
            "retry-ingress" => {
                let input: RetryIngress = parse_body(body)?;
                self.queue
                    .retry(&id, input.expected_version, input.state.as_ref())
            }
            _ => return Err(ApiError::not_found().into()),
        }
        .map_err(ApiError::from)?;
        json_value(job)
    }

    pub(crate) async fn put_json(&self, path: &str, body: Value) -> Result<Value, ServiceError> {
        let id = path
            .strip_prefix("/api/v1/audio-ingress/pieces/")
            .and_then(|tail| tail.strip_suffix("/ingress-checkpoint"))
            .ok_or_else(ApiError::not_found)?;
        let id = decode_piece_id(id)?;
        let input: CheckpointIngress = parse_body(body)?;
        let job = self.queue.checkpoint(&id, &input).map_err(ApiError::from)?;
        json_value(job)
    }

    fn synchronize_completed_transcripts(&self) -> anyhow::Result<()> {
        for recording in self.audio.status().map_err(anyhow::Error::new)?.recordings {
            let RecordingState::Complete { transcript } = &recording.state else {
                continue;
            };
            self.queue.ensure_recording(&recording, transcript)?;
        }
        Ok(())
    }

    fn browser_snapshot(&self) -> Result<Vec<BrowserRecording>, ApiError> {
        self.synchronize_completed_transcripts()
            .map_err(ApiError::internal)?;
        let mut recordings = Vec::new();
        for recording in self.audio.status().map_err(library_error)?.recordings {
            let jobs = self
                .queue
                .for_recording(recording.id)
                .map_err(ApiError::from)?;
            recordings.push(BrowserRecording::from_status(recording, &jobs));
        }
        Ok(recordings)
    }
}

pub(crate) fn router(service: Service) -> Router {
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
            get(recording_history),
        )
        .route(
            "/api/v1/audio-ingress/{recording_id}/retry",
            post(retry_recording),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/retry-ingress",
            post(retry_ingress),
        )
        .layer(DefaultBodyLimit::max(service.max_upload_bytes))
        .with_state(service)
}

async fn health(State(service): State<Service>) -> Result<Json<Value>, ApiError> {
    service.audio.status().map_err(library_error)?;
    Ok(Json(json!({
        "service":"audio-ingress",
        "status":"ok",
        "transcriber":"ready",
        "input":"bytes",
    })))
}

async fn upload_recording(
    State(service): State<Service>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut recorded_at = None;
    let mut original_filename = None;
    let mut bytes = None;
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
                recorded_at = Some(
                    DateTime::parse_from_rfc3339(value.trim())
                        .map_err(|_| {
                            ApiError::bad(
                                "recorded_at must be an RFC 3339 timestamp with an offset.",
                            )
                        })?
                        .with_timezone(&Utc),
                );
            }
            Some("file") => {
                if bytes.is_some() {
                    return Err(ApiError::bad("Exactly one audio file is required."));
                }
                original_filename = field.file_name().map(str::to_owned);
                let mut audio = Vec::new();
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|_| ApiError::bad("The audio file upload was interrupted."))?
                {
                    if audio.len().saturating_add(chunk.len()) > service.max_upload_bytes {
                        return Err(ApiError::new(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "audio_too_large",
                            "The audio recording exceeds Kennedy's configured upload limit.",
                        ));
                    }
                    audio.extend_from_slice(&chunk);
                }
                bytes = Some(audio);
            }
            _ => {}
        }
    }
    let bytes = bytes
        .filter(|bytes| !bytes.is_empty())
        .ok_or_else(|| ApiError::bad("A non-empty multipart field named file is required."))?;
    let recorded_at = recorded_at.ok_or_else(|| {
        ApiError::bad("recorded_at is required and must describe when recording began.")
    })?;
    let submission = service
        .audio
        .submit(AudioInput {
            bytes,
            recorded_at,
            original_filename,
        })
        .await
        .map_err(library_error)?;
    let recording = service
        .browser_snapshot()?
        .into_iter()
        .find(|recording| recording.id == submission.recording_id)
        .ok_or_else(ApiError::not_found)?;
    Ok((
        if submission.deduplicated {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        Json(json!({
            "recording":recording,
            "deduplicated":submission.deduplicated,
        })),
    ))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default = "default_list_limit")]
    limit: usize,
}

fn default_list_limit() -> usize {
    100
}

async fn list_recordings(
    State(service): State<Service>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 50_000);
    let recordings = service
        .browser_snapshot()?
        .into_iter()
        .take(limit)
        .collect::<Vec<_>>();
    Ok(Json(json!({"recordings":recordings})))
}

async fn recording_by_sha256(
    State(service): State<Service>,
    AxumPath(sha256): AxumPath<String>,
) -> Result<Json<BrowserRecording>, ApiError> {
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::bad(
            "sha256 must contain 64 hexadecimal characters.",
        ));
    }
    service
        .browser_snapshot()?
        .into_iter()
        .find(|recording| recording.sha256 == sha256.to_ascii_lowercase())
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn recording_history(
    State(service): State<Service>,
    AxumPath(recording_id): AxumPath<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let recording = service
        .browser_snapshot()?
        .into_iter()
        .find(|recording| recording.id == recording_id)
        .ok_or_else(ApiError::not_found)?;
    let transcript = service
        .audio
        .status()
        .map_err(library_error)?
        .recordings
        .into_iter()
        .find(|candidate| candidate.id == recording_id)
        .and_then(|candidate| match candidate.state {
            RecordingState::Complete { transcript } => Some(transcript),
            _ => None,
        });
    let pieces = service
        .queue
        .for_recording(recording_id)
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "recording":recording,
        "final_transcript":transcript,
        "chunks":[],
        "pieces":pieces,
    })))
}

async fn retry_recording(
    State(service): State<Service>,
    AxumPath(recording_id): AxumPath<Uuid>,
) -> Result<Json<Value>, ApiError> {
    service.audio.retry(recording_id).map_err(library_error)?;
    Ok(Json(json!({"recording_id":recording_id,"queued":true})))
}

async fn retry_ingress(
    State(service): State<Service>,
    AxumPath(piece_id): AxumPath<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<Job>, ApiError> {
    service
        .queue
        .retry(&piece_id, input.expected_version, input.state.as_ref())
        .map(Json)
        .map_err(ApiError::from)
}

#[derive(Clone, Debug, Serialize)]
struct BrowserRecording {
    id: Uuid,
    sha256: String,
    original_filename: String,
    content_type: &'static str,
    size_bytes: u64,
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
    transcript_piece_count: usize,
    completed_piece_count: usize,
}

impl BrowserRecording {
    fn from_status(recording: RecordingStatus, jobs: &[Job]) -> Self {
        let (mut status, transcription_status, attempt_count, last_error) = match recording.state {
            RecordingState::Queued => ("uploaded".into(), None, 0, None),
            RecordingState::Processing { attempt, progress } => (
                processing_stage(&progress).into(),
                serde_json::to_value(progress).ok(),
                i64::from(attempt),
                None,
            ),
            RecordingState::Complete { .. } => ("ready_for_ingress".into(), None, 0, None),
            RecordingState::Failed {
                attempts, error, ..
            } => ("failed".into(), None, i64::from(attempts), Some(error)),
        };
        if !jobs.is_empty() {
            status = if jobs.iter().all(|job| job.phase == "complete") {
                "complete".into()
            } else if jobs.iter().any(|job| job.phase == "ingress_failed") {
                "ingress_failed".into()
            } else if jobs.iter().any(|job| job.phase == "ingress_in_progress") {
                "ingressing".into()
            } else {
                "ready_for_ingress".into()
            };
        }
        let completed_piece_count = jobs.iter().filter(|job| job.phase == "complete").count();
        Self {
            id: recording.id,
            sha256: recording.sha256,
            original_filename: recording.original_filename,
            content_type: "audio/wav",
            size_bytes: recording.size_bytes,
            source_created_at: recording.recorded_at.to_rfc3339(),
            received_at: recording.received_at.to_rfc3339(),
            updated_at: recording.received_at.to_rfc3339(),
            status,
            gemini_model: recording.transcription_model,
            reconciliation_model: recording.reconciliation_model,
            reconciliation_reasoning: recording.reconciliation_reasoning,
            transcription_status,
            attempt_count,
            next_attempt_at: None,
            last_error,
            transcript_piece_count: jobs.len(),
            completed_piece_count,
        }
    }
}

fn processing_stage(status: &kcode_audio_ingress::TranscriptionStatus) -> &'static str {
    let plan_complete = status.steps.iter().any(|entry| {
        entry.step == kcode_audio_ingress::Step::PlanChunks
            && entry.state == kcode_audio_ingress::StepState::Completed
    });
    if !plan_complete {
        return "chunking";
    }
    let chunks_complete = status
        .steps
        .iter()
        .filter(|entry| {
            matches!(
                entry.step,
                kcode_audio_ingress::Step::TranscribeChunk { .. }
            )
        })
        .all(|entry| entry.state == kcode_audio_ingress::StepState::Completed);
    if chunks_complete {
        "reconciling"
    } else {
        "transcribing"
    }
}

#[derive(Clone)]
struct Queue {
    db: Arc<Mutex<Connection>>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Job {
    id: String,
    recording_id: Uuid,
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

impl Queue {
    fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection =
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;",
        )?;
        connection.execute_batch(QUEUE_SCHEMA)?;
        connection.execute(
            "UPDATE kennedy_audio_ingress_jobs
             SET phase='ingress_pending',next_attempt_at=?1,updated_at=?2,version=version+1
             WHERE phase='ingress_in_progress'",
            params![
                (Utc::now() + Duration::seconds(RETRY_DELAY_SECONDS)).to_rfc3339(),
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(Self {
            db: Arc::new(Mutex::new(connection)),
        })
    }

    fn ensure_recording(
        &self,
        recording: &RecordingStatus,
        transcript: &str,
    ) -> anyhow::Result<()> {
        let mut db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("Kennedy audio queue lock was poisoned"))?;
        let exists = db.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM kennedy_audio_ingress_jobs WHERE recording_id=?1
             )",
            [recording.id.to_string()],
            |row| row.get::<_, i64>(0),
        )? == 1;
        if exists {
            return Ok(());
        }
        let pieces = split_transcript(transcript)?;
        let piece_count = i64::try_from(pieces.len())?;
        let now = Utc::now().to_rfc3339();
        let tx = db.transaction()?;
        for (index, piece) in pieces.into_iter().enumerate() {
            let index = i64::try_from(index)?;
            let id = format!("{}:{index}", recording.id);
            tx.execute(
                "INSERT INTO kennedy_audio_ingress_jobs(
                    id,recording_id,sha256,original_filename,source_created_at,
                    piece_index,piece_count,transcript_text,estimated_tokens,
                    phase,created_at,updated_at
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'ingress_pending',?10,?10)",
                params![
                    id,
                    recording.id.to_string(),
                    recording.sha256,
                    recording.original_filename,
                    recording.recorded_at.to_rfc3339(),
                    index,
                    piece_count,
                    piece,
                    estimate_tokens(&piece) as i64,
                    now,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn import_audio_ingress_database(&self, path: &Path) -> anyhow::Result<()> {
        let legacy = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let has_table = legacy.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_schema
                WHERE type='table' AND name='audio_ingress_pieces'
             )",
            [],
            |row| row.get::<_, i64>(0),
        )? == 1;
        if !has_table {
            return Ok(());
        }
        let mut statement = legacy.prepare(
            "SELECT p.id,p.recording_id,r.sha256,r.original_filename,r.source_created_at,
                    p.piece_index,
                    (SELECT COUNT(*) FROM audio_ingress_pieces c
                     WHERE c.recording_id=p.recording_id),
                    p.transcript_text,p.estimated_tokens,p.phase,p.provenance_id,
                    p.state_json,p.version,p.ingress_failure_count,p.ingress_failures_json,
                    p.next_attempt_at,p.created_at,p.updated_at
             FROM audio_ingress_pieces p
             JOIN audio_recordings r ON r.id=p.recording_id
             ORDER BY datetime(r.source_created_at),p.piece_index,p.id",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, i64>(13)?,
                    row.get::<_, String>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, String>(16)?,
                    row.get::<_, String>(17)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(legacy);
        let mut db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("Kennedy audio queue lock was poisoned"))?;
        let tx = db.transaction()?;
        for (
            id,
            recording_id,
            sha256,
            filename,
            created,
            index,
            count,
            text,
            tokens,
            mut phase,
            provenance,
            state,
            mut version,
            failures,
            failure_log,
            mut next,
            row_created,
            updated,
        ) in rows
        {
            if phase == "ingress_in_progress" {
                phase = "ingress_pending".into();
                version += 1;
                next = Some((Utc::now() + Duration::seconds(RETRY_DELAY_SECONDS)).to_rfc3339());
            }
            tx.execute(
                "INSERT INTO kennedy_audio_ingress_jobs(
                    id,recording_id,sha256,original_filename,source_created_at,
                    piece_index,piece_count,transcript_text,estimated_tokens,phase,
                    provenance_id,state_json,version,failure_count,failures_json,
                    next_attempt_at,created_at,updated_at
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)
                 ON CONFLICT(recording_id,piece_index) DO NOTHING",
                params![
                    id,
                    recording_id,
                    sha256,
                    filename,
                    created,
                    index,
                    count,
                    text,
                    tokens,
                    phase,
                    provenance,
                    state,
                    version,
                    failures,
                    failure_log,
                    next,
                    row_created,
                    updated,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn for_recording(&self, recording_id: Uuid) -> Result<Vec<Job>, QueueError> {
        let db = self.lock()?;
        let mut statement = db
            .prepare(&format!(
                "{} WHERE recording_id=?1 ORDER BY piece_index,id",
                job_select()
            ))
            .map_err(QueueError::internal)?;
        statement
            .query_map([recording_id.to_string()], row_job)
            .map_err(QueueError::internal)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(QueueError::internal)
    }

    fn get(&self, id: &str) -> Result<Option<Job>, QueueError> {
        let db = self.lock()?;
        optional_job(&db, id)
    }

    fn next(&self) -> Result<Option<Job>, QueueError> {
        let db = self.lock()?;
        db.query_row(
            &format!(
                "{} WHERE phase IN ('ingress_in_progress','ingress_pending')
                 AND (next_attempt_at IS NULL OR datetime(next_attempt_at)<=datetime('now'))
                 ORDER BY CASE phase WHEN 'ingress_in_progress' THEN 0 ELSE 1 END,
                          datetime(source_created_at),piece_index,id
                 LIMIT 1",
                job_select()
            ),
            [],
            row_job,
        )
        .optional()
        .map_err(QueueError::internal)
    }

    fn start(&self, id: &str, input: &StartIngress) -> Result<Job, QueueError> {
        validate_version(input.expected_version)?;
        if input.provenance_id.trim().is_empty() {
            return Err(QueueError::invalid("provenance_id must not be empty."));
        }
        if input.completion_protocol.as_deref() != Some(COMPLETION_PROTOCOL) {
            return Err(QueueError::conflict(
                "This client does not support the required completion protocol.",
            ));
        }
        let db = self.lock()?;
        let existing = fetch_job(&db, id)?;
        if existing.phase == "ingress_in_progress"
            && existing.provenance_id.as_deref() == Some(&input.provenance_id)
        {
            return Ok(existing);
        }
        let changed = db
            .execute(
                "UPDATE kennedy_audio_ingress_jobs
                 SET phase='ingress_in_progress',provenance_id=?1,next_attempt_at=NULL,
                     updated_at=?2,version=version+1
                 WHERE id=?3 AND phase='ingress_pending' AND version=?4
                   AND NOT EXISTS(
                       SELECT 1 FROM kennedy_audio_ingress_jobs
                       WHERE phase='ingress_in_progress'
                   )",
                params![
                    input.provenance_id,
                    Utc::now().to_rfc3339(),
                    id,
                    input.expected_version
                ],
            )
            .map_err(QueueError::internal)?;
        if changed == 0 {
            return Err(QueueError::conflict(
                "Another job is active or this job is not ready.",
            ));
        }
        fetch_job(&db, id)
    }

    fn checkpoint(&self, id: &str, input: &CheckpointIngress) -> Result<Job, QueueError> {
        validate_version(input.expected_version)?;
        let state = serde_json::to_string(&input.state).map_err(QueueError::internal)?;
        let db = self.lock()?;
        let changed = db
            .execute(
                "UPDATE kennedy_audio_ingress_jobs
                 SET state_json=?1,updated_at=?2,version=version+1
                 WHERE id=?3 AND phase='ingress_in_progress' AND version=?4",
                params![state, Utc::now().to_rfc3339(), id, input.expected_version],
            )
            .map_err(QueueError::internal)?;
        if changed == 0 {
            return Err(QueueError::conflict(
                "Audio ingress changed in another session.",
            ));
        }
        fetch_job(&db, id)
    }

    fn complete(&self, id: &str, expected_version: i64) -> Result<Job, QueueError> {
        validate_version(expected_version)?;
        let db = self.lock()?;
        let existing = fetch_job(&db, id)?;
        if existing.phase == "complete" {
            return Ok(existing);
        }
        if !has_successful_final_commit(&existing.state) {
            return Err(QueueError::conflict(
                "Audio ingress cannot complete without a successful Chatend/Kweb commit.",
            ));
        }
        let changed = db
            .execute(
                "UPDATE kennedy_audio_ingress_jobs
                 SET phase='complete',
                     state_json=json_remove(state_json,'$.historyIngressRepairRequired'),
                     next_attempt_at=NULL,updated_at=?1,version=version+1
                 WHERE id=?2 AND phase='ingress_in_progress' AND version=?3",
                params![Utc::now().to_rfc3339(), id, expected_version],
            )
            .map_err(QueueError::internal)?;
        if changed == 0 {
            return Err(QueueError::conflict(
                "Audio ingress is not in the expected state.",
            ));
        }
        fetch_job(&db, id)
    }

    fn fail(&self, id: &str, input: &RecordIngressFailure) -> Result<Job, QueueError> {
        validate_version(input.expected_version)?;
        let mut db = self.lock()?;
        let tx = db.transaction().map_err(QueueError::internal)?;
        let existing = fetch_job(&tx, id)?;
        if !matches!(
            existing.phase.as_str(),
            "ingress_pending" | "ingress_in_progress"
        ) || existing.version != input.expected_version
        {
            return Err(QueueError::conflict(
                "Audio ingress is no longer in the expected attempt.",
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
            "stage":concise(&input.stage,80,"unknown"),
            "code":input.code.as_deref().map(|value|concise(value,80,"unknown_error")),
            "message":concise(&input.message,2000,"Audio ingress failed."),
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
        let next = (!terminal).then(|| (now + Duration::seconds(RETRY_DELAY_SECONDS)).to_rfc3339());
        tx.execute(
            "UPDATE kennedy_audio_ingress_jobs
             SET phase=?1,failure_count=?2,failures_json=?3,next_attempt_at=?4,
                 updated_at=?5,version=version+1
             WHERE id=?6 AND phase IN ('ingress_pending','ingress_in_progress')
               AND version=?7",
            params![
                phase,
                attempt,
                serde_json::to_string(&failures).map_err(QueueError::internal)?,
                next,
                now.to_rfc3339(),
                id,
                input.expected_version
            ],
        )
        .map_err(QueueError::internal)?;
        tx.commit().map_err(QueueError::internal)?;
        let job = fetch_job(&db, id)?;
        if terminal {
            tracing::error!(
                job_id = id,
                recording_id = %existing.recording_id,
                attempt,
                stage = %input.stage,
                code = input.code.as_deref().unwrap_or("ingress_error"),
                terminal_reason = if input.code.as_deref() == Some("input_too_large") {
                    "non_retryable"
                } else {
                    "retry_limit"
                },
                "Audio ingress stopped after a terminal failure"
            );
        }
        Ok(job)
    }

    fn retry(
        &self,
        id: &str,
        expected_version: i64,
        replacement: Option<&Value>,
    ) -> Result<Job, QueueError> {
        validate_version(expected_version)?;
        let db = self.lock()?;
        let existing = fetch_job(&db, id)?;
        let state = replacement.unwrap_or(&existing.state);
        let state = serde_json::to_string(state).map_err(QueueError::internal)?;
        let changed = db
            .execute(
                "UPDATE kennedy_audio_ingress_jobs
                 SET phase='ingress_pending',state_json=?1,failure_count=0,
                     next_attempt_at=NULL,updated_at=?2,version=version+1
                 WHERE id=?3 AND phase='ingress_failed' AND version=?4",
                params![state, Utc::now().to_rfc3339(), id, expected_version],
            )
            .map_err(QueueError::internal)?;
        if changed == 0 {
            return Err(QueueError::conflict(
                "Audio ingress is not in the expected failed state.",
            ));
        }
        fetch_job(&db, id)
    }

    fn release_repairs(&self) -> Result<usize, QueueError> {
        let db = self.lock()?;
        db.execute(
            "UPDATE kennedy_audio_ingress_jobs
             SET phase='ingress_pending',
                 state_json=json_remove(
                     state_json,'$.historyIngress','$.historyIngressRepairReleasePending'
                 ),
                 next_attempt_at=NULL,failure_count=0,updated_at=?1,version=version+1
             WHERE phase='ingress_failed'
               AND json_extract(state_json,'$.historyIngressRepairRequired')=1
               AND json_extract(state_json,'$.historyIngressRepairReleasePending')=1",
            [Utc::now().to_rfc3339()],
        )
        .map_err(QueueError::internal)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, QueueError> {
        self.db.lock().map_err(QueueError::internal)
    }
}

fn job_select() -> &'static str {
    "SELECT id,recording_id,sha256,original_filename,source_created_at,piece_index,
            piece_count,transcript_text,estimated_tokens,phase,provenance_id,state_json,
            version,failure_count,failures_json,next_attempt_at,created_at,updated_at
     FROM kennedy_audio_ingress_jobs"
}

fn row_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let recording_id: String = row.get(1)?;
    let state: String = row.get(11)?;
    let failures: String = row.get(14)?;
    let _: Option<String> = row.get(15)?;
    Ok(Job {
        id: row.get(0)?,
        recording_id: Uuid::parse_str(&recording_id).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        sha256: row.get(2)?,
        original_filename: row.get(3)?,
        source_created_at: row.get(4)?,
        piece_index: row.get(5)?,
        piece_count: row.get(6)?,
        transcript_text: row.get(7)?,
        estimated_tokens: row.get(8)?,
        phase: row.get(9)?,
        provenance_id: row.get(10)?,
        state: serde_json::from_str(&state).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        version: row.get(12)?,
        ingress_failure_count: row.get(13)?,
        ingress_failures: serde_json::from_str(&failures).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                14,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        created_at: row.get(16)?,
        updated_at: row.get(17)?,
    })
}

fn optional_job(db: &Connection, id: &str) -> Result<Option<Job>, QueueError> {
    db.query_row(&format!("{} WHERE id=?1", job_select()), [id], row_job)
        .optional()
        .map_err(QueueError::internal)
}

fn fetch_job(db: &Connection, id: &str) -> Result<Job, QueueError> {
    optional_job(db, id)?.ok_or_else(QueueError::not_found)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueErrorKind {
    Invalid,
    NotFound,
    Conflict,
    Internal,
}

#[derive(Debug)]
struct QueueError {
    kind: QueueErrorKind,
    message: String,
}

impl QueueError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: QueueErrorKind::Invalid,
            message: message.into(),
        }
    }

    fn not_found() -> Self {
        Self {
            kind: QueueErrorKind::NotFound,
            message: "Audio-ingress job not found.".into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: QueueErrorKind::Conflict,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "Kennedy audio queue operation failed");
        Self {
            kind: QueueErrorKind::Internal,
            message: "An unexpected Kennedy audio queue error occurred.".into(),
        }
    }
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QueueError {}

impl From<QueueError> for ApiError {
    fn from(error: QueueError) -> Self {
        let (status, code) = match error.kind {
            QueueErrorKind::Invalid => (StatusCode::BAD_REQUEST, "invalid_request"),
            QueueErrorKind::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            QueueErrorKind::Conflict => (StatusCode::CONFLICT, "state_conflict"),
            QueueErrorKind::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };
        Self::new(status, code, error.message)
    }
}

fn library_error(error: kcode_audio_ingress::Error) -> ApiError {
    let (status, code) = match error.kind() {
        LibraryErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_request"),
        LibraryErrorKind::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        LibraryErrorKind::Conflict => (StatusCode::CONFLICT, "state_conflict"),
        LibraryErrorKind::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
    };
    ApiError::new(status, code, error.to_string())
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: Value) -> Result<T, ServiceError> {
    serde_json::from_value(body)
        .map_err(|error| ApiError::bad(format!("Invalid internal request: {error}")))
        .map_err(Into::into)
}

// Local orchestration passes HTTP-shaped paths directly, without Axum decoding route parameters.
fn decode_piece_id(encoded: &str) -> Result<String, ApiError> {
    let decoded = urlencoding::decode(encoded).map_err(|_| ApiError::not_found())?;
    if decoded.is_empty() || decoded.contains('/') {
        return Err(ApiError::not_found());
    }
    Ok(decoded.into_owned())
}

fn json_value(value: impl Serialize) -> Result<Value, ServiceError> {
    serde_json::to_value(value)
        .map_err(ApiError::internal)
        .map_err(Into::into)
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

fn validate_version(version: i64) -> Result<(), QueueError> {
    if version < 1 {
        Err(QueueError::invalid("expected_version must be positive."))
    } else {
        Ok(())
    }
}

fn has_successful_final_commit(state: &Value) -> bool {
    let current = state.get("historyIngress").is_some_and(|history| {
        history.get("completed").and_then(Value::as_bool) == Some(true)
            && history
                .get("sessionObjectId")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.trim().is_empty())
    });
    let legacy = state
        .pointer("/historyIngress/tools/log")
        .and_then(Value::as_array)
        .is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry.get("name").and_then(Value::as_str) == Some("EndSession")
                    && entry.get("ok").and_then(Value::as_bool) == Some(true)
            })
        });
    current || legacy
}

fn split_transcript(transcript: &str) -> anyhow::Result<Vec<String>> {
    let maximum_characters = usize::try_from(MAX_INGRESS_TOKENS * ESTIMATED_CHARACTERS_PER_TOKEN)?;
    let mut remaining = transcript.trim();
    ensure!(!remaining.is_empty(), "completed audio transcript is empty");
    let mut pieces = Vec::new();
    while remaining.chars().count() > maximum_characters {
        let cutoff = remaining
            .char_indices()
            .nth(maximum_characters)
            .map(|(index, _)| index)
            .unwrap_or(remaining.len());
        let prefix = &remaining[..cutoff];
        let minimum = prefix
            .char_indices()
            .nth(maximum_characters / 2)
            .map(|(index, _)| index)
            .unwrap_or(0);
        let boundary = prefix
            .rfind("\n\n")
            .filter(|index| *index >= minimum)
            .or_else(|| prefix.rfind('\n').filter(|index| *index >= minimum))
            .unwrap_or(cutoff);
        let piece = remaining[..boundary].trim();
        ensure!(!piece.is_empty(), "could not split audio transcript");
        pieces.push(piece.to_owned());
        remaining = remaining[boundary..].trim();
    }
    if !remaining.is_empty() {
        pieces.push(remaining.to_owned());
    }
    Ok(pieces)
}

fn estimate_tokens(value: &str) -> u64 {
    (value.chars().count() as u64).div_ceil(ESTIMATED_CHARACTERS_PER_TOKEN)
}

fn concise(value: &str, limit: usize, fallback: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let bounded = normalized.chars().take(limit).collect::<String>();
    if bounded.is_empty() {
        fallback.into()
    } else {
        bounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue() -> Queue {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(QUEUE_SCHEMA).unwrap();
        Queue {
            db: Arc::new(Mutex::new(connection)),
        }
    }

    #[test]
    fn transcript_queue_is_idempotent_and_ordered() {
        let queue = queue();
        let recording = RecordingStatus {
            id: Uuid::new_v4(),
            sha256: "a".repeat(64),
            original_filename: "note.wav".into(),
            size_bytes: 4,
            recorded_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            received_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            transcription_model: "gemini".into(),
            reconciliation_model: "codex".into(),
            reconciliation_reasoning: "xhigh".into(),
            state: RecordingState::Complete {
                transcript: "hello".into(),
            },
        };
        queue.ensure_recording(&recording, "hello").unwrap();
        queue.ensure_recording(&recording, "hello").unwrap();
        let jobs = queue.for_recording(recording.id).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].transcript_text, "hello");
        assert_eq!(queue.next().unwrap().unwrap().id, jobs[0].id);
    }

    #[test]
    fn oversized_transcripts_are_split_without_audio_library_metadata() {
        let transcript = format!("{}\n\n{}", "a".repeat(150_000), "b".repeat(100_000));
        let pieces = split_transcript(&transcript).unwrap();
        assert!(pieces.len() >= 2);
        assert!(
            pieces
                .iter()
                .all(|piece| estimate_tokens(piece) <= MAX_INGRESS_TOKENS)
        );
        assert!(!pieces.join("\n\n").contains("# Audio transcript"));
    }

    #[test]
    fn internal_audio_paths_decode_generated_piece_ids() {
        let id = format!("{}:0", Uuid::new_v4());
        let encoded = urlencoding::encode(&id);
        assert_eq!(decode_piece_id(&encoded).unwrap(), id);
        assert!(decode_piece_id("recording%2F0").is_err());
    }
}
