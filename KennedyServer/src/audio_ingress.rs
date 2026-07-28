use std::collections::HashSet;

use anyhow::ensure;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use kcode_audio_ingress::{
    AudioIngress, AudioInput, ErrorKind as LibraryErrorKind, RecordingState, RecordingStatus,
};
use kcode_session_history::{
    NewIngressSession, RetryIngress as HistoryRetryIngress, SessionHistory, SessionRecord,
    chatend::SessionKind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const MAX_INGRESS_TOKENS: u64 = 50_000;
const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 4;

#[derive(Clone)]
pub(crate) struct Service {
    audio: AudioIngress,
    history: SessionHistory,
    max_upload_bytes: usize,
    user_id: String,
    effective_context_tokens: u64,
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
        history: SessionHistory,
        max_upload_bytes: usize,
        user_id: String,
        effective_context_tokens: u64,
    ) -> anyhow::Result<Self> {
        ensure!(max_upload_bytes > 0, "audio upload limit must be positive");
        ensure!(
            !user_id.trim().is_empty(),
            "audio user ID must not be empty"
        );
        ensure!(
            effective_context_tokens > 0,
            "audio ingress context limit must be positive"
        );
        Ok(Self {
            audio,
            history,
            max_upload_bytes,
            user_id,
            effective_context_tokens,
        })
    }

    pub(crate) async fn synchronize_completed_transcripts(&self) -> Result<(), ServiceError> {
        self.synchronize().await.map_err(Into::into)
    }

    async fn synchronize(&self) -> Result<(), ApiError> {
        let histories = self.history.list().await.map_err(history_error)?;
        let mut existing = histories
            .iter()
            .filter_map(ingress_source_id)
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        for recording in self.audio.status().map_err(library_error)?.recordings {
            let RecordingState::Complete { transcript } = &recording.state else {
                continue;
            };
            let pieces = split_transcript(transcript).map_err(ApiError::internal)?;
            let piece_count = u32::try_from(pieces.len()).map_err(ApiError::internal)?;
            for (index, piece) in pieces.into_iter().enumerate() {
                let piece_index = u32::try_from(index).map_err(ApiError::internal)?;
                let idempotency_id = audio_piece_id(recording.id, piece_index);
                if existing.contains(&idempotency_id) {
                    continue;
                }
                self.history
                    .enqueue_ingress(NewIngressSession {
                        idempotency_id: idempotency_id.clone(),
                        started_at: recording.recorded_at.to_rfc3339(),
                        source_session_type: "audio".into(),
                        kind: SessionKind::AudioIngress,
                        effective_context_tokens: self.effective_context_tokens,
                        text: format_ingress_piece(&recording, piece_index, piece_count, &piece),
                        metadata: audio_piece_metadata(&recording, piece_index, piece_count),
                    })
                    .await
                    .map_err(history_error)?;
                existing.insert(idempotency_id);
            }
        }
        Ok(())
    }

    async fn browser_snapshot(&self) -> Result<Vec<BrowserRecording>, ApiError> {
        let histories = self.history.list().await.map_err(history_error)?;
        let mut recordings = Vec::new();
        for recording in self.audio.status().map_err(library_error)?.recordings {
            let pieces = ingress_pieces(&recording, &histories)?;
            recordings.push(BrowserRecording::from_status(recording, &pieces));
        }
        Ok(recordings)
    }

    async fn recording_pieces(
        &self,
        recording: &RecordingStatus,
    ) -> Result<Vec<IngressPiece>, ApiError> {
        let histories = self.history.list().await.map_err(history_error)?;
        ingress_pieces(recording, &histories)
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
            user_id: service.user_id.clone(),
            bytes,
            recorded_at,
            original_filename,
        })
        .await
        .map_err(library_error)?;
    let recording = service
        .browser_snapshot()
        .await?
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
        .browser_snapshot()
        .await?
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
        .browser_snapshot()
        .await?
        .into_iter()
        .find(|recording| recording.sha256 == sha256.to_ascii_lowercase())
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn recording_history(
    State(service): State<Service>,
    AxumPath(recording_id): AxumPath<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let status = service.audio.status().map_err(library_error)?;
    let recording_status = status
        .recordings
        .into_iter()
        .find(|recording| recording.id == recording_id)
        .ok_or_else(ApiError::not_found)?;
    let pieces = service.recording_pieces(&recording_status).await?;
    let recording = BrowserRecording::from_status(recording_status.clone(), &pieces);
    let transcript = match recording_status.state {
        RecordingState::Complete { transcript } => Some(transcript),
        _ => None,
    };
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

#[derive(Deserialize)]
struct RetryIngress {
    expected_version: i64,
    #[serde(default)]
    state: Option<Value>,
}

async fn retry_ingress(
    State(service): State<Service>,
    AxumPath(piece_id): AxumPath<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<SessionRecord>, ApiError> {
    let current = service
        .history
        .get(&piece_id)
        .await
        .map_err(history_error)?;
    let record = service
        .history
        .retry_ingress(
            &piece_id,
            HistoryRetryIngress {
                expected_version: input.expected_version,
                state: input.state.unwrap_or(current.state),
            },
        )
        .await
        .map_err(history_error)?;
    Ok(Json(record))
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
    fn from_status(recording: RecordingStatus, pieces: &[IngressPiece]) -> Self {
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
        if !pieces.is_empty() {
            status = if pieces.iter().all(|piece| piece.phase == "complete") {
                "complete".into()
            } else if pieces.iter().any(|piece| piece.phase == "ingress_failed") {
                "ingress_failed".into()
            } else if pieces
                .iter()
                .any(|piece| piece.phase == "ingress_in_progress")
            {
                "ingressing".into()
            } else {
                "ready_for_ingress".into()
            };
        }
        let completed_piece_count = pieces
            .iter()
            .filter(|piece| piece.phase == "complete")
            .count();
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
            transcript_piece_count: pieces.len(),
            completed_piece_count,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct IngressPiece {
    id: String,
    recording_id: Uuid,
    sha256: String,
    original_filename: String,
    source_created_at: String,
    piece_index: u32,
    piece_count: u32,
    transcript_text: String,
    estimated_tokens: u64,
    phase: String,
    provenance_id: Option<String>,
    state: Value,
    version: i64,
    ingress_failure_count: i64,
    ingress_failures: Value,
    created_at: String,
    updated_at: String,
}

impl IngressPiece {
    fn from_record(
        recording: &RecordingStatus,
        piece_index: u32,
        piece_count: u32,
        transcript_text: String,
        record: &SessionRecord,
    ) -> Self {
        Self {
            id: record.id.clone(),
            recording_id: recording.id,
            sha256: recording.sha256.clone(),
            original_filename: recording.original_filename.clone(),
            source_created_at: recording.recorded_at.to_rfc3339(),
            piece_index,
            piece_count,
            estimated_tokens: estimate_tokens(&transcript_text),
            transcript_text,
            phase: record.phase.clone(),
            provenance_id: record.provenance_id.clone(),
            state: record.state.clone(),
            version: record.version,
            ingress_failure_count: record.ingress_failure_count,
            ingress_failures: record.ingress_failures.clone(),
            created_at: record.started_at.clone(),
            updated_at: record.updated_at.clone(),
        }
    }
}

fn ingress_pieces(
    recording: &RecordingStatus,
    histories: &[SessionRecord],
) -> Result<Vec<IngressPiece>, ApiError> {
    let RecordingState::Complete { transcript } = &recording.state else {
        return Ok(Vec::new());
    };
    let transcript_pieces = split_transcript(transcript).map_err(ApiError::internal)?;
    let piece_count = u32::try_from(transcript_pieces.len()).map_err(ApiError::internal)?;
    let mut pieces = Vec::with_capacity(transcript_pieces.len());
    for (index, transcript_text) in transcript_pieces.into_iter().enumerate() {
        let piece_index = u32::try_from(index).map_err(ApiError::internal)?;
        let idempotency_id = audio_piece_id(recording.id, piece_index);
        let Some(record) = histories
            .iter()
            .find(|record| ingress_source_id(record) == Some(idempotency_id.as_str()))
        else {
            continue;
        };
        pieces.push(IngressPiece::from_record(
            recording,
            piece_index,
            piece_count,
            transcript_text,
            record,
        ));
    }
    Ok(pieces)
}

fn ingress_source_id(record: &SessionRecord) -> Option<&str> {
    record
        .state
        .pointer("/ingressSource/idempotencyId")
        .and_then(Value::as_str)
}

fn audio_piece_id(recording_id: Uuid, piece_index: u32) -> String {
    format!("audio:{recording_id}:{piece_index}")
}

fn audio_piece_metadata(recording: &RecordingStatus, piece_index: u32, piece_count: u32) -> Value {
    json!({
        "kind":"audio-transcript",
        "recordingId":recording.id,
        "sha256":recording.sha256,
        "originalFilename":recording.original_filename,
        "extension":file_name_extension(&recording.original_filename),
        "mimeType":"audio/wav",
        "sizeBytes":recording.size_bytes,
        "sourceCreatedAt":recording.recorded_at.to_rfc3339(),
        "pieceIndex":piece_index,
        "pieceCount":piece_count,
    })
}

fn format_ingress_piece(
    recording: &RecordingStatus,
    piece_index: u32,
    piece_count: u32,
    transcript: &str,
) -> String {
    format!(
        "Vnote final transcript piece\n\nRecording began: {}\nRecording SHA-256: {}\nOriginal filename: {}\nExtension: {}\nMIME type: audio/wav\nSize: {} bytes\nTranscript piece: {} of {}\n\n{}",
        recording.recorded_at.to_rfc3339(),
        recording.sha256,
        recording.original_filename,
        file_name_extension(&recording.original_filename),
        recording.size_bytes,
        piece_index + 1,
        piece_count,
        transcript,
    )
}

fn file_name_extension(file_name: &str) -> String {
    file_name
        .rsplit_once('.')
        .and_then(|(stem, extension)| {
            (!stem.is_empty() && !extension.is_empty()).then_some(extension)
        })
        .map(|extension| format!(".{extension}"))
        .unwrap_or_else(|| "(none)".into())
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

fn library_error(error: kcode_audio_ingress::Error) -> ApiError {
    let (status, code) = match error.kind() {
        LibraryErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_request"),
        LibraryErrorKind::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        LibraryErrorKind::Conflict => (StatusCode::CONFLICT, "state_conflict"),
        LibraryErrorKind::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
    };
    ApiError::new(status, code, error.to_string())
}

fn history_error(error: kcode_session_history::Error) -> ApiError {
    let status = match error.kind {
        kcode_session_history::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        kcode_session_history::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        kcode_session_history::ErrorKind::Conflict => StatusCode::CONFLICT,
        kcode_session_history::ErrorKind::Storage => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiError::new(status, error.kind.code(), error.message)
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn audio_piece_ids_are_stable() {
        let recording_id = Uuid::new_v4();
        assert_eq!(
            audio_piece_id(recording_id, 2),
            format!("audio:{recording_id}:2")
        );
    }

    #[test]
    fn audio_ingress_exposes_the_complete_file_metadata_contract() {
        let now = Utc::now();
        let recording = RecordingStatus {
            id: Uuid::new_v4(),
            user_id: "user".into(),
            sha256: "0".repeat(64),
            original_filename: "meeting.final.WAV".into(),
            size_bytes: 42,
            recorded_at: now,
            received_at: now,
            transcription_model: "transcription-model".into(),
            reconciliation_model: "reconciliation-model".into(),
            reconciliation_reasoning: "xhigh".into(),
            state: RecordingState::Complete {
                transcript: "Transcript".into(),
            },
        };

        let metadata = audio_piece_metadata(&recording, 0, 1);
        assert_eq!(metadata["originalFilename"], "meeting.final.WAV");
        assert_eq!(metadata["extension"], ".WAV");
        assert_eq!(metadata["mimeType"], "audio/wav");
        assert_eq!(metadata["sizeBytes"], 42);
        let text = format_ingress_piece(&recording, 0, 1, "Transcript");
        assert!(text.contains("Original filename: meeting.final.WAV"));
        assert!(text.contains("Extension: .WAV"));
        assert!(text.contains("MIME type: audio/wav"));
        assert!(text.contains("Size: 42 bytes"));
    }

    #[test]
    fn browser_reads_allow_a_completed_transcript_before_worker_synchronization() {
        let now = Utc::now();
        let recording = RecordingStatus {
            id: Uuid::new_v4(),
            user_id: "user".into(),
            sha256: "0".repeat(64),
            original_filename: "recording.wav".into(),
            size_bytes: 1,
            recorded_at: now,
            received_at: now,
            transcription_model: "transcription-model".into(),
            reconciliation_model: "reconciliation-model".into(),
            reconciliation_reasoning: "xhigh".into(),
            state: RecordingState::Complete {
                transcript: "New transcript".into(),
            },
        };

        assert!(ingress_pieces(&recording, &[]).unwrap().is_empty());
    }
}
