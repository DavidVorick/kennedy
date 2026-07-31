//! Browser-facing HTTP adapter for the audio/session-ingress coordinator.

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
    ConfirmationState, CorrectionPacket, ObservationConfirmation, RecordingConfirmation,
};
use kcode_audio_session_ingress::{
    Coordinator, ErrorKind, IngressPiece as CoordinatorIngressPiece,
    Recording as CoordinatorRecording, RecordingInput,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Service {
    coordinator: Coordinator,
    max_upload_bytes: usize,
}

impl Service {
    pub(crate) fn open(coordinator: Coordinator, max_upload_bytes: usize) -> anyhow::Result<Self> {
        ensure!(max_upload_bytes > 0, "audio upload limit must be positive");
        Ok(Self {
            coordinator,
            max_upload_bytes,
        })
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
            "/api/v1/audio-ingress/{recording_id}/confirm-speakers",
            post(confirm_speakers),
        )
        .route(
            "/api/v1/audio-ingress/pieces/{piece_id}/retry-ingress",
            post(retry_ingress),
        )
        .layer(DefaultBodyLimit::max(service.max_upload_bytes))
        .with_state(service)
}

async fn health(State(service): State<Service>) -> Result<Json<Value>, ApiError> {
    service.coordinator.health().map_err(coordinator_error)?;
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
        .coordinator
        .submit(RecordingInput {
            bytes,
            recorded_at,
            original_filename,
        })
        .await
        .map_err(coordinator_error)?;
    Ok((
        if submission.deduplicated {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        Json(json!({
            "recording":BrowserRecording::from(submission.recording),
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
        .coordinator
        .recordings()
        .await
        .map_err(coordinator_error)?
        .into_iter()
        .take(limit)
        .map(BrowserRecording::from)
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
        .coordinator
        .recording_by_sha256(&sha256)
        .await
        .map(BrowserRecording::from)
        .map(Json)
        .map_err(coordinator_error)
}

async fn recording_history(
    State(service): State<Service>,
    AxumPath(recording_id): AxumPath<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let history = service
        .coordinator
        .recording_history(recording_id)
        .await
        .map_err(coordinator_error)?;
    let chunks = history
        .correction_packet
        .as_ref()
        .map(|packet| packet.chunks.clone())
        .unwrap_or_default();
    Ok(Json(json!({
        "recording":BrowserRecording::from(history.recording),
        "final_transcript":history.final_transcript,
        "correction_packet":history.correction_packet,
        "chunks":chunks,
        "pieces":history.pieces.into_iter().map(IngressPiece::from).collect::<Vec<_>>(),
    })))
}

async fn retry_recording(
    State(service): State<Service>,
    AxumPath(recording_id): AxumPath<Uuid>,
) -> Result<Json<Value>, ApiError> {
    service
        .coordinator
        .retry_recording(recording_id)
        .map_err(coordinator_error)?;
    Ok(Json(json!({"recording_id":recording_id,"queued":true})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmSpeakers {
    observations: Vec<ObservationConfirmation>,
}

async fn confirm_speakers(
    State(service): State<Service>,
    AxumPath(recording_id): AxumPath<Uuid>,
    Json(input): Json<ConfirmSpeakers>,
) -> Result<Json<CorrectionPacket>, ApiError> {
    service
        .coordinator
        .confirm_speakers(RecordingConfirmation {
            recording_id,
            observations: input.observations,
        })
        .await
        .map(Json)
        .map_err(coordinator_error)
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
) -> Result<Json<kcode_session_history::SessionRecord>, ApiError> {
    service
        .coordinator
        .retry_ingress(kcode_audio_session_ingress::RetryIngress {
            piece_id,
            expected_version: input.expected_version,
            state: input.state,
        })
        .await
        .map(Json)
        .map_err(coordinator_error)
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
    speaker_confirmation_state: Option<ConfirmationState>,
    speaker_labels_clean: Option<bool>,
    speaker_observation_count: Option<usize>,
    transcript_piece_count: usize,
    completed_piece_count: usize,
}

impl From<CoordinatorRecording> for BrowserRecording {
    fn from(recording: CoordinatorRecording) -> Self {
        let (speaker_confirmation_state, speaker_labels_clean, speaker_observation_count) =
            recording
                .speaker_review
                .map(|review| {
                    (
                        Some(review.confirmation_state),
                        Some(review.clean),
                        Some(review.observation_count),
                    )
                })
                .unwrap_or((None, None, None));
        Self {
            id: recording.id,
            sha256: recording.sha256,
            original_filename: recording.original_filename,
            content_type: recording.content_type,
            size_bytes: recording.size_bytes,
            source_created_at: recording.source_created_at,
            received_at: recording.received_at,
            updated_at: recording.updated_at,
            status: recording.status,
            gemini_model: recording.transcription_model,
            reconciliation_model: recording.reconciliation_model,
            reconciliation_reasoning: recording.reconciliation_reasoning,
            transcription_status: recording.transcription_status,
            attempt_count: recording.attempt_count,
            next_attempt_at: recording.next_attempt_at,
            last_error: recording.last_error,
            speaker_confirmation_state,
            speaker_labels_clean,
            speaker_observation_count,
            transcript_piece_count: recording.transcript_piece_count,
            completed_piece_count: recording.completed_piece_count,
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

impl From<CoordinatorIngressPiece> for IngressPiece {
    fn from(piece: CoordinatorIngressPiece) -> Self {
        Self {
            id: piece.id,
            recording_id: piece.recording_id,
            sha256: piece.sha256,
            original_filename: piece.original_filename,
            source_created_at: piece.source_created_at,
            piece_index: piece.piece_index,
            piece_count: piece.piece_count,
            transcript_text: piece.transcript_text,
            estimated_tokens: piece.estimated_tokens,
            phase: piece.phase,
            provenance_id: piece.provenance_id,
            state: piece.state,
            version: piece.version,
            ingress_failure_count: piece.ingress_failure_count,
            ingress_failures: piece.ingress_failures,
            created_at: piece.created_at,
            updated_at: piece.updated_at,
        }
    }
}

fn coordinator_error(error: kcode_audio_session_ingress::Error) -> ApiError {
    let (status, code, message) = match error.kind() {
        ErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_request", error.message()),
        ErrorKind::NotFound => (StatusCode::NOT_FOUND, "not_found", error.message()),
        ErrorKind::Conflict => (StatusCode::CONFLICT, "state_conflict", error.message()),
        ErrorKind::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected Kennedy audio error occurred.",
        ),
    };
    ApiError::new(status, code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_dto_preserves_the_pre_extraction_wire_contract() {
        let id = Uuid::nil();
        let recording = BrowserRecording::from(CoordinatorRecording {
            id,
            sha256: "0".repeat(64),
            original_filename: "voice.wav".into(),
            content_type: "audio/wav",
            size_bytes: 42,
            source_created_at: "2026-07-31T00:00:00+00:00".into(),
            received_at: "2026-07-31T00:01:00+00:00".into(),
            updated_at: "2026-07-31T00:01:00+00:00".into(),
            status: "failed".into(),
            transcription_model: "transcription-model".into(),
            reconciliation_model: "reconciliation-model".into(),
            reconciliation_reasoning: "xhigh".into(),
            transcription_status: None,
            attempt_count: 5,
            next_attempt_at: None,
            last_error: Some("failed".into()),
            speaker_review: None,
            transcript_piece_count: 0,
            completed_piece_count: 0,
        });

        assert_eq!(
            serde_json::to_value(recording).unwrap(),
            json!({
                "id":id,
                "sha256":"0".repeat(64),
                "original_filename":"voice.wav",
                "content_type":"audio/wav",
                "size_bytes":42,
                "source_created_at":"2026-07-31T00:00:00+00:00",
                "received_at":"2026-07-31T00:01:00+00:00",
                "updated_at":"2026-07-31T00:01:00+00:00",
                "status":"failed",
                "gemini_model":"transcription-model",
                "reconciliation_model":"reconciliation-model",
                "reconciliation_reasoning":"xhigh",
                "transcription_status":null,
                "attempt_count":5,
                "next_attempt_at":null,
                "last_error":"failed",
                "speaker_confirmation_state":null,
                "speaker_labels_clean":null,
                "speaker_observation_count":null,
                "transcript_piece_count":0,
                "completed_piece_count":0,
            })
        );
    }

    #[test]
    fn retry_dto_retains_optional_state_and_ignores_unknown_fields() {
        let retry: RetryIngress = serde_json::from_value(json!({
            "expected_version":7,
            "state":{"kept":true},
            "future_field":"ignored",
        }))
        .unwrap();

        assert_eq!(retry.expected_version, 7);
        assert_eq!(retry.state, Some(json!({"kept":true})));
    }

    #[test]
    fn speaker_confirmation_body_requires_only_observation_assignments() {
        let input: ConfirmSpeakers = serde_json::from_value(json!({
            "observations":[{
                "observation_key":{"object_id":"recording:chunk:0","piece_index":1},
                "confirmed_full_name":"Human Choice",
            }],
        }))
        .unwrap();

        assert_eq!(input.observations.len(), 1);
        assert_eq!(input.observations[0].confirmed_full_name, "Human Choice");
        assert!(
            serde_json::from_value::<ConfirmSpeakers>(json!({
                "observations":[],
                "recording_id":Uuid::nil(),
            }))
            .is_err()
        );
    }
}
