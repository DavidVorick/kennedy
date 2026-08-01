//! Kennedy's browser-facing HTTP adapter for `kcode-session-history`.
//!
//! The library owns session-history behavior and persistence. Route names,
//! multipart parsing, headers, and HTTP error responses remain KennedyServer
//! concerns.

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{
        HeaderName, HeaderValue, StatusCode,
        header::{self},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use kcode_session_history::{
    NewCommand, NewCurrentWorkStop, NewObject, RetryIngress, SessionHistory, StartSession,
};
use serde_json::{Value, json};

const MAX_OBJECT_BYTES: usize = 32 * 1024 * 1024 * 1024;

pub(crate) fn router(history: SessionHistory) -> Router {
    Router::new()
        .route("/api/v1/conversations/health", get(health))
        .route("/api/v1/conversations/summaries", get(list))
        .route("/api/v1/conversations/start", post(start))
        .route("/api/v1/conversation-commands", get(command_heads))
        .route("/api/v1/conversations/{session_id}", get(session))
        .route("/api/v1/conversations/{session_id}/commands", post(enqueue))
        .route(
            "/api/v1/conversations/{session_id}/objects",
            post(stage_object),
        )
        .route(
            "/api/v1/conversations/{session_id}/objects/{pending_id}",
            get(object),
        )
        .route("/api/v1/conversations/{session_id}/stop", post(stop))
        .route(
            "/api/v1/conversations/{session_id}/retry-ingress",
            post(retry_ingress),
        )
        .layer(DefaultBodyLimit::max(MAX_OBJECT_BYTES))
        .with_state(history)
}

async fn health(State(history): State<SessionHistory>) -> Result<Json<Value>, ApiError> {
    history.health()?;
    Ok(Json(json!({"service":"session-history","status":"ok"})))
}

async fn list(State(history): State<SessionHistory>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({"conversations":history.list().await?})))
}

async fn start(
    State(history): State<SessionHistory>,
    Json(input): Json<StartSession>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let created = history.start(input).await?;
    Ok((
        if created.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(serde_json::to_value(created.value).map_err(ApiError::internal)?),
    ))
}

async fn command_heads(State(history): State<SessionHistory>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({"commands":history.command_heads().await?})))
}

async fn session(
    State(history): State<SessionHistory>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(history.get(&id).await?).map_err(ApiError::internal)?,
    ))
}

async fn enqueue(
    State(history): State<SessionHistory>,
    Path(id): Path<String>,
    Json(input): Json<NewCommand>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let created = history.enqueue(&id, input).await?;
    Ok((
        if created.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(serde_json::to_value(created.value).map_err(ApiError::internal)?),
    ))
}

async fn stage_object(
    State(history): State<SessionHistory>,
    Path(id): Path<String>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut object = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad(error.to_string()))?
    {
        if field.name() != Some("file") {
            continue;
        }
        let file_name = field.file_name().map(str::to_owned);
        let media_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_owned();
        let bytes = field
            .bytes()
            .await
            .map_err(|error| ApiError::bad(error.to_string()))?
            .to_vec();
        if object
            .replace(NewObject {
                file_name,
                media_type,
                bytes,
            })
            .is_some()
        {
            return Err(ApiError::bad("Upload exactly one object per request."));
        }
    }
    let object = object.ok_or_else(|| ApiError::bad("Multipart upload omitted the file field."))?;
    let bytes = object.bytes.len();
    let pending_id = history.stage_object(&id, object).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"pendingId":pending_id,"bytes":bytes})),
    ))
}

async fn object(
    State(history): State<SessionHistory>,
    Path((id, pending_id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let object = history.object(&id, &pending_id)?;
    let content_type = HeaderValue::from_str(&object.media_type).map_err(ApiError::internal)?;
    let disposition = HeaderValue::from_str(&format!("inline; filename=\"{}\"", object.file_name))
        .map_err(ApiError::internal)?;
    let content_length =
        HeaderValue::from_str(&object.bytes.len().to_string()).map_err(ApiError::internal)?;
    let mut response = Response::new(Body::from(object.bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    response
        .headers_mut()
        .insert(header::CONTENT_DISPOSITION, disposition);
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, content_length);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store, max-age=0"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

async fn stop(
    State(history): State<SessionHistory>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let request = history
        .request_current_work_stop(
            &id,
            NewCurrentWorkStop {
                idempotency_id: uuid::Uuid::new_v4().to_string(),
            },
        )
        .await?
        .value;
    Ok(Json(json!({
        "id":id,
        "scope":request.scope,
        "status":"stopping",
        "stopRequested":true,
        "stopRequestId":request.id,
    })))
}

async fn retry_ingress(
    State(history): State<SessionHistory>,
    Path(id): Path<String>,
    Json(input): Json<RetryIngress>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(history.retry_ingress(&id, input).await?)
            .map_err(ApiError::internal)?,
    ))
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn bad(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "Kennedy Session History HTTP adapter failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: "An unexpected Session History adapter error occurred.".into(),
        }
    }
}

impl From<kcode_session_history::Error> for ApiError {
    fn from(error: kcode_session_history::Error) -> Self {
        Self {
            status: history_status(error.kind),
            code: error.kind.code(),
            message: error.message,
        }
    }
}

fn history_status(kind: kcode_session_history::ErrorKind) -> StatusCode {
    match kind {
        kcode_session_history::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        kcode_session_history::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        kcode_session_history::ErrorKind::Conflict => StatusCode::CONFLICT,
        kcode_session_history::ErrorKind::Storage => StatusCode::INTERNAL_SERVER_ERROR,
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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[tokio::test]
    async fn stop_response_preserves_the_browser_contract_without_orchestration_state() {
        let root = std::env::temp_dir().join(format!(
            "kennedy-session-history-http-stop-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let history = SessionHistory::open(kcode_session_history::Config {
            directory: root.join("sessions"),
            completed_list: root.join("session-history.txt"),
            provider_cost_compatibility: None,
        })
        .unwrap();
        let record = history
            .start(StartSession {
                idempotency_id: "start".into(),
                started_at: "2026-08-01T00:00:00Z".into(),
                session_type: "conversation".into(),
                duration_minutes: None,
                custom_prompt: None,
            })
            .await
            .unwrap()
            .value;

        let Json(response) = stop(State(history.clone()), Path(record.id.clone()))
            .await
            .unwrap();
        assert_eq!(response["id"], record.id);
        assert_eq!(response["scope"], "turn");
        assert_eq!(response["status"], "stopping");
        assert_eq!(response["stopRequested"], true);
        assert_eq!(
            response["stopRequestId"],
            history.stop_heads().await.unwrap()[0].id
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
