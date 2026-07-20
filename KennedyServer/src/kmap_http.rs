use std::{
    collections::HashSet,
    path::{Path as FilePath, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use kweb_db_core::{
    CreateNode, Error as KmapError, IdempotencyId, Kmap, NewProvenance, NewProvenanceArtifact,
    NodeId, Owner, ProvenanceStorage, UpdateNode,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower_http::{services::ServeDir, trace::TraceLayer};

const MAX_REQUEST_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SystemRoots {
    pub user: NodeId,
    pub kennedy: NodeId,
}

#[derive(Clone)]
struct AppState {
    kmap: Arc<Mutex<Kmap>>,
    roots: SystemRoots,
    prompts_dir: PathBuf,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error=%error, "Kmap request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: "An unexpected Kmap database error occurred.".into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: message.into(),
        }
    }
}

impl From<KmapError> for ApiError {
    fn from(error: KmapError) -> Self {
        match error {
            KmapError::InvalidInput(message) => Self {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_request",
                message,
            },
            KmapError::NotFound(kind) => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: format!("{kind} not found."),
            },
            KmapError::Conflict(message) => Self {
                status: StatusCode::CONFLICT,
                code: "conflict",
                message,
            },
            KmapError::Database(error) => Self::internal(error),
            KmapError::Io(error) => Self::internal(error),
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

#[derive(Deserialize)]
struct CreateNodeRequest {
    idempotency_id: IdempotencyId,
    #[serde(default)]
    node_id: Option<NodeId>,
    provenance_id: NodeId,
    owner_node_id: String,
    model_attribution: String,
    short_name: String,
    short_description: String,
    long_description: String,
    #[serde(default)]
    fixed_connections: Vec<NodeId>,
    #[serde(default)]
    recent_connections: Vec<NodeId>,
}

#[derive(Deserialize)]
struct UpdateNodeRequest {
    idempotency_id: IdempotencyId,
    provenance_id: NodeId,
    owner_node_id: String,
    model_attribution: String,
    short_name: String,
    short_description: String,
    long_description: String,
    fixed_connections: Vec<NodeId>,
    recent_connections: Vec<NodeId>,
}

#[derive(Deserialize)]
struct CreateProvenanceRequest {
    idempotency_id: IdempotencyId,
    data: String,
    source: String,
    source_created_at: String,
}

pub(crate) fn initialize(
    kmap_database: &FilePath,
    artifact_directory: &FilePath,
    identity_database: &FilePath,
) -> anyhow::Result<(Kmap, SystemRoots)> {
    let identity = Connection::open(identity_database).with_context(|| {
        format!(
            "opening identity database {} for system roots",
            identity_database.display()
        )
    })?;
    identity.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
         CREATE TABLE IF NOT EXISTS kmap_system_roots (
             role TEXT PRIMARY KEY CHECK(role IN ('user','kennedy')),
             root_node_id TEXT NOT NULL UNIQUE CHECK(length(root_node_id)=40),
             created_at TEXT NOT NULL
         );",
    )?;
    let mut kmap =
        Kmap::open_with_artifacts(kmap_database, artifact_directory).map_err(anyhow::Error::new)?;
    let mut bootstrap_provenance = None;
    let roots = {
        let mut ensure = |role: &str| -> anyhow::Result<NodeId> {
            let existing = identity
                .query_row(
                    "SELECT root_node_id FROM kmap_system_roots WHERE role=?1",
                    [role],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let id = if let Some(existing) = existing {
                NodeId::from_hex(&existing).map_err(anyhow::Error::new)?
            } else {
                let provenance_id = match bootstrap_provenance {
                    Some(id) => id,
                    None => {
                        let id = kmap
                            .create_provenance(
                                IdempotencyId::random(),
                                NewProvenance {
                                    data: "Initial Kmap system-root bootstrap.".into(),
                                    source: "system-bootstrap".into(),
                                    source_created_at: Utc::now().to_rfc3339(),
                                },
                            )
                            .map_err(anyhow::Error::new)?;
                        bootstrap_provenance = Some(id);
                        id
                    }
                };
                let id = NodeId::random();
                let (short_name, short_description, long_description) = if role == "kennedy" {
                    (
                        "Kennedy's Root",
                        "The root of Kennedy's own Kmap knowledge.",
                        "This is Kennedy's root node. It anchors Kennedy's own durable knowledge and learned lessons in the Kmap.",
                    )
                } else {
                    ("Initial User Root", "", "")
                };
                kmap.create_node(
                    IdempotencyId::random(),
                    CreateNode {
                        id,
                        provenance_id,
                        owner: Owner::SelfNode,
                        short_name: short_name.into(),
                        short_description: short_description.into(),
                        long_description: long_description.into(),
                        model_attribution: "system-bootstrap".into(),
                        fixed_connections: vec![],
                        recent_connections: vec![],
                    },
                )
                .map_err(anyhow::Error::new)?;
                identity.execute(
                    "INSERT INTO kmap_system_roots(role,root_node_id,created_at) VALUES(?1,?2,?3)",
                    params![role, id.to_string(), Utc::now().to_rfc3339()],
                )?;
                id
            };
            kmap.get_node(id).map_err(anyhow::Error::new)?;
            Ok(id)
        };
        SystemRoots {
            user: ensure("user")?,
            kennedy: ensure("kennedy")?,
        }
    };
    Ok((kmap, roots))
}

pub(crate) struct MergedRouters {
    telegram_directory: Router,
    intelligence: Router,
    conversation_history: Router,
    audio_ingress: Router,
}

impl MergedRouters {
    pub(crate) fn new(
        telegram_directory: Router,
        intelligence: Router,
        conversation_history: Router,
        audio_ingress: Router,
    ) -> Self {
        Self {
            telegram_directory,
            intelligence,
            conversation_history,
            audio_ingress,
        }
    }
}

pub(crate) async fn serve_with_listener(
    kmap: Kmap,
    roots: SystemRoots,
    frontend_dir: PathBuf,
    system_prompts_dir: PathBuf,
    merged_routers: MergedRouters,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    let artifact_directory = kmap.artifact_path().to_owned();
    let state = AppState {
        kmap: Arc::new(Mutex::new(kmap)),
        roots,
        prompts_dir: system_prompts_dir,
    };
    let app = Router::new()
        .route("/api/v1/kmap/health", get(health))
        .route("/api/v1/kmap/roots", get(get_roots))
        .route("/api/v1/kmap/stats", get(get_stats))
        .route(
            "/api/v1/kmap/nodes/{node_id}",
            get(get_node).put(update_node),
        )
        .route("/api/v1/kmap/nodes/{node_id}/history", get(get_history))
        .route("/api/v1/kmap/nodes", post(create_node))
        .route("/api/v1/kmap/provenance", post(create_provenance))
        .route(
            "/api/v1/kmap/provenance-with-artifacts",
            post(create_provenance_with_artifacts),
        )
        .route(
            "/api/v1/kmap/provenance/{provenance_id}",
            get(get_provenance),
        )
        .nest_service(
            "/api/v1/kmap/provenance-artifacts",
            ServeDir::new(artifact_directory),
        )
        .route("/system-prompts/{filename}", get(get_prompt))
        .with_state(state)
        .merge(merged_routers.telegram_directory)
        .merge(merged_routers.intelligence)
        .merge(merged_routers.conversation_history)
        .merge(merged_routers.audio_ingress)
        .fallback_service(ServeDir::new(frontend_dir).append_index_html_on_directories(true))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(TraceLayer::new_for_http())
        .layer(middleware::map_response(prevent_stale_frontend_assets));
    tracing::info!(address=%listener.local_addr()?, "Kennedy main HTTP server ready");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn prevent_stale_frontend_assets(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    state
        .kmap
        .lock()
        .map_err(ApiError::internal)?
        .stats()
        .map_err(ApiError::from)?;
    Ok(Json(json!({"service":"kmap","status":"ok"})))
}

async fn get_roots(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "user_root_node_id":state.roots.user,
        "kennedy_root_node_id":state.roots.kennedy,
    }))
}

async fn get_stats(State(state): State<AppState>) -> Result<Json<kweb_db_core::Stats>, ApiError> {
    let stats = state
        .kmap
        .lock()
        .map_err(ApiError::internal)?
        .stats()
        .map_err(ApiError::from)?;
    Ok(Json(stats))
}

async fn create_provenance(
    State(state): State<AppState>,
    Json(input): Json<CreateProvenanceRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let id = state
        .kmap
        .lock()
        .map_err(ApiError::internal)?
        .create_provenance(
            input.idempotency_id,
            NewProvenance {
                data: input.data,
                source: input.source,
                source_created_at: input.source_created_at,
            },
        )
        .map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(json!({"id":id}))))
}

async fn create_provenance_with_artifacts(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut idempotency_id = None;
    let mut data = None;
    let mut source = None;
    let mut source_created_at = None;
    let mut data_filename = None;
    let mut artifacts = Vec::new();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::invalid(error.to_string()))?
    {
        let name = field
            .name()
            .ok_or_else(|| ApiError::invalid("Multipart fields must have names."))?
            .to_owned();
        match name.as_str() {
            "artifact" => {
                let original_filename = field
                    .file_name()
                    .ok_or_else(|| ApiError::invalid("Artifact parts must have filenames."))?
                    .to_owned();
                let media_type = field
                    .content_type()
                    .unwrap_or("application/octet-stream")
                    .to_owned();
                let data = field
                    .bytes()
                    .await
                    .map_err(|error| ApiError::invalid(error.to_string()))?
                    .to_vec();
                artifacts.push(NewProvenanceArtifact {
                    original_filename,
                    media_type,
                    role: "media".into(),
                    data,
                });
            }
            "idempotency_id" => {
                set_once(
                    &mut idempotency_id,
                    IdempotencyId::from_hex(
                        &field
                            .text()
                            .await
                            .map_err(|error| ApiError::invalid(error.to_string()))?,
                    )
                    .map_err(ApiError::from)?,
                    "idempotency_id",
                )?;
            }
            "data" => set_once(
                &mut data,
                field
                    .text()
                    .await
                    .map_err(|error| ApiError::invalid(error.to_string()))?,
                "data",
            )?,
            "source" => set_once(
                &mut source,
                field
                    .text()
                    .await
                    .map_err(|error| ApiError::invalid(error.to_string()))?,
                "source",
            )?,
            "source_created_at" => set_once(
                &mut source_created_at,
                field
                    .text()
                    .await
                    .map_err(|error| ApiError::invalid(error.to_string()))?,
                "source_created_at",
            )?,
            "data_filename" => set_once(
                &mut data_filename,
                field
                    .text()
                    .await
                    .map_err(|error| ApiError::invalid(error.to_string()))?,
                "data_filename",
            )?,
            _ => {
                return Err(ApiError::invalid(format!(
                    "Unknown provenance multipart field {name}."
                )));
            }
        }
    }
    let id = state
        .kmap
        .lock()
        .map_err(ApiError::internal)?
        .create_provenance_with_storage(
            idempotency_id.ok_or_else(|| ApiError::invalid("Missing idempotency_id."))?,
            NewProvenance {
                data: data.ok_or_else(|| ApiError::invalid("Missing data."))?,
                source: source.ok_or_else(|| ApiError::invalid("Missing source."))?,
                source_created_at: source_created_at
                    .ok_or_else(|| ApiError::invalid("Missing source_created_at."))?,
            },
            ProvenanceStorage {
                data_filename: data_filename
                    .ok_or_else(|| ApiError::invalid("Missing data_filename."))?,
                artifacts,
            },
        )
        .map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(json!({"id":id}))))
}

fn set_once<T>(slot: &mut Option<T>, value: T, name: &str) -> Result<(), ApiError> {
    if slot.replace(value).is_some() {
        return Err(ApiError::invalid(format!(
            "Multipart field {name} must appear once."
        )));
    }
    Ok(())
}

async fn get_provenance(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<kweb_db_core::Provenance>, ApiError> {
    let id = NodeId::from_hex(&id).map_err(ApiError::from)?;
    let provenance = state
        .kmap
        .lock()
        .map_err(ApiError::internal)?
        .get_provenance(id)
        .map_err(ApiError::from)?;
    Ok(Json(provenance))
}

async fn create_node(
    State(state): State<AppState>,
    Json(input): Json<CreateNodeRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let id = input
        .node_id
        .unwrap_or_else(|| generated_node_id(input.idempotency_id));
    let owner = parse_owner(&input.owner_node_id)?;
    let mut kmap = state.kmap.lock().map_err(ApiError::internal)?;
    let node = kmap
        .create_node(
            input.idempotency_id,
            CreateNode {
                id,
                provenance_id: input.provenance_id,
                owner,
                short_name: input.short_name,
                short_description: input.short_description,
                long_description: input.long_description,
                model_attribution: input.model_attribution,
                fixed_connections: input.fixed_connections,
                recent_connections: input.recent_connections,
            },
        )
        .map_err(ApiError::from)?;
    let response = node_response(&mut kmap, &node)?;
    Ok((StatusCode::CREATED, Json(json!({"node":response}))))
}

fn generated_node_id(idempotency_id: IdempotencyId) -> NodeId {
    let mut hash = Sha256::new();
    hash.update(b"kennedy-kmap-http-node-id-v1\0");
    hash.update(idempotency_id.to_hex());
    let encoded = hash
        .finalize()
        .iter()
        .take(20)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    NodeId::from_hex(&encoded).expect("SHA-256 prefix is a valid Kmap node identifier")
}

fn node_response(kmap: &mut Kmap, node: &kweb_db_core::Node) -> Result<Value, ApiError> {
    let mut seen = HashSet::new();
    let mut connection_summaries = Vec::new();
    for connection_id in node
        .fixed_connections
        .iter()
        .chain(node.recent_connections.iter())
        .copied()
    {
        if !seen.insert(connection_id) {
            continue;
        }
        let connection = kmap.get_node(connection_id).map_err(ApiError::from)?;
        connection_summaries.push(json!({
            "id": connection.id,
            "short_name": connection.short_name,
            "short_description": connection.short_description,
        }));
    }
    let mut response = serde_json::to_value(node).map_err(ApiError::internal)?;
    response
        .as_object_mut()
        .expect("a serialized Kmap node is an object")
        .insert(
            "connection_summaries".into(),
            Value::Array(connection_summaries),
        );
    Ok(response)
}

async fn get_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = NodeId::from_hex(&id).map_err(ApiError::from)?;
    let mut kmap = state.kmap.lock().map_err(ApiError::internal)?;
    let node = kmap.get_node(id).map_err(ApiError::from)?;
    Ok(Json(node_response(&mut kmap, &node)?))
}

async fn update_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<UpdateNodeRequest>,
) -> Result<Json<Value>, ApiError> {
    let id = NodeId::from_hex(&id).map_err(ApiError::from)?;
    let owner = parse_owner(&input.owner_node_id)?;
    let mut kmap = state.kmap.lock().map_err(ApiError::internal)?;
    let node = kmap
        .update_node(
            input.idempotency_id,
            id,
            UpdateNode {
                provenance_id: input.provenance_id,
                owner,
                short_name: input.short_name,
                short_description: input.short_description,
                long_description: input.long_description,
                model_attribution: input.model_attribution,
                fixed_connections: input.fixed_connections,
                recent_connections: input.recent_connections,
            },
        )
        .map_err(ApiError::from)?;
    let response = node_response(&mut kmap, &node)?;
    Ok(Json(json!({"node":response})))
}

async fn get_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = NodeId::from_hex(&id).map_err(ApiError::from)?;
    let provenance_ids = state
        .kmap
        .lock()
        .map_err(ApiError::internal)?
        .get_node_history(id)
        .map_err(ApiError::from)?;
    Ok(Json(json!({"node_id":id,"provenance_ids":provenance_ids})))
}

async fn get_prompt(
    State(state): State<AppState>,
    Path(filename): Path<String>,
) -> Result<Response, ApiError> {
    let safe_name = filename.ends_with(".txt")
        && !filename.starts_with('.')
        && filename
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'));
    if !safe_name {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: "Prompt manual not found.".into(),
        });
    }
    let body = tokio::fs::read_to_string(state.prompts_dir.join(&filename))
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ApiError {
                    status: StatusCode::NOT_FOUND,
                    code: "not_found",
                    message: "Prompt manual not found.".into(),
                }
            } else {
                ApiError::internal(error)
            }
        })?;
    Ok(([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response())
}

fn parse_owner(value: &str) -> Result<Owner, ApiError> {
    match value {
        "self" => Ok(Owner::SelfNode),
        "unowned" => Ok(Owner::Unowned),
        _ => NodeId::from_hex(value)
            .map(Owner::Node)
            .map_err(ApiError::from),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn http_request(
        address: std::net::SocketAddr,
        method: &str,
        path: &str,
        body: &str,
    ) -> String {
        http_request_with_content_type(address, method, path, "application/json", body).await
    }

    async fn http_request_with_content_type(
        address: std::net::SocketAddr,
        method: &str,
        path: &str,
        content_type: &str,
        body: &str,
    ) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn initializes_external_roots_and_serves_namespaced_api() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-kmap-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(directory.join("frontend")).unwrap();
        std::fs::create_dir_all(directory.join("prompts")).unwrap();
        let kmap_path = directory.join("kmap.sqlite3");
        let identity_path = directory.join("users.sqlite3");
        let (kmap, roots) =
            initialize(&kmap_path, &directory.join("artifacts"), &identity_path).unwrap();
        let identity = Connection::open(&identity_path).unwrap();
        let role_count: i64 = identity
            .query_row("SELECT COUNT(*) FROM kmap_system_roots", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(role_count, 2);
        drop(identity);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let intelligence = Router::new().route(
            "/api/v1/test-intelligence",
            get(|| async { Json(json!({"service":"intelligence"})) }),
        );
        let server = tokio::spawn(serve_with_listener(
            kmap,
            roots,
            directory.join("frontend"),
            directory.join("prompts"),
            MergedRouters::new(Router::new(), intelligence, Router::new(), Router::new()),
            listener,
        ));
        let response = http_request(address, "GET", "/api/v1/kmap/roots", "").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains(&roots.user.to_string()));
        assert!(response.contains(&roots.kennedy.to_string()));
        let intelligence_response =
            http_request(address, "GET", "/api/v1/test-intelligence", "").await;
        assert!(intelligence_response.starts_with("HTTP/1.1 200 OK"));
        assert!(intelligence_response.contains("\"service\":\"intelligence\""));
        let idempotency_id = IdempotencyId::random();
        let body = json!({
            "idempotency_id":idempotency_id,
            "data":"HTTP replay source",
            "source":"test",
            "source_created_at":"2026-07-18T00:00:00Z",
        })
        .to_string();
        let first = http_request(address, "POST", "/api/v1/kmap/provenance", &body).await;
        let replay = http_request(address, "POST", "/api/v1/kmap/provenance", &body).await;
        assert!(first.starts_with("HTTP/1.1 201 Created"));
        assert!(replay.starts_with("HTTP/1.1 201 Created"));
        let response_id = |response: &str| {
            serde_json::from_str::<Value>(response.split("\r\n\r\n").nth(1).unwrap()).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        assert_eq!(response_id(&first), response_id(&replay));

        let provenance_id = response_id(&first);
        let node_body = json!({
            "idempotency_id":IdempotencyId::random(),
            "provenance_id":provenance_id,
            "owner_node_id":"self",
            "model_attribution":"http-test",
            "short_name":"HTTP Replay Node",
            "short_description":"",
            "long_description":"",
            "fixed_connections":[],
            "recent_connections":[],
        })
        .to_string();
        let first_node = http_request(address, "POST", "/api/v1/kmap/nodes", &node_body).await;
        let replayed_node = http_request(address, "POST", "/api/v1/kmap/nodes", &node_body).await;
        assert!(first_node.starts_with("HTTP/1.1 201 Created"));
        assert!(replayed_node.starts_with("HTTP/1.1 201 Created"));
        let node_id = |response: &str| {
            serde_json::from_str::<Value>(response.split("\r\n\r\n").nth(1).unwrap()).unwrap()
                ["node"]["id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        assert_eq!(node_id(&first_node), node_id(&replayed_node));
        let history = http_request(
            address,
            "GET",
            &format!("/api/v1/kmap/nodes/{}/history", node_id(&first_node)),
            "",
        )
        .await;
        let history: Value =
            serde_json::from_str(history.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(history["provenance_ids"].as_array().unwrap().len(), 1);

        let connected_node_body = json!({
            "idempotency_id":IdempotencyId::random(),
            "provenance_id":provenance_id,
            "owner_node_id":"self",
            "model_attribution":"http-test",
            "short_name":"HTTP Connected Node",
            "short_description":"Connection metadata test",
            "long_description":"",
            "fixed_connections":[],
            "recent_connections":[node_id(&first_node)],
        })
        .to_string();
        let connected_node =
            http_request(address, "POST", "/api/v1/kmap/nodes", &connected_node_body).await;
        assert!(connected_node.starts_with("HTTP/1.1 201 Created"));
        let connected_node_json: Value =
            serde_json::from_str(connected_node.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            connected_node_json["node"]["connection_summaries"][0]["short_name"],
            "HTTP Replay Node"
        );
        let connected_node_get = http_request(
            address,
            "GET",
            &format!(
                "/api/v1/kmap/nodes/{}",
                connected_node_json["node"]["id"].as_str().unwrap()
            ),
            "",
        )
        .await;
        let connected_node_get: Value =
            serde_json::from_str(connected_node_get.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            connected_node_get["connection_summaries"][0]["short_description"],
            ""
        );

        let multipart_id = IdempotencyId::random();
        let boundary = "kweb-artifact-boundary";
        let multipart = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"idempotency_id\"\r\n\r\n{multipart_id}\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"source\"\r\n\r\nconversation-history\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"source_created_at\"\r\n\r\n2026-07-18T00:00:00Z\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"data_filename\"\r\n\r\nconversation-archive.json\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"artifact\"; filename=\"telegram-vnote.wav\"\r\nContent-Type: audio/wav\r\n\r\nvoice-note\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"data\"\r\n\r\n{{\"media\":[{{\"provenanceArtifactIndex\":0}}]}}\r\n\
             --{boundary}--\r\n"
        );
        let content_type = format!("multipart/form-data; boundary={boundary}");
        let first_artifact = http_request_with_content_type(
            address,
            "POST",
            "/api/v1/kmap/provenance-with-artifacts",
            &content_type,
            &multipart,
        )
        .await;
        let replayed_artifact = http_request_with_content_type(
            address,
            "POST",
            "/api/v1/kmap/provenance-with-artifacts",
            &content_type,
            &multipart,
        )
        .await;
        assert!(first_artifact.starts_with("HTTP/1.1 201 Created"));
        assert!(replayed_artifact.starts_with("HTTP/1.1 201 Created"));
        assert_eq!(
            response_id(&first_artifact),
            response_id(&replayed_artifact)
        );
        let artifact_provenance = http_request(
            address,
            "GET",
            &format!("/api/v1/kmap/provenance/{}", response_id(&first_artifact)),
            "",
        )
        .await;
        let artifact_provenance: Value =
            serde_json::from_str(artifact_provenance.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        let relative_path = artifact_provenance["artifacts"][0]["relative_path"]
            .as_str()
            .unwrap();
        assert!(relative_path.contains("/telegram-vnote."));
        assert!(relative_path.ends_with(".wav"));
        let served = http_request(
            address,
            "GET",
            &format!("/api/v1/kmap/provenance-artifacts/{relative_path}"),
            "",
        )
        .await;
        assert!(served.starts_with("HTTP/1.1 200 OK"));
        assert!(served.ends_with("voice-note"));

        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(directory).unwrap();
    }
}
