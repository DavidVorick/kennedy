#[cfg(test)]
use std::collections::BTreeMap;
use std::{
    collections::HashSet,
    path::Path as FilePath,
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
#[cfg(test)]
use kcode_commit_session::PlannedNode;
use kcode_commit_session::{CommitReceipt, CommitRequest, ErrorKind as CommitErrorKind};
use kcode_kweb_db::{
    Config, Error as KwebError, KwebDb, Node, NodeData, NodeId, ObjectId, Owner, Provenance,
};
#[cfg(test)]
use kcode_server_object_envelopes::encode_file;
use kcode_server_object_envelopes::{
    StoredFile, StoredProvenance, decode_file, decode_provenance, encode_provenance,
    sanitize_file_name,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower_http::trace::TraceLayer;

const MAX_REQUEST_BYTES: usize = 128 * 1024 * 1024;
const MAX_EMBEDDED_PROVENANCE_BYTES: usize = 1024 * 1024;
const KUI_LOADER_MODULE: &str = "/lib/kcode-kui-loader/v0.1";
const KUI_LOADER_PAGE: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kennedy</title>
</head>
<body>
  <p>Loading Kennedy…</p>
  <script type="module">
    import { load } from "/lib/kcode-kui-loader/v0.1";
    load();
  </script>
</body>
</html>
"#;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SystemRoots {
    pub user: NodeId,
    pub kennedy: NodeId,
}

#[derive(Clone)]
pub(crate) struct Service {
    database: Arc<KwebDb>,
    roots: SystemRoots,
    receipts: Arc<Mutex<Connection>>,
    receipt_database: std::path::PathBuf,
}

impl Service {
    pub(crate) fn new(
        database: KwebDb,
        roots: SystemRoots,
        identity_database: &FilePath,
    ) -> anyhow::Result<Self> {
        let receipts = Connection::open(identity_database).with_context(|| {
            format!(
                "opening identity database {} for Kmap idempotency receipts",
                identity_database.display()
            )
        })?;
        receipts.execute_batch("PRAGMA busy_timeout=5000;")?;
        Ok(Self {
            database: Arc::new(database),
            roots,
            receipts: Arc::new(Mutex::new(receipts)),
            receipt_database: identity_database.to_path_buf(),
        })
    }

    pub(crate) async fn get_json(&self, path: &str) -> Result<Value, ApiError> {
        let state = State(self.clone());
        match path {
            "/api/v1/kmap/health" => {
                let Json(value) = health(state).await?;
                Ok(value)
            }
            "/api/v1/kmap/roots" => {
                let Json(value) = get_roots(state).await;
                Ok(value)
            }
            _ if path.starts_with("/api/v1/kmap/nodes/") && path.ends_with("/history") => {
                let id = path
                    .trim_start_matches("/api/v1/kmap/nodes/")
                    .trim_end_matches("/history");
                let Json(value) = get_history(state, Path(id.into())).await?;
                Ok(value)
            }
            _ if path.starts_with("/api/v1/kmap/nodes/") => {
                let id = path.trim_start_matches("/api/v1/kmap/nodes/");
                let Json(value) = get_node(state, Path(id.into())).await?;
                Ok(value)
            }
            _ if path.starts_with("/api/v1/kmap/provenance/") => {
                let id = path.trim_start_matches("/api/v1/kmap/provenance/");
                let Json(value) = get_provenance(state, Path(id.into())).await?;
                Ok(value)
            }
            _ if path.starts_with("/api/v1/session-history/") => {
                let id = path.trim_start_matches("/api/v1/session-history/");
                let id = parse_object_id(id)?;
                let bytes = self.database.get_object(id)?;
                serde_json::from_slice(&bytes).map_err(|error| {
                    ApiError::internal(format!(
                        "session archive object {id} is not valid JSON: {error}"
                    ))
                })
            }
            _ => Err(ApiError::not_found("Kmap resource not found.")),
        }
    }

    pub(crate) async fn post_json(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        let state = State(self.clone());
        match path {
            "/api/v1/kmap/provenance" => {
                let (_, Json(value)) = create_provenance(
                    state,
                    Json(
                        serde_json::from_value(body)
                            .map_err(|error| ApiError::invalid(error.to_string()))?,
                    ),
                )
                .await?;
                Ok(value)
            }
            "/api/v1/kmap/nodes" => {
                let (_, Json(value)) = create_node(
                    state,
                    Json(
                        serde_json::from_value(body)
                            .map_err(|error| ApiError::invalid(error.to_string()))?,
                    ),
                )
                .await?;
                Ok(value)
            }
            _ => Err(ApiError::not_found("Kmap resource not found.")),
        }
    }

    pub(crate) async fn put_json(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        let id = path
            .strip_prefix("/api/v1/kmap/nodes/")
            .filter(|id| !id.contains('/'))
            .ok_or_else(|| ApiError::not_found("Kmap node not found."))?;
        let Json(value) = update_node(
            State(self.clone()),
            Path(id.into()),
            Json(
                serde_json::from_value(body)
                    .map_err(|error| ApiError::invalid(error.to_string()))?,
            ),
        )
        .await?;
        Ok(value)
    }

    pub(crate) fn get_file(&self, id: &str) -> Result<StoredFile, ApiError> {
        let id = parse_object_id(id)?;
        let bytes = self.database.get_object(id)?;
        decode_file(id, bytes).map_err(ApiError::internal)
    }

    pub(crate) fn save_rust_binary_object(&self, bytes: Vec<u8>) -> Result<String, ApiError> {
        let mut transaction = self.database.start_transaction(Provenance {
            author: "Kennedy".into(),
            source: "kennedy-rust-binary".into(),
            source_created_at: Utc::now(),
            data: "Output payload from a managed Rust-binary call.".into(),
        })?;
        let id = transaction.create_object(bytes)?;
        transaction.finalize()?;
        Ok(id.to_string())
    }

    pub(crate) fn commit_session(&self, request: CommitRequest) -> Result<CommitReceipt, ApiError> {
        // All Kmap mutations share this lane. The library owns the session
        // receipt connection, while this guard preserves Kennedy's scheduling
        // relationship with its other idempotent Kweb mutations.
        let _receipt_lane = self
            .receipts
            .lock()
            .map_err(|_| ApiError::internal("Kmap idempotency mutex is poisoned"))?;
        kcode_commit_session::commit_session(&self.database, &self.receipt_database, request)
            .map_err(ApiError::from)
    }

    fn with_idempotency(
        &self,
        idempotency_id: &str,
        operation: &'static str,
        request_sha256: [u8; 32],
        mutation: impl FnOnce() -> Result<String, ApiError>,
    ) -> Result<String, ApiError> {
        validate_idempotency_id(idempotency_id)?;
        let receipts = self
            .receipts
            .lock()
            .map_err(|_| ApiError::internal("Kmap idempotency mutex is poisoned"))?;
        let existing = receipts
            .query_row(
                "SELECT operation,request_sha256,result_id
                 FROM kmap_idempotency_receipts WHERE idempotency_id=?1",
                [idempotency_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(ApiError::internal)?;
        if let Some((stored_operation, stored_hash, result_id)) = existing {
            if stored_operation != operation || stored_hash.as_slice() != request_sha256 {
                return Err(ApiError::conflict(
                    "idempotency_id was already used for a different Kmap mutation",
                ));
            }
            return result_id.ok_or_else(|| {
                ApiError::conflict(
                    "a prior Kmap mutation with this idempotency_id has an unknown outcome; offline recovery is required",
                )
            });
        }

        receipts
            .execute(
                "INSERT INTO kmap_idempotency_receipts(
                     idempotency_id,operation,request_sha256,result_id,started_at,committed_at
                 ) VALUES(?1,?2,?3,NULL,?4,NULL)",
                params![
                    idempotency_id,
                    operation,
                    request_sha256.as_slice(),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(ApiError::internal)?;
        let result_id = mutation()?;
        let updated = receipts
            .execute(
                "UPDATE kmap_idempotency_receipts
                 SET result_id=?2,committed_at=?3
                 WHERE idempotency_id=?1 AND result_id IS NULL",
                params![idempotency_id, &result_id, Utc::now().to_rfc3339()],
            )
            .map_err(ApiError::internal)?;
        if updated != 1 {
            return Err(ApiError::internal(
                "Kmap idempotency receipt disappeared during mutation",
            ));
        }
        Ok(result_id)
    }
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl ApiError {
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(error=%error, "Kmap request failed");
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

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "conflict",
            message: message.into(),
        }
    }
}

impl From<KwebError> for ApiError {
    fn from(error: KwebError) -> Self {
        match error {
            KwebError::InvalidInput(message) | KwebError::InvalidTransaction(message) => {
                Self::invalid(message)
            }
            KwebError::NotFound(message) => Self::not_found(message),
            KwebError::Busy(message) => Self {
                status: StatusCode::CONFLICT,
                code: "conflict",
                message,
            },
            KwebError::Io(_)
            | KwebError::Corrupt(_)
            | KwebError::InvalidConfig(_)
            | KwebError::OfflineUpgradeRequired(_) => Self::internal(error),
        }
    }
}

impl From<kcode_commit_session::Error> for ApiError {
    fn from(error: kcode_commit_session::Error) -> Self {
        match error.kind() {
            CommitErrorKind::InvalidInput => Self::invalid(error.to_string()),
            CommitErrorKind::NotFound => Self::not_found(error.to_string()),
            CommitErrorKind::Conflict => Self::conflict(error.to_string()),
            _ => Self::internal(error),
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

pub(crate) struct ArtifactInput {
    pub original_filename: String,
    pub media_type: String,
    pub data: Vec<u8>,
}

#[derive(Deserialize)]
struct CreateNodeRequest {
    idempotency_id: String,
    provenance_id: String,
    owner_node_id: String,
    model_attribution: String,
    short_name: String,
    short_description: String,
    long_description: String,
    #[serde(default)]
    fixed_connections: Vec<String>,
    #[serde(default)]
    recent_connections: Vec<String>,
}

#[derive(Deserialize)]
struct UpdateNodeRequest {
    idempotency_id: String,
    provenance_id: String,
    owner_node_id: String,
    model_attribution: String,
    short_name: String,
    short_description: String,
    long_description: String,
    fixed_connections: Vec<String>,
    recent_connections: Vec<String>,
}

#[derive(Deserialize)]
struct CreateProvenanceRequest {
    idempotency_id: String,
    data: String,
    source: String,
    source_created_at: String,
}

pub(crate) fn initialize(
    kweb_root: &FilePath,
    config: Config,
    identity_database: &FilePath,
) -> anyhow::Result<(KwebDb, SystemRoots)> {
    let mut identity = Connection::open(identity_database).with_context(|| {
        format!(
            "opening identity database {} for system roots",
            identity_database.display()
        )
    })?;
    identity.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
         CREATE TABLE IF NOT EXISTS kmap_system_roots (
             role TEXT PRIMARY KEY CHECK(role IN ('user','kennedy')),
             root_node_id TEXT NOT NULL UNIQUE CHECK(length(root_node_id)=8),
             created_at TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS kmap_idempotency_receipts (
             idempotency_id TEXT PRIMARY KEY CHECK(length(idempotency_id)=32),
             operation TEXT NOT NULL,
             request_sha256 BLOB NOT NULL CHECK(length(request_sha256)=32),
             result_id TEXT CHECK(result_id IS NULL OR length(result_id)=8),
             started_at TEXT NOT NULL,
             committed_at TEXT,
             CHECK((result_id IS NULL) = (committed_at IS NULL))
         );",
    )?;
    let database = KwebDb::open(kweb_root, config).map_err(anyhow::Error::new)?;
    let existing_user = system_root(&identity, "user")?;
    let existing_kennedy = system_root(&identity, "kennedy")?;
    let roots = match (existing_user, existing_kennedy) {
        (Some(user), Some(kennedy)) => SystemRoots {
            user: user
                .parse::<NodeId>()
                .with_context(|| format!("invalid stored user root node ID {user:?}"))?,
            kennedy: kennedy
                .parse::<NodeId>()
                .with_context(|| format!("invalid stored Kennedy root node ID {kennedy:?}"))?,
        },
        (None, None) => {
            let mut transaction = database
                .start_transaction(Provenance {
                    author: "system-bootstrap".into(),
                    source: "system-bootstrap".into(),
                    source_created_at: Utc::now(),
                    data: "Initial Kweb system-root bootstrap.".into(),
                })
                .map_err(anyhow::Error::new)?;
            let user = transaction
                .create_node(root_data("Initial User Root", "", ""))
                .map_err(anyhow::Error::new)?;
            let kennedy = transaction
                .create_node(root_data(
                    "Kennedy's Root",
                    "The root of Kennedy's own Kmap knowledge.",
                    "This is Kennedy's root node. It anchors Kennedy's own durable knowledge and learned lessons in the Kmap.",
                ))
                .map_err(anyhow::Error::new)?;
            transaction.finalize().map_err(anyhow::Error::new)?;
            let sql = identity.transaction()?;
            let now = Utc::now().to_rfc3339();
            sql.execute(
                "INSERT INTO kmap_system_roots(role,root_node_id,created_at) VALUES('user',?1,?2)",
                params![user.to_string(), now],
            )?;
            sql.execute(
                "INSERT INTO kmap_system_roots(role,root_node_id,created_at) VALUES('kennedy',?1,?2)",
                params![kennedy.to_string(), now],
            )?;
            sql.commit()?;
            SystemRoots { user, kennedy }
        }
        _ => anyhow::bail!("the system-root directory contains only one of its two required roles"),
    };
    database.get_node(roots.user).map_err(anyhow::Error::new)?;
    database
        .get_node(roots.kennedy)
        .map_err(anyhow::Error::new)?;
    Ok((database, roots))
}

fn system_root(identity: &Connection, role: &str) -> anyhow::Result<Option<String>> {
    Ok(identity
        .query_row(
            "SELECT root_node_id FROM kmap_system_roots WHERE role=?1",
            [role],
            |row| row.get(0),
        )
        .optional()?)
}

fn root_data(short_name: &str, short_description: &str, long_description: &str) -> NodeData {
    NodeData {
        short_name: short_name.into(),
        short_description: short_description.into(),
        long_description: long_description.into(),
        owner: Owner::SelfNode,
        fixed_connections: Vec::new(),
        recent_connections: Vec::new(),
        objects: Vec::new(),
    }
}

pub(crate) struct MergedRouters {
    session_history: Router,
    audio_ingress: Router,
    web_libraries: Router,
}

impl MergedRouters {
    pub(crate) fn new(
        session_history: Router,
        audio_ingress: Router,
        web_libraries: Router,
    ) -> Self {
        Self {
            session_history,
            audio_ingress,
            web_libraries,
        }
    }
}

pub(crate) async fn serve_with_listener(
    state: Service,
    merged_routers: MergedRouters,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(kui_loader_page))
        .route("/index.html", get(kui_loader_page))
        .route("/api/v1/kmap/health", get(health))
        .route("/api/v1/kmap/roots", get(get_roots))
        .route("/api/v1/kmap/nodes/{node_id}", get(get_node))
        .route("/api/v1/kmap/nodes/{node_id}/history", get(get_history))
        .route(
            "/api/v1/kmap/provenance/{provenance_id}",
            get(get_provenance),
        )
        .route(
            "/api/v1/session-history/{session_object_id}",
            get(get_session_archive),
        )
        .route("/api/v1/objects/{object_id}", get(get_object_file))
        .with_state(state)
        .merge(merged_routers.session_history)
        .merge(merged_routers.audio_ingress)
        .merge(merged_routers.web_libraries)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(TraceLayer::new_for_http())
        .layer(middleware::map_response(set_default_cache_control));
    tracing::info!(address=%listener.local_addr()?, "Kennedy main HTTP server ready");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn kui_loader_page() -> Response {
    debug_assert!(KUI_LOADER_PAGE.contains(KUI_LOADER_MODULE));
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store, max-age=0")
        .header("x-content-type-options", "nosniff")
        .body(Body::from(KUI_LOADER_PAGE))
        .expect("fixed Kennedy UI loader response is valid")
}

async fn set_default_cache_control(mut response: Response) -> Response {
    if !response.headers().contains_key(header::CACHE_CONTROL) {
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store, max-age=0"),
        );
    }
    response
}

async fn health(State(state): State<Service>) -> Result<Json<Value>, ApiError> {
    state.database.get_node(state.roots.user)?;
    state.database.get_node(state.roots.kennedy)?;
    Ok(Json(json!({"service":"kmap","status":"ok"})))
}

async fn get_roots(State(state): State<Service>) -> Json<Value> {
    Json(json!({
        "user_root_node_id":state.roots.user.to_string(),
        "kennedy_root_node_id":state.roots.kennedy.to_string(),
    }))
}

async fn create_provenance(
    State(state): State<Service>,
    Json(input): Json<CreateProvenanceRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let source_created_at = parse_time(&input.source_created_at)?;
    let digest = provenance_request_digest(
        "create_provenance",
        &input.data,
        &input.source,
        source_created_at,
        None,
        &[],
    );
    let id = state.with_idempotency(&input.idempotency_id, "create_provenance", digest, || {
        let mut transaction = state
            .database
            .start_transaction(storage_provenance(&input.source, source_created_at))?;
        let envelope = StoredProvenance {
            data: input.data,
            source: input.source,
            source_created_at,
            artifacts: Vec::new(),
        };
        let id =
            transaction.create_object(encode_provenance(&envelope).map_err(ApiError::internal)?)?;
        transaction.finalize()?;
        Ok(id.to_string())
    })?;
    Ok((StatusCode::CREATED, Json(json!({"id":id.to_string()}))))
}

async fn get_provenance(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_object_id(&id)?;
    let provenance = load_provenance(&state.database, id)?;
    Ok(Json(provenance_response(&provenance)))
}

async fn get_session_archive(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_object_id(&id)?;
    let bytes = state.database.get_object(id)?;
    let archive = serde_json::from_slice(&bytes).map_err(|error| {
        ApiError::internal(format!(
            "session archive object {id} is not valid JSON: {error}"
        ))
    })?;
    Ok(Json(archive))
}

async fn get_object_file(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    stored_file_response(state.get_file(&id)?)
}

fn stored_file_response(file: StoredFile) -> Result<Response, ApiError> {
    let content_type = HeaderValue::from_str(&file.media_type)
        .map_err(|_| ApiError::internal("stored object has an invalid media type"))?;
    let disposition = HeaderValue::from_str(&format!(
        "inline; filename=\"{}\"",
        ascii_response_file_name(&file.file_name)
    ))
    .map_err(|_| ApiError::internal("stored object has an invalid filename"))?;
    let content_length = HeaderValue::from_str(&file.bytes.len().to_string())
        .map_err(|_| ApiError::internal("stored object has an invalid content length"))?;
    let mut response = Response::new(Body::from(file.bytes));
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

fn ascii_response_file_name(value: &str) -> String {
    let output = sanitize_file_name(value, "object.bin")
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() && !matches!(character, '"' | '\\' | '/' | ';') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if output.trim_matches('_').is_empty() {
        "object.bin".into()
    } else {
        output
    }
}

async fn create_node(
    State(state): State<Service>,
    Json(input): Json<CreateNodeRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let provenance_id = parse_object_id(&input.provenance_id)?;
    let data = NodeData {
        short_name: input.short_name,
        short_description: input.short_description,
        long_description: input.long_description,
        owner: parse_owner(&input.owner_node_id)?,
        fixed_connections: parse_node_ids(&input.fixed_connections)?,
        recent_connections: parse_node_ids(&input.recent_connections)?,
        objects: Vec::new(),
    };
    let digest = node_request_digest(
        "create_node",
        None,
        provenance_id,
        &input.model_attribution,
        &data,
    );
    let id = state.with_idempotency(&input.idempotency_id, "create_node", digest, || {
        let provenance = load_provenance(&state.database, provenance_id)?;
        let mut transaction = state.database.start_transaction(transaction_provenance(
            &provenance,
            provenance_id,
            input.model_attribution,
        ))?;
        let id = transaction.create_node(data)?;
        transaction.finalize()?;
        Ok(id.to_string())
    })?;
    let id = parse_node_id(&id)?;
    let node = state.database.get_node(id)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"node":node_response(&state.database, &node)?})),
    ))
}

async fn get_node(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_node_id(&id)?;
    let node = state.database.get_node(id)?;
    Ok(Json(node_response(&state.database, &node)?))
}

async fn update_node(
    State(state): State<Service>,
    Path(id): Path<String>,
    Json(input): Json<UpdateNodeRequest>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_node_id(&id)?;
    let provenance_id = parse_object_id(&input.provenance_id)?;
    let data = NodeData {
        short_name: input.short_name,
        short_description: input.short_description,
        long_description: input.long_description,
        owner: parse_owner(&input.owner_node_id)?,
        fixed_connections: parse_node_ids(&input.fixed_connections)?,
        recent_connections: parse_node_ids(&input.recent_connections)?,
        objects: Vec::new(),
    };
    let digest = node_request_digest(
        "update_node",
        Some(id),
        provenance_id,
        &input.model_attribution,
        &data,
    );
    state.with_idempotency(&input.idempotency_id, "update_node", digest, || {
        let provenance = load_provenance(&state.database, provenance_id)?;
        let mut data = data;
        data.objects = state.database.get_node(id)?.data.objects;
        let mut transaction = state.database.start_transaction(transaction_provenance(
            &provenance,
            provenance_id,
            input.model_attribution,
        ))?;
        transaction.update_node(id, data)?;
        transaction.finalize()?;
        Ok(id.to_string())
    })?;
    let node = state.database.get_node(id)?;
    Ok(Json(json!({"node":node_response(&state.database, &node)?})))
}

async fn get_history(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_node_id(&id)?;
    let history = state.database.get_node_history(id)?;
    let entries = history
        .entries
        .into_iter()
        .map(|entry| {
            json!({
                "transaction_id":entry.transaction_id.to_string(),
                "writer":entry.writer.to_string(),
                "committed_at":entry.committed_at.to_rfc3339(),
                "author":entry.provenance.author,
                "source":entry.provenance.source,
                "source_created_at":entry.provenance.source_created_at.to_rfc3339(),
                "data":entry.provenance.data,
                "active":entry.active,
                "created":entry.created,
                "updated":entry.updated,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "node_id":id.to_string(),
        "visible_transaction":history.visible.map(|value| value.to_string()),
        "entries":entries,
    })))
}

fn node_response(database: &KwebDb, node: &Node) -> Result<Value, ApiError> {
    let mut seen = HashSet::new();
    let mut connection_summaries = Vec::new();
    for connection_id in node
        .data
        .fixed_connections
        .iter()
        .chain(&node.data.recent_connections)
        .copied()
    {
        if !seen.insert(connection_id) {
            continue;
        }
        let connection = database.get_node(connection_id)?;
        connection_summaries.push(json!({
            "id":connection.id.to_string(),
            "short_name":connection.data.short_name,
            "short_description":connection.data.short_description,
        }));
    }
    let owner = match node.data.owner {
        Owner::Unowned => Value::Null,
        Owner::SelfNode => Value::String(node.id.to_string()),
        Owner::Node(id) => Value::String(id.to_string()),
    };
    Ok(json!({
        "id":node.id.to_string(),
        "short_name":node.data.short_name,
        "short_description":node.data.short_description,
        "long_description":node.data.long_description,
        "last_modified_by":node.last_author,
        "last_modified_at":node.committed_at.to_rfc3339(),
        "owner_node_id":owner,
        "fixed_connections":node.data.fixed_connections.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "recent_connections":node.data.recent_connections.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "objects":node.data.objects.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "connection_summaries":connection_summaries,
    }))
}

fn provenance_response(provenance: &StoredProvenance) -> Value {
    json!({
        "data":provenance.data,
        "source":provenance.source,
        "source_created_at":provenance.source_created_at.to_rfc3339(),
        "artifacts":provenance.artifacts.iter().map(|artifact| json!({
            "object_id":artifact.object_id.to_string(),
            "original_filename":artifact.original_filename,
            "media_type":artifact.media_type,
            "role":artifact.role,
            "byte_length":artifact.byte_length,
            "sha256":hex::encode(artifact.sha256),
        })).collect::<Vec<_>>(),
    })
}

fn storage_provenance(source: &str, source_created_at: DateTime<Utc>) -> Provenance {
    Provenance {
        author: "kennedy-provenance".into(),
        source: if source.trim().is_empty() {
            "kennedy".into()
        } else {
            source.into()
        },
        source_created_at,
        data: "Stored Kennedy provenance object.".into(),
    }
}

fn transaction_provenance(
    stored: &StoredProvenance,
    object_id: ObjectId,
    author: String,
) -> Provenance {
    let data = if stored.data.len() <= MAX_EMBEDDED_PROVENANCE_BYTES {
        stored.data.clone()
    } else {
        format!("Kennedy provenance is stored in object {object_id}.")
    };
    Provenance {
        author,
        source: stored.source.clone(),
        source_created_at: stored.source_created_at,
        data,
    }
}

fn load_provenance(database: &KwebDb, id: ObjectId) -> Result<StoredProvenance, ApiError> {
    let bytes = database.get_object(id)?;
    decode_provenance(&bytes)
        .map_err(|error| ApiError::internal(format!("invalid provenance object {id}: {error}")))
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ApiError::invalid("source_created_at must be RFC 3339"))
}

fn parse_owner(value: &str) -> Result<Owner, ApiError> {
    match value {
        "self" => Ok(Owner::SelfNode),
        "unowned" => Ok(Owner::Unowned),
        _ => Ok(Owner::Node(parse_node_id(value)?)),
    }
}

fn parse_node_ids(values: &[String]) -> Result<Vec<NodeId>, ApiError> {
    values.iter().map(|value| parse_node_id(value)).collect()
}

fn parse_node_id(value: &str) -> Result<NodeId, ApiError> {
    NodeId::from_str(value).map_err(ApiError::from)
}

fn parse_object_id(value: &str) -> Result<ObjectId, ApiError> {
    ObjectId::from_str(value).map_err(ApiError::from)
}

fn validate_idempotency_id(value: &str) -> Result<(), ApiError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApiError::invalid(
            "idempotency_id must encode 16 bytes as lowercase hexadecimal",
        ));
    }
    Ok(())
}

struct RequestDigest(Sha256);

impl RequestDigest {
    fn new(operation: &str) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"kennedy kmap idempotency v1\0");
        let mut value = Self(hash);
        value.field(operation.as_bytes());
        value
    }

    fn field(&mut self, bytes: &[u8]) {
        self.0.update((bytes.len() as u64).to_be_bytes());
        self.0.update(bytes);
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

fn provenance_request_digest(
    operation: &str,
    data: &str,
    source: &str,
    source_created_at: DateTime<Utc>,
    data_filename: Option<&str>,
    artifacts: &[ArtifactInput],
) -> [u8; 32] {
    let mut hash = RequestDigest::new(operation);
    hash.field(data.as_bytes());
    hash.field(source.as_bytes());
    hash.field(&source_created_at.timestamp().to_be_bytes());
    hash.field(&source_created_at.timestamp_subsec_nanos().to_be_bytes());
    hash.field(data_filename.unwrap_or("").as_bytes());
    hash.field(&(artifacts.len() as u64).to_be_bytes());
    for artifact in artifacts {
        hash.field(artifact.original_filename.as_bytes());
        hash.field(artifact.media_type.as_bytes());
        hash.field(&artifact.data);
    }
    hash.finish()
}

fn node_request_digest(
    operation: &str,
    id: Option<NodeId>,
    provenance_id: ObjectId,
    model_attribution: &str,
    data: &NodeData,
) -> [u8; 32] {
    let mut hash = RequestDigest::new(operation);
    hash.field(&id.map(NodeId::to_bytes).unwrap_or([0; 6]));
    hash.field(&provenance_id.to_bytes());
    hash.field(model_attribution.as_bytes());
    hash.field(data.short_name.as_bytes());
    hash.field(data.short_description.as_bytes());
    hash.field(data.long_description.as_bytes());
    match data.owner {
        Owner::Unowned => hash.field(&[0]),
        Owner::SelfNode => hash.field(&[1]),
        Owner::Node(owner) => {
            hash.field(&[2]);
            hash.field(&owner.to_bytes());
        }
    }
    hash.field(&(data.fixed_connections.len() as u64).to_be_bytes());
    for connection in &data.fixed_connections {
        hash.field(&connection.to_bytes());
    }
    hash.field(&(data.recent_connections.len() as u64).to_be_bytes());
    for connection in &data.recent_connections {
        hash.field(&connection.to_bytes());
    }
    hash.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kcode_kweb_db::{NoopGossip, WriterId};

    #[tokio::test]
    async fn root_page_loads_the_floating_kui_patch_line_without_caching() {
        let response = kui_loader_page().await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "no-store, max-age=0"
        );
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains(r#"import { load } from "/lib/kcode-kui-loader/v0.1""#));
        assert!(body.contains("load();"));
        assert!(!body.contains("kcode-kennedy-ui"));
    }

    fn config() -> Config {
        let signing_key = rand::random::<[u8; 32]>();
        Config {
            signing_key,
            writers_by_priority: vec![WriterId::from_signing_key(&signing_key)],
            gossip: Arc::new(NoopGossip),
        }
    }

    #[test]
    fn initializes_canonical_system_roots() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-kweb-http-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let (database, roots) = initialize(
            &directory.join("kweb"),
            config(),
            &directory.join("users.sqlite3"),
        )
        .unwrap();
        assert_eq!(roots.user.to_string().len(), 8);
        assert_eq!(roots.kennedy.to_string().len(), 8);
        database.get_node(roots.user).unwrap();
        database.get_node(roots.kennedy).unwrap();
        drop(database);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rust_binary_payloads_are_stored_raw_and_file_envelopes_decode_to_exact_bytes() {
        let directory = std::env::temp_dir().join(format!(
            "kennedy-rust-binary-objects-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let identity = directory.join("users.sqlite3");
        let (database, roots) = initialize(&directory.join("kweb"), config(), &identity).unwrap();
        let service = Service::new(database, roots, &identity).unwrap();

        let object_id = service
            .save_rust_binary_object(b"%PDF-1.7\nexact binary output\n".to_vec())
            .unwrap();
        assert_eq!(object_id.len(), 8);
        let output = service.get_file(&object_id).unwrap();
        assert_eq!(output.bytes, b"%PDF-1.7\nexact binary output\n");
        assert_eq!(output.media_type, "application/pdf");
        assert_eq!(output.file_name, format!("{object_id}.pdf"));
        assert!(!output.enveloped);

        let envelope = encode_file(
            "pending:1",
            Some("original.jpg"),
            "image/jpeg",
            Some("photo"),
            b"\xff\xd8\xffexact original".to_vec(),
        )
        .unwrap();
        let object_id = service.save_rust_binary_object(envelope).unwrap();
        let input = service.get_file(&object_id).unwrap();
        assert_eq!(input.bytes, b"\xff\xd8\xffexact original");
        assert!(input.enveloped);

        drop(service);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn file_envelope_preserves_original_bytes_and_safe_metadata() {
        let object_id = ObjectId::from_bytes([0x80, 1, 2, 3, 4, 6]).unwrap();
        let encoded = encode_file(
            "pending:4",
            Some("../résumé.pdf"),
            "application/pdf",
            Some("document"),
            b"%PDF-original".to_vec(),
        )
        .unwrap();
        let decoded = decode_file(object_id, encoded).unwrap();
        assert_eq!(decoded.file_name, "résumé.pdf");
        assert_eq!(decoded.media_type, "application/pdf");
        assert_eq!(decoded.transport_kind.as_deref(), Some("document"));
        assert_eq!(decoded.bytes, b"%PDF-original");
        assert!(decoded.enveloped);
        let response = stored_file_response(decoded).unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            "inline; filename=\"r_sum_.pdf\""
        );
    }

    #[tokio::test]
    async fn application_idempotency_replays_without_duplicate_kweb_transactions() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-kweb-receipts-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let identity = directory.join("users.sqlite3");
        let (database, roots) = initialize(&directory.join("kweb"), config(), &identity).unwrap();
        let service = Service::new(database, roots, &identity).unwrap();

        let provenance_request = json!({
            "idempotency_id":"00000000000000000000000000000001",
            "data":"source material",
            "source":"test",
            "source_created_at":"2026-07-23T00:00:00Z",
        });
        let first = service
            .post_json("/api/v1/kmap/provenance", provenance_request.clone())
            .await
            .unwrap();
        let replay = service
            .post_json("/api/v1/kmap/provenance", provenance_request)
            .await
            .unwrap();
        assert_eq!(first, replay);

        let conflicting = service
            .post_json(
                "/api/v1/kmap/provenance",
                json!({
                    "idempotency_id":"00000000000000000000000000000001",
                    "data":"different source material",
                    "source":"test",
                    "source_created_at":"2026-07-23T00:00:00Z",
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(conflicting.status, StatusCode::CONFLICT);

        let node_request = json!({
            "idempotency_id":"00000000000000000000000000000002",
            "provenance_id":first["id"],
            "owner_node_id":"self",
            "model_attribution":"test",
            "short_name":"Test Node",
            "short_description":"",
            "long_description":"",
            "fixed_connections":[],
            "recent_connections":[],
        });
        let first_node = service
            .post_json("/api/v1/kmap/nodes", node_request.clone())
            .await
            .unwrap();
        let replayed_node = service
            .post_json("/api/v1/kmap/nodes", node_request)
            .await
            .unwrap();
        assert_eq!(first_node, replayed_node);
        let node_id = first_node["node"]["id"].as_str().unwrap();
        let history = service
            .get_json(&format!("/api/v1/kmap/nodes/{node_id}/history"))
            .await
            .unwrap();
        assert_eq!(history["entries"].as_array().unwrap().len(), 1);

        drop(service);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn one_session_commit_supports_circular_creates_with_archive_objects_and_updates() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-session-commit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let identity = directory.join("users.sqlite3");
        let (database, roots) = initialize(&directory.join("kweb"), config(), &identity).unwrap();
        let service = Service::new(database, roots, &identity).unwrap();
        let root = service.database.get_node(roots.user).unwrap();
        let pending_node = "pending:7".to_owned();
        let circular_node = "pending:9".to_owned();
        let pending_object = "pending:8".to_owned();
        let result = service
            .commit_session(CommitRequest {
                idempotency_key: "session-test".into(),
                author: "test-model".into(),
                source_created_at: DateTime::parse_from_rfc3339("2026-07-23T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                archive: format!("{{\"session\":\"archive\",\"object\":\"{pending_object}\"}}")
                    .into_bytes(),
                objects: BTreeMap::from([(
                    pending_object.clone(),
                    encode_file(
                        &pending_object,
                        Some("attachment.txt"),
                        "text/plain",
                        Some("document"),
                        b"attachment".to_vec(),
                    )
                    .unwrap(),
                )]),
                creates: BTreeMap::from([
                    (
                        pending_node.clone(),
                        PlannedNode {
                            short_name: "Created Memory".into(),
                            short_description: String::new(),
                            long_description: format!(
                                "Created in one session transaction with object {pending_object}."
                            ),
                            owner: roots.user.to_string(),
                            fixed_connections: Vec::new(),
                            recent_connections: vec![roots.user.to_string(), circular_node.clone()],
                            objects: vec![pending_object.clone()],
                            attach_session_archive: true,
                        },
                    ),
                    (
                        circular_node.clone(),
                        PlannedNode {
                            short_name: "Circular Memory".into(),
                            short_description: String::new(),
                            long_description: "References the other created node.".into(),
                            owner: roots.user.to_string(),
                            fixed_connections: Vec::new(),
                            recent_connections: vec![pending_node.clone()],
                            objects: Vec::new(),
                            attach_session_archive: false,
                        },
                    ),
                ]),
                updates: BTreeMap::from([(
                    roots.user,
                    PlannedNode {
                        short_name: root.data.short_name,
                        short_description: root.data.short_description,
                        long_description: root.data.long_description,
                        owner: "self".into(),
                        fixed_connections: root
                            .data
                            .fixed_connections
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                        recent_connections: vec![pending_node.clone()],
                        objects: root.data.objects.iter().map(ToString::to_string).collect(),
                        attach_session_archive: true,
                    },
                )]),
            })
            .unwrap();
        let created_id = result.node_ids[&pending_node];
        let circular_id = result.node_ids[&circular_node];
        let created = service.database.get_node(created_id).unwrap();
        let circular = service.database.get_node(circular_id).unwrap();
        let updated_root = service.database.get_node(roots.user).unwrap();
        assert_eq!(
            created.data.recent_connections,
            vec![roots.user, circular_id]
        );
        assert_eq!(circular.data.recent_connections, vec![created_id]);
        assert_eq!(updated_root.data.recent_connections, vec![created_id]);
        assert!(created.data.objects.contains(&result.session_object_id));
        assert_eq!(
            created.data.long_description,
            format!(
                "Created in one session transaction with object {}.",
                result.object_ids[&pending_object]
            )
        );
        assert_eq!(
            service
                .get_file(&result.object_ids[&pending_object].to_string())
                .unwrap()
                .bytes,
            b"attachment"
        );
        let archive = service
            .database
            .get_object(result.session_object_id)
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&archive).unwrap()["object"],
            result.object_ids[&pending_object].to_string()
        );
        drop(service);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn session_commit_preserves_legacy_node_text_outside_live_kennedy_policy() {
        let directory = std::env::temp_dir().join(format!(
            "kennedy-session-legacy-node-text-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let identity = directory.join("users.sqlite3");
        let (database, roots) = initialize(&directory.join("kweb"), config(), &identity).unwrap();
        let service = Service::new(database, roots, &identity).unwrap();
        let pending_node = "pending:7".to_owned();
        let long_description = std::iter::repeat_n("a", 3_001)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(long_description.chars().count() > 5_000);
        assert!(long_description.split_whitespace().count() > 1_000);

        let result = service
            .commit_session(CommitRequest {
                idempotency_key: "legacy-node-text".into(),
                author: "test-model".into(),
                source_created_at: DateTime::parse_from_rfc3339("2026-07-23T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                archive: b"{\"session\":\"sealed-before-policy-change\"}".to_vec(),
                objects: BTreeMap::new(),
                creates: BTreeMap::from([(
                    pending_node.clone(),
                    PlannedNode {
                        short_name: String::new(),
                        short_description: "x".repeat(201),
                        long_description: long_description.clone(),
                        owner: "self".into(),
                        fixed_connections: Vec::new(),
                        recent_connections: Vec::new(),
                        objects: Vec::new(),
                        attach_session_archive: true,
                    },
                )]),
                updates: BTreeMap::new(),
            })
            .unwrap();
        let created = service
            .database
            .get_node(result.node_ids[&pending_node])
            .unwrap();
        assert_eq!(created.data.short_name, "");
        assert_eq!(created.data.short_description.chars().count(), 201);
        assert_eq!(created.data.long_description, long_description);

        drop(service);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn session_commit_replay_returns_the_original_receipt_without_a_second_transaction() {
        let directory = std::env::temp_dir().join(format!(
            "kennedy-session-commit-replay-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let identity = directory.join("users.sqlite3");
        let kweb = directory.join("kweb");
        let (database, roots) = initialize(&kweb, config(), &identity).unwrap();
        let service = Service::new(database, roots, &identity).unwrap();
        let input = CommitRequest {
            idempotency_key: "session-replay-test".into(),
            author: "test-model".into(),
            source_created_at: DateTime::parse_from_rfc3339("2026-07-23T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            archive: b"{\"header\":{},\"events\":[]}".to_vec(),
            objects: BTreeMap::new(),
            creates: BTreeMap::new(),
            updates: BTreeMap::new(),
        };
        let first = service.commit_session(input.clone()).unwrap();
        let committed_length = std::fs::metadata(kweb.join("transactions.kwl"))
            .unwrap()
            .len();
        service
            .receipts
            .lock()
            .unwrap()
            .execute(
                "UPDATE kmap_session_commit_receipts
                 SET result_json=NULL,committed_at=NULL
                 WHERE session_id='session-replay-test'",
                [],
            )
            .unwrap();
        let replay = service.commit_session(input.clone()).unwrap();
        assert_eq!(replay.transaction_id, None);
        assert_eq!(replay.session_object_id, first.session_object_id);
        assert_eq!(replay.node_ids, first.node_ids);
        assert_eq!(replay.object_ids, first.object_ids);
        assert_eq!(
            std::fs::metadata(kweb.join("transactions.kwl"))
                .unwrap()
                .len(),
            committed_length
        );
        assert_eq!(service.commit_session(input).unwrap(), replay);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
