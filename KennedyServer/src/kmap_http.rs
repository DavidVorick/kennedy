use std::{collections::HashSet, path::Path as FilePath, str::FromStr};

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
use chrono::Utc;
use kcode_kmap::{ErrorKind as KmapErrorKind, Kmap};
use kcode_kweb_db::{
    Config, Error as KwebError, KwebDb, Node, NodeData, NodeId, ObjectId, Owner, Provenance,
};
use kcode_server_object_envelopes::{
    StoredFile, StoredProvenance, decode_file, decode_provenance, sanitize_file_name,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use tower_http::trace::TraceLayer;

const MAX_REQUEST_BYTES: usize = 128 * 1024 * 1024;
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
    kmap: Kmap,
    roots: SystemRoots,
}

impl Service {
    pub(crate) fn new(kmap: Kmap, roots: SystemRoots) -> Self {
        Self { kmap, roots }
    }

    pub(crate) fn get_file(&self, id: &str) -> Result<StoredFile, ApiError> {
        let id = parse_object_id(id)?;
        let bytes = self.kmap.get_object(id)?;
        decode_file(id, bytes).map_err(ApiError::internal)
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

impl From<kcode_kmap::Error> for ApiError {
    fn from(error: kcode_kmap::Error) -> Self {
        match error.kind() {
            KmapErrorKind::InvalidInput => Self::invalid(error.to_string()),
            KmapErrorKind::NotFound => Self::not_found(error.to_string()),
            KmapErrorKind::Conflict => Self::conflict(error.to_string()),
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

pub(crate) fn initialize(
    kweb_root: &FilePath,
    config: Config,
    identity_database: &FilePath,
) -> anyhow::Result<(Kmap, SystemRoots)> {
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
                .create_node(root_data(
                    "Initial User Root",
                    "The root of the primary user's Kmap knowledge.",
                    "This root anchors durable knowledge associated with the primary Kennedy user.",
                ))
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
    let kmap = Kmap::open(database, identity_database)
        .map_err(anyhow::Error::new)
        .context("opening Kmap application service")?;
    Ok((kmap, roots))
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
    state.kmap.get_node(state.roots.user)?;
    state.kmap.get_node(state.roots.kennedy)?;
    Ok(Json(json!({"service":"kmap","status":"ok"})))
}

async fn get_roots(State(state): State<Service>) -> Json<Value> {
    Json(json!({
        "user_root_node_id":state.roots.user.to_string(),
        "kennedy_root_node_id":state.roots.kennedy.to_string(),
    }))
}

async fn get_provenance(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_object_id(&id)?;
    let provenance = load_provenance(&state.kmap, id)?;
    Ok(Json(provenance_response(&provenance)))
}

async fn get_session_archive(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_object_id(&id)?;
    let bytes = state.kmap.get_object(id)?;
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

async fn get_node(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_node_id(&id)?;
    let node = state.kmap.get_node(id)?;
    Ok(Json(node_response(&state.kmap, &node)?))
}

async fn get_history(
    State(state): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_node_id(&id)?;
    let history = state.kmap.get_node_history(id)?;
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

fn node_response(kmap: &Kmap, node: &Node) -> Result<Value, ApiError> {
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
        let connection = kmap.get_node(connection_id)?;
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

fn load_provenance(kmap: &Kmap, id: ObjectId) -> Result<StoredProvenance, ApiError> {
    let bytes = kmap.get_object(id)?;
    decode_provenance(&bytes)
        .map_err(|error| ApiError::internal(format!("invalid provenance object {id}: {error}")))
}

fn parse_node_id(value: &str) -> Result<NodeId, ApiError> {
    NodeId::from_str(value).map_err(ApiError::from)
}

fn parse_object_id(value: &str) -> Result<ObjectId, ApiError> {
    ObjectId::from_str(value).map_err(ApiError::from)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use kcode_kweb_db::{NoopGossip, WriterId};
    use kcode_server_object_envelopes::encode_file;

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
    fn initializes_canonical_system_roots_and_hands_database_to_kmap() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-kweb-http-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let (kmap, roots) = initialize(
            &directory.join("kweb"),
            config(),
            &directory.join("users.sqlite3"),
        )
        .unwrap();
        assert_eq!(roots.user.to_string().len(), 8);
        assert_eq!(roots.kennedy.to_string().len(), 8);
        kmap.get_node(roots.user).unwrap();
        kmap.get_node(roots.kennedy).unwrap();
        drop(kmap);
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
}
