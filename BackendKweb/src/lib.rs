use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use rand::RngCore;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::{services::ServeDir, trace::TraceLayer};

const MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const PROVENANCE_IDEMPOTENCY_MIGRATION: &str =
    include_str!("../migrations/002_provenance_idempotency.sql");
const SYSTEM_ROOTS_MIGRATION: &str = include_str!("../migrations/003_system_roots.sql");
const MODEL_ATTRIBUTION_MIGRATION: &str = include_str!("../migrations/004_model_attribution.sql");
const NODE_OWNERSHIP_MIGRATION: &str = include_str!("../migrations/005_node_ownership.sql");
const MAX_REQUEST_BYTES: usize = 128 * 1024 * 1024;
const FIXED_SLOT_1_ORDER: i64 = -1;
const FIXED_SLOT_2_ORDER: i64 = -2;
const FIXED_SLOT_3_ORDER: i64 = -3;
#[derive(Clone, Debug)]
pub struct Config {
    pub bind: String,
    pub database: PathBuf,
    pub frontend_dir: PathBuf,
    pub system_prompts_dir: PathBuf,
    pub active_limit: usize,
    pub fanout_limit: usize,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    prompts_dir: PathBuf,
    active_limit: usize,
    fanout_limit: usize,
}

async fn prevent_stale_frontend_assets(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
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
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error = %error, "kweb request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: "An unexpected database error occurred.".into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error": {"code": self.code, "message": self.message}})),
        )
            .into_response()
    }
}

#[derive(Clone, Serialize)]
struct ConnectionSummary {
    id: String,
    short_name: String,
    short_description: String,
}

#[derive(Clone, Serialize)]
struct FixedConnectionSummary {
    id: String,
    short_name: String,
    short_description: String,
    slot: i64,
}

#[derive(Clone, Serialize)]
struct KnowledgeNode {
    id: String,
    short_name: String,
    short_description: String,
    long_description: String,
    last_modified_by: String,
    owner_root_node_id: Option<String>,
    fixed_connections: Vec<FixedConnectionSummary>,
    active_connections: Vec<ConnectionSummary>,
    fanout_connections: Vec<ConnectionSummary>,
    history_head_id: Option<String>,
}

#[derive(Deserialize)]
struct ProvenanceInput {
    data: String,
    source: String,
    source_created_at: String,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
struct CreateNodeInput {
    provenance_id: String,
    model_attribution: String,
    parent_node_ids: Vec<String>,
    short_name: String,
    short_description: String,
    long_description: String,
    owner_root_node_id: String,
}

#[derive(Deserialize)]
struct UpdateNodeInput {
    provenance_id: String,
    model_attribution: String,
    short_name: String,
    short_description: String,
    long_description: String,
    owner_root_node_id: String,
}

#[derive(Deserialize)]
struct ConnectInput {
    node_ids: Vec<String>,
    model_attribution: String,
}

#[derive(Deserialize)]
struct ConsolidateFanoutInput {
    parent_node_id: String,
    aggregator_node_id: String,
    fanout_node_ids: Vec<String>,
    model_attribution: String,
}

#[derive(Deserialize)]
struct SetFixedConnectionInput {
    parent_node_id: String,
    child_node_id: Option<String>,
    slot: i64,
    model_attribution: String,
}

#[derive(Deserialize)]
struct LegacyAssignTaskInput {
    parent_node_id: String,
    child_node_id: Option<String>,
    priority: String,
    model_attribution: String,
}

#[derive(Deserialize)]
struct BootstrapNodeInput {
    node_id: String,
    #[serde(default)]
    short_name: Option<String>,
}

pub async fn serve(config: Config) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    serve_with_listener(config, listener).await
}

pub async fn serve_with_listener(
    config: Config,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    if config.active_limit == 0 {
        anyhow::bail!("active_limit must be positive");
    }
    if config.fanout_limit == 0 {
        anyhow::bail!("fanout_limit must be positive");
    }
    let mut connection = Connection::open(&config.database)
        .with_context(|| format!("opening {}", config.database.display()))?;
    configure_database(&connection)?;
    connection
        .execute_batch(MIGRATION)
        .context("applying schema migration")?;
    connection
        .execute_batch(PROVENANCE_IDEMPOTENCY_MIGRATION)
        .context("applying provenance idempotency migration")?;
    connection
        .execute_batch(SYSTEM_ROOTS_MIGRATION)
        .context("applying system roots migration")?;
    connection
        .execute_batch(MODEL_ATTRIBUTION_MIGRATION)
        .context("applying model attribution migration")?;
    migrate_node_ownership(&connection).context("applying node ownership migration")?;
    bootstrap(&mut connection)?;
    let state = AppState {
        db: Arc::new(Mutex::new(connection)),
        prompts_dir: config.system_prompts_dir,
        active_limit: config.active_limit,
        fanout_limit: config.fanout_limit,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/user", get(get_user))
        .route("/api/v1/nodes/{node_id}", get(get_node).put(update_node))
        .route("/api/v1/nodes/{node_id}/context", get(get_node_context))
        .route("/api/v1/nodes/{node_id}/history", get(get_history))
        .route("/api/v1/nodes", post(create_node))
        .route("/api/v1/nodes/bootstrap", post(bootstrap_node))
        .route("/api/v1/provenance", post(create_provenance))
        .route("/api/v1/provenance/{provenance_id}", get(get_provenance))
        .route(
            "/api/v1/connections/consolidate-fanout",
            post(consolidate_fanout),
        )
        .route("/api/v1/connections", post(connect_nodes))
        .route("/api/v1/fixed-connections", post(set_fixed_connection))
        .route("/api/v1/tasks", post(assign_task_compatibility))
        .route("/system-prompts/{filename}", get(get_prompt))
        .fallback_service(ServeDir::new(config.frontend_dir).append_index_html_on_directories(true))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(TraceLayer::new_for_http())
        .layer(middleware::map_response(prevent_stale_frontend_assets))
        .with_state(state);
    tracing::info!(address = %config.bind, "Kweb ready");
    axum::serve(listener, app).await?;
    Ok(())
}

fn configure_database(db: &Connection) -> anyhow::Result<()> {
    db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
    Ok(())
}

fn migrate_node_ownership(db: &Connection) -> anyhow::Result<()> {
    let columns = db
        .prepare("PRAGMA table_info(knowledge_nodes)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|column| column == "owner_root_node_id") {
        db.execute_batch(NODE_OWNERSHIP_MIGRATION)?;
    }
    db.execute_batch(
        "CREATE INDEX IF NOT EXISTS knowledge_nodes_by_owner ON knowledge_nodes(owner_root_node_id);",
    )?;
    db.execute(
        "UPDATE knowledge_nodes SET owner_root_node_id=id WHERE id IN (SELECT knowledge_node_id FROM kmap_roots) AND owner_root_node_id IS NULL",
        [],
    )?;
    Ok(())
}

fn new_id() -> Vec<u8> {
    let mut id = vec![0_u8; 20];
    rand::rng().fill_bytes(&mut id);
    id
}

fn decode_id(value: &str) -> Result<Vec<u8>, ApiError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(ApiError::bad(
            "Identifiers must be 40 lowercase hexadecimal characters.",
        ));
    }
    hex::decode(value).map_err(|_| ApiError::bad("Invalid identifier."))
}

fn validate_node_text(name: &str, short: &str, long: &str) -> Result<(String, String), ApiError> {
    let name = name.trim().to_string();
    let short = short.trim().to_string();
    let name_len = name.chars().count();
    if !(4..=50).contains(&name_len) {
        return Err(ApiError::bad("Short name must contain 4 to 50 characters."));
    }
    if short.chars().count() > 200 {
        return Err(ApiError::bad(
            "Short description must not exceed 200 characters.",
        ));
    }
    if long.split_whitespace().count() > 1000 {
        return Err(ApiError::bad(
            "Long description must not exceed 1000 words.",
        ));
    }
    Ok((name, short))
}

fn validate_model_attribution(value: &str) -> Result<String, ApiError> {
    let value = value.trim().to_string();
    if value.is_empty() || value.chars().count() > 200 {
        return Err(ApiError::bad(
            "model_attribution must contain 1 to 200 characters.",
        ));
    }
    Ok(value)
}

fn set_model_attribution(
    tx: &Transaction<'_>,
    node_ids: &[Vec<u8>],
    model_attribution: &str,
) -> rusqlite::Result<()> {
    for node_id in node_ids {
        tx.execute(
            "INSERT INTO knowledge_node_model_attribution(knowledge_node_id,last_modified_by) VALUES(?1,?2) ON CONFLICT(knowledge_node_id) DO UPDATE SET last_modified_by=excluded.last_modified_by",
            params![node_id, model_attribution],
        )?;
    }
    Ok(())
}

fn insert_bootstrap_node(
    tx: &Transaction<'_>,
    provenance_id: &[u8],
    short_name: &str,
    short_description: &str,
    long_description: &str,
    is_user_root: bool,
) -> anyhow::Result<Vec<u8>> {
    let node_id = new_id();
    let history_id = new_id();
    tx.execute(
        "INSERT INTO knowledge_nodes(id,short_name,short_description,long_description,is_user_root,owner_root_node_id) VALUES(?1,?2,?3,?4,?5,?1)",
        params![node_id, short_name, short_description, long_description, is_user_root],
    )?;
    tx.execute("INSERT INTO data_history_nodes(id,knowledge_node_id,previous_history_id,provenance_id) VALUES(?1,?2,NULL,?3)", params![history_id, node_id, provenance_id])?;
    tx.execute(
        "UPDATE knowledge_nodes SET history_head_id=?1 WHERE id=?2",
        params![history_id, &node_id],
    )?;
    set_model_attribution(tx, std::slice::from_ref(&node_id), "system-bootstrap")?;
    Ok(node_id)
}

fn bootstrap(db: &mut Connection) -> anyhow::Result<()> {
    let tx = db.transaction()?;
    let mut user_id: Option<Vec<u8>> = tx
        .query_row(
            "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let mut kennedy_id: Option<Vec<u8>> = tx
        .query_row(
            "SELECT knowledge_node_id FROM kmap_roots WHERE role='kennedy'",
            [],
            |row| row.get(0),
        )
        .optional()?;

    if user_id.is_none() || kennedy_id.is_none() {
        let provenance_id = new_id();
        tx.execute(
            "INSERT INTO data_provenance_nodes(id,data,source,source_created_at) VALUES(?1,?2,?3,?4)",
            params![
                provenance_id,
                "Initial Kmap root bootstrap.",
                "bootstrap",
                Utc::now().to_rfc3339()
            ],
        )?;
        if user_id.is_none() {
            user_id = Some(insert_bootstrap_node(
                &tx,
                &provenance_id,
                "Initial User Root",
                "",
                "",
                true,
            )?);
        }
        if kennedy_id.is_none() {
            kennedy_id = Some(insert_bootstrap_node(
                &tx,
                &provenance_id,
                "Kennedy's Root",
                "The root of Kennedy's own Kmap knowledge.",
                "This is Kennedy's root node. It anchors Kennedy's own durable knowledge and learned lessons in the Kmap.",
                false,
            )?);
        }
    }

    tx.execute(
        "INSERT INTO kmap_roots(role,knowledge_node_id) VALUES('user',?1) ON CONFLICT(role) DO UPDATE SET knowledge_node_id=excluded.knowledge_node_id",
        [user_id.as_ref().expect("bootstrap creates user root")],
    )?;
    tx.execute(
        "INSERT INTO kmap_roots(role,knowledge_node_id) VALUES('kennedy',?1) ON CONFLICT(role) DO UPDATE SET knowledge_node_id=excluded.knowledge_node_id",
        [kennedy_id.as_ref().expect("bootstrap creates Kennedy root")],
    )?;
    tx.commit()?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Response {
    match state
        .db
        .lock()
        .ok()
        .and_then(|db| db.query_row("SELECT 1", [], |r| r.get::<_, i64>(0)).ok())
    {
        Some(_) => Json(json!({"service":"kweb","status":"ok"})).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"service":"kweb","status":"unavailable"})),
        )
            .into_response(),
    }
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
        return Err(ApiError::not_found("Prompt manual not found."));
    }
    let body = match tokio::fs::read_to_string(state.prompts_dir.join(&filename)).await {
        Ok(body) => body,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ApiError::not_found("Prompt manual not found."));
        }
        Err(error) => return Err(ApiError::internal(error)),
    };
    Ok(([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response())
}

async fn get_user(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let root = |role: &str| {
        db.query_row(
            "SELECT knowledge_node_id FROM kmap_roots WHERE role=?1",
            [role],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map(hex::encode)
        .map_err(ApiError::internal)
    };
    let user_root_node_id = root("user")?;
    let kennedy_root_node_id = root("kennedy")?;
    Ok(Json(json!({
        "name":"Legacy local user root",
        "root_node_id":user_root_node_id,
        "user_root_node_id":user_root_node_id,
        "kennedy_root_node_id":kennedy_root_node_id
    })))
}

async fn bootstrap_node(
    State(state): State<AppState>,
    Json(input): Json<BootstrapNodeInput>,
) -> Result<(StatusCode, Json<KnowledgeNode>), ApiError> {
    let node_id = decode_id(&input.node_id)?;
    let requested_name = input.short_name.unwrap_or_else(|| "User Root".into());
    let (short_name, _) = validate_node_text(&requested_name, "", "")?;
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    match fetch_node(&db, &node_id) {
        Ok(_) => {
            db.execute(
                "UPDATE knowledge_nodes SET owner_root_node_id=id WHERE id=?1 AND owner_root_node_id IS NULL",
                [&node_id],
            )
            .map_err(ApiError::internal)?;
            return Ok((StatusCode::OK, Json(fetch_node(&db, &node_id)?)));
        }
        Err(error) if error.status != StatusCode::NOT_FOUND => return Err(error),
        Err(_) => {}
    }
    let tx = db.transaction().map_err(ApiError::internal)?;
    let provenance_id = new_id();
    let history_id = new_id();
    tx.execute(
        "INSERT INTO data_provenance_nodes(id,data,source,source_created_at) VALUES(?1,?2,?3,?4)",
        params![
            provenance_id,
            "Automatically provisioned blank Kmap root node.",
            "system-bootstrap",
            Utc::now().to_rfc3339()
        ],
    )
    .map_err(ApiError::internal)?;
    tx.execute(
        "INSERT INTO knowledge_nodes(id,short_name,short_description,long_description,is_user_root,owner_root_node_id) VALUES(?1,?2,'','',0,?1)",
        params![node_id, short_name],
    )
    .map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
            ApiError::conflict("The requested bootstrap node identifier already exists.")
        } else {
            ApiError::internal(error)
        }
    })?;
    tx.execute(
        "INSERT INTO data_history_nodes(id,knowledge_node_id,previous_history_id,provenance_id) VALUES(?1,?2,NULL,?3)",
        params![history_id, node_id, provenance_id],
    )
    .map_err(ApiError::internal)?;
    tx.execute(
        "UPDATE knowledge_nodes SET history_head_id=?1 WHERE id=?2",
        params![history_id, node_id],
    )
    .map_err(ApiError::internal)?;
    set_model_attribution(&tx, std::slice::from_ref(&node_id), "system-bootstrap")
        .map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    let node = fetch_node(&db, &node_id)?;
    Ok((StatusCode::CREATED, Json(node)))
}

fn fetch_summaries(
    db: &Connection,
    source: &[u8],
    tier: &str,
) -> rusqlite::Result<Vec<ConnectionSummary>> {
    let sql = if tier == "fanout" {
        "SELECT n.id,n.short_name,n.short_description FROM knowledge_connections c JOIN knowledge_nodes n ON n.id=c.target_node_id WHERE c.source_node_id=?1 AND c.tier=?2 AND c.activation_order>=0 ORDER BY c.activation_order DESC"
    } else {
        "SELECT n.id,n.short_name,n.short_description FROM knowledge_connections c JOIN knowledge_nodes n ON n.id=c.target_node_id WHERE c.source_node_id=?1 AND c.tier=?2 ORDER BY c.activation_order DESC"
    };
    let mut stmt = db.prepare(sql)?;
    stmt.query_map(params![source, tier], |row| {
        Ok(ConnectionSummary {
            id: hex::encode(row.get::<_, Vec<u8>>(0)?),
            short_name: row.get(1)?,
            short_description: row.get(2)?,
        })
    })?
    .collect()
}

fn fixed_slot(order: i64) -> Option<i64> {
    match order {
        FIXED_SLOT_1_ORDER => Some(1),
        FIXED_SLOT_2_ORDER => Some(2),
        FIXED_SLOT_3_ORDER => Some(3),
        _ => None,
    }
}

fn fixed_order(slot: i64) -> Result<i64, ApiError> {
    match slot {
        1 => Ok(FIXED_SLOT_1_ORDER),
        2 => Ok(FIXED_SLOT_2_ORDER),
        3 => Ok(FIXED_SLOT_3_ORDER),
        _ => Err(ApiError::bad("Fixed connection slot must be 1, 2, or 3.")),
    }
}

fn fetch_fixed_summaries(
    db: &Connection,
    source: &[u8],
) -> rusqlite::Result<Vec<FixedConnectionSummary>> {
    let mut stmt = db.prepare("SELECT n.id,n.short_name,n.short_description,c.activation_order FROM knowledge_connections c JOIN knowledge_nodes n ON n.id=c.target_node_id WHERE c.source_node_id=?1 AND c.tier='fanout' AND c.activation_order BETWEEN ?2 AND ?3 ORDER BY c.activation_order DESC")?;
    stmt.query_map(
        params![source, FIXED_SLOT_3_ORDER, FIXED_SLOT_1_ORDER],
        |row| {
            let order = row.get::<_, i64>(3)?;
            Ok(FixedConnectionSummary {
                id: hex::encode(row.get::<_, Vec<u8>>(0)?),
                short_name: row.get(1)?,
                short_description: row.get(2)?,
                slot: fixed_slot(order).unwrap_or(0),
            })
        },
    )?
    .collect()
}

fn fetch_node(db: &Connection, id: &[u8]) -> Result<KnowledgeNode, ApiError> {
    let core = db.query_row("SELECT n.short_name,n.short_description,n.long_description,n.history_head_id,COALESCE(a.last_modified_by,'legacy-unknown'),n.owner_root_node_id FROM knowledge_nodes n LEFT JOIN knowledge_node_model_attribution a ON a.knowledge_node_id=n.id WHERE n.id=?1", [id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<Vec<u8>>>(3)?,r.get::<_,String>(4)?,r.get::<_,Option<Vec<u8>>>(5)?))).optional().map_err(ApiError::internal)?;
    let Some((
        short_name,
        short_description,
        long_description,
        history,
        last_modified_by,
        owner_root_node_id,
    )) = core
    else {
        return Err(ApiError::not_found("Knowledge node not found."));
    };
    Ok(KnowledgeNode {
        id: hex::encode(id),
        short_name,
        short_description,
        long_description,
        last_modified_by,
        owner_root_node_id: owner_root_node_id.map(hex::encode),
        fixed_connections: fetch_fixed_summaries(db, id).map_err(ApiError::internal)?,
        active_connections: fetch_summaries(db, id, "active").map_err(ApiError::internal)?,
        fanout_connections: fetch_summaries(db, id, "fanout").map_err(ApiError::internal)?,
        history_head_id: history.map(hex::encode),
    })
}

async fn get_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<KnowledgeNode>, ApiError> {
    let id = decode_id(&node_id)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    Ok(Json(fetch_node(&db, &id)?))
}

async fn get_node_context(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = decode_id(&node_id)?;
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    require_exists(&tx, "knowledge_nodes", &id, "Knowledge node")?;
    enforce_connection_limits_in_tx(&tx, &id, state.active_limit, state.fanout_limit, "LoadNode")?;
    tx.commit().map_err(ApiError::internal)?;
    let requested = fetch_node(&db, &id)?;
    let active_ids: Vec<Vec<u8>> = {
        let mut stmt = db.prepare("SELECT target_node_id FROM knowledge_connections WHERE source_node_id=?1 AND tier='active' ORDER BY activation_order DESC").map_err(ApiError::internal)?;
        stmt.query_map([&id], |r| r.get(0))
            .map_err(ApiError::internal)?
            .collect::<Result<_, _>>()
            .map_err(ApiError::internal)?
    };
    let active = active_ids
        .iter()
        .map(|id| fetch_node(&db, id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(
        json!({"requested_node":requested,"active_connection_nodes":active}),
    ))
}

async fn create_provenance(
    State(state): State<AppState>,
    Json(input): Json<ProvenanceInput>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let source = input.source.trim();
    if source.is_empty() || source.chars().count() > 200 {
        return Err(ApiError::bad("Source must contain 1 to 200 characters."));
    }
    DateTime::parse_from_rfc3339(&input.source_created_at)
        .map_err(|_| ApiError::bad("source_created_at must be an RFC 3339 timestamp."))?;
    let idempotency_key = input.idempotency_key.map(|key| key.trim().to_owned());
    if idempotency_key
        .as_deref()
        .is_some_and(|key| key.is_empty() || key.chars().count() > 200)
    {
        return Err(ApiError::bad(
            "idempotency_key must contain 1 to 200 characters when supplied.",
        ));
    }
    let id = new_id();
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    if let Some(key) = idempotency_key.as_deref() {
        let existing: Option<Vec<u8>> = tx
            .query_row(
                "SELECT provenance_id FROM provenance_idempotency WHERE idempotency_key=?1",
                [key],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if let Some(existing) = existing {
            tx.commit().map_err(ApiError::internal)?;
            return Ok((StatusCode::OK, Json(json!({"id":hex::encode(existing)}))));
        }
    }
    tx.execute(
        "INSERT INTO data_provenance_nodes(id,data,source,source_created_at) VALUES(?1,?2,?3,?4)",
        params![id, input.data, source, input.source_created_at],
    )
    .map_err(ApiError::internal)?;
    if let Some(key) = idempotency_key {
        tx.execute(
            "INSERT INTO provenance_idempotency(idempotency_key,provenance_id) VALUES(?1,?2)",
            params![key, id],
        )
        .map_err(ApiError::internal)?;
    }
    tx.commit().map_err(ApiError::internal)?;
    Ok((StatusCode::CREATED, Json(json!({"id":hex::encode(id)}))))
}

async fn get_provenance(
    State(state): State<AppState>,
    Path(provenance_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = decode_id(&provenance_id)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let row = db
        .query_row(
            "SELECT data,source,source_created_at FROM data_provenance_nodes WHERE id=?1",
            [&id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((data, source, source_created_at)) = row else {
        return Err(ApiError::not_found("Provenance node not found."));
    };
    Ok(Json(
        json!({"id":provenance_id,"data":data,"source":source,"source_created_at":source_created_at}),
    ))
}

fn require_exists(
    tx: &Transaction<'_>,
    table: &str,
    id: &[u8],
    label: &str,
) -> Result<(), ApiError> {
    let sql = format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id=?1)");
    let exists: bool = tx
        .query_row(&sql, [id], |r| r.get(0))
        .map_err(ApiError::internal)?;
    if !exists {
        return Err(ApiError::not_found(format!("{label} not found.")));
    }
    Ok(())
}

fn require_owner_root(tx: &Transaction<'_>, id: &[u8]) -> Result<(), ApiError> {
    let self_owned: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM knowledge_nodes WHERE id=?1 AND owner_root_node_id=id)",
            [id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    if !self_owned {
        return Err(ApiError::bad(
            "owner_root_node_id must identify a self-owned Kennedy, user, or group root.",
        ));
    }
    Ok(())
}

fn validate_distinct_ids(
    values: &[String],
    min: usize,
    label: &str,
) -> Result<Vec<Vec<u8>>, ApiError> {
    if values.len() < min {
        return Err(ApiError::bad(format!(
            "{label} requires at least {min} distinct identifiers."
        )));
    }
    let ids = values
        .iter()
        .map(|v| decode_id(v))
        .collect::<Result<Vec<_>, _>>()?;
    let unique: HashSet<&Vec<u8>> = ids.iter().collect();
    if unique.len() != ids.len() {
        return Err(ApiError::bad(format!(
            "{label} identifiers must be distinct."
        )));
    }
    Ok(ids)
}

fn enforce_connection_limits_in_tx(
    tx: &Transaction<'_>,
    source: &[u8],
    active_limit: usize,
    fanout_limit: usize,
    operation: &str,
) -> Result<usize, ApiError> {
    let active_count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM knowledge_connections WHERE source_node_id=?1 AND tier='active'",
            [source],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    let overflow = (active_count as usize).saturating_sub(active_limit);
    if overflow == 0 {
        return Ok(0);
    }

    let fanout_count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM knowledge_connections WHERE source_node_id=?1 AND tier='fanout' AND activation_order>=0",
            [source],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    if fanout_count as usize + overflow > fanout_limit {
        return Err(ApiError::conflict(format!(
            "{operation} would exceed the fanout connection limit of {fanout_limit}."
        )));
    }

    tx.execute(
        "UPDATE knowledge_connections SET tier='fanout' WHERE source_node_id=?1 AND target_node_id IN (SELECT target_node_id FROM knowledge_connections WHERE source_node_id=?1 AND tier='active' ORDER BY activation_order ASC LIMIT ?2)",
        params![source, overflow as i64],
    )
    .map_err(ApiError::internal)?;
    Ok(overflow)
}

fn connect_in_tx(
    tx: &Transaction<'_>,
    ids: &[Vec<u8>],
    active_limit: usize,
    fanout_limit: usize,
) -> Result<(), ApiError> {
    for id in ids {
        require_exists(tx, "knowledge_nodes", id, "Knowledge node")?;
    }
    let mut order: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(activation_order),0) FROM knowledge_connections WHERE activation_order>=0",
            [],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;
    for source in ids {
        for target in ids {
            if source == target {
                continue;
            }
            order += 1;
            tx.execute("INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'active',?3) ON CONFLICT(source_node_id,target_node_id) DO UPDATE SET tier='active',activation_order=excluded.activation_order", params![source,target,order]).map_err(ApiError::internal)?;
        }
    }
    for source in ids {
        enforce_connection_limits_in_tx(tx, source, active_limit, fanout_limit, "ConnectNodes")?;
    }
    Ok(())
}

async fn connect_nodes(
    State(state): State<AppState>,
    Json(input): Json<ConnectInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ids = validate_distinct_ids(&input.node_ids, 2, "ConnectNodes")?;
    let model_attribution = validate_model_attribution(&input.model_attribution)?;
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    connect_in_tx(&tx, &ids, state.active_limit, state.fanout_limit)?;
    set_model_attribution(&tx, &ids, &model_attribution).map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    let nodes = ids
        .iter()
        .map(|id| fetch_node(&db, id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({"nodes":nodes})))
}

fn ordinary_fanout_exists(
    tx: &Transaction<'_>,
    source: &[u8],
    target: &[u8],
) -> Result<bool, ApiError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM knowledge_connections WHERE source_node_id=?1 AND target_node_id=?2 AND tier='fanout' AND activation_order>=0)",
        params![source, target],
        |row| row.get(0),
    )
    .map_err(ApiError::internal)
}

fn consolidate_fanout_in_tx(
    tx: &Transaction<'_>,
    parent: &[u8],
    aggregator: &[u8],
    fanout_ids: &[Vec<u8>],
    fanout_limit: usize,
) -> Result<(), ApiError> {
    require_exists(tx, "knowledge_nodes", parent, "Parent knowledge node")?;
    require_exists(
        tx,
        "knowledge_nodes",
        aggregator,
        "Aggregator knowledge node",
    )?;
    if parent == aggregator || fanout_ids.iter().any(|id| id == parent || id == aggregator) {
        return Err(ApiError::bad(
            "The parent, aggregator, and fanout identifiers must all be distinct.",
        ));
    }
    for id in fanout_ids {
        require_exists(tx, "knowledge_nodes", id, "Fanout knowledge node")?;
    }
    if !ordinary_fanout_exists(tx, parent, aggregator)? {
        return Err(ApiError::conflict(
            "The aggregator must already be a fanout connection of the parent.",
        ));
    }
    for id in fanout_ids {
        if !ordinary_fanout_exists(tx, parent, id)? {
            return Err(ApiError::conflict(
                "Every consolidated node must currently be a fanout connection of the parent.",
            ));
        }
    }

    let existing_count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM knowledge_connections WHERE source_node_id=?1 AND tier='fanout' AND activation_order>=0",
            [aggregator],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    let mut added = 0_usize;
    for id in fanout_ids {
        if !ordinary_fanout_exists(tx, aggregator, id)? {
            added += 1;
        }
    }
    if existing_count as usize + added > fanout_limit {
        return Err(ApiError::conflict(format!(
            "ConsolidateFanout would exceed the fanout connection limit of {fanout_limit}."
        )));
    }

    let mut order: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(activation_order),0) FROM knowledge_connections WHERE activation_order>=0",
            [],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    for id in fanout_ids {
        tx.execute(
            "DELETE FROM knowledge_connections WHERE source_node_id=?1 AND target_node_id=?2",
            params![parent, id],
        )
        .map_err(ApiError::internal)?;
        order += 1;
        tx.execute(
            "INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'fanout',?3) ON CONFLICT(source_node_id,target_node_id) DO UPDATE SET tier='fanout',activation_order=excluded.activation_order",
            params![aggregator, id, order],
        )
        .map_err(ApiError::internal)?;
    }
    Ok(())
}

async fn consolidate_fanout(
    State(state): State<AppState>,
    Json(input): Json<ConsolidateFanoutInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let parent = decode_id(&input.parent_node_id)?;
    let aggregator = decode_id(&input.aggregator_node_id)?;
    let fanout_ids =
        validate_distinct_ids(&input.fanout_node_ids, 1, "ConsolidateFanout fanout list")?;
    let model_attribution = validate_model_attribution(&input.model_attribution)?;
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    consolidate_fanout_in_tx(&tx, &parent, &aggregator, &fanout_ids, state.fanout_limit)?;
    let mut affected = vec![parent.clone(), aggregator.clone()];
    affected.extend(fanout_ids.iter().cloned());
    set_model_attribution(&tx, &affected, &model_attribution).map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    let visible = [&parent, &aggregator];
    let nodes = visible
        .into_iter()
        .map(|id| fetch_node(&db, id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({"nodes":nodes})))
}

async fn set_fixed_connection(
    State(state): State<AppState>,
    Json(input): Json<SetFixedConnectionInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let parent = decode_id(&input.parent_node_id)?;
    let child = input.child_node_id.as_deref().map(decode_id).transpose()?;
    let slot_order = fixed_order(input.slot)?;
    let model_attribution = validate_model_attribution(&input.model_attribution)?;
    if child.as_deref() == Some(parent.as_slice()) {
        return Err(ApiError::bad("A node cannot be its own fixed connection."));
    }
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    require_exists(&tx, "knowledge_nodes", &parent, "Parent knowledge node")?;
    if let Some(child) = child.as_deref() {
        require_exists(
            &tx,
            "knowledge_nodes",
            child,
            "Fixed-connection knowledge node",
        )?;
    }
    let mut replaced = fetch_fixed_summaries(&tx, &parent)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|connection| connection.slot == input.slot);
    if replaced.as_ref().is_some_and(|connection| {
        child
            .as_ref()
            .is_some_and(|id| connection.id == hex::encode(id))
    }) {
        replaced = None;
    }
    let mut affected = vec![parent.clone()];
    if let Some(child) = child.as_ref() {
        affected.push(child.clone());
    }
    if let Some(replaced_connection) = replaced.as_ref() {
        let replaced_id = decode_id(&replaced_connection.id)?;
        if !affected.contains(&replaced_id) {
            affected.push(replaced_id);
        }
    }
    tx.execute(
        "DELETE FROM knowledge_connections WHERE source_node_id=?1 AND tier='fanout' AND activation_order=?2",
        params![parent, slot_order],
    )
    .map_err(ApiError::internal)?;
    if let Some(child) = child {
        tx.execute(
            "INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'fanout',?3) ON CONFLICT(source_node_id,target_node_id) DO UPDATE SET tier='fanout',activation_order=excluded.activation_order",
            params![parent, child, slot_order],
        )
        .map_err(ApiError::internal)?;
    }
    set_model_attribution(&tx, &affected, &model_attribution).map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    let node = fetch_node(&db, &parent)?;
    Ok(Json(
        json!({"node":node,"replaced_fixed_connection":replaced}),
    ))
}

async fn assign_task_compatibility(
    state: State<AppState>,
    Json(input): Json<LegacyAssignTaskInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let slot = match input.priority.as_str() {
        "high" => 1,
        "medium" => 2,
        "low" => 3,
        _ => return Err(ApiError::bad("Task priority must be high, medium, or low.")),
    };
    set_fixed_connection(
        state,
        Json(SetFixedConnectionInput {
            parent_node_id: input.parent_node_id,
            child_node_id: input.child_node_id,
            slot,
            model_attribution: input.model_attribution,
        }),
    )
    .await
}

async fn create_node(
    State(state): State<AppState>,
    Json(input): Json<CreateNodeInput>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let provenance = decode_id(&input.provenance_id)?;
    let owner_root = decode_id(&input.owner_root_node_id)?;
    let model_attribution = validate_model_attribution(&input.model_attribution)?;
    let parents = validate_distinct_ids(&input.parent_node_ids, 1, "CreateNode parent list")?;
    let (name, short) = validate_node_text(
        &input.short_name,
        &input.short_description,
        &input.long_description,
    )?;
    let node_id = new_id();
    let history_id = new_id();
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    require_exists(&tx, "data_provenance_nodes", &provenance, "Provenance node")?;
    require_owner_root(&tx, &owner_root)?;
    for parent in &parents {
        require_exists(&tx, "knowledge_nodes", parent, "Parent knowledge node")?;
    }
    tx.execute("INSERT INTO knowledge_nodes(id,short_name,short_description,long_description,is_user_root,owner_root_node_id) VALUES(?1,?2,?3,?4,0,?5)",params![node_id,name,short,input.long_description,owner_root]).map_err(ApiError::internal)?;
    tx.execute("INSERT INTO data_history_nodes(id,knowledge_node_id,previous_history_id,provenance_id) VALUES(?1,?2,NULL,?3)",params![history_id,node_id,provenance]).map_err(ApiError::internal)?;
    tx.execute(
        "UPDATE knowledge_nodes SET history_head_id=?1 WHERE id=?2",
        params![history_id, node_id],
    )
    .map_err(ApiError::internal)?;
    let mut connected = vec![node_id.clone()];
    connected.extend(parents);
    connect_in_tx(&tx, &connected, state.active_limit, state.fanout_limit)?;
    set_model_attribution(&tx, &connected, &model_attribution).map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    let node = fetch_node(&db, &node_id)?;
    let nodes = connected
        .iter()
        .map(|id| fetch_node(&db, id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"node":node,"nodes":nodes,"history_node_id":hex::encode(history_id)})),
    ))
}

async fn update_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(input): Json<UpdateNodeInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = decode_id(&node_id)?;
    let provenance = decode_id(&input.provenance_id)?;
    let owner_root = decode_id(&input.owner_root_node_id)?;
    let model_attribution = validate_model_attribution(&input.model_attribution)?;
    let (name, short) = validate_node_text(
        &input.short_name,
        &input.short_description,
        &input.long_description,
    )?;
    let history_id = new_id();
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    require_exists(&tx, "data_provenance_nodes", &provenance, "Provenance node")?;
    require_owner_root(&tx, &owner_root)?;
    let previous: Option<Vec<u8>> = tx
        .query_row(
            "SELECT history_head_id FROM knowledge_nodes WHERE id=?1",
            [&id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("Knowledge node not found."))?;
    tx.execute("INSERT INTO data_history_nodes(id,knowledge_node_id,previous_history_id,provenance_id) VALUES(?1,?2,?3,?4)",params![history_id,id,previous,provenance]).map_err(ApiError::internal)?;
    tx.execute("UPDATE knowledge_nodes SET short_name=?1,short_description=?2,long_description=?3,history_head_id=?4,owner_root_node_id=?5 WHERE id=?6",params![name,short,input.long_description,history_id,owner_root,id]).map_err(ApiError::internal)?;
    set_model_attribution(&tx, std::slice::from_ref(&id), &model_attribution)
        .map_err(ApiError::internal)?;
    tx.commit().map_err(ApiError::internal)?;
    let node = fetch_node(&db, &id)?;
    Ok(Json(
        json!({"node":node,"history_node_id":hex::encode(history_id)}),
    ))
}

async fn get_history(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = decode_id(&node_id)?;
    let db = state.db.lock().map_err(ApiError::internal)?;
    let mut current: Option<Vec<u8>> = db
        .query_row(
            "SELECT history_head_id FROM knowledge_nodes WHERE id=?1",
            [&id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("Knowledge node not found."))?;
    let mut history = Vec::new();
    let mut seen = HashSet::new();
    while let Some(history_id) = current {
        if !seen.insert(history_id.clone()) {
            return Err(ApiError::conflict("History chain contains a cycle."));
        }
        let (previous,provenance): (Option<Vec<u8>>,Vec<u8>) = db.query_row("SELECT previous_history_id,provenance_id FROM data_history_nodes WHERE id=?1 AND knowledge_node_id=?2",params![history_id,id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(ApiError::internal)?;
        history.push(json!({"id":hex::encode(&history_id),"previous_history_id":previous.as_ref().map(hex::encode),"provenance_id":hex::encode(provenance)}));
        current = previous;
    }
    Ok(Json(json!({"node_id":node_id,"history":history})))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn db() -> Connection {
        let mut db = Connection::open_in_memory().unwrap();
        configure_database(&db).unwrap();
        db.execute_batch(MIGRATION).unwrap();
        db.execute_batch(PROVENANCE_IDEMPOTENCY_MIGRATION).unwrap();
        db.execute_batch(SYSTEM_ROOTS_MIGRATION).unwrap();
        db.execute_batch(MODEL_ATTRIBUTION_MIGRATION).unwrap();
        migrate_node_ownership(&db).unwrap();
        bootstrap(&mut db).unwrap();
        db
    }

    fn state(db: Connection) -> AppState {
        AppState {
            db: Arc::new(Mutex::new(db)),
            prompts_dir: PathBuf::new(),
            active_limit: 8,
            fanout_limit: 64,
        }
    }

    fn insert_node(db: &Connection, name: &str) -> Vec<u8> {
        let id = new_id();
        db.execute(
            "INSERT INTO knowledge_nodes(id,short_name,short_description,long_description) VALUES(?1,?2,'','')",
            params![id, name],
        )
        .unwrap();
        id
    }

    #[test]
    fn text_validation_enforces_contract() {
        assert!(validate_node_text("abc", "", "ok").is_err());
        assert!(validate_node_text("Valid", "", "word ".repeat(1001).as_str()).is_err());
        assert!(validate_node_text(" Valid Name ", " short ", "ok").is_ok());
        assert!(validate_model_attribution("").is_err());
        assert_eq!(
            validate_model_attribution(" gpt-5.6-sol-xhigh ").unwrap(),
            "gpt-5.6-sol-xhigh"
        );
    }

    #[test]
    fn connection_overflow_demotes_oldest() {
        let mut db = db();
        let tx = db.transaction().unwrap();
        let provenance: Vec<u8> = tx
            .query_row("SELECT id FROM data_provenance_nodes LIMIT 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        let mut ids = Vec::new();
        for i in 0..4 {
            let id = new_id();
            tx.execute("INSERT INTO knowledge_nodes(id,short_name,short_description,long_description) VALUES(?1,?2,'','')",params![id,format!("Node {i}")]).unwrap();
            ids.push(id);
        }
        connect_in_tx(&tx, &ids, 2, 60).unwrap();
        for id in &ids {
            let count:i64=tx.query_row("SELECT COUNT(*) FROM knowledge_connections WHERE source_node_id=?1 AND tier='active'",[id],|r|r.get(0)).unwrap();
            assert_eq!(count, 2);
        }
        let _ = provenance;
        tx.commit().unwrap();
    }

    #[test]
    fn connection_overflow_fails_before_exceeding_the_fanout_limit() {
        let mut db = db();
        let tx = db.transaction().unwrap();
        let ids = [
            insert_node(&tx, "Source Node"),
            insert_node(&tx, "Second Node"),
            insert_node(&tx, "Third Node"),
        ];
        let existing_fanout = insert_node(&tx, "Existing Fanout");
        tx.execute(
            "INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'fanout',1)",
            params![ids[0], existing_fanout],
        )
        .unwrap();
        let error = connect_in_tx(&tx, &ids, 1, 1).unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn load_context_lazily_normalizes_legacy_active_overflow() {
        let db = db();
        let source = insert_node(&db, "Legacy Source");
        let active = (0..12)
            .map(|index| {
                let target = insert_node(&db, &format!("Active Node {index}"));
                db.execute(
                    "INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'active',?3)",
                    params![source, target, index + 1],
                )
                .unwrap();
                target
            })
            .collect::<Vec<_>>();
        for index in 0..60 {
            let target = insert_node(&db, &format!("Fanout Node {index}"));
            db.execute(
                "INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'fanout',?3)",
                params![source, target, index + 100],
            )
            .unwrap();
        }
        let state = state(db);

        let Json(payload) = get_node_context(State(state.clone()), Path(hex::encode(&source)))
            .await
            .unwrap();

        assert_eq!(
            payload["requested_node"]["active_connections"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
        assert_eq!(
            payload["requested_node"]["fanout_connections"]
                .as_array()
                .unwrap()
                .len(),
            64
        );
        assert_eq!(
            payload["active_connection_nodes"].as_array().unwrap().len(),
            8
        );

        let db = state.db.lock().unwrap();
        let mut stmt = db
            .prepare(
                "SELECT target_node_id FROM knowledge_connections WHERE source_node_id=?1 AND tier='active' ORDER BY activation_order ASC",
            )
            .unwrap();
        let remaining = stmt
            .query_map([&source], |row| row.get::<_, Vec<u8>>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(remaining, active[4..]);
    }

    #[test]
    fn bootstrap_has_two_roots_with_valid_history() {
        let mut db = db();
        bootstrap(&mut db).unwrap();
        let count:i64=db.query_row("SELECT COUNT(*) FROM kmap_roots r JOIN knowledge_nodes n ON n.id=r.knowledge_node_id JOIN data_history_nodes h ON h.id=n.history_head_id JOIN data_provenance_nodes p ON p.id=h.provenance_id",[],|r|r.get(0)).unwrap();
        assert_eq!(count, 2);
        let distinct: i64 = db
            .query_row(
                "SELECT COUNT(DISTINCT knowledge_node_id) FROM kmap_roots",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(distinct, 2);
    }

    #[test]
    fn model_attribution_migration_backfills_legacy_nodes_idempotently() {
        let db = Connection::open_in_memory().unwrap();
        configure_database(&db).unwrap();
        db.execute_batch(MIGRATION).unwrap();
        db.execute_batch(PROVENANCE_IDEMPOTENCY_MIGRATION).unwrap();
        db.execute_batch(SYSTEM_ROOTS_MIGRATION).unwrap();
        let legacy = insert_node(&db, "Legacy Node");

        db.execute_batch(MODEL_ATTRIBUTION_MIGRATION).unwrap();
        db.execute_batch(MODEL_ATTRIBUTION_MIGRATION).unwrap();
        let attribution: String = db
            .query_row(
                "SELECT last_modified_by FROM knowledge_node_model_attribution WHERE knowledge_node_id=?1",
                [&legacy],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attribution, "legacy-unknown");
    }

    #[test]
    fn bootstrap_upgrades_an_existing_single_root_database() {
        let mut db = Connection::open_in_memory().unwrap();
        configure_database(&db).unwrap();
        db.execute_batch(MIGRATION).unwrap();
        db.execute_batch(PROVENANCE_IDEMPOTENCY_MIGRATION).unwrap();
        db.execute_batch(SYSTEM_ROOTS_MIGRATION).unwrap();
        db.execute_batch(MODEL_ATTRIBUTION_MIGRATION).unwrap();
        migrate_node_ownership(&db).unwrap();
        let tx = db.transaction().unwrap();
        let provenance_id = new_id();
        tx.execute(
            "INSERT INTO data_provenance_nodes(id,data,source,source_created_at) VALUES(?1,'Legacy bootstrap','bootstrap',?2)",
            params![provenance_id, Utc::now().to_rfc3339()],
        )
        .unwrap();
        let original_user = insert_bootstrap_node(
            &tx,
            &provenance_id,
            "David Vorick",
            "The user Kennedy assists.",
            "Existing user root.",
            true,
        )
        .unwrap();
        tx.commit().unwrap();

        bootstrap(&mut db).unwrap();
        let user: Vec<u8> = db
            .query_row(
                "SELECT knowledge_node_id FROM kmap_roots WHERE role='user'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let root_count: i64 = db
            .query_row("SELECT COUNT(*) FROM kmap_roots", [], |row| row.get(0))
            .unwrap();
        assert_eq!(user, original_user);
        assert_eq!(root_count, 2);
    }

    #[tokio::test]
    async fn user_metadata_exposes_both_root_ids() {
        let response = get_user(State(state(db()))).await.unwrap();
        assert_eq!(response.0["root_node_id"], response.0["user_root_node_id"]);
        assert_ne!(
            response.0["user_root_node_id"],
            response.0["kennedy_root_node_id"]
        );
    }

    #[tokio::test]
    async fn arbitrary_roots_are_bootstrapped_with_a_requested_label_and_idempotently() {
        let state = state(db());
        let node_id = hex::encode([0xabu8; 20]);
        let first = bootstrap_node(
            State(state.clone()),
            Json(BootstrapNodeInput {
                node_id: node_id.clone(),
                short_name: Some("Group Root".into()),
            }),
        )
        .await
        .unwrap();
        let second = bootstrap_node(
            State(state.clone()),
            Json(BootstrapNodeInput {
                node_id: node_id.clone(),
                short_name: Some("Group Root".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(first.0, StatusCode::CREATED);
        assert_eq!(second.0, StatusCode::OK);
        assert_eq!(first.1.id, node_id);
        assert_eq!(first.1.short_name, "Group Root");
        assert_eq!(first.1.short_description, "");
        assert_eq!(first.1.long_description, "");
        assert_eq!(first.1.last_modified_by, "system-bootstrap");
        assert_eq!(
            first.1.owner_root_node_id.as_deref(),
            Some(node_id.as_str())
        );
        let count: i64 = state
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM knowledge_nodes WHERE id=?1",
                [[0xabu8; 20].as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn audio_ingress_prompt_is_served_from_the_prompt_directory() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-prompts-{}", hex::encode(new_id())));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(
            directory.join("AudioIngressSession.txt"),
            "Audio ingress instructions.",
        )
        .unwrap();
        let mut prompt_state = state(db());
        prompt_state.prompts_dir = directory.clone();

        let response = get_prompt(
            State(prompt_state),
            Path("AudioIngressSession.txt".to_owned()),
        )
        .await
        .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        assert_eq!(body, "Audio ingress instructions.");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn prompt_route_rejects_non_prompt_names_without_a_second_filename_allowlist() {
        let missing = get_prompt(State(state(db())), Path("FutureMode.txt".to_owned()))
            .await
            .unwrap_err();
        let unsafe_name = get_prompt(State(state(db())), Path("README.md".to_owned()))
            .await
            .unwrap_err();

        assert_eq!(missing.status, StatusCode::NOT_FOUND);
        assert_eq!(unsafe_name.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn roots_self_own_while_legacy_non_roots_remain_unowned() {
        let db = db();
        let root: Vec<u8> = db
            .query_row(
                "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let node = fetch_node(&db, &root).unwrap();
        assert!(node.fixed_connections.is_empty());
        assert_eq!(node.owner_root_node_id, Some(hex::encode(&root)));
        assert_eq!(node.last_modified_by, "system-bootstrap");
        let legacy = insert_node(&db, "Legacy Memory");
        assert_eq!(fetch_node(&db, &legacy).unwrap().owner_root_node_id, None);
    }

    #[tokio::test]
    async fn descriptive_and_connection_mutations_update_model_attribution() {
        let db = db();
        let parent: Vec<u8> = db
            .query_row(
                "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let provenance: Vec<u8> = db
            .query_row("SELECT id FROM data_provenance_nodes LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let peer = insert_node(&db, "Peer Node");
        let state = state(db);

        let connected = connect_nodes(
            State(state.clone()),
            Json(ConnectInput {
                node_ids: vec![hex::encode(&parent), hex::encode(&peer)],
                model_attribution: "gpt-5.5-medium".into(),
            }),
        )
        .await
        .unwrap();
        assert!(
            connected.0["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|node| node["last_modified_by"] == "gpt-5.5-medium")
        );

        let created = create_node(
            State(state.clone()),
            Json(CreateNodeInput {
                provenance_id: hex::encode(&provenance),
                model_attribution: "gpt-5.6-sol-high".into(),
                parent_node_ids: vec![hex::encode(&parent)],
                owner_root_node_id: hex::encode(&parent),
                short_name: "Created Memory".into(),
                short_description: "Created by a model.".into(),
                long_description: "Durable model-attributed knowledge.".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(created.1.0["node"]["last_modified_by"], "gpt-5.6-sol-high");
        assert!(
            created.1.0["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|node| node["last_modified_by"] == "gpt-5.6-sol-high")
        );
        let created_id = created.1.0["node"]["id"].as_str().unwrap().to_string();

        let updated = update_node(
            State(state),
            Path(created_id),
            Json(UpdateNodeInput {
                provenance_id: hex::encode(provenance),
                model_attribution: "gpt-5.6-sol-xhigh".into(),
                owner_root_node_id: hex::encode(&parent),
                short_name: "Updated Memory".into(),
                short_description: "Updated by a model.".into(),
                long_description: "Updated durable model-attributed knowledge.".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(updated.0["node"]["last_modified_by"], "gpt-5.6-sol-xhigh");
    }

    #[tokio::test]
    async fn fixed_slots_can_be_assigned_replaced_and_cleared() {
        let db = db();
        let parent: Vec<u8> = db
            .query_row(
                "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let first = insert_node(&db, "First Fixed Node");
        let second = insert_node(&db, "Second Fixed Node");
        let state = state(db);

        let assigned = set_fixed_connection(
            State(state.clone()),
            Json(SetFixedConnectionInput {
                parent_node_id: hex::encode(&parent),
                child_node_id: Some(hex::encode(&first)),
                slot: 1,
                model_attribution: "gpt-test-low".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            assigned.0["node"]["fixed_connections"][0]["id"],
            hex::encode(&first)
        );
        assert_eq!(assigned.0["node"]["fixed_connections"][0]["slot"], 1);
        assert_eq!(assigned.0["node"]["fanout_connections"], json!([]));
        assert_eq!(assigned.0["node"]["last_modified_by"], "gpt-test-low");
        assert_eq!(
            fetch_node(&state.db.lock().unwrap(), &first)
                .unwrap()
                .last_modified_by,
            "gpt-test-low"
        );

        let replaced = set_fixed_connection(
            State(state.clone()),
            Json(SetFixedConnectionInput {
                parent_node_id: hex::encode(&parent),
                child_node_id: Some(hex::encode(&second)),
                slot: 1,
                model_attribution: "gpt-test-high".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            replaced.0["replaced_fixed_connection"]["id"],
            hex::encode(&first)
        );
        assert_eq!(
            replaced.0["node"]["fixed_connections"][0]["id"],
            hex::encode(&second)
        );
        assert_eq!(replaced.0["node"]["last_modified_by"], "gpt-test-high");
        assert_eq!(
            fetch_node(&state.db.lock().unwrap(), &first)
                .unwrap()
                .last_modified_by,
            "gpt-test-high"
        );
        assert_eq!(
            fetch_node(&state.db.lock().unwrap(), &second)
                .unwrap()
                .last_modified_by,
            "gpt-test-high"
        );

        let cleared = set_fixed_connection(
            State(state.clone()),
            Json(SetFixedConnectionInput {
                parent_node_id: hex::encode(parent),
                child_node_id: None,
                slot: 1,
                model_attribution: "gpt-test-xhigh".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            cleared.0["replaced_fixed_connection"]["id"],
            hex::encode(&second)
        );
        assert_eq!(cleared.0["node"]["fixed_connections"], json!([]));
        assert_eq!(cleared.0["node"]["last_modified_by"], "gpt-test-xhigh");
        assert_eq!(
            fetch_node(&state.db.lock().unwrap(), &second)
                .unwrap()
                .last_modified_by,
            "gpt-test-xhigh"
        );
    }

    #[tokio::test]
    async fn fanout_consolidation_moves_children_under_the_aggregator() {
        let db = db();
        let parent: Vec<u8> = db
            .query_row(
                "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let aggregator = insert_node(&db, "Task Group");
        let first = insert_node(&db, "Fanout One");
        let second = insert_node(&db, "Fanout Two");
        for (order, target) in [&aggregator, &first, &second].into_iter().enumerate() {
            db.execute(
                "INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'fanout',?3)",
                params![parent, target, order as i64 + 1],
            )
            .unwrap();
        }
        let state = state(db);
        let result = consolidate_fanout(
            State(state.clone()),
            Json(ConsolidateFanoutInput {
                parent_node_id: hex::encode(&parent),
                aggregator_node_id: hex::encode(&aggregator),
                fanout_node_ids: vec![hex::encode(&first), hex::encode(&second)],
                model_attribution: "gpt-test-medium".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            result.0["nodes"][0]["fanout_connections"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            result.0["nodes"][0]["fanout_connections"][0]["id"],
            hex::encode(&aggregator)
        );
        assert_eq!(
            result.0["nodes"][1]["fanout_connections"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(result.0["nodes"][0]["last_modified_by"], "gpt-test-medium");
        assert_eq!(result.0["nodes"][1]["last_modified_by"], "gpt-test-medium");
        let db = state.db.lock().unwrap();
        for id in [&first, &second] {
            assert_eq!(
                fetch_node(&db, id).unwrap().last_modified_by,
                "gpt-test-medium"
            );
        }
    }

    #[tokio::test]
    async fn provenance_creation_is_idempotent_when_keyed() {
        let state = state(db());
        let input = || ProvenanceInput {
            data: "David: hello".into(),
            source: "conversation".into(),
            source_created_at: "2026-07-12T00:00:00Z".into(),
            idempotency_key: Some("conversation:test".into()),
        };
        let first = create_provenance(State(state.clone()), Json(input()))
            .await
            .unwrap();
        let second = create_provenance(State(state), Json(input()))
            .await
            .unwrap();
        assert_eq!(first.0, StatusCode::CREATED);
        assert_eq!(second.0, StatusCode::OK);
        assert_eq!(first.1.0["id"], second.1.0["id"]);
    }
}
