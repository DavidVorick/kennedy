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
const MAX_REQUEST_BYTES: usize = 128 * 1024 * 1024;
const TASK_HIGH_ORDER: i64 = -1;
const TASK_MEDIUM_ORDER: i64 = -2;
const TASK_LOW_ORDER: i64 = -3;
const PROMPT_FILES: [&str; 3] = [
    "KmapAgentManual.txt",
    "ConversationAgentManual.txt",
    "HistoryIngressAgentManual.txt",
];

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
struct TaskConnectionSummary {
    id: String,
    short_name: String,
    short_description: String,
    priority: String,
}

#[derive(Clone, Serialize)]
struct KnowledgeNode {
    id: String,
    short_name: String,
    short_description: String,
    long_description: String,
    task_connections: Vec<TaskConnectionSummary>,
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
    parent_node_ids: Vec<String>,
    short_name: String,
    short_description: String,
    long_description: String,
}

#[derive(Deserialize)]
struct UpdateNodeInput {
    provenance_id: String,
    short_name: String,
    short_description: String,
    long_description: String,
}

#[derive(Deserialize)]
struct ConnectInput {
    node_ids: Vec<String>,
}

#[derive(Deserialize)]
struct ConsolidateFanoutInput {
    parent_node_id: String,
    aggregator_node_id: String,
    fanout_node_ids: Vec<String>,
}

#[derive(Deserialize)]
struct AssignTaskInput {
    parent_node_id: String,
    child_node_id: Option<String>,
    priority: String,
}

pub async fn serve(config: Config) -> anyhow::Result<()> {
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
        .route("/api/v1/provenance", post(create_provenance))
        .route("/api/v1/provenance/{provenance_id}", get(get_provenance))
        .route(
            "/api/v1/connections/consolidate-fanout",
            post(consolidate_fanout),
        )
        .route("/api/v1/connections", post(connect_nodes))
        .route("/api/v1/tasks", post(assign_task))
        .route("/system-prompts/{filename}", get(get_prompt))
        .fallback_service(ServeDir::new(config.frontend_dir).append_index_html_on_directories(true))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(TraceLayer::new_for_http())
        .layer(middleware::map_response(prevent_stale_frontend_assets))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(address = %config.bind, "Kennedy Kweb listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn configure_database(db: &Connection) -> anyhow::Result<()> {
    db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
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

fn bootstrap(db: &mut Connection) -> anyhow::Result<()> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM knowledge_nodes WHERE is_user_root=1)",
        [],
        |r| r.get(0),
    )?;
    if exists {
        return Ok(());
    }
    let tx = db.transaction()?;
    let provenance_id = new_id();
    let node_id = new_id();
    let history_id = new_id();
    tx.execute(
        "INSERT INTO data_provenance_nodes(id,data,source,source_created_at) VALUES(?1,?2,?3,?4)",
        params![
            provenance_id,
            "Initial local user bootstrap.",
            "bootstrap",
            Utc::now().to_rfc3339()
        ],
    )?;
    tx.execute("INSERT INTO knowledge_nodes(id,short_name,short_description,long_description,is_user_root) VALUES(?1,?2,?3,?4,1)", params![node_id, "David Vorick", "The user Kennedy assists.", "David Vorick is the user of this local Kennedy installation."])?;
    tx.execute("INSERT INTO data_history_nodes(id,knowledge_node_id,previous_history_id,provenance_id) VALUES(?1,?2,NULL,?3)", params![history_id, node_id, provenance_id])?;
    tx.execute(
        "UPDATE knowledge_nodes SET history_head_id=?1 WHERE id=?2",
        params![history_id, node_id],
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
    if !PROMPT_FILES.contains(&filename.as_str()) {
        return Err(ApiError::not_found("Prompt manual not found."));
    }
    let body = tokio::fs::read_to_string(state.prompts_dir.join(&filename))
        .await
        .map_err(ApiError::internal)?;
    Ok(([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response())
}

async fn get_user(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let db = state.db.lock().map_err(ApiError::internal)?;
    let id: Vec<u8> = db
        .query_row(
            "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
            [],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;
    Ok(Json(
        json!({"name":"David Vorick","root_node_id":hex::encode(id)}),
    ))
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

fn task_priority(order: i64) -> Option<&'static str> {
    match order {
        TASK_HIGH_ORDER => Some("high"),
        TASK_MEDIUM_ORDER => Some("medium"),
        TASK_LOW_ORDER => Some("low"),
        _ => None,
    }
}

fn task_order(priority: &str) -> Result<i64, ApiError> {
    match priority {
        "high" => Ok(TASK_HIGH_ORDER),
        "medium" => Ok(TASK_MEDIUM_ORDER),
        "low" => Ok(TASK_LOW_ORDER),
        _ => Err(ApiError::bad("Task priority must be high, medium, or low.")),
    }
}

fn fetch_task_summaries(
    db: &Connection,
    source: &[u8],
) -> rusqlite::Result<Vec<TaskConnectionSummary>> {
    let mut stmt = db.prepare("SELECT n.id,n.short_name,n.short_description,c.activation_order FROM knowledge_connections c JOIN knowledge_nodes n ON n.id=c.target_node_id WHERE c.source_node_id=?1 AND c.tier='fanout' AND c.activation_order BETWEEN ?2 AND ?3 ORDER BY c.activation_order DESC")?;
    stmt.query_map(params![source, TASK_LOW_ORDER, TASK_HIGH_ORDER], |row| {
        let order = row.get::<_, i64>(3)?;
        Ok(TaskConnectionSummary {
            id: hex::encode(row.get::<_, Vec<u8>>(0)?),
            short_name: row.get(1)?,
            short_description: row.get(2)?,
            priority: task_priority(order).unwrap_or("unknown").to_string(),
        })
    })?
    .collect()
}

fn fetch_node(db: &Connection, id: &[u8]) -> Result<KnowledgeNode, ApiError> {
    let core = db.query_row("SELECT short_name,short_description,long_description,history_head_id FROM knowledge_nodes WHERE id=?1", [id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<Vec<u8>>>(3)?))).optional().map_err(ApiError::internal)?;
    let Some((short_name, short_description, long_description, history)) = core else {
        return Err(ApiError::not_found("Knowledge node not found."));
    };
    Ok(KnowledgeNode {
        id: hex::encode(id),
        short_name,
        short_description,
        long_description,
        task_connections: fetch_task_summaries(db, id).map_err(ApiError::internal)?,
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
    let db = state.db.lock().map_err(ApiError::internal)?;
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
        let count: i64 = tx.query_row("SELECT COUNT(*) FROM knowledge_connections WHERE source_node_id=?1 AND tier='active'", [source], |r| r.get(0)).map_err(ApiError::internal)?;
        let count = count as usize;
        if count > active_limit {
            let fanout_count: i64 = tx.query_row("SELECT COUNT(*) FROM knowledge_connections WHERE source_node_id=?1 AND tier='fanout' AND activation_order>=0", [source], |r| r.get(0)).map_err(ApiError::internal)?;
            if fanout_count as usize + (count - active_limit) > fanout_limit {
                return Err(ApiError::conflict(format!(
                    "ConnectNodes would exceed the fanout connection limit of {fanout_limit}."
                )));
            }
            tx.execute("UPDATE knowledge_connections SET tier='fanout' WHERE source_node_id=?1 AND target_node_id IN (SELECT target_node_id FROM knowledge_connections WHERE source_node_id=?1 AND tier='active' ORDER BY activation_order ASC LIMIT ?2)", params![source,(count-active_limit) as i64]).map_err(ApiError::internal)?;
        }
    }
    Ok(())
}

async fn connect_nodes(
    State(state): State<AppState>,
    Json(input): Json<ConnectInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ids = validate_distinct_ids(&input.node_ids, 2, "ConnectNodes")?;
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    connect_in_tx(&tx, &ids, state.active_limit, state.fanout_limit)?;
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
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    consolidate_fanout_in_tx(&tx, &parent, &aggregator, &fanout_ids, state.fanout_limit)?;
    tx.commit().map_err(ApiError::internal)?;
    let nodes = [parent, aggregator]
        .iter()
        .map(|id| fetch_node(&db, id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({"nodes":nodes})))
}

async fn assign_task(
    State(state): State<AppState>,
    Json(input): Json<AssignTaskInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let parent = decode_id(&input.parent_node_id)?;
    let child = input.child_node_id.as_deref().map(decode_id).transpose()?;
    let priority_order = task_order(&input.priority)?;
    if child.as_deref() == Some(parent.as_slice()) {
        return Err(ApiError::bad("A node cannot be its own task connection."));
    }
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    require_exists(&tx, "knowledge_nodes", &parent, "Parent knowledge node")?;
    if let Some(child) = child.as_deref() {
        require_exists(&tx, "knowledge_nodes", child, "Task knowledge node")?;
    }
    let mut replaced = fetch_task_summaries(&tx, &parent)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|task| task.priority == input.priority);
    if replaced
        .as_ref()
        .is_some_and(|task| child.as_ref().is_some_and(|id| task.id == hex::encode(id)))
    {
        replaced = None;
    }
    tx.execute(
        "DELETE FROM knowledge_connections WHERE source_node_id=?1 AND tier='fanout' AND activation_order=?2",
        params![parent, priority_order],
    )
    .map_err(ApiError::internal)?;
    if let Some(child) = child {
        tx.execute(
            "INSERT INTO knowledge_connections(source_node_id,target_node_id,tier,activation_order) VALUES(?1,?2,'fanout',?3) ON CONFLICT(source_node_id,target_node_id) DO UPDATE SET tier='fanout',activation_order=excluded.activation_order",
            params![parent, child, priority_order],
        )
        .map_err(ApiError::internal)?;
    }
    tx.commit().map_err(ApiError::internal)?;
    let node = fetch_node(&db, &parent)?;
    Ok(Json(json!({"node":node,"replaced_task":replaced})))
}

async fn create_node(
    State(state): State<AppState>,
    Json(input): Json<CreateNodeInput>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let provenance = decode_id(&input.provenance_id)?;
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
    for parent in &parents {
        require_exists(&tx, "knowledge_nodes", parent, "Parent knowledge node")?;
    }
    tx.execute("INSERT INTO knowledge_nodes(id,short_name,short_description,long_description,is_user_root) VALUES(?1,?2,?3,?4,0)",params![node_id,name,short,input.long_description]).map_err(ApiError::internal)?;
    tx.execute("INSERT INTO data_history_nodes(id,knowledge_node_id,previous_history_id,provenance_id) VALUES(?1,?2,NULL,?3)",params![history_id,node_id,provenance]).map_err(ApiError::internal)?;
    tx.execute(
        "UPDATE knowledge_nodes SET history_head_id=?1 WHERE id=?2",
        params![history_id, node_id],
    )
    .map_err(ApiError::internal)?;
    let mut connected = vec![node_id.clone()];
    connected.extend(parents);
    connect_in_tx(&tx, &connected, state.active_limit, state.fanout_limit)?;
    tx.commit().map_err(ApiError::internal)?;
    let node = fetch_node(&db, &node_id)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"node":node,"history_node_id":hex::encode(history_id)})),
    ))
}

async fn update_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(input): Json<UpdateNodeInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = decode_id(&node_id)?;
    let provenance = decode_id(&input.provenance_id)?;
    let (name, short) = validate_node_text(
        &input.short_name,
        &input.short_description,
        &input.long_description,
    )?;
    let history_id = new_id();
    let mut db = state.db.lock().map_err(ApiError::internal)?;
    let tx = db.transaction().map_err(ApiError::internal)?;
    require_exists(&tx, "data_provenance_nodes", &provenance, "Provenance node")?;
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
    tx.execute("UPDATE knowledge_nodes SET short_name=?1,short_description=?2,long_description=?3,history_head_id=?4 WHERE id=?5",params![name,short,input.long_description,history_id,id]).map_err(ApiError::internal)?;
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
        bootstrap(&mut db).unwrap();
        db
    }

    fn state(db: Connection) -> AppState {
        AppState {
            db: Arc::new(Mutex::new(db)),
            prompts_dir: PathBuf::new(),
            active_limit: 12,
            fanout_limit: 60,
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

    #[test]
    fn bootstrap_has_valid_history() {
        let db = db();
        let count:i64=db.query_row("SELECT COUNT(*) FROM knowledge_nodes n JOIN data_history_nodes h ON h.id=n.history_head_id JOIN data_provenance_nodes p ON p.id=h.provenance_id WHERE n.is_user_root=1",[],|r|r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn legacy_nodes_have_no_task_connections_without_a_schema_upgrade() {
        let db = db();
        let root: Vec<u8> = db
            .query_row(
                "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let node = fetch_node(&db, &root).unwrap();
        assert!(node.task_connections.is_empty());
    }

    #[tokio::test]
    async fn task_slots_can_be_assigned_replaced_and_cleared() {
        let db = db();
        let parent: Vec<u8> = db
            .query_row(
                "SELECT id FROM knowledge_nodes WHERE is_user_root=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let first = insert_node(&db, "First Task");
        let second = insert_node(&db, "Second Task");
        let state = state(db);

        let assigned = assign_task(
            State(state.clone()),
            Json(AssignTaskInput {
                parent_node_id: hex::encode(&parent),
                child_node_id: Some(hex::encode(&first)),
                priority: "high".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            assigned.0["node"]["task_connections"][0]["id"],
            hex::encode(&first)
        );
        assert_eq!(
            assigned.0["node"]["task_connections"][0]["priority"],
            "high"
        );
        assert_eq!(assigned.0["node"]["fanout_connections"], json!([]));

        let replaced = assign_task(
            State(state.clone()),
            Json(AssignTaskInput {
                parent_node_id: hex::encode(&parent),
                child_node_id: Some(hex::encode(&second)),
                priority: "high".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(replaced.0["replaced_task"]["id"], hex::encode(&first));
        assert_eq!(
            replaced.0["node"]["task_connections"][0]["id"],
            hex::encode(&second)
        );

        let cleared = assign_task(
            State(state),
            Json(AssignTaskInput {
                parent_node_id: hex::encode(parent),
                child_node_id: None,
                priority: "high".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(cleared.0["replaced_task"]["id"], hex::encode(second));
        assert_eq!(cleared.0["node"]["task_connections"], json!([]));
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
            State(state),
            Json(ConsolidateFanoutInput {
                parent_node_id: hex::encode(&parent),
                aggregator_node_id: hex::encode(&aggregator),
                fanout_node_ids: vec![hex::encode(&first), hex::encode(&second)],
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
