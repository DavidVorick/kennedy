use std::{collections::HashSet, path::Path, sync::Mutex};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use kennedy_telegram_relay::{
    AddUserOutcome, IdentityObservation, IdentitySink, WhitelistSnapshot,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const IDENTITY_MIGRATION: &str = include_str!("../migrations/telegram_identity.sql");

pub(crate) struct Directory {
    database: Mutex<Connection>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DirectoryUser {
    handle: String,
    telegram_user_id: Option<i64>,
    current_username: Option<String>,
    display_name: Option<String>,
    root_node_id: String,
    root_ready: bool,
    can_add_users: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DirectoryGroup {
    group_id: String,
    root_node_id: String,
    root_ready: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompleteRoot {
    root_node_id: String,
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

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: "Telegram directory entry not found.".into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "state_conflict",
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error=%error, "Telegram identity-directory request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: "An unexpected Telegram identity-directory error occurred.".into(),
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

impl Directory {
    pub(crate) fn open(path: &Path, bootstrap_handle: &str) -> anyhow::Result<Self> {
        let connection =
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;",
        )?;
        connection.execute_batch(IDENTITY_MIGRATION)?;
        let directory = Self {
            database: Mutex::new(connection),
        };
        directory.seed_bootstrap_user(bootstrap_handle)?;
        Ok(directory)
    }

    fn seed_bootstrap_user(&self, handle: &str) -> anyhow::Result<()> {
        let handle = normalize_username(handle);
        anyhow::ensure!(
            !handle.is_empty(),
            "Telegram bootstrap handle must not be empty"
        );
        let database = self.lock()?;
        let root_node_id = random_unassigned_node_id(&database)?;
        let now = Utc::now().to_rfc3339();
        database.execute(
            "INSERT INTO whitelist_entries(handle,root_node_id,can_add_users,whitelisted_at,updated_at)
             VALUES(?1,?2,1,?3,?3)
             ON CONFLICT(handle) DO UPDATE SET can_add_users=1,updated_at=excluded.updated_at",
            params![handle, root_node_id, now],
        )?;
        Ok(())
    }

    fn lock(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Connection>> {
        self.database
            .lock()
            .map_err(|_| anyhow::anyhow!("locking Telegram identity directory"))
    }
}

impl IdentitySink for Directory {
    fn observe_identity(&self, observation: &IdentityObservation) -> anyhow::Result<()> {
        let database = self.lock()?;
        observe_identity(&database, observation)?;
        Ok(())
    }

    fn whitelist(&self) -> anyhow::Result<WhitelistSnapshot> {
        let database = self.lock()?;
        let telegram_user_ids = database
            .prepare(
                "SELECT telegram_user_id FROM whitelist_entries
                 WHERE telegram_user_id IS NOT NULL ORDER BY telegram_user_id",
            )?
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<Result<HashSet<_>, _>>()?;
        Ok(WhitelistSnapshot { telegram_user_ids })
    }

    fn request_add_user(
        &self,
        requested_by_telegram_user_id: i64,
        handle: &str,
    ) -> anyhow::Result<AddUserOutcome> {
        let database = self.lock()?;
        let can_add = directory_user_by_id(&database, requested_by_telegram_user_id)?
            .is_some_and(|user| user.can_add_users);
        if !can_add {
            return Ok(AddUserOutcome::Forbidden);
        }
        let user = whitelist_handle(&database, handle, requested_by_telegram_user_id)?;
        Ok(AddUserOutcome::Whitelisted {
            handle: user.handle,
            telegram_user_id: user.telegram_user_id,
        })
    }

    fn observe_group(&self, group_id: &str) -> anyhow::Result<()> {
        let database = self.lock()?;
        let now = Utc::now().to_rfc3339();
        let root_node_id = random_unassigned_node_id(&database)?;
        database.execute(
            "INSERT INTO telegram_group_roots(group_id,root_node_id,created_at,updated_at)
             VALUES(?1,?2,?3,?3) ON CONFLICT(group_id) DO NOTHING",
            params![group_id, root_node_id, now],
        )?;
        Ok(())
    }
}

pub(crate) fn router(directory: std::sync::Arc<Directory>) -> Router {
    Router::new()
        .route(
            "/api/v1/telegram-directory/users/provisioning",
            get(list_provisioning_users),
        )
        .route(
            "/api/v1/telegram-directory/users/by-handle/{handle}",
            get(user_by_handle),
        )
        .route(
            "/api/v1/telegram-directory/users/by-handle/{handle}/root-ready",
            post(complete_handle_root),
        )
        .route(
            "/api/v1/telegram-directory/users/{telegram_user_id}",
            get(user_by_id),
        )
        .route(
            "/api/v1/telegram-directory/users/{telegram_user_id}/root-ready",
            post(complete_user_root),
        )
        .route(
            "/api/v1/telegram-directory/groups/provisioning",
            get(list_provisioning_groups),
        )
        .route(
            "/api/v1/telegram-directory/groups/{group_id}",
            get(group_by_id),
        )
        .route(
            "/api/v1/telegram-directory/groups/{group_id}/root-ready",
            post(complete_group_root),
        )
        .with_state(directory)
}

fn normalize_username(value: &str) -> String {
    value.trim().trim_start_matches('@').to_ascii_lowercase()
}

fn random_node_id() -> String {
    hex::encode(rand::random::<[u8; 20]>())
}

fn random_unassigned_node_id(database: &Connection) -> anyhow::Result<String> {
    loop {
        let candidate = random_node_id();
        let assigned = database.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM whitelist_entries WHERE root_node_id=?1
                 UNION ALL SELECT 1 FROM telegram_group_roots WHERE root_node_id=?1
                 UNION ALL SELECT 1 FROM kmap_system_roots WHERE node_id=?1
             )",
            [&candidate],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if !assigned {
            return Ok(candidate);
        }
    }
}

fn directory_user_by_clause(
    database: &Connection,
    clause: &str,
    value: &dyn rusqlite::ToSql,
) -> Result<Option<DirectoryUser>, rusqlite::Error> {
    database
        .query_row(
            &format!(
                "SELECT handle,telegram_user_id,current_username,display_name,root_node_id,root_ready,can_add_users
                 FROM whitelist_entries WHERE {clause}"
            ),
            [value],
            |row| {
                Ok(DirectoryUser {
                    handle: row.get(0)?,
                    telegram_user_id: row.get(1)?,
                    current_username: row.get(2)?,
                    display_name: row.get(3)?,
                    root_node_id: row.get(4)?,
                    root_ready: row.get::<_, i64>(5)? != 0,
                    can_add_users: row.get::<_, i64>(6)? != 0,
                })
            },
        )
        .optional()
}

fn directory_user_by_id(
    database: &Connection,
    telegram_user_id: i64,
) -> anyhow::Result<Option<DirectoryUser>> {
    Ok(directory_user_by_clause(
        database,
        "telegram_user_id=?1",
        &telegram_user_id,
    )?)
}

fn directory_user_by_handle(
    database: &Connection,
    handle: &str,
) -> anyhow::Result<Option<DirectoryUser>> {
    Ok(directory_user_by_clause(database, "handle=?1", &handle)?)
}

fn directory_group_by_id(
    database: &Connection,
    group_id: &str,
) -> anyhow::Result<Option<DirectoryGroup>> {
    Ok(database
        .query_row(
            "SELECT group_id,root_node_id,root_ready FROM telegram_group_roots WHERE group_id=?1",
            [group_id],
            |row| {
                Ok(DirectoryGroup {
                    group_id: row.get(0)?,
                    root_node_id: row.get(1)?,
                    root_ready: row.get::<_, i64>(2)? != 0,
                })
            },
        )
        .optional()?)
}

fn observe_identity(
    database: &Connection,
    observation: &IdentityObservation,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    let normalized = observation
        .username
        .as_deref()
        .map(normalize_username)
        .filter(|value| !value.is_empty());
    database.execute(
        "INSERT INTO observed_identities(telegram_user_id,current_username,display_name,first_seen_at,last_seen_at)
         VALUES(?1,?2,?3,?4,?4)
         ON CONFLICT(telegram_user_id) DO UPDATE SET
             current_username=excluded.current_username,
             display_name=excluded.display_name,last_seen_at=excluded.last_seen_at",
        params![
            observation.telegram_user_id,
            normalized,
            observation.display_name,
            now
        ],
    )?;
    if directory_user_by_id(database, observation.telegram_user_id)?.is_some() {
        database.execute(
            "UPDATE whitelist_entries SET current_username=?1,display_name=?2,updated_at=?3
             WHERE telegram_user_id=?4",
            params![
                normalized,
                observation.display_name,
                now,
                observation.telegram_user_id
            ],
        )?;
        return Ok(());
    }
    let Some(handle) = normalized else {
        return Ok(());
    };
    let Some(entry) = directory_user_by_handle(database, &handle)? else {
        return Ok(());
    };
    if entry.telegram_user_id.is_some() {
        return Ok(());
    }
    database.execute(
        "UPDATE whitelist_entries SET telegram_user_id=?1,current_username=?2,display_name=?3,
             resolved_at=?4,updated_at=?4 WHERE handle=?2 AND telegram_user_id IS NULL",
        params![
            observation.telegram_user_id,
            handle,
            observation.display_name,
            now
        ],
    )?;
    Ok(())
}

fn whitelist_handle(
    database: &Connection,
    handle: &str,
    added_by: i64,
) -> anyhow::Result<DirectoryUser> {
    let handle = normalize_username(handle.trim_matches(['\'', '"']));
    anyhow::ensure!(!handle.is_empty(), "the Telegram handle must not be empty");
    let now = Utc::now().to_rfc3339();
    let root_node_id = random_unassigned_node_id(database)?;
    database.execute(
        "INSERT INTO whitelist_entries(handle,current_username,root_node_id,added_by_telegram_user_id,whitelisted_at,updated_at)
         VALUES(?1,?1,?2,?3,?4,?4) ON CONFLICT(handle) DO UPDATE SET updated_at=excluded.updated_at",
        params![handle, root_node_id, added_by, now],
    )?;
    directory_user_by_handle(database, &handle)?.context("reading whitelisted Telegram handle")
}

fn validate_root(root_node_id: &str) -> Result<(), ApiError> {
    if root_node_id.len() != 40 || hex::decode(root_node_id).is_err() {
        return Err(ApiError::bad(
            "rootNodeId must be a 40-character hexadecimal Kmap identifier.",
        ));
    }
    Ok(())
}

async fn list_provisioning_users(
    State(directory): State<std::sync::Arc<Directory>>,
) -> Result<Json<Value>, ApiError> {
    let database = directory.lock().map_err(ApiError::internal)?;
    let users = database
        .prepare(
            "SELECT handle,telegram_user_id,current_username,display_name,root_node_id,root_ready,can_add_users
             FROM whitelist_entries WHERE root_ready=0 ORDER BY whitelisted_at,handle",
        )
        .map_err(ApiError::internal)?
        .query_map([], |row| {
            Ok(DirectoryUser {
                handle: row.get(0)?,
                telegram_user_id: row.get(1)?,
                current_username: row.get(2)?,
                display_name: row.get(3)?,
                root_node_id: row.get(4)?,
                root_ready: row.get::<_, i64>(5)? != 0,
                can_add_users: row.get::<_, i64>(6)? != 0,
            })
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"users":users})))
}

async fn user_by_handle(
    State(directory): State<std::sync::Arc<Directory>>,
    AxumPath(handle): AxumPath<String>,
) -> Result<Json<DirectoryUser>, ApiError> {
    let database = directory.lock().map_err(ApiError::internal)?;
    directory_user_by_handle(&database, &normalize_username(&handle))
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn user_by_id(
    State(directory): State<std::sync::Arc<Directory>>,
    AxumPath(telegram_user_id): AxumPath<i64>,
) -> Result<Json<DirectoryUser>, ApiError> {
    let database = directory.lock().map_err(ApiError::internal)?;
    directory_user_by_id(&database, telegram_user_id)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn complete_user_root(
    State(directory): State<std::sync::Arc<Directory>>,
    AxumPath(telegram_user_id): AxumPath<i64>,
    Json(input): Json<CompleteRoot>,
) -> Result<Json<DirectoryUser>, ApiError> {
    validate_root(&input.root_node_id)?;
    let database = directory.lock().map_err(ApiError::internal)?;
    let current = directory_user_by_id(&database, telegram_user_id)
        .map_err(ApiError::internal)?
        .ok_or_else(ApiError::not_found)?;
    if current.root_ready && current.root_node_id != input.root_node_id {
        return Err(ApiError::conflict(
            "This Telegram identity already has a different root node.",
        ));
    }
    database
        .execute(
            "UPDATE whitelist_entries SET root_node_id=?1,root_ready=1,updated_at=?2
             WHERE telegram_user_id=?3",
            params![
                input.root_node_id,
                Utc::now().to_rfc3339(),
                telegram_user_id
            ],
        )
        .map_err(ApiError::internal)?;
    Ok(Json(
        directory_user_by_id(&database, telegram_user_id)
            .map_err(ApiError::internal)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

async fn complete_handle_root(
    State(directory): State<std::sync::Arc<Directory>>,
    AxumPath(handle): AxumPath<String>,
    Json(input): Json<CompleteRoot>,
) -> Result<Json<DirectoryUser>, ApiError> {
    validate_root(&input.root_node_id)?;
    let handle = normalize_username(&handle);
    let database = directory.lock().map_err(ApiError::internal)?;
    let current = directory_user_by_handle(&database, &handle)
        .map_err(ApiError::internal)?
        .ok_or_else(ApiError::not_found)?;
    if current.root_ready && current.root_node_id != input.root_node_id {
        return Err(ApiError::conflict(
            "This whitelisted handle already has a different root node.",
        ));
    }
    database
        .execute(
            "UPDATE whitelist_entries SET root_node_id=?1,root_ready=1,updated_at=?2 WHERE handle=?3",
            params![input.root_node_id, Utc::now().to_rfc3339(), handle],
        )
        .map_err(ApiError::internal)?;
    Ok(Json(
        directory_user_by_handle(&database, &handle)
            .map_err(ApiError::internal)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

async fn list_provisioning_groups(
    State(directory): State<std::sync::Arc<Directory>>,
) -> Result<Json<Value>, ApiError> {
    let database = directory.lock().map_err(ApiError::internal)?;
    let groups = database
        .prepare(
            "SELECT group_id,root_node_id,root_ready FROM telegram_group_roots
             WHERE root_ready=0 ORDER BY datetime(created_at),group_id",
        )
        .map_err(ApiError::internal)?
        .query_map([], |row| {
            Ok(DirectoryGroup {
                group_id: row.get(0)?,
                root_node_id: row.get(1)?,
                root_ready: row.get::<_, i64>(2)? != 0,
            })
        })
        .map_err(ApiError::internal)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"groups":groups})))
}

async fn group_by_id(
    State(directory): State<std::sync::Arc<Directory>>,
    AxumPath(group_id): AxumPath<String>,
) -> Result<Json<DirectoryGroup>, ApiError> {
    let database = directory.lock().map_err(ApiError::internal)?;
    directory_group_by_id(&database, &group_id)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(ApiError::not_found)
}

async fn complete_group_root(
    State(directory): State<std::sync::Arc<Directory>>,
    AxumPath(group_id): AxumPath<String>,
    Json(input): Json<CompleteRoot>,
) -> Result<Json<DirectoryGroup>, ApiError> {
    validate_root(&input.root_node_id)?;
    let database = directory.lock().map_err(ApiError::internal)?;
    let current = directory_group_by_id(&database, &group_id)
        .map_err(ApiError::internal)?
        .ok_or_else(ApiError::not_found)?;
    if current.root_node_id != input.root_node_id {
        return Err(ApiError::conflict(
            "This Telegram group has a different reserved root node.",
        ));
    }
    database
        .execute(
            "UPDATE telegram_group_roots SET root_ready=1,updated_at=?1 WHERE group_id=?2",
            params![Utc::now().to_rfc3339(), group_id],
        )
        .map_err(ApiError::internal)?;
    Ok(Json(
        directory_group_by_id(&database, &group_id)
            .map_err(ApiError::internal)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kennedy_telegram_relay::IdentitySink;

    fn directory() -> Directory {
        let database = Connection::open_in_memory().unwrap();
        database
            .execute_batch(
                "CREATE TABLE kmap_system_roots(role TEXT PRIMARY KEY,node_id TEXT UNIQUE NOT NULL);
                 INSERT INTO kmap_system_roots VALUES('user','1111111111111111111111111111111111111111');",
            )
            .unwrap();
        database.execute_batch(IDENTITY_MIGRATION).unwrap();
        let directory = Directory {
            database: Mutex::new(database),
        };
        directory.seed_bootstrap_user("@taek42").unwrap();
        directory
    }

    #[test]
    fn tofu_is_owned_by_kennedy_and_numeric_ids_remain_authoritative() {
        let directory = directory();
        directory
            .observe_identity(&IdentityObservation {
                telegram_user_id: 42,
                username: Some("TaEk42".into()),
                display_name: "David".into(),
            })
            .unwrap();
        assert!(
            directory
                .whitelist()
                .unwrap()
                .telegram_user_ids
                .contains(&42)
        );
        directory
            .observe_identity(&IdentityObservation {
                telegram_user_id: 43,
                username: Some("taek42".into()),
                display_name: "Other".into(),
            })
            .unwrap();
        assert!(
            !directory
                .whitelist()
                .unwrap()
                .telegram_user_ids
                .contains(&43)
        );
    }

    #[test]
    fn add_user_capability_and_group_roots_stay_in_kennedy() {
        let directory = directory();
        directory
            .observe_identity(&IdentityObservation {
                telegram_user_id: 42,
                username: Some("taek42".into()),
                display_name: "David".into(),
            })
            .unwrap();
        assert!(matches!(
            directory.request_add_user(77, "@friend").unwrap(),
            AddUserOutcome::Forbidden
        ));
        assert!(matches!(
            directory.request_add_user(42, "@friend").unwrap(),
            AddUserOutcome::Whitelisted { .. }
        ));
        directory.observe_group("opaque-group").unwrap();
        let database = directory.lock().unwrap();
        let group = directory_group_by_id(&database, "opaque-group")
            .unwrap()
            .unwrap();
        assert_eq!(group.root_node_id.len(), 40);
        assert!(!group.root_ready);
    }

    #[tokio::test]
    async fn root_completion_is_owned_and_conflict_checked_by_kennedy() {
        let directory = std::sync::Arc::new(directory());
        directory
            .observe_identity(&IdentityObservation {
                telegram_user_id: 42,
                username: Some("taek42".into()),
                display_name: "David".into(),
            })
            .unwrap();
        directory.observe_group("opaque-group").unwrap();

        let user_root = "2222222222222222222222222222222222222222";
        let user = complete_user_root(
            State(directory.clone()),
            AxumPath(42),
            Json(CompleteRoot {
                root_node_id: user_root.into(),
            }),
        )
        .await
        .unwrap();
        assert!(user.0.root_ready);
        assert_eq!(user.0.root_node_id, user_root);

        let reserved_group_root = {
            let database = directory.lock().unwrap();
            directory_group_by_id(&database, "opaque-group")
                .unwrap()
                .unwrap()
                .root_node_id
        };
        let mismatch = complete_group_root(
            State(directory.clone()),
            AxumPath("opaque-group".into()),
            Json(CompleteRoot {
                root_node_id: "3333333333333333333333333333333333333333".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch.code, "state_conflict");
        let group = complete_group_root(
            State(directory),
            AxumPath("opaque-group".into()),
            Json(CompleteRoot {
                root_node_id: reserved_group_root.clone(),
            }),
        )
        .await
        .unwrap();
        assert!(group.0.root_ready);
        assert_eq!(group.0.root_node_id, reserved_group_root);
    }
}
