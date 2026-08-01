use std::path::Path;

use anyhow::Context;
use chrono::Utc;
use kcode_kweb_db::{Config, KwebDb, NodeData, NodeId, Owner, Provenance};
use kcode_kweb_manager::KwebManager;
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SystemRoots {
    pub user: NodeId,
    pub kennedy: NodeId,
}

pub(crate) fn initialize(
    kweb_root: &Path,
    config: Config,
    identity_database: &Path,
) -> anyhow::Result<(KwebManager, SystemRoots)> {
    let mut identity = Connection::open(identity_database).with_context(|| {
        format!(
            "opening identity database {} for system roots",
            identity_database.display()
        )
    })?;
    identity.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=15000;
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
    let kmap = KwebManager::open(database, identity_database)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kcode_kweb_db::{NoopGossip, WriterId};

    use super::*;

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
            std::env::temp_dir().join(format!("kennedy-kweb-bootstrap-{}", uuid::Uuid::new_v4()));
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
}
