use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use ed25519_dalek::{Signer, SigningKey};
use kcode_kweb_db::{
    Config, KwebDb, NodeData, NodeId, NoopGossip, Owner, TransactionPackage, WriterId,
};
use rand::random;
use rusqlite::{Connection, OpenFlags, params};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;
use zeroize::Zeroize;

const MAX_SIGNED_TRANSACTION_BYTES: usize = 64 * 1024 * 1024;
const STATE_MAGIC: &[u8; 8] = b"KWSTATE3";

pub(crate) struct MigrationOptions {
    pub target_root: PathBuf,
    pub legacy_database: PathBuf,
    pub legacy_artifacts: PathBuf,
    pub identity_database: PathBuf,
    pub conversation_database: PathBuf,
    pub memory_ingress_database: PathBuf,
    pub archive_directory: PathBuf,
}

pub(crate) struct MigrationResult {
    pub nodes: usize,
    pub migration_writer: WriterId,
    pub purged_conversations: usize,
    pub purged_memory_jobs: usize,
    pub archive: PathBuf,
}

struct LegacyNode {
    old_id: [u8; 20],
    short_name: String,
    short_description: String,
    long_description: String,
    owner: Option<[u8; 20]>,
    fixed_connections: Vec<[u8; 20]>,
    recent_connections: Vec<[u8; 20]>,
}

pub(crate) fn run(options: &MigrationOptions) -> anyhow::Result<MigrationResult> {
    ensure!(
        !options.target_root.exists(),
        "new Kweb root {} already exists",
        options.target_root.display()
    );
    ensure!(
        options.legacy_database.is_file(),
        "legacy Kweb database {} does not exist",
        options.legacy_database.display()
    );
    ensure!(
        options.identity_database.is_file()
            && options.conversation_database.is_file()
            && options.memory_ingress_database.is_file(),
        "Kennedy application databases required for migration are missing"
    );
    ensure!(
        !options.archive_directory.exists(),
        "archive destination {} already exists",
        options.archive_directory.display()
    );

    let legacy = Connection::open_with_flags(
        &options.legacy_database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| {
        format!(
            "opening legacy Kweb database {}",
            options.legacy_database.display()
        )
    })?;
    legacy
        .execute_batch("PRAGMA query_only=ON; PRAGMA foreign_keys=ON;")
        .context("configuring the legacy Kweb reader")?;
    let nodes = read_legacy_nodes(&legacy)?;
    ensure!(!nodes.is_empty(), "the legacy Kweb contains no nodes");
    validate_legacy_references(&nodes)?;

    let mut id_map = HashMap::with_capacity(nodes.len());
    let mut assigned = HashSet::with_capacity(nodes.len());
    for node in &nodes {
        let id = loop {
            let mut bytes = random::<[u8; 6]>();
            bytes[0] &= 0x7f;
            let candidate = NodeId::from_bytes(bytes).map_err(anyhow::Error::new)?;
            if assigned.insert(candidate) {
                break candidate;
            }
        };
        id_map.insert(node.old_id, id);
    }

    let mut migration_key = random::<[u8; 32]>();
    let migration_writer = WriterId::from_signing_key(&migration_key);
    let staging = options.target_root.with_file_name(format!(
        ".{}.{}.migration",
        options
            .target_root
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("kweb"),
        Uuid::new_v4()
    ));
    let migration_result = (|| -> anyhow::Result<()> {
        let database = KwebDb::open(
            &staging,
            Config {
                signing_key: migration_key,
                writers_by_priority: vec![migration_writer],
                gossip: Arc::new(NoopGossip),
            },
        )
        .map_err(anyhow::Error::new)?;
        let package = migration_package(&nodes, &id_map, &migration_key)?;
        ensure!(
            database
                .accept_transaction(package)
                .map_err(anyhow::Error::new)?,
            "the migration transaction was unexpectedly already present"
        );
        for node in &nodes {
            let id = id_map[&node.old_id];
            let imported = database.get_node(id).map_err(anyhow::Error::new)?;
            ensure!(
                imported.data == translated_node(node, &id_map)?,
                "new Kweb node {id} differs from its legacy source"
            );
        }
        drop(database);
        sync_directory(
            staging
                .parent()
                .filter(|value| !value.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?;
        fs::rename(&staging, &options.target_root).with_context(|| {
            format!(
                "publishing migrated Kweb root {}",
                options.target_root.display()
            )
        })?;
        sync_directory(
            options
                .target_root
                .parent()
                .filter(|value| !value.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?;
        Ok(())
    })();
    migration_key.zeroize();
    if let Err(error) = migration_result {
        if staging.exists() {
            let _ = fs::remove_dir_all(&staging);
        }
        return Err(error);
    }
    drop(legacy);

    rewrite_identity_roots(&options.identity_database, &id_map)?;
    rewrite_active_conversations(&options.conversation_database, &id_map)?;
    let (purged_conversations, purged_memory_jobs) = purge_terminal_ingress(
        &options.conversation_database,
        &options.memory_ingress_database,
    )?;
    let archive = archive_legacy_files(options, migration_writer, nodes.len())?;

    Ok(MigrationResult {
        nodes: nodes.len(),
        migration_writer,
        purged_conversations,
        purged_memory_jobs,
        archive,
    })
}

fn read_legacy_nodes(database: &Connection) -> anyhow::Result<Vec<LegacyNode>> {
    let mut statement = database.prepare(
        "SELECT id,short_name,short_description,long_description,owner_node_id
         FROM knowledge_nodes ORDER BY id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<Vec<u8>>>(4)?,
        ))
    })?;
    let mut nodes = Vec::new();
    for row in rows {
        let (id, short_name, short_description, long_description, owner) = row?;
        let old_id = legacy_id(&id)?;
        nodes.push(LegacyNode {
            old_id,
            short_name,
            short_description,
            long_description,
            owner: owner.as_deref().map(legacy_id).transpose()?,
            fixed_connections: read_connections(database, "fixed_connections", &old_id)?,
            recent_connections: read_connections(database, "recent_connections", &old_id)?,
        });
    }
    Ok(nodes)
}

fn read_connections(
    database: &Connection,
    table: &str,
    source: &[u8; 20],
) -> anyhow::Result<Vec<[u8; 20]>> {
    let mut statement = database.prepare(&format!(
        "SELECT target_node_id FROM {table} WHERE source_node_id=?1 ORDER BY position"
    ))?;
    statement
        .query_map([source.as_slice()], |row| row.get::<_, Vec<u8>>(0))?
        .map(|row| legacy_id(&row?))
        .collect()
}

fn legacy_id(bytes: &[u8]) -> anyhow::Result<[u8; 20]> {
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("legacy Kweb identifier has the wrong length"))
}

fn validate_legacy_references(nodes: &[LegacyNode]) -> anyhow::Result<()> {
    let ids = nodes.iter().map(|node| node.old_id).collect::<HashSet<_>>();
    for node in nodes {
        if let Some(owner) = node.owner {
            ensure!(ids.contains(&owner), "legacy node has a missing owner");
        }
        for reference in node
            .fixed_connections
            .iter()
            .chain(&node.recent_connections)
        {
            ensure!(
                ids.contains(reference),
                "legacy node has a missing connection"
            );
        }
    }
    Ok(())
}

fn translated_node(
    node: &LegacyNode,
    mapping: &HashMap<[u8; 20], NodeId>,
) -> anyhow::Result<NodeData> {
    let id = mapping[&node.old_id];
    let owner = match node.owner {
        None => Owner::Unowned,
        Some(owner) if owner == node.old_id => Owner::SelfNode,
        Some(owner) => Owner::Node(
            *mapping
                .get(&owner)
                .context("legacy owner is absent from the migration mapping")?,
        ),
    };
    let translate = |values: &[[u8; 20]]| -> anyhow::Result<Vec<NodeId>> {
        values
            .iter()
            .map(|value| {
                mapping
                    .get(value)
                    .copied()
                    .context("legacy connection is absent from the migration mapping")
            })
            .collect()
    };
    let data = NodeData {
        short_name: node.short_name.clone(),
        short_description: node.short_description.clone(),
        long_description: node.long_description.clone(),
        owner,
        fixed_connections: translate(&node.fixed_connections)?,
        recent_connections: translate(&node.recent_connections)?,
        objects: Vec::new(),
    };
    let _ = id;
    Ok(data)
}

fn migration_package(
    nodes: &[LegacyNode],
    mapping: &HashMap<[u8; 20], NodeId>,
    signing_key: &[u8; 32],
) -> anyhow::Result<TransactionPackage> {
    let now = Utc::now();
    let signing = SigningKey::from_bytes(signing_key);
    let writer = signing.verifying_key().to_bytes();
    let mut creates = nodes
        .iter()
        .map(|node| Ok((mapping[&node.old_id], translated_node(node, mapping)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    creates.sort_by_key(|(id, _)| *id);

    let mut bytes = Vec::with_capacity(4 * 1024 * 1024);
    bytes.extend_from_slice(b"KWTX\0\x02\0\0");
    bytes.extend_from_slice(&writer);
    put_datetime(&mut bytes, now);
    put_u64(&mut bytes, 0);
    put_string(&mut bytes, "migration")?;
    put_string(&mut bytes, "migration")?;
    put_datetime(&mut bytes, now);
    put_string(&mut bytes, "migration")?;
    put_u64(&mut bytes, 0);
    put_u64(&mut bytes, 0);
    put_u64(
        &mut bytes,
        u64::try_from(creates.len()).context("too many legacy nodes")?,
    );
    for (id, data) in creates {
        bytes.extend_from_slice(&id.to_bytes());
        put_node_data(&mut bytes, &data)?;
    }
    put_u64(&mut bytes, 0);
    ensure!(
        bytes.len() + 64 <= MAX_SIGNED_TRANSACTION_BYTES,
        "the migration transaction exceeds the 64 MiB signed-metadata limit"
    );
    bytes.extend_from_slice(&signing.sign(&bytes).to_bytes());
    Ok(TransactionPackage {
        transaction: bytes,
        objects: Vec::new(),
    })
}

fn put_node_data(bytes: &mut Vec<u8>, data: &NodeData) -> anyhow::Result<()> {
    put_string(bytes, &data.short_name)?;
    put_string(bytes, &data.short_description)?;
    put_string(bytes, &data.long_description)?;
    match data.owner {
        Owner::Unowned => bytes.push(0),
        Owner::SelfNode => bytes.push(1),
        Owner::Node(id) => {
            bytes.push(2);
            bytes.extend_from_slice(&id.to_bytes());
        }
    }
    put_node_ids(bytes, &data.fixed_connections)?;
    put_node_ids(bytes, &data.recent_connections)?;
    put_u64(bytes, 0);
    Ok(())
}

fn put_node_ids(bytes: &mut Vec<u8>, ids: &[NodeId]) -> anyhow::Result<()> {
    put_u64(
        bytes,
        u64::try_from(ids.len()).context("too many node identifiers")?,
    );
    for id in ids {
        bytes.extend_from_slice(&id.to_bytes());
    }
    Ok(())
}

fn put_string(bytes: &mut Vec<u8>, value: &str) -> anyhow::Result<()> {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .context("migration string exceeds u32")?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_datetime(bytes: &mut Vec<u8>, value: DateTime<Utc>) {
    bytes.extend_from_slice(&value.timestamp().to_be_bytes());
    bytes.extend_from_slice(&value.timestamp_subsec_nanos().to_be_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn rewrite_identity_roots(path: &Path, mapping: &HashMap<[u8; 20], NodeId>) -> anyhow::Result<()> {
    let mut database =
        Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    database.execute_batch("PRAGMA busy_timeout=5000; PRAGMA foreign_keys=OFF;")?;
    database.execute_batch(
        "DROP TABLE IF EXISTS temp.kweb_node_id_migration;
         CREATE TEMP TABLE kweb_node_id_migration(
             old_id TEXT PRIMARY KEY,
             new_id TEXT NOT NULL UNIQUE
         );",
    )?;
    {
        let transaction = database.transaction()?;
        for (old, new) in mapping {
            transaction.execute(
                "INSERT INTO temp.kweb_node_id_migration(old_id,new_id) VALUES(?1,?2)",
                params![hex::encode(old), new.to_string()],
            )?;
        }
        transaction.commit()?;
    }
    database.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE kmap_system_roots_v2 (
             role TEXT PRIMARY KEY CHECK(role IN ('user','kennedy')),
             root_node_id TEXT NOT NULL UNIQUE CHECK(length(root_node_id)=8),
             created_at TEXT NOT NULL
         );
         INSERT INTO kmap_system_roots_v2
         SELECT r.role,m.new_id,r.created_at
         FROM kmap_system_roots r
         JOIN temp.kweb_node_id_migration m ON m.old_id=r.root_node_id;

         CREATE TABLE whitelist_entries_v2 (
             handle TEXT PRIMARY KEY,
             telegram_user_id INTEGER UNIQUE,
             current_username TEXT,
             display_name TEXT,
             root_node_id TEXT UNIQUE CHECK(root_node_id IS NULL OR length(root_node_id)=8),
             root_ready INTEGER NOT NULL DEFAULT 0 CHECK(root_ready IN (0,1)),
             can_add_users INTEGER NOT NULL DEFAULT 0 CHECK(can_add_users IN (0,1)),
             added_by_telegram_user_id INTEGER,
             whitelisted_at TEXT NOT NULL,
             resolved_at TEXT,
             updated_at TEXT NOT NULL,
             CHECK(root_ready=0 OR root_node_id IS NOT NULL)
         );
         INSERT INTO whitelist_entries_v2
         SELECT w.handle,w.telegram_user_id,w.current_username,w.display_name,m.new_id,
                w.root_ready,w.can_add_users,w.added_by_telegram_user_id,w.whitelisted_at,
                w.resolved_at,w.updated_at
         FROM whitelist_entries w
         JOIN temp.kweb_node_id_migration m ON m.old_id=w.root_node_id;

         CREATE TABLE telegram_group_roots_v2 (
             group_id TEXT PRIMARY KEY,
             root_node_id TEXT UNIQUE CHECK(root_node_id IS NULL OR length(root_node_id)=8),
             root_ready INTEGER NOT NULL DEFAULT 0 CHECK(root_ready IN (0,1)),
             created_at TEXT NOT NULL,
             updated_at TEXT NOT NULL,
             CHECK(root_ready=0 OR root_node_id IS NOT NULL)
         );
         INSERT INTO telegram_group_roots_v2
         SELECT g.group_id,m.new_id,g.root_ready,g.created_at,g.updated_at
         FROM telegram_group_roots g
         JOIN temp.kweb_node_id_migration m ON m.old_id=g.root_node_id;

         DROP TABLE kmap_system_roots;
         ALTER TABLE kmap_system_roots_v2 RENAME TO kmap_system_roots;
         DROP TABLE whitelist_entries;
         ALTER TABLE whitelist_entries_v2 RENAME TO whitelist_entries;
         DROP TABLE telegram_group_roots;
         ALTER TABLE telegram_group_roots_v2 RENAME TO telegram_group_roots;
         CREATE INDEX whitelist_entries_username ON whitelist_entries(current_username);
         COMMIT;
         PRAGMA foreign_keys=ON;",
    )?;
    let roots: i64 = database.query_row(
        "SELECT
           (SELECT COUNT(*) FROM kmap_system_roots) +
           (SELECT COUNT(*) FROM whitelist_entries) +
           (SELECT COUNT(*) FROM telegram_group_roots)",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        roots == 16,
        "identity root migration did not preserve 16 roots"
    );
    Ok(())
}

fn rewrite_active_conversations(
    path: &Path,
    mapping: &HashMap<[u8; 20], NodeId>,
) -> anyhow::Result<()> {
    let text_mapping = mapping
        .iter()
        .map(|(old, new)| (hex::encode(old), new.to_string()))
        .collect::<HashMap<_, _>>();
    let mut database =
        Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    let rows = {
        let mut statement = database.prepare(
            "SELECT id,state_json,summary_state_json FROM conversations WHERE phase='active'",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let transaction = database.transaction()?;
    for (id, state, summary) in rows {
        let state = rewrite_active_state(&state, &text_mapping)?;
        let summary = summary
            .as_deref()
            .map(|value| rewrite_active_state(value, &text_mapping))
            .transpose()?;
        transaction.execute(
            "UPDATE conversations SET state_json=?1,summary_state_json=?2 WHERE id=?3",
            params![state, summary, id],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn rewrite_active_state(
    encoded: &str,
    mapping: &HashMap<String, String>,
) -> anyhow::Result<String> {
    let mut value: Value = serde_json::from_str(encoded)?;
    rewrite_exact_ids(&mut value, mapping);
    if let Some(object) = value.as_object_mut() {
        object.insert("loadedNodeIds".into(), Value::Array(Vec::new()));
        if let Some(archive) = object.get_mut("archive").and_then(Value::as_object_mut) {
            archive.remove("context");
        }
    }
    Ok(serde_json::to_string(&value)?)
}

fn rewrite_exact_ids(value: &mut Value, mapping: &HashMap<String, String>) {
    match value {
        Value::String(text) => {
            if let Some(replacement) = mapping.get(text) {
                *text = replacement.clone();
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite_exact_ids(value, mapping);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                rewrite_exact_ids(value, mapping);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn purge_terminal_ingress(
    conversation_path: &Path,
    memory_path: &Path,
) -> anyhow::Result<(usize, usize)> {
    let conversations = Connection::open(conversation_path)?;
    let memory = Connection::open(memory_path)?;
    let conversation_count: i64 = conversations.query_row(
        "SELECT COUNT(*) FROM conversations WHERE phase='ingress_failed'",
        [],
        |row| row.get(0),
    )?;
    let memory_count: i64 = memory.query_row(
        "SELECT COUNT(*) FROM memory_ingress_jobs WHERE phase='ingress_failed'",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        conversation_count == 2 && memory_count == 2,
        "expected exactly two terminal failed conversations and two mirrored memory jobs"
    );
    let purged_memory = memory.execute(
        "DELETE FROM memory_ingress_jobs WHERE phase='ingress_failed'",
        [],
    )?;
    let purged_conversations =
        conversations.execute("DELETE FROM conversations WHERE phase='ingress_failed'", [])?;
    Ok((purged_conversations, purged_memory))
}

fn archive_legacy_files(
    options: &MigrationOptions,
    writer: WriterId,
    nodes: usize,
) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(&options.archive_directory).with_context(|| {
        format!(
            "creating legacy archive {}",
            options.archive_directory.display()
        )
    })?;
    let database_name = options
        .legacy_database
        .file_name()
        .context("legacy database has no filename")?;
    for source in [
        options.legacy_database.clone(),
        PathBuf::from(format!("{}-wal", options.legacy_database.display())),
        PathBuf::from(format!("{}-shm", options.legacy_database.display())),
    ] {
        if source.exists() {
            let destination = options.archive_directory.join(
                source
                    .file_name()
                    .context("legacy SQLite sidecar has no filename")?,
            );
            fs::rename(&source, &destination)
                .with_context(|| format!("moving legacy Kweb file {}", source.display()))?;
        }
    }
    if options.legacy_artifacts.exists() {
        let destination = options.archive_directory.join(
            options
                .legacy_artifacts
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("kweb-provenance-artifacts")),
        );
        fs::rename(&options.legacy_artifacts, &destination).with_context(|| {
            format!(
                "moving legacy provenance artifacts {}",
                options.legacy_artifacts.display()
            )
        })?;
    }
    let readme = format!(
        "Legacy kweb-db-core archive\n\nMigrated at: {}\nImported current nodes: {nodes}\nMigration writer public key: {writer}\n\nThe legacy SQLite database and provenance artifacts are archival only. Kennedy no longer reads them at runtime.\n",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    let mut file = File::create(options.archive_directory.join("README.txt"))?;
    file.write_all(readme.as_bytes())?;
    file.sync_all()?;
    let _ = database_name;
    sync_directory(&options.archive_directory)?;
    Ok(options.archive_directory.clone())
}

pub(crate) fn install_permanent_writer(
    root: &Path,
    permanent_writer: WriterId,
) -> anyhow::Result<Vec<WriterId>> {
    let state_path = root.join("state.kws");
    let bytes =
        fs::read(&state_path).with_context(|| format!("reading {}", state_path.display()))?;
    let (prefix, existing) = decode_state_writer_section(&bytes)?;
    if existing.first() == Some(&permanent_writer) {
        return Ok(existing);
    }
    ensure!(
        !existing.contains(&permanent_writer),
        "the permanent writer is configured at the wrong priority"
    );
    let mut writers = vec![permanent_writer];
    writers.extend(existing);
    let mut payload = prefix;
    put_u64(
        &mut payload,
        u64::try_from(writers.len()).context("too many configured writers")?,
    );
    for writer in &writers {
        payload.extend_from_slice(&writer.to_bytes());
    }
    let encoded = encode_state_record(&payload);
    write_atomic(&state_path, &encoded)?;
    Ok(writers)
}

fn decode_state_writer_section(bytes: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<WriterId>)> {
    ensure!(bytes.len() >= 48, "Kweb state record is truncated");
    ensure!(
        &bytes[..8] == STATE_MAGIC,
        "Kweb state record has unknown magic"
    );
    let payload_length = u64::from_be_bytes(bytes[8..16].try_into()?) as usize;
    ensure!(
        bytes.len() == 16 + payload_length + 32,
        "Kweb state record length is invalid"
    );
    ensure!(
        Sha256::digest(&bytes[..16 + payload_length]).as_slice() == &bytes[16 + payload_length..],
        "Kweb state checksum is invalid"
    );
    let payload = &bytes[16..16 + payload_length];
    ensure!(payload.len() >= 28, "Kweb state payload is truncated");
    let mut offset = 4 + 8 + 8;
    let heads = read_u64(payload, &mut offset)? as usize;
    offset = offset
        .checked_add(heads.checked_mul(32).context("head count overflow")?)
        .context("head offset overflow")?;
    ensure!(
        offset + 8 <= payload.len(),
        "Kweb state heads are truncated"
    );
    let prefix = payload[..offset].to_vec();
    let writer_count = read_u64(payload, &mut offset)? as usize;
    ensure!(
        offset + writer_count * 32 == payload.len(),
        "Kweb state writer list is invalid"
    );
    let mut writers = Vec::with_capacity(writer_count);
    for _ in 0..writer_count {
        let bytes: [u8; 32] = payload[offset..offset + 32].try_into()?;
        writers.push(WriterId::from_verifying_key(bytes).map_err(anyhow::Error::new)?);
        offset += 32;
    }
    ensure!(!writers.is_empty(), "Kweb state has no writers");
    Ok((prefix, writers))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    let end = offset.checked_add(8).context("state offset overflow")?;
    ensure!(end <= bytes.len(), "Kweb state field is truncated");
    let value = u64::from_be_bytes(bytes[*offset..end].try_into()?);
    *offset = end;
    Ok(value)
}

fn encode_state_record(payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16 + payload.len() + 32);
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&Sha256::digest(&bytes));
    bytes
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temporary = parent.join(format!(".state.kws.{}.tmp", Uuid::new_v4()));
    let result = (|| -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    File::open(path)
        .with_context(|| format!("opening directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing directory {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_writer_installation_preserves_the_migration_writer() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-kweb-writer-{}", Uuid::new_v4()));
        let migration_key = [7_u8; 32];
        let migration_writer = WriterId::from_signing_key(&migration_key);
        let database = KwebDb::open(
            &directory,
            Config {
                signing_key: migration_key,
                writers_by_priority: vec![migration_writer],
                gossip: Arc::new(NoopGossip),
            },
        )
        .unwrap();
        drop(database);

        let permanent_key = [8_u8; 32];
        let permanent_writer = WriterId::from_signing_key(&permanent_key);
        let writers = install_permanent_writer(&directory, permanent_writer).unwrap();
        assert_eq!(writers, vec![permanent_writer, migration_writer]);

        let reopened = KwebDb::open(
            &directory,
            Config {
                signing_key: permanent_key,
                writers_by_priority: writers,
                gossip: Arc::new(NoopGossip),
            },
        )
        .unwrap();
        drop(reopened);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
