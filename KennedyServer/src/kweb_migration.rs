use std::{
    collections::HashSet,
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use kweb::{IdempotencyId, Kmap, NewProvenance, NewProvenanceArtifact, NodeId, ProvenanceStorage};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct MigrationOptions {
    pub bind: String,
    pub source_database: PathBuf,
    pub destination_database: PathBuf,
    pub artifact_directory: PathBuf,
}

#[derive(Debug)]
pub(crate) struct MigrationReport {
    pub provenance_rows: u64,
    pub extracted_media_artifacts: u64,
    pub externally_stored_provenance_rows: u64,
    pub source_database_bytes: u64,
    pub destination_database_bytes: u64,
    pub artifact_bytes: u64,
}

pub(crate) async fn run(options: MigrationOptions) -> anyhow::Result<MigrationReport> {
    let _maintenance_guard = tokio::net::TcpListener::bind(&options.bind)
        .await
        .with_context(|| {
            format!(
                "binding migration lock {}; stop the running Kennedy server before migrating Kweb storage",
                options.bind
            )
        })?;
    tokio::task::spawn_blocking(move || migrate(&options))
        .await
        .context("the Kweb migration worker stopped unexpectedly")?
}

fn migrate(options: &MigrationOptions) -> anyhow::Result<MigrationReport> {
    ensure!(
        options.source_database.is_file(),
        "legacy Kweb database {} does not exist or is not a regular file",
        options.source_database.display()
    );
    ensure!(
        !options.destination_database.exists(),
        "destination Kweb database {} already exists; refusing to overwrite it",
        options.destination_database.display()
    );
    ensure!(
        !options.artifact_directory.exists(),
        "Kweb artifact directory {} already exists; refusing to overwrite it",
        options.artifact_directory.display()
    );
    ensure_distinct_paths(&options.source_database, &options.destination_database)?;
    let destination_parent = options
        .destination_database
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let artifact_parent = options
        .artifact_directory
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    ensure!(
        destination_parent == artifact_parent,
        "the Kweb database and provenance-artifact directory must be siblings"
    );
    fs::create_dir_all(destination_parent).with_context(|| {
        format!(
            "creating Kweb destination directory {}",
            destination_parent.display()
        )
    })?;

    let nonce = Uuid::new_v4();
    let destination_name = options
        .destination_database
        .file_name()
        .and_then(|name| name.to_str())
        .context("destination Kweb database must have a UTF-8 filename")?;
    let artifact_name = options
        .artifact_directory
        .file_name()
        .and_then(|name| name.to_str())
        .context("Kweb artifact directory must have a UTF-8 filename")?;
    let staging_database =
        destination_parent.join(format!(".{destination_name}.{nonce}.migration.sqlite3"));
    let staging_artifacts = destination_parent.join(format!(".{artifact_name}.{nonce}.migration"));
    fs::create_dir(&staging_artifacts).with_context(|| {
        format!(
            "creating Kweb artifact staging directory {}",
            staging_artifacts.display()
        )
    })?;
    set_directory_private(&staging_artifacts)?;

    let result = migrate_staged(
        &options.source_database,
        &staging_database,
        &staging_artifacts,
    );
    let mut report = match result {
        Ok(report) => report,
        Err(error) => {
            cleanup_staging(&staging_database, &staging_artifacts);
            return Err(error);
        }
    };

    fs::rename(&staging_artifacts, &options.artifact_directory).with_context(|| {
        format!(
            "publishing Kweb artifact directory {}",
            options.artifact_directory.display()
        )
    })?;
    if let Err(error) = fs::rename(&staging_database, &options.destination_database) {
        let _ = fs::rename(&options.artifact_directory, &staging_artifacts);
        cleanup_staging(&staging_database, &staging_artifacts);
        return Err(error).with_context(|| {
            format!(
                "publishing Kweb database {}",
                options.destination_database.display()
            )
        });
    }
    sync_directory(destination_parent)?;
    report.destination_database_bytes = fs::metadata(&options.destination_database)?.len();
    Ok(report)
}

fn migrate_staged(
    source_path: &Path,
    destination_path: &Path,
    artifact_path: &Path,
) -> anyhow::Result<MigrationReport> {
    let source = Connection::open_with_flags(
        source_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening legacy Kweb database {}", source_path.display()))?;
    source.busy_timeout(std::time::Duration::from_secs(5))?;
    require_split_ready_schema(&source)?;
    let expected_counts = table_counts(&source)?;

    let mut kweb = Kmap::open_with_artifacts(destination_path, artifact_path)
        .map_err(anyhow::Error::new)
        .context("creating split Kweb database")?;
    let mut provenance_rows = 0_u64;
    let mut extracted_media_artifacts = 0_u64;
    {
        let mut statement = source.prepare(
            "SELECT lower(hex(id)),data,source,source_created_at
             FROM data_provenance_nodes ORDER BY rowid",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id_text = row.get::<_, String>(0)?;
            let data = row.get::<_, String>(1).with_context(|| {
                format!(
                    "legacy provenance {id_text} has no inline data; this migration only accepts the pre-split database"
                )
            })?;
            let source_name = row.get::<_, String>(2)?;
            let source_created_at = row.get::<_, String>(3)?;
            let (data, artifacts) = extract_embedded_media(data)?;
            extracted_media_artifacts = extracted_media_artifacts
                .checked_add(artifacts.len() as u64)
                .context("counting extracted provenance artifacts")?;
            let id = NodeId::from_hex(&id_text).map_err(anyhow::Error::new)?;
            kweb.import_provenance_for_migration(
                id,
                IdempotencyId::random(),
                NewProvenance {
                    data,
                    source: source_name.clone(),
                    source_created_at,
                },
                ProvenanceStorage {
                    data_filename: archive_filename(&source_name),
                    artifacts,
                },
            )
            .map_err(anyhow::Error::new)
            .with_context(|| format!("migrating provenance {id_text}"))?;
            provenance_rows = provenance_rows
                .checked_add(1)
                .context("counting migrated provenance rows")?;
        }
    }
    drop(kweb);

    let destination = Connection::open(destination_path).with_context(|| {
        format!(
            "opening staged Kweb database {}",
            destination_path.display()
        )
    })?;
    destination.busy_timeout(std::time::Duration::from_secs(5))?;
    copy_relational_tables(&destination, source_path, &source)?;
    verify_table_counts(&destination, &expected_counts)?;
    verify_sqlite(&destination, destination_path)?;
    let externally_stored_provenance_rows = destination.query_row(
        "SELECT COUNT(*) FROM data_provenance_nodes WHERE data_artifact_path IS NOT NULL",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let externally_stored_provenance_rows = u64::try_from(externally_stored_provenance_rows)
        .context("external provenance row count is negative")?;
    verify_artifacts(&destination, artifact_path)?;
    destination
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE; VACUUM;")?;
    verify_sqlite(&destination, destination_path)?;
    drop(destination);
    ensure!(
        !sidecar_path(destination_path, "-wal").exists()
            && !sidecar_path(destination_path, "-shm").exists(),
        "staged Kweb database unexpectedly requires SQLite sidecar files"
    );
    set_file_private(destination_path)?;
    File::open(destination_path)?.sync_all()?;
    sync_directory(artifact_path)?;
    sync_directory(
        destination_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;

    Ok(MigrationReport {
        provenance_rows,
        extracted_media_artifacts,
        externally_stored_provenance_rows,
        source_database_bytes: fs::metadata(source_path)?.len(),
        destination_database_bytes: fs::metadata(destination_path)?.len(),
        artifact_bytes: directory_file_bytes(artifact_path)?,
    })
}

fn require_split_ready_schema(connection: &Connection) -> anyhow::Result<()> {
    for table in [
        "data_provenance_nodes",
        "knowledge_nodes",
        "data_history_nodes",
        "fixed_connections",
        "recent_connections",
        "idempotency_receipts",
    ] {
        let exists = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
            [table],
            |row| row.get::<_, bool>(0),
        )?;
        ensure!(
            exists,
            "legacy Kweb database is missing required table {table}; run the normal schema migration before splitting storage"
        );
    }
    let provenance_columns = table_columns(connection, "data_provenance_nodes")?;
    ensure!(
        provenance_columns.contains("data") && !provenance_columns.contains("data_artifact_path"),
        "source Kweb database is not the expected pre-split schema"
    );
    Ok(())
}

fn table_columns(connection: &Connection, table: &str) -> anyhow::Result<HashSet<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    Ok(statement
        .query_map([], |row| row.get(1))?
        .collect::<Result<HashSet<_>, _>>()?)
}

fn extract_embedded_media(data: String) -> anyhow::Result<(String, Vec<NewProvenanceArtifact>)> {
    let Ok(mut archive) = serde_json::from_str::<Value>(&data) else {
        return Ok((data, Vec::new()));
    };
    let Some(media) = archive.get_mut("media").and_then(Value::as_array_mut) else {
        return Ok((data, Vec::new()));
    };
    let mut artifacts = Vec::new();
    for (position, item) in media.iter_mut().enumerate() {
        let Some(object) = item.as_object_mut() else {
            continue;
        };
        let Some(data_url) = object
            .get("dataUrl")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            continue;
        };
        let (url_media_type, bytes) = decode_data_url(&data_url)
            .with_context(|| format!("decoding embedded provenance media item {}", position + 1))?;
        let media_type = object
            .get("mimeType")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&url_media_type)
            .to_owned();
        let filename = object
            .get("fileName")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(safe_basename)
            .unwrap_or_else(|| generated_media_filename(position, &media_type));
        let artifact_index = artifacts.len();
        object.remove("dataUrl");
        object.insert(
            "provenanceArtifactIndex".into(),
            Value::from(artifact_index as u64),
        );
        artifacts.push(NewProvenanceArtifact {
            original_filename: filename,
            media_type,
            role: "media".into(),
            data: bytes,
        });
    }
    if artifacts.is_empty() {
        return Ok((data, artifacts));
    }
    Ok((serde_json::to_string(&archive)?, artifacts))
}

fn decode_data_url(value: &str) -> anyhow::Result<(String, Vec<u8>)> {
    let (header, encoded) = value
        .split_once(',')
        .context("embedded media is not a data URL")?;
    let metadata = header
        .strip_prefix("data:")
        .context("embedded media is not a data URL")?;
    ensure!(
        metadata
            .split(';')
            .any(|parameter| parameter.eq_ignore_ascii_case("base64")),
        "embedded media data URL is not Base64 encoded"
    );
    let media_type = metadata
        .split(';')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let bytes = STANDARD
        .decode(encoded)
        .context("embedded media contains invalid Base64")?;
    Ok((media_type, bytes))
}

fn safe_basename(value: &str) -> String {
    value
        .rsplit(['/', '\\'])
        .find(|component| !component.is_empty())
        .unwrap_or("provenance-media")
        .to_owned()
}

fn generated_media_filename(position: usize, media_type: &str) -> String {
    let extension = match media_type.split(';').next().unwrap_or_default() {
        "audio/ogg" => "ogg",
        "audio/mpeg" => "mp3",
        "audio/mp4" => "m4a",
        "audio/wav" | "audio/x-wav" => "wav",
        "audio/webm" => "webm",
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "application/pdf" => "pdf",
        _ => "bin",
    };
    format!("provenance-media-{}.{extension}", position + 1)
}

fn archive_filename(source: &str) -> String {
    let mut safe = source
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .take(180)
        .collect::<String>();
    if safe.is_empty() {
        safe.push_str("provenance");
    }
    format!("{safe}-archive.json")
}

fn copy_relational_tables(
    destination: &Connection,
    source_path: &Path,
    source: &Connection,
) -> anyhow::Result<()> {
    destination.execute_batch("PRAGMA foreign_keys=OFF;")?;
    destination.execute(
        "ATTACH DATABASE ?1 AS legacy",
        [source_path.to_string_lossy().as_ref()],
    )?;
    let copy_result = (|| -> anyhow::Result<()> {
        destination.execute_batch(
            "BEGIN IMMEDIATE;
             INSERT INTO knowledge_nodes(id,short_name,short_description,long_description,history_head_id,owner_node_id,last_modified_by,last_modified_at)
             SELECT id,short_name,short_description,long_description,history_head_id,owner_node_id,last_modified_by,last_modified_at FROM legacy.knowledge_nodes;
             INSERT INTO data_history_nodes(id,knowledge_node_id,previous_history_id,provenance_id)
             SELECT id,knowledge_node_id,previous_history_id,provenance_id FROM legacy.data_history_nodes;
             INSERT INTO fixed_connections(source_node_id,target_node_id,position)
             SELECT source_node_id,target_node_id,position FROM legacy.fixed_connections;
             INSERT INTO recent_connections(source_node_id,target_node_id,position)
             SELECT source_node_id,target_node_id,position FROM legacy.recent_connections;
             INSERT INTO idempotency_receipts(id,operation_kind,request_hash,result_id,committed_at)
             SELECT id,operation_kind,request_hash,result_id,committed_at FROM legacy.idempotency_receipts;",
        )?;
        if table_exists(source, "kmap_roots")? {
            destination.execute_batch(
                "CREATE TABLE kmap_roots (
                     role TEXT PRIMARY KEY CHECK(role IN ('user','kennedy')),
                     knowledge_node_id BLOB NOT NULL UNIQUE CHECK(length(knowledge_node_id)=20)
                 );
                 INSERT INTO kmap_roots(role,knowledge_node_id)
                 SELECT role,knowledge_node_id FROM legacy.kmap_roots;",
            )?;
        }
        destination.execute_batch("COMMIT;")?;
        Ok(())
    })();
    if copy_result.is_err() {
        let _ = destination.execute_batch("ROLLBACK;");
    }
    let detach_result = destination.execute_batch("DETACH DATABASE legacy;");
    destination.execute_batch("PRAGMA foreign_keys=ON;")?;
    copy_result?;
    detach_result?;
    Ok(())
}

fn table_exists(connection: &Connection, name: &str) -> anyhow::Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [name],
        |row| row.get(0),
    )?)
}

fn table_counts(connection: &Connection) -> anyhow::Result<Vec<(&'static str, u64)>> {
    [
        "data_provenance_nodes",
        "knowledge_nodes",
        "data_history_nodes",
        "fixed_connections",
        "recent_connections",
        "idempotency_receipts",
    ]
    .into_iter()
    .map(|table| {
        let count = connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })?;
        let count = u64::try_from(count).context("Kweb table count is negative")?;
        Ok((table, count))
    })
    .collect()
}

fn verify_table_counts(
    connection: &Connection,
    expected: &[(&'static str, u64)],
) -> anyhow::Result<()> {
    for (table, expected_count) in expected {
        let actual = connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })?;
        let actual = u64::try_from(actual).context("Kweb table count is negative")?;
        ensure!(
            actual == *expected_count,
            "Kweb migration count mismatch for {table}: expected {expected_count}, found {actual}"
        );
    }
    Ok(())
}

fn verify_sqlite(connection: &Connection, path: &Path) -> anyhow::Result<()> {
    let integrity =
        connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?;
    ensure!(
        integrity == "ok",
        "SQLite integrity check failed for {}: {integrity}",
        path.display()
    );
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    let mut rows = statement.query([])?;
    ensure!(
        rows.next()?.is_none(),
        "SQLite foreign-key check failed for {}",
        path.display()
    );
    Ok(())
}

fn verify_artifacts(connection: &Connection, root: &Path) -> anyhow::Result<()> {
    let mut expected = HashSet::new();
    let mut statement = connection.prepare(
        "SELECT relative_path,byte_length,sha256 FROM provenance_artifacts ORDER BY relative_path",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    })?;
    for row in rows {
        let (relative_path, expected_length, expected_sha256) = row?;
        let expected_length =
            u64::try_from(expected_length).context("Kweb artifact byte length is negative")?;
        let path = checked_artifact_path(root, &relative_path)?;
        let metadata = fs::metadata(&path)
            .with_context(|| format!("reading migrated artifact {}", path.display()))?;
        ensure!(
            metadata.is_file() && metadata.len() == expected_length,
            "migrated artifact {} has the wrong size",
            path.display()
        );
        let mut file = File::open(&path)?;
        let mut hash = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        ensure!(
            hash.finalize().as_slice() == expected_sha256,
            "migrated artifact {} failed its SHA-256 check",
            path.display()
        );
        expected.insert(relative_path);
    }
    let actual = collect_relative_files(root)?;
    ensure!(
        actual == expected,
        "Kweb artifact directory contains files not represented by the database"
    );
    Ok(())
}

fn checked_artifact_path(root: &Path, relative: &str) -> anyhow::Result<PathBuf> {
    let components = Path::new(relative).components().collect::<Vec<_>>();
    ensure!(
        components.len() == 2
            && components
                .iter()
                .all(|component| matches!(component, Component::Normal(_))),
        "database contains unsafe Kweb artifact path {relative}"
    );
    Ok(root.join(relative))
}

fn collect_relative_files(root: &Path) -> anyhow::Result<HashSet<String>> {
    let mut files = HashSet::new();
    for shard in fs::read_dir(root)? {
        let shard = shard?;
        let metadata = fs::symlink_metadata(shard.path())?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "unexpected non-directory in Kweb artifact root {}",
            shard.path().display()
        );
        for artifact in fs::read_dir(shard.path())? {
            let artifact = artifact?;
            let metadata = fs::symlink_metadata(artifact.path())?;
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "unexpected non-file in Kweb artifact shard {}",
                artifact.path().display()
            );
            files.insert(format!(
                "{}/{}",
                shard.file_name().to_string_lossy(),
                artifact.file_name().to_string_lossy()
            ));
        }
    }
    Ok(files)
}

fn directory_file_bytes(root: &Path) -> anyhow::Result<u64> {
    let mut total = 0_u64;
    for shard in fs::read_dir(root)? {
        let shard = shard?;
        if shard.file_type()?.is_dir() {
            for artifact in fs::read_dir(shard.path())? {
                let artifact = artifact?;
                if artifact.file_type()?.is_file() {
                    total = total
                        .checked_add(artifact.metadata()?.len())
                        .context("counting Kweb artifact bytes")?;
                }
            }
        }
    }
    Ok(total)
}

fn ensure_distinct_paths(source: &Path, destination: &Path) -> anyhow::Result<()> {
    let source = fs::canonicalize(source)?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let destination = fs::canonicalize(parent)?.join(
        destination
            .file_name()
            .context("destination Kweb database has no filename")?,
    );
    ensure!(
        source != destination,
        "source and destination Kweb database paths are the same"
    );
    Ok(())
}

fn cleanup_staging(database: &Path, artifacts: &Path) {
    let _ = fs::remove_file(database);
    let _ = fs::remove_file(sidecar_path(database, "-wal"));
    let _ = fs::remove_file(sidecar_path(database, "-shm"));
    let _ = fs::remove_dir_all(artifacts);
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{}", path.display(), suffix))
}

fn set_directory_private(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_file_private(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_embedded_media_and_keeps_filename() {
        let data = serde_json::json!({
            "messages": [],
            "media": [{
                "fileName": "telegram-vnote.wav",
                "mimeType": "audio/wav",
                "dataUrl": "data:audio/wav;base64,aGVsbG8="
            }]
        })
        .to_string();
        let (data, artifacts) = extract_embedded_media(data).unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].original_filename, "telegram-vnote.wav");
        assert_eq!(artifacts[0].data, b"hello");
        let rewritten: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(rewritten["media"][0]["provenanceArtifactIndex"], 0);
        assert!(rewritten["media"][0].get("dataUrl").is_none());
    }

    #[test]
    fn migrates_pre_split_database_without_changing_source() {
        let directory =
            std::env::temp_dir().join(format!("kweb-storage-migration-test-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let source_path = directory.join("kennedy.sqlite3");
        let destination_path = directory.join("kweb.sqlite3");
        let artifact_path = directory.join("kweb-provenance-artifacts");
        let provenance_id = NodeId::random();
        let node_id = NodeId::random();
        let history_id = NodeId::random();
        let original_data = serde_json::json!({
            "messages": [],
            "media": [{
                "fileName": "telegram-vnote.wav",
                "mimeType": "audio/wav",
                "dataUrl": "data:audio/wav;base64,aGVsbG8="
            }]
        })
        .to_string();
        let source = Connection::open(&source_path).unwrap();
        source
            .execute_batch(
                "PRAGMA foreign_keys=ON;
                 CREATE TABLE data_provenance_nodes (
                     id BLOB PRIMARY KEY CHECK(length(id)=20), data TEXT NOT NULL,
                     source TEXT NOT NULL, source_created_at TEXT NOT NULL
                 );
                 CREATE TABLE knowledge_nodes (
                     id BLOB PRIMARY KEY CHECK(length(id)=20), short_name TEXT NOT NULL,
                     short_description TEXT NOT NULL, long_description TEXT NOT NULL,
                     history_head_id BLOB, owner_node_id BLOB, last_modified_by TEXT NOT NULL,
                     last_modified_at TEXT
                 );
                 CREATE TABLE data_history_nodes (
                     id BLOB PRIMARY KEY CHECK(length(id)=20), knowledge_node_id BLOB NOT NULL,
                     previous_history_id BLOB, provenance_id BLOB NOT NULL
                 );
                 CREATE TABLE fixed_connections (
                     source_node_id BLOB NOT NULL, target_node_id BLOB NOT NULL,
                     position INTEGER NOT NULL, PRIMARY KEY(source_node_id,target_node_id)
                 );
                 CREATE TABLE recent_connections (
                     source_node_id BLOB NOT NULL, target_node_id BLOB NOT NULL,
                     position INTEGER NOT NULL, PRIMARY KEY(source_node_id,target_node_id)
                 );
                 CREATE TABLE idempotency_receipts (
                     id BLOB PRIMARY KEY, operation_kind TEXT NOT NULL,
                     request_hash BLOB NOT NULL, result_id BLOB NOT NULL,
                     committed_at TEXT NOT NULL
                 );
                 CREATE TABLE kmap_roots (
                     role TEXT PRIMARY KEY, knowledge_node_id BLOB NOT NULL UNIQUE
                 );",
            )
            .unwrap();
        source
            .execute(
                &format!(
                    "INSERT INTO data_provenance_nodes VALUES(X'{provenance_id}',?1,'conversation-history','2026-07-18T00:00:00Z')"
                ),
                [&original_data],
            )
            .unwrap();
        source
            .execute_batch(&format!(
                "INSERT INTO knowledge_nodes VALUES(X'{node_id}','Test Node','','',NULL,X'{node_id}','test','2026-07-18T00:00:00Z');
                 INSERT INTO data_history_nodes VALUES(X'{history_id}',X'{node_id}',NULL,X'{provenance_id}');
                 UPDATE knowledge_nodes SET history_head_id=X'{history_id}' WHERE id=X'{node_id}';
                 INSERT INTO kmap_roots VALUES('user',X'{node_id}');"
            ))
            .unwrap();
        drop(source);

        let report = migrate(&MigrationOptions {
            bind: "127.0.0.1:0".into(),
            source_database: source_path.clone(),
            destination_database: destination_path.clone(),
            artifact_directory: artifact_path.clone(),
        })
        .unwrap();
        assert_eq!(report.provenance_rows, 1);
        assert_eq!(report.extracted_media_artifacts, 1);

        let source = Connection::open(&source_path).unwrap();
        let retained: String = source
            .query_row("SELECT data FROM data_provenance_nodes", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(retained.contains("data:audio/wav;base64"));
        drop(source);

        let migrated = Kmap::open_with_artifacts(&destination_path, &artifact_path).unwrap();
        let provenance = migrated.get_provenance(provenance_id).unwrap();
        assert!(!provenance.data.contains("data:audio/wav;base64"));
        assert_eq!(provenance.artifacts.len(), 1);
        assert_eq!(
            provenance.artifacts[0].original_filename,
            "telegram-vnote.wav"
        );
        let (_, filename) = provenance.artifacts[0]
            .relative_path
            .split_once('/')
            .unwrap();
        assert!(filename.starts_with("telegram-vnote."));
        assert!(filename.ends_with(".wav"));
        assert_eq!(
            migrated.get_node_history(node_id).unwrap(),
            vec![provenance_id]
        );
        drop(migrated);
        fs::remove_dir_all(directory).unwrap();
    }
}
