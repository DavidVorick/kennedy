use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use axum::{
    Router,
    http::{StatusCode, header},
    response::Html,
    routing::get,
};
use chrono::{DateTime, SecondsFormat, Utc};
use flate2::{Compression, GzBuilder};
use rusqlite::{Connection, MAIN_DB, OpenFlags};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use uuid::Uuid;

const BACKUP_FORMAT_VERSION: u32 = 13;
const ARCHIVE_PREFIX: &str = "kennedy-backup";
const BACKUP_PAGE: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>Kennedy backup in progress</title>
  <style>body{font:18px system-ui,sans-serif;max-width:42rem;margin:15vh auto;padding:0 1.5rem;line-height:1.5;color:#252525}h1{font-size:1.8rem}</style>
</head>
<body><h1>Kennedy backup in progress</h1><p>Kennedy is temporarily offline while a verified backup is created. Please come back in a few minutes.</p></body>
</html>"#;

#[derive(Clone, Debug)]
pub(crate) struct BackupOptions {
    pub bind: String,
    pub backup_dir: PathBuf,
    pub kweb_root: PathBuf,
    pub include_kweb_objects: bool,
    /// Optional offline legacy archive source. New runtimes do not recreate it.
    pub conversation_database: PathBuf,
    pub session_directory: PathBuf,
    pub session_history_file: PathBuf,
    pub telegram_database: PathBuf,
    pub user_database: PathBuf,
    /// Optional pre-library AudioIngress database retained as migration evidence.
    pub legacy_audio_database: Option<PathBuf>,
    pub audio_memory_ingress_database: PathBuf,
    pub audio_ingress_directory: PathBuf,
    pub vault: PathBuf,
}

#[derive(Serialize)]
struct Manifest {
    backup_format_version: u32,
    kennedy_commit: String,
    kennedy_version: String,
    build_source_dirty: String,
    created_at: String,
    archive_root: String,
    snapshot_mode: &'static str,
    guard_bind: String,
    sqlite_version: String,
    kweb_objects_included: bool,
    files: Vec<ManifestFile>,
}

#[derive(Serialize)]
struct ManifestFile {
    path: String,
    source_path: String,
    role: &'static str,
    size_bytes: u64,
    sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlite: Option<SqliteMetadata>,
}

#[derive(Serialize)]
struct SqliteMetadata {
    user_version: i64,
    application_id: i64,
    schema_version: i64,
    page_size: i64,
    page_count: i64,
    schema: Vec<SchemaObject>,
}

#[derive(Clone, Serialize)]
struct SchemaObject {
    object_type: String,
    name: String,
    table_name: String,
    sql: String,
}

pub(crate) async fn run(options: BackupOptions) -> anyhow::Result<PathBuf> {
    let listener = tokio::net::TcpListener::bind(&options.bind)
        .await
        .with_context(|| {
            format!(
                "binding the backup guard page to {}; stop the running Kennedy server before creating a backup",
                options.bind
            )
        })?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let page = backup_page_router();
    let page_task = tokio::spawn(async move {
        axum::serve(listener, page)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let worker_options = options.clone();
    let backup_join =
        tokio::task::spawn_blocking(move || create_archive(&worker_options, Utc::now())).await;

    let _ = shutdown_tx.send(());
    let page_result = page_task
        .await
        .context("the backup guard page stopped unexpectedly")?;
    let backup_result = backup_join.context("the backup worker stopped unexpectedly")?;
    page_result.context("serving the backup guard page")?;
    backup_result
}

fn backup_page_router() -> Router {
    Router::new().fallback(get(|| async {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                (header::CACHE_CONTROL, "no-store"),
                (header::RETRY_AFTER, "30"),
            ],
            Html(BACKUP_PAGE),
        )
    }))
}

fn create_archive(options: &BackupOptions, created_at: DateTime<Utc>) -> anyhow::Result<PathBuf> {
    validate_kweb_root(&options.kweb_root)?;
    if options.conversation_database.exists() {
        validate_database_source(
            &options.conversation_database,
            "legacy conversation history",
        )?;
    }
    ensure!(
        options.session_directory.is_dir(),
        "in-progress session directory {} does not exist",
        options.session_directory.display()
    );
    ensure!(
        options.session_history_file.is_file(),
        "Session History object-ID list {} does not exist",
        options.session_history_file.display()
    );
    validate_database_source(&options.telegram_database, "Telegram relay")?;
    validate_database_source(&options.user_database, "user directory")?;
    ensure!(
        options.audio_ingress_directory.is_dir(),
        "AudioIngress persistence root {} does not exist or is not a directory",
        options.audio_ingress_directory.display()
    );
    let audio_database = options.audio_ingress_directory.join("state.sqlite3");
    validate_database_source(&audio_database, "AudioIngress")?;
    validate_database_source(
        &options.audio_memory_ingress_database,
        "Kennedy audio memory ingress",
    )?;
    if let Some(path) = options
        .legacy_audio_database
        .as_deref()
        .filter(|path| path.exists())
    {
        validate_database_source(path, "legacy AudioIngress")?;
    }
    if options.vault.exists() {
        ensure!(
            options.vault.is_file(),
            "credential vault {} is not a regular file",
            options.vault.display()
        );
    }

    let backup_directory_existed = options.backup_dir.exists();
    fs::create_dir_all(&options.backup_dir)
        .with_context(|| format!("creating backup directory {}", options.backup_dir.display()))?;
    if !backup_directory_existed {
        set_directory_private(&options.backup_dir)?;
    }

    let timestamp = created_at.format("%Y-%m-%dT%H-%M-%SZ").to_string();
    let archive_root = format!("{ARCHIVE_PREFIX}-{timestamp}");
    let final_path = options.backup_dir.join(format!("{archive_root}.tar.gz"));
    ensure!(
        !final_path.exists(),
        "backup {} already exists; wait one second before retrying",
        final_path.display()
    );

    let nonce = Uuid::new_v4();
    let staging = options
        .backup_dir
        .join(format!(".{archive_root}.{nonce}.staging"));
    let temporary_archive = options
        .backup_dir
        .join(format!(".{archive_root}.{nonce}.tar.gz.tmp"));
    let mut published = false;
    let result = (|| -> anyhow::Result<()> {
        fs::create_dir(&staging)
            .with_context(|| format!("creating staging directory {}", staging.display()))?;
        set_directory_private(&staging)?;
        let archive_directory = staging.join(&archive_root);
        let data_directory = archive_directory.join("data");
        fs::create_dir_all(&data_directory)
            .with_context(|| format!("creating {}", data_directory.display()))?;
        set_directory_private(&archive_directory)?;
        set_directory_private(&data_directory)?;

        // These stores form one quiescent recovery point because the server
        // address is held before this worker starts and a normal server binds
        // that address before opening any persistent state.
        let audio_directory = data_directory.join("audio-ingress");
        fs::create_dir_all(&audio_directory)?;
        let mut files = vec![
            snapshot_database(
                &audio_database,
                &audio_directory.join("state.sqlite3"),
                "data/audio-ingress/state.sqlite3",
                "AudioIngress-owned recording and transcript database",
            )?,
            snapshot_database(
                &options.audio_memory_ingress_database,
                &data_directory.join("audio-memory-ingress.sqlite3"),
                "data/audio-memory-ingress.sqlite3",
                "Kennedy-owned audio transcript memory-ingress queue",
            )?,
            snapshot_database(
                &options.user_database,
                &data_directory.join("users.sqlite3"),
                "data/users.sqlite3",
                "user directory database",
            )?,
            snapshot_database(
                &options.telegram_database,
                &data_directory.join("telegram.sqlite3"),
                "data/telegram.sqlite3",
                "Telegram relay database",
            )?,
        ];
        if let Some(path) = options
            .legacy_audio_database
            .as_deref()
            .filter(|path| path.exists())
        {
            files.push(snapshot_database(
                path,
                &data_directory.join("legacy-audio-ingress.sqlite3"),
                "data/legacy-audio-ingress.sqlite3",
                "pre-library AudioIngress migration source",
            )?);
        }
        if options.conversation_database.exists() {
            files.push(snapshot_database(
                &options.conversation_database,
                &data_directory.join("legacy-conversations.sqlite3"),
                "data/legacy-conversations.sqlite3",
                "offline legacy conversation history database",
            )?);
        }
        copy_persistent_directory(
            &options.session_directory,
            &data_directory.join("sessions"),
            "data/sessions",
            "in-progress session log, pending object, or Session History control journal",
            &mut files,
        )?;
        let session_history_destination = data_directory.join("session-history.txt");
        fs::copy(&options.session_history_file, &session_history_destination).with_context(
            || {
                format!(
                    "copying Session History list {}",
                    options.session_history_file.display()
                )
            },
        )?;
        set_file_private(&session_history_destination)?;
        sync_file(&session_history_destination)?;
        files.push(manifest_file(
            &session_history_destination,
            "data/session-history.txt",
            &options.session_history_file,
            "completed Session History Kweb object-ID list",
            None,
        )?);
        copy_kweb_root(
            &options.kweb_root,
            &data_directory.join("kweb"),
            "data/kweb",
            options.include_kweb_objects,
            &mut files,
        )?;
        for name in ["originals", "chunks"] {
            let source = options.audio_ingress_directory.join(name);
            if source.exists() {
                copy_persistent_directory(
                    &source,
                    &audio_directory.join(name),
                    &format!("data/audio-ingress/{name}"),
                    "AudioIngress-owned retained audio",
                    &mut files,
                )?;
            }
        }
        if options.vault.exists() {
            let destination = data_directory.join("kennedy-secrets.age");
            fs::copy(&options.vault, &destination).with_context(|| {
                format!(
                    "copying encrypted credential vault {}",
                    options.vault.display()
                )
            })?;
            set_file_private(&destination)?;
            sync_file(&destination)?;
            files.push(manifest_file(
                &destination,
                "data/kennedy-secrets.age",
                &options.vault,
                "encrypted Kennedy credential vault",
                None,
            )?);
        }

        files.sort_by(|left, right| left.path.cmp(&right.path));
        let manifest = Manifest {
            backup_format_version: BACKUP_FORMAT_VERSION,
            kennedy_commit: env!("KENNEDY_GIT_COMMIT").to_owned(),
            kennedy_version: env!("CARGO_PKG_VERSION").to_owned(),
            build_source_dirty: env!("KENNEDY_GIT_DIRTY").to_owned(),
            created_at: created_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            archive_root: archive_root.clone(),
            snapshot_mode: "offline-port-guard",
            guard_bind: options.bind.clone(),
            sqlite_version: rusqlite::version().to_owned(),
            kweb_objects_included: options.include_kweb_objects,
            files,
        };
        write_private(
            &archive_directory.join("manifest.json"),
            &serde_json::to_vec_pretty(&manifest).context("serializing backup manifest")?,
        )?;
        let readme = render_readme(&manifest);
        write_private(&archive_directory.join("README.md"), readme.as_bytes())?;

        write_tarball(
            &temporary_archive,
            &staging,
            &archive_root,
            &archive_directory,
        )?;
        fs::remove_dir_all(&staging)
            .with_context(|| format!("removing staging directory {}", staging.display()))?;
        fs::hard_link(&temporary_archive, &final_path).with_context(|| {
            format!(
                "publishing backup {} as {}",
                temporary_archive.display(),
                final_path.display()
            )
        })?;
        published = true;
        fs::remove_file(&temporary_archive).with_context(|| {
            format!(
                "removing temporary archive name {}",
                temporary_archive.display()
            )
        })?;
        sync_directory(&options.backup_dir)?;
        Ok(())
    })();

    let _ = fs::remove_dir_all(&staging);
    if result.is_err() {
        let _ = fs::remove_file(&temporary_archive);
        if published {
            let _ = fs::remove_file(&final_path);
        }
    }
    result?;
    Ok(final_path)
}

fn validate_database_source(path: &Path, label: &str) -> anyhow::Result<()> {
    ensure!(
        path.exists(),
        "{label} database {} does not exist; refusing to create an incomplete backup",
        path.display()
    );
    ensure!(
        path.is_file(),
        "{label} database {} is not a regular file",
        path.display()
    );
    Ok(())
}

fn validate_kweb_root(path: &Path) -> anyhow::Result<()> {
    ensure!(
        path.is_dir(),
        "Kweb root {} does not exist or is not a directory; refusing to create an incomplete backup",
        path.display()
    );
    let format = fs::read(path.join("FORMAT"))
        .with_context(|| format!("reading Kweb FORMAT marker in {}", path.display()))?;
    ensure!(
        format.as_slice() == b"KWDBROOT\0\0\0\0\0\0\0\x03",
        "Kweb root {} does not have the kcode-kweb-db 1.0 root-format marker",
        path.display()
    );
    for entry in ["state.kws", "transactions.kwl", "nodes", "objects"] {
        ensure!(
            path.join(entry).exists(),
            "Kweb root {} is missing required entry {entry}",
            path.display()
        );
    }
    Ok(())
}

fn snapshot_database(
    source_path: &Path,
    destination_path: &Path,
    archive_path: &str,
    role: &'static str,
) -> anyhow::Result<ManifestFile> {
    tracing::info!(source=%source_path.display(), archive_path, "Creating SQLite backup snapshot");
    let source = Connection::open_with_flags(
        source_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {} read-only", source_path.display()))?;
    source
        .busy_timeout(std::time::Duration::from_secs(5))
        .context("configuring the SQLite backup source")?;
    source
        .backup(MAIN_DB, destination_path, None)
        .with_context(|| {
            format!(
                "snapshotting {} as {}",
                source_path.display(),
                destination_path.display()
            )
        })?;
    drop(source);

    let snapshot = Connection::open(destination_path)
        .with_context(|| format!("opening snapshot {}", destination_path.display()))?;
    snapshot
        .pragma_update(None, "journal_mode", "DELETE")
        .with_context(|| {
            format!(
                "making {} a standalone SQLite file",
                destination_path.display()
            )
        })?;
    verify_integrity(&snapshot, destination_path)?;
    let sqlite = inspect_sqlite(&snapshot)?;
    drop(snapshot);

    ensure!(
        !sidecar_path(destination_path, "-wal").exists()
            && !sidecar_path(destination_path, "-shm").exists(),
        "snapshot {} unexpectedly requires SQLite sidecar files",
        destination_path.display()
    );
    set_file_private(destination_path)?;
    sync_file(destination_path)?;
    let file = manifest_file(
        destination_path,
        archive_path,
        source_path,
        role,
        Some(sqlite),
    )?;
    tracing::info!(
        archive_path,
        bytes = file.size_bytes,
        "Verified SQLite backup snapshot"
    );
    Ok(file)
}

fn copy_persistent_directory(
    source: &Path,
    destination: &Path,
    archive_prefix: &str,
    role: &'static str,
    files: &mut Vec<ManifestFile>,
) -> anyhow::Result<()> {
    fs::create_dir(destination)
        .with_context(|| format!("creating backup media directory {}", destination.display()))?;
    set_directory_private(destination)?;
    let mut entries = fs::read_dir(source)
        .with_context(|| format!("reading persistent directory {}", source.display()))?
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let archive_path = format!("{}/{}", archive_prefix, entry.file_name().to_string_lossy());
        let metadata = fs::symlink_metadata(&source_path)
            .with_context(|| format!("inspecting {}", source_path.display()))?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "persistent data {} is a symbolic link; refusing an ambiguous backup",
            source_path.display()
        );
        if metadata.is_dir() {
            copy_persistent_directory(&source_path, &destination_path, &archive_path, role, files)?;
        } else {
            ensure!(
                metadata.is_file(),
                "persistent data {} is not a regular file",
                source_path.display()
            );
            fs::copy(&source_path, &destination_path).with_context(|| {
                format!(
                    "copying persistent data {} to {}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
            set_file_private(&destination_path)?;
            sync_file(&destination_path)?;
            files.push(manifest_file(
                &destination_path,
                &archive_path,
                &source_path,
                role,
                None,
            )?);
        }
    }
    Ok(())
}

fn copy_kweb_root(
    source: &Path,
    destination: &Path,
    archive_prefix: &str,
    include_objects: bool,
    files: &mut Vec<ManifestFile>,
) -> anyhow::Result<()> {
    fs::create_dir(destination)
        .with_context(|| format!("creating Kweb backup directory {}", destination.display()))?;
    set_directory_private(destination)?;
    let mut entries = fs::read_dir(source)
        .with_context(|| format!("reading Kweb root {}", source.display()))?
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        let source_path = entry.path();
        let destination_path = destination.join(&name);
        let archive_path = format!("{archive_prefix}/{}", name.to_string_lossy());
        let metadata = fs::symlink_metadata(&source_path)
            .with_context(|| format!("inspecting {}", source_path.display()))?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "Kweb entry {} is a symbolic link; refusing an ambiguous backup",
            source_path.display()
        );
        if metadata.is_dir() {
            if name == "objects" && !include_objects {
                fs::create_dir(&destination_path).with_context(|| {
                    format!(
                        "creating omitted-object marker directory {}",
                        destination_path.display()
                    )
                })?;
                set_directory_private(&destination_path)?;
            } else {
                copy_persistent_directory(
                    &source_path,
                    &destination_path,
                    &archive_path,
                    if name == "objects" {
                        "immutable Kweb object"
                    } else {
                        "kcode-kweb-db persistent file"
                    },
                    files,
                )?;
            }
        } else {
            ensure!(
                metadata.is_file(),
                "Kweb entry {} is not a regular file",
                source_path.display()
            );
            fs::copy(&source_path, &destination_path).with_context(|| {
                format!(
                    "copying Kweb file {} to {}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
            set_file_private(&destination_path)?;
            sync_file(&destination_path)?;
            files.push(manifest_file(
                &destination_path,
                &archive_path,
                &source_path,
                "kcode-kweb-db persistent file",
                None,
            )?);
        }
    }
    Ok(())
}

fn verify_integrity(connection: &Connection, path: &Path) -> anyhow::Result<()> {
    let mut integrity = connection
        .prepare("PRAGMA integrity_check")
        .context("preparing SQLite integrity check")?;
    let messages = integrity
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        messages.as_slice() == ["ok"],
        "SQLite integrity check failed for {}: {}",
        path.display(),
        messages.join("; ")
    );

    let mut foreign_keys = connection
        .prepare("PRAGMA foreign_key_check")
        .context("preparing SQLite foreign-key check")?;
    let mut rows = foreign_keys.query([])?;
    ensure!(
        rows.next()?.is_none(),
        "SQLite foreign-key check failed for {}",
        path.display()
    );
    Ok(())
}

fn inspect_sqlite(connection: &Connection) -> anyhow::Result<SqliteMetadata> {
    let mut statement = connection.prepare(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'view' THEN 2 ELSE 3 END,name",
    )?;
    let schema = statement
        .query_map([], |row| {
            Ok(SchemaObject {
                object_type: row.get(0)?,
                name: row.get(1)?,
                table_name: row.get(2)?,
                sql: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SqliteMetadata {
        user_version: pragma_i64(connection, "user_version")?,
        application_id: pragma_i64(connection, "application_id")?,
        schema_version: pragma_i64(connection, "schema_version")?,
        page_size: pragma_i64(connection, "page_size")?,
        page_count: pragma_i64(connection, "page_count")?,
        schema,
    })
}

fn pragma_i64(connection: &Connection, name: &str) -> rusqlite::Result<i64> {
    connection.query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
}

fn manifest_file(
    path: &Path,
    archive_path: &str,
    source_path: &Path,
    role: &'static str,
    sqlite: Option<SqliteMetadata>,
) -> anyhow::Result<ManifestFile> {
    Ok(ManifestFile {
        path: archive_path.to_owned(),
        source_path: source_path.display().to_string(),
        role,
        size_bytes: fs::metadata(path)?.len(),
        sha256: sha256(path)?,
        sqlite,
    })
}

fn sha256(path: &Path) -> anyhow::Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("opening {} for checksum", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("reading {} for checksum", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn render_readme(manifest: &Manifest) -> String {
    let mut readme = format!(
        "Kennedy commit: {}\n\n# Kennedy backup\n\nThis is Kennedy backup format {}. It was created at `{}` by `kennedy-server` {} from a build whose source-tree state was `{}`. The SQLite runtime was {}.\n\nThe backup command held `{}` for its entire run and served a maintenance page there. A normal Kennedy server binds that address before opening persistent data. Therefore these files were copied while Kennedy was offline and form one quiescent recovery point.\n\n## Archive inventory\n\n- `README.md` is this human-readable format and recovery specification.\n- `manifest.json` is UTF-8 JSON containing the backup metadata and SHA-256 digest, byte length, original configured path, role, and SQLite metadata for every file under `data/`. It intentionally does not hash itself or this README.\n",
        manifest.kennedy_commit,
        manifest.backup_format_version,
        manifest.created_at,
        manifest.kennedy_version,
        manifest.build_source_dirty,
        manifest.sqlite_version,
        manifest.guard_bind,
    );
    for file in &manifest.files {
        readme.push_str(&format!(
            "- `{}` is the {}; its configured source path was `{}`. It is {} bytes with SHA-256 `{}`.\n",
            file.path, file.role, file.source_path, file.size_bytes, file.sha256
        ));
    }
    readme.push_str("- `data/audio-ingress/` is the complete AudioIngress-owned persistence root: its standalone SQLite state and private content-addressed originals. Upgraded installations may also contain obsolete generated WAV chunks. Empty directories have no checksum entry in the manifest.\n");
    if manifest.kweb_objects_included {
        readme.push_str("- `data/kweb/objects/` contains the immutable Kweb object store. Its files are copied and checksummed like the rest of the Kweb root.\n");
    } else {
        readme.push_str("- The contents of `data/kweb/objects/` were intentionally omitted by lightweight-Kweb mode. Nodes and transactions remain, but omitted object payloads cannot be recovered from this archive alone.\n");
    }
    if !manifest
        .files
        .iter()
        .any(|file| file.path == "data/kennedy-secrets.age")
    {
        readme.push_str("- `data/kennedy-secrets.age` is absent because no credential vault existed at the configured path.\n");
    }

    readme.push_str(
        r#"

All files are stored beneath one top-level archive directory. Runtime `.sqlite3` entries are standalone SQLite database files; no `-wal` or `-shm` sidecar is required. `data/kweb/` is copied as a quiescent binary kcode-kweb-db root. Gzip provides compression only. The SQLite databases, Kweb files, session logs, pending objects, control journals, and audio media contain private plaintext. The optional credential vault remains encrypted, but the archive as a whole is not encrypted.

## Kmap data format

`data/kweb/` is the durable knowledge web owned by `kcode-kweb-db` 1.0. Node and object IDs are canonical eight-character URL-safe unpadded Base64 strings whose type domains cannot collide. Current node records live in the two-level `nodes/` tree; immutable objects live in the similarly sharded `objects/` tree. Per-node history and accepted transaction packages are binary, canonical, checksummed records. `transactions.kwl` is the append-only transaction log, while `state.kws` stores the current generation, heads, log offset, and writer priority list. `wal/` provides atomic recovery for a transaction spanning the log, current node files, histories, objects, transaction index/package, state, and gossip outbox. Fixed and recent connections are complete ordered node-ID arrays with no application-level limits imposed by the library. Root roles remain in `users.sqlite3`, not in the Kweb. A lightweight backup deliberately lacks immutable object payloads and therefore requires those objects from another trusted copy for complete recovery.

## Session History and session-log data format

`data/sessions/` contains in-progress `.session-log` transcripts. Each logical transcript is a three-field header followed by an ordered array of role/text events. Physical frames have a kind byte, payload length, SHA-256 checksum, and payload. Complete appends are synchronized. Recovery truncates an incomplete trailing frame and rejects durable structural or checksum errors. A sealed footer prevents later events.

Each pending object is a separate `<session-id>-<event-position>.pending-object` file containing a fixed header, filename, media type, declared byte length, checksum, and bytes. The object file is durable before its event is appended. `<session-id>.session-control` contains checksummed Session History lifecycle and command records outside the transcript.

`data/session-history.txt` is the local completion index. New entries are JSON-lines receipts containing the Kweb transaction ID when available, canonical archive object ID, pending-node mappings, and pending-object mappings. Older one-object-ID lines remain readable. Details are not duplicated locally. The referenced immutable Kweb object contains the session header and ordered role/text event array and is loaded from `data/kweb/objects/` on demand. An optional `data/legacy-conversations.sqlite3` is an offline pre-overhaul archive only; no runtime code reads it.

## Telegram-relay data format

`data/telegram.sqlite3` stores ordered durable Telegram work in `telegram_events`; numeric-ID-only private pointers in `telegram_private_sessions`; opaque stable group identity, chat-ID history, membership, security decisions, and cursors in `telegram_groups`, `telegram_group_chat_ids`, and `telegram_group_members`; `(opaque group ID, Telegram user)` pointers in `telegram_group_sessions`; archived group messages; and queued 80-message background-ingress batches. The transport database contains no Kmap root IDs. Telegram, chat, update, and message identifiers are SQLite integers; opaque group IDs, Kennedy event IDs, and conversation IDs are text. Event `kind` distinguishes text, voice, document, reset, photo, video, animation, audio, video-note, and sticker work. Retained bounded media bytes use the compatibility-named `voice_bytes` column; `mime_type`, `file_name`, and `duration_seconds` describe essential transport metadata. `status` is the pending/processing/complete queue state. Transcription, model, conversation binding, creation time, completion time, session kind, stable group ID, and identity-only dynamic group context are retained alongside the event.

## User-directory data format

`data/users.sqlite3` is Kennedy's identity authority. `kmap_system_roots` maps the application roles `user` and `kennedy` to their Kmap node IDs outside the Kmap storage library. `whitelist_entries` maps a normalized whitelisted handle to its reserved Kmap root and, after first observation, an immutable numeric Telegram user ID. It also records root-provisioning readiness and the administrator capability used by `/adduser`. `observed_identities` records first/last sightings. `telegram_group_roots` maps each relay-owned opaque group ID to Kennedy's reserved Kmap root and root-readiness state. Telegram chat IDs, membership, cursors, aliases, and security decisions do not live in this database.

## Audio-ingress data format

`data/audio-ingress/` is the standalone AudioIngress persistence root. `state.sqlite3` tracks submitted recordings by SHA-256, making renamed or recopied audio idempotent. `audio_recordings` owns source metadata, provider-model attribution, the latest serialized transcription progress snapshot, the fixed five-attempt retry budget, and the final canonical transcript. `originals/` contains the content-addressed input bytes. Upgraded databases can retain historical `audio_chunks` and `audio_ingress_pieces` tables, but the standalone library no longer reads or writes them.

`data/audio-memory-ingress.sqlite3` is KennedyServer-owned persistence. It stores the transcript pieces, claims, retry scheduling, Kennedy ingress checkpoints, provenance identifiers, optimistic versions, and failure logs used to deliver completed AudioIngress transcripts into Kennedy's memory. This queue and its policies are not part of the AudioIngress library.

An optional `data/legacy-audio-ingress.sqlite3` is the pre-library database retained as migration evidence. It is not live runtime persistence after `data/audio-ingress/state.sqlite3` has been created.

Paths in the AudioIngress database are relative to `data/audio-ingress/` after restoration. In-progress dependency jobs are intentionally not recoverable: after restoration AudioIngress starts a new in-memory attempt from the durable original bytes. Preserve the complete directory as one unit.

## Credential-vault data format

When present, `data/kennedy-secrets.age` is copied byte-for-byte without unlocking it. It is an age passphrase-encrypted file using age's scrypt recipient. After successful decryption with the original vault passphrase, its plaintext is UTF-8 JSON shaped as `{"version":1,"secrets":{"name":"value"}}`; `secrets` is an object of arbitrary validated secret names to string values. Never write or log the decrypted form during recovery.

## Exact SQLite schemas

The following DDL was read from each verified snapshot's `sqlite_schema`. It is authoritative for this backup even if it differs from the prose above or from the source commit's current migrations.
"#,
    );
    for file in manifest.files.iter().filter(|file| file.sqlite.is_some()) {
        let sqlite = file.sqlite.as_ref().expect("filtered SQLite metadata");
        readme.push_str(&format!(
            "\n### `{}`\n\n- `user_version`: {}\n- `application_id`: {}\n- `schema_version`: {}\n- page size/count: {}/{}\n\n```sql\n",
            file.path,
            sqlite.user_version,
            sqlite.application_id,
            sqlite.schema_version,
            sqlite.page_size,
            sqlite.page_count,
        ));
        for object in &sqlite.schema {
            readme.push_str(&format!(
                "-- {} {} (table {})\n{};\n\n",
                object.object_type, object.name, object.table_name, object.sql
            ));
        }
        readme.push_str("```\n");
    }
    readme.push_str(
        r#"

## Recovery procedure

1. Keep this original archive immutable. Extract a working copy into a private directory.
2. Recompute SHA-256 for every `data/` entry and compare it with `manifest.json`.
3. Open each database with SQLite and run `PRAGMA integrity_check` and `PRAGMA foreign_key_check` before attempting a migration.
4. Use the commit at the first line of this README as the primary behavioral reference. If that build was marked dirty or unavailable, use the exact schemas and JSON descriptions above to construct an explicit migration from copies of the files.
5. Stop Kennedy. Place the standalone runtime databases, complete `data/kweb/` root, `data/sessions/`, `data/session-history.txt`, complete `data/audio-ingress/` persistence root, and optional encrypted vault at the paths supplied to the target binary. Restore `data/audio-memory-ingress.sqlite3` to Kennedy's configured audio-memory queue path. Do not restore old SQLite `-wal` or `-shm` files. A lightweight backup is not a complete Kweb recovery source unless its omitted objects are restored independently.
6. Preserve another untouched extracted copy before allowing a newer Kennedy binary to run migrations. Start Kennedy and verify the external user/Kennedy role mappings, several Kmap histories and fixed/recent arrays, active session logs and completed Session History objects, pending session/audio/group ingress, Telegram TOFU bindings/group decisions/events, audio SHA lookups, and vault unlock.

Tracked frontend assets, system prompts, source migrations, and external Codex/ChatGPT state are not duplicated here; the source commit identifies tracked assets, and the latter is not Kennedy-owned runtime persistence.
"#,
    );
    readme
}

fn write_private(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("writing {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing {}", path.display()))?;
    Ok(())
}

fn write_tarball(
    path: &Path,
    staging: &Path,
    archive_root: &str,
    archive_directory: &Path,
) -> anyhow::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .with_context(|| format!("creating temporary archive {}", path.display()))?;
    let gzip = GzBuilder::new()
        .mtime(0)
        .write(file, Compression::default());
    let mut archive = tar::Builder::new(gzip);
    archive.follow_symlinks(false);
    archive
        .append_dir_all(archive_root, archive_directory)
        .with_context(|| format!("packing staged backup from {}", staging.display()))?;
    let gzip = archive.into_inner().context("finishing tar archive")?;
    let file = gzip.finish().context("finishing gzip stream")?;
    file.sync_all()
        .with_context(|| format!("syncing temporary archive {}", path.display()))?;
    Ok(())
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn sync_file(path: &Path) -> anyhow::Result<()> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing {}", path.display()))
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    File::open(path)
        .with_context(|| format!("opening directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing directory {}", path.display()))?;
    Ok(())
}

fn set_file_private(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("setting private permissions on {}", path.display()))?;
    }
    Ok(())
}

fn set_directory_private(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("setting private permissions on {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use flate2::read::GzDecoder;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("kennedy-backup-test-{}", Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn database(path: &Path, value: &str) -> Connection {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE records(value TEXT NOT NULL);",
            )
            .unwrap();
        connection
            .execute("INSERT INTO records(value) VALUES(?1)", [value])
            .unwrap();
        connection
    }

    fn options(directory: &TestDirectory) -> BackupOptions {
        let audio_ingress_directory = directory.path().join("audio-ingress");
        fs::create_dir(&audio_ingress_directory).unwrap();
        let session_directory = directory.path().join("sessions");
        fs::create_dir(&session_directory).unwrap();
        fs::write(
            session_directory.join("in-progress.session-log"),
            b"test session log",
        )
        .unwrap();
        let session_history_file = directory.path().join("session-history.txt");
        fs::write(&session_history_file, b"AAAAAAAB\n").unwrap();
        let kweb_root = directory.path().join("kweb");
        for entry in ["nodes/AA", "objects/gA", "history", "transactions", "wal"] {
            fs::create_dir_all(kweb_root.join(entry)).unwrap();
        }
        fs::write(kweb_root.join("FORMAT"), b"KWDBROOT\0\0\0\0\0\0\0\x03").unwrap();
        fs::write(kweb_root.join("state.kws"), b"test state").unwrap();
        fs::write(kweb_root.join("transactions.kwl"), b"test log").unwrap();
        fs::write(kweb_root.join("nodes/AA/AAAAAA.kwn"), b"test node").unwrap();
        fs::write(kweb_root.join("objects/gA/AAAAAA.kwo"), b"test object").unwrap();
        BackupOptions {
            bind: "127.0.0.1:4321".to_owned(),
            backup_dir: directory.path().join("backups"),
            kweb_root,
            include_kweb_objects: true,
            conversation_database: directory.path().join("conversations.sqlite3"),
            session_directory,
            session_history_file,
            telegram_database: directory.path().join("telegram.sqlite3"),
            user_database: directory.path().join("users.sqlite3"),
            legacy_audio_database: Some(directory.path().join("legacy-audio.sqlite3")),
            audio_memory_ingress_database: directory.path().join("memory-ingress.sqlite3"),
            audio_ingress_directory,
            vault: directory.path().join("secrets.age"),
        }
    }

    #[test]
    fn archive_contains_verified_standalone_snapshots_and_format_documentation() {
        let directory = TestDirectory::new();
        let options = options(&directory);
        let conversations = database(&options.conversation_database, "conversation in wal");
        let telegram = database(&options.telegram_database, "telegram in wal");
        let users = database(&options.user_database, "user directory in wal");
        let audio = database(
            &options.audio_ingress_directory.join("state.sqlite3"),
            "audio state in wal",
        );
        let memory = database(
            &options.audio_memory_ingress_database,
            "audio memory queue in wal",
        );
        let legacy_audio = database(
            options.legacy_audio_database.as_ref().unwrap(),
            "legacy audio state in wal",
        );
        fs::create_dir(options.audio_ingress_directory.join("originals")).unwrap();
        fs::write(
            options.audio_ingress_directory.join("originals/vnote.wav"),
            b"private audio",
        )
        .unwrap();
        fs::write(&options.vault, b"age-encrypted-test-vault").unwrap();
        let created_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 34, 56).unwrap();

        let archive_path = create_archive(&options, created_at).unwrap();
        assert_eq!(
            archive_path.file_name().unwrap(),
            "kennedy-backup-2026-07-16T12-34-56Z.tar.gz"
        );

        let extracted = directory.path().join("extracted");
        fs::create_dir(&extracted).unwrap();
        let decoder = GzDecoder::new(File::open(&archive_path).unwrap());
        tar::Archive::new(decoder).unpack(&extracted).unwrap();
        let root = extracted.join("kennedy-backup-2026-07-16T12-34-56Z");
        let readme = fs::read_to_string(root.join("README.md")).unwrap();
        assert!(readme.starts_with(&format!("Kennedy commit: {}\n", env!("KENNEDY_GIT_COMMIT"))));
        assert!(readme.contains("ordered array of role/text events"));
        assert!(readme.contains("kcode-kweb-db"));
        assert!(readme.contains("CREATE TABLE records"));

        let manifest: Value =
            serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest["backup_format_version"], 13);
        assert_eq!(manifest["snapshot_mode"], "offline-port-guard");
        assert!(manifest["files"].as_array().unwrap().len() >= 10);
        for file in manifest["files"].as_array().unwrap() {
            let path = root.join(file["path"].as_str().unwrap());
            assert_eq!(sha256(&path).unwrap(), file["sha256"]);
            assert_eq!(fs::metadata(path).unwrap().len(), file["size_bytes"]);
        }

        for (name, expected) in [
            ("legacy-conversations.sqlite3", "conversation in wal"),
            ("telegram.sqlite3", "telegram in wal"),
            ("users.sqlite3", "user directory in wal"),
            ("audio-ingress/state.sqlite3", "audio state in wal"),
            ("audio-memory-ingress.sqlite3", "audio memory queue in wal"),
            ("legacy-audio-ingress.sqlite3", "legacy audio state in wal"),
        ] {
            let path = root.join("data").join(name);
            let snapshot = Connection::open(&path).unwrap();
            let value: String = snapshot
                .query_row("SELECT value FROM records", [], |row| row.get(0))
                .unwrap();
            assert_eq!(value, expected);
            assert!(!sidecar_path(&path, "-wal").exists());
            assert!(!sidecar_path(&path, "-shm").exists());
        }
        assert_eq!(
            fs::read(root.join("data/kennedy-secrets.age")).unwrap(),
            b"age-encrypted-test-vault"
        );
        assert_eq!(
            fs::read(root.join("data/audio-ingress/originals/vnote.wav")).unwrap(),
            b"private audio"
        );
        assert_eq!(
            fs::read(root.join("data/kweb/nodes/AA/AAAAAA.kwn")).unwrap(),
            b"test node"
        );

        drop((conversations, telegram, users, audio, memory, legacy_audio));
    }

    #[test]
    fn full_and_lightweight_backups_handle_kweb_objects_explicitly() {
        let directory = TestDirectory::new();
        let mut options = options(&directory);
        for (path, value) in [
            (&options.conversation_database, "conversations"),
            (&options.telegram_database, "telegram"),
            (&options.user_database, "users"),
            (
                &options.audio_ingress_directory.join("state.sqlite3"),
                "audio",
            ),
            (&options.audio_memory_ingress_database, "audio memory"),
        ] {
            drop(database(path, value));
        }
        let full_time = Utc.with_ymd_and_hms(2026, 7, 16, 12, 35, 0).unwrap();
        let full = create_archive(&options, full_time).unwrap();
        let full_extracted = directory.path().join("full-extracted");
        fs::create_dir(&full_extracted).unwrap();
        tar::Archive::new(GzDecoder::new(File::open(full).unwrap()))
            .unpack(&full_extracted)
            .unwrap();
        let full_root = full_extracted.join("kennedy-backup-2026-07-16T12-35-00Z");
        assert_eq!(
            fs::read(full_root.join("data/kweb/objects/gA/AAAAAA.kwo")).unwrap(),
            b"test object"
        );
        let full_manifest: Value =
            serde_json::from_slice(&fs::read(full_root.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(full_manifest["kweb_objects_included"], true);

        options.include_kweb_objects = false;
        let lightweight_time = Utc.with_ymd_and_hms(2026, 7, 16, 12, 35, 1).unwrap();
        let lightweight = create_archive(&options, lightweight_time).unwrap();
        let lightweight_extracted = directory.path().join("lightweight-extracted");
        fs::create_dir(&lightweight_extracted).unwrap();
        tar::Archive::new(GzDecoder::new(File::open(lightweight).unwrap()))
            .unpack(&lightweight_extracted)
            .unwrap();
        let lightweight_root = lightweight_extracted.join("kennedy-backup-2026-07-16T12-35-01Z");
        assert!(
            !lightweight_root
                .join("data/kweb/objects/gA/AAAAAA.kwo")
                .exists()
        );
        let lightweight_manifest: Value =
            serde_json::from_slice(&fs::read(lightweight_root.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(lightweight_manifest["kweb_objects_included"], false);
        assert!(
            fs::read_to_string(lightweight_root.join("README.md"))
                .unwrap()
                .contains("intentionally omitted")
        );
    }

    #[test]
    fn missing_database_never_publishes_an_archive() {
        let directory = TestDirectory::new();
        let options = options(&directory);
        let error = create_archive(&options, Utc::now()).unwrap_err();
        assert!(error.to_string().contains("does not exist"));
        assert!(!options.backup_dir.exists());
    }

    #[test]
    fn existing_timestamped_archive_is_never_overwritten() {
        let directory = TestDirectory::new();
        let options = options(&directory);
        drop(database(&options.conversation_database, "conversations"));
        drop(database(&options.telegram_database, "telegram"));
        drop(database(&options.user_database, "users"));
        drop(database(
            &options.audio_ingress_directory.join("state.sqlite3"),
            "audio",
        ));
        drop(database(
            &options.audio_memory_ingress_database,
            "audio memory",
        ));
        let created_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 34, 56).unwrap();
        fs::create_dir(&options.backup_dir).unwrap();
        let existing = options
            .backup_dir
            .join("kennedy-backup-2026-07-16T12-34-56Z.tar.gz");
        fs::write(&existing, b"keep me").unwrap();

        let error = create_archive(&options, created_at).unwrap_err();
        assert!(error.to_string().contains("already exists"));
        assert_eq!(fs::read(existing).unwrap(), b"keep me");
    }

    #[tokio::test]
    async fn occupied_kweb_port_fails_before_touching_backup_paths() {
        let directory = TestDirectory::new();
        let mut options = options(&directory);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        options.bind = listener.local_addr().unwrap().to_string();

        let error = run(options.clone()).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stop the running Kennedy server")
        );
        assert!(!options.backup_dir.exists());
    }

    #[tokio::test]
    async fn backup_guard_serves_the_maintenance_page() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let page = tokio::spawn(async move { axum::serve(listener, backup_page_router()).await });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /anything HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(
            response
                .to_ascii_lowercase()
                .contains("cache-control: no-store")
        );
        assert!(response.contains("Kennedy backup in progress"));
        page.abort();
    }
}
