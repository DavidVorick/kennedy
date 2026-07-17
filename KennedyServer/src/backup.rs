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

const BACKUP_FORMAT_VERSION: u32 = 3;
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
    pub kmap_database: PathBuf,
    pub conversation_database: PathBuf,
    pub telegram_database: PathBuf,
    pub user_database: PathBuf,
    pub audio_database: PathBuf,
    pub audio_media_directory: PathBuf,
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
    validate_database_source(&options.kmap_database, "Kmap")?;
    validate_database_source(&options.conversation_database, "conversation history")?;
    validate_database_source(&options.telegram_database, "Telegram relay")?;
    validate_database_source(&options.user_database, "user directory")?;
    validate_database_source(&options.audio_database, "audio ingress")?;
    ensure!(
        options.audio_media_directory.is_dir(),
        "audio-ingress media directory {} does not exist or is not a directory; refusing to create an incomplete backup",
        options.audio_media_directory.display()
    );
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

        // These databases form one quiescent recovery point because the Kweb
        // address is held before this worker starts and a normal server binds
        // that address before opening any persistent state.
        let mut files = vec![
            snapshot_database(
                &options.audio_database,
                &data_directory.join("audio-ingress.sqlite3"),
                "data/audio-ingress.sqlite3",
                "audio-ingress database",
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
            snapshot_database(
                &options.conversation_database,
                &data_directory.join("conversations.sqlite3"),
                "data/conversations.sqlite3",
                "conversation history database",
            )?,
            snapshot_database(
                &options.kmap_database,
                &data_directory.join("kmap.sqlite3"),
                "data/kmap.sqlite3",
                "Kmap database",
            )?,
        ];
        copy_persistent_directory(
            &options.audio_media_directory,
            &data_directory.join("audio-ingress-media"),
            "data/audio-ingress-media",
            "durable audio-ingress media",
            &mut files,
        )?;

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
            "audio-ingress media {} is a symbolic link; refusing an ambiguous backup",
            source_path.display()
        );
        if metadata.is_dir() {
            copy_persistent_directory(&source_path, &destination_path, &archive_path, role, files)?;
        } else {
            ensure!(
                metadata.is_file(),
                "audio-ingress media {} is not a regular file",
                source_path.display()
            );
            fs::copy(&source_path, &destination_path).with_context(|| {
                format!(
                    "copying persistent audio media {} to {}",
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
    readme.push_str("- `data/audio-ingress-media/` contains private original vnote audio and any durable in-progress WAV chunks. Empty directories have no checksum entry in the manifest.\n");
    if !manifest
        .files
        .iter()
        .any(|file| file.path == "data/kennedy-secrets.age")
    {
        readme.push_str("- `data/kennedy-secrets.age` is absent because no credential vault existed at the configured path.\n");
    }

    readme.push_str(
        r#"

All files are stored beneath one top-level archive directory. The five `.sqlite3` entries are standalone SQLite database files; no `-wal` or `-shm` sidecar is required. Gzip provides compression only. The SQLite databases and audio media contain private plaintext. The optional credential vault remains encrypted, but the archive as a whole is not encrypted.

## Kmap data format

`data/kmap.sqlite3` is the durable Kennedy memory graph. Twenty-byte binary identifiers are exposed by the API as 40 lowercase hexadecimal characters. `knowledge_nodes` stores current node text, its nullable owner root, and its history head. Null ownership means unowned; valid Kennedy, user, and group roots own themselves. `data_provenance_nodes` stores immutable source material and source timestamps. `data_history_nodes` is an append-only per-node linked history. `knowledge_connections` stores directed fixed, active, and fanout edges; fixed slots 1 through 3 are encoded as reserved negative activation orders. `provenance_idempotency` maps caller keys to immutable provenance. `kmap_roots` maps the `user` and `kennedy` roles to their root nodes. `knowledge_node_model_attribution` records the latest modifying model description. Foreign keys are restrictive and timestamps are textual RFC 3339 values unless the exact schema below says otherwise.

## Conversation-history data format

`data/conversations.sqlite3` stores one row per durable browser or Telegram conversation. `phase` is the recovery state machine, timestamps are RFC 3339 text, `provenance_id` is an optional hexadecimal Kmap provenance identifier, and `version` is an optimistic-concurrency counter rather than a file-format version. `ingress_failures_json` is a JSON array of bounded failure diagnostics.

`state_json` is UTF-8 JSON owned by the frontend. Current top-level state has `stateVersion: 2`, `sessionType`, optional `channel`, direct `rootNodeIds`, optional unloaded `referenceRootNodeIds`, `startedAt`, `transcript`, `media`, `loadedNodeIds`, `pendingTurn`, optional `pendingExternalEventId`, `lastContextWarningBand`, and `archive`. Group channel state may contain its participant/root ledger, recent messages, and a durable background-batch ID. The nested archive has `format: "kennedy-chatend"` and `version: 2`; it preserves session/provider/model metadata, roots and channel context, the system prompt, retained and complete message arrays, reset-history segments, Kmap context snapshot/diagnostics/restoration state, tool counters and logs, usage counters, pending external-event metadata, and serializable media. History-ingress archives use the same format marker/version, `sessionType: "history-ingress"`, and additionally preserve source-session type, provenance ID, completion state, model-round count, referenced roots, and group context. Treat unknown JSON fields as data to preserve, not fields to discard.

## Telegram-relay data format

`data/telegram.sqlite3` stores ordered durable Telegram work in `telegram_events`, persistent `(group root, Telegram user)` pointers in `telegram_group_user_sessions`, archived group messages, and queued 80-message background-ingress batches. `authorized_users` is retained only as private-DM transport state for active conversation pointers. Telegram, chat, update, and message identifiers are SQLite integers; Kennedy event and conversation identifiers are text. Event `kind` distinguishes text, voice, document, and reset work. Original voice/document bytes are stored in `voice_bytes`; `mime_type` and `file_name` describe that media. `status` is the pending/processing/complete queue state. Transcription, model, conversation binding, creation time, completion time, session kind, stable group root, and dynamic group context are retained alongside the event.

## User-directory data format

`data/users.sqlite3` is the Telegram identity authority. `whitelist_entries` maps a normalized whitelisted handle to its reserved Kmap root and, after first observation, an immutable numeric Telegram user ID. It also records root-provisioning readiness and the administrator capability used by `/adduser`. `observed_identities` records first/last sightings. `telegram_groups` assigns each observed group a reserved Kmap root and root-readiness state alongside the complete membership ledger, cursors, and permanent blacklist decision; a blacklisted chat ID is never returned to an allowed state. `telegram_group_aliases` preserves logical group identity across Telegram chat-ID migration.

## Audio-ingress data format

`data/audio-ingress.sqlite3` tracks uploaded recordings by SHA-256, making renamed or recopied audio idempotent. `audio_recordings` owns the recording timestamp, original filename, provider-model attribution, durable processing stage, retry state, and final Sol transcript. `audio_chunks` preserves ordered four-minute-or-shorter WAV windows, their recording-relative offsets, and Gemini's structured transcript JSON. Neighboring chunks overlap by fifteen seconds. `audio_ingress_pieces` stores Sol-selected transcript pieces of no more than an estimated 50,000 tokens and their independent Kennedy ingress checkpoints, provenance identifiers, versions, and failure logs.

Paths in the audio database are relative to `data/audio-ingress-media/` after restoration. `originals/` contains content-addressed uploaded WAV files. `chunks/{recording UUID}/` contains derived WAV windows needed by unfinished transcription jobs. Keep the complete directory during recovery.

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
5. Stop Kennedy. Place the five standalone databases, the complete audio-ingress media directory, and optional encrypted vault at the paths supplied to the target binary. Do not restore old `-wal` or `-shm` files.
6. Preserve another untouched extracted copy before allowing a newer Kennedy binary to run migrations. Start Kennedy and verify several user and Kennedy roots, Kmap histories/connections, active and completed conversations, pending conversation/audio/group ingress, Telegram TOFU bindings/group decisions/events, audio SHA lookups, and vault unlock.

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
        let audio_media_directory = directory.path().join("audio-media");
        fs::create_dir(&audio_media_directory).unwrap();
        BackupOptions {
            bind: "127.0.0.1:4321".to_owned(),
            backup_dir: directory.path().join("backups"),
            kmap_database: directory.path().join("kmap.sqlite3"),
            conversation_database: directory.path().join("conversations.sqlite3"),
            telegram_database: directory.path().join("telegram.sqlite3"),
            user_database: directory.path().join("users.sqlite3"),
            audio_database: directory.path().join("audio.sqlite3"),
            audio_media_directory,
            vault: directory.path().join("secrets.age"),
        }
    }

    #[test]
    fn archive_contains_verified_standalone_snapshots_and_format_documentation() {
        let directory = TestDirectory::new();
        let options = options(&directory);
        let kmap = database(&options.kmap_database, "memory in wal");
        let conversations = database(&options.conversation_database, "conversation in wal");
        let telegram = database(&options.telegram_database, "telegram in wal");
        let users = database(&options.user_database, "user directory in wal");
        let audio = database(&options.audio_database, "audio queue in wal");
        fs::create_dir(options.audio_media_directory.join("originals")).unwrap();
        fs::write(
            options.audio_media_directory.join("originals/vnote.wav"),
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
        assert!(readme.contains("stateVersion: 2"));
        assert!(readme.contains("CREATE TABLE records"));

        let manifest: Value =
            serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest["backup_format_version"], 3);
        assert_eq!(manifest["snapshot_mode"], "offline-port-guard");
        assert_eq!(manifest["files"].as_array().unwrap().len(), 7);
        for file in manifest["files"].as_array().unwrap() {
            let path = root.join(file["path"].as_str().unwrap());
            assert_eq!(sha256(&path).unwrap(), file["sha256"]);
            assert_eq!(fs::metadata(path).unwrap().len(), file["size_bytes"]);
        }

        for (name, expected) in [
            ("kmap.sqlite3", "memory in wal"),
            ("conversations.sqlite3", "conversation in wal"),
            ("telegram.sqlite3", "telegram in wal"),
            ("users.sqlite3", "user directory in wal"),
            ("audio-ingress.sqlite3", "audio queue in wal"),
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
            fs::read(root.join("data/audio-ingress-media/originals/vnote.wav")).unwrap(),
            b"private audio"
        );

        drop((kmap, conversations, telegram, users, audio));
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
        drop(database(&options.kmap_database, "kmap"));
        drop(database(&options.conversation_database, "conversations"));
        drop(database(&options.telegram_database, "telegram"));
        drop(database(&options.user_database, "users"));
        drop(database(&options.audio_database, "audio"));
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
