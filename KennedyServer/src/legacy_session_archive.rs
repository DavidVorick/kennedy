use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use rusqlite::{Connection, OpenFlags, params};

pub(crate) struct ArchiveOptions {
    pub conversation_database: PathBuf,
    pub memory_ingress_database: PathBuf,
    pub archive_directory: PathBuf,
    pub session_directory: PathBuf,
    pub session_history_file: PathBuf,
}

pub(crate) struct ArchiveResult {
    pub unfinished_sessions: usize,
    pub memory_jobs: usize,
    pub archive_directory: PathBuf,
}

pub(crate) fn run(options: &ArchiveOptions) -> anyhow::Result<ArchiveResult> {
    ensure!(
        options.conversation_database.is_file(),
        "legacy conversation database {} does not exist",
        options.conversation_database.display()
    );
    ensure!(
        options.memory_ingress_database.is_file(),
        "memory-ingress database {} does not exist",
        options.memory_ingress_database.display()
    );
    ensure!(
        !options.archive_directory.exists(),
        "legacy session archive {} already exists",
        options.archive_directory.display()
    );

    create_private_directory(&options.archive_directory)?;
    let unfinished_directory = options.archive_directory.join("unfinished-sessions");
    create_private_directory(&unfinished_directory)?;

    let conversation = Connection::open_with_flags(
        &options.conversation_database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| {
        format!(
            "opening legacy conversation database {}",
            options.conversation_database.display()
        )
    })?;
    ensure!(
        conversation.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?
            == "ok",
        "legacy conversation database failed SQLite integrity_check"
    );
    let unfinished_sessions = export_unfinished(&conversation, &unfinished_directory)?;
    drop(conversation);

    let memory_archive = options
        .archive_directory
        .join("legacy-conversation-memory-ingress.sqlite3");
    let memory_jobs = archive_memory_jobs(
        &options.memory_ingress_database,
        &memory_archive,
        &options.archive_directory,
    )?;

    move_sqlite_family(&options.conversation_database, &options.archive_directory)?;
    write_private(
        &options.archive_directory.join("README.txt"),
        format!(
            concat!(
                "Legacy Kennedy sessions archived during the Chatend overhaul.\n\n",
                "Unfinished sessions exported: {unfinished_sessions}\n",
                "Legacy conversation memory-ingress jobs archived and removed from the live mixed queue: {memory_jobs}\n\n",
                "unfinished-sessions/ contains one complete text export per session that was not complete.\n",
                "kennedy-conversations.sqlite3 is the full legacy Session History database, including completed sessions.\n",
                "legacy-conversation-memory-ingress.sqlite3 contains every legacy conversation row removed from the live memory-ingress queue.\n",
                "No runtime code reads this directory. It is an offline provenance archive.\n"
            ),
            unfinished_sessions = unfinished_sessions,
            memory_jobs = memory_jobs,
        )
        .as_bytes(),
    )?;

    create_private_directory(&options.session_directory)?;
    if !options.session_history_file.exists() {
        write_private(&options.session_history_file, b"")?;
    }
    sync_directory(&options.archive_directory)?;
    if let Some(parent) = options.archive_directory.parent() {
        sync_directory(parent)?;
    }

    Ok(ArchiveResult {
        unfinished_sessions,
        memory_jobs,
        archive_directory: options.archive_directory.clone(),
    })
}

fn export_unfinished(database: &Connection, directory: &Path) -> anyhow::Result<usize> {
    let mut statement = database.prepare(
        "SELECT id,phase,started_at,updated_at,state_json,provenance_id,ended_at
         FROM conversations
         WHERE phase <> 'complete'
         ORDER BY started_at,id",
    )?;
    let mut rows = statement.query([])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        ensure!(
            id.chars()
                .all(|value| value.is_ascii_alphanumeric() || value == '-'),
            "legacy session has an unsafe identifier {id:?}"
        );
        let phase: String = row.get(1)?;
        let started_at: String = row.get(2)?;
        let updated_at: String = row.get(3)?;
        let state: String = row.get(4)?;
        let provenance_id: Option<String> = row.get(5)?;
        let ended_at: Option<String> = row.get(6)?;
        let contents = format!(
            concat!(
                "Legacy unfinished Kennedy session\n\n",
                "ID: {id}\n",
                "Phase: {phase}\n",
                "Started: {started_at}\n",
                "Last updated: {updated_at}\n",
                "Ended: {ended_at}\n",
                "Legacy provenance ID: {provenance_id}\n\n",
                "Complete legacy state JSON follows.\n\n",
                "{state}\n"
            ),
            id = id,
            phase = phase,
            started_at = started_at,
            updated_at = updated_at,
            ended_at = ended_at.as_deref().unwrap_or("not ended"),
            provenance_id = provenance_id.as_deref().unwrap_or("none"),
            state = state,
        );
        write_private(&directory.join(format!("{id}.txt")), contents.as_bytes())?;
        count += 1;
    }
    sync_directory(directory)?;
    Ok(count)
}

fn archive_memory_jobs(
    live_path: &Path,
    archive_path: &Path,
    archive_directory: &Path,
) -> anyhow::Result<usize> {
    let archive = Connection::open(archive_path)
        .with_context(|| format!("creating {}", archive_path.display()))?;
    archive.execute(
        "ATTACH DATABASE ?1 AS source",
        params![live_path.to_string_lossy().as_ref()],
    )?;
    archive.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE memory_ingress_jobs AS
         SELECT * FROM source.memory_ingress_jobs WHERE source_kind='conversation';
         CREATE UNIQUE INDEX memory_ingress_jobs_id ON memory_ingress_jobs(id);
         CREATE UNIQUE INDEX memory_ingress_jobs_source
           ON memory_ingress_jobs(source_kind,source_id);
         CREATE TABLE archive_metadata(
           key TEXT PRIMARY KEY NOT NULL,
           value TEXT NOT NULL
         );
         INSERT INTO archive_metadata VALUES
           ('purpose','Legacy conversation memory-ingress rows removed during the Chatend overhaul');
         COMMIT;",
    )?;
    let archived: i64 =
        archive.query_row("SELECT COUNT(*) FROM memory_ingress_jobs", [], |row| {
            row.get(0)
        })?;
    ensure!(
        archive.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))? == "ok",
        "archived legacy memory-ingress database failed SQLite integrity_check"
    );
    archive.execute_batch("DETACH DATABASE source")?;
    drop(archive);
    sync_file(archive_path)?;
    sync_directory(archive_directory)?;

    let mut live = Connection::open(live_path)
        .with_context(|| format!("opening live mixed ingress queue {}", live_path.display()))?;
    let live_count: i64 = live.query_row(
        "SELECT COUNT(*) FROM memory_ingress_jobs WHERE source_kind='conversation'",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        live_count == archived,
        "legacy memory-ingress archive count changed before deletion: archived {archived}, live {live_count}"
    );
    let transaction = live.transaction()?;
    let removed = transaction.execute(
        "DELETE FROM memory_ingress_jobs WHERE source_kind='conversation'",
        [],
    )?;
    ensure!(
        removed as i64 == archived,
        "removed {removed} legacy memory-ingress rows after archiving {archived}"
    );
    transaction.commit()?;
    live.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(live);
    sync_file(live_path)?;
    usize::try_from(archived).context("legacy memory-ingress row count exceeds usize")
}

fn move_sqlite_family(source: &Path, destination_directory: &Path) -> anyhow::Result<()> {
    for candidate in [
        source.to_path_buf(),
        PathBuf::from(format!("{}-wal", source.display())),
        PathBuf::from(format!("{}-shm", source.display())),
    ] {
        if !candidate.exists() {
            continue;
        }
        let destination = destination_directory.join(
            candidate
                .file_name()
                .context("legacy SQLite path has no filename")?,
        );
        fs::rename(&candidate, &destination).with_context(|| {
            format!(
                "moving legacy SQLite file {} to {}",
                candidate.display(),
                destination.display()
            )
        })?;
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        ensure!(path.is_dir(), "{} is not a directory", path.display());
        return Ok(());
    }
    let mut options = fs::DirBuilder::new();
    options.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        options.mode(0o700);
    }
    options
        .create(path)
        .with_context(|| format!("creating {}", path.display()))
}

fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing {}", path.display()))
}

fn sync_file(path: &Path) -> anyhow::Result<()> {
    File::open(path)
        .with_context(|| format!("opening {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing {}", path.display()))
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
    use uuid::Uuid;

    #[test]
    fn cutover_exports_unfinished_moves_legacy_and_preserves_only_audio_live() {
        let root = std::env::temp_dir().join(format!("kennedy-legacy-cutover-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let conversations = root.join("kennedy-conversations.sqlite3");
        let database = Connection::open(&conversations).unwrap();
        database
            .execute_batch(
                "CREATE TABLE conversations(
                   id TEXT PRIMARY KEY,
                   phase TEXT NOT NULL,
                   started_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL,
                   state_json TEXT NOT NULL,
                   provenance_id TEXT,
                   ended_at TEXT
                 );",
            )
            .unwrap();
        database
            .execute(
                "INSERT INTO conversations VALUES
                 ('active-id','active','start','update','{\"transcript\":[\"kept\"]}',NULL,NULL)",
                [],
            )
            .unwrap();
        database
            .execute(
                "INSERT INTO conversations VALUES
                 ('complete-id','complete','start','update','{}',NULL,'end')",
                [],
            )
            .unwrap();
        drop(database);

        let memory = root.join("memory.sqlite3");
        let database = Connection::open(&memory).unwrap();
        database
            .execute_batch(
                "CREATE TABLE memory_ingress_jobs(
                   id TEXT PRIMARY KEY,
                   source_kind TEXT NOT NULL,
                   source_id TEXT NOT NULL
                 );
                 INSERT INTO memory_ingress_jobs VALUES
                   ('conversation-job','conversation','active-id'),
                   ('audio-job','audio','piece-id');",
            )
            .unwrap();
        drop(database);

        let archive = root.join("archive");
        let sessions = root.join("sessions/in-progress");
        let history = root.join("session-history.txt");
        let result = run(&ArchiveOptions {
            conversation_database: conversations.clone(),
            memory_ingress_database: memory.clone(),
            archive_directory: archive.clone(),
            session_directory: sessions.clone(),
            session_history_file: history.clone(),
        })
        .unwrap();
        assert_eq!(result.unfinished_sessions, 1);
        assert_eq!(result.memory_jobs, 1);
        assert!(!conversations.exists());
        assert!(archive.join("kennedy-conversations.sqlite3").is_file());
        assert!(
            fs::read_to_string(archive.join("unfinished-sessions/active-id.txt"))
                .unwrap()
                .contains("\"kept\"")
        );
        let database = Connection::open(&memory).unwrap();
        let conversation_rows: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM memory_ingress_jobs WHERE source_kind='conversation'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let audio_rows: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM memory_ingress_jobs WHERE source_kind='audio'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(conversation_rows, 0);
        assert_eq!(audio_rows, 1);
        assert!(sessions.is_dir());
        assert_eq!(fs::read_to_string(history).unwrap(), "");
        fs::remove_dir_all(root).unwrap();
    }
}
