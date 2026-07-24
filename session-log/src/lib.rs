//! Durable, append-ordered session history.
//!
//! A session log stores one three-field header followed by ordered `{role,
//! text}` events. Event positions are their stable identities and are not
//! serialized. Pending objects are written to one self-contained file each
//! before their corresponding events are appended.

use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

use anyhow::{Context as _, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const FORMAT_VERSION: &str = "0.2.1";

const SESSION_MAGIC: &[u8] = b"KSESSIONLOG\n";
const OBJECT_MAGIC: &[u8] = b"KSPENDING01\n";
const FRAME_HEADER_BYTES: u64 = 1 + 8 + 32;
const HEADER_FRAME: u8 = 1;
const EVENT_FRAME: u8 = 2;
const SEALED_FRAME: u8 = 3;
const OBJECT_FIXED_HEADER_BYTES: usize = 2 + 4 + 4 + 8 + 32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionHeader {
    pub format_version: String,
    pub session_id: String,
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    SystemMessage,
    SystemError,
    UserMessage,
    KennedyMessage,
    KennedyToolCall,
    ToolResult,
    ToolError,
    Object,
    PendingObject,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionEvent {
    pub role: Role,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionLog {
    pub header: SessionHeader,
    pub events: Vec<SessionEvent>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventPosition(pub u64);

impl EventPosition {
    pub fn index(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for EventPosition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingObject {
    pub event_position: EventPosition,
    pub text: String,
    pub file_name: String,
    pub media_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedSession {
    log: SessionLog,
    directory: PathBuf,
}

impl SealedSession {
    pub fn list(&self) -> &SessionLog {
        &self.log
    }

    pub fn pending_objects(&self) -> anyhow::Result<Vec<PendingObject>> {
        self.log
            .events
            .iter()
            .enumerate()
            .filter(|(_, event)| event.role == Role::PendingObject)
            .map(|(position, event)| {
                read_pending_object_file(
                    &self.directory,
                    &self.log.header.session_id,
                    EventPosition(position as u64),
                    event.text.clone(),
                )
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct SessionStore {
    directory: PathBuf,
}

impl SessionStore {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn create_session(
        &self,
        session_id: impl Into<String>,
        created_at: impl Into<String>,
    ) -> anyhow::Result<Session> {
        let session_id = session_id.into();
        let created_at = created_at.into();
        validate_session_id(&session_id)?;
        ensure!(
            !created_at.trim().is_empty(),
            "session creation time cannot be empty"
        );
        std::fs::create_dir_all(&self.directory)
            .with_context(|| format!("creating {}", self.directory.display()))?;
        let path = session_path(&self.directory, &session_id);
        let append_lock = session_lock(&path);
        let _guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session-log append lock is poisoned"))?;
        let header = SessionHeader {
            format_version: FORMAT_VERSION.into(),
            session_id,
            created_at,
        };
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("creating {}", path.display()))?;
        file.write_all(SESSION_MAGIC)?;
        append_frame(&mut file, HEADER_FRAME, &serde_json::to_vec(&header)?)?;
        file.sync_all()?;
        sync_directory(&self.directory)?;
        drop(_guard);
        Ok(Session {
            directory: self.directory.clone(),
            path,
            log: SessionLog {
                header,
                events: Vec::new(),
            },
            sealed: false,
            append_lock,
        })
    }

    pub fn open_session(&self, session_id: &str) -> anyhow::Result<Session> {
        validate_session_id(session_id)?;
        let path = session_path(&self.directory, session_id);
        let append_lock = session_lock(&path);
        let _guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session-log append lock is poisoned"))?;
        let loaded = load_session_file(&path, true)?;
        ensure!(
            loaded.log.header.session_id == session_id,
            "session filename and header identity differ"
        );
        cleanup_orphan_objects(&self.directory, &loaded.log)?;
        verify_referenced_objects(&self.directory, &loaded.log)?;
        drop(_guard);
        Ok(Session {
            directory: self.directory.clone(),
            path,
            log: loaded.log,
            sealed: loaded.sealed,
            append_lock,
        })
    }

    pub fn session_ids(&self) -> anyhow::Result<Vec<String>> {
        if !self.directory.exists() {
            return Ok(Vec::new());
        }
        let mut ids = std::fs::read_dir(&self.directory)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_suffix(".session-log"))
                    .map(str::to_owned)
            })
            .filter(|id| validate_session_id(id).is_ok())
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }
}

pub struct Session {
    directory: PathBuf,
    path: PathBuf,
    log: SessionLog,
    sealed: bool,
    append_lock: Arc<Mutex<()>>,
}

impl Session {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn list(&self) -> SessionLog {
        self.log.clone()
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn add_event(
        &mut self,
        role: Role,
        text: impl Into<String>,
    ) -> anyhow::Result<EventPosition> {
        ensure!(
            role != Role::PendingObject,
            "pending objects must be added through add_pending_object"
        );
        let event = SessionEvent {
            role,
            text: text.into(),
        };
        let append_lock = self.append_lock.clone();
        let _guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session-log append lock is poisoned"))?;
        self.refresh_locked()?;
        ensure!(!self.sealed, "session is sealed");
        let position = EventPosition(self.log.events.len() as u64);
        append_event_file(&self.path, &event)?;
        self.log.events.push(event);
        Ok(position)
    }

    pub fn add_pending_object(
        &mut self,
        text: impl Into<String>,
        file_name: impl Into<String>,
        media_type: impl Into<String>,
        bytes: &[u8],
    ) -> anyhow::Result<EventPosition> {
        let text = text.into();
        let file_name = file_name.into();
        let media_type = media_type.into();
        ensure!(
            !file_name.trim().is_empty(),
            "object filename cannot be empty"
        );
        ensure!(
            !media_type.trim().is_empty(),
            "object media type cannot be empty"
        );
        let append_lock = self.append_lock.clone();
        let _guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session-log append lock is poisoned"))?;
        self.refresh_locked()?;
        ensure!(!self.sealed, "session is sealed");
        let position = EventPosition(self.log.events.len() as u64);
        write_pending_object_file(
            &self.directory,
            &self.log.header.session_id,
            position,
            &file_name,
            &media_type,
            bytes,
        )?;
        let event = SessionEvent {
            role: Role::PendingObject,
            text,
        };
        append_event_file(&self.path, &event)?;
        self.log.events.push(event);
        Ok(position)
    }

    pub fn read_pending_object(&self, position: EventPosition) -> anyhow::Result<PendingObject> {
        let index = usize::try_from(position.0).context("event position does not fit memory")?;
        let event = self
            .log
            .events
            .get(index)
            .with_context(|| format!("event {position} does not exist"))?;
        ensure!(
            event.role == Role::PendingObject,
            "event {position} is not a pending object"
        );
        read_pending_object_file(
            &self.directory,
            &self.log.header.session_id,
            position,
            event.text.clone(),
        )
    }

    pub fn seal(&mut self) -> anyhow::Result<SealedSession> {
        let append_lock = self.append_lock.clone();
        let _guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session-log append lock is poisoned"))?;
        self.refresh_locked()?;
        if !self.sealed {
            verify_referenced_objects(&self.directory, &self.log)?;
            let mut file = OpenOptions::new().append(true).open(&self.path)?;
            append_frame(&mut file, SEALED_FRAME, &[])?;
            file.sync_all()?;
            sync_directory(&self.directory)?;
            self.sealed = true;
        }
        Ok(SealedSession {
            log: self.log.clone(),
            directory: self.directory.clone(),
        })
    }

    pub fn delete_committed(self) -> anyhow::Result<()> {
        self.delete_files()
    }

    pub fn delete_abandoned(self) -> anyhow::Result<()> {
        self.delete_files()
    }

    fn refresh_locked(&mut self) -> anyhow::Result<()> {
        let loaded = load_session_file(&self.path, true)?;
        ensure!(
            loaded.log.header.session_id == self.log.header.session_id,
            "session identity changed on disk"
        );
        self.log = loaded.log;
        self.sealed = loaded.sealed;
        Ok(())
    }

    fn delete_files(self) -> anyhow::Result<()> {
        let append_lock = self.append_lock.clone();
        let _guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session-log append lock is poisoned"))?;
        let session_id = self.log.header.session_id;
        if self.path.exists() {
            std::fs::remove_file(&self.path)
                .with_context(|| format!("removing {}", self.path.display()))?;
        }
        if self.directory.exists() {
            for entry in std::fs::read_dir(&self.directory)? {
                let entry = entry?;
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                if parse_object_filename(&session_id, &name).is_some() {
                    std::fs::remove_file(entry.path())
                        .with_context(|| format!("removing {}", entry.path().display()))?;
                }
            }
        }
        sync_directory(&self.directory)?;
        Ok(())
    }
}

struct LoadedSession {
    log: SessionLog,
    sealed: bool,
}

fn validate_session_id(session_id: &str) -> anyhow::Result<()> {
    ensure!(!session_id.is_empty(), "session ID cannot be empty");
    ensure!(session_id.len() <= 255, "session ID exceeds 255 characters");
    ensure!(
        session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "session ID contains characters that are unsafe in filenames"
    );
    Ok(())
}

fn session_path(directory: &Path, session_id: &str) -> PathBuf {
    directory.join(format!("{session_id}.session-log"))
}

fn pending_object_path(directory: &Path, session_id: &str, position: EventPosition) -> PathBuf {
    directory.join(format!("{session_id}-{}.pending-object", position.0))
}

fn pending_object_temp_path(
    directory: &Path,
    session_id: &str,
    position: EventPosition,
) -> PathBuf {
    directory.join(format!("{session_id}-{}.pending-object.tmp", position.0))
}

fn parse_object_filename(session_id: &str, name: &str) -> Option<(EventPosition, bool)> {
    let tail = name.strip_prefix(&format!("{session_id}-"))?;
    let (number, temporary) = if let Some(number) = tail.strip_suffix(".pending-object.tmp") {
        (number, true)
    } else {
        (tail.strip_suffix(".pending-object")?, false)
    };
    if number.is_empty() || number.starts_with('0') && number != "0" {
        return None;
    }
    let position = number.parse::<u64>().ok()?;
    (position.to_string() == number).then_some((EventPosition(position), temporary))
}

fn append_event_file(path: &Path, event: &SessionEvent) -> anyhow::Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    append_frame(&mut file, EVENT_FRAME, &serde_json::to_vec(event)?)?;
    file.sync_all()?;
    Ok(())
}

fn load_session_file(path: &Path, repair_tail: bool) -> anyhow::Result<LoadedSession> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(repair_tail)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut magic = vec![0_u8; SESSION_MAGIC.len()];
    file.read_exact(&mut magic)?;
    ensure!(
        magic == SESSION_MAGIC,
        "{} is not a session log",
        path.display()
    );
    let file_len = file.metadata()?.len();
    let mut cursor = SESSION_MAGIC.len() as u64;
    let mut header = None;
    let mut events = Vec::new();
    let mut sealed = false;
    while cursor < file_len {
        let remaining = file_len - cursor;
        if remaining < FRAME_HEADER_BYTES {
            if repair_tail {
                file.set_len(cursor)?;
                file.sync_all()?;
                break;
            }
            bail!("session log has an incomplete trailing frame header");
        }
        file.seek(SeekFrom::Start(cursor))?;
        let mut frame_header = [0_u8; FRAME_HEADER_BYTES as usize];
        file.read_exact(&mut frame_header)?;
        let kind = frame_header[0];
        let payload_len = u64::from_le_bytes(frame_header[1..9].try_into().unwrap());
        let frame_end = cursor
            .checked_add(FRAME_HEADER_BYTES)
            .and_then(|value| value.checked_add(payload_len))
            .context("session-log frame length overflow")?;
        if frame_end > file_len {
            if repair_tail {
                file.set_len(cursor)?;
                file.sync_all()?;
                break;
            }
            bail!("session log has an incomplete trailing frame");
        }
        let payload_len_usize =
            usize::try_from(payload_len).context("session-log frame does not fit memory")?;
        let mut payload = vec![0_u8; payload_len_usize];
        file.read_exact(&mut payload)?;
        if Sha256::digest(&payload).as_slice() != &frame_header[9..] {
            bail!("session log has a checksum-invalid complete frame");
        }
        match kind {
            HEADER_FRAME => {
                ensure!(
                    cursor == SESSION_MAGIC.len() as u64,
                    "duplicate session header"
                );
                let value: SessionHeader = serde_json::from_slice(&payload)?;
                ensure!(
                    value.format_version == FORMAT_VERSION,
                    "unsupported session-log format {}",
                    value.format_version
                );
                validate_session_id(&value.session_id)?;
                ensure!(
                    !value.created_at.trim().is_empty(),
                    "session creation time cannot be empty"
                );
                header = Some(value);
            }
            EVENT_FRAME => {
                ensure!(header.is_some(), "session event precedes header");
                ensure!(!sealed, "session event follows sealed footer");
                events.push(serde_json::from_slice(&payload)?);
            }
            SEALED_FRAME => {
                ensure!(header.is_some(), "sealed footer precedes header");
                ensure!(payload.is_empty(), "sealed footer payload must be empty");
                ensure!(!sealed, "duplicate sealed footer");
                sealed = true;
            }
            other => bail!("unknown complete session-log frame kind {other}"),
        }
        cursor = frame_end;
    }
    let header = header.context("session log has no header")?;
    Ok(LoadedSession {
        log: SessionLog { header, events },
        sealed,
    })
}

fn append_frame(file: &mut File, kind: u8, payload: &[u8]) -> anyhow::Result<()> {
    file.write_all(&[kind])?;
    file.write_all(&(payload.len() as u64).to_le_bytes())?;
    file.write_all(&Sha256::digest(payload))?;
    file.write_all(payload)?;
    Ok(())
}

fn write_pending_object_file(
    directory: &Path,
    session_id: &str,
    position: EventPosition,
    file_name: &str,
    media_type: &str,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let version = FORMAT_VERSION.as_bytes();
    let version_len = u16::try_from(version.len()).context("format version is too long")?;
    let file_name_len = u32::try_from(file_name.len()).context("object filename exceeds 4 GiB")?;
    let media_type_len =
        u32::try_from(media_type.len()).context("object media type exceeds 4 GiB")?;
    let object_len = u64::try_from(bytes.len()).context("object exceeds addressable size")?;
    let final_path = pending_object_path(directory, session_id, position);
    let temp_path = pending_object_temp_path(directory, session_id, position);
    ensure!(
        !final_path.exists(),
        "pending object file {} already exists",
        final_path.display()
    );
    if temp_path.exists() {
        std::fs::remove_file(&temp_path)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)?;
    file.write_all(OBJECT_MAGIC)?;
    file.write_all(&version_len.to_le_bytes())?;
    file.write_all(&file_name_len.to_le_bytes())?;
    file.write_all(&media_type_len.to_le_bytes())?;
    file.write_all(&object_len.to_le_bytes())?;
    file.write_all(&Sha256::digest(bytes))?;
    file.write_all(version)?;
    file.write_all(file_name.as_bytes())?;
    file.write_all(media_type.as_bytes())?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temp_path, &final_path)?;
    sync_directory(directory)?;
    Ok(())
}

fn read_pending_object_file(
    directory: &Path,
    session_id: &str,
    position: EventPosition,
    text: String,
) -> anyhow::Result<PendingObject> {
    let path = pending_object_path(directory, session_id, position);
    let mut file =
        File::open(&path).with_context(|| format!("opening pending object {}", path.display()))?;
    let mut magic = vec![0_u8; OBJECT_MAGIC.len()];
    file.read_exact(&mut magic)?;
    ensure!(
        magic == OBJECT_MAGIC,
        "{} is not a pending-object file",
        path.display()
    );
    let mut fixed = [0_u8; OBJECT_FIXED_HEADER_BYTES];
    file.read_exact(&mut fixed)?;
    let version_len = u16::from_le_bytes(fixed[0..2].try_into().unwrap()) as usize;
    let file_name_len = u32::from_le_bytes(fixed[2..6].try_into().unwrap()) as usize;
    let media_type_len = u32::from_le_bytes(fixed[6..10].try_into().unwrap()) as usize;
    let object_len = u64::from_le_bytes(fixed[10..18].try_into().unwrap());
    let checksum = &fixed[18..50];
    let variable_len = version_len
        .checked_add(file_name_len)
        .and_then(|value| value.checked_add(media_type_len))
        .context("pending-object header length overflow")?;
    let expected_file_len = u64::try_from(OBJECT_MAGIC.len() + OBJECT_FIXED_HEADER_BYTES)
        .context("pending-object fixed header does not fit u64")?
        .checked_add(
            u64::try_from(variable_len)
                .context("pending-object variable header does not fit u64")?,
        )
        .and_then(|value| value.checked_add(object_len))
        .context("pending-object declared length overflow")?;
    ensure!(
        file.metadata()?.len() == expected_file_len,
        "pending-object declared length differs from file length"
    );
    let mut variable = vec![0_u8; variable_len];
    file.read_exact(&mut variable)?;
    let version = std::str::from_utf8(&variable[..version_len])?;
    ensure!(
        version == FORMAT_VERSION,
        "unsupported pending-object format {version}"
    );
    let file_name_end = version_len + file_name_len;
    let file_name = std::str::from_utf8(&variable[version_len..file_name_end])?.to_owned();
    let media_type =
        std::str::from_utf8(&variable[file_name_end..file_name_end + media_type_len])?.to_owned();
    ensure!(!file_name.trim().is_empty(), "object filename is empty");
    ensure!(!media_type.trim().is_empty(), "object media type is empty");
    let object_len_usize =
        usize::try_from(object_len).context("pending object does not fit memory")?;
    let mut bytes = vec![0_u8; object_len_usize];
    file.read_exact(&mut bytes)?;
    ensure!(
        file.stream_position()? == file.metadata()?.len(),
        "pending-object file has trailing bytes"
    );
    ensure!(
        Sha256::digest(&bytes).as_slice() == checksum,
        "pending-object checksum mismatch"
    );
    Ok(PendingObject {
        event_position: position,
        text,
        file_name,
        media_type,
        bytes,
    })
}

fn referenced_pending_positions(log: &SessionLog) -> HashSet<EventPosition> {
    log.events
        .iter()
        .enumerate()
        .filter_map(|(position, event)| {
            (event.role == Role::PendingObject).then_some(EventPosition(position as u64))
        })
        .collect()
}

fn verify_referenced_objects(directory: &Path, log: &SessionLog) -> anyhow::Result<()> {
    for position in referenced_pending_positions(log) {
        let event = &log.events[position.0 as usize];
        read_pending_object_file(
            directory,
            &log.header.session_id,
            position,
            event.text.clone(),
        )?;
    }
    Ok(())
}

fn cleanup_orphan_objects(directory: &Path, log: &SessionLog) -> anyhow::Result<()> {
    let referenced = referenced_pending_positions(log);
    if !directory.exists() {
        return Ok(());
    }
    let mut removed = false;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some((position, temporary)) = parse_object_filename(&log.header.session_id, &name)
        else {
            continue;
        };
        if temporary || !referenced.contains(&position) {
            std::fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        sync_directory(directory)?;
    }
    Ok(())
}

fn session_lock(path: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().expect("session-log lock registry is poisoned");
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

fn sync_directory(directory: &Path) -> anyhow::Result<()> {
    File::open(directory)
        .with_context(|| format!("opening directory {}", directory.display()))?
        .sync_all()
        .with_context(|| format!("synchronizing directory {}", directory.display()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn directory(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "session-log-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn events_are_an_ordered_role_and_text_array_without_serialized_ids() {
        let directory = directory("ordered");
        let store = SessionStore::new(&directory);
        let mut session = store
            .create_session("session-1", "2026-07-24T00:00:00Z")
            .unwrap();
        assert_eq!(
            session.add_event(Role::SystemMessage, "system").unwrap(),
            EventPosition(0)
        );
        assert_eq!(
            session.add_event(Role::UserMessage, "hello").unwrap(),
            EventPosition(1)
        );
        drop(session);

        let reopened = store.open_session("session-1").unwrap();
        assert_eq!(
            reopened.list(),
            SessionLog {
                header: SessionHeader {
                    format_version: "0.2.1".into(),
                    session_id: "session-1".into(),
                    created_at: "2026-07-24T00:00:00Z".into(),
                },
                events: vec![
                    SessionEvent {
                        role: Role::SystemMessage,
                        text: "system".into(),
                    },
                    SessionEvent {
                        role: Role::UserMessage,
                        text: "hello".into(),
                    },
                ],
            }
        );
        let serialized = serde_json::to_string(&reopened.list().events).unwrap();
        assert!(!serialized.contains("\"id\""));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pending_object_is_durable_before_its_event_and_uses_event_position() {
        let directory = directory("object");
        let store = SessionStore::new(&directory);
        let mut session = store
            .create_session("object-session", "2026-07-24T00:00:00Z")
            .unwrap();
        session.add_event(Role::UserMessage, "upload").unwrap();
        let position = session
            .add_pending_object("notes.txt", "notes.txt", "text/plain", b"durable bytes")
            .unwrap();
        assert_eq!(position, EventPosition(1));
        assert!(directory.join("object-session-1.pending-object").exists());
        let object = session.read_pending_object(position).unwrap();
        assert_eq!(object.file_name, "notes.txt");
        assert_eq!(object.media_type, "text/plain");
        assert_eq!(object.bytes, b"durable bytes");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn open_removes_unreferenced_final_and_temporary_objects() {
        let directory = directory("orphans");
        let store = SessionStore::new(&directory);
        let session = store
            .create_session("orphan-session", "2026-07-24T00:00:00Z")
            .unwrap();
        drop(session);
        std::fs::write(directory.join("orphan-session-0.pending-object"), b"orphan").unwrap();
        std::fs::write(
            directory.join("orphan-session-1.pending-object.tmp"),
            b"temporary",
        )
        .unwrap();
        store.open_session("orphan-session").unwrap();
        assert!(!directory.join("orphan-session-0.pending-object").exists());
        assert!(
            !directory
                .join("orphan-session-1.pending-object.tmp")
                .exists()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn incomplete_event_tail_is_discarded() {
        let directory = directory("tail");
        let store = SessionStore::new(&directory);
        let mut session = store
            .create_session("tail-session", "2026-07-24T00:00:00Z")
            .unwrap();
        session.add_event(Role::UserMessage, "complete").unwrap();
        let path = session.path().to_path_buf();
        drop(session);
        let valid_len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[EVENT_FRAME, 20, 0, 0])
            .unwrap();
        let reopened = store.open_session("tail-session").unwrap();
        assert_eq!(reopened.list().events.len(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn checksum_invalid_complete_frame_is_corruption_not_a_recoverable_tail() {
        let directory = directory("checksum");
        let store = SessionStore::new(&directory);
        let mut session = store
            .create_session("checksum-session", "2026-07-24T00:00:00Z")
            .unwrap();
        session.add_event(Role::UserMessage, "complete").unwrap();
        let path = session.path().to_path_buf();
        drop(session);
        let mut bytes = std::fs::read(&path).unwrap();
        let payload_byte = SESSION_MAGIC.len() + FRAME_HEADER_BYTES as usize;
        bytes[payload_byte] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(store.open_session("checksum-session").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn seal_is_durable_idempotent_and_rejects_later_events() {
        let directory = directory("seal");
        let store = SessionStore::new(&directory);
        let mut session = store
            .create_session("sealed-session", "2026-07-24T00:00:00Z")
            .unwrap();
        session.add_event(Role::UserMessage, "hello").unwrap();
        session.seal().unwrap();
        session.seal().unwrap();
        assert!(session.add_event(Role::KennedyMessage, "too late").is_err());
        drop(session);
        assert!(store.open_session("sealed-session").unwrap().is_sealed());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn deletion_matches_only_exact_session_object_names() {
        let directory = directory("delete");
        let store = SessionStore::new(&directory);
        let session = store.create_session("abc", "2026-07-24T00:00:00Z").unwrap();
        std::fs::write(directory.join("abc-other-0.pending-object"), b"keep").unwrap();
        std::fs::write(directory.join("abc-not-a-number.pending-object"), b"keep").unwrap();
        session.delete_abandoned().unwrap();
        assert!(directory.join("abc-other-0.pending-object").exists());
        assert!(directory.join("abc-not-a-number.pending-object").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
