//! Session lifecycle and local Session History.
//!
//! In-progress transcript entries live in `kcode-session-log`. Session
//! lifecycle and command records live in a separate Session History control
//! journal.
//! Successfully committed sessions leave only one local line containing their
//! immutable Kweb object ID; their details are loaded from Kweb on demand.

pub mod chatend;
pub use chatend::Session;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path as FilePath, PathBuf},
    sync::{Arc, Mutex, Weak},
};

use anyhow::{Context as _, ensure};
use chrono::{DateTime, Duration, Utc};
use kcode_session_log::{EventPosition, Role, Session as DurableSession, SessionLog, SessionStore};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const LIFECYCLE_SIDEBAND: &str = "session_lifecycle";
const COMMAND_SIDEBAND: &str = "session_command";
const CONTROL_EXTENSION: &str = "session-control";
const INGRESS_FAILURE_LIMIT: i64 = 5;
const INGRESS_RETRY_DELAY_SECONDS: i64 = 15;
const RETAINED_INGRESS_FAILURES: usize = 5;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ControlRecord {
    kind: String,
    recorded_at: String,
    value: Value,
}

struct ControlJournal {
    path: PathBuf,
    records: Vec<ControlRecord>,
}

impl ControlJournal {
    fn create(path: PathBuf) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        file.sync_all()?;
        sync_directory(path.parent().unwrap_or_else(|| FilePath::new(".")))?;
        Ok(Self {
            path,
            records: Vec::new(),
        })
    }

    fn open(path: PathBuf) -> anyhow::Result<Self> {
        if !path.exists() {
            return Self::create(path);
        }
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut records = Vec::new();
        let mut cursor = 0_usize;
        while cursor < bytes.len() {
            let Some(relative_end) = bytes[cursor..].iter().position(|byte| *byte == b'\n') else {
                file.set_len(cursor as u64)?;
                file.sync_all()?;
                break;
            };
            let end = cursor + relative_end;
            let line = &bytes[cursor..end];
            let separator = line
                .iter()
                .position(|byte| *byte == b' ')
                .context("session-control record has no checksum separator")?;
            let expected = std::str::from_utf8(&line[..separator])?;
            let payload = &line[separator + 1..];
            ensure!(
                hex_sha256(payload) == expected,
                "session-control record checksum mismatch"
            );
            records.push(serde_json::from_slice(payload)?);
            cursor = end + 1;
        }
        Ok(Self { path, records })
    }

    fn records(&self) -> &[ControlRecord] {
        &self.records
    }

    fn append(
        &mut self,
        kind: impl Into<String>,
        recorded_at: impl Into<String>,
        value: Value,
    ) -> anyhow::Result<()> {
        let record = ControlRecord {
            kind: kind.into(),
            recorded_at: recorded_at.into(),
            value,
        };
        let payload = serde_json::to_vec(&record)?;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        writeln!(
            file,
            "{} {}",
            hex_sha256(&payload),
            String::from_utf8(payload)?
        )?;
        file.sync_all()?;
        self.records.push(record);
        Ok(())
    }
}

struct SessionJournal {
    log: DurableSession,
    control: ControlJournal,
}

impl SessionJournal {
    fn create(directory: &FilePath, id: &str, created_at: &str) -> anyhow::Result<Self> {
        let log = SessionStore::new(directory).create_session(id, created_at)?;
        let control = ControlJournal::create(control_path(directory, id))?;
        Ok(Self { log, control })
    }

    fn open(path: impl AsRef<FilePath>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        ensure!(
            path.extension().and_then(|value| value.to_str()) == Some("session-log"),
            "{} is not a session-log path",
            path.display()
        );
        let directory = path.parent().unwrap_or_else(|| FilePath::new("."));
        let id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .context("session-log filename is not valid UTF-8")?;
        Ok(Self {
            log: SessionStore::new(directory).open_session(id)?,
            control: ControlJournal::open(control_path(directory, id))?,
        })
    }

    fn list(&self) -> SessionLog {
        self.log.list()
    }

    fn records(&self) -> &[ControlRecord] {
        self.control.records()
    }

    fn append_control(
        &mut self,
        kind: impl Into<String>,
        recorded_at: impl Into<String>,
        value: Value,
    ) -> anyhow::Result<()> {
        self.control.append(kind, recorded_at, value)
    }

    fn stage_object(
        &mut self,
        media_type: String,
        file_name: Option<String>,
        bytes: &[u8],
    ) -> anyhow::Result<String> {
        let file_name = file_name.unwrap_or_else(|| "uploaded-object".into());
        let position =
            self.log
                .add_pending_object(file_name.clone(), file_name, media_type, bytes)?;
        Ok(format!("pending:{}", position.index() + 1))
    }
}

fn control_path(directory: &FilePath, id: &str) -> PathBuf {
    directory.join(format!("{id}.{CONTROL_EXTENSION}"))
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone, Debug)]
pub struct Config {
    pub directory: PathBuf,
    pub completed_list: PathBuf,
}

#[derive(Clone, Debug)]
pub struct NewSession {
    pub kind: chatend::SessionKind,
    pub created_at: String,
    pub effective_context_tokens: u64,
    pub channel: Value,
}

#[derive(Clone)]
struct AppState {
    config: Config,
    catalog_mutation: Arc<Mutex<()>>,
    session_mutations: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
}

#[derive(Clone)]
pub struct SessionHistory {
    state: AppState,
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    InvalidInput,
    NotFound,
    Conflict,
    Storage,
}

impl ErrorKind {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_request",
            Self::NotFound => "not_found",
            Self::Conflict => "state_conflict",
            Self::Storage => "internal_error",
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<ApiError> for Error {
    fn from(error: ApiError) -> Self {
        Self {
            kind: error.kind,
            message: error.message,
        }
    }
}

#[derive(Debug)]
struct ApiError {
    kind: ErrorKind,
    message: String,
}

impl ApiError {
    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn bad(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidInput, message)
    }

    fn not_found() -> Self {
        Self::new(ErrorKind::NotFound, "Session not found.")
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(error=%error, "Session History request failed");
        Self::new(
            ErrorKind::Storage,
            "An unexpected Session History storage error occurred.",
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionRecord {
    pub id: String,
    pub phase: String,
    pub started_at: String,
    pub updated_at: String,
    pub state: Value,
    pub provenance_id: Option<String>,
    pub version: i64,
    pub last_user_message_at: Option<String>,
    pub ended_at: Option<String>,
    pub ingress_failure_count: i64,
    pub ingress_failures: Value,
    pub ingress_next_attempt_at: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub summary: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegisterSession {
    pub id: String,
    pub started_at: String,
    pub state: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StartSession {
    pub idempotency_id: String,
    pub started_at: String,
    pub session_type: String,
    #[serde(default)]
    pub duration_minutes: Option<f64>,
    #[serde(default)]
    pub custom_prompt: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NewCommand {
    pub idempotency_id: String,
    pub kind: String,
    #[serde(default = "empty_object")]
    pub payload: Value,
}

fn empty_object() -> Value {
    json!({})
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandOutcome {
    #[serde(default = "empty_object")]
    pub outcome: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionCommand {
    pub id: String,
    pub conversation_id: String,
    pub sequence: i64,
    pub kind: String,
    pub payload: Value,
    pub status: String,
    pub cancel_requested: bool,
    pub outcome: Option<Value>,
    pub created_at: String,
    pub processing_started_at: Option<String>,
    pub completed_at: Option<String>,
    pub idempotency_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    pub expected_version: i64,
    pub state: Value,
    #[serde(default)]
    pub user_activity: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExpectedVersion {
    pub expected_version: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RetryIngress {
    pub expected_version: i64,
    pub state: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StartIngress {
    pub expected_version: i64,
    pub provenance_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IngressFailure {
    pub expected_version: i64,
    pub stage: String,
    #[serde(default)]
    pub code: Option<String>,
    pub message: String,
    #[serde(default)]
    pub rounds_used: Option<u64>,
    #[serde(default)]
    pub context_tokens: Option<u64>,
    #[serde(default)]
    pub context_window_tokens: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecordCompletion {
    pub session_object_id: String,
    #[serde(default)]
    pub commit_receipt: Option<CompletionReceipt>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub session_type: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionReceipt {
    #[serde(default)]
    pub transaction_id: Option<String>,
    pub session_object_id: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub session_type: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub committed_at: Option<String>,
    #[serde(default)]
    pub node_ids: BTreeMap<String, String>,
    #[serde(default)]
    pub object_ids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Created<T> {
    pub value: T,
    pub created: bool,
}

#[derive(Clone, Debug)]
pub struct NewObject {
    pub file_name: Option<String>,
    pub media_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct StoredObject {
    pub file_name: String,
    pub media_type: String,
    pub bytes: Vec<u8>,
}

impl SessionHistory {
    pub fn open(config: Config) -> anyhow::Result<Self> {
        create_private_directory(&config.directory)?;
        compact_control_journals(&config.directory)?;
        if let Some(parent) = config
            .completed_list
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            create_private_directory(parent)?;
        }
        if !config.completed_list.exists() {
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&config.completed_list)
                .with_context(|| format!("creating {}", config.completed_list.display()))?;
            file.sync_all()?;
            sync_directory(
                config
                    .completed_list
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| FilePath::new(".")),
            )?;
        }
        Ok(Self {
            state: AppState {
                config,
                catalog_mutation: Arc::new(Mutex::new(())),
                session_mutations: Arc::new(Mutex::new(HashMap::new())),
            },
        })
    }

    pub fn health(&self) -> Result<(), Error> {
        read_completed_ids(&self.state.config.completed_list).map_err(ApiError::internal)?;
        Ok(())
    }

    pub fn create_session(&self, input: NewSession) -> anyhow::Result<Session> {
        let metadata = chatend::SessionMetadata {
            session_id: Uuid::new_v4().to_string(),
            kind: input.kind,
            created_at: input.created_at,
            effective_context_tokens: input.effective_context_tokens,
            channel: input.channel,
        };
        Session::create(
            self.state
                .config
                .directory
                .join(format!("{}.session-log", metadata.session_id)),
            metadata,
        )
    }

    pub fn open_session(&self, metadata: chatend::SessionMetadata) -> anyhow::Result<Session> {
        validate_session_id(&metadata.session_id)
            .map_err(|error| anyhow::anyhow!(error.message))?;
        Session::open_with_metadata(
            self.state
                .config
                .directory
                .join(format!("{}.session-log", metadata.session_id)),
            metadata,
        )
    }

    pub async fn register(&self, input: RegisterSession) -> Result<SessionRecord, Error> {
        create_session(self.state.clone(), input)
            .await
            .map_err(Into::into)
    }

    pub async fn start(&self, input: StartSession) -> Result<Created<SessionRecord>, Error> {
        let (created, record) = start_managed_session(self.state.clone(), input).await?;
        Ok(Created {
            value: record,
            created,
        })
    }

    pub async fn list(&self) -> Result<Vec<SessionRecord>, Error> {
        let value = list_session_summaries(self.state.clone()).await?;
        serde_json::from_value(
            value
                .get("conversations")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .map_err(ApiError::internal)
        .map_err(Into::into)
    }

    pub async fn get(&self, id: &str) -> Result<SessionRecord, Error> {
        get_session(self.state.clone(), id.to_owned())
            .await
            .map_err(Into::into)
    }

    pub async fn enqueue(
        &self,
        id: &str,
        input: NewCommand,
    ) -> Result<Created<SessionCommand>, Error> {
        let (created, command) =
            queue_session_command(self.state.clone(), id.to_owned(), input).await?;
        Ok(Created {
            value: command,
            created,
        })
    }

    pub async fn command_heads(&self) -> Result<Vec<SessionCommand>, Error> {
        let value = list_command_heads(self.state.clone()).await?;
        serde_json::from_value(value.get("commands").cloned().unwrap_or_else(|| json!([])))
            .map_err(ApiError::internal)
            .map_err(Into::into)
    }

    pub async fn claim_command(&self, id: &str) -> Result<SessionCommand, Error> {
        claim_command(self.state.clone(), id.to_owned())
            .await
            .map_err(Into::into)
    }

    pub async fn complete_command(
        &self,
        id: &str,
        outcome: CommandOutcome,
    ) -> Result<SessionCommand, Error> {
        complete_command(self.state.clone(), id.to_owned(), outcome)
            .await
            .map_err(Into::into)
    }

    pub async fn stop(&self, id: &str) -> Result<SessionRecord, Error> {
        let current = fetch_active(&self.state, id)?;
        let record = transition(
            self.state.clone(),
            id,
            current.version,
            "ingress_pending",
            None,
        )
        .await?;
        Ok(record)
    }

    pub async fn stage_object(&self, id: &str, object: NewObject) -> Result<String, Error> {
        stage_session_object(&self.state, id, object).map_err(Into::into)
    }

    pub fn object(&self, id: &str, pending_id: &str) -> Result<StoredObject, Error> {
        get_session_object(&self.state, id, pending_id).map_err(Into::into)
    }

    pub async fn checkpoint(&self, id: &str, input: Checkpoint) -> Result<SessionRecord, Error> {
        let record = checkpoint(
            self.state.clone(),
            id,
            input.expected_version,
            input.state,
            input.user_activity,
        )
        .await?;
        Ok(record)
    }

    pub async fn request_ingress(
        &self,
        id: &str,
        input: Checkpoint,
    ) -> Result<SessionRecord, Error> {
        let record =
            transition_with_checkpoint(self.state.clone(), id, input, "ingress_pending").await?;
        Ok(record)
    }

    pub async fn start_ingress(
        &self,
        id: &str,
        input: StartIngress,
    ) -> Result<SessionRecord, Error> {
        let record = transition(
            self.state.clone(),
            id,
            input.expected_version,
            "ingress_in_progress",
            Some(input.provenance_id),
        )
        .await?;
        Ok(record)
    }

    pub async fn complete_ingress(
        &self,
        id: &str,
        input: ExpectedVersion,
    ) -> Result<SessionRecord, Error> {
        let current = fetch_active(&self.state, id)?;
        let record = complete_session(
            self.state.clone(),
            id,
            input.expected_version,
            current.state,
        )
        .await?;
        Ok(record)
    }

    pub async fn fail_ingress(
        &self,
        id: &str,
        input: IngressFailure,
    ) -> Result<SessionRecord, Error> {
        let record = record_ingress_failure(self.state.clone(), id, input).await?;
        Ok(record)
    }

    pub async fn retry_ingress(
        &self,
        id: &str,
        input: RetryIngress,
    ) -> Result<SessionRecord, Error> {
        retry_ingress(self.state.clone(), id.to_owned(), input)
            .await
            .map_err(Into::into)
    }

    pub async fn release_interrupted_ingress(&self) -> Result<Vec<String>, Error> {
        let value = release_ingress_repairs(self.state.clone()).await?;
        serde_json::from_value(value.get("released").cloned().unwrap_or_else(|| json!([])))
            .map_err(ApiError::internal)
            .map_err(Into::into)
    }

    pub async fn complete(&self, id: &str, input: Checkpoint) -> Result<SessionRecord, Error> {
        let record =
            complete_session(self.state.clone(), id, input.expected_version, input.state).await?;
        Ok(record)
    }

    pub async fn record_completion(&self, input: RecordCompletion) -> Result<(), Error> {
        record_completed_session(self.state.clone(), input).await?;
        Ok(())
    }
}

async fn record_completed_session(
    state: AppState,
    input: RecordCompletion,
) -> Result<Value, ApiError> {
    let session_guard = input
        .session_id
        .as_deref()
        .map(|id| session_mutation(&state, id))
        .transpose()?;
    let _session_guard = session_guard
        .as_ref()
        .map(|guard| guard.lock().map_err(ApiError::internal))
        .transpose()?;
    let _catalog_guard = state.catalog_mutation.lock().map_err(ApiError::internal)?;
    let mut receipt = input.commit_receipt.unwrap_or(CompletionReceipt {
        transaction_id: None,
        session_object_id: input.session_object_id.clone(),
        session_id: None,
        session_type: None,
        created_at: None,
        committed_at: None,
        node_ids: BTreeMap::new(),
        object_ids: BTreeMap::new(),
    });
    if receipt.session_object_id != input.session_object_id {
        return Err(ApiError::conflict(
            "completion receipt and requested session object differ",
        ));
    }
    receipt.session_id = receipt.session_id.or(input.session_id.clone());
    receipt.session_type = receipt.session_type.or(input.session_type);
    receipt.created_at = receipt.created_at.or(input.created_at);
    receipt.committed_at.get_or_insert_with(now);
    append_completion_receipt(&state.config.completed_list, &receipt)
        .map_err(ApiError::internal)?;
    if let Some(id) = input.session_id {
        validate_session_id(&id)?;
        let path = state.config.directory.join(format!("{id}.session-log"));
        if path.exists() {
            SessionJournal::open(&path)
                .map_err(ApiError::internal)?
                .log
                .delete_committed()
                .map_err(ApiError::internal)?;
            let control = control_path(&state.config.directory, &id);
            if control.exists() {
                std::fs::remove_file(&control)
                    .with_context(|| format!("removing {}", control.display()))
                    .map_err(ApiError::internal)?;
                sync_directory(&state.config.directory).map_err(ApiError::internal)?;
            }
        }
    }
    Ok(json!({
        "sessionObjectId":input.session_object_id,
        "recorded":true
    }))
}

async fn create_session(
    state: AppState,
    input: RegisterSession,
) -> Result<SessionRecord, ApiError> {
    validate_started_at(&input.started_at)?;
    validate_session_id(&input.id)?;
    let path = state
        .config
        .directory
        .join(format!("{}.session-log", input.id));
    let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
    let id = journal.list().header.session_id;
    if id != input.id {
        return Err(ApiError::bad(
            "registered session ID does not match its durable session log",
        ));
    }
    drop(journal);
    let session_guard = session_mutation(&state, &id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
    if latest_lifecycle(&journal).is_some() {
        return Err(ApiError::conflict("Session is already registered."));
    }
    let record = SessionRecord {
        id,
        phase: "active".into(),
        started_at: input.started_at.clone(),
        updated_at: input.started_at,
        state: control_state(&input.state),
        provenance_id: None,
        version: 1,
        last_user_message_at: None,
        ended_at: None,
        ingress_failure_count: 0,
        ingress_failures: json!([]),
        ingress_next_attempt_at: None,
        summary: false,
    };
    append_lifecycle(&mut journal, &record)?;
    Ok(materialize(record, &journal))
}

async fn start_managed_session(
    state: AppState,
    input: StartSession,
) -> Result<(bool, SessionRecord), ApiError> {
    validate_started_at(&input.started_at)?;
    validate_idempotency(&input.idempotency_id)?;
    let _catalog_guard = state.catalog_mutation.lock().map_err(ApiError::internal)?;
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
        if let Some(record) = latest_lifecycle(&journal)
            && record
                .state
                .get("startIdempotencyId")
                .and_then(Value::as_str)
                == Some(&input.idempotency_id)
        {
            return Ok((false, materialize(record, &journal)));
        }
    }
    let id = Uuid::new_v4().to_string();
    let mut journal = SessionJournal::create(&state.config.directory, &id, &input.started_at)
        .map_err(ApiError::internal)?;
    let mut session_state = json!({
        "stateVersion":3,
        "sessionId":id,
        "sessionType":input.session_type,
        "startedAt":input.started_at,
        "startIdempotencyId":input.idempotency_id,
        "orchestration":{"owner":"backend","status":"idle"},
    });
    if input.session_type == "free-time" {
        session_state["selfTimeIntent"] = json!({
            "requestedAt":input.started_at,
            "durationMinutes":input.duration_minutes,
            "customPrompt":input.custom_prompt.unwrap_or_default(),
        });
    }
    let record = SessionRecord {
        id,
        phase: "active".into(),
        started_at: input.started_at.clone(),
        updated_at: input.started_at,
        state: session_state,
        provenance_id: None,
        version: 1,
        last_user_message_at: None,
        ended_at: None,
        ingress_failure_count: 0,
        ingress_failures: json!([]),
        ingress_next_attempt_at: None,
        summary: false,
    };
    append_lifecycle(&mut journal, &record)?;
    Ok((true, materialize(record, &journal)))
}

async fn list_session_summaries(state: AppState) -> Result<Value, ApiError> {
    let mut sessions = Vec::new();
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
        if let Some(mut record) = latest_lifecycle(&journal) {
            record.summary = true;
            record.state = summary_state(&record.state, &journal);
            sessions.push(record);
        }
    }
    for receipt in
        read_completion_receipts(&state.config.completed_list).map_err(ApiError::internal)?
    {
        let object_id = receipt.session_object_id.clone();
        sessions.push(SessionRecord {
            id: object_id.clone(),
            phase: "complete".into(),
            started_at: receipt.created_at.clone().unwrap_or_default(),
            updated_at: receipt.committed_at.clone().unwrap_or_default(),
            state: json!({
                "sessionObjectId":object_id,
                "sessionId":receipt.session_id.clone(),
                "sessionType":receipt.session_type.clone(),
                "commitReceipt":receipt,
            }),
            provenance_id: None,
            version: 1,
            last_user_message_at: None,
            ended_at: None,
            ingress_failure_count: 0,
            ingress_failures: json!([]),
            ingress_next_attempt_at: None,
            summary: true,
        });
    }
    sessions.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    Ok(json!({"conversations":sessions}))
}

async fn get_session(state: AppState, id: String) -> Result<SessionRecord, ApiError> {
    if let Some(receipt) = read_completion_receipts(&state.config.completed_list)
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|receipt| receipt.session_object_id == id)
    {
        let receipt_value = serde_json::to_value(&receipt).map_err(ApiError::internal)?;
        return Ok(SessionRecord {
            id: id.clone(),
            phase: "complete".into(),
            started_at: receipt.created_at.clone().unwrap_or_default(),
            updated_at: receipt.committed_at.clone().unwrap_or_default(),
            state: json!({
                "sessionObjectId":id,
                "sessionId":receipt.session_id,
                "sessionType":receipt.session_type,
                "commitReceipt":receipt_value,
            }),
            provenance_id: None,
            version: 1,
            last_user_message_at: None,
            ended_at: None,
            ingress_failure_count: 0,
            ingress_failures: json!([]),
            ingress_next_attempt_at: None,
            summary: false,
        });
    }
    let journal = open_by_id(&state, &id)?;
    let record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    Ok(materialize(record, &journal))
}

fn get_session_object(
    state: &AppState,
    id: &str,
    pending_id: &str,
) -> Result<StoredObject, ApiError> {
    let number = pending_id
        .strip_prefix("pending:")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::bad("Object ID must have the form pending:N."))?;
    let journal = open_by_id(state, id)?;
    let object = journal
        .log
        .read_pending_object(EventPosition(number - 1))
        .map_err(|error| {
            tracing::warn!(session_id=id, pending_id, %error, "pending session object is unavailable");
            ApiError::not_found()
        })?;
    let media_type = if object.media_type.trim().is_empty()
        || object
            .media_type
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || !object.media_type.contains('/')
    {
        "application/octet-stream"
    } else {
        &object.media_type
    };
    let mut file_name = object
        .file_name
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() && !matches!(character, '"' | '\\' | '/' | ';') {
                character
            } else {
                '_'
            }
        })
        .take(255)
        .collect::<String>();
    if file_name.is_empty() {
        file_name = "uploaded-object".into();
    }
    Ok(StoredObject {
        file_name,
        media_type: media_type.to_owned(),
        bytes: object.bytes,
    })
}

async fn queue_session_command(
    state: AppState,
    id: String,
    input: NewCommand,
) -> Result<(bool, SessionCommand), ApiError> {
    validate_idempotency(&input.idempotency_id)?;
    let session_guard = session_mutation(&state, &id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, &id)?;
    let record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    if record.phase != "active" {
        return Err(ApiError::conflict("Session is no longer active."));
    }
    let commands = commands(&journal);
    if let Some(command) = commands
        .values()
        .find(|command| command.idempotency_id == input.idempotency_id)
    {
        return Ok((false, command.clone()));
    }
    let command = SessionCommand {
        id: Uuid::new_v4().to_string(),
        conversation_id: id,
        sequence: commands
            .values()
            .map(|command| command.sequence)
            .max()
            .unwrap_or(0)
            + 1,
        kind: input.kind,
        payload: input.payload,
        status: "pending".into(),
        cancel_requested: false,
        outcome: None,
        created_at: now(),
        processing_started_at: None,
        completed_at: None,
        idempotency_id: input.idempotency_id,
    };
    append_command(&mut journal, &command)?;
    Ok((true, command))
}

fn stage_session_object(state: &AppState, id: &str, object: NewObject) -> Result<String, ApiError> {
    let NewObject {
        file_name,
        media_type,
        bytes,
    } = object;
    let session_guard = session_mutation(state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(state, id)?;
    let record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    if record.phase != "active" {
        return Err(ApiError::conflict(
            "Objects can only be supplied to an active session.",
        ));
    }
    if commands(&journal)
        .values()
        .any(|command| matches!(command.status.as_str(), "pending" | "processing"))
    {
        return Err(ApiError::conflict(
            "Objects cannot be supplied while the session is processing a command.",
        ));
    }
    let pending_id = journal
        .stage_object(media_type, file_name, &bytes)
        .map_err(|error| ApiError::bad(error.to_string()))?;
    Ok(pending_id)
}

async fn list_command_heads(state: AppState) -> Result<Value, ApiError> {
    let mut heads = Vec::new();
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(path).map_err(ApiError::internal)?;
        let mut active = commands(&journal)
            .into_values()
            .filter(|command| matches!(command.status.as_str(), "pending" | "processing"))
            .collect::<Vec<_>>();
        active.sort_by_key(|command| command.sequence);
        if let Some(head) = active.into_iter().next() {
            heads.push(head);
        }
    }
    heads.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    Ok(json!({"commands":heads}))
}

async fn claim_command(state: AppState, command_id: String) -> Result<SessionCommand, ApiError> {
    mutate_command(&state, &command_id, |command| {
        if command.status == "pending" {
            command.status = "processing".into();
            command.processing_started_at = Some(now());
        } else if command.status != "processing" {
            return Err(ApiError::conflict("Command is already complete."));
        }
        Ok(())
    })
}

async fn complete_command(
    state: AppState,
    command_id: String,
    input: CommandOutcome,
) -> Result<SessionCommand, ApiError> {
    mutate_command(&state, &command_id, |command| {
        if command.status == "complete" {
            return Ok(());
        }
        if command.status != "processing" {
            return Err(ApiError::conflict("Command was not claimed."));
        }
        command.status = "complete".into();
        command.outcome = Some(input.outcome);
        command.completed_at = Some(now());
        Ok(())
    })
}

fn mutate_command(
    state: &AppState,
    command_id: &str,
    mutation: impl FnOnce(&mut SessionCommand) -> Result<(), ApiError>,
) -> Result<SessionCommand, ApiError> {
    let mut target = None;
    for path in journal_paths(&state.config.directory)? {
        let journal = SessionJournal::open(&path).map_err(ApiError::internal)?;
        if let Some(command) = commands(&journal).remove(command_id) {
            target = Some((path, command.conversation_id));
            break;
        }
    }
    let (path, conversation_id) = target.ok_or_else(ApiError::not_found)?;
    let session_guard = session_mutation(state, &conversation_id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = SessionJournal::open(path).map_err(ApiError::internal)?;
    let mut command = commands(&journal)
        .remove(command_id)
        .ok_or_else(ApiError::not_found)?;
    mutation(&mut command)?;
    append_command(&mut journal, &command)?;
    Ok(command)
}

async fn checkpoint(
    state: AppState,
    id: &str,
    expected_version: i64,
    new_state: Value,
    user_activity: bool,
) -> Result<SessionRecord, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, expected_version)?;
    record.state = control_state(&new_state);
    record.version += 1;
    record.updated_at = now();
    if user_activity {
        record.last_user_message_at = Some(record.updated_at.clone());
    }
    append_lifecycle(&mut journal, &record)?;
    Ok(materialize(record, &journal))
}

async fn transition_with_checkpoint(
    state: AppState,
    id: &str,
    input: Checkpoint,
    phase: &str,
) -> Result<SessionRecord, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, input.expected_version)?;
    record.state = control_state(&input.state);
    record.phase = phase.into();
    if phase == "ingress_pending" {
        record.ingress_next_attempt_at = None;
    }
    record.version += 1;
    record.updated_at = now();
    append_lifecycle(&mut journal, &record)?;
    Ok(materialize(record, &journal))
}

async fn transition(
    state: AppState,
    id: &str,
    expected_version: i64,
    phase: &str,
    provenance_id: Option<String>,
) -> Result<SessionRecord, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, expected_version)?;
    record.phase = phase.into();
    record.provenance_id = provenance_id;
    if phase == "ingress_in_progress" {
        record.ingress_next_attempt_at = None;
    }
    record.version += 1;
    record.updated_at = now();
    append_lifecycle(&mut journal, &record)?;
    Ok(materialize(record, &journal))
}

async fn record_ingress_failure(
    state: AppState,
    id: &str,
    input: IngressFailure,
) -> Result<SessionRecord, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, input.expected_version)?;
    if !matches!(
        record.phase.as_str(),
        "ingress_pending" | "ingress_in_progress"
    ) {
        return Err(ApiError::conflict(
            "Session History ingress is not in an active attempt.",
        ));
    }
    let attempt = record.ingress_failure_count.saturating_add(1);
    let terminal =
        input.code.as_deref() == Some("input_too_large") || attempt >= INGRESS_FAILURE_LIMIT;
    let mut failures = record
        .ingress_failures
        .as_array()
        .cloned()
        .unwrap_or_default();
    failures.push(json!({
        "attempt":attempt,
        "at":now(),
        "stage":input.stage,
        "code":input.code,
        "message":input.message,
        "roundsUsed":input.rounds_used,
        "contextTokens":input.context_tokens,
        "contextWindowTokens":input.context_window_tokens,
    }));
    if failures.len() > RETAINED_INGRESS_FAILURES {
        failures.drain(..failures.len() - RETAINED_INGRESS_FAILURES);
    }
    record.ingress_failures = Value::Array(failures);
    record.ingress_failure_count = attempt;
    record.phase = if terminal {
        "ingress_failed".into()
    } else {
        "ingress_pending".into()
    };
    let updated_at = now();
    record.ingress_next_attempt_at = (!terminal)
        .then(|| (Utc::now() + Duration::seconds(INGRESS_RETRY_DELAY_SECONDS)).to_rfc3339());
    record.version += 1;
    record.updated_at = updated_at;
    append_lifecycle(&mut journal, &record)?;
    if terminal {
        tracing::error!(
            session_id = id,
            attempt,
            stage = %input.stage,
            code = input.code.as_deref().unwrap_or("ingress_error"),
            terminal_reason = if input.code.as_deref() == Some("input_too_large") {
                "non_retryable"
            } else {
                "retry_limit"
            },
            "Session History ingress stopped after a terminal failure"
        );
    }
    Ok(materialize(record, &journal))
}

async fn retry_ingress(
    state: AppState,
    id: String,
    input: RetryIngress,
) -> Result<SessionRecord, ApiError> {
    let session_guard = session_mutation(&state, &id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let mut journal = open_by_id(&state, &id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, input.expected_version)?;
    if record.phase != "ingress_failed" {
        return Err(ApiError::conflict(
            "Session History ingress is not in the failed state.",
        ));
    }
    record.state = control_state(&input.state);
    record.phase = "ingress_pending".into();
    record.ingress_failure_count = 0;
    record.ingress_next_attempt_at = None;
    record.version += 1;
    record.updated_at = now();
    append_lifecycle(&mut journal, &record)?;
    Ok(materialize(record, &journal))
}

async fn release_ingress_repairs(state: AppState) -> Result<Value, ApiError> {
    let mut released = Vec::new();
    for path in journal_paths(&state.config.directory)? {
        let Some(id) = path
            .file_stem()
            .and_then(|value| value.to_str())
            .map(str::to_owned)
        else {
            continue;
        };
        let session_guard = session_mutation(&state, &id)?;
        let _guard = session_guard.lock().map_err(ApiError::internal)?;
        let mut journal = open_by_id(&state, &id)?;
        let Some(mut record) = latest_lifecycle(&journal) else {
            continue;
        };
        if record.phase != "ingress_in_progress" {
            continue;
        }
        record.phase = "ingress_pending".into();
        record.ingress_next_attempt_at = None;
        if record.ingress_failure_count > INGRESS_FAILURE_LIMIT {
            record.ingress_failure_count = 0;
        }
        if let Some(failures) = record.ingress_failures.as_array_mut()
            && failures.len() > RETAINED_INGRESS_FAILURES
        {
            failures.drain(..failures.len() - RETAINED_INGRESS_FAILURES);
        }
        record.version += 1;
        record.updated_at = now();
        append_lifecycle(&mut journal, &record)?;
        released.push(id);
    }
    Ok(json!({"released":released}))
}

async fn complete_session(
    state: AppState,
    id: &str,
    expected_version: i64,
    new_state: Value,
) -> Result<SessionRecord, ApiError> {
    let session_guard = session_mutation(&state, id)?;
    let _guard = session_guard.lock().map_err(ApiError::internal)?;
    let journal = open_by_id(&state, id)?;
    let mut record = latest_lifecycle(&journal).ok_or_else(ApiError::not_found)?;
    require_version(&record, expected_version)?;
    let completion_state = new_state
        .get("historyIngress")
        .filter(|state| state.get("sessionObjectId").is_some_and(Value::is_string))
        .cloned()
        .unwrap_or_else(|| new_state.clone());
    record.state = control_state(&new_state);
    let object_id = completion_state
        .get("sessionObjectId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ApiError::conflict("completed session has no permanent Kweb session object")
        })?
        .to_owned();
    let _catalog_guard = state.catalog_mutation.lock().map_err(ApiError::internal)?;
    let mut receipt = completion_state
        .get("commitReceipt")
        .filter(|receipt| !receipt.is_null())
        .cloned()
        .map(serde_json::from_value::<CompletionReceipt>)
        .transpose()
        .map_err(|error| ApiError::conflict(format!("invalid session commit receipt: {error}")))?
        .unwrap_or(CompletionReceipt {
            transaction_id: None,
            session_object_id: object_id.clone(),
            session_id: None,
            session_type: None,
            created_at: None,
            committed_at: None,
            node_ids: BTreeMap::new(),
            object_ids: BTreeMap::new(),
        });
    if receipt.session_object_id != object_id {
        return Err(ApiError::conflict(
            "session commit receipt names a different archive object",
        ));
    }
    let completed_at = now();
    receipt.session_id = Some(record.id.clone());
    receipt.session_type = record
        .state
        .get("sessionType")
        .and_then(Value::as_str)
        .map(str::to_owned);
    receipt.created_at = Some(record.started_at.clone());
    receipt.committed_at = Some(completed_at.clone());
    append_completion_receipt(&state.config.completed_list, &receipt)
        .map_err(ApiError::internal)?;
    record.phase = "complete".into();
    record.version += 1;
    record.updated_at = completed_at;
    record.ended_at = Some(record.updated_at.clone());
    record.state["sessionObjectId"] = json!(object_id);
    record.state["commitReceipt"] = completion_state
        .get("commitReceipt")
        .cloned()
        .unwrap_or(Value::Null);
    let output = materialize(record, &journal);
    let control_path = control_path(&state.config.directory, id);
    journal.log.delete_committed().map_err(ApiError::internal)?;
    if control_path.exists() {
        std::fs::remove_file(&control_path)
            .with_context(|| format!("removing {}", control_path.display()))
            .map_err(ApiError::internal)?;
        sync_directory(&state.config.directory).map_err(ApiError::internal)?;
    }
    Ok(output)
}

fn fetch_active(state: &AppState, id: &str) -> Result<SessionRecord, ApiError> {
    let journal = open_by_id(state, id)?;
    latest_lifecycle(&journal).ok_or_else(ApiError::not_found)
}

fn session_mutation(state: &AppState, id: &str) -> Result<Arc<Mutex<()>>, ApiError> {
    let mut sessions = state.session_mutations.lock().map_err(ApiError::internal)?;
    sessions.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = sessions.get(id).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(Mutex::new(()));
    sessions.insert(id.to_owned(), Arc::downgrade(&lock));
    Ok(lock)
}

fn open_by_id(state: &AppState, id: &str) -> Result<SessionJournal, ApiError> {
    validate_session_id(id)?;
    let path = state.config.directory.join(format!("{id}.session-log"));
    SessionJournal::open(&path).map_err(|error| {
        if !path.exists() {
            ApiError::not_found()
        } else {
            ApiError::internal(error)
        }
    })
}

fn latest_lifecycle(journal: &SessionJournal) -> Option<SessionRecord> {
    journal
        .records()
        .iter()
        .rev()
        .find(|record| record.kind == LIFECYCLE_SIDEBAND)
        .and_then(|record| serde_json::from_value(record.value.clone()).ok())
}

fn append_lifecycle(journal: &mut SessionJournal, record: &SessionRecord) -> Result<(), ApiError> {
    journal
        .append_control(
            LIFECYCLE_SIDEBAND,
            now(),
            serde_json::to_value(record).map_err(ApiError::internal)?,
        )
        .map_err(ApiError::internal)
}

fn commands(journal: &SessionJournal) -> BTreeMap<String, SessionCommand> {
    let mut commands = BTreeMap::new();
    for record in journal
        .records()
        .iter()
        .filter(|record| record.kind == COMMAND_SIDEBAND)
    {
        if let Ok(command) = serde_json::from_value::<SessionCommand>(record.value.clone()) {
            commands.insert(command.id.clone(), command);
        }
    }
    commands
}

fn append_command(journal: &mut SessionJournal, command: &SessionCommand) -> Result<(), ApiError> {
    journal
        .append_control(
            COMMAND_SIDEBAND,
            now(),
            serde_json::to_value(command).map_err(ApiError::internal)?,
        )
        .map_err(ApiError::internal)
}

fn materialize(mut record: SessionRecord, journal: &SessionJournal) -> SessionRecord {
    let log = journal.list();
    record.state["sessionId"] = json!(log.header.session_id);
    if !record.state.get("transcript").is_some_and(Value::is_array) {
        record.state["transcript"] = Value::Array(
            log.events
                .iter()
                .enumerate()
                .filter_map(|(position, event)| transcript_entry(position, event))
                .collect(),
        );
    }
    if !record.state.get("events").is_some_and(Value::is_array) {
        record.state["events"] = serde_json::to_value(&log.events).unwrap_or(Value::Null);
    }
    if !record
        .state
        .get("chatendText")
        .is_some_and(Value::is_string)
    {
        record.state["chatendText"] = json!(
            log.events
                .iter()
                .map(|event| format!("[{}]\n{}", role_name(event.role), display_text(event)))
                .collect::<Vec<_>>()
                .join("\n\n")
        );
    }
    record
}

fn summary_state(control: &Value, journal: &SessionJournal) -> Value {
    let log = journal.list();
    let first_user = log
        .events
        .iter()
        .find(|event| event.role == Role::UserMessage)
        .map(display_text)
        .map(|text| text.chars().take(512).collect::<String>());
    json!({
        "sessionType":control.get("sessionType"),
        "channel":control.get("channel"),
        "freeTime":control.get("freeTime"),
        "orchestration":control.get("orchestration"),
        "firstUserMessage":first_user,
        "boxCount":log.events.len(),
        "eventCount":log.events.len(),
        "pendingTurn":control.get("pendingTurn").cloned().unwrap_or(Value::Bool(false)),
    })
}

fn persisted_context_kind(event: &kcode_session_log::SessionEvent) -> Option<Value> {
    serde_json::from_str::<Value>(&event.text)
        .ok()?
        .get("kind")
        .cloned()
}

fn display_text(event: &kcode_session_log::SessionEvent) -> String {
    persisted_context_kind(event)
        .and_then(|kind| {
            (kind.get("type").and_then(Value::as_str) == Some("box_created"))
                .then(|| {
                    kind.get("content")
                        .and_then(|content| content.get("text"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .flatten()
        })
        .unwrap_or_else(|| event.text.clone())
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::SystemMessage => "system-message",
        Role::SystemError => "system-error",
        Role::UserMessage => "user-message",
        Role::KennedyMessage => "kennedy-message",
        Role::KennedyToolCall => "kennedy-tool-call",
        Role::ToolResult => "tool-result",
        Role::ToolError => "tool-error",
        Role::Object => "object",
        Role::PendingObject => "pending-object",
    }
}

fn transcript_entry(position: usize, event: &kcode_session_log::SessionEvent) -> Option<Value> {
    let kind = persisted_context_kind(event);
    let box_content = kind
        .as_ref()
        .filter(|kind| kind.get("type").and_then(Value::as_str) == Some("box_created"))
        .and_then(|kind| kind.get("content"));
    let metadata = box_content
        .and_then(|content| content.get("metadata"))
        .filter(|value| value.is_object());
    let role = match event.role {
        Role::UserMessage => "user",
        Role::KennedyMessage => "kennedy",
        Role::SystemError => "system",
        Role::SystemMessage => (box_content?
            .get("metadata")
            .and_then(|metadata| metadata.get("transcriptRole"))
            .and_then(Value::as_str)
            == Some("system"))
        .then_some("system")?,
        _ => return None,
    };
    let mut item = json!({
        "role":role,
        "content":display_text(event),
        "boxId":position + 1,
    });
    if let Some(objects) = box_content
        .and_then(|content| content.get("objects"))
        .filter(|value| value.is_array())
    {
        item["objects"] = objects.clone();
    }
    if let Some(metadata) = metadata {
        for key in ["inputKind", "externalEventId"] {
            if let Some(value) = metadata.get(key) {
                item[key] = value.clone();
            }
        }
        if let Some(attachments) = metadata.get("attachments").filter(|value| value.is_array()) {
            item["attachments"] = attachments.clone();
        } else if let Some(media) = metadata.get("media").filter(|value| value.is_object()) {
            item["attachments"] = json!([media]);
        }
    }
    Some(item)
}

fn control_state(value: &Value) -> Value {
    const KEYS: &[&str] = &[
        "format",
        "version",
        "stateVersion",
        "sessionId",
        "sessionType",
        "sourceSessionType",
        "channel",
        "freeTime",
        "selfTimeIntent",
        "orchestration",
        "provenanceId",
        "rustLibSessionId",
        "rootNodeIds",
        "referenceRootNodeIds",
        "startedAt",
        "pendingTurn",
        "pendingExternalEventId",
        "roundsUsed",
        "completed",
        "sessionObjectId",
        "commitReceipt",
        "commitAuthor",
        "kwebPlan",
        "startIdempotencyId",
        "historyIngress",
    ];
    let mut output = Map::new();
    for key in KEYS {
        if let Some(item) = value.get(*key) {
            if *key == "commitReceipt" && item.is_null() {
                continue;
            }
            let item = if *key == "historyIngress" {
                control_state(item)
            } else {
                item.clone()
            };
            output.insert((*key).into(), item);
        }
    }
    Value::Object(output)
}

fn compact_control_journals(directory: &FilePath) -> anyhow::Result<()> {
    let mut paths = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some(CONTROL_EXTENSION))
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        compact_control_journal(&path)?;
    }
    Ok(())
}

fn compact_control_journal(path: &FilePath) -> anyhow::Result<()> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let original_bytes = file.metadata()?.len();
    if original_bytes >= 16 * 1024 * 1024 {
        tracing::info!(
            path = %path.display(),
            original_bytes,
            "Compacting legacy Session History control journal"
        );
    }
    let mut reader = BufReader::new(file);
    let mut latest_lifecycle = None;
    let mut latest_commands = BTreeMap::<String, (u64, ControlRecord)>::new();
    let mut retained_other = Vec::new();
    let mut sequence = 0_u64;
    let mut needs_rewrite = false;

    loop {
        let mut line = Vec::new();
        let bytes = reader.read_until(b'\n', &mut line)?;
        if bytes == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            needs_rewrite = true;
            break;
        }
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let mut record = parse_control_record(&line)?;
        match record.kind.as_str() {
            LIFECYCLE_SIDEBAND => {
                if let Some(state) = record.value.get_mut("state") {
                    let projected = control_state(state);
                    if *state != projected {
                        *state = projected;
                        needs_rewrite = true;
                    }
                }
                if latest_lifecycle.replace((sequence, record)).is_some() {
                    needs_rewrite = true;
                }
            }
            COMMAND_SIDEBAND => {
                let id = record
                    .value
                    .get("id")
                    .and_then(Value::as_str)
                    .context("session command record has no ID")?
                    .to_owned();
                if latest_commands.insert(id, (sequence, record)).is_some() {
                    needs_rewrite = true;
                }
            }
            _ => retained_other.push((sequence, record)),
        }
        sequence += 1;
    }
    drop(reader);

    if !needs_rewrite {
        return Ok(());
    }

    let mut retained = retained_other;
    retained.extend(latest_lifecycle);
    retained.extend(latest_commands.into_values());
    retained.sort_by_key(|(sequence, _)| *sequence);
    rewrite_control_journal(path, retained.into_iter().map(|(_, record)| record))?;
    tracing::info!(
        path = %path.display(),
        original_bytes,
        compacted_bytes = std::fs::metadata(path)?.len(),
        "Compacted Session History control journal"
    );
    Ok(())
}

fn parse_control_record(line: &[u8]) -> anyhow::Result<ControlRecord> {
    let separator = line
        .iter()
        .position(|byte| *byte == b' ')
        .context("session-control record has no checksum separator")?;
    let expected = std::str::from_utf8(&line[..separator])?;
    let payload = &line[separator + 1..];
    ensure!(
        hex_sha256(payload) == expected,
        "session-control record checksum mismatch"
    );
    serde_json::from_slice(payload).context("decoding session-control record")
}

fn rewrite_control_journal(
    path: &FilePath,
    records: impl IntoIterator<Item = ControlRecord>,
) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or_else(|| FilePath::new("."));
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("session-control filename is not valid UTF-8")?;
    let temporary = parent.join(format!(".{file_name}.compact-{}.tmp", Uuid::new_v4()));
    let result = (|| -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("creating {}", temporary.display()))?;
        file.set_permissions(std::fs::metadata(path)?.permissions())?;
        for record in records {
            write_control_record(&mut file, &record)?;
        }
        file.sync_all()?;
        std::fs::rename(&temporary, path)
            .with_context(|| format!("installing compacted {}", path.display()))?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() && temporary.exists() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn write_control_record(file: &mut File, record: &ControlRecord) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(record)?;
    writeln!(
        file,
        "{} {}",
        hex_sha256(&payload),
        String::from_utf8(payload)?
    )?;
    Ok(())
}

fn journal_paths(directory: &FilePath) -> Result<Vec<PathBuf>, ApiError> {
    let mut paths = std::fs::read_dir(directory)
        .map_err(ApiError::internal)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("session-log"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn read_completed_ids(path: &FilePath) -> anyhow::Result<Vec<String>> {
    Ok(read_completion_receipts(path)?
        .into_iter()
        .map(|receipt| receipt.session_object_id)
        .collect())
}

fn read_completion_receipts(path: &FilePath) -> anyhow::Result<Vec<CompletionReceipt>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut receipts = Vec::new();
    let mut seen = HashSet::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let receipt = if line.starts_with('{') {
            serde_json::from_str::<CompletionReceipt>(line)
                .context("decoding Session History completion receipt")?
        } else {
            CompletionReceipt {
                transaction_id: None,
                session_object_id: line.to_owned(),
                session_id: None,
                session_type: None,
                created_at: None,
                committed_at: None,
                node_ids: BTreeMap::new(),
                object_ids: BTreeMap::new(),
            }
        };
        if seen.insert(receipt.session_object_id.clone()) {
            receipts.push(receipt);
        }
    }
    Ok(receipts)
}

#[cfg(test)]
fn append_completed_id(path: &FilePath, id: &str) -> anyhow::Result<()> {
    append_completion_receipt(
        path,
        &CompletionReceipt {
            transaction_id: None,
            session_object_id: id.into(),
            session_id: None,
            session_type: None,
            created_at: None,
            committed_at: None,
            node_ids: BTreeMap::new(),
            object_ids: BTreeMap::new(),
        },
    )
}

fn append_completion_receipt(path: &FilePath, receipt: &CompletionReceipt) -> anyhow::Result<()> {
    if read_completed_ids(path)?
        .iter()
        .any(|existing| existing == &receipt.session_object_id)
    {
        return Ok(());
    }
    let mut file = OpenOptions::new().append(true).open(path)?;
    writeln!(file, "{}", serde_json::to_string(receipt)?)?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn create_private_directory(path: &FilePath) -> anyhow::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .with_context(|| format!("creating {}", path.display()))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| FilePath::new("."));
    sync_directory(parent)
}

fn sync_directory(path: &FilePath) -> anyhow::Result<()> {
    File::open(path)
        .with_context(|| format!("opening directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing directory {}", path.display()))
}

fn validate_started_at(value: &str) -> Result<(), ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map(|_| ())
        .map_err(|_| ApiError::bad("started_at must be an RFC 3339 timestamp"))
}

fn validate_idempotency(value: &str) -> Result<(), ApiError> {
    if value.is_empty() || value.len() > 255 {
        return Err(ApiError::bad(
            "idempotency_id must contain between 1 and 255 bytes",
        ));
    }
    Ok(())
}

fn validate_session_id(value: &str) -> Result<(), ApiError> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| ApiError::bad("invalid session ID"))
}

fn require_version(record: &SessionRecord, expected: i64) -> Result<(), ApiError> {
    if record.version != expected {
        return Err(ApiError::conflict(format!(
            "Expected session version {expected}, found {}.",
            record.version
        )));
    }
    Ok(())
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "kennedy-session-history-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn service(label: &str) -> SessionHistory {
        let root = root(label);
        std::fs::create_dir_all(&root).unwrap();
        SessionHistory::open(Config {
            directory: root.join("sessions"),
            completed_list: root.join("session-history.txt"),
        })
        .unwrap()
    }

    async fn start(service: &SessionHistory, idempotency_id: &str) -> SessionRecord {
        service
            .start(StartSession {
                idempotency_id: idempotency_id.into(),
                started_at: "2026-07-23T00:00:00Z".into(),
                session_type: "conversation".into(),
                duration_minutes: None,
                custom_prompt: None,
            })
            .await
            .unwrap()
            .value
    }

    #[test]
    fn control_state_retains_only_authoritative_lifecycle_and_recovery_fields() {
        let saved = control_state(&json!({
            "sessionType":"conversation",
            "pendingTurn":true,
            "transcript":[{"role":"user","content":"hello"}],
            "boxCount":1,
            "eventCount":1,
            "boxes":{"1":{"id":1}},
            "events":[{"id":1}],
            "context":{"estimatedTokens":10},
            "chatendText":"hello",
            "historyIngress":{
                "format":"kennedy-chatend",
                "sessionType":"history-ingress",
                "completed":true,
                "commitReceipt":{"sessionObjectId":"A1234567"},
                "boxes":{"2":{"id":2}},
                "events":[{"id":2}],
                "context":{"estimatedTokens":20},
                "chatendText":"ingress"
            },
            "unrecognized":"discard me"
        }));
        assert_eq!(saved["sessionType"], "conversation");
        assert_eq!(saved["pendingTurn"], true);
        assert_eq!(saved["historyIngress"]["sessionType"], "history-ingress");
        assert_eq!(saved["historyIngress"]["completed"], true);
        assert_eq!(
            saved["historyIngress"]["commitReceipt"]["sessionObjectId"],
            "A1234567"
        );
        for field in [
            "transcript",
            "boxCount",
            "eventCount",
            "boxes",
            "events",
            "context",
            "chatendText",
        ] {
            assert!(saved.get(field).is_none(), "{field} was retained");
            assert!(
                saved["historyIngress"].get(field).is_none(),
                "nested {field} was retained"
            );
        }
        assert!(saved.get("unrecognized").is_none());
        assert!(
            control_state(&json!({"commitReceipt":null}))
                .get("commitReceipt")
                .is_none()
        );
    }

    #[test]
    fn startup_compacts_superseded_control_records_without_losing_recovery_state() {
        let root = root("control-compaction");
        let sessions = root.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let id = "9078bb6e-0931-4477-9ce9-b1430d0335a2";
        drop(
            SessionStore::new(&sessions)
                .create_session(id, "2026-07-24T00:00:00Z")
                .unwrap(),
        );
        let control_path = control_path(&sessions, id);
        let mut file = File::create(&control_path).unwrap();
        let base = SessionRecord {
            id: id.into(),
            phase: "active".into(),
            started_at: "2026-07-24T00:00:00Z".into(),
            updated_at: "2026-07-24T00:00:00Z".into(),
            state: json!({
                "sessionType":"conversation",
                "boxes":{"1":{"canonical":{"content":{"text":"x".repeat(20_000)}}}},
                "events":[{"kind":"old presentation"}],
                "context":{"estimatedTokens":5_000},
                "chatendText":"x".repeat(20_000),
            }),
            provenance_id: None,
            version: 1,
            last_user_message_at: None,
            ended_at: None,
            ingress_failure_count: 0,
            ingress_failures: json!([]),
            ingress_next_attempt_at: None,
            summary: false,
        };
        write_control_record(
            &mut file,
            &ControlRecord {
                kind: LIFECYCLE_SIDEBAND.into(),
                recorded_at: "2026-07-24T00:00:00Z".into(),
                value: serde_json::to_value(&base).unwrap(),
            },
        )
        .unwrap();
        let pending_command = SessionCommand {
            id: "command-1".into(),
            conversation_id: id.into(),
            sequence: 1,
            kind: "message".into(),
            payload: json!({"text":"hello"}),
            status: "pending".into(),
            cancel_requested: false,
            outcome: None,
            created_at: "2026-07-24T00:00:01Z".into(),
            processing_started_at: None,
            completed_at: None,
            idempotency_id: "message-1".into(),
        };
        write_control_record(
            &mut file,
            &ControlRecord {
                kind: COMMAND_SIDEBAND.into(),
                recorded_at: "2026-07-24T00:00:01Z".into(),
                value: serde_json::to_value(&pending_command).unwrap(),
            },
        )
        .unwrap();
        let mut latest = base;
        latest.version = 2;
        latest.updated_at = "2026-07-24T00:00:02Z".into();
        latest.state["pendingTurn"] = json!(true);
        latest.state["historyIngress"] = json!({
            "format":"kennedy-chatend",
            "sessionType":"history-ingress",
            "completed":true,
            "commitReceipt":{"sessionObjectId":"A1234567"},
            "boxes":{"2":{"canonical":{"content":{"text":"y".repeat(20_000)}}}},
            "events":[{"kind":"ingress presentation"}],
            "context":{"estimatedTokens":7_000},
            "chatendText":"y".repeat(20_000),
        });
        write_control_record(
            &mut file,
            &ControlRecord {
                kind: LIFECYCLE_SIDEBAND.into(),
                recorded_at: "2026-07-24T00:00:02Z".into(),
                value: serde_json::to_value(&latest).unwrap(),
            },
        )
        .unwrap();
        let mut completed_command = pending_command;
        completed_command.status = "complete".into();
        completed_command.outcome = Some(json!({"accepted":true}));
        completed_command.completed_at = Some("2026-07-24T00:00:03Z".into());
        write_control_record(
            &mut file,
            &ControlRecord {
                kind: COMMAND_SIDEBAND.into(),
                recorded_at: "2026-07-24T00:00:03Z".into(),
                value: serde_json::to_value(&completed_command).unwrap(),
            },
        )
        .unwrap();
        file.sync_all().unwrap();
        drop(file);
        let original_bytes = std::fs::metadata(&control_path).unwrap().len();

        let service = SessionHistory::open(Config {
            directory: sessions.clone(),
            completed_list: root.join("session-history.txt"),
        })
        .unwrap();
        let compacted_bytes = std::fs::metadata(&control_path).unwrap().len();
        assert!(compacted_bytes < original_bytes / 10);
        let journal = open_by_id(&service.state, id).unwrap();
        assert_eq!(journal.records().len(), 2);
        let restored = latest_lifecycle(&journal).unwrap();
        assert_eq!(restored.version, 2);
        assert_eq!(restored.state["pendingTurn"], true);
        assert!(restored.state.get("boxes").is_none());
        assert!(restored.state.get("events").is_none());
        assert!(restored.state.get("context").is_none());
        assert!(restored.state.get("chatendText").is_none());
        assert_eq!(
            restored.state["historyIngress"]["commitReceipt"]["sessionObjectId"],
            "A1234567"
        );
        assert!(restored.state["historyIngress"].get("boxes").is_none());
        let restored_commands = commands(&journal);
        assert_eq!(restored_commands.len(), 1);
        assert_eq!(restored_commands["command-1"].status, "complete");
        assert_eq!(
            restored_commands["command-1"].outcome,
            Some(json!({"accepted":true}))
        );
    }

    #[tokio::test]
    async fn managed_session_log_and_control_journal_remain_coordinated() {
        let service = service("commands");
        let record = start(&service, "start-1").await;
        let id = record.id.clone();
        let mut journal = open_by_id(&service.state, &id).unwrap();
        journal
            .log
            .add_event(Role::SystemError, "The message exceeded capacity.")
            .unwrap();
        drop(journal);
        let command = service
            .enqueue(
                &id,
                NewCommand {
                    idempotency_id: "message-1".into(),
                    kind: "message".into(),
                    payload: json!({"text":"hello"}),
                },
            )
            .await
            .unwrap()
            .value;
        assert_eq!(command.status, "pending");
        let materialized = service.get(&id).await.unwrap();
        assert!(
            materialized.state["chatendText"]
                .as_str()
                .is_some_and(|text| text.contains("[system-error]"))
        );
        assert_eq!(
            materialized.state["transcript"][0],
            json!({
                "role":"system",
                "content":"The message exceeded capacity.",
                "boxId":1,
            })
        );
        let listed = service.command_heads().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            std::fs::read_dir(&service.state.config.directory)
                .unwrap()
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn checkpointed_presentation_is_rebuilt_from_the_session_log() {
        let service = service("live-ui");
        let record = start(&service, "live-ui-start").await;
        let id = record.id.clone();
        let mut journal = open_by_id(&service.state, &id).unwrap();
        journal
            .log
            .add_event(Role::UserMessage, "hello from the log")
            .unwrap();
        drop(journal);
        let boxes = json!({
            "1":{
                "id":1,
                "owner":{"kind":"user"},
                "canonical":{"eventId":1,"content":{"text":"hello"}},
                "representation":{"kind":"hydrated"},
                "active":true
            }
        });
        let context = json!({"items":[{"boxId":1,"text":"hello"}]});
        let checkpointed = service
            .checkpoint(
                &id,
                Checkpoint {
                    expected_version: record.version,
                    state: json!({
                        "sessionId":id,
                        "sessionType":"conversation",
                        "boxes":boxes,
                        "events":[],
                        "context":context,
                        "chatendText":"hello",
                        "historyIngress":{"format":"kennedy-chatend","version":1}
                    }),
                    user_activity: false,
                },
            )
            .await
            .unwrap();
        assert!(checkpointed.state.get("boxes").is_none());
        assert!(checkpointed.state.get("context").is_none());
        assert_eq!(
            checkpointed.state["transcript"][0]["content"],
            "hello from the log"
        );
        assert_eq!(checkpointed.state["events"][0]["role"], "user-message");
        assert!(
            checkpointed.state["chatendText"]
                .as_str()
                .is_some_and(|text| text.contains("hello from the log"))
        );
        assert_eq!(
            checkpointed.state["historyIngress"]["format"],
            "kennedy-chatend"
        );

        let fetched = service.get(&id).await.unwrap();
        assert!(fetched.state.get("boxes").is_none());
        assert!(fetched.state.get("context").is_none());
        assert_eq!(
            fetched.state["transcript"][0]["content"],
            "hello from the log"
        );
    }

    #[tokio::test]
    async fn active_pending_objects_are_streamed_with_original_metadata() {
        let service = service("pending-object");
        let record = start(&service, "pending-object-start").await;
        let id = record.id;
        let mut journal = open_by_id(&service.state, &id).unwrap();
        let position = journal
            .log
            .add_pending_object(
                "object event",
                "photo.png",
                "image/png",
                b"\x89PNG\r\n\x1a\npayload",
            )
            .unwrap();
        let object = service
            .object(&id, &format!("pending:{}", position.index() + 1))
            .unwrap();
        assert_eq!(object.media_type, "image/png");
        assert_eq!(object.file_name, "photo.png");
        assert_eq!(object.bytes, b"\x89PNG\r\n\x1a\npayload");
    }

    #[tokio::test]
    async fn conversation_ingress_failures_back_off_then_stop_and_can_be_retried() {
        let service = service("ingress-retries");
        let created = start(&service, "ingress-retries-start").await;
        let id = created.id.clone();
        let mut record = service
            .request_ingress(
                &id,
                Checkpoint {
                    expected_version: created.version,
                    state: created.state,
                    user_activity: false,
                },
            )
            .await
            .unwrap();

        for attempt in 1..=INGRESS_FAILURE_LIMIT {
            record = service
                .start_ingress(
                    &id,
                    StartIngress {
                        expected_version: record.version,
                        provenance_id: format!("session:{id}"),
                    },
                )
                .await
                .unwrap();
            record = service
                .fail_ingress(
                    &id,
                    IngressFailure {
                        expected_version: record.version,
                        stage: "model_loop".into(),
                        code: Some("ingress_error".into()),
                        message: "transient failure".into(),
                        rounds_used: None,
                        context_tokens: None,
                        context_window_tokens: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(record.ingress_failure_count, attempt);
            if attempt < INGRESS_FAILURE_LIMIT {
                assert_eq!(record.phase, "ingress_pending");
                assert!(record.ingress_next_attempt_at.is_some());
            } else {
                assert_eq!(record.phase, "ingress_failed");
                assert!(record.ingress_next_attempt_at.is_none());
            }
        }
        assert_eq!(
            record.ingress_failures.as_array().unwrap().len(),
            RETAINED_INGRESS_FAILURES
        );

        let retried = service
            .retry_ingress(
                &id,
                RetryIngress {
                    expected_version: record.version,
                    state: record.state,
                },
            )
            .await
            .unwrap();
        assert_eq!(retried.phase, "ingress_pending");
        assert_eq!(retried.ingress_failure_count, 0);
        assert!(retried.ingress_next_attempt_at.is_none());
    }

    #[tokio::test]
    async fn startup_releases_legacy_hot_loop_without_discarding_checkpoint_state() {
        let service = service("ingress-release");
        let created = start(&service, "ingress-release-start").await;
        let id = created.id.clone();
        let pending = service
            .request_ingress(
                &id,
                Checkpoint {
                    expected_version: created.version,
                    state: json!({
                        "sessionType":"conversation",
                        "historyIngress":{"kwebPlan":{"creates":[{"pendingId":"pending:9"}]}}
                    }),
                    user_activity: false,
                },
            )
            .await
            .unwrap();
        let started = service
            .start_ingress(
                &id,
                StartIngress {
                    expected_version: pending.version,
                    provenance_id: format!("session:{id}"),
                },
            )
            .await
            .unwrap();
        let mut journal = open_by_id(&service.state, &id).unwrap();
        let mut legacy = started;
        legacy.ingress_failure_count = 150;
        legacy.ingress_failures = Value::Array(
            (1..=150)
                .map(|attempt| json!({"attempt":attempt,"message":"legacy hot loop"}))
                .collect(),
        );
        legacy.version += 1;
        append_lifecycle(&mut journal, &legacy).unwrap();
        drop(journal);

        let released = service.release_interrupted_ingress().await.unwrap();
        assert_eq!(released, vec![id.clone()]);
        let repaired = service.get(&id).await.unwrap();
        assert_eq!(repaired.phase, "ingress_pending");
        assert_eq!(repaired.ingress_failure_count, 0);
        assert_eq!(
            repaired.ingress_failures.as_array().unwrap().len(),
            RETAINED_INGRESS_FAILURES
        );
        assert_eq!(
            repaired.state["historyIngress"]["kwebPlan"]["creates"][0]["pendingId"],
            "pending:9"
        );
    }

    #[tokio::test]
    async fn nested_history_ingress_receipt_completes_the_source_session() {
        let service = service("nested-completion");
        let record = start(&service, "nested-completion-start").await;
        let id = record.id.clone();
        let mut state = record.state.clone();
        state["historyIngress"] = json!({
            "sessionType":"history-ingress",
            "completed":true,
            "sessionObjectId":"A1234567",
            "commitReceipt":{
                "transactionId":"T1234567",
                "sessionObjectId":"A1234567",
                "nodeIds":{},
                "objectIds":{}
            }
        });
        let completed = service
            .complete(
                &id,
                Checkpoint {
                    expected_version: record.version,
                    state,
                    user_activity: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(completed.phase, "complete");
        assert_eq!(completed.state["sessionObjectId"], "A1234567");
        assert_eq!(
            completed.state["commitReceipt"]["transactionId"],
            "T1234567"
        );
        assert!(
            !service
                .state
                .config
                .directory
                .join(format!("{id}.session-log"))
                .exists()
        );
        assert!(!control_path(&service.state.config.directory, &id).exists());
        assert_eq!(
            read_completion_receipts(&service.state.config.completed_list).unwrap()[0]
                .session_object_id,
            "A1234567"
        );
    }

    #[tokio::test]
    async fn committed_history_records_a_structured_receipt() {
        let service = service("completed");
        append_completed_id(&service.state.config.completed_list, "A1234567").unwrap();
        let listed = service.list().await.unwrap();
        assert_eq!(listed[0].state["sessionObjectId"], "A1234567");
        assert_eq!(
            listed[0].state["commitReceipt"]["sessionObjectId"],
            "A1234567"
        );
        let stored = std::fs::read_to_string(&service.state.config.completed_list).unwrap();
        let receipt: CompletionReceipt = serde_json::from_str(stored.trim()).unwrap();
        assert_eq!(receipt.session_object_id, "A1234567");
    }
}
