use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Context as _;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use kcode_server_object_envelopes::sanitize_file_name;
use serde_json::{Value, json};
use tokio::sync::{Mutex, OnceCell, RwLock};
use uuid::Uuid;

use super::{
    AgentMode, Api, Config, Manuals, RuntimeModel, Session,
    http::{data_url, encode_path},
    session::{SessionOptions, is_agent_loop_round_limit},
};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const STARTUP_RETRY: Duration = Duration::from_secs(2);
const TELEGRAM_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const TELEGRAM_TIMEOUT_NOTICE: &str = "Kennedy could not complete a response within 30 minutes, so this request was stopped. Please send it again if you want to retry it.";

#[derive(Clone)]
struct Runtime {
    manuals: Manuals,
    model: RuntimeModel,
    user_root_node_id: String,
    kennedy_root_node_id: String,
}

#[derive(Debug, Eq, PartialEq)]
enum MissingGroupSessionRecovery {
    CompleteSilentReset,
    DetachCurrent {
        group_id: String,
        telegram_user_id: i64,
    },
}

struct TelegramEventRetry {
    failures: u32,
    not_before: Instant,
    last_error: String,
}

pub(crate) struct Orchestrator {
    config: Config,
    api: Api,
    runtime: OnceCell<Runtime>,
    writer: Arc<Mutex<()>>,
    writer_job_active: AtomicBool,
    commands_in_flight: Mutex<HashSet<String>>,
    events_in_flight: Mutex<HashSet<String>>,
    event_retries: Mutex<HashMap<String, TelegramEventRetry>>,
    group_updates_in_flight: Mutex<HashSet<String>>,
    group_ingress_in_flight: Mutex<HashSet<String>>,
    directory_in_flight: Mutex<HashSet<String>>,
    active_operations: Mutex<HashMap<String, Uuid>>,
    conversation_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    last_poll_error: RwLock<Option<String>>,
}

impl Orchestrator {
    pub(crate) fn new(config: Config, api: Api) -> Self {
        Self {
            config,
            api,
            runtime: OnceCell::new(),
            writer: Arc::new(Mutex::new(())),
            writer_job_active: AtomicBool::new(false),
            commands_in_flight: Mutex::new(HashSet::new()),
            events_in_flight: Mutex::new(HashSet::new()),
            event_retries: Mutex::new(HashMap::new()),
            group_updates_in_flight: Mutex::new(HashSet::new()),
            group_ingress_in_flight: Mutex::new(HashSet::new()),
            directory_in_flight: Mutex::new(HashSet::new()),
            active_operations: Mutex::new(HashMap::new()),
            conversation_locks: Mutex::new(HashMap::new()),
            last_poll_error: RwLock::new(None),
        }
    }

    pub(crate) async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        self.initialize_until_ready().await;
        let wakeup_worker = self.clone();
        tokio::spawn(async move {
            wakeup_worker.run_wakeup_scheduler().await;
        });
        loop {
            match self.poll_once().await {
                Ok(()) => *self.last_poll_error.write().await = None,
                Err(error) => {
                    let message = error.to_string();
                    let mut previous = self.last_poll_error.write().await;
                    if previous.as_deref() != Some(message.as_str()) {
                        tracing::warn!(error=%error, "Backend orchestration poll will retry");
                        *previous = Some(message);
                    }
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn initialize_until_ready(&self) {
        let mut previous = None;
        loop {
            match self.initialize().await {
                Ok(runtime) => {
                    let model = runtime.model.model.clone();
                    let _ = self.runtime.set(runtime);
                    tracing::info!(%model, "Native Rust orchestration worker ready");
                    return;
                }
                Err(error) => {
                    let message = error.to_string();
                    if previous.as_deref() != Some(message.as_str()) {
                        tracing::warn!(error=%error, "Waiting for Kennedy services before starting orchestration");
                        previous = Some(message);
                    }
                    tokio::time::sleep(STARTUP_RETRY).await;
                }
            }
        }
    }

    async fn initialize(&self) -> anyhow::Result<Runtime> {
        self.api.kmap_node(self.api.user_root_node_id())?;
        self.api.kmap_node(self.api.kennedy_root_node_id())?;
        let (history, telegram) =
            tokio::join!(self.api.history_health(), self.api.telegram_health());
        history?;
        telegram?;
        let manuals = Manuals::load(&self.config.system_prompts_directory)?;
        let runtime = Runtime {
            manuals,
            model: self.config.runtime_model.clone(),
            user_root_node_id: self.api.user_root_node_id().to_owned(),
            kennedy_root_node_id: self.api.kennedy_root_node_id().to_owned(),
        };
        self.api.history_release_interrupted_ingress().await?;
        Ok(runtime)
    }

    fn runtime(&self) -> anyhow::Result<&Runtime> {
        self.runtime
            .get()
            .context("orchestration runtime is not initialized")
    }

    async fn run_wakeup_scheduler(self: Arc<Self>) {
        loop {
            let marker = next_wakeup_marker(Utc::now());
            let delay = (marker - Utc::now()).to_std().unwrap_or(Duration::ZERO);
            tokio::time::sleep(delay).await;
            if let Err(error) = self.create_wakeup_sessions(marker).await {
                tracing::warn!(
                    marker=%marker.to_rfc3339(),
                    error=%error,
                    "Scheduled wakeup session creation failed; this marker will not be retried"
                );
            }
        }
    }

    async fn create_wakeup_sessions(&self, marker: DateTime<Utc>) -> anyhow::Result<()> {
        let private_sessions = self.api.telegram_get("/api/v1/private-sessions").await?;
        for private_session in private_sessions
            .get("sessions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(telegram_user_id) = private_session
                .get("telegramUserId")
                .and_then(Value::as_i64)
            else {
                tracing::warn!("Telegram private-session discovery returned no numeric user ID");
                continue;
            };
            if let Err(error) = self.create_wakeup_session(telegram_user_id, marker).await {
                tracing::warn!(
                    %telegram_user_id,
                    marker=%marker.to_rfc3339(),
                    error=%error,
                    "Could not create this user's scheduled wakeup session"
                );
            }
        }
        Ok(())
    }

    async fn create_wakeup_session(
        &self,
        telegram_user_id: i64,
        marker: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let runtime = self.runtime()?.clone();
        let user = self.api.directory_user(telegram_user_id).await?;
        let user_root = user
            .root_node_id
            .context("Telegram user root is not ready for a wakeup session")?;
        let mut options =
            SessionOptions::conversation("wakeup", vec![user_root, runtime.kennedy_root_node_id]);
        options.mode = AgentMode::Wakeup;
        options.channel = json!({
            "kind":"wakeup",
            "telegramUserId":telegram_user_id,
            "username":user.current_username.or(Some(user.handle)),
            "displayName":user.display_name,
            "wakeupMarker":marker.to_rfc3339(),
        });
        options.orchestration = json!({"owner":"backend","status":"scheduled"});
        let mut session = Session::new(
            self.api.clone(),
            runtime.manuals,
            runtime.model,
            options,
            None,
        )
        .await?;
        session.stage_wakeup_opening()?;
        let state = session.snapshot()?;
        self.api
            .history_register(kcode_session_history::RegisterSession {
                id: required_string(&state, "sessionId")?,
                started_at: session.started_at.clone(),
                state,
            })
            .await?;
        Ok(())
    }

    async fn poll_once(self: &Arc<Self>) -> anyhow::Result<()> {
        self.api.synchronize_audio_ingress().await?;
        let histories = self.list_history().await?;
        self.sync_conversation_commands().await?;
        self.sync_directory_provisioning().await?;
        self.sync_group_updates().await?;
        self.sync_group_ingress().await?;
        self.sync_telegram_events().await?;
        self.schedule_writer_job(&histories).await?;
        Ok(())
    }

    async fn list_history(&self) -> anyhow::Result<Vec<Value>> {
        Ok(self
            .api
            .history_list()
            .await?
            .get("conversations")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    async fn conversation_lock(&self, id: &str) -> Arc<Mutex<()>> {
        self.conversation_locks
            .lock()
            .await
            .entry(id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn sync_conversation_commands(self: &Arc<Self>) -> anyhow::Result<()> {
        let commands = self
            .api
            .history_command_heads()
            .await?
            .get("commands")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for command in commands {
            let id = required_string(&command, "id")?;
            let conversation_id = required_string(&command, "conversationId")?;
            if command
                .get("cancelRequested")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && self.commands_in_flight.lock().await.contains(&id)
            {
                if let Some(operation) = self
                    .active_operations
                    .lock()
                    .await
                    .get(&conversation_id)
                    .copied()
                {
                    let _ = self.api.cancel_intelligence(operation);
                }
                continue;
            }
            let mut in_flight = self.commands_in_flight.lock().await;
            if !in_flight.insert(id.clone()) {
                continue;
            }
            drop(in_flight);
            let worker = self.clone();
            tokio::spawn(async move {
                if let Err(error) = worker.process_conversation_command(command).await {
                    tracing::warn!(command_id=%id, error=%error, "Browser conversation command will retry");
                }
                worker.commands_in_flight.lock().await.remove(&id);
            });
        }
        Ok(())
    }

    async fn process_conversation_command(&self, command: Value) -> anyhow::Result<()> {
        let command_id = required_string(&command, "id")?;
        let conversation_id = required_string(&command, "conversationId")?;
        let lock = self.conversation_lock(&conversation_id).await;
        let _conversation_guard = lock.lock().await;
        let command = if command.get("status").and_then(Value::as_str) == Some("pending") {
            self.api.history_claim_command(&command_id).await?
        } else {
            command
        };
        let record = self.get_conversation(&conversation_id).await?;
        if record.get("phase").and_then(Value::as_str) != Some("active")
            || !is_browser_conversation(&record)
        {
            self.complete_command(&command_id, json!({"status":"conversation_closed"}))
                .await?;
            return Ok(());
        }
        let kind = required_string(&command, "kind")?;
        let payload = command.get("payload").cloned().unwrap_or_else(|| json!({}));
        let record = Arc::new(Mutex::new(record));
        if command
            .get("cancelRequested")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let mut state = record
                .lock()
                .await
                .get("state")
                .cloned()
                .unwrap_or_else(|| json!({}));
            state["orchestration"] = json!({"owner":"backend","status":"stopped"});
            persist_record(&self.api, &record, state, false).await?;
            self.complete_command(&command_id, json!({"status":"stopped"}))
                .await?;
            return Ok(());
        }
        if kind == "end" {
            let mut state = record
                .lock()
                .await
                .get("state")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let abandoned_pending_turn = state
                .get("pendingTurn")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            state["orchestration"] = json!({
                "owner":"backend",
                "status":"ending",
                "abandonedPendingTurn":abandoned_pending_turn,
            });
            if let Some(session_id) = state.get("rustLibSessionId").and_then(Value::as_str) {
                self.api.release_managed_sources(session_id).await;
            }
            self.request_conversation_ingress(&record, Some(state))
                .await?;
            self.complete_command(&command_id, json!({"status":"closed"}))
                .await?;
            return Ok(());
        }
        let mut session = {
            let locked = record.lock().await;
            self.session_for_record(&locked).await?
        };
        if session.orchestration.get("owner").and_then(Value::as_str) != Some("backend") {
            session.orchestration = json!({"owner":"backend","status":"idle"});
            persist_record(&self.api, &record, session.snapshot()?, false).await?;
        }
        let external_event_id = format!("web:{command_id}");
        let outcome = match kind.as_str() {
            "message" => {
                if session
                    .answer_for_external_event(&external_event_id)
                    .is_none()
                {
                    if !session.pending_turn {
                        let mut metadata = payload
                            .get("metadata")
                            .cloned()
                            .unwrap_or_else(|| json!({}));
                        metadata["externalEventId"] = json!(external_event_id);
                        anyhow::ensure!(
                            session.begin_user_turn(
                                payload
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default(),
                                &metadata,
                            ),
                            "The queued message contained no usable input"
                        );
                    }
                    session.orchestration = json!({"owner":"backend","status":"working"});
                    persist_record(&self.api, &record, session.snapshot()?, true).await?;
                    let operation = Uuid::new_v4();
                    self.active_operations
                        .lock()
                        .await
                        .insert(conversation_id.clone(), operation);
                    let api = self.api.clone();
                    let saved_record = record.clone();
                    let result = session
                        .run_pending_turn(operation, move |state| {
                            let api = api.clone();
                            let record = saved_record.clone();
                            async move {
                                persist_record(&api, &record, state, false).await?;
                                Ok(())
                            }
                        })
                        .await;
                    self.active_operations.lock().await.remove(&conversation_id);
                    if let Err(error) = result {
                        let round_limit = is_agent_loop_round_limit(&error);
                        session.orchestration = if is_cancelled(&error) {
                            json!({"owner":"backend","status":"stopped"})
                        } else if round_limit {
                            json!({"owner":"backend","status":"stopped","lastError":bounded_error(&error)})
                        } else {
                            json!({"owner":"backend","status":"retrying","lastError":bounded_error(&error)})
                        };
                        let persisted =
                            persist_record(&self.api, &record, session.snapshot()?, false).await;
                        if round_limit {
                            persisted?;
                            tracing::warn!(command_id=%command_id, "Browser conversation stopped at the tool-loop round limit");
                            self.complete_command(
                                &command_id,
                                json!({"status":"stopped","reason":"tool_loop_round_limit"}),
                            )
                            .await?;
                            return Ok(());
                        }
                        persisted.ok();
                        if is_cancelled(&error) {
                            self.complete_command(&command_id, json!({"status":"stopped"}))
                                .await?;
                            return Ok(());
                        }
                        return Err(error);
                    }
                    if session.requires_history_ingress() {
                        session.orchestration =
                            json!({"owner":"backend","status":"ending","reason":"context-limit"});
                        persist_record(&self.api, &record, session.snapshot()?, false).await?;
                        self.request_conversation_ingress(&record, None).await?;
                        self.complete_command(
                            &command_id,
                            json!({"status":"closed","reason":"context_limit"}),
                        )
                        .await?;
                        return Ok(());
                    }
                }
                anyhow::ensure!(
                    session
                        .answer_for_external_event(&external_event_id)
                        .is_some(),
                    "Kennedy completed the web turn without a recoverable response"
                );
                session.orchestration = json!({"owner":"backend","status":"idle"});
                persist_record(&self.api, &record, session.snapshot()?, false).await?;
                json!({"status":"answered"})
            }
            "retry" => {
                if session.pending_turn {
                    session.reset_exhausted_turn_rounds_for_retry();
                    session.orchestration = json!({"owner":"backend","status":"working"});
                    persist_record(&self.api, &record, session.snapshot()?, false).await?;
                    let operation = Uuid::new_v4();
                    self.active_operations
                        .lock()
                        .await
                        .insert(conversation_id.clone(), operation);
                    let api = self.api.clone();
                    let saved_record = record.clone();
                    let result = session
                        .run_pending_turn(operation, move |state| {
                            let api = api.clone();
                            let record = saved_record.clone();
                            async move {
                                persist_record(&api, &record, state, false).await?;
                                Ok(())
                            }
                        })
                        .await;
                    self.active_operations.lock().await.remove(&conversation_id);
                    if let Err(error) = result {
                        let round_limit = is_agent_loop_round_limit(&error);
                        session.orchestration = if is_cancelled(&error) {
                            json!({"owner":"backend","status":"stopped"})
                        } else if round_limit {
                            json!({"owner":"backend","status":"stopped","lastError":bounded_error(&error)})
                        } else {
                            json!({"owner":"backend","status":"retrying","lastError":bounded_error(&error)})
                        };
                        let persisted =
                            persist_record(&self.api, &record, session.snapshot()?, false).await;
                        if round_limit {
                            persisted?;
                            tracing::warn!(command_id=%command_id, "Browser conversation stopped at the tool-loop round limit");
                            self.complete_command(
                                &command_id,
                                json!({"status":"stopped","reason":"tool_loop_round_limit"}),
                            )
                            .await?;
                            return Ok(());
                        }
                        persisted.ok();
                        if is_cancelled(&error) {
                            self.complete_command(&command_id, json!({"status":"stopped"}))
                                .await?;
                            return Ok(());
                        }
                        return Err(error);
                    }
                    if session.requires_history_ingress() {
                        session.orchestration =
                            json!({"owner":"backend","status":"ending","reason":"context-limit"});
                        persist_record(&self.api, &record, session.snapshot()?, false).await?;
                        self.request_conversation_ingress(&record, None).await?;
                        self.complete_command(
                            &command_id,
                            json!({"status":"closed","reason":"context_limit"}),
                        )
                        .await?;
                        return Ok(());
                    }
                }
                session.orchestration = json!({"owner":"backend","status":"idle"});
                persist_record(&self.api, &record, session.snapshot()?, false).await?;
                json!({"status":"retried"})
            }
            "send-and-end" => {
                anyhow::ensure!(
                    !session.pending_turn,
                    "The saved query must finish before this conversation can end"
                );
                if !session.transcript.iter().any(|item| {
                    item.get("externalEventId").and_then(Value::as_str) == Some(&external_event_id)
                }) {
                    let mut metadata = payload
                        .get("metadata")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    metadata["externalEventId"] = json!(external_event_id);
                    anyhow::ensure!(
                        session.append_final_user_message(
                            payload
                                .get("text")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            &metadata
                        ),
                        "The final conversation command contained no usable input"
                    );
                }
                persist_record(&self.api, &record, session.snapshot()?, true).await?;
                self.close_conversation(&record, &session).await?;
                json!({"status":"closed"})
            }
            _ => anyhow::bail!("Unsupported browser conversation command {kind}"),
        };
        self.complete_command(&command_id, outcome).await?;
        Ok(())
    }

    async fn session_for_record(&self, record: &Value) -> anyhow::Result<Session> {
        let runtime = self.runtime()?.clone();
        let mut state = record.get("state").cloned().unwrap_or_else(|| json!({}));
        let session_type = session_type(record);
        if matches!(session_type.as_str(), "telegram" | "telegram-group") {
            if !state.get("channel").is_some_and(Value::is_object) {
                state["channel"] = json!({});
            }
            state["channel"]["maxObjectBytes"] = json!(self.config.telegram_max_media_bytes);
        }
        let roots = string_array(state.get("rootNodeIds"));
        let roots = if roots.is_empty() {
            vec![
                runtime.user_root_node_id.clone(),
                runtime.kennedy_root_node_id.clone(),
            ]
        } else {
            roots
        };
        let mut options = SessionOptions::conversation(session_type.clone(), roots);
        options.reference_root_node_ids = string_array(state.get("referenceRootNodeIds"));
        options.channel = state.get("channel").cloned().unwrap_or(Value::Null);
        options.free_time = state.get("freeTime").cloned().unwrap_or(Value::Null);
        options.orchestration = state
            .get("orchestration")
            .cloned()
            .unwrap_or_else(|| json!({"owner":"backend","status":"idle"}));
        options.provenance_id = state
            .get("provenanceId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        options.mode = match session_type.as_str() {
            "free-time" => AgentMode::FreeTime,
            "wakeup" => AgentMode::Wakeup,
            _ => AgentMode::Conversation,
        };
        Session::new(
            self.api.clone(),
            runtime.manuals,
            runtime.model,
            options,
            Some(&state),
        )
        .await
    }

    async fn close_conversation(
        &self,
        record: &Arc<Mutex<Value>>,
        session: &Session,
    ) -> anyhow::Result<Value> {
        session.release_managed_sources().await;
        self.request_conversation_ingress(record, None).await
    }

    async fn request_conversation_ingress(
        &self,
        record: &Arc<Mutex<Value>>,
        state: Option<Value>,
    ) -> anyhow::Result<Value> {
        let mut locked = record.lock().await;
        let id = required_string(&locked, "id")?;
        let state =
            state.unwrap_or_else(|| locked.get("state").cloned().unwrap_or_else(|| json!({})));
        let response = self
            .api
            .history_request_ingress(
                &id,
                kcode_session_history::Checkpoint {
                    expected_version: version(&locked)?,
                    state,
                    user_activity: false,
                },
            )
            .await?;
        *locked = response.clone();
        Ok(response)
    }

    async fn complete_command(&self, id: &str, outcome: Value) -> anyhow::Result<()> {
        self.api.history_complete_command(id, outcome).await?;
        Ok(())
    }

    async fn get_conversation(&self, id: &str) -> anyhow::Result<Value> {
        Ok(self.api.history_get_session(id).await?)
    }

    async fn schedule_writer_job(self: &Arc<Self>, histories: &[Value]) -> anyhow::Result<()> {
        if self.writer_job_active.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(record) = histories
            .iter()
            .find(|record| {
                record.get("phase").and_then(Value::as_str) == Some("active")
                    && session_type(record) == "wakeup"
            })
            .cloned()
        {
            self.launch_writer_job("scheduled wakeup", move |worker| async move {
                let id = required_string(&record, "id")?;
                let record = worker.get_conversation(&id).await?;
                worker.process_wakeup(record).await
            })
            .await;
            return Ok(());
        }
        if let Some(record) = histories
            .iter()
            .find(|record| {
                record.get("phase").and_then(Value::as_str) == Some("active")
                    && session_type(record) == "free-time"
            })
            .cloned()
        {
            self.launch_writer_job("self time", move |worker| async move {
                let id = required_string(&record, "id")?;
                let record = worker.get_conversation(&id).await?;
                worker.process_self_time(record).await
            })
            .await;
            return Ok(());
        }
        if let Some(record) = next_ingress(histories, Utc::now()).cloned() {
            self.launch_writer_job("memory ingress", move |worker| async move {
                let id = required_string(&record, "id")?;
                let record = worker.get_conversation(&id).await?;
                worker.process_ingress(record).await
            })
            .await;
        }
        Ok(())
    }

    async fn launch_writer_job<F, Fut>(self: &Arc<Self>, label: &'static str, task: F)
    where
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if self
            .writer_job_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let worker = self.clone();
        tokio::spawn(async move {
            let _writer_guard = worker.writer.lock().await;
            if let Err(error) = task(worker.clone()).await {
                tracing::warn!(
                    %label,
                    error=%bounded_error(&error),
                    "Kmap writer job will retry"
                );
            }
            worker.writer_job_active.store(false, Ordering::Release);
        });
    }

    async fn process_ingress(&self, mut record: Value) -> anyhow::Result<()> {
        let id = required_string(&record, "id")?;
        let rust_session_id = format!("kennedy:history-ingress:{id}");
        let mut stage = "prepare";
        let result = async {
            if record.get("phase").and_then(Value::as_str) == Some("ingress_pending") {
                record
                    .get("state")
                    .and_then(|state| state.get("sessionId"))
                    .and_then(Value::as_str)
                    .context("The queued session has no Session History ID")?;
                stage = "claim";
                record = self
                    .api
                    .history_start_ingress(
                        &id,
                        kcode_session_history::StartIngress {
                            expected_version: version(&record)?,
                            provenance_id: format!("session:{id}"),
                        },
                    )
                    .await?;
            }
            if record.get("phase").and_then(Value::as_str) != Some("ingress_in_progress") {
                return Ok(());
            }
            stage = "model_loop";
            let runtime = self.runtime()?.clone();
            let state = record.get("state").cloned().unwrap_or_else(|| json!({}));
            let source_session_type = state
                .get("sessionType")
                .and_then(Value::as_str)
                .unwrap_or("conversation")
                .to_owned();
            let roots = {
                let roots = string_array(state.get("rootNodeIds"));
                if roots.is_empty() {
                    vec![
                        runtime.user_root_node_id.clone(),
                        runtime.kennedy_root_node_id.clone(),
                    ]
                } else {
                    roots
                }
            };
            let options = SessionOptions {
                session_type: "history-ingress".into(),
                root_node_ids: roots,
                reference_root_node_ids: string_array(state.get("referenceRootNodeIds")),
                channel: state.get("channel").cloned().unwrap_or(Value::Null),
                free_time: Value::Null,
                orchestration: Value::Null,
                provenance_id: None,
                mode: AgentMode::Ingress {
                    record_id: Some(id.clone()),
                },
                source_session_type: Some(source_session_type),
                group_context: state
                    .get("channel")
                    .and_then(|channel| channel.get("groupContext"))
                    .cloned()
                    .unwrap_or(Value::Null),
                rust_lib_session_id: Some(rust_session_id.clone()),
            };
            let restored = ingress_restore_state(&state);
            let mut session = Session::new(
                self.api.clone(),
                runtime.manuals,
                runtime.model,
                options,
                Some(restored),
            )
            .await?;
            let record = Arc::new(Mutex::new(record));
            persist_ingress_record(&self.api, &record, session.snapshot()?).await?;
            if !session.completed {
                session.pending_turn = true;
                let api = self.api.clone();
                let saved_record = record.clone();
                session
                    .run_pending_turn(Uuid::new_v4(), move |session_state| {
                        let api = api.clone();
                        let record = saved_record.clone();
                        async move {
                            persist_ingress_record(&api, &record, session_state).await?;
                            Ok(())
                        }
                    })
                    .await?;
            }
            persist_ingress_record(&self.api, &record, session.snapshot()?).await?;
            stage = "completion";
            let mut locked = record.lock().await;
            let completed = self
                .api
                .history_complete_ingress(&id, version(&locked)?)
                .await?;
            *locked = completed.clone();
            if let Some(batch) = completed
                .get("state")
                .and_then(|state| state.get("channel"))
                .and_then(|channel| channel.get("groupIngressBatchId"))
                .and_then(Value::as_str)
            {
                self.api
                    .telegram_post(
                        &format!("/api/v1/group-ingress/{}/complete", encode_path(batch)),
                        json!({}),
                    )
                    .await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            self.record_ingress_failure(&id, stage, &error).await.ok();
            return Err(error);
        }
        self.api.release_managed_sources(&rust_session_id).await;
        Ok(())
    }

    async fn record_ingress_failure(
        &self,
        id: &str,
        stage: &str,
        error: &anyhow::Error,
    ) -> anyhow::Result<()> {
        let latest = self.get_conversation(id).await?;
        if !matches!(
            latest.get("phase").and_then(Value::as_str),
            Some("ingress_pending" | "ingress_in_progress")
        ) {
            return Ok(());
        }
        self.api
            .history_fail_ingress(
                id,
                kcode_session_history::IngressFailure {
                    expected_version: version(&latest)?,
                    stage: stage.to_owned(),
                    code: Some("ingress_error".into()),
                    message: bounded_error(error),
                    rounds_used: None,
                    context_tokens: None,
                    context_window_tokens: None,
                },
            )
            .await?;
        Ok(())
    }

    async fn process_self_time(&self, record: Value) -> anyhow::Result<()> {
        let runtime = self.runtime()?.clone();
        let id = required_string(&record, "id")?;
        let mut state = record.get("state").cloned().unwrap_or_else(|| json!({}));
        if state.get("freeTime").is_none() {
            let intent = state
                .get("selfTimeIntent")
                .context("backend self-time record is missing its durable start intent")?;
            let duration = intent
                .get("durationMinutes")
                .and_then(Value::as_f64)
                .context("self-time duration is missing")?;
            let requested = intent
                .get("requestedAt")
                .and_then(Value::as_str)
                .or_else(|| record.get("started_at").and_then(Value::as_str))
                .context("self-time request time is missing")?;
            let requested_at = DateTime::parse_from_rfc3339(requested)?.with_timezone(&Utc);
            let deadline =
                requested_at + ChronoDuration::milliseconds((duration * 60_000.0).round() as i64);
            state["freeTime"] = json!({"runId":id,"runStartedAt":requested_at.to_rfc3339(),"deadlineAt":deadline.to_rfc3339(),"durationMinutes":duration,"customPrompt":intent.get("customPrompt").and_then(Value::as_str).unwrap_or(""),"sliceIndex":1});
            state["orchestration"] = json!({"owner":"backend","status":"running"});
        }
        let mut options = SessionOptions::conversation(
            "free-time",
            vec![
                runtime.user_root_node_id.clone(),
                runtime.kennedy_root_node_id.clone(),
            ],
        );
        options.mode = AgentMode::FreeTime;
        options.free_time = state.get("freeTime").cloned().unwrap_or(Value::Null);
        options.provenance_id = state
            .get("provenanceId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        options.orchestration = json!({"owner":"backend","status":"running"});
        let mut session = Session::new(
            self.api.clone(),
            runtime.manuals.clone(),
            runtime.model.clone(),
            options,
            Some(&state),
        )
        .await?;
        session.stage_free_time_opening();
        let record_arc = Arc::new(Mutex::new(record));
        persist_record(&self.api, &record_arc, session.snapshot()?, true).await?;
        let deadline = session
            .free_time
            .get("deadlineAt")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .context("self-time deadline is invalid")?;
        let timeout = (deadline - Utc::now() + ChronoDuration::minutes(2))
            .to_std()
            .unwrap_or(Duration::ZERO);
        let operation = Uuid::new_v4();
        let api = self.api.clone();
        let saved = record_arc.clone();
        let result = tokio::time::timeout(
            timeout,
            session.run_pending_turn(operation, move |state| {
                let api = api.clone();
                let record = saved.clone();
                async move {
                    persist_record(&api, &record, state, false).await?;
                    Ok(())
                }
            }),
        )
        .await;
        let reason = match result {
            Ok(Ok(_)) => session
                .free_time
                .get("sliceEndedReason")
                .and_then(Value::as_str)
                .unwrap_or_else(|| {
                    if Utc::now() >= deadline {
                        "deadline"
                    } else {
                        "tool"
                    }
                })
                .to_owned(),
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                let _ = self.api.cancel_intelligence(operation);
                "hard-stop".into()
            }
        };
        session.finalize_free_time(&reason)?;
        session.commit_current_write_session()?;
        persist_record(&self.api, &record_arc, session.snapshot()?, false).await?;
        session.release_managed_sources().await;
        let mut locked = record_arc.lock().await;
        let completed = self
            .api
            .history_complete(
                &id,
                kcode_session_history::Checkpoint {
                    expected_version: version(&locked)?,
                    state: locked.get("state").cloned().unwrap_or(Value::Null),
                    user_activity: false,
                },
            )
            .await?;
        *locked = completed;
        if deadline - Utc::now() >= ChronoDuration::minutes(5) {
            self.create_next_self_time_slice(
                &runtime,
                session.free_time.clone(),
                session.provenance_id.clone(),
                deadline,
            )
            .await?;
        }
        Ok(())
    }

    async fn process_wakeup(&self, record: Value) -> anyhow::Result<()> {
        let id = required_string(&record, "id")?;
        let mut session = self.session_for_record(&record).await?;
        session.stage_wakeup_opening()?;
        let record = Arc::new(Mutex::new(record));
        persist_record(&self.api, &record, session.snapshot()?, false).await?;
        let operation = Uuid::new_v4();
        let api = self.api.clone();
        let saved = record.clone();
        session
            .run_pending_turn(operation, move |state| {
                let api = api.clone();
                let record = saved.clone();
                async move {
                    persist_record(&api, &record, state, false).await?;
                    Ok(())
                }
            })
            .await?;
        session.commit_current_write_session()?;
        persist_record(&self.api, &record, session.snapshot()?, false).await?;
        session.release_managed_sources().await;
        let mut locked = record.lock().await;
        let completed = self
            .api
            .history_complete(
                &id,
                kcode_session_history::Checkpoint {
                    expected_version: version(&locked)?,
                    state: locked.get("state").cloned().unwrap_or(Value::Null),
                    user_activity: false,
                },
            )
            .await?;
        *locked = completed;
        Ok(())
    }

    async fn create_next_self_time_slice(
        &self,
        runtime: &Runtime,
        mut free: Value,
        provenance_id: Option<String>,
        deadline: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        free["sliceIndex"] = json!(
            free.get("sliceIndex")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                + 1
        );
        if let Some(object) = free.as_object_mut() {
            object.remove("sliceEndedReason");
            object.remove("sliceEndedAt");
            object.remove("warningNoticeAt");
            object.remove("expiredNoticeAt");
        }
        if let Some(message) = free.get("nextSessionMessage").cloned() {
            free["handoffMessage"] = message;
        }
        if let Some(object) = free.as_object_mut() {
            object.remove("nextSessionMessage");
        }
        free["deadlineAt"] = json!(deadline.to_rfc3339());
        let mut options = SessionOptions::conversation(
            "free-time",
            vec![
                runtime.user_root_node_id.clone(),
                runtime.kennedy_root_node_id.clone(),
            ],
        );
        options.mode = AgentMode::FreeTime;
        options.free_time = free;
        options.provenance_id = provenance_id;
        options.orchestration = json!({"owner":"backend","status":"running"});
        let mut session = Session::new(
            self.api.clone(),
            runtime.manuals.clone(),
            runtime.model.clone(),
            options,
            None,
        )
        .await?;
        session.stage_free_time_opening();
        let state = session.snapshot()?;
        self.api
            .history_register(kcode_session_history::RegisterSession {
                id: required_string(&state, "sessionId")?,
                started_at: session.started_at.clone(),
                state,
            })
            .await?;
        Ok(())
    }

    async fn sync_directory_provisioning(self: &Arc<Self>) -> anyhow::Result<()> {
        let worker = self.clone();
        let mut set = self.directory_in_flight.lock().await;
        if !set.insert("directory".into()) {
            return Ok(());
        }
        drop(set);
        tokio::spawn(async move {
            if let Err(error) = worker.provision_directory().await {
                tracing::warn!(error=%error,"Telegram directory provisioning will retry");
            }
            worker.directory_in_flight.lock().await.remove("directory");
        });
        Ok(())
    }

    async fn provision_directory(&self) -> anyhow::Result<()> {
        let runtime = self.runtime()?;
        let (users, groups) = tokio::try_join!(
            self.api.directory_provisioning_users(),
            self.api.directory_provisioning_groups()
        )?;
        for user in users {
            let handle = user.handle;
            let is_web = handle
                .trim_start_matches('@')
                .eq_ignore_ascii_case(self.config.telegram_web_user_handle.trim_start_matches('@'));
            let root = if is_web {
                runtime.user_root_node_id.clone()
            } else {
                let _guard = self.writer.lock().await;
                self.api.bootstrap_node(None)?.id.to_string()
            };
            self.api
                .directory_complete_handle_root(
                    &handle,
                    root.parse()
                        .context("created an invalid user root node ID")?,
                )
                .await?;
        }
        for group in groups {
            let group_id = group.group_id;
            let _guard = self.writer.lock().await;
            let root = self.api.bootstrap_node(Some("Group Root"))?.id.to_string();
            self.api
                .directory_complete_group_root(
                    &group_id,
                    root.parse()
                        .context("created an invalid group root node ID")?,
                )
                .await?;
        }
        Ok(())
    }

    async fn directory_user(&self, event: &Value) -> anyhow::Result<kcode_telegram_identity::User> {
        let id = event
            .get("telegramUserId")
            .map(value_string)
            .context("Telegram event omitted user ID")?
            .parse::<i64>()
            .context("Telegram event has an invalid user ID")?;
        let mut user = self.api.directory_user(id).await?;
        if !user.root_ready {
            let _guard = self.writer.lock().await;
            let root = self.api.bootstrap_node(None)?.id.to_string();
            user = self
                .api
                .directory_complete_user_root(
                    id,
                    root.parse()
                        .context("created an invalid user root node ID")?,
                )
                .await?;
        }
        Ok(user)
    }

    async fn directory_group(
        &self,
        group_id: &str,
    ) -> anyhow::Result<kcode_telegram_identity::Group> {
        let mut group = self.api.directory_group(group_id).await?;
        if !group.root_ready {
            let _guard = self.writer.lock().await;
            let root = self.api.bootstrap_node(Some("Group Root"))?.id.to_string();
            group = self
                .api
                .directory_complete_group_root(
                    group_id,
                    root.parse()
                        .context("created an invalid group root node ID")?,
                )
                .await?;
        }
        Ok(group)
    }

    async fn decorate_group_context(
        &self,
        mut context: Value,
        group_id: &str,
    ) -> anyhow::Result<Value> {
        let group = self.directory_group(group_id).await?;
        context["groupId"] = json!(group_id);
        context["groupRootNodeId"] = json!(group.root_node_id);
        context["groupRootReady"] = json!(group.root_ready);
        let mut participants = Vec::new();
        for participant in context
            .get("participants")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let user = self.directory_user(&participant).await?;
            let mut participant = participant;
            participant["rootNodeId"] = json!(user.root_node_id);
            participant["rootReady"] = json!(user.root_ready);
            participants.push(participant);
        }
        context["participants"] = json!(participants);
        Ok(context)
    }

    async fn prepare_group_context(
        &self,
        mut context: Value,
        excluded_message_id: Option<&str>,
        group_id: &str,
    ) -> anyhow::Result<Value> {
        let chat_id = context.get("chatId").map(value_string).unwrap_or_default();
        let mut messages = Vec::new();
        for mut message in context
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let message_id = message
                .get("messageId")
                .map(value_string)
                .unwrap_or_default();
            let excluded = excluded_message_id == Some(message_id.as_str());
            let kind = message
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("text")
                .to_owned();
            if !excluded
                && message.get("sentByKennedy").and_then(Value::as_bool) != Some(true)
                && kind == "document"
                && message
                    .get("preparedText")
                    .and_then(Value::as_str)
                    .is_none()
                && message.get("hasMedia").and_then(Value::as_bool) == Some(true)
            {
                let prepared = async {
                    let (bytes, mime) = self
                        .api
                        .telegram_bytes(&format!(
                            "/api/v1/group-messages/{}/{}/media",
                            encode_path(&chat_id),
                            encode_path(&message_id)
                        ))
                        .await?;
                    let result = self
                        .api
                        .extract_document(
                            bytes,
                            message
                                .get("fileName")
                                .and_then(Value::as_str)
                                .unwrap_or("telegram-document")
                                .to_owned(),
                            &mime,
                        )
                        .await?;
                    Ok::<_, anyhow::Error>((
                        required_string(&result, "text")?,
                        None::<String>,
                        result
                            .get("format")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        result
                            .get("truncated")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    ))
                }
                .await;
                let (text, model, format, truncated) = match prepared {
                    Ok(value) => value,
                    Err(error) => (
                        format!("Document extraction failed: {error}"),
                        Some("preparation-error".into()),
                        None,
                        false,
                    ),
                };
                message["preparedText"] = json!(text);
                message["preparationModel"] = json!(model);
                message["documentFormat"] = json!(format);
                message["preparationTruncated"] = json!(truncated);
                let _ = self
                    .api
                    .telegram_post(
                        &format!(
                            "/api/v1/group-messages/{}/{}/preparation",
                            encode_path(&chat_id),
                            encode_path(&message_id)
                        ),
                        json!({"text":text,"model":model,"format":format,"truncated":truncated}),
                    )
                    .await;
            }
            if !excluded
                && matches!(
                    kind.as_str(),
                    "voice"
                        | "document"
                        | "photo"
                        | "video"
                        | "animation"
                        | "audio"
                        | "video_note"
                        | "sticker"
                )
            {
                let has_media = message.get("hasMedia").and_then(Value::as_bool) == Some(true);
                if has_media {
                    let media_path = format!(
                        "/api/v1/group-messages/{}/{}/media",
                        encode_path(&chat_id),
                        encode_path(&message_id)
                    );
                    let (size_bytes, downloaded_mime_type) =
                        self.api.telegram_file_metadata(&media_path).await?;
                    let mime_type = normalized_file_mime_type(
                        message
                            .get("mimeType")
                            .and_then(Value::as_str)
                            .unwrap_or(&downloaded_mime_type),
                    );
                    let supplied_file_name = message
                        .get("fileName")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty());
                    let file_name_supplied = supplied_file_name.is_some();
                    let file_name = telegram_group_context_file_name(
                        supplied_file_name,
                        &kind,
                        &mime_type,
                        &message_id,
                    );
                    message["fileName"] = json!(file_name);
                    message["fileNameSource"] = json!(if file_name_supplied {
                        "transport"
                    } else {
                        "synthesized"
                    });
                    message["mimeType"] = json!(mime_type);
                    message["sizeBytes"] = json!(size_bytes);
                    message["mediaRef"] = json!({"kind":kind,"source":"telegram-group","chatId":context.get("chatId").cloned().unwrap_or(Value::Null),"messageId":message.get("messageId").cloned().unwrap_or(Value::Null),"fileName":file_name,"fileNameSource":message.get("fileNameSource").cloned().unwrap_or(Value::Null),"mimeType":mime_type,"sizeBytes":size_bytes,"durationSeconds":message.get("durationSeconds").cloned().unwrap_or(Value::Null)});
                }
                let base = message.get("text").and_then(Value::as_str).unwrap_or("");
                let prepared = message
                    .get("preparedText")
                    .and_then(Value::as_str)
                    .unwrap_or("Document text extraction unavailable.");
                message["text"] = json!(if has_media {
                    let file_metadata = telegram_group_context_file_metadata(&message);
                    if kind == "voice" {
                        format!(
                            "{base}\n\n{file_metadata}\nThe voice note was not automatically transcribed."
                        )
                    } else if kind == "document" {
                        format!("{base}\n\n{file_metadata}\n\n{prepared}")
                    } else {
                        format!("{base}\n\n{file_metadata}")
                    }
                } else {
                    format!("{base}\n\n[The Telegram {kind} file is unavailable.]")
                });
            }
            messages.push(message);
        }
        context["messages"] = json!(messages);
        self.decorate_group_context(context, group_id).await
    }

    async fn sync_telegram_events(self: &Arc<Self>) -> anyhow::Result<()> {
        let events = self
            .api
            .telegram_get("/api/v1/events")
            .await?
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let listed_ids = events
            .iter()
            .filter_map(|event| event.get("id").and_then(Value::as_str))
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        self.event_retries
            .lock()
            .await
            .retain(|id, _| listed_ids.contains(id));
        for event in events {
            let id = required_string(&event, "id")?;
            if self
                .event_retries
                .lock()
                .await
                .get(&id)
                .is_some_and(|retry| Instant::now() < retry.not_before)
            {
                continue;
            }
            let mut set = self.events_in_flight.lock().await;
            if !set.insert(id.clone()) {
                continue;
            }
            drop(set);
            let worker = self.clone();
            tokio::spawn(async move {
                worker.run_telegram_event(event).await;
                worker.events_in_flight.lock().await.remove(&id);
            });
        }
        Ok(())
    }

    async fn run_telegram_event(&self, event: Value) {
        let id = event
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let operation = Uuid::new_v4();
        self.active_operations
            .lock()
            .await
            .insert(format!("telegram:{id}"), operation);
        let conversation_id = Arc::new(Mutex::new(
            event
                .get("conversationId")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ));
        let result = tokio::time::timeout(
            telegram_timeout(&event),
            self.process_telegram_event(&event, operation, conversation_id.clone()),
        )
        .await;
        self.active_operations
            .lock()
            .await
            .remove(&format!("telegram:{id}"));
        match result {
            Ok(Ok(())) => {
                self.event_retries.lock().await.remove(&id);
            }
            Ok(Err(error)) => {
                let message = bounded_error(&error);
                let (attempt, delay, should_warn) =
                    self.record_telegram_event_retry(&id, &message).await;
                if should_warn {
                    tracing::warn!(
                        event_id=%id,
                        attempt,
                        retry_in_seconds=delay.as_secs(),
                        error=%message,
                        "Telegram event will retry"
                    );
                } else {
                    tracing::debug!(
                        event_id=%id,
                        attempt,
                        retry_in_seconds=delay.as_secs(),
                        error=%message,
                        "Telegram event retry remains unsuccessful"
                    );
                }
            }
            Err(_) => {
                self.event_retries.lock().await.remove(&id);
                let _ = self.api.cancel_intelligence(operation);
                let conversation = conversation_id.lock().await.clone();
                if let Some(conversation_id) = &conversation
                    && let Err(error) = self
                        .transition_timed_out_telegram_to_ingress(conversation_id)
                        .await
                {
                    tracing::error!(event_id=%id,error=%error,"Timed-out Telegram conversation could not be queued for ingress");
                }
                let _ = self
                    .api
                    .telegram_post(
                        &format!("/api/v1/events/{}/abort", encode_path(&id)),
                        json!({"conversationId":conversation,"message":TELEGRAM_TIMEOUT_NOTICE}),
                    )
                    .await;
                tracing::error!(event_id=%id,"Telegram event reached its 30-minute deadline and was aborted");
            }
        }
    }

    async fn record_telegram_event_retry(&self, id: &str, error: &str) -> (u32, Duration, bool) {
        let mut retries = self.event_retries.lock().await;
        let failures = retries
            .get(id)
            .map_or(1, |retry| retry.failures.saturating_add(1));
        let delay = telegram_event_retry_delay(failures);
        let should_warn = telegram_event_retry_should_warn(
            retries.get(id).map(|retry| retry.last_error.as_str()),
            error,
            failures,
        );
        retries.insert(
            id.to_owned(),
            TelegramEventRetry {
                failures,
                not_before: Instant::now() + delay,
                last_error: error.to_owned(),
            },
        );
        (failures, delay, should_warn)
    }

    async fn process_telegram_event(
        &self,
        event: &Value,
        operation: Uuid,
        bound_conversation_id: Arc<Mutex<Option<String>>>,
    ) -> anyhow::Result<()> {
        let id = required_string(event, "id")?;
        let _private_user_guard =
            if event.get("sessionKind").and_then(Value::as_str) != Some("group") {
                let telegram_user_id = event
                    .get("telegramUserId")
                    .and_then(Value::as_i64)
                    .context("private Telegram event is missing its numeric user identity")?;
                Some(
                    self.api
                        .telegram_user_lock(telegram_user_id)
                        .await
                        .lock_owned()
                        .await,
                )
            } else {
                None
            };
        self.directory_user(event).await?;
        if event.get("kind").and_then(Value::as_str) == Some("reset") {
            return self.process_telegram_reset(event).await;
        }
        let (record_arc, _) = self.telegram_session(event).await?;
        let conversation_id = {
            let locked = record_arc.lock().await;
            required_string(&locked, "id")?
        };
        *bound_conversation_id.lock().await = Some(conversation_id.clone());
        let lock = self.conversation_lock(&conversation_id).await;
        let _guard = lock.lock().await;
        let mut session = {
            let record = record_arc.lock().await;
            self.session_for_record(&record).await?
        };
        if session.answer_for_external_event(&id).is_none() {
            if session.pending_turn && session.pending_external_event_id.as_deref() != Some(&id) {
                anyhow::bail!("This Telegram session has an earlier saved query to finish.");
            }
            if !session.pending_turn {
                let input = self.telegram_input(event).await;
                let (text, metadata) = match input {
                    Ok(input) => input,
                    Err(error) if event.get("kind").and_then(Value::as_str) == Some("document") => {
                        let filename = event
                            .get("fileName")
                            .and_then(Value::as_str)
                            .unwrap_or("that document");
                        self.api.telegram_post(&format!("/api/v1/events/{}/reply",encode_path(&id)),json!({"conversationId":conversation_id,"text":format!("I couldn't read {filename}: {error} Please try sending it again."),"contextWarning":Value::Null})).await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                session.begin_user_turn(&text, &metadata);
                persist_record(&self.api, &record_arc, session.snapshot()?, true).await?;
            }
            let api = self.api.clone();
            let saved = record_arc.clone();
            session
                .run_pending_turn(operation, move |state| {
                    let api = api.clone();
                    let record = saved.clone();
                    async move {
                        persist_record(&api, &record, state, false).await?;
                        Ok(())
                    }
                })
                .await?;
            persist_record(&self.api, &record_arc, session.snapshot()?, false).await?;
            if session.requires_history_ingress() {
                session.orchestration =
                    json!({"owner":"backend","status":"ending","reason":"context-limit"});
                persist_record(&self.api, &record_arc, session.snapshot()?, false).await?;
                self.request_conversation_ingress(&record_arc, None).await?;
                self.deliver_telegram_responses(
                    &mut session,
                    &id,
                    &conversation_id,
                    Some("session ended after crossing the 75% forced-ingress threshold"),
                )
                .await?;
                return Ok(());
            }
        }
        self.deliver_telegram_responses(&mut session, &id, &conversation_id, None)
            .await
    }

    async fn deliver_telegram_responses(
        &self,
        session: &mut Session,
        event_id: &str,
        conversation_id: &str,
        forced_context_warning: Option<&str>,
    ) -> anyhow::Result<()> {
        enum Delivery {
            Object(String),
            Text(String, Value),
        }

        let mut deliveries = Vec::new();
        for response in session.responses_for_external_event(event_id) {
            if let Some(objects) = response.get("objects").and_then(Value::as_array) {
                for object_id in objects.iter().filter_map(Value::as_str) {
                    deliveries.push(Delivery::Object(object_id.to_owned()));
                }
            }
            if let Some(text) = response
                .get("content")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                deliveries.push(Delivery::Text(
                    text.to_owned(),
                    response
                        .get("contextWarning")
                        .cloned()
                        .unwrap_or(Value::Null),
                ));
            }
        }
        if let Some(warning) = forced_context_warning {
            let has_text = deliveries
                .iter()
                .any(|delivery| matches!(delivery, Delivery::Text(_, _)));
            if !has_text {
                deliveries.push(Delivery::Text(
                    "This session reached its context limit and has been sent to history ingress."
                        .into(),
                    json!(warning),
                ));
            }
        }
        anyhow::ensure!(
            !deliveries.is_empty(),
            "Kennedy completed the turn without a recoverable Telegram response"
        );
        let delivery_count = deliveries.len();
        for (index, delivery) in deliveries.into_iter().enumerate() {
            let complete = index + 1 == delivery_count;
            match delivery {
                Delivery::Object(object_id) => {
                    let file = session.resolve_object(&object_id)?;
                    anyhow::ensure!(
                        file.bytes.len() <= self.config.telegram_max_media_bytes,
                        "object {object_id} is {} bytes, over the configured {}-byte Telegram media limit",
                        file.bytes.len(),
                        self.config.telegram_max_media_bytes
                    );
                    self.api
                        .telegram_send_object(
                            &encode_path(event_id),
                            conversation_id,
                            &file,
                            complete,
                        )
                        .await?;
                }
                Delivery::Text(text, response_warning) => {
                    let warning = forced_context_warning
                        .map(|warning| json!(warning))
                        .unwrap_or(response_warning);
                    self.api
                        .telegram_post(
                            &format!("/api/v1/events/{}/reply", encode_path(event_id)),
                            json!({
                                "conversationId":conversation_id,
                                "text":text,
                                "contextWarning":warning,
                            }),
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn transition_timed_out_telegram_to_ingress(
        &self,
        conversation_id: &str,
    ) -> anyhow::Result<()> {
        let record = match self.get_conversation(conversation_id).await {
            Ok(record) => record,
            Err(error)
                if error
                    .downcast_ref::<super::ApiError>()
                    .is_some_and(|error| error.code == "not_found") =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if record.get("phase").and_then(Value::as_str) != Some("active") {
            return Ok(());
        }
        let mut state = record.get("state").cloned().unwrap_or_else(|| json!({}));
        state["orchestration"] =
            json!({"owner":"backend","status":"stopped","reason":"telegram-timeout"});
        self.api
            .history_request_ingress(
                conversation_id,
                kcode_session_history::Checkpoint {
                    expected_version: version(&record)?,
                    state: state.clone(),
                    user_activity: false,
                },
            )
            .await?;
        if let Some(session_id) = state.get("rustLibSessionId").and_then(Value::as_str) {
            self.api.release_managed_sources(session_id).await;
        }
        Ok(())
    }

    async fn telegram_session(
        &self,
        event: &Value,
    ) -> anyhow::Result<(Arc<Mutex<Value>>, Session)> {
        let histories = self.list_history().await?;
        let group = event.get("sessionKind").and_then(Value::as_str) == Some("group");
        let user_id = event
            .get("telegramUserId")
            .map(value_string)
            .unwrap_or_default();
        let group_id = event.get("groupId").and_then(Value::as_str);
        let mut record = event
            .get("conversationId")
            .and_then(Value::as_str)
            .and_then(|id| {
                histories
                    .iter()
                    .find(|record| record.get("id").and_then(Value::as_str) == Some(id))
                    .cloned()
            });
        if record.is_none() {
            record = histories.into_iter().find(|record| {
                record.get("phase").and_then(Value::as_str) == Some("active")
                    && if group {
                        session_type(record) == "telegram-group"
                            && record_group_id(record) == group_id
                            && record_user_id(record) == user_id
                    } else {
                        session_type(record) == "telegram" && record_user_id(record) == user_id
                    }
            });
        }
        let created = record
            .as_ref()
            .is_none_or(|record| record.get("phase").and_then(Value::as_str) != Some("active"));
        let (record, mut session) = if let Some(record) =
            record.filter(|record| record.get("phase").and_then(Value::as_str) == Some("active"))
        {
            let record = self
                .get_conversation(&required_string(&record, "id")?)
                .await?;
            let session = self.session_for_record(&record).await?;
            (record, session)
        } else {
            self.create_telegram_session(event).await?
        };
        let record = Arc::new(Mutex::new(record));
        let id = {
            let locked = record.lock().await;
            required_string(&locked, "id")?
        };
        if event.get("conversationId").and_then(Value::as_str) != Some(&id)
            || event.get("processingStartedAt").is_none()
        {
            self.api.telegram_post(&format!("/api/v1/events/{}/bind",encode_path(required_string(event,"id")?)),json!({"conversationId":id,"expectedConversationId":event.get("conversationId").cloned().unwrap_or(Value::Null)})).await?;
        }
        if group && !created {
            let group_id = required_string(event, "groupId")?;
            if let Some(context) = event.get("groupContext") {
                let context = self
                    .prepare_group_context(
                        context.clone(),
                        event.get("messageId").map(value_string).as_deref(),
                        &group_id,
                    )
                    .await?;
                session.refresh_telegram_group_context(
                    &context,
                    event.get("messageId").map(value_string).as_deref(),
                )?;
                persist_record(&self.api, &record, session.snapshot()?, false).await?;
            }
        }
        Ok((record, session))
    }

    async fn create_telegram_session(&self, event: &Value) -> anyhow::Result<(Value, Session)> {
        let runtime = self.runtime()?.clone();
        let user = self.directory_user(event).await?;
        let group = event.get("sessionKind").and_then(Value::as_str) == Some("group");
        let mut roots = vec![
            user.root_node_id
                .context("Telegram user root is not ready")?,
        ];
        let mut channel = json!({"kind":if group{"telegram-group"}else{"telegram"},"telegramUserId":event.get("telegramUserId").cloned().unwrap_or(Value::Null),"chatId":event.get("chatId").cloned().unwrap_or(Value::Null),"groupId":event.get("groupId").cloned().unwrap_or(Value::Null),"username":event.get("username").cloned().unwrap_or(Value::Null),"displayName":event.get("displayName").cloned().unwrap_or(Value::Null),"maxObjectBytes":self.config.telegram_max_media_bytes});
        let mut references = Vec::new();
        if group {
            let group_id = required_string(event, "groupId")?;
            let group_record = self.directory_group(&group_id).await?;
            let group_root = group_record
                .root_node_id
                .context("Telegram group root is not ready")?;
            roots.push(group_root.clone());
            if let Some(context) = event.get("groupContext") {
                let context = self
                    .prepare_group_context(
                        context.clone(),
                        event.get("messageId").map(value_string).as_deref(),
                        &group_id,
                    )
                    .await?;
                channel["groupContext"] = context.clone();
                channel["groupRootNodeId"] = json!(group_root);
                references = participant_references(&context, &roots);
            }
        }
        roots.push(runtime.kennedy_root_node_id.clone());
        references.retain(|id| !roots.contains(id));
        let mut options =
            SessionOptions::conversation(if group { "telegram-group" } else { "telegram" }, roots);
        options.channel = channel;
        options.reference_root_node_ids = references;
        let session = Session::new(
            self.api.clone(),
            runtime.manuals,
            runtime.model,
            options,
            None,
        )
        .await?;
        let state = session.snapshot()?;
        let record = self
            .api
            .history_register(kcode_session_history::RegisterSession {
                id: required_string(&state, "sessionId")?,
                started_at: session.started_at.clone(),
                state,
            })
            .await?;
        Ok((record, session))
    }

    async fn telegram_input(&self, event: &Value) -> anyhow::Result<(String, Value)> {
        let id = required_string(event, "id")?;
        match event.get("kind").and_then(Value::as_str).unwrap_or("text") {
            "voice" => {
                let (bytes, mime) = self
                    .api
                    .telegram_bytes(&format!("/api/v1/events/{}/media", encode_path(&id)))
                    .await?;
                let filename = event
                    .get("fileName")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("telegram-voice.ogg");
                Ok((
                    event
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                    json!({"externalEventId":id,"inputKind":"voice","media":{"id":format!("telegram:{id}"),"kind":"voice","source":"telegram","mimeType":mime,"fileName":filename,"dataUrl":data_url(&mime,&bytes),"sizeBytes":bytes.len(),"durationSeconds":event.get("durationSeconds").cloned().unwrap_or(Value::Null)}}),
                ))
            }
            "document" => {
                let (bytes, mime) = self
                    .api
                    .telegram_bytes(&format!("/api/v1/events/{}/media", encode_path(&id)))
                    .await?;
                let filename = event
                    .get("fileName")
                    .and_then(Value::as_str)
                    .unwrap_or("telegram-document")
                    .to_owned();
                let extraction = self
                    .api
                    .extract_document(bytes.clone(), filename.clone(), &mime)
                    .await;
                let mut attachment = json!({
                    "id":format!("telegram:{id}"),
                    "kind":"document",
                    "source":"telegram",
                    "fileName":filename,
                    "mimeType":mime,
                    "sizeBytes":bytes.len(),
                    "dataUrl":data_url(&mime,&bytes),
                });
                match extraction {
                    Ok(result) => {
                        attachment["format"] = result.get("format").cloned().unwrap_or(Value::Null);
                        attachment["text"] = result.get("text").cloned().unwrap_or(Value::Null);
                        attachment["characters"] =
                            result.get("characters").cloned().unwrap_or(Value::Null);
                        attachment["truncated"] =
                            result.get("truncated").cloned().unwrap_or(json!(false));
                    }
                    Err(error) => {
                        attachment["extractionError"] = json!(error.to_string());
                    }
                }
                Ok((
                    event
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                    json!({"externalEventId":id,"inputKind":"document","attachments":[attachment]}),
                ))
            }
            kind @ ("photo" | "video" | "animation" | "audio" | "video_note" | "sticker") => {
                let (bytes, downloaded_mime) = self
                    .api
                    .telegram_bytes(&format!("/api/v1/events/{}/media", encode_path(&id)))
                    .await?;
                let mime = event
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(&downloaded_mime)
                    .to_owned();
                let extension = match kind {
                    "photo" => "jpg",
                    "video" | "video_note" => "mp4",
                    "animation" => "gif",
                    "audio" => "mp3",
                    "sticker" => "webp",
                    _ => "bin",
                };
                let filename = event
                    .get("fileName")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("telegram-{kind}.{extension}"));
                let mut attachment = json!({
                    "id":format!("telegram:{id}"),
                    "kind":kind,
                    "source":"telegram",
                    "fileName":filename,
                    "mimeType":mime,
                    "sizeBytes":bytes.len(),
                    "dataUrl":data_url(&mime,&bytes),
                });
                if let Some(value) = event.get("durationSeconds") {
                    attachment["durationSeconds"] = value.clone();
                }
                Ok((
                    event
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                    json!({
                        "externalEventId":id,
                        "inputKind":kind,
                        "attachments":[attachment],
                    }),
                ))
            }
            _ => Ok((
                event
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .into(),
                json!({"externalEventId":id,"inputKind":"text"}),
            )),
        }
    }

    async fn process_telegram_reset(&self, event: &Value) -> anyhow::Result<()> {
        let id = required_string(event, "id")?;
        let Some(conversation_id) = event.get("conversationId").and_then(Value::as_str) else {
            self.api.telegram_post(&format!("/api/v1/events/{}/reset-completed",encode_path(&id)),json!({"message":"There is no active Telegram session to reset. Your next message will begin one."})).await?;
            return Ok(());
        };
        let record = match self.get_conversation(conversation_id).await {
            Ok(record) => record,
            Err(error)
                if error
                    .downcast_ref::<super::ApiError>()
                    .is_some_and(|error| error.code == "not_found") =>
            {
                self.api.telegram_post(&format!("/api/v1/events/{}/reset-completed",encode_path(&id)),json!({"message":"There is no active Telegram session to reset. Your next message will begin one."})).await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if record.get("phase").and_then(Value::as_str) != Some("active") {
            self.api.telegram_post(&format!("/api/v1/events/{}/reset-completed",encode_path(&id)),json!({"message":"There is no active Telegram session to reset. Your next message will begin one."})).await?;
            return Ok(());
        }
        let session = self.session_for_record(&record).await?;
        session.release_managed_sources().await;
        self.api
            .history_request_ingress(
                conversation_id,
                kcode_session_history::Checkpoint {
                    expected_version: version(&record)?,
                    state: record.get("state").cloned().unwrap_or(Value::Null),
                    user_activity: false,
                },
            )
            .await?;
        self.api.telegram_post(&format!("/api/v1/events/{}/reset-completed",encode_path(&id)),json!({"message":"Conversation reset. The Telegram session has been queued for memory ingress; your next message will begin a new session."})).await?;
        Ok(())
    }

    async fn sync_group_updates(self: &Arc<Self>) -> anyhow::Result<()> {
        let updates = self
            .api
            .telegram_get("/api/v1/group-sessions/updates")
            .await?
            .get("updates")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for update in updates {
            let id = required_string(&update, "conversationId")?;
            let mut set = self.group_updates_in_flight.lock().await;
            if !set.insert(id.clone()) {
                continue;
            }
            drop(set);
            let worker = self.clone();
            tokio::spawn(async move {
                if let Err(error) = worker.process_group_update(update).await {
                    tracing::warn!(conversation_id=%id,error=%error,"Telegram group context update will retry");
                }
                worker.group_updates_in_flight.lock().await.remove(&id);
            });
        }
        Ok(())
    }

    async fn process_group_update(&self, update: Value) -> anyhow::Result<()> {
        let id = required_string(&update, "conversationId")?;
        let lock = self.conversation_lock(&id).await;
        let _guard = lock.lock().await;
        let record = match self.get_conversation(&id).await {
            Ok(record) => record,
            Err(error)
                if error
                    .downcast_ref::<super::ApiError>()
                    .is_some_and(|error| error.code == "not_found") =>
            {
                self.reconcile_missing_group_session(&update, &id).await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if record.get("phase").and_then(Value::as_str) != Some("active") {
            if update.get("resetRequired").and_then(Value::as_bool) == Some(true) {
                self.api
                    .telegram_post(
                        &format!(
                            "/api/v1/group-sessions/{}/silent-reset-completed",
                            encode_path(&id)
                        ),
                        json!({}),
                    )
                    .await?;
            }
            return Ok(());
        }
        let mut session = self.session_for_record(&record).await?;
        if session.pending_turn {
            return Ok(());
        }
        let group_id = required_string(&update, "groupId")?;
        let context = self
            .prepare_group_context(
                update.get("groupContext").cloned().unwrap_or(Value::Null),
                None,
                &group_id,
            )
            .await?;
        session.refresh_telegram_group_context(&context, None)?;
        let record = Arc::new(Mutex::new(record));
        persist_record(&self.api, &record, session.snapshot()?, false).await?;
        if update.get("resetRequired").and_then(Value::as_bool) == Some(true) {
            self.close_conversation(&record, &session).await?;
            self.api
                .telegram_post(
                    &format!(
                        "/api/v1/group-sessions/{}/silent-reset-completed",
                        encode_path(&id)
                    ),
                    json!({}),
                )
                .await?;
        } else {
            self.api.telegram_post(&format!("/api/v1/group-sessions/{}/context-ack",encode_path(&id)),json!({"throughMessageId":update.get("throughMessageId").cloned().unwrap_or(json!(0))})).await?;
        }
        Ok(())
    }

    async fn reconcile_missing_group_session(
        &self,
        update: &Value,
        conversation_id: &str,
    ) -> anyhow::Result<()> {
        match missing_group_session_recovery(update)? {
            MissingGroupSessionRecovery::CompleteSilentReset => {
                self.api
                    .telegram_post(
                        &format!(
                            "/api/v1/group-sessions/{}/silent-reset-completed",
                            encode_path(conversation_id)
                        ),
                        json!({}),
                    )
                    .await?;
                tracing::info!(
                    %conversation_id,
                    "Completed orphaned Telegram group reset"
                );
            }
            MissingGroupSessionRecovery::DetachCurrent {
                group_id,
                telegram_user_id,
            } => {
                let result = self
                    .api
                    .telegram_post(
                        &format!(
                            "/api/v1/group-sessions/{}/detach-if-current",
                            encode_path(conversation_id)
                        ),
                        json!({
                            "groupId":group_id,
                            "telegramUserId":telegram_user_id,
                        }),
                    )
                    .await;
                match result {
                    Ok(_) => {
                        tracing::info!(
                            %conversation_id,
                            %group_id,
                            telegram_user_id,
                            "Detached orphaned Telegram group session"
                        );
                    }
                    Err(error) if error.code == "state_conflict" => {
                        tracing::info!(
                            %conversation_id,
                            %group_id,
                            telegram_user_id,
                            "Telegram group session was already detached or rebound"
                        );
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(())
    }

    async fn sync_group_ingress(self: &Arc<Self>) -> anyhow::Result<()> {
        let batches = self
            .api
            .telegram_get("/api/v1/group-ingress")
            .await?
            .get("batches")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for batch in batches {
            let id = required_string(&batch, "id")?;
            let mut set = self.group_ingress_in_flight.lock().await;
            if !set.insert(id.clone()) {
                continue;
            }
            drop(set);
            let worker = self.clone();
            tokio::spawn(async move {
                if let Err(error) = worker.process_group_ingress(batch).await {
                    tracing::warn!(batch_id=%id,error=%error,"Telegram group ingress preparation will retry");
                }
                worker.group_ingress_in_flight.lock().await.remove(&id);
            });
        }
        Ok(())
    }

    async fn process_group_ingress(&self, batch: Value) -> anyhow::Result<()> {
        let runtime = self.runtime()?.clone();
        let id = required_string(&batch, "id")?;
        if let Some(existing) = self.list_history().await?.into_iter().find(|record| {
            record
                .get("state")
                .and_then(|state| state.get("channel"))
                .and_then(|channel| channel.get("groupIngressBatchId"))
                .and_then(Value::as_str)
                == Some(&id)
        }) {
            match existing.get("phase").and_then(Value::as_str) {
                Some("complete") => {
                    self.api
                        .telegram_post(
                            &format!("/api/v1/group-ingress/{}/complete", encode_path(&id)),
                            json!({}),
                        )
                        .await?;
                }
                Some("active") => {
                    let existing = self
                        .get_conversation(&required_string(&existing, "id")?)
                        .await?;
                    self.api
                        .history_request_ingress(
                            &required_string(&existing, "id")?,
                            kcode_session_history::Checkpoint {
                                expected_version: version(&existing)?,
                                state: existing.get("state").cloned().unwrap_or(Value::Null),
                                user_activity: false,
                            },
                        )
                        .await?;
                }
                _ => {}
            }
            return Ok(());
        }
        let group_id = required_string(&batch, "groupId")?;
        let group = self.directory_group(&group_id).await?;
        let raw_context = json!({"groupTitle":batch.get("groupTitle").cloned().unwrap_or(json!("Telegram group")),"chatId":batch.get("chatId").cloned().unwrap_or(Value::Null),"participants":batch.get("participants").cloned().unwrap_or(json!([])),"messages":batch.get("messages").cloned().unwrap_or(json!([]))});
        let context = self
            .prepare_group_context(raw_context, None, &group_id)
            .await?;
        let group_root = group
            .root_node_id
            .context("Telegram group root is not ready")?;
        let roots = vec![group_root.clone(), runtime.kennedy_root_node_id];
        let channel = json!({"kind":"telegram-group","chatId":batch.get("chatId").cloned().unwrap_or(Value::Null),"groupId":group_id,"groupRootNodeId":group_root,"groupIngressBatchId":id,"backgroundIngress":true,"groupContext":context});
        let mut options = SessionOptions::conversation("telegram-group", roots.clone());
        options.channel = channel;
        options.reference_root_node_ids = participant_references(&context, &roots);
        options.source_session_type = Some("telegram-group".into());
        let mut session = Session::new(
            self.api.clone(),
            runtime.manuals,
            runtime.model,
            options,
            None,
        )
        .await?;
        for message in context
            .get("messages")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            session.stage_source_message(
                message.get("sentByKennedy").and_then(Value::as_bool) == Some(true),
                message
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                message.clone(),
            )?;
        }
        let state = session.snapshot()?;
        let record = self
            .api
            .history_register(kcode_session_history::RegisterSession {
                id: required_string(&state, "sessionId")?,
                started_at: batch
                    .get("createdAt")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| Utc::now().to_rfc3339()),
                state,
            })
            .await?;
        self.api
            .history_request_ingress(
                &required_string(&record, "id")?,
                kcode_session_history::Checkpoint {
                    expected_version: version(&record)?,
                    state: record.get("state").cloned().unwrap_or(Value::Null),
                    user_activity: false,
                },
            )
            .await?;
        Ok(())
    }
}

async fn persist_record(
    api: &Api,
    record: &Arc<Mutex<Value>>,
    state: Value,
    user_activity: bool,
) -> anyhow::Result<()> {
    let mut record = record.lock().await;
    let id = required_string(&record, "id")?;
    let result = match api
        .history_checkpoint(
            &id,
            kcode_session_history::Checkpoint {
                expected_version: version(&record)?,
                state: state.clone(),
                user_activity,
            },
            false,
        )
        .await
    {
        Ok(result) => result,
        Err(error) if error.code == "state_conflict" => {
            let latest = api.history_get_session(&id).await?;
            if latest.get("state") == Some(&state) {
                latest
            } else {
                return Err(error.into());
            }
        }
        Err(error) => return Err(error.into()),
    };
    *record = result;
    Ok(())
}
async fn persist_ingress_record(
    api: &Api,
    record: &Arc<Mutex<Value>>,
    archive: Value,
) -> anyhow::Result<()> {
    let mut record = record.lock().await;
    let id = required_string(&record, "id")?;
    let mut state = record.get("state").cloned().unwrap_or_else(|| json!({}));
    state["historyIngress"] = archive;
    let result = match api
        .history_checkpoint(
            &id,
            kcode_session_history::Checkpoint {
                expected_version: version(&record)?,
                state: state.clone(),
                user_activity: false,
            },
            true,
        )
        .await
    {
        Ok(result) => result,
        Err(error) if error.code == "state_conflict" => {
            let latest = api.history_get_session(&id).await?;
            if latest.get("state") == Some(&state) {
                latest
            } else {
                return Err(error.into());
            }
        }
        Err(error) => return Err(error.into()),
    };
    *record = result;
    Ok(())
}
fn session_type(record: &Value) -> String {
    record
        .get("state")
        .and_then(|state| state.get("sessionType"))
        .and_then(Value::as_str)
        .unwrap_or("conversation")
        .into()
}

fn next_wakeup_marker(now: DateTime<Utc>) -> DateTime<Utc> {
    let day_start = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always a valid UTC time")
        .and_utc();
    for hour in [0_i64, 4, 8, 12, 16, 20] {
        let candidate = day_start + ChronoDuration::hours(hour);
        if candidate > now {
            return candidate;
        }
    }
    day_start + ChronoDuration::days(1)
}

fn next_ingress(histories: &[Value], now: DateTime<Utc>) -> Option<&Value> {
    histories
        .iter()
        .filter(|record| match record.get("phase").and_then(Value::as_str) {
            Some("ingress_in_progress") => true,
            Some("ingress_pending") => record
                .get("ingress_next_attempt_at")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .is_none_or(|next| next.with_timezone(&Utc) <= now),
            _ => false,
        })
        .min_by(|left, right| ingress_record_order(left, right))
}

fn ingress_record_order(left: &Value, right: &Value) -> std::cmp::Ordering {
    let rank = |record: &Value| {
        if record.get("phase").and_then(Value::as_str) == Some("ingress_in_progress") {
            0
        } else {
            1
        }
    };
    rank(left)
        .cmp(&rank(right))
        .then_with(|| ingress_record_time(left).cmp(&ingress_record_time(right)))
        .then_with(|| {
            left.get("id")
                .and_then(Value::as_str)
                .cmp(&right.get("id").and_then(Value::as_str))
        })
}

fn ingress_record_time(record: &Value) -> DateTime<Utc> {
    ["updated_at", "source_created_at", "started_at"]
        .into_iter()
        .find_map(|field| {
            record
                .get(field)
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
        })
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

fn is_browser_conversation(record: &Value) -> bool {
    session_type(record) == "conversation"
}
fn record_channel(record: &Value) -> Option<&Value> {
    record.get("state").and_then(|state| state.get("channel"))
}
fn record_group_id(record: &Value) -> Option<&str> {
    record_channel(record)
        .and_then(|channel| {
            channel.get("groupId").or_else(|| {
                channel
                    .get("groupContext")
                    .and_then(|context| context.get("groupId"))
            })
        })
        .and_then(Value::as_str)
}
fn record_user_id(record: &Value) -> String {
    record_channel(record)
        .and_then(|channel| channel.get("telegramUserId"))
        .map(value_string)
        .unwrap_or_default()
}
fn participant_references(context: &Value, roots: &[String]) -> Vec<String> {
    let mut values = context
        .get("participants")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|participant| participant.get("rootNodeId").and_then(Value::as_str))
        .filter(|id| !roots.iter().any(|root| root == id))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}
fn normalized_file_mime_type(value: &str) -> String {
    let value = value
        .split(';')
        .next()
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase();
    if value.contains('/')
        && !value.is_empty()
        && !value.chars().any(char::is_whitespace)
        && !value.chars().any(char::is_control)
    {
        value
    } else {
        "application/octet-stream".into()
    }
}
fn file_extension_for_mime_type(mime_type: &str) -> &'static str {
    match normalized_file_mime_type(mime_type).as_str() {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "audio/ogg" | "audio/opus" | "application/ogg" => "ogg",
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/mp4" | "video/mp4" => "mp4",
        "audio/webm" | "video/webm" => "webm",
        "audio/wav" | "audio/x-wav" => "wav",
        "application/pdf" => "pdf",
        _ => "bin",
    }
}
fn telegram_group_context_file_name(
    supplied: Option<&str>,
    kind: &str,
    mime_type: &str,
    message_id: &str,
) -> String {
    let fallback = format!(
        "telegram-group-{kind}-{message_id}.{}",
        file_extension_for_mime_type(mime_type)
    );
    sanitize_file_name(supplied.unwrap_or_default(), &fallback)
}
fn file_name_extension(file_name: &str) -> String {
    file_name
        .rsplit_once('.')
        .and_then(|(stem, extension)| {
            (!stem.is_empty() && !extension.is_empty()).then_some(extension)
        })
        .map(|extension| format!(".{extension}"))
        .unwrap_or_else(|| "(none)".into())
}
fn telegram_group_context_file_metadata(message: &Value) -> String {
    let file_name = message
        .get("fileName")
        .and_then(Value::as_str)
        .unwrap_or("telegram-file");
    let source_note = (message.get("fileNameSource").and_then(Value::as_str)
        == Some("synthesized"))
    .then_some(" (synthesized because Telegram supplied no filename)")
    .unwrap_or_default();
    let mime_type = normalized_file_mime_type(
        message
            .get("mimeType")
            .and_then(Value::as_str)
            .unwrap_or("application/octet-stream"),
    );
    let size_bytes = message
        .get("sizeBytes")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    format!(
        "User-provided file\nOriginal filename: {file_name}{source_note}\nExtension: {}\nMIME type: {mime_type}\nSize: {size_bytes} bytes",
        file_name_extension(file_name),
    )
}
fn required_string(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("backend response omitted {key}"))
}
fn missing_group_session_recovery(update: &Value) -> anyhow::Result<MissingGroupSessionRecovery> {
    if update.get("resetRequired").and_then(Value::as_bool) == Some(true) {
        return Ok(MissingGroupSessionRecovery::CompleteSilentReset);
    }
    Ok(MissingGroupSessionRecovery::DetachCurrent {
        group_id: required_string(update, "groupId")?,
        telegram_user_id: update
            .get("telegramUserId")
            .and_then(Value::as_i64)
            .context("backend response omitted telegramUserId")?,
    })
}
fn version(value: &Value) -> anyhow::Result<i64> {
    value
        .get("version")
        .and_then(Value::as_i64)
        .context("backend record omitted version")
}
fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}
fn ingress_restore_state(state: &Value) -> &Value {
    state.get("historyIngress").unwrap_or(state)
}
fn value_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}
fn bounded_error(error: &anyhow::Error) -> String {
    format!("{error:#}").chars().take(1_000).collect()
}
fn telegram_event_retry_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(5);
    Duration::from_secs((2_u64 << exponent).min(60))
}
fn telegram_event_retry_should_warn(
    previous_error: Option<&str>,
    error: &str,
    failures: u32,
) -> bool {
    previous_error != Some(error) || failures.is_multiple_of(10)
}
fn is_cancelled(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<super::ApiError>()
        .is_some_and(|error| error.code == "operation_cancelled")
}
fn telegram_timeout(event: &Value) -> Duration {
    let elapsed = event
        .get("processingStartedAt")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| {
            (Utc::now() - value.with_timezone(&Utc))
                .to_std()
                .unwrap_or_default()
        })
        .unwrap_or_default();
    TELEGRAM_TIMEOUT.saturating_sub(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wakeup_markers_are_strictly_future_four_hour_utc_boundaries() {
        let before = DateTime::parse_from_rfc3339("2026-07-28T03:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            next_wakeup_marker(before).to_rfc3339(),
            "2026-07-28T04:00:00+00:00"
        );
        let exactly = DateTime::parse_from_rfc3339("2026-07-28T20:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            next_wakeup_marker(exactly).to_rfc3339(),
            "2026-07-29T00:00:00+00:00"
        );
    }

    #[test]
    fn browser_and_telegram_sessions_are_classified_from_current_control_state() {
        let browser = json!({
            "phase":"active",
            "state":{
                "sessionType":"conversation",
                "orchestration":{"owner":"backend","status":"idle"}
            }
        });
        assert!(is_browser_conversation(&browser));
        assert!(!is_browser_conversation(&json!({
            "phase":"active",
            "state":{"sessionType":"telegram"}
        })));
    }

    #[test]
    fn history_ingress_restart_prefers_its_own_checkpoint() {
        let source = json!({
            "sessionType":"conversation",
            "kwebPlan":{"creates":[]},
            "historyIngress":{
                "sessionType":"history-ingress",
                "completed":true,
                "commitReceipt":{"sessionObjectId":"A1234567"},
                "kwebPlan":{"creates":[{"pendingId":"pending:1"}]}
            }
        });
        let restored = ingress_restore_state(&source);
        assert_eq!(restored["sessionType"], "history-ingress");
        assert_eq!(restored["completed"], true);
        assert_eq!(restored["commitReceipt"]["sessionObjectId"], "A1234567");
        assert_eq!(restored["kwebPlan"]["creates"].as_array().unwrap().len(), 1);
        assert_eq!(
            ingress_restore_state(&json!({"sessionType":"conversation"}))["sessionType"],
            "conversation"
        );
    }

    #[test]
    fn missing_group_sessions_choose_the_transport_recovery_for_the_update_kind() {
        assert_eq!(
            missing_group_session_recovery(&json!({
                "groupId":"group-1",
                "telegramUserId":42,
                "resetRequired":false,
            }))
            .unwrap(),
            MissingGroupSessionRecovery::DetachCurrent {
                group_id: "group-1".into(),
                telegram_user_id: 42,
            }
        );
        assert_eq!(
            missing_group_session_recovery(&json!({
                "resetRequired":true,
            }))
            .unwrap(),
            MissingGroupSessionRecovery::CompleteSilentReset
        );
    }

    #[test]
    fn ingress_scheduler_uses_due_oldest_work_not_newest_updates() {
        let now = DateTime::parse_from_rfc3339("2026-07-25T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let histories = vec![
            json!({
                "id":"newest-not-due",
                "phase":"ingress_pending",
                "updated_at":"2026-07-25T02:59:00Z",
                "ingress_next_attempt_at":"2026-07-25T03:01:00Z"
            }),
            json!({
                "id":"newer-due",
                "phase":"ingress_pending",
                "updated_at":"2026-07-25T02:30:00Z",
                "ingress_next_attempt_at":null
            }),
            json!({
                "id":"oldest-due",
                "phase":"ingress_pending",
                "updated_at":"2026-07-25T01:30:00Z",
                "ingress_next_attempt_at":"2026-07-25T02:00:00Z"
            }),
        ];
        assert_eq!(
            next_ingress(&histories, now)
                .and_then(|record| record.get("id"))
                .and_then(Value::as_str),
            Some("oldest-due")
        );
    }

    #[test]
    fn bounded_errors_include_the_cause_chain() {
        let error = anyhow::anyhow!("inner cause").context("outer context");
        assert_eq!(bounded_error(&error), "outer context: inner cause");
    }

    #[test]
    fn retained_group_files_use_the_same_complete_metadata_contract() {
        let file_name = telegram_group_context_file_name(None, "voice", "audio/ogg", "77");
        assert_eq!(file_name, "telegram-group-voice-77.ogg");
        let rendered = telegram_group_context_file_metadata(&json!({
            "fileName":file_name,
            "fileNameSource":"synthesized",
            "mimeType":"audio/ogg; codecs=opus",
            "sizeBytes":42,
        }));
        assert!(rendered.contains(
            "Original filename: telegram-group-voice-77.ogg (synthesized because Telegram supplied no filename)"
        ));
        assert!(rendered.contains("Extension: .ogg"));
        assert!(rendered.contains("MIME type: audio/ogg"));
        assert!(rendered.contains("Size: 42 bytes"));
    }

    #[test]
    fn telegram_event_retries_back_off_to_one_per_minute() {
        assert_eq!(telegram_event_retry_delay(1), Duration::from_secs(2));
        assert_eq!(telegram_event_retry_delay(2), Duration::from_secs(4));
        assert_eq!(telegram_event_retry_delay(3), Duration::from_secs(8));
        assert_eq!(telegram_event_retry_delay(6), Duration::from_secs(60));
        assert_eq!(
            telegram_event_retry_delay(u32::MAX),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn identical_telegram_retry_warnings_are_periodic() {
        assert!(telegram_event_retry_should_warn(None, "failure", 1));
        assert!(!telegram_event_retry_should_warn(
            Some("failure"),
            "failure",
            2
        ));
        assert!(telegram_event_retry_should_warn(
            Some("old failure"),
            "new failure",
            2
        ));
        assert!(telegram_event_retry_should_warn(
            Some("failure"),
            "failure",
            10
        ));
    }

    #[test]
    fn ingress_scheduler_resumes_claimed_work_before_pending_work() {
        let pending = json!({
            "id":"pending",
            "phase":"ingress_pending",
            "updated_at":"2026-07-25T01:00:00Z"
        });
        let claimed = json!({
            "id":"claimed",
            "phase":"ingress_in_progress",
            "updated_at":"2026-07-25T02:00:00Z",
            "source_created_at":"2026-07-25T00:00:00Z"
        });
        let now = DateTime::parse_from_rfc3339("2026-07-25T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let histories = vec![pending, claimed];
        assert_eq!(
            next_ingress(&histories, now)
                .and_then(|record| record.get("id"))
                .and_then(Value::as_str),
            Some("claimed")
        );
    }

    #[tokio::test]
    async fn writer_mutex_serializes_tasks_while_other_tasks_remain_independent() {
        let writer = Arc::new(Mutex::new(()));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let maximum = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let writer = writer.clone();
            let active = active.clone();
            let maximum = maximum.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = writer.lock().await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let first = {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
            })
        };
        let second = {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
            })
        };
        barrier.wait().await;
        first.await.unwrap();
        second.await.unwrap();
    }
}
