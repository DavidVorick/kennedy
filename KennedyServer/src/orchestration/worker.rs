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
use kcode_session_history::{SessionCommand, SessionRecord, SessionStopRequest};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, OnceCell, RwLock};
use uuid::Uuid;

use super::{
    AgentMode, Api, Config, Manuals, RuntimeModel, Session,
    services::{data_url, telegram_caption_for},
    session::{
        ResolvedObject, SessionOptions, is_agent_loop_round_limit, validate_delivery_file_name,
    },
};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const STARTUP_RETRY: Duration = Duration::from_secs(2);
const TELEGRAM_TIMEOUT: Duration = Duration::from_secs(90 * 60);
const TELEGRAM_SESSION_MAX_AGE: ChronoDuration = ChronoDuration::hours(6);
const TELEGRAM_TIMEOUT_NOTICE: &str = "Kennedy could not complete a response within 90 minutes, so this request was stopped. Please send it again if you want to retry it.";

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

enum TelegramDelivery {
    Object {
        object_id: String,
        file_name: Option<String>,
    },
    Text {
        text: String,
        response_warning: Value,
        captionable: bool,
    },
}

#[derive(Clone)]
struct ActiveOperation {
    operation_id: Uuid,
    stopped: Arc<AtomicBool>,
    notification: Arc<Notify>,
}

impl ActiveOperation {
    fn new(operation_id: Uuid) -> Self {
        Self {
            operation_id,
            stopped: Arc::new(AtomicBool::new(false)),
            notification: Arc::new(Notify::new()),
        }
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.notification.notify_waiters();
    }

    async fn stopped(&self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        self.notification.notified().await;
    }
}

enum TurnCompletion {
    Finished,
    Stopped,
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
    active_operations: Mutex<HashMap<String, ActiveOperation>>,
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

    pub(crate) async fn request_stop(&self, id: &str) -> anyhow::Result<Value> {
        let record = self.get_conversation(id).await?;
        let scope = stop_scope(&record);
        let request = self
            .api
            .history_request_stop(
                id,
                kcode_session_history::NewStopRequest {
                    idempotency_id: Uuid::new_v4().to_string(),
                    scope: scope.into(),
                },
            )
            .await?;
        let signaled = self.signal_stop(id).await;
        if scope == "turn" && !signaled {
            let has_command = self
                .api
                .history_command_heads()
                .await?
                .iter()
                .any(|command| command.conversation_id == id);
            if !has_command {
                self.finish_idle_turn_stop(id).await?;
            }
        }
        Ok(json!({
            "id":id,
            "scope":scope,
            "status":"stopping",
            "stopRequested":true,
            "stopRequestId":request.id,
        }))
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
        self.api.history_health()?;
        self.api.telegram_health();
        let manuals = Manuals::load(&self.config.system_prompts_directory)?;
        let runtime = Runtime {
            manuals,
            model: self.config.runtime_model.clone(),
            user_root_node_id: self.api.user_root_node_id().to_owned(),
            kennedy_root_node_id: self.api.kennedy_root_node_id().to_owned(),
        };
        self.api.history_release_interrupted_ingress().await?;
        self.queue_detached_private_telegram_sessions().await?;
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
        let private_sessions = self.api.telegram_private_sessions().await?;
        for private_session in private_sessions {
            let telegram_user_id = private_session.telegram_user_id;
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
        let user = self.api.directory_user(telegram_user_id)?;
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
        let histories = self.list_history().await?;
        self.signal_pending_stops().await?;
        self.queue_expired_telegram_sessions(&histories, Utc::now())
            .await?;
        self.sync_conversation_commands().await?;
        self.sync_directory_provisioning().await?;
        self.sync_group_updates().await?;
        self.sync_group_ingress().await?;
        self.sync_telegram_events().await?;
        self.schedule_writer_job(&histories).await?;
        self.api.synchronize_audio_ingress().await?;
        Ok(())
    }

    async fn signal_pending_stops(&self) -> anyhow::Result<()> {
        let command_conversations = self
            .api
            .history_command_heads()
            .await?
            .into_iter()
            .map(|command| command.conversation_id)
            .collect::<HashSet<_>>();
        for request in self.pending_stops().await? {
            let signaled = self.signal_stop(&request.session_id).await;
            if !signaled
                && request.scope == "turn"
                && !command_conversations.contains(&request.session_id)
            {
                self.finish_idle_turn_stop(&request.session_id).await?;
            }
        }
        Ok(())
    }

    async fn pending_stops(&self) -> anyhow::Result<Vec<SessionStopRequest>> {
        Ok(self.api.history_stop_heads().await?)
    }

    async fn pending_stop(&self, session_id: &str) -> anyhow::Result<Option<SessionStopRequest>> {
        Ok(self
            .pending_stops()
            .await?
            .into_iter()
            .find(|request| request.session_id == session_id))
    }

    async fn complete_pending_stop(&self, session_id: &str, outcome: Value) -> anyhow::Result<()> {
        if let Some(request) = self.pending_stop(session_id).await? {
            self.api.history_complete_stop(&request.id, outcome).await?;
        }
        Ok(())
    }

    async fn signal_stop(&self, session_id: &str) -> bool {
        let active = self.active_operations.lock().await.get(session_id).cloned();
        if let Some(active) = active {
            let _ = self.api.cancel_intelligence(active.operation_id);
            active.stop();
            true
        } else {
            false
        }
    }

    async fn finish_idle_turn_stop(&self, session_id: &str) -> anyhow::Result<()> {
        let lock = self.conversation_lock(session_id).await;
        let _guard = lock.lock().await;
        if self.signal_stop(session_id).await {
            return Ok(());
        }
        if self.pending_stop(session_id).await?.is_none() {
            return Ok(());
        }
        let record = self.get_conversation(session_id).await?;
        if record.phase != "active" {
            return Ok(());
        }
        let record = Arc::new(Mutex::new(record));
        let mut session = {
            let locked = record.lock().await;
            self.session_for_record(&locked).await?
        };
        let telegram_event = matches!(session.session_type.as_str(), "telegram" | "telegram-group")
            .then(|| session.pending_external_event_id.clone())
            .flatten();
        session.interrupt_current_turn()?;
        persist_record(&self.api, &record, session.snapshot()?, false).await?;
        if let Some(event_id) = telegram_event {
            self.api
                .telegram_interrupt_event(&event_id, session_id)
                .await?;
        }
        self.complete_pending_stop(session_id, json!({"status":"stopped","scope":"turn"}))
            .await
    }

    async fn register_operation(
        &self,
        session_id: &str,
        active: ActiveOperation,
    ) -> anyhow::Result<()> {
        self.active_operations
            .lock()
            .await
            .insert(session_id.to_owned(), active.clone());
        if self.pending_stop(session_id).await?.is_some() {
            let _ = self.api.cancel_intelligence(active.operation_id);
            active.stop();
        }
        Ok(())
    }

    async fn remove_operation(&self, session_id: &str, operation_id: Uuid) {
        let mut active = self.active_operations.lock().await;
        if active
            .get(session_id)
            .is_some_and(|operation| operation.operation_id == operation_id)
        {
            active.remove(session_id);
        }
    }

    async fn run_session_turn<C, F>(
        &self,
        session_id: &str,
        session: &mut Session,
        active: ActiveOperation,
        checkpoint: C,
    ) -> anyhow::Result<TurnCompletion>
    where
        C: FnMut(Value) -> F,
        F: std::future::Future<Output = anyhow::Result<()>>,
    {
        self.register_operation(session_id, active.clone()).await?;
        let result: anyhow::Result<TurnCompletion> = {
            let turn = session.run_pending_turn(active.operation_id, checkpoint);
            tokio::pin!(turn);
            tokio::select! {
                biased;
                _ = active.stopped() => Ok({
                    let _ = self.api.cancel_intelligence(active.operation_id);
                    TurnCompletion::Stopped
                }),
                result = &mut turn => result.map(|_| TurnCompletion::Finished),
            }
        };
        self.remove_operation(session_id, active.operation_id).await;
        result
    }

    async fn list_history(&self) -> anyhow::Result<Vec<SessionRecord>> {
        Ok(self.api.history_list().await?)
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
        let commands = self.api.history_command_heads().await?;
        for command in commands {
            let id = command.id.clone();
            let conversation_id = command.conversation_id.clone();
            if command.cancel_requested && self.commands_in_flight.lock().await.contains(&id) {
                self.signal_stop(&conversation_id).await;
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

    async fn process_conversation_command(&self, command: SessionCommand) -> anyhow::Result<()> {
        let command_id = command.id.clone();
        let conversation_id = command.conversation_id.clone();
        let lock = self.conversation_lock(&conversation_id).await;
        let _conversation_guard = lock.lock().await;
        let command = if command.status == "pending" {
            self.api.history_claim_command(&command_id).await?
        } else {
            command
        };
        let record = self.get_conversation(&conversation_id).await?;
        if record.phase != "active" || !is_browser_conversation(&record) {
            self.complete_command(&command_id, json!({"status":"conversation_closed"}))
                .await?;
            return Ok(());
        }
        let kind = command.kind.clone();
        let payload = command.payload.clone();
        let record = Arc::new(Mutex::new(record));
        if kind == "end" {
            let mut state = record.lock().await.state.clone();
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
        if command.cancel_requested {
            session.interrupt_current_turn()?;
            persist_record(&self.api, &record, session.snapshot()?, false).await?;
            self.complete_command(
                &command_id,
                json!({"status":"stopped","reason":"user_stopped"}),
            )
            .await?;
            self.complete_pending_stop(
                &conversation_id,
                json!({"status":"stopped","scope":"turn"}),
            )
            .await?;
            return Ok(());
        }
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
                    let active = ActiveOperation::new(Uuid::new_v4());
                    let api = self.api.clone();
                    let saved_record = record.clone();
                    let result = self
                        .run_session_turn(&conversation_id, &mut session, active, move |state| {
                            let api = api.clone();
                            let record = saved_record.clone();
                            async move {
                                persist_record(&api, &record, state, false).await?;
                                Ok(())
                            }
                        })
                        .await;
                    if matches!(&result, Ok(TurnCompletion::Stopped)) {
                        session.interrupt_current_turn()?;
                        persist_record(&self.api, &record, session.snapshot()?, false).await?;
                        self.complete_command(
                            &command_id,
                            json!({"status":"stopped","reason":"user_stopped"}),
                        )
                        .await?;
                        self.complete_pending_stop(
                            &conversation_id,
                            json!({"status":"stopped","scope":"turn"}),
                        )
                        .await?;
                        return Ok(());
                    }
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
                    let active = ActiveOperation::new(Uuid::new_v4());
                    let api = self.api.clone();
                    let saved_record = record.clone();
                    let result = self
                        .run_session_turn(&conversation_id, &mut session, active, move |state| {
                            let api = api.clone();
                            let record = saved_record.clone();
                            async move {
                                persist_record(&api, &record, state, false).await?;
                                Ok(())
                            }
                        })
                        .await;
                    if matches!(&result, Ok(TurnCompletion::Stopped)) {
                        session.interrupt_current_turn()?;
                        persist_record(&self.api, &record, session.snapshot()?, false).await?;
                        self.complete_command(
                            &command_id,
                            json!({"status":"stopped","reason":"user_stopped"}),
                        )
                        .await?;
                        self.complete_pending_stop(
                            &conversation_id,
                            json!({"status":"stopped","scope":"turn"}),
                        )
                        .await?;
                        return Ok(());
                    }
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
        self.complete_pending_stop(
            &conversation_id,
            json!({"status":"already-completed","scope":"turn"}),
        )
        .await?;
        Ok(())
    }

    async fn session_for_record(&self, record: &SessionRecord) -> anyhow::Result<Session> {
        let runtime = self.runtime()?.clone();
        let mut state = record.state.clone();
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
        record: &Arc<Mutex<SessionRecord>>,
        session: &Session,
    ) -> anyhow::Result<()> {
        session.release_managed_sources().await;
        self.request_conversation_ingress(record, None).await
    }

    async fn request_conversation_ingress(
        &self,
        record: &Arc<Mutex<SessionRecord>>,
        state: Option<Value>,
    ) -> anyhow::Result<()> {
        let mut locked = record.lock().await;
        let id = locked.id.clone();
        let state = state.unwrap_or_else(|| locked.state.clone());
        let response = self
            .api
            .history_request_ingress(
                &id,
                kcode_session_history::Checkpoint {
                    expected_version: locked.version,
                    state,
                    user_activity: false,
                },
            )
            .await?;
        *locked = response;
        Ok(())
    }

    async fn complete_command(&self, id: &str, outcome: Value) -> anyhow::Result<()> {
        self.api.history_complete_command(id, outcome).await?;
        Ok(())
    }

    async fn get_conversation(&self, id: &str) -> anyhow::Result<SessionRecord> {
        Ok(self.api.history_get_session(id).await?)
    }

    async fn schedule_writer_job(
        self: &Arc<Self>,
        histories: &[SessionRecord],
    ) -> anyhow::Result<()> {
        if self.writer_job_active.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(record) = histories
            .iter()
            .find(|record| record.phase == "active" && session_type(record) == "wakeup")
            .cloned()
        {
            self.launch_writer_job("scheduled wakeup", move |worker| async move {
                let id = record.id;
                let record = worker.get_conversation(&id).await?;
                worker.process_wakeup(record).await
            })
            .await;
            return Ok(());
        }
        if let Some(record) = histories
            .iter()
            .find(|record| record.phase == "active" && session_type(record) == "free-time")
            .cloned()
        {
            self.launch_writer_job("self time", move |worker| async move {
                let id = record.id;
                let record = worker.get_conversation(&id).await?;
                worker.process_self_time(record).await
            })
            .await;
            return Ok(());
        }
        if let Some(record) = next_ingress(histories, Utc::now()).cloned() {
            self.launch_writer_job("memory ingress", move |worker| async move {
                let id = record.id;
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

    async fn process_ingress(&self, mut record: SessionRecord) -> anyhow::Result<()> {
        let id = record.id.clone();
        let rust_session_id = format!("kennedy:history-ingress:{id}");
        let mut stage = "prepare";
        let result = async {
            if record.phase == "ingress_pending" {
                record
                    .state
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .context("The queued session has no Session History ID")?;
                stage = "claim";
                record = self
                    .api
                    .history_start_ingress(
                        &id,
                        kcode_session_history::StartIngress {
                            expected_version: record.version,
                            provenance_id: format!("session:{id}"),
                        },
                    )
                    .await?;
            }
            if record.phase != "ingress_in_progress" {
                return Ok(());
            }
            stage = "model_loop";
            let runtime = self.runtime()?.clone();
            let state = record.state.clone();
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
                let completion = self
                    .run_session_turn(
                        &id,
                        &mut session,
                        ActiveOperation::new(Uuid::new_v4()),
                        move |session_state| {
                            let api = api.clone();
                            let record = saved_record.clone();
                            async move {
                                persist_ingress_record(&api, &record, session_state).await?;
                                Ok(())
                            }
                        },
                    )
                    .await?;
                if matches!(completion, TurnCompletion::Stopped) {
                    session.interrupt_current_turn()?;
                    session.commit_current_write_session()?;
                    stage = "stop-completion";
                }
            }
            persist_ingress_record(&self.api, &record, session.snapshot()?).await?;
            stage = "completion";
            let mut locked = record.lock().await;
            let completed = self
                .api
                .history_complete_ingress(&id, locked.version)
                .await?;
            *locked = completed.clone();
            if let Some(batch) = completed
                .state
                .get("channel")
                .and_then(|channel| channel.get("groupIngressBatchId"))
                .and_then(Value::as_str)
            {
                self.api.telegram_complete_group_ingress(batch).await?;
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
            latest.phase.as_str(),
            "ingress_pending" | "ingress_in_progress"
        ) {
            return Ok(());
        }
        self.api
            .history_fail_ingress(
                id,
                kcode_session_history::IngressFailure {
                    expected_version: latest.version,
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

    async fn process_self_time(&self, record: SessionRecord) -> anyhow::Result<()> {
        let runtime = self.runtime()?.clone();
        let id = record.id.clone();
        let mut state = record.state.clone();
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
                .or(Some(record.started_at.as_str()))
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
        let timeout = (deadline - Utc::now() + ChronoDuration::minutes(6))
            .to_std()
            .unwrap_or(Duration::ZERO);
        let active = ActiveOperation::new(Uuid::new_v4());
        let api = self.api.clone();
        let saved = record_arc.clone();
        let result = tokio::time::timeout(
            timeout,
            self.run_session_turn(&id, &mut session, active.clone(), move |state| {
                let api = api.clone();
                let record = saved.clone();
                async move {
                    persist_record(&api, &record, state, false).await?;
                    Ok(())
                }
            }),
        )
        .await;
        let mut reason = match result {
            Ok(Ok(TurnCompletion::Stopped)) => "user-stop".into(),
            Ok(Ok(TurnCompletion::Finished)) => session
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
                let _ = self.api.cancel_intelligence(active.operation_id);
                active.stop();
                self.remove_operation(&id, active.operation_id).await;
                "hard-stop".into()
            }
        };
        if reason != "user-stop" && self.pending_stop(&id).await?.is_some() {
            reason = "user-stop".into();
        }
        if reason == "user-stop" {
            session.interrupt_current_turn()?;
        }
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
                    expected_version: locked.version,
                    state: locked.state.clone(),
                    user_activity: false,
                },
            )
            .await?;
        *locked = completed;
        if reason != "user-stop" && deadline - Utc::now() >= ChronoDuration::minutes(5) {
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

    async fn process_wakeup(&self, record: SessionRecord) -> anyhow::Result<()> {
        let id = record.id.clone();
        let mut session = self.session_for_record(&record).await?;
        session.stage_wakeup_opening()?;
        let record = Arc::new(Mutex::new(record));
        persist_record(&self.api, &record, session.snapshot()?, false).await?;
        let api = self.api.clone();
        let saved = record.clone();
        let completion = self
            .run_session_turn(
                &id,
                &mut session,
                ActiveOperation::new(Uuid::new_v4()),
                move |state| {
                    let api = api.clone();
                    let record = saved.clone();
                    async move {
                        persist_record(&api, &record, state, false).await?;
                        Ok(())
                    }
                },
            )
            .await?;
        if matches!(completion, TurnCompletion::Stopped) {
            session.interrupt_current_turn()?;
        }
        session.commit_current_write_session()?;
        persist_record(&self.api, &record, session.snapshot()?, false).await?;
        session.release_managed_sources().await;
        let mut locked = record.lock().await;
        let completed = self
            .api
            .history_complete(
                &id,
                kcode_session_history::Checkpoint {
                    expected_version: locked.version,
                    state: locked.state.clone(),
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
        let users = self.api.directory_provisioning_users()?;
        let groups = self.api.directory_provisioning_groups()?;
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
            self.api.directory_complete_handle_root(
                &handle,
                root.parse()
                    .context("created an invalid user root node ID")?,
            )?;
        }
        for group in groups {
            let group_id = group.group_id;
            let _guard = self.writer.lock().await;
            let root = self.api.bootstrap_node(Some("Group Root"))?.id.to_string();
            self.api.directory_complete_group_root(
                &group_id,
                root.parse()
                    .context("created an invalid group root node ID")?,
            )?;
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
        let mut user = self.api.directory_user(id)?;
        if !user.root_ready {
            let _guard = self.writer.lock().await;
            let root = self.api.bootstrap_node(None)?.id.to_string();
            user = self.api.directory_complete_user_root(
                id,
                root.parse()
                    .context("created an invalid user root node ID")?,
            )?;
        }
        Ok(user)
    }

    async fn directory_group(
        &self,
        group_id: &str,
    ) -> anyhow::Result<kcode_telegram_identity::Group> {
        let mut group = self.api.directory_group(group_id)?;
        if !group.root_ready {
            let _guard = self.writer.lock().await;
            let root = self.api.bootstrap_node(Some("Group Root"))?.id.to_string();
            group = self.api.directory_complete_group_root(
                group_id,
                root.parse()
                    .context("created an invalid group root node ID")?,
            )?;
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
        let chat_id = context
            .get("chatId")
            .and_then(Value::as_i64)
            .context("Telegram group context omitted its numeric chat ID")?;
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
            let numeric_message_id = message
                .get("messageId")
                .and_then(Value::as_i64)
                .context("Telegram group context message omitted its numeric message ID")?;
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
                        .telegram_group_message_media(chat_id, numeric_message_id)?;
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
                        result.text,
                        None::<String>,
                        Some(result.format),
                        result.truncated,
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
                    .telegram_save_group_message_preparation(
                        chat_id,
                        numeric_message_id,
                        &text,
                        model.as_deref(),
                        format.as_deref(),
                        truncated,
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
                    let (size_bytes, downloaded_mime_type) = self
                        .api
                        .telegram_group_message_media_metadata(chat_id, numeric_message_id)?;
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
            .telegram_events()
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
        let active = ActiveOperation::new(Uuid::new_v4());
        self.active_operations
            .lock()
            .await
            .insert(format!("telegram:{id}"), active.clone());
        let conversation_id = Arc::new(Mutex::new(
            event
                .get("conversationId")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ));
        if let Some(conversation_id) = conversation_id.lock().await.clone() {
            self.active_operations
                .lock()
                .await
                .insert(conversation_id, active.clone());
        }
        let result = tokio::time::timeout(
            telegram_timeout(&event),
            self.process_telegram_event(&event, active.clone(), conversation_id.clone()),
        )
        .await;
        self.active_operations
            .lock()
            .await
            .remove(&format!("telegram:{id}"));
        if let Some(conversation_id) = conversation_id.lock().await.clone() {
            self.remove_operation(&conversation_id, active.operation_id)
                .await;
        }
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
                let _ = self.api.cancel_intelligence(active.operation_id);
                let conversation = conversation_id.lock().await.clone();
                if let Some(conversation_id) = &conversation
                    && let Err(error) = self
                        .transition_timed_out_telegram_to_ingress(conversation_id)
                        .await
                {
                    let message = bounded_error(&error);
                    let (attempt, delay, should_warn) =
                        self.record_telegram_event_retry(&id, &message).await;
                    if should_warn {
                        tracing::warn!(
                            event_id=%id,
                            attempt,
                            retry_in_seconds=delay.as_secs(),
                            error=%message,
                            "Timed-out Telegram event retained until its conversation can be queued for ingress"
                        );
                    } else {
                        tracing::debug!(
                            event_id=%id,
                            attempt,
                            retry_in_seconds=delay.as_secs(),
                            error=%message,
                            "Timed-out Telegram ingress handoff remains unsuccessful"
                        );
                    }
                    return;
                }
                self.event_retries.lock().await.remove(&id);
                let _ = self
                    .api
                    .telegram_abort_event(&id, conversation.as_deref(), TELEGRAM_TIMEOUT_NOTICE)
                    .await;
                tracing::error!(event_id=%id,"Telegram event reached its 90-minute deadline and was aborted");
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
        active: ActiveOperation,
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
            locked.id.clone()
        };
        *bound_conversation_id.lock().await = Some(conversation_id.clone());
        self.register_operation(&conversation_id, active.clone())
            .await?;
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
                        self.api
                            .telegram_reply_event(
                                &id,
                                &conversation_id,
                                &format!(
                                    "I couldn't read {filename}: {error} Please try sending it again."
                                ),
                                None,
                            )
                            .await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                session.begin_user_turn(&text, &metadata);
                persist_record(&self.api, &record_arc, session.snapshot()?, true).await?;
            }
            let api = self.api.clone();
            let saved = record_arc.clone();
            let completion = self
                .run_session_turn(&conversation_id, &mut session, active, move |state| {
                    let api = api.clone();
                    let record = saved.clone();
                    async move {
                        persist_record(&api, &record, state, false).await?;
                        Ok(())
                    }
                })
                .await?;
            if matches!(completion, TurnCompletion::Stopped) {
                session.interrupt_current_turn()?;
                persist_record(&self.api, &record_arc, session.snapshot()?, false).await?;
                self.api
                    .telegram_interrupt_event(&id, &conversation_id)
                    .await?;
                self.complete_pending_stop(
                    &conversation_id,
                    json!({"status":"stopped","scope":"turn"}),
                )
                .await?;
                return Ok(());
            }
            persist_record(&self.api, &record_arc, session.snapshot()?, false).await?;
            if session.requires_history_ingress() {
                session.orchestration =
                    json!({"owner":"backend","status":"ending","reason":"context-limit"});
                persist_record(&self.api, &record_arc, session.snapshot()?, false).await?;
                self.request_conversation_ingress(&record_arc, None).await?;
                self.deliver_telegram_responses(&mut session, &id, &conversation_id)
                    .await?;
                self.complete_pending_stop(
                    &conversation_id,
                    json!({"status":"already-completed","scope":"turn"}),
                )
                .await?;
                return Ok(());
            }
        }
        self.deliver_telegram_responses(&mut session, &id, &conversation_id)
            .await?;
        self.complete_pending_stop(
            &conversation_id,
            json!({"status":"already-completed","scope":"turn"}),
        )
        .await?;
        Ok(())
    }

    async fn deliver_telegram_responses(
        &self,
        session: &mut Session,
        event_id: &str,
        conversation_id: &str,
    ) -> anyhow::Result<()> {
        let mut deliveries = Vec::new();
        for response in session.responses_for_external_event(event_id) {
            for (object_id, file_name) in telegram_response_object_deliveries(response) {
                deliveries.push(TelegramDelivery::Object {
                    object_id,
                    file_name,
                });
            }
            if let Some(text) = response
                .get("content")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                deliveries.push(TelegramDelivery::Text {
                    text: text.to_owned(),
                    response_warning: response
                        .get("contextWarning")
                        .cloned()
                        .unwrap_or(Value::Null),
                    captionable: response.get("role").and_then(Value::as_str) == Some("kennedy"),
                });
            }
        }
        anyhow::ensure!(
            !deliveries.is_empty(),
            "Kennedy completed the turn without a recoverable Telegram response"
        );
        let delivery_count = deliveries.len();
        let mut index = 0;
        while index < delivery_count {
            match &deliveries[index] {
                TelegramDelivery::Object {
                    object_id,
                    file_name,
                } => {
                    let mut file = session.resolve_object(object_id)?;
                    if let Some(file_name) = file_name {
                        validate_delivery_file_name(file_name)?;
                        file.file_name = file_name.clone();
                    }
                    anyhow::ensure!(
                        file.bytes.len() <= self.config.telegram_max_media_bytes,
                        "object {object_id} is {} bytes, over the configured {}-byte Telegram media limit",
                        file.bytes.len(),
                        self.config.telegram_max_media_bytes
                    );
                    let caption = telegram_reply_caption(&deliveries, index, &file);
                    let complete = caption.is_some() || index + 1 == delivery_count;
                    self.api
                        .telegram_send_object(event_id, conversation_id, &file, caption, complete)
                        .await?;
                    index += if caption.is_some() { 2 } else { 1 };
                }
                TelegramDelivery::Text {
                    text,
                    response_warning,
                    ..
                } => {
                    self.api
                        .telegram_reply_event(
                            event_id,
                            conversation_id,
                            text,
                            response_warning.as_str(),
                        )
                        .await?;
                    index += 1;
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
        if record.phase != "active" {
            return Ok(());
        }
        let mut state = record.state.clone();
        state["orchestration"] =
            json!({"owner":"backend","status":"stopped","reason":"telegram-timeout"});
        self.api
            .history_request_ingress(
                conversation_id,
                kcode_session_history::Checkpoint {
                    expected_version: record.version,
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

    async fn queue_detached_private_telegram_sessions(&self) -> anyhow::Result<()> {
        let bound = self
            .api
            .telegram_private_sessions()
            .await?
            .into_iter()
            .filter_map(|session| session.current_conversation_id)
            .collect::<HashSet<_>>();
        let histories = self.list_history().await?;
        for record in histories.iter().filter(|record| {
            record.phase == "active"
                && session_type(record) == "telegram"
                && !bound.contains(&record.id)
        }) {
            self.queue_telegram_session_for_ingress(record, "telegram-detached")
                .await?;
        }
        Ok(())
    }

    async fn queue_expired_telegram_sessions(
        &self,
        histories: &[SessionRecord],
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        for record in histories
            .iter()
            .filter(|record| telegram_session_is_expired(record, now))
        {
            self.queue_telegram_session_for_ingress(record, "telegram-session-timeout")
                .await?;
        }
        Ok(())
    }

    async fn queue_telegram_session_for_ingress(
        &self,
        summary: &SessionRecord,
        reason: &str,
    ) -> anyhow::Result<bool> {
        let id = summary.id.clone();
        let lock = self.conversation_lock(&id).await;
        let _guard = lock.lock().await;
        let record = self.get_conversation(&id).await?;
        if record.phase != "active"
            || !matches!(
                session_type(&record).as_str(),
                "telegram" | "telegram-group"
            )
        {
            return Ok(false);
        }
        if reason == "telegram-session-timeout" && !telegram_session_is_expired(&record, Utc::now())
        {
            return Ok(false);
        }
        let mut state = record.state.clone();
        state["orchestration"] = json!({
            "owner":"backend",
            "status":"stopped",
            "reason":reason,
        });
        self.api
            .history_request_ingress(
                &id,
                kcode_session_history::Checkpoint {
                    expected_version: record.version,
                    state: state.clone(),
                    user_activity: false,
                },
            )
            .await?;
        if let Some(session_id) = state.get("rustLibSessionId").and_then(Value::as_str) {
            self.api.release_managed_sources(session_id).await;
        }
        tracing::info!(session_id=%id, %reason, "Queued Telegram session for history ingress");
        Ok(true)
    }

    async fn telegram_session(
        &self,
        event: &Value,
    ) -> anyhow::Result<(Arc<Mutex<SessionRecord>>, Session)> {
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
            .and_then(|id| histories.iter().find(|record| record.id == id).cloned());
        if record.is_none() {
            record = histories.into_iter().find(|record| {
                record.phase == "active"
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
            .is_none_or(|record| record.phase != "active");
        let (record, mut session) =
            if let Some(record) = record.filter(|record| record.phase == "active") {
                let record = self.get_conversation(&record.id).await?;
                let session = self.session_for_record(&record).await?;
                (record, session)
            } else {
                self.create_telegram_session(event).await?
            };
        let record = Arc::new(Mutex::new(record));
        let id = {
            let locked = record.lock().await;
            locked.id.clone()
        };
        if event.get("conversationId").and_then(Value::as_str) != Some(&id)
            || event.get("processingStartedAt").is_none()
        {
            self.api
                .telegram_bind_event(
                    &required_string(event, "id")?,
                    &id,
                    event.get("conversationId").and_then(Value::as_str),
                )
                .await?;
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

    async fn create_telegram_session(
        &self,
        event: &Value,
    ) -> anyhow::Result<(SessionRecord, Session)> {
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
                let (bytes, mime) = self.api.telegram_event_media(&id)?;
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
                let (bytes, mime) = self.api.telegram_event_media(&id)?;
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
                        attachment["format"] = json!(result.format);
                        attachment["text"] = json!(result.text);
                        attachment["characters"] = json!(result.characters);
                        attachment["truncated"] = json!(result.truncated);
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
                let (bytes, downloaded_mime) = self.api.telegram_event_media(&id)?;
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
            self.api
                .telegram_complete_reset(
                    &id,
                    Some(
                        "There is no active Telegram session to reset. Your next message will begin one.",
                    ),
                )
                .await?;
            return Ok(());
        };
        let record = match self.get_conversation(conversation_id).await {
            Ok(record) => record,
            Err(error)
                if error
                    .downcast_ref::<super::ApiError>()
                    .is_some_and(|error| error.code == "not_found") =>
            {
                self.api
                    .telegram_complete_reset(
                        &id,
                        Some(
                            "There is no active Telegram session to reset. Your next message will begin one.",
                        ),
                    )
                    .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if record.phase != "active" {
            self.api
                .telegram_complete_reset(
                    &id,
                    Some(
                        "There is no active Telegram session to reset. Your next message will begin one.",
                    ),
                )
                .await?;
            return Ok(());
        }
        let session = self.session_for_record(&record).await?;
        session.release_managed_sources().await;
        self.api
            .history_request_ingress(
                conversation_id,
                kcode_session_history::Checkpoint {
                    expected_version: record.version,
                    state: record.state,
                    user_activity: false,
                },
            )
            .await?;
        self.api
            .telegram_complete_reset(
                &id,
                Some(
                    "Conversation reset. The Telegram session has been queued for memory ingress; your next message will begin a new session.",
                ),
            )
            .await?;
        Ok(())
    }

    async fn sync_group_updates(self: &Arc<Self>) -> anyhow::Result<()> {
        let updates = self
            .api
            .telegram_group_session_updates()
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
        if record.phase != "active" {
            if update.get("resetRequired").and_then(Value::as_bool) == Some(true) {
                self.api.telegram_complete_silent_group_reset(&id).await?;
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
            self.api.telegram_complete_silent_group_reset(&id).await?;
        } else {
            self.api
                .telegram_acknowledge_group_context(
                    &id,
                    update
                        .get("throughMessageId")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                )
                .await?;
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
                    .telegram_complete_silent_group_reset(conversation_id)
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
                    .telegram_detach_group_session(conversation_id, &group_id, telegram_user_id)
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
            .telegram_group_ingress()
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
                .state
                .get("channel")
                .and_then(|channel| channel.get("groupIngressBatchId"))
                .and_then(Value::as_str)
                == Some(&id)
        }) {
            match existing.phase.as_str() {
                "complete" => {
                    self.api.telegram_complete_group_ingress(&id).await?;
                }
                "active" => {
                    let existing = self.get_conversation(&existing.id).await?;
                    self.api
                        .history_request_ingress(
                            &existing.id,
                            kcode_session_history::Checkpoint {
                                expected_version: existing.version,
                                state: existing.state,
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
                &record.id,
                kcode_session_history::Checkpoint {
                    expected_version: record.version,
                    state: record.state,
                    user_activity: false,
                },
            )
            .await?;
        Ok(())
    }
}

async fn persist_record(
    api: &Api,
    record: &Arc<Mutex<SessionRecord>>,
    state: Value,
    user_activity: bool,
) -> anyhow::Result<()> {
    let mut record = record.lock().await;
    let id = record.id.clone();
    let result = match api
        .history_checkpoint(
            &id,
            kcode_session_history::Checkpoint {
                expected_version: record.version,
                state: state.clone(),
                user_activity,
            },
        )
        .await
    {
        Ok(result) => result,
        Err(error) if error.code == "state_conflict" => {
            let latest = api.history_get_session(&id).await?;
            if latest.state == state {
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
    record: &Arc<Mutex<SessionRecord>>,
    archive: Value,
) -> anyhow::Result<()> {
    let mut record = record.lock().await;
    let id = record.id.clone();
    let mut state = record.state.clone();
    state["historyIngress"] = archive;
    let result = match api
        .history_checkpoint(
            &id,
            kcode_session_history::Checkpoint {
                expected_version: record.version,
                state: state.clone(),
                user_activity: false,
            },
        )
        .await
    {
        Ok(result) => result,
        Err(error) if error.code == "state_conflict" => {
            let latest = api.history_get_session(&id).await?;
            if latest.state == state {
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
fn session_type(record: &SessionRecord) -> String {
    record
        .state
        .get("sessionType")
        .and_then(Value::as_str)
        .unwrap_or("conversation")
        .into()
}

fn stop_scope(record: &SessionRecord) -> &'static str {
    let phase = record.phase.as_str();
    let session_type = session_type(record);
    if phase == "active"
        && matches!(
            session_type.as_str(),
            "conversation" | "telegram" | "telegram-group"
        )
    {
        "turn"
    } else if phase == "active" && session_type == "free-time" {
        "self-time-run"
    } else {
        "session"
    }
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

fn next_ingress(histories: &[SessionRecord], now: DateTime<Utc>) -> Option<&SessionRecord> {
    histories
        .iter()
        .filter(|record| match record.phase.as_str() {
            "ingress_in_progress" => true,
            "ingress_pending" => record
                .ingress_next_attempt_at
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .is_none_or(|next| next.with_timezone(&Utc) <= now),
            _ => false,
        })
        .min_by(|left, right| ingress_record_order(left, right))
}

fn ingress_record_order(left: &SessionRecord, right: &SessionRecord) -> std::cmp::Ordering {
    let rank = |record: &SessionRecord| {
        if record.phase == "ingress_in_progress" {
            0
        } else {
            1
        }
    };
    rank(left)
        .cmp(&rank(right))
        .then_with(|| ingress_record_time(left).cmp(&ingress_record_time(right)))
        .then_with(|| left.id.cmp(&right.id))
}

fn ingress_record_time(record: &SessionRecord) -> DateTime<Utc> {
    [&record.updated_at, &record.started_at]
        .into_iter()
        .find_map(|value| {
            DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|value| value.with_timezone(&Utc))
        })
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

fn is_browser_conversation(record: &SessionRecord) -> bool {
    session_type(record) == "conversation"
}
fn telegram_session_is_expired(record: &SessionRecord, now: DateTime<Utc>) -> bool {
    if record.phase != "active"
        || !matches!(session_type(record).as_str(), "telegram" | "telegram-group")
        || record.state.get("pendingTurn").and_then(Value::as_bool) == Some(true)
    {
        return false;
    }
    DateTime::parse_from_rfc3339(&record.started_at)
        .ok()
        .is_some_and(|started| now >= started.with_timezone(&Utc) + TELEGRAM_SESSION_MAX_AGE)
}
fn record_channel(record: &SessionRecord) -> Option<&Value> {
    record.state.get("channel")
}
fn record_group_id(record: &SessionRecord) -> Option<&str> {
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
fn record_user_id(record: &SessionRecord) -> String {
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
    let source_note =
        if message.get("fileNameSource").and_then(Value::as_str) == Some("synthesized") {
            " (synthesized because Telegram supplied no filename)"
        } else {
            ""
        };
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

fn telegram_response_object_deliveries(response: &Value) -> Vec<(String, Option<String>)> {
    let objects = response
        .get("objects")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let attachments = response
        .get("attachments")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    objects
        .iter()
        .filter_map(Value::as_str)
        .enumerate()
        .map(|(index, object_id)| {
            let descriptor = attachments
                .iter()
                .find(|candidate| {
                    ["objectId", "pendingId", "id"]
                        .iter()
                        .any(|key| candidate.get(key).and_then(Value::as_str) == Some(object_id))
                })
                .or_else(|| attachments.get(index));
            let file_name = descriptor
                .and_then(|descriptor| descriptor.get("fileName"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            (object_id.to_owned(), file_name)
        })
        .collect()
}

fn telegram_reply_caption<'a>(
    deliveries: &'a [TelegramDelivery],
    object_index: usize,
    file: &ResolvedObject,
) -> Option<&'a str> {
    if object_index + 2 != deliveries.len() {
        return None;
    }
    match &deliveries[object_index + 1] {
        TelegramDelivery::Text {
            text,
            response_warning,
            captionable: true,
        } if response_warning.is_null() || response_warning.as_str() == Some("") => {
            telegram_caption_for(file, text)
        }
        _ => None,
    }
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

    fn session_record(overrides: Value) -> SessionRecord {
        let mut record = json!({
            "id":"session",
            "phase":"active",
            "started_at":"2026-07-30T00:00:00Z",
            "updated_at":"2026-07-30T00:00:00Z",
            "state":{},
            "provenance_id":null,
            "version":1,
            "last_user_message_at":null,
            "ended_at":null,
            "ingress_failure_count":0,
            "ingress_failures":[],
            "ingress_next_attempt_at":null
        });
        record
            .as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        serde_json::from_value(record).unwrap()
    }

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
        let browser = session_record(json!({
            "phase":"active",
            "state":{
                "sessionType":"conversation",
                "orchestration":{"owner":"backend","status":"idle"}
            }
        }));
        assert!(is_browser_conversation(&browser));
        assert!(!is_browser_conversation(&session_record(json!({
            "phase":"active",
            "state":{"sessionType":"telegram"}
        }))));
    }

    #[test]
    fn stop_scope_preserves_interactive_sessions_and_terminates_autonomous_work() {
        for session_type in ["conversation", "telegram", "telegram-group"] {
            assert_eq!(
                stop_scope(&session_record(json!({
                    "phase":"active",
                    "state":{"sessionType":session_type}
                }))),
                "turn"
            );
        }
        assert_eq!(
            stop_scope(&session_record(json!({
                "phase":"active",
                "state":{"sessionType":"free-time"}
            }))),
            "self-time-run"
        );
        for (phase, session_type) in [
            ("active", "wakeup"),
            ("ingress_pending", "conversation"),
            ("ingress_in_progress", "telegram"),
        ] {
            assert_eq!(
                stop_scope(&session_record(json!({
                    "phase":phase,
                    "state":{"sessionType":session_type}
                }))),
                "session"
            );
        }
    }

    #[test]
    fn telegram_sessions_roll_over_six_hours_after_creation_once_idle() {
        let now = DateTime::parse_from_rfc3339("2026-07-30T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let record = session_record(json!({
            "phase":"active",
            "started_at":"2026-07-30T06:00:00Z",
            "state":{"sessionType":"telegram","pendingTurn":false}
        }));
        assert!(telegram_session_is_expired(&record, now));
        assert!(!telegram_session_is_expired(
            &session_record(json!({
                "phase":"active",
                "started_at":"2026-07-30T06:00:01Z",
                "state":{"sessionType":"telegram-group","pendingTurn":false}
            })),
            now
        ));
        assert!(!telegram_session_is_expired(
            &session_record(json!({
                "phase":"active",
                "started_at":"2026-07-30T05:00:00Z",
                "state":{"sessionType":"telegram","pendingTurn":true}
            })),
            now
        ));
        assert!(!telegram_session_is_expired(
            &session_record(json!({
                "phase":"ingress_pending",
                "started_at":"2026-07-30T05:00:00Z",
                "state":{"sessionType":"telegram","pendingTurn":false}
            })),
            now
        ));
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
            session_record(json!({
                "id":"newest-not-due",
                "phase":"ingress_pending",
                "updated_at":"2026-07-25T02:59:00Z",
                "ingress_next_attempt_at":"2026-07-25T03:01:00Z"
            })),
            session_record(json!({
                "id":"newer-due",
                "phase":"ingress_pending",
                "updated_at":"2026-07-25T02:30:00Z",
                "ingress_next_attempt_at":null
            })),
            session_record(json!({
                "id":"oldest-due",
                "phase":"ingress_pending",
                "updated_at":"2026-07-25T01:30:00Z",
                "ingress_next_attempt_at":"2026-07-25T02:00:00Z"
            })),
        ];
        assert_eq!(
            next_ingress(&histories, now).map(|record| record.id.as_str()),
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
    fn telegram_object_delivery_carries_each_emitted_filename_override() {
        let deliveries = telegram_response_object_deliveries(&json!({
            "objects":["pending:2","AAECAwQF"],
            "attachments":[
                {"objectId":"AAECAwQF","fileName":"canonical-report.pdf"},
                {"objectId":"pending:2","fileName":"draft-report.pdf"}
            ]
        }));
        assert_eq!(
            deliveries,
            vec![
                ("pending:2".into(), Some("draft-report.pdf".into())),
                ("AAECAwQF".into(), Some("canonical-report.pdf".into())),
            ]
        );
        assert_eq!(
            telegram_response_object_deliveries(&json!({
                "objects":["AAECAwQG"],
                "attachments":[{"fileName":"index-fallback.txt"}]
            })),
            vec![("AAECAwQG".into(), Some("index-fallback.txt".into()))]
        );
    }

    #[test]
    fn final_kennedy_text_becomes_one_exact_caption_when_supported() {
        let file = ResolvedObject {
            object_id: "object".into(),
            bytes: vec![1],
            file_name: "photo.jpg".into(),
            media_type: "image/jpeg".into(),
            transport_kind: Some("photo".into()),
        };
        let deliveries = vec![
            TelegramDelivery::Object {
                object_id: "object".into(),
                file_name: None,
            },
            TelegramDelivery::Text {
                text: "  exact caption\n".into(),
                response_warning: Value::Null,
                captionable: true,
            },
        ];
        assert_eq!(
            telegram_reply_caption(&deliveries, 0, &file),
            Some("  exact caption\n")
        );

        let warning = vec![
            TelegramDelivery::Object {
                object_id: "object".into(),
                file_name: None,
            },
            TelegramDelivery::Text {
                text: "caption".into(),
                response_warning: json!("warning"),
                captionable: true,
            },
        ];
        assert_eq!(telegram_reply_caption(&warning, 0, &file), None);
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
        let pending = session_record(json!({
            "id":"pending",
            "phase":"ingress_pending",
            "updated_at":"2026-07-25T01:00:00Z"
        }));
        let claimed = session_record(json!({
            "id":"claimed",
            "phase":"ingress_in_progress",
            "updated_at":"2026-07-25T02:00:00Z",
            "state":{"sourceCreatedAt":"2026-07-25T00:00:00Z"}
        }));
        let now = DateTime::parse_from_rfc3339("2026-07-25T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let histories = vec![pending, claimed];
        assert_eq!(
            next_ingress(&histories, now).map(|record| record.id.as_str()),
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
