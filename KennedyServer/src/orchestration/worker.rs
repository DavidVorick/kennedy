use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context as _;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
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

pub(crate) struct Orchestrator {
    config: Config,
    api: Api,
    runtime: OnceCell<Runtime>,
    writer: Arc<Mutex<()>>,
    writer_job_active: AtomicBool,
    commands_in_flight: Mutex<HashSet<String>>,
    events_in_flight: Mutex<HashSet<String>>,
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
        loop {
            match self.poll_once().await {
                Ok(()) => *self.last_poll_error.write().await = None,
                Err(error) => {
                    let message = error.to_string();
                    let mut previous = self.last_poll_error.write().await;
                    if previous.as_deref() != Some(message.as_str()) {
                        tracing::error!(error=%error, "Backend orchestration poll will retry");
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
                    let provider = runtime.model.provider.clone();
                    let model = runtime.model.model.clone();
                    let _ = self.runtime.set(runtime);
                    tracing::info!(%provider, %model, "Native Rust orchestration worker ready");
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
        let (kweb, intelligence, history, telegram, audio) = tokio::join!(
            self.api.kmap_get("/api/v1/kmap/health"),
            self.api.intelligence_get("/health"),
            self.api.history_get("/api/v1/conversations/health"),
            self.api.telegram_health(),
            self.api.audio_get("/api/v1/audio-ingress/health"),
        );
        kweb?;
        intelligence?;
        history?;
        telegram?;
        audio?;
        let manuals = Manuals::load(&self.config.system_prompts_directory)?;
        let (roots, providers) = tokio::try_join!(
            self.api.kmap_get("/api/v1/kmap/roots"),
            self.api.intelligence_get("/api/v1/providers"),
        )?;
        let runtime = Runtime {
            manuals,
            model: RuntimeModel::from_provider_payload(&providers)?,
            user_root_node_id: required_string(&roots, "user_root_node_id")?,
            kennedy_root_node_id: required_string(&roots, "kennedy_root_node_id")?,
        };
        let (history_repairs, audio_repairs) = tokio::join!(
            self.api
                .history_post("/api/v1/conversations/ingress/repairs/release", json!({}),),
            self.api
                .audio_post("/api/v1/audio-ingress/ingress/repairs/release", json!({}),),
        );
        history_repairs?;
        audio_repairs?;
        self.api
            .history_delete("/api/v1/conversations/unstarted", None)
            .await?;
        Ok(runtime)
    }

    fn runtime(&self) -> anyhow::Result<&Runtime> {
        self.runtime
            .get()
            .context("orchestration runtime is not initialized")
    }

    async fn poll_once(self: &Arc<Self>) -> anyhow::Result<()> {
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
            .history_get("/api/v1/conversations/summaries")
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
            .history_get("/api/v1/conversation-commands")
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
                    let _ = self
                        .api
                        .intelligence_post(
                            &format!("/api/v1/operations/{operation}/cancel"),
                            json!({}),
                        )
                        .await;
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
                    tracing::error!(command_id=%id, error=%error, "Browser conversation command will retry");
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
            self.api
                .history_post(
                    &format!(
                        "/api/v1/conversation-commands/{}/claim",
                        encode_path(&command_id)
                    ),
                    json!({}),
                )
                .await?
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
                self.api.release_rust_libs(session_id).await;
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
        let state = record.get("state").cloned().unwrap_or_else(|| json!({}));
        let session_type = session_type(record);
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
        options.mode = if session_type == "free-time" {
            AgentMode::FreeTime
        } else {
            AgentMode::Conversation
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
        session.release_rust_libs().await;
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
            .history_post(
                &format!("/api/v1/conversations/{}/request-ingress", encode_path(&id)),
                json!({
                    "expected_version":version(&locked)?,
                    "state":state,
                }),
            )
            .await?;
        *locked = response.clone();
        Ok(response)
    }

    async fn complete_command(&self, id: &str, outcome: Value) -> anyhow::Result<()> {
        self.api
            .history_post(
                &format!("/api/v1/conversation-commands/{}/complete", encode_path(id)),
                json!({"outcome":outcome}),
            )
            .await?;
        Ok(())
    }

    async fn get_conversation(&self, id: &str) -> anyhow::Result<Value> {
        Ok(self
            .api
            .history_get(&format!("/api/v1/conversations/{}", encode_path(id)))
            .await?)
    }

    async fn schedule_writer_job(self: &Arc<Self>, histories: &[Value]) -> anyhow::Result<()> {
        if self.writer_job_active.load(Ordering::Acquire) {
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
        if let Some(record) = histories
            .iter()
            .find(|record| {
                matches!(
                    record.get("phase").and_then(Value::as_str),
                    Some("ingress_pending" | "ingress_in_progress")
                )
            })
            .cloned()
        {
            self.launch_writer_job("session history ingress", move |worker| async move {
                let id = required_string(&record, "id")?;
                let record = worker.get_conversation(&id).await?;
                worker.process_conversation_ingress(record).await
            })
            .await;
            return Ok(());
        }
        let Some(job) = self.api.next_memory_ingress()? else {
            return Ok(());
        };
        match job.source_kind {
            kennedy_memory_ingress::SourceKind::Conversation => {
                anyhow::bail!(
                    "legacy conversation memory-ingress job {} remained after the Session History cutover",
                    job.source_id
                );
            }
            kennedy_memory_ingress::SourceKind::Audio => {
                let piece = self
                    .api
                    .audio_get(&format!(
                        "/api/v1/audio-ingress/pieces/{}",
                        encode_path(&job.source_id)
                    ))
                    .await?;
                self.launch_writer_job("audio ingress", move |worker| async move {
                    worker.process_audio_ingress(piece).await
                })
                .await;
            }
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
                tracing::error!(%label, error=%error, "Kmap writer job will retry");
            }
            worker.writer_job_active.store(false, Ordering::Release);
        });
    }

    async fn process_conversation_ingress(&self, mut record: Value) -> anyhow::Result<()> {
        let id = required_string(&record, "id")?;
        let rust_session_id = format!("kennedy:history-ingress:{id}");
        let mut stage = "prepare";
        let result = async {
            if record.get("phase").and_then(Value::as_str) == Some("ingress_pending") {
                record
                    .get("state")
                    .and_then(|state| state.get("journalPath"))
                    .and_then(Value::as_str)
                    .context("The queued session has no authoritative Chatend journal")?;
                stage = "claim";
                record = self
                    .api
                    .history_post(
                        &format!("/api/v1/conversations/{}/ingress-started", encode_path(&id)),
                        json!({
                            "expected_version":version(&record)?,
                            "provenance_id":format!("session:{id}"),
                            "completion_protocol":"one-session-transaction-v1"
                        }),
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
            let mut session = Session::new(
                self.api.clone(),
                runtime.manuals,
                runtime.model,
                options,
                Some(&state),
            )
            .await?;
            session.pending_turn = true;
            let record = Arc::new(Mutex::new(record));
            persist_ingress_record(&self.api, &record, session.snapshot()?).await?;
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
            persist_ingress_record(&self.api, &record, session.snapshot()?).await?;
            stage = "completion";
            let mut locked = record.lock().await;
            let completed = self
                .api
                .history_post(
                    &format!(
                        "/api/v1/conversations/{}/ingress-completed",
                        encode_path(&id)
                    ),
                    json!({"expected_version":version(&locked)?}),
                )
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
            self.record_conversation_ingress_failure(&id, stage, &error)
                .await
                .ok();
            return Err(error);
        }
        self.api.release_rust_libs(&rust_session_id).await;
        Ok(())
    }

    async fn record_conversation_ingress_failure(
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
        self.api.history_post(
                &format!(
                    "/api/v1/conversations/{}/ingress-failure",
                    encode_path(id)
                ),
                json!({"expected_version":version(&latest)?,"stage":stage,"code":"ingress_error","message":bounded_error(error)}),
            )
            .await?;
        Ok(())
    }

    async fn process_audio_ingress(&self, mut piece: Value) -> anyhow::Result<()> {
        let id = required_string(&piece, "id")?;
        let rust_session_id = format!("kennedy:audio-ingress:{id}");
        let mut stage = "prepare";
        let result = async {
            if piece.get("phase").and_then(Value::as_str) == Some("ingress_pending") {
                stage = "claim";
                piece = self
                    .api.audio_post(
                        &format!(
                            "/api/v1/audio-ingress/pieces/{}/ingress-started",
                            encode_path(&id)
                        ),
                        json!({
                            "expected_version":version(&piece)?,
                            "provenance_id":format!("session:audio:{id}"),
                            "completion_protocol":kennedy_memory_ingress::COMPLETION_PROTOCOL
                        }),
                    )
                    .await?;
            }
            if piece.get("phase").and_then(Value::as_str) != Some("ingress_in_progress") {
                return Ok(());
            }
            stage = "model_loop";
            let runtime = self.runtime()?.clone();
            let options = SessionOptions {
                session_type: "history-ingress".into(),
                root_node_ids: vec![runtime.user_root_node_id.clone(), runtime.kennedy_root_node_id.clone()],
                reference_root_node_ids: Vec::new(), channel:Value::Null, free_time:Value::Null, orchestration:Value::Null,
                provenance_id:None,mode:AgentMode::Ingress{record_id:None},source_session_type:Some("audio".into()),group_context:Value::Null,rust_lib_session_id:Some(rust_session_id.clone()),
            };
            let state=piece.get("state").cloned().unwrap_or_else(||json!({}));
            let mut session=Session::new(self.api.clone(),runtime.manuals,runtime.model,options,state.get("historyIngress")).await?;
            session.stage_ingress_source(
                &format!(
                    "Vnote final transcript piece\n\nRecording began: {}\nRecording SHA-256: {}\nOriginal filename: {}\nTranscript piece: {} of {}\n\n{}",
                    piece.get("source_created_at").and_then(Value::as_str).unwrap_or("unknown"),
                    piece.get("sha256").and_then(Value::as_str).unwrap_or("unknown"),
                    piece.get("original_filename").and_then(Value::as_str).unwrap_or("unknown"),
                    piece.get("piece_index").and_then(Value::as_u64).unwrap_or_default()+1,
                    piece.get("piece_count").and_then(Value::as_u64).unwrap_or_default(),
                    piece.get("transcript_text").and_then(Value::as_str).unwrap_or("")
                ),
                &json!({"kind":"audio-transcript","audioPieceId":id}),
            ).await?;
            let piece=Arc::new(Mutex::new(piece));persist_audio_ingress(&self.api,&piece,session.snapshot()?).await?;
            let api=self.api.clone();let saved=piece.clone();
            session.run_pending_turn(Uuid::new_v4(),move|session_state|{let api=api.clone();let piece=saved.clone();async move{persist_audio_ingress(&api,&piece,session_state).await?;Ok(())}}).await?;
            let completed_state = session.snapshot()?;
            persist_audio_ingress(&self.api,&piece,completed_state.clone()).await?;
            self.api.history_post(
                "/api/v1/session-history",
                json!({
                    "session_object_id":completed_state.get("sessionObjectId").and_then(Value::as_str).context("audio ingress session has no permanent object ID")?,
                    "journal_path":completed_state.get("journalPath"),
                }),
            ).await?;
            stage="completion";
            let locked=piece.lock().await;
            self.api.audio_post(&format!("/api/v1/audio-ingress/pieces/{}/ingress-completed",encode_path(&id)),json!({"expected_version":version(&locked)?})).await?;
            Ok(())
        }.await;
        if let Err(error) = result {
            if let Ok(latest) = self
                .api
                .audio_get(&format!(
                    "/api/v1/audio-ingress/pieces/{}",
                    encode_path(&id)
                ))
                .await
                && matches!(
                    latest.get("phase").and_then(Value::as_str),
                    Some("ingress_pending" | "ingress_in_progress")
                )
            {
                let _=self.api.audio_post(&format!("/api/v1/audio-ingress/pieces/{}/ingress-failure",encode_path(&id)),json!({"expected_version":version(&latest)?,"stage":stage,"code":"ingress_error","message":bounded_error(&error)})).await;
            }
            return Err(error);
        }
        self.api.release_rust_libs(&rust_session_id).await;
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
                let _ = self
                    .api
                    .intelligence_post(&format!("/api/v1/operations/{operation}/cancel"), json!({}))
                    .await;
                "hard-stop".into()
            }
        };
        session.finalize_free_time(&reason)?;
        session.commit_current_write_session()?;
        persist_record(&self.api, &record_arc, session.snapshot()?, false).await?;
        session.release_rust_libs().await;
        let mut locked = record_arc.lock().await;
        let completed=self.api.history_post(&format!("/api/v1/conversations/{}/complete",encode_path(&id)),json!({"expected_version":version(&locked)?,"state":locked.get("state").cloned().unwrap_or(Value::Null)})).await?;
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
        self.api
            .history_post(
                "/api/v1/conversations",
                json!({"started_at":session.started_at,"state":session.snapshot()?}),
            )
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
                tracing::error!(error=%error,"Telegram directory provisioning will retry");
            }
            worker.directory_in_flight.lock().await.remove("directory");
        });
        Ok(())
    }

    async fn provision_directory(&self) -> anyhow::Result<()> {
        let runtime = self.runtime()?;
        let (users, groups) = tokio::try_join!(
            self.api
                .directory_get("/api/v1/telegram-directory/users/provisioning"),
            self.api
                .directory_get("/api/v1/telegram-directory/groups/provisioning")
        )?;
        for user in users
            .get("users")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let handle = required_string(user, "handle")?;
            let is_web = handle
                .trim_start_matches('@')
                .eq_ignore_ascii_case(self.config.telegram_web_user_handle.trim_start_matches('@'));
            let root = if is_web {
                runtime.user_root_node_id.clone()
            } else {
                let _guard = self.writer.lock().await;
                let created = self.api.bootstrap_node(None).await?;
                created
                    .pointer("/node/id")
                    .and_then(Value::as_str)
                    .context("created user root omitted its node ID")?
                    .to_owned()
            };
            self.api
                .directory_post(
                    &format!(
                        "/api/v1/telegram-directory/users/by-handle/{}/root-ready",
                        encode_path(&handle)
                    ),
                    json!({"rootNodeId":root}),
                )
                .await?;
        }
        for group in groups
            .get("groups")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let group_id = required_string(group, "groupId")?;
            let _guard = self.writer.lock().await;
            let created = self.api.bootstrap_node(Some("Group Root")).await?;
            let root = created
                .pointer("/node/id")
                .and_then(Value::as_str)
                .context("created group root omitted its node ID")?
                .to_owned();
            self.api
                .directory_post(
                    &format!(
                        "/api/v1/telegram-directory/groups/{}/root-ready",
                        encode_path(&group_id)
                    ),
                    json!({"rootNodeId":root}),
                )
                .await?;
        }
        Ok(())
    }

    async fn directory_user(&self, event: &Value) -> anyhow::Result<Value> {
        let id = event
            .get("telegramUserId")
            .map(value_string)
            .context("Telegram event omitted user ID")?;
        let mut user = self
            .api
            .directory_get(&format!(
                "/api/v1/telegram-directory/users/{}",
                encode_path(&id)
            ))
            .await?;
        if user.get("rootReady").and_then(Value::as_bool) != Some(true) {
            let _guard = self.writer.lock().await;
            let created = self.api.bootstrap_node(None).await?;
            let root = created
                .pointer("/node/id")
                .and_then(Value::as_str)
                .context("created user root omitted its node ID")?;
            user = self
                .api
                .directory_post(
                    &format!(
                        "/api/v1/telegram-directory/users/{}/root-ready",
                        encode_path(&id)
                    ),
                    json!({"rootNodeId":root}),
                )
                .await?;
        }
        Ok(user)
    }
    async fn directory_group(&self, group_id: &str) -> anyhow::Result<Value> {
        let mut group = self
            .api
            .directory_get(&format!(
                "/api/v1/telegram-directory/groups/{}",
                encode_path(group_id)
            ))
            .await?;
        if group.get("rootReady").and_then(Value::as_bool) != Some(true) {
            let _guard = self.writer.lock().await;
            let created = self.api.bootstrap_node(Some("Group Root")).await?;
            let root = created
                .pointer("/node/id")
                .and_then(Value::as_str)
                .context("created group root omitted its node ID")?;
            group = self
                .api
                .directory_post(
                    &format!(
                        "/api/v1/telegram-directory/groups/{}/root-ready",
                        encode_path(group_id)
                    ),
                    json!({"rootNodeId":root}),
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
        context["groupRootNodeId"] = group.get("rootNodeId").cloned().unwrap_or(Value::Null);
        context["groupRootReady"] = group.get("rootReady").cloned().unwrap_or(json!(true));
        let mut participants = Vec::new();
        for participant in context
            .get("participants")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let user = self.directory_user(&participant).await?;
            let mut participant = participant;
            participant["rootNodeId"] = user.get("rootNodeId").cloned().unwrap_or(Value::Null);
            participant["rootReady"] = user.get("rootReady").cloned().unwrap_or(json!(true));
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
                && matches!(kind.as_str(), "voice" | "document")
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
                    if kind == "voice" {
                        let result = self
                            .api
                            .transcribe(
                                &self.runtime()?.model.provider,
                                &self.runtime()?.model.model,
                                bytes,
                                message
                                    .get("fileName")
                                    .and_then(Value::as_str)
                                    .unwrap_or("telegram-group-voice.ogg")
                                    .to_owned(),
                                &mime,
                            )
                            .await?;
                        Ok::<_, anyhow::Error>((
                            required_string(&result, "text")?,
                            Some(required_string(&result, "transcription_model")?),
                            None,
                            false,
                        ))
                    } else {
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
                        Ok((
                            required_string(&result, "text")?,
                            None,
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
                }
                .await;
                let (text, model, format, truncated) = match prepared {
                    Ok(value) => value,
                    Err(error) => (
                        format!(
                            "{} failed: {error}",
                            if kind == "voice" {
                                "Voice transcription"
                            } else {
                                "Document extraction"
                            }
                        ),
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
            if !excluded && matches!(kind.as_str(), "voice" | "document") {
                message["mediaRef"] = json!({"kind":kind,"source":"telegram-group","chatId":context.get("chatId").cloned().unwrap_or(Value::Null),"messageId":message.get("messageId").cloned().unwrap_or(Value::Null),"fileName":message.get("fileName").cloned().unwrap_or(Value::Null),"mimeType":message.get("mimeType").cloned().unwrap_or(Value::Null),"durationSeconds":message.get("durationSeconds").cloned().unwrap_or(Value::Null)});
                let base = message.get("text").and_then(Value::as_str).unwrap_or("");
                let prepared = message
                    .get("preparedText")
                    .and_then(Value::as_str)
                    .unwrap_or(if kind == "voice" {
                        "Voice note transcription unavailable."
                    } else {
                        "Document text extraction unavailable."
                    });
                message["text"] = json!(if kind == "voice" {
                    format!("[Voice note transcription]\n{prepared}")
                } else {
                    format!(
                        "{base}\n\n[Document: {}]\n{prepared}",
                        message
                            .get("fileName")
                            .and_then(Value::as_str)
                            .unwrap_or("telegram-document")
                    )
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
        for event in events {
            let id = required_string(&event, "id")?;
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
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(event_id=%id,error=%error,"Telegram event will retry")
            }
            Err(_) => {
                let _ = self
                    .api
                    .intelligence_post(&format!("/api/v1/operations/{operation}/cancel"), json!({}))
                    .await;
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

    async fn process_telegram_event(
        &self,
        event: &Value,
        operation: Uuid,
        bound_conversation_id: Arc<Mutex<Option<String>>>,
    ) -> anyhow::Result<()> {
        let id = required_string(event, "id")?;
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
                self.api.telegram_post(
                    &format!("/api/v1/events/{}/reply",encode_path(&id)),
                    json!({
                        "conversationId":conversation_id,
                        "text":"This session reached its context limit and has been sent to history ingress.",
                        "contextWarning":"session ended at the emergency context limit"
                    }),
                ).await?;
                return Ok(());
            }
        }
        let response = session
            .answer_for_external_event(&id)
            .context("Kennedy completed the turn without a recoverable Telegram response")?;
        if let Some(text) = response
            .get("content")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            self.api.telegram_post(&format!("/api/v1/events/{}/reply",encode_path(&id)),json!({"conversationId":conversation_id,"text":text,"contextWarning":response.get("contextWarning").cloned().unwrap_or(Value::Null)})).await?;
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
            .history_post(
                &format!(
                    "/api/v1/conversations/{}/request-ingress",
                    encode_path(conversation_id)
                ),
                json!({"expected_version":version(&record)?,"state":state}),
            )
            .await?;
        if let Some(session_id) = state.get("rustLibSessionId").and_then(Value::as_str) {
            self.api.release_rust_libs(session_id).await;
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
        let mut roots = vec![required_string(&user, "rootNodeId")?];
        let mut channel = json!({"kind":if group{"telegram-group"}else{"telegram"},"telegramUserId":event.get("telegramUserId").cloned().unwrap_or(Value::Null),"chatId":event.get("chatId").cloned().unwrap_or(Value::Null),"groupId":event.get("groupId").cloned().unwrap_or(Value::Null),"username":event.get("username").cloned().unwrap_or(Value::Null),"displayName":event.get("displayName").cloned().unwrap_or(Value::Null)});
        let mut references = Vec::new();
        if group {
            let group_id = required_string(event, "groupId")?;
            let group_record = self.directory_group(&group_id).await?;
            roots.push(required_string(&group_record, "rootNodeId")?);
            if let Some(context) = event.get("groupContext") {
                let context = self
                    .prepare_group_context(
                        context.clone(),
                        event.get("messageId").map(value_string).as_deref(),
                        &group_id,
                    )
                    .await?;
                channel["groupContext"] = context.clone();
                channel["groupRootNodeId"] = group_record
                    .get("rootNodeId")
                    .cloned()
                    .unwrap_or(Value::Null);
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
        let record = self
            .api
            .history_post(
                "/api/v1/conversations",
                json!({"started_at":session.started_at,"state":session.snapshot()?}),
            )
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
                let existing = event.get("transcription").and_then(Value::as_str);
                let (text, model) = if let Some(text) = existing {
                    (
                        text.to_owned(),
                        event
                            .get("transcriptionModel")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned(),
                    )
                } else {
                    let result = self
                        .api
                        .transcribe(
                            &self.runtime()?.model.provider,
                            &self.runtime()?.model.model,
                            bytes.clone(),
                            "telegram-voice.ogg".into(),
                            &mime,
                        )
                        .await?;
                    let text = required_string(&result, "text")?;
                    let model = required_string(&result, "transcription_model")?;
                    self.api
                        .telegram_post(
                            &format!("/api/v1/events/{}/transcription", encode_path(&id)),
                            json!({"text":text,"transcriptionModel":model}),
                        )
                        .await?;
                    (text, model)
                };
                Ok((
                    text.clone(),
                    json!({"externalEventId":id,"inputKind":"voice","transcriptionModel":model,"media":{"id":format!("telegram:{id}"),"kind":"voice","source":"telegram","mimeType":mime,"fileName":"telegram-voice.ogg","dataUrl":data_url(&mime,&bytes),"sizeBytes":bytes.len(),"durationSeconds":event.get("durationSeconds").cloned().unwrap_or(Value::Null)}}),
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
                let result = self
                    .api
                    .extract_document(bytes.clone(), filename.clone(), &mime)
                    .await?;
                Ok((
                    event
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                    json!({"externalEventId":id,"inputKind":"document","attachments":[{"id":format!("telegram:{id}"),"kind":"document","source":"telegram","fileName":filename,"mimeType":mime,"sizeBytes":bytes.len(),"dataUrl":data_url(&mime,&bytes),"format":result.get("format").cloned().unwrap_or(Value::Null),"text":result.get("text").cloned().unwrap_or(Value::Null),"characters":result.get("characters").cloned().unwrap_or(Value::Null),"truncated":result.get("truncated").cloned().unwrap_or(json!(false))}]}),
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
        session.release_rust_libs().await;
        self.api.history_post(&format!("/api/v1/conversations/{}/request-ingress",encode_path(conversation_id)),json!({"expected_version":version(&record)?,"state":record.get("state").cloned().unwrap_or(Value::Null)})).await?;
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
                    tracing::error!(conversation_id=%id,error=%error,"Telegram group context update will retry");
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
        let record = self.get_conversation(&id).await?;
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
                    tracing::error!(batch_id=%id,error=%error,"Telegram group ingress preparation will retry");
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
                    self.api.history_post(&format!("/api/v1/conversations/{}/request-ingress",encode_path(required_string(&existing,"id")?)),json!({"expected_version":version(&existing)?,"state":existing.get("state").cloned().unwrap_or(Value::Null)})).await?;
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
        let roots = vec![
            required_string(&group, "rootNodeId")?,
            runtime.kennedy_root_node_id,
        ];
        let channel = json!({"kind":"telegram-group","chatId":batch.get("chatId").cloned().unwrap_or(Value::Null),"groupId":group_id,"groupRootNodeId":group.get("rootNodeId").cloned().unwrap_or(Value::Null),"groupIngressBatchId":id,"backgroundIngress":true,"groupContext":context});
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
        let record=self.api.history_post("/api/v1/conversations",json!({"started_at":batch.get("createdAt").cloned().unwrap_or(json!(Utc::now().to_rfc3339())),"state":session.snapshot()?})).await?;
        self.api.history_post(&format!("/api/v1/conversations/{}/request-ingress",encode_path(required_string(&record,"id")?)),json!({"expected_version":version(&record)?,"state":record.get("state").cloned().unwrap_or(Value::Null)})).await?;
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
        .history_put(
            &format!(
                "/api/v1/conversations/{}/checkpoint",
                encode_path(&id)
            ),
            json!({"expected_version":version(&record)?,"state":state,"user_activity":user_activity}),
        )
        .await
    {
        Ok(result) => result,
        Err(error) if error.code == "state_conflict" => {
            let latest = api
                .history_get(
                    &format!("/api/v1/conversations/{}", encode_path(&id)),
                )
                .await?;
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
        .history_put(
            &format!(
                "/api/v1/conversations/{}/ingress-checkpoint",
                encode_path(&id)
            ),
            json!({"expected_version":version(&record)?,"state":state}),
        )
        .await
    {
        Ok(result) => result,
        Err(error) if error.code == "state_conflict" => {
            let latest = api
                .history_get(&format!("/api/v1/conversations/{}", encode_path(&id)))
                .await?;
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
async fn persist_audio_ingress(
    api: &Api,
    piece: &Arc<Mutex<Value>>,
    archive: Value,
) -> anyhow::Result<()> {
    let mut piece = piece.lock().await;
    let id = required_string(&piece, "id")?;
    let mut state = piece.get("state").cloned().unwrap_or_else(|| json!({}));
    state["historyIngress"] = archive;
    let result = match api
        .audio_put(
            &format!(
                "/api/v1/audio-ingress/pieces/{}/ingress-checkpoint",
                encode_path(&id)
            ),
            json!({"expected_version":version(&piece)?,"state":state}),
        )
        .await
    {
        Ok(result) => result,
        Err(error) if error.code == "state_conflict" => {
            let latest = api
                .audio_get(&format!(
                    "/api/v1/audio-ingress/pieces/{}",
                    encode_path(&id)
                ))
                .await?;
            if latest.get("state") == Some(&state) {
                latest
            } else {
                return Err(error.into());
            }
        }
        Err(error) => return Err(error.into()),
    };
    *piece = result;
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
fn required_string(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("backend response omitted {key}"))
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
fn value_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}
fn bounded_error(error: &anyhow::Error) -> String {
    error.to_string().chars().take(1_000).collect()
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
