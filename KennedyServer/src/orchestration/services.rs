//! Typed access to Kennedy's in-process service capabilities.

use std::{collections::HashMap, sync::Arc};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use kcode_kmap::{CreateProvenance, Kmap, NodeContents, NodeWrite};
use kcode_kweb_db::{Node, NodeId, ObjectId, Owner, Provenance};
use kcode_server_object_envelopes::{StoredFile, StoredProvenance, decode_file, encode_file};
use serde_json::Value;
#[cfg(test)]
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::Config;
use super::session::ResolvedObject;

const TELEGRAM_CAPTION_LIMIT_UTF16: usize = 1_024;

#[derive(Clone)]
pub(crate) struct LocalServices {
    pub kmap: Kmap,
    pub intelligence: kcode_intelligence_router::Intelligence,
    pub history: kcode_session_history::SessionHistory,
    pub audio: kcode_audio_session_ingress::Coordinator,
    pub speech_classifier: Arc<kcode_speech_classification::SpeechClassifier>,
    pub directory: std::sync::Arc<kcode_telegram_identity::Directory>,
    pub dev_tools: kcode_dev_tools::Service,
    pub agents: kcode_agent_runtime::AgentRuntime,
    pub telegram: kcode_tg_kennedy_bot::Service,
}

#[derive(Debug, Clone)]
pub(crate) struct ApiError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ApiError {}

#[derive(Clone)]
pub(crate) struct Api {
    services: Arc<LocalServices>,
    user_root_node_id: String,
    kennedy_root_node_id: String,
    telegram_user_locks: Arc<tokio::sync::Mutex<HashMap<i64, Arc<tokio::sync::Mutex<()>>>>>,
    telegram_group_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

pub(crate) struct AgentTurn {
    turn: Box<kcode_intelligence_router::AgentTurn>,
}

impl Api {
    pub fn new(config: &Config, services: LocalServices) -> Self {
        Self {
            services: Arc::new(services),
            user_root_node_id: config.user_root_node_id.clone(),
            kennedy_root_node_id: config.kennedy_root_node_id.clone(),
            telegram_user_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            telegram_group_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn telegram_user_lock(
        &self,
        telegram_user_id: i64,
    ) -> Arc<tokio::sync::Mutex<()>> {
        self.telegram_user_locks
            .lock()
            .await
            .entry(telegram_user_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub(crate) async fn telegram_group_lock(&self, group_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.telegram_group_locks
            .lock()
            .await
            .entry(group_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn telegram(&self) -> &kcode_tg_kennedy_bot::Service {
        &self.services.telegram
    }

    pub(crate) fn create_history_session(
        &self,
        input: kcode_session_history::NewSession,
    ) -> anyhow::Result<kcode_session_history::Session> {
        self.services.history.create_session(input)
    }

    pub(crate) fn history_session(
        &self,
        metadata: kcode_session_history::chatend::SessionMetadata,
        provider_model: &str,
    ) -> anyhow::Result<kcode_session_history::Session> {
        self.services
            .history
            .open_session_with_provider_model(metadata, Some(provider_model))
    }

    pub fn kmap_node(&self, node_id: &str) -> Result<Node, ApiError> {
        let node_id = node_id.parse::<NodeId>().map_err(local_api_error)?;
        self.services.kmap.get_node(node_id).map_err(kmap_error)
    }

    pub(crate) fn user_root_node_id(&self) -> &str {
        &self.user_root_node_id
    }

    pub(crate) fn kennedy_root_node_id(&self) -> &str {
        &self.kennedy_root_node_id
    }

    pub(crate) fn commit_kweb_session(
        &self,
        input: kcode_commit_session::CommitRequest,
    ) -> Result<kcode_commit_session::CommitReceipt, ApiError> {
        self.services.kmap.commit_session(input).map_err(kmap_error)
    }

    pub(crate) fn kmap_file(&self, object_id: &str) -> Result<StoredFile, ApiError> {
        let object_id = object_id.parse::<ObjectId>().map_err(local_api_error)?;
        let bytes = self
            .services
            .kmap
            .get_object(object_id)
            .map_err(kmap_error)?;
        decode_file(object_id, bytes).map_err(local_api_error)
    }

    pub(crate) fn save_generated_image(
        &self,
        bytes: Vec<u8>,
        file_name: &str,
        media_type: &str,
        model: &str,
    ) -> Result<String, ApiError> {
        let bytes = encode_file(
            "generated-image",
            Some(file_name),
            media_type,
            Some("image"),
            bytes,
        )
        .map_err(local_api_error)?;
        self.services
            .kmap
            .store_object(
                Provenance {
                    author: model.into(),
                    source: "kennedy-generated-image".into(),
                    source_created_at: chrono::Utc::now(),
                    data: "Image generated or modified through Kennedy intelligence.".into(),
                },
                bytes,
            )
            .map(|id| id.to_string())
            .map_err(kmap_error)
    }

    pub async fn start_agent_turn(
        &self,
        user_id: &str,
        operation_id: Uuid,
        request: kcode_codex_runtime_v2::AgentRequest,
    ) -> Result<AgentTurn, ApiError> {
        self.services
            .intelligence
            .for_user(user_id)
            .map_err(intelligence_error)?
            .start_agent_turn(operation_id, None, request)
            .await
            .map(|turn| AgentTurn {
                turn: Box::new(turn),
            })
            .map_err(intelligence_error)
    }

    pub(crate) fn agent_runtime(&self) -> kcode_agent_runtime::AgentRuntime {
        self.services.agents.clone()
    }

    pub fn cancel_intelligence(&self, operation_id: Uuid) -> Result<bool, ApiError> {
        self.services
            .intelligence
            .cancel(operation_id)
            .map_err(intelligence_error)
    }

    pub async fn search(
        &self,
        user_id: &str,
        request: kcode_intelligence_router::SearchRequest,
    ) -> Result<kcode_intelligence_router::SearchResponse, ApiError> {
        self.services
            .intelligence
            .for_user(user_id)
            .map_err(intelligence_error)?
            .search(request)
            .await
            .map_err(intelligence_error)
    }

    pub async fn fetch(
        &self,
        user_id: &str,
        request: kcode_intelligence_router::FetchRequest,
    ) -> Result<kcode_intelligence_router::FetchResponse, ApiError> {
        self.services
            .intelligence
            .for_user(user_id)
            .map_err(intelligence_error)?
            .fetch(request)
            .await
            .map_err(intelligence_error)
    }

    pub fn history_health(&self) -> Result<(), ApiError> {
        self.services.history.health().map_err(history_error)
    }

    pub async fn history_list(
        &self,
    ) -> Result<Vec<kcode_session_history::SessionRecord>, ApiError> {
        self.services.history.list().await.map_err(history_error)
    }

    pub async fn history_get_session(
        &self,
        id: &str,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services.history.get(id).await.map_err(history_error)
    }

    pub async fn history_register(
        &self,
        input: kcode_session_history::RegisterSession,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services
            .history
            .register(input)
            .await
            .map_err(history_error)
    }

    pub async fn history_command_heads(
        &self,
    ) -> Result<Vec<kcode_session_history::SessionCommand>, ApiError> {
        self.services
            .history
            .command_heads()
            .await
            .map_err(history_error)
    }

    pub async fn history_claim_command(
        &self,
        id: &str,
    ) -> Result<kcode_session_history::SessionCommand, ApiError> {
        self.services
            .history
            .claim_command(id)
            .await
            .map_err(history_error)
    }

    pub async fn history_complete_command(
        &self,
        id: &str,
        outcome: Value,
    ) -> Result<kcode_session_history::SessionCommand, ApiError> {
        self.services
            .history
            .complete_command(id, kcode_session_history::CommandOutcome { outcome })
            .await
            .map_err(history_error)
    }

    pub async fn history_request_stop(
        &self,
        id: &str,
        input: kcode_session_history::NewStopRequest,
    ) -> Result<kcode_session_history::SessionStopRequest, ApiError> {
        self.services
            .history
            .request_stop(id, input)
            .await
            .map(|created| created.value)
            .map_err(history_error)
    }

    pub async fn history_stop_heads(
        &self,
    ) -> Result<Vec<kcode_session_history::SessionStopRequest>, ApiError> {
        self.services
            .history
            .stop_heads()
            .await
            .map_err(history_error)
    }

    pub async fn history_complete_stop(
        &self,
        id: &str,
        outcome: Value,
    ) -> Result<kcode_session_history::SessionStopRequest, ApiError> {
        self.services
            .history
            .complete_stop(id, kcode_session_history::StopOutcome { outcome })
            .await
            .map_err(history_error)
    }

    pub async fn history_checkpoint(
        &self,
        id: &str,
        input: kcode_session_history::Checkpoint,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services
            .history
            .checkpoint(id, input)
            .await
            .map_err(history_error)
    }

    pub async fn history_request_ingress(
        &self,
        id: &str,
        input: kcode_session_history::Checkpoint,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services
            .history
            .request_ingress(id, input)
            .await
            .map_err(history_error)
    }

    pub async fn history_start_ingress(
        &self,
        id: &str,
        input: kcode_session_history::StartIngress,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services
            .history
            .start_ingress(id, input)
            .await
            .map_err(history_error)
    }

    pub async fn history_complete_ingress(
        &self,
        id: &str,
        expected_version: i64,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services
            .history
            .complete_ingress(
                id,
                kcode_session_history::ExpectedVersion { expected_version },
            )
            .await
            .map_err(history_error)
    }

    pub async fn history_fail_ingress(
        &self,
        id: &str,
        input: kcode_session_history::IngressFailure,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services
            .history
            .fail_ingress(id, input)
            .await
            .map_err(history_error)
    }

    pub async fn history_complete(
        &self,
        id: &str,
        input: kcode_session_history::Checkpoint,
    ) -> Result<kcode_session_history::SessionRecord, ApiError> {
        self.services
            .history
            .complete(id, input)
            .await
            .map_err(history_error)
    }

    pub async fn history_release_interrupted_ingress(&self) -> Result<Vec<String>, ApiError> {
        self.services
            .history
            .release_interrupted_ingress()
            .await
            .map_err(history_error)
    }

    pub fn directory_provisioning_users(
        &self,
    ) -> Result<Vec<kcode_telegram_identity::User>, ApiError> {
        self.services
            .directory
            .provisioning_users()
            .map_err(directory_error)
    }

    pub fn directory_provisioning_groups(
        &self,
    ) -> Result<Vec<kcode_telegram_identity::Group>, ApiError> {
        self.services
            .directory
            .provisioning_groups()
            .map_err(directory_error)
    }

    pub fn directory_user(
        &self,
        telegram_user_id: i64,
    ) -> Result<kcode_telegram_identity::User, ApiError> {
        self.services
            .directory
            .user(telegram_user_id)
            .map_err(directory_error)
    }

    pub fn directory_group(
        &self,
        group_id: &str,
    ) -> Result<kcode_telegram_identity::Group, ApiError> {
        self.services
            .directory
            .group(group_id)
            .map_err(directory_error)
    }

    pub fn directory_group_for_root(
        &self,
        root_node_id: kcode_kweb_db::NodeId,
    ) -> Result<kcode_telegram_identity::Group, ApiError> {
        self.services
            .directory
            .group_for_root(root_node_id)
            .map_err(directory_error)
    }

    pub fn directory_complete_handle_root(
        &self,
        handle: &str,
        root_node_id: kcode_kweb_db::NodeId,
    ) -> Result<kcode_telegram_identity::User, ApiError> {
        self.services
            .directory
            .complete_handle_root(handle, root_node_id)
            .map_err(directory_error)
    }

    pub fn directory_complete_user_root(
        &self,
        telegram_user_id: i64,
        root_node_id: kcode_kweb_db::NodeId,
    ) -> Result<kcode_telegram_identity::User, ApiError> {
        self.services
            .directory
            .complete_user_root(telegram_user_id, root_node_id)
            .map_err(directory_error)
    }

    pub fn directory_complete_group_root(
        &self,
        group_id: &str,
        root_node_id: kcode_kweb_db::NodeId,
    ) -> Result<kcode_telegram_identity::Group, ApiError> {
        self.services
            .directory
            .complete_group_root(group_id, root_node_id)
            .map_err(directory_error)
    }

    pub async fn managed_source_execute(
        &self,
        session_id: &str,
        name: &str,
        arguments: Value,
        objects: Vec<Vec<u8>>,
    ) -> Result<kcode_dev_tools::ToolExecution, ApiError> {
        let mut execution = self
            .services
            .dev_tools
            .execute(session_id.to_owned(), name.to_owned(), arguments, objects)
            .await
            .map_err(dev_tools_error)?;
        let mut object_ids = Vec::with_capacity(execution.objects.len());
        for bytes in std::mem::take(&mut execution.objects) {
            object_ids.push(
                self.services
                    .kmap
                    .store_object(
                        Provenance {
                            author: "Kennedy".into(),
                            source: "kennedy-rust-binary".into(),
                            source_created_at: chrono::Utc::now(),
                            data: "Output payload from a managed Rust-binary call.".into(),
                        },
                        bytes,
                    )
                    .map_err(kmap_error)?
                    .to_string(),
            );
        }
        append_object_ids(&mut execution.text, &object_ids);
        Ok(execution)
    }

    pub async fn execute_speech_classification_tool(
        &self,
        name: &str,
        arguments: Value,
    ) -> Result<String, ApiError> {
        let call = kcode_speech_classification::decode_ktool(name, &arguments)
            .map_err(speech_ktool_error)?;
        let classifier = Arc::clone(&self.services.speech_classifier);
        tokio::task::spawn_blocking(move || classifier.execute_ktool(call))
            .await
            .map_err(speech_task_error)?
            .map_err(speech_ktool_error)
    }

    pub async fn release_managed_sources(&self, session_id: &str) {
        if let Err(error) = self.services.dev_tools.release(session_id.to_owned()).await {
            tracing::warn!(error=%error.message, "Managed-source session release failed");
        }
    }

    pub fn telegram_health(&self) {
        let _ = self.telegram().status();
    }

    pub fn telegram_max_media_bytes(&self) -> u64 {
        self.telegram().status().max_media_bytes as u64
    }

    pub async fn telegram_private_sessions(
        &self,
    ) -> Result<Vec<kcode_tg_kennedy_bot::PrivateSession>, ApiError> {
        self.telegram()
            .list_private_sessions()
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_send_private_message(
        &self,
        telegram_user_id: i64,
        conversation_id: &str,
        expected_conversation_id: Option<&str>,
        text: &str,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .send_private_message(
                telegram_user_id,
                conversation_id.to_owned(),
                expected_conversation_id.map(ToOwned::to_owned),
                text.to_owned(),
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_send_group_message(
        &self,
        group_id: &str,
        text: &str,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .send_group_message(group_id.to_owned(), text.to_owned())
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_events(&self) -> Result<Value, ApiError> {
        self.telegram().list_events().await.map_err(telegram_error)
    }

    pub async fn telegram_group_ingress(&self) -> Result<Value, ApiError> {
        self.telegram()
            .list_group_ingress()
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_complete_group_ingress(&self, batch_id: &str) -> Result<Value, ApiError> {
        self.telegram()
            .complete_group_ingress(batch_id.to_owned())
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_group_session_updates(&self) -> Result<Value, ApiError> {
        self.telegram()
            .list_group_session_updates()
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_complete_silent_group_reset(
        &self,
        conversation_id: &str,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .complete_silent_group_reset(conversation_id.to_owned())
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_acknowledge_group_context(
        &self,
        conversation_id: &str,
        through_message_id: i64,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .acknowledge_group_session_context(conversation_id.to_owned(), through_message_id)
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_detach_group_session(
        &self,
        conversation_id: &str,
        group_id: &str,
        telegram_user_id: i64,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .detach_group_session(
                conversation_id.to_owned(),
                group_id.to_owned(),
                telegram_user_id,
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_save_group_message_preparation(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        model: Option<&str>,
        format: Option<&str>,
        truncated: bool,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .save_group_message_preparation(
                chat_id,
                message_id,
                text.to_owned(),
                model.map(ToOwned::to_owned),
                format.map(ToOwned::to_owned),
                truncated,
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_bind_event(
        &self,
        event_id: &str,
        conversation_id: &str,
        expected_conversation_id: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .bind_event(
                event_id.to_owned(),
                conversation_id.to_owned(),
                expected_conversation_id.map(ToOwned::to_owned),
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_reply_event(
        &self,
        event_id: &str,
        conversation_id: &str,
        text: &str,
        context_warning: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .reply_event(
                event_id.to_owned(),
                conversation_id.to_owned(),
                text.to_owned(),
                context_warning.map(ToOwned::to_owned),
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_abort_event(
        &self,
        event_id: &str,
        conversation_id: Option<&str>,
        message: &str,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .abort_event(
                event_id.to_owned(),
                conversation_id.map(ToOwned::to_owned),
                message.to_owned(),
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_interrupt_event(
        &self,
        event_id: &str,
        conversation_id: &str,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .interrupt_event(event_id.to_owned(), conversation_id.to_owned())
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_complete_reset(
        &self,
        event_id: &str,
        message: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .complete_reset(event_id.to_owned(), message.map(ToOwned::to_owned))
            .await
            .map_err(telegram_error)
    }

    pub fn telegram_event_media(&self, event_id: &str) -> Result<(Vec<u8>, String), ApiError> {
        self.telegram()
            .event_media(event_id)
            .map(|media| (media.bytes, media.media_type))
            .map_err(telegram_error)
    }

    pub fn telegram_group_message_media(
        &self,
        chat_id: i64,
        message_id: i64,
    ) -> Result<(Vec<u8>, String), ApiError> {
        self.telegram()
            .group_message_media(chat_id, message_id)
            .map(|media| (media.bytes, media.media_type))
            .map_err(telegram_error)
    }

    pub fn telegram_group_message_media_metadata(
        &self,
        chat_id: i64,
        message_id: i64,
    ) -> Result<(u64, String), ApiError> {
        self.telegram()
            .group_message_media_metadata(chat_id, message_id)
            .map(|media| (media.size_bytes, media.media_type))
            .map_err(telegram_error)
    }

    pub async fn telegram_send_object(
        &self,
        event_id: &str,
        conversation_id: &str,
        file: &ResolvedObject,
        caption: Option<&str>,
        complete: bool,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .send_event_attachment(
                event_id.to_owned(),
                conversation_id.to_owned(),
                telegram_attachment(file, caption),
                complete,
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_send_private_object(
        &self,
        telegram_user_id: i64,
        conversation_id: &str,
        expected_conversation_id: Option<&str>,
        file: &ResolvedObject,
        caption: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .send_private_attachment(
                telegram_user_id,
                conversation_id.to_owned(),
                expected_conversation_id.map(ToOwned::to_owned),
                telegram_attachment(file, caption),
            )
            .await
            .map_err(telegram_error)
    }

    pub async fn telegram_send_group_object(
        &self,
        group_id: &str,
        file: &ResolvedObject,
        caption: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.telegram()
            .send_group_attachment(group_id.to_owned(), telegram_attachment(file, caption))
            .await
            .map_err(telegram_error)
    }

    pub fn bootstrap_node(&self, short_name: Option<&str>) -> Result<Node, ApiError> {
        let (short_name, short_description, long_description) = bootstrap_root_metadata(short_name);
        let source_created_at = chrono::Utc::now();
        let kmap = &self.services.kmap;
        let provenance_id = kmap
            .create_provenance(CreateProvenance {
                idempotency_id: idempotency_id(),
                value: StoredProvenance {
                    data: "Automatically provisioned Kmap root node.".into(),
                    source: "system-bootstrap".into(),
                    source_created_at,
                    artifacts: Vec::new(),
                },
                storage_provenance: Provenance {
                    author: "kennedy-provenance".into(),
                    source: "system-bootstrap".into(),
                    source_created_at,
                    data: "Stored provenance for a Kennedy Kmap mutation.".into(),
                },
            })
            .map_err(kmap_error)?;
        kmap.create_node(NodeWrite {
            idempotency_id: idempotency_id(),
            provenance_id,
            author: "system-bootstrap".into(),
            contents: NodeContents {
                short_name: short_name.into(),
                short_description: short_description.into(),
                long_description: long_description.into(),
                owner: Owner::SelfNode,
                fixed_connections: Vec::new(),
                recent_connections: Vec::new(),
            },
        })
        .map_err(kmap_error)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transcribe_audio(
        &self,
        user_id: &str,
        model: &str,
        prompt: &str,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
        parent_operation_id: Uuid,
    ) -> Result<kcode_intelligence_router::TranscriptionResponse, ApiError> {
        self.services
            .intelligence
            .for_user(user_id)
            .map_err(intelligence_error)?
            .transcribe(kcode_intelligence_router::TranscriptionRequest {
                prompt: prompt.to_owned(),
                model: model.to_owned(),
                media: kcode_intelligence_router::Media::audio(bytes, filename, mime)
                    .map_err(intelligence_error)?,
                operation_id: Uuid::new_v4(),
                parent_operation_id: Some(parent_operation_id),
            })
            .await
            .map_err(intelligence_error)
    }

    pub async fn extract_document(
        &self,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
    ) -> Result<kcode_intelligence_router::DocumentExtraction, ApiError> {
        self.services
            .intelligence
            .extract_document(kcode_intelligence_router::Document {
                bytes,
                file_name: filename,
                content_type: mime.to_owned(),
            })
            .await
            .map_err(intelligence_error)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn annotate_media(
        &self,
        user_id: &str,
        model: &str,
        prompt: &str,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
        parent_operation_id: Uuid,
    ) -> Result<kcode_intelligence_router::AnnotationResponse, ApiError> {
        self.services
            .intelligence
            .for_user(user_id)
            .map_err(intelligence_error)?
            .annotate(kcode_intelligence_router::AnnotationRequest {
                prompt: prompt.to_owned(),
                model: model.to_owned(),
                media: media_for_annotation(bytes, filename, mime).map_err(intelligence_error)?,
                operation_id: Uuid::new_v4(),
                parent_operation_id: Some(parent_operation_id),
            })
            .await
            .map_err(intelligence_error)
    }

    pub async fn generate_image(
        &self,
        user_id: &str,
        model: &str,
        prompt: &str,
        references: Vec<(Vec<u8>, String, String)>,
        parent_operation_id: Uuid,
    ) -> Result<kcode_intelligence_router::ImageResponse, ApiError> {
        let references = references
            .into_iter()
            .map(|(bytes, filename, mime)| {
                media_for_image(bytes, filename, &mime).map_err(intelligence_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.services
            .intelligence
            .for_user(user_id)
            .map_err(intelligence_error)?
            .generate_image(kcode_intelligence_router::ImageRequest {
                model: model.to_owned(),
                prompt: prompt.to_owned(),
                references,
                operation_id: Uuid::new_v4(),
                parent_operation_id: Some(parent_operation_id),
            })
            .await
            .map_err(intelligence_error)
    }

    pub async fn synchronize_audio_ingress(&self) -> Result<(), ApiError> {
        self.services
            .audio
            .synchronize_completed_transcripts()
            .await
            .map_err(audio_error)
    }
}

fn bootstrap_root_metadata(short_name: Option<&str>) -> (&str, &'static str, &'static str) {
    match short_name {
        Some("Group Root") => (
            "Group Root",
            "The root of this Telegram group's shared Kmap knowledge.",
            "This root anchors durable knowledge shared in this Telegram group.",
        ),
        Some(short_name) => (
            short_name,
            "An automatically provisioned Kmap root.",
            "This root anchors durable Kmap knowledge.",
        ),
        None => (
            "User Root",
            "The root of this Telegram user's Kmap knowledge.",
            "This root anchors durable knowledge associated with this Telegram user.",
        ),
    }
}

impl AgentTurn {
    pub(crate) async fn next_event(
        &mut self,
    ) -> Option<Result<kcode_codex_runtime_v2::AgentEvent, ApiError>> {
        match self.turn.next_event().await {
            Ok(event) => event.map(Ok),
            Err(error) => Some(Err(intelligence_error(error))),
        }
    }

    pub(crate) async fn respond(
        &mut self,
        call_id: &str,
        result: kcode_codex_runtime_v2::ToolResult,
    ) -> Result<(), ApiError> {
        self.turn
            .respond(call_id, result)
            .await
            .map_err(intelligence_error)
    }
}

fn kmap_error(error: kcode_kmap::Error) -> ApiError {
    let (code, message) = match error.kind() {
        kcode_kmap::ErrorKind::InvalidInput => ("invalid_request", error.to_string()),
        kcode_kmap::ErrorKind::NotFound => ("not_found", error.to_string()),
        kcode_kmap::ErrorKind::Conflict => ("conflict", error.to_string()),
        _ => (
            "internal_error",
            "An unexpected Kmap database error occurred.".into(),
        ),
    };
    ApiError {
        code: code.into(),
        message,
    }
}

fn intelligence_error(error: kcode_intelligence_router::Error) -> ApiError {
    ApiError {
        code: error.code().into(),
        message: error.message().into(),
    }
}

fn directory_error(error: kcode_telegram_identity::Error) -> ApiError {
    let code = match error.kind() {
        kcode_telegram_identity::ErrorKind::InvalidInput => "invalid_request",
        kcode_telegram_identity::ErrorKind::NotFound => "not_found",
        kcode_telegram_identity::ErrorKind::Conflict => "state_conflict",
        kcode_telegram_identity::ErrorKind::Storage => "internal_error",
    };
    ApiError {
        code: code.into(),
        message: error.message().into(),
    }
}

fn append_object_ids(text: &mut String, object_ids: &[String]) {
    if object_ids.is_empty() {
        return;
    }
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&object_ids.join("\n"));
}

fn dev_tools_error(error: kcode_dev_tools::ToolError) -> ApiError {
    ApiError {
        code: error.code.into(),
        message: error.message,
    }
}

fn history_error(error: kcode_session_history::Error) -> ApiError {
    ApiError {
        code: error.kind.code().into(),
        message: error.message,
    }
}

fn audio_error(error: kcode_audio_session_ingress::Error) -> ApiError {
    let internal = error.kind() == kcode_audio_session_ingress::ErrorKind::Internal;
    ApiError {
        code: match error.kind() {
            kcode_audio_session_ingress::ErrorKind::InvalidInput => "invalid_request",
            kcode_audio_session_ingress::ErrorKind::NotFound => "not_found",
            kcode_audio_session_ingress::ErrorKind::Conflict => "state_conflict",
            kcode_audio_session_ingress::ErrorKind::Internal => "internal_error",
        }
        .into(),
        message: if internal {
            "An unexpected Kennedy audio error occurred.".into()
        } else {
            error.message().into()
        },
    }
}

fn speech_task_error(error: tokio::task::JoinError) -> ApiError {
    tracing::error!(%error, "In-process speaker-classification task stopped unexpectedly");
    ApiError {
        code: "internal_error".into(),
        message: "An unexpected Kennedy speaker-classification error occurred.".into(),
    }
}

fn speech_ktool_error(error: kcode_speech_classification::KtoolError) -> ApiError {
    let kcode_speech_classification::KtoolError::Classifier(error) = error else {
        return ApiError {
            code: "invalid_request".into(),
            message: error.to_string(),
        };
    };
    let internal = matches!(
        error,
        kcode_speech_classification::Error::UnsupportedSchema { .. }
            | kcode_speech_classification::Error::Storage(_)
            | kcode_speech_classification::Error::CorruptStorage(_)
    );
    ApiError {
        code: match &error {
            kcode_speech_classification::Error::Validation { .. } => "invalid_request",
            kcode_speech_classification::Error::Conflict { .. } => "state_conflict",
            kcode_speech_classification::Error::UnsupportedSchema { .. }
            | kcode_speech_classification::Error::Storage(_)
            | kcode_speech_classification::Error::CorruptStorage(_) => "internal_error",
        }
        .into(),
        message: if internal {
            tracing::error!(%error, "Speaker-classification storage failed");
            "An unexpected Kennedy speaker-classification error occurred.".into()
        } else {
            error.to_string()
        },
    }
}

fn local_api_error(error: impl std::fmt::Display) -> ApiError {
    ApiError {
        code: "invalid_request".into(),
        message: error.to_string(),
    }
}

fn telegram_error(error: kcode_tg_kennedy_bot::Error) -> ApiError {
    ApiError {
        code: error.code().to_owned(),
        message: error.message().to_owned(),
    }
}

fn telegram_attachment(
    file: &ResolvedObject,
    caption: Option<&str>,
) -> kcode_tg_kennedy_bot::Attachment {
    kcode_tg_kennedy_bot::Attachment {
        bytes: file.bytes.clone(),
        file_name: Some(file.file_name.clone()),
        media_type: Some(file.media_type.clone()),
        kind: telegram_native_kind(&file.media_type, file.transport_kind.as_deref())
            .map(ToOwned::to_owned),
        caption: caption.map(ToOwned::to_owned),
    }
}

pub(crate) fn telegram_caption_for<'a>(file: &ResolvedObject, text: &'a str) -> Option<&'a str> {
    if text.is_empty() || text.encode_utf16().count() > TELEGRAM_CAPTION_LIMIT_UTF16 {
        return None;
    }
    if matches!(
        telegram_native_kind(&file.media_type, file.transport_kind.as_deref()),
        Some("video_note" | "sticker")
    ) {
        return None;
    }
    Some(text)
}

pub(crate) fn idempotency_id() -> String {
    Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
pub(crate) fn stable_idempotency_id(namespace: &str, value: &str) -> String {
    let digest = Sha256::digest(format!("{namespace}\0{value}").as_bytes());
    hex::encode(&digest[..16])
}

pub(crate) fn data_url(mime: &str, bytes: &[u8]) -> String {
    format!("data:{mime};base64,{}", BASE64.encode(bytes))
}

fn media_for_annotation(
    bytes: Vec<u8>,
    filename: String,
    mime: &str,
) -> kcode_intelligence_router::Result<kcode_intelligence_router::Media> {
    let normalized = mime
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_ascii_lowercase();
    let kind = if normalized.starts_with("image/") {
        kcode_intelligence_router::MediaKind::Image
    } else if normalized.starts_with("audio/")
        || matches!(normalized.as_str(), "application/ogg" | "video/ogg")
        || filename.rsplit_once('.').is_some_and(|(_, extension)| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "ogg" | "oga" | "opus"
            )
        })
    {
        kcode_intelligence_router::MediaKind::Audio
    } else if normalized.starts_with("video/") {
        kcode_intelligence_router::MediaKind::Video
    } else {
        return Err(kcode_intelligence_router::Error::invalid(
            "annotation requires image, audio, or video media",
        ));
    };
    kcode_intelligence_router::Media::new(kind, bytes, filename, normalized)
}

fn media_for_image(
    bytes: Vec<u8>,
    filename: String,
    mime: &str,
) -> kcode_intelligence_router::Result<kcode_intelligence_router::Media> {
    let normalized = mime
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_ascii_lowercase();
    if !normalized.starts_with("image/") {
        return Err(kcode_intelligence_router::Error::invalid(
            "image references must use an image content type",
        ));
    }
    kcode_intelligence_router::Media::new(
        kcode_intelligence_router::MediaKind::Image,
        bytes,
        filename,
        normalized,
    )
}

fn telegram_native_kind(media_type: &str, transport_kind: Option<&str>) -> Option<&'static str> {
    match transport_kind {
        Some("photo") => return Some("photo"),
        Some("video") => return Some("video"),
        Some("animation") => return Some("animation"),
        Some("audio") => return Some("audio"),
        Some("video_note") => return Some("video_note"),
        Some("sticker") => return Some("sticker"),
        _ => {}
    }
    if media_type == "image/gif" {
        Some("animation")
    } else if media_type.starts_with("image/") {
        Some("photo")
    } else if media_type.starts_with("video/") {
        Some("video")
    } else if media_type.starts_with("audio/") {
        Some("audio")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_output_ids_preserve_exact_guest_text() {
        let mut text = "plain text".to_owned();
        append_object_ids(&mut text, &[]);
        assert_eq!(text, "plain text");

        append_object_ids(&mut text, &["object-1".into()]);
        assert_eq!(text, "plain text\nobject-1");

        let mut text = "already terminated\n".to_owned();
        append_object_ids(&mut text, &["one".into(), "two".into()]);
        assert_eq!(text, "already terminated\none\ntwo");
    }

    #[test]
    fn known_ogg_audio_is_not_misclassified_as_video() {
        let media = media_for_annotation(vec![1], "voice.ogg".into(), "video/ogg").unwrap();
        assert_eq!(media.kind, kcode_intelligence_router::MediaKind::Audio);
        assert_eq!(media.content_type, "audio/ogg");
    }

    #[test]
    fn image_generation_references_require_image_media() {
        let media =
            media_for_image(vec![1], "reference.png".into(), "image/png; charset=binary").unwrap();
        assert_eq!(media.kind, kcode_intelligence_router::MediaKind::Image);
        assert_eq!(media.content_type, "image/png");
        assert!(media_for_image(vec![1], "notes.txt".into(), "text/plain").is_err());
    }

    #[test]
    fn speech_ktool_errors_keep_the_existing_public_failure_boundary() {
        let malformed =
            speech_ktool_error(kcode_speech_classification::KtoolError::InvalidArguments {
                tool: kcode_speech_classification::IDENTIFY_TOOL,
                source: serde_json::from_str::<Value>("{").unwrap_err(),
            });
        assert_eq!(malformed.code, "invalid_request");
        assert_eq!(
            malformed.message,
            "decoding kcode-speech-classification/identify arguments"
        );

        let validation = speech_ktool_error(kcode_speech_classification::KtoolError::Classifier(
            kcode_speech_classification::Error::Validation {
                field: "row.perceived_age".into(),
                message: "must be positive".into(),
            },
        ));
        assert_eq!(validation.code, "invalid_request");
        assert_eq!(validation.message, "row.perceived_age: must be positive");

        let storage = speech_ktool_error(kcode_speech_classification::KtoolError::Classifier(
            kcode_speech_classification::Error::Storage("private detail".into()),
        ));
        assert_eq!(storage.code, "internal_error");
        assert_eq!(
            storage.message,
            "An unexpected Kennedy speaker-classification error occurred."
        );
    }

    #[test]
    fn durable_work_uses_stable_valid_idempotency_ids() {
        let first = stable_idempotency_id("audio-ingress", "piece-1");
        assert_eq!(first, stable_idempotency_id("audio-ingress", "piece-1"));
        assert_ne!(
            first,
            stable_idempotency_id("conversation-ingress", "piece-1")
        );
        assert_eq!(first.len(), 32);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn provisioned_roots_have_complete_context_summaries() {
        let (user_name, user_description, user_long_description) = bootstrap_root_metadata(None);
        assert_eq!(user_name, "User Root");
        assert!(!user_description.trim().is_empty());
        assert!(!user_long_description.trim().is_empty());

        let (group_name, group_description, group_long_description) =
            bootstrap_root_metadata(Some("Group Root"));
        assert_eq!(group_name, "Group Root");
        assert!(!group_description.trim().is_empty());
        assert!(!group_long_description.trim().is_empty());

        let (custom_name, custom_description, custom_long_description) =
            bootstrap_root_metadata(Some("Custom Root"));
        assert_eq!(custom_name, "Custom Root");
        assert!(!custom_description.trim().is_empty());
        assert!(!custom_long_description.trim().is_empty());
    }

    #[test]
    fn telegram_native_delivery_prefers_preserved_transport_kind() {
        assert_eq!(
            telegram_native_kind("image/webp", Some("sticker")),
            Some("sticker")
        );
        assert_eq!(
            telegram_native_kind("video/mp4", Some("video_note")),
            Some("video_note")
        );
        assert_eq!(telegram_native_kind("image/gif", None), Some("animation"));
        assert_eq!(
            telegram_native_kind("application/pdf", Some("document")),
            None
        );
    }

    #[test]
    fn telegram_captions_are_exact_and_fall_back_when_telegram_cannot_attach_them() {
        let file = |media_type: &str, transport_kind: Option<&str>| ResolvedObject {
            object_id: "object".into(),
            bytes: vec![1],
            file_name: "object.bin".into(),
            media_type: media_type.into(),
            transport_kind: transport_kind.map(ToOwned::to_owned),
        };
        let exact = "  exact caption\n";
        assert_eq!(
            telegram_caption_for(&file("image/jpeg", Some("photo")), exact),
            Some(exact)
        );
        assert_eq!(
            telegram_caption_for(&file("application/pdf", Some("document")), exact),
            Some(exact)
        );
        assert_eq!(
            telegram_caption_for(&file("image/webp", Some("sticker")), exact),
            None
        );
        assert_eq!(
            telegram_caption_for(&file("video/mp4", Some("video_note")), exact),
            None
        );
        assert_eq!(
            telegram_caption_for(
                &file("image/jpeg", Some("photo")),
                &"x".repeat(TELEGRAM_CAPTION_LIMIT_UTF16 + 1),
            ),
            None
        );
    }
}
