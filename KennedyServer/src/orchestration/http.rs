#[cfg(test)]
use std::collections::VecDeque;
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use kcode_kmap::{CreateProvenance, Kmap, NodeContents, NodeWrite};
use kcode_kweb_db::{Node, NodeId, ObjectId, Owner, Provenance};
use kcode_server_object_envelopes::{StoredFile, StoredProvenance, decode_file, encode_file};
use reqwest::multipart;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
#[cfg(test)]
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::Config;
use super::session::ResolvedObject;

const TELEGRAM_STARTUP_RETRY: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub(crate) struct LocalServices {
    pub kmap: Kmap,
    pub intelligence: kcode_intelligence_router::Intelligence,
    pub history: kcode_session_history::SessionHistory,
    pub audio: crate::audio_ingress::Service,
    pub directory: std::sync::Arc<kcode_telegram_identity::Directory>,
    pub dev_tools: kcode_dev_tools::Service,
    pub agents: kcode_agent_runtime::AgentRuntime,
}

#[derive(Debug, Clone)]
pub(crate) struct ApiError {
    #[allow(dead_code)] // Retained for transport diagnostics and the test HTTP backend.
    pub status: Option<StatusCode>,
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
    client: Client,
    services: ServiceBackend,
    history_sessions: kcode_session_history::SessionHistory,
    telegram: String,
    user_root_node_id: String,
    kennedy_root_node_id: String,
    telegram_user_locks: Arc<tokio::sync::Mutex<HashMap<i64, Arc<tokio::sync::Mutex<()>>>>>,
}

#[derive(Clone)]
enum ServiceBackend {
    Local(std::sync::Arc<LocalServices>),
    #[cfg(test)]
    Http(TestBases),
}

enum AgentTurnBackend {
    Local(Box<kcode_intelligence_router::AgentTurn>),
    #[cfg(test)]
    Http(HttpAgentTurn),
}

pub(crate) struct AgentTurn {
    backend: AgentTurnBackend,
}

#[cfg(test)]
pub(crate) struct HttpAgentTurn {
    events: VecDeque<Result<kcode_codex_runtime_v2::AgentEvent, ApiError>>,
}

#[cfg(test)]
#[derive(Clone)]
struct TestBases {
    kweb: String,
    intelligence: String,
    history: String,
}

impl Api {
    #[cfg(test)]
    pub fn new(config: &Config) -> anyhow::Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("building reqwest client")?;
        let history_root = std::env::temp_dir().join(format!(
            "kennedy-orchestration-history-tests-{}",
            Uuid::new_v4()
        ));
        let history_sessions =
            kcode_session_history::SessionHistory::open(kcode_session_history::Config {
                directory: history_root.join("sessions"),
                completed_list: history_root.join("completed.jsonl"),
            })
            .context("opening test Session History")?;
        Ok(Self {
            client,
            services: ServiceBackend::Http(TestBases {
                kweb: trim_base(&config.kweb_base),
                intelligence: trim_base(&config.intelligence_base),
                history: trim_base(&config.session_history_base),
            }),
            history_sessions,
            telegram: trim_base(&config.telegram_relay_base),
            user_root_node_id: config.user_root_node_id.clone(),
            kennedy_root_node_id: config.kennedy_root_node_id.clone(),
            telegram_user_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    pub fn local(config: &Config, services: LocalServices) -> anyhow::Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("building Telegram relay HTTP client")?;
        let history_sessions = services.history.clone();
        Ok(Self {
            client,
            services: ServiceBackend::Local(std::sync::Arc::new(services)),
            history_sessions,
            telegram: trim_base(&config.telegram_relay_base),
            user_root_node_id: config.user_root_node_id.clone(),
            kennedy_root_node_id: config.kennedy_root_node_id.clone(),
            telegram_user_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
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

    pub(crate) fn create_history_session(
        &self,
        input: kcode_session_history::NewSession,
    ) -> anyhow::Result<kcode_session_history::Session> {
        self.history_sessions.create_session(input)
    }

    pub(crate) fn history_session(
        &self,
        metadata: kcode_session_history::chatend::SessionMetadata,
    ) -> anyhow::Result<kcode_session_history::Session> {
        self.history_sessions.open_session(metadata)
    }

    pub fn kmap_node(&self, node_id: &str) -> Result<Node, ApiError> {
        let node_id = node_id.parse::<NodeId>().map_err(local_api_error)?;
        self.local_kmap()?.get_node(node_id).map_err(kmap_error)
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
        self.local_kmap()?.commit_session(input).map_err(kmap_error)
    }

    pub(crate) fn kmap_file(&self, object_id: &str) -> Result<StoredFile, ApiError> {
        let object_id = object_id.parse::<ObjectId>().map_err(local_api_error)?;
        let bytes = self
            .local_kmap()?
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
        self.local_kmap()?
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
        match &self.services {
            ServiceBackend::Local(local) => local
                .intelligence
                .for_user(user_id)
                .map_err(intelligence_error)?
                .start_agent_turn(operation_id, None, request)
                .await
                .map(|turn| AgentTurn {
                    backend: AgentTurnBackend::Local(Box::new(turn)),
                })
                .map_err(intelligence_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let body = json!({
                    "provider":"primary",
                    "model":request.model,
                    "chatend":request.input,
                    "previous_response_id":request.previous_thread_id,
                    "timeout_seconds":request.timeout.as_secs(),
                });
                let exact = format!(
                    "{}\n",
                    serde_json::to_string(&body).expect("JSON values always serialize")
                );
                let response = self
                    .request(
                        Method::POST,
                        &bases.intelligence,
                        "/api/v1/generate",
                        Some(body),
                    )
                    .await?;
                let content = response
                    .pointer("/message/content")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let mut events =
                    VecDeque::from([Ok(kcode_codex_runtime_v2::AgentEvent::ProviderInput(exact))]);
                let calls = response
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|call| {
                        Some((
                            call.get("name")?.as_str()?.to_owned(),
                            call.get("arguments").cloned().unwrap_or_else(|| json!({})),
                        ))
                    })
                    .collect::<Vec<_>>();
                for (index, (name, arguments)) in calls.iter().enumerate() {
                    events.push_back(Ok(kcode_codex_runtime_v2::AgentEvent::ToolCall(
                        kcode_codex_runtime_v2::DynamicToolCall {
                            call_id: format!("test-call-{index}"),
                            tool: "call_ktool".into(),
                            arguments: json!({"name":name,"arguments":arguments}),
                        },
                    )));
                }
                let usage = response
                    .get("usage")
                    .map(|usage| kcode_codex_runtime_v2::TokenUsage {
                        input_tokens: usage
                            .get("input_tokens")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                        output_tokens: usage
                            .get("output_tokens")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                        cached_input_tokens: usage
                            .get("cached_tokens")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                        reasoning_output_tokens: usage
                            .get("reasoning_tokens")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                        last_input_tokens: usage.get("last_input_tokens").and_then(Value::as_u64),
                        last_output_tokens: usage.get("last_output_tokens").and_then(Value::as_u64),
                    });
                if let Some(usage) = usage.clone() {
                    events.push_back(Ok(kcode_codex_runtime_v2::AgentEvent::UsageUpdated(usage)));
                }
                events.push_back(Ok(kcode_codex_runtime_v2::AgentEvent::Completed(
                    kcode_codex_runtime_v2::CompletedTurn {
                        thread_id: response
                            .get("response_id")
                            .and_then(Value::as_str)
                            .unwrap_or("00000000-0000-0000-0000-000000000000")
                            .to_owned(),
                        turn_id: Uuid::new_v4().to_string(),
                        answer: if calls.is_empty() {
                            content.to_owned()
                        } else {
                            String::new()
                        },
                        usage,
                    },
                )));
                Ok(AgentTurn {
                    backend: AgentTurnBackend::Http(HttpAgentTurn { events }),
                })
            }
        }
    }

    pub(crate) fn agent_runtime(&self) -> Result<kcode_agent_runtime::AgentRuntime, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => Ok(local.agents.clone()),
            #[cfg(test)]
            ServiceBackend::Http(_) => Err(ApiError {
                status: None,
                code: "local_service_unavailable".into(),
                message: "Subagents require the in-process agent runtime.".into(),
            }),
        }
    }

    pub fn cancel_intelligence(&self, operation_id: Uuid) -> Result<bool, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .intelligence
                .cancel(operation_id)
                .map_err(intelligence_error),
            #[cfg(test)]
            ServiceBackend::Http(_) => Ok(false),
        }
    }

    pub async fn search(
        &self,
        user_id: &str,
        request: kcode_intelligence_router::SearchRequest,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let response = local
                    .intelligence
                    .for_user(user_id)
                    .map_err(intelligence_error)?
                    .search(request)
                    .await
                    .map_err(intelligence_error)?;
                serde_json::to_value(response).map_err(local_api_error)
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.intelligence,
                    "/api/v1/web/search",
                    Some(json!({
                        "question": request.question,
                        "model": request.model,
                        "operation_id": request.operation_id,
                        "parent_operation_id": request.parent_operation_id,
                    })),
                )
                .await
            }
        }
    }

    pub async fn fetch(
        &self,
        user_id: &str,
        request: kcode_intelligence_router::FetchRequest,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let response = local
                    .intelligence
                    .for_user(user_id)
                    .map_err(intelligence_error)?
                    .fetch(request)
                    .await
                    .map_err(intelligence_error)?;
                serde_json::to_value(response).map_err(local_api_error)
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.intelligence,
                    "/api/v1/web/fetch",
                    Some(json!({
                        "url": request.url,
                        "operation_id": request.operation_id,
                        "parent_operation_id": request.parent_operation_id,
                    })),
                )
                .await
            }
        }
    }

    pub async fn history_health(&self) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                local.history.health().map_err(history_error)?;
                Ok(json!({"service":"session-history","status":"ok"}))
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::GET,
                    &bases.history,
                    "/api/v1/conversations/health",
                    None,
                )
                .await
            }
        }
    }

    pub async fn history_list(&self) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let conversations = local.history.list().await.map_err(history_error)?;
                Ok(json!({"conversations":conversations}))
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::GET,
                    &bases.history,
                    "/api/v1/conversations/summaries",
                    None,
                )
                .await
            }
        }
    }

    pub async fn history_get_session(&self, id: &str) -> Result<Value, ApiError> {
        #[cfg(test)]
        let path = format!("/api/v1/conversations/{}", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .get(id)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(Method::GET, &bases.history, &path, None).await
            }
        }
    }

    pub async fn history_register(
        &self,
        input: kcode_session_history::RegisterSession,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .register(input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    "/api/v1/conversations",
                    Some(json_value(input)?),
                )
                .await
            }
        }
    }

    pub async fn history_command_heads(&self) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let commands = local.history.command_heads().await.map_err(history_error)?;
                Ok(json!({"commands":commands}))
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::GET,
                    &bases.history,
                    "/api/v1/conversation-commands",
                    None,
                )
                .await
            }
        }
    }

    pub async fn history_claim_command(&self, id: &str) -> Result<Value, ApiError> {
        #[cfg(test)]
        let path = format!("/api/v1/conversation-commands/{}/claim", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .claim_command(id)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(Method::POST, &bases.history, &path, Some(json!({})))
                    .await
            }
        }
    }

    pub async fn history_complete_command(
        &self,
        id: &str,
        outcome: Value,
    ) -> Result<Value, ApiError> {
        let input = kcode_session_history::CommandOutcome { outcome };
        #[cfg(test)]
        let path = format!("/api/v1/conversation-commands/{}/complete", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .complete_command(id, input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    &path,
                    Some(json_value(input)?),
                )
                .await
            }
        }
    }

    pub async fn history_checkpoint(
        &self,
        id: &str,
        input: kcode_session_history::Checkpoint,
        _ingress: bool,
    ) -> Result<Value, ApiError> {
        #[cfg(test)]
        let action = if _ingress {
            "ingress-checkpoint"
        } else {
            "checkpoint"
        };
        #[cfg(test)]
        let path = format!("/api/v1/conversations/{}/{action}", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .checkpoint(id, input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(Method::PUT, &bases.history, &path, Some(json_value(input)?))
                    .await
            }
        }
    }

    pub async fn history_request_ingress(
        &self,
        id: &str,
        input: kcode_session_history::Checkpoint,
    ) -> Result<Value, ApiError> {
        #[cfg(test)]
        let path = format!("/api/v1/conversations/{}/request-ingress", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .request_ingress(id, input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    &path,
                    Some(json_value(input)?),
                )
                .await
            }
        }
    }

    pub async fn history_start_ingress(
        &self,
        id: &str,
        input: kcode_session_history::StartIngress,
    ) -> Result<Value, ApiError> {
        #[cfg(test)]
        let path = format!("/api/v1/conversations/{}/ingress-started", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .start_ingress(id, input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    &path,
                    Some(json_value(input)?),
                )
                .await
            }
        }
    }

    pub async fn history_complete_ingress(
        &self,
        id: &str,
        expected_version: i64,
    ) -> Result<Value, ApiError> {
        let input = kcode_session_history::ExpectedVersion { expected_version };
        #[cfg(test)]
        let path = format!(
            "/api/v1/conversations/{}/ingress-completed",
            encode_path(id)
        );
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .complete_ingress(id, input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    &path,
                    Some(json_value(input)?),
                )
                .await
            }
        }
    }

    pub async fn history_fail_ingress(
        &self,
        id: &str,
        input: kcode_session_history::IngressFailure,
    ) -> Result<Value, ApiError> {
        #[cfg(test)]
        let path = format!("/api/v1/conversations/{}/ingress-failure", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .fail_ingress(id, input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    &path,
                    Some(json_value(input)?),
                )
                .await
            }
        }
    }

    pub async fn history_complete(
        &self,
        id: &str,
        input: kcode_session_history::Checkpoint,
    ) -> Result<Value, ApiError> {
        #[cfg(test)]
        let path = format!("/api/v1/conversations/{}/complete", encode_path(id));
        match &self.services {
            ServiceBackend::Local(local) => local
                .history
                .complete(id, input)
                .await
                .map_err(history_error)
                .and_then(json_value),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    &path,
                    Some(json_value(input)?),
                )
                .await
            }
        }
    }

    pub async fn history_release_interrupted_ingress(&self) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let released = local
                    .history
                    .release_interrupted_ingress()
                    .await
                    .map_err(history_error)?;
                Ok(json!({"released":released}))
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    "/api/v1/conversations/ingress/repairs/release",
                    Some(json!({})),
                )
                .await
            }
        }
    }

    pub async fn directory_provisioning_users(
        &self,
    ) -> Result<Vec<kcode_telegram_identity::User>, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .directory
                .provisioning_users()
                .map_err(directory_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let value = self
                    .request(
                        Method::GET,
                        &bases.kweb,
                        "/api/v1/telegram-directory/users/provisioning",
                        None,
                    )
                    .await?;
                serde_json::from_value(value.get("users").cloned().unwrap_or_else(|| json!([])))
                    .map_err(local_api_error)
            }
        }
    }

    pub async fn directory_provisioning_groups(
        &self,
    ) -> Result<Vec<kcode_telegram_identity::Group>, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .directory
                .provisioning_groups()
                .map_err(directory_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let value = self
                    .request(
                        Method::GET,
                        &bases.kweb,
                        "/api/v1/telegram-directory/groups/provisioning",
                        None,
                    )
                    .await?;
                serde_json::from_value(value.get("groups").cloned().unwrap_or_else(|| json!([])))
                    .map_err(local_api_error)
            }
        }
    }

    pub async fn directory_user(
        &self,
        telegram_user_id: i64,
    ) -> Result<kcode_telegram_identity::User, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .directory
                .user(telegram_user_id)
                .map_err(directory_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => self
                .request(
                    Method::GET,
                    &bases.kweb,
                    &format!(
                        "/api/v1/telegram-directory/users/{}",
                        encode_path(telegram_user_id)
                    ),
                    None,
                )
                .await
                .and_then(|value| serde_json::from_value(value).map_err(local_api_error)),
        }
    }

    pub async fn directory_group(
        &self,
        group_id: &str,
    ) -> Result<kcode_telegram_identity::Group, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                local.directory.group(group_id).map_err(directory_error)
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => self
                .request(
                    Method::GET,
                    &bases.kweb,
                    &format!(
                        "/api/v1/telegram-directory/groups/{}",
                        encode_path(group_id)
                    ),
                    None,
                )
                .await
                .and_then(|value| serde_json::from_value(value).map_err(local_api_error)),
        }
    }

    pub async fn directory_complete_handle_root(
        &self,
        handle: &str,
        root_node_id: kcode_kweb_db::NodeId,
    ) -> Result<kcode_telegram_identity::User, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .directory
                .complete_handle_root(handle, root_node_id)
                .map_err(directory_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => self
                .request(
                    Method::POST,
                    &bases.kweb,
                    &format!(
                        "/api/v1/telegram-directory/users/by-handle/{}/root-ready",
                        encode_path(handle)
                    ),
                    Some(json!({"rootNodeId":root_node_id.to_string()})),
                )
                .await
                .and_then(|value| serde_json::from_value(value).map_err(local_api_error)),
        }
    }

    pub async fn directory_complete_user_root(
        &self,
        telegram_user_id: i64,
        root_node_id: kcode_kweb_db::NodeId,
    ) -> Result<kcode_telegram_identity::User, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .directory
                .complete_user_root(telegram_user_id, root_node_id)
                .map_err(directory_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => self
                .request(
                    Method::POST,
                    &bases.kweb,
                    &format!(
                        "/api/v1/telegram-directory/users/{}/root-ready",
                        encode_path(telegram_user_id)
                    ),
                    Some(json!({"rootNodeId":root_node_id.to_string()})),
                )
                .await
                .and_then(|value| serde_json::from_value(value).map_err(local_api_error)),
        }
    }

    pub async fn directory_complete_group_root(
        &self,
        group_id: &str,
        root_node_id: kcode_kweb_db::NodeId,
    ) -> Result<kcode_telegram_identity::Group, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .directory
                .complete_group_root(group_id, root_node_id)
                .map_err(directory_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => self
                .request(
                    Method::POST,
                    &bases.kweb,
                    &format!(
                        "/api/v1/telegram-directory/groups/{}/root-ready",
                        encode_path(group_id)
                    ),
                    Some(json!({"rootNodeId":root_node_id.to_string()})),
                )
                .await
                .and_then(|value| serde_json::from_value(value).map_err(local_api_error)),
        }
    }

    pub async fn managed_source_execute(
        &self,
        session_id: &str,
        name: &str,
        arguments: Value,
        objects: Vec<Vec<u8>>,
    ) -> Result<kcode_dev_tools::ToolExecution, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let mut execution = local
                    .dev_tools
                    .execute(session_id.to_owned(), name.to_owned(), arguments, objects)
                    .await
                    .map_err(dev_tools_error)?;
                let mut object_ids = Vec::with_capacity(execution.objects.len());
                for bytes in std::mem::take(&mut execution.objects) {
                    object_ids.push(
                        local
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
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                if !objects.is_empty() {
                    return Err(ApiError {
                        status: None,
                        code: "local_service_unavailable".into(),
                        message: "Managed-source object calls require the in-process Kweb service."
                            .into(),
                    });
                }
                let kind = managed_source_kind(name).ok_or_else(|| ApiError {
                    status: None,
                    code: "unknown_managed_source_tool".into(),
                    message: format!("Unknown managed-source tool {name}."),
                })?;
                let path = match kind {
                    kcode_dev_tools::ManagedSourceKind::RustLibrary => "/api/v1/rust-libs/execute",
                    kcode_dev_tools::ManagedSourceKind::WebLibrary => "/api/v1/web-libs/execute",
                    kcode_dev_tools::ManagedSourceKind::RustBinary => "/api/v1/rust-bins/execute",
                };
                let payload = self
                    .request(
                        Method::POST,
                        &bases.kweb,
                        path,
                        Some(json!({
                            "session_id":session_id,
                            "name":name,
                            "arguments":arguments,
                        })),
                    )
                    .await?;
                let text = payload
                    .get("result")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| ApiError {
                        status: None,
                        code: "invalid_tool_result".into(),
                        message: "Managed-source tool returned a non-text result.".into(),
                    })?;
                let snapshot = payload
                    .get("snapshot")
                    .and_then(Value::as_str)
                    .map(|snapshot| kcode_dev_tools::SourceSnapshot {
                        kind,
                        name: arguments
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        text: snapshot.to_owned(),
                    })
                    .or_else(|| {
                        kcode_dev_tools::proposed_write_snapshot(name, &arguments)
                            .or_else(|| {
                                let preview = matches!(
                                    name,
                                    kcode_dev_tools::PREVIEW_WRITE_FILE_RUST_LIB_TOOL
                                        | kcode_dev_tools::PREVIEW_WRITE_FILE_WEB_LIB_TOOL
                                        | kcode_dev_tools::PREVIEW_WRITE_FILE_RUST_BIN_TOOL
                                );
                                preview.then(|| kcode_dev_tools::SourceSnapshot {
                                    kind,
                                    name: arguments
                                        .get("name")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                        .to_owned(),
                                    text: text.clone(),
                                })
                            })
                            .or_else(|| {
                                matches!(
                                    name,
                                    kcode_dev_tools::CREATE_RUST_LIB_TOOL
                                        | kcode_dev_tools::OPEN_RUST_LIB_TOOL
                                        | kcode_dev_tools::CREATE_WEB_LIB_TOOL
                                        | kcode_dev_tools::OPEN_WEB_LIB_TOOL
                                        | kcode_dev_tools::CREATE_RUST_BIN_TOOL
                                        | kcode_dev_tools::OPEN_RUST_BIN_TOOL
                                )
                                .then(|| {
                                    arguments.get("name").and_then(Value::as_str).map(|name| {
                                        kcode_dev_tools::SourceSnapshot {
                                            kind,
                                            name: name.to_owned(),
                                            text: text.clone(),
                                        }
                                    })
                                })
                                .flatten()
                            })
                    });
                Ok(kcode_dev_tools::ToolExecution {
                    text,
                    objects: Vec::new(),
                    snapshot,
                })
            }
        }
    }

    pub async fn release_managed_sources(&self, session_id: &str) {
        match &self.services {
            ServiceBackend::Local(local) => {
                if let Err(error) = local.dev_tools.release(session_id.to_owned()).await {
                    tracing::warn!(error=%error.message, "Managed-source session release failed");
                }
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let _ = self
                    .request(
                        Method::POST,
                        &bases.kweb,
                        "/api/v1/rust-libs/release",
                        Some(json!({"session_id":session_id})),
                    )
                    .await;
            }
        }
    }

    pub async fn telegram_get(&self, path: &str) -> Result<Value, ApiError> {
        self.request(Method::GET, &self.telegram, path, None).await
    }

    pub async fn telegram_post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.request(Method::POST, &self.telegram, path, Some(body))
            .await
    }

    pub async fn telegram_health(&self) -> Result<(), ApiError> {
        self.telegram_get("/health").await.map(|_| ())
    }

    pub async fn wait_until_telegram_ready(&self) {
        while self.telegram_health().await.is_err() {
            tokio::time::sleep(TELEGRAM_STARTUP_RETRY).await;
        }
    }

    async fn request(
        &self,
        method: Method,
        base: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, ApiError> {
        let mut request = self.client.request(method, format!("{base}{path}"));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|_| ApiError {
            status: None,
            code: "network_error".into(),
            message: format!("Could not reach {base}."),
        })?;
        decode_response(response).await
    }

    pub async fn telegram_bytes(&self, path: &str) -> Result<(Vec<u8>, String), ApiError> {
        let base = &self.telegram;
        let response = self
            .client
            .get(format!("{base}{path}"))
            .send()
            .await
            .map_err(|_| ApiError {
                status: None,
                code: "network_error".into(),
                message: format!("Could not reach {base}."),
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(decode_error(response).await);
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let bytes = response.bytes().await.map_err(|error| ApiError {
            status: Some(status),
            code: "invalid_response".into(),
            message: format!("Could not read response bytes: {error}"),
        })?;
        Ok((bytes.to_vec(), content_type))
    }

    pub async fn telegram_file_metadata(&self, path: &str) -> Result<(u64, String), ApiError> {
        let base = &self.telegram;
        let response = self
            .client
            .head(format!("{base}{path}"))
            .send()
            .await
            .map_err(|_| ApiError {
                status: None,
                code: "network_error".into(),
                message: format!("Could not reach {base}."),
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(decode_error(response).await);
        }
        let size_bytes = response.content_length().ok_or_else(|| ApiError {
            status: Some(status),
            code: "invalid_response".into(),
            message: "Telegram media metadata omitted its exact byte length.".into(),
        })?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        Ok((size_bytes, content_type))
    }

    async fn multipart(
        &self,
        base: &str,
        path: &str,
        form: multipart::Form,
    ) -> Result<Value, ApiError> {
        let response = self
            .client
            .post(format!("{base}{path}"))
            .multipart(form)
            .send()
            .await
            .map_err(|_| ApiError {
                status: None,
                code: "network_error".into(),
                message: format!("Could not reach {base}."),
            })?;
        decode_response(response).await
    }

    pub async fn telegram_send_object(
        &self,
        event_id: &str,
        conversation_id: &str,
        file: &ResolvedObject,
        complete: bool,
    ) -> Result<Value, ApiError> {
        if let Some(kind) = telegram_native_kind(&file.media_type, file.transport_kind.as_deref()) {
            let form = telegram_file_form(conversation_id, file, complete, Some(kind))?;
            return self
                .multipart(
                    &self.telegram,
                    &format!("/api/v1/events/{event_id}/media"),
                    form,
                )
                .await;
        }
        let form = telegram_file_form(conversation_id, file, complete, None)?;
        self.multipart(
            &self.telegram,
            &format!("/api/v1/events/{event_id}/file"),
            form,
        )
        .await
    }

    pub async fn telegram_send_private_object(
        &self,
        telegram_user_id: i64,
        conversation_id: &str,
        expected_conversation_id: Option<&str>,
        file: &ResolvedObject,
    ) -> Result<Value, ApiError> {
        let kind = telegram_native_kind(&file.media_type, file.transport_kind.as_deref());
        let form =
            telegram_private_file_form(conversation_id, expected_conversation_id, file, kind)?;
        self.multipart(
            &self.telegram,
            &format!("/api/v1/private-sessions/{telegram_user_id}/attachments"),
            form,
        )
        .await
    }

    pub fn bootstrap_node(&self, short_name: Option<&str>) -> Result<Node, ApiError> {
        let (short_name, short_description, long_description) = bootstrap_root_metadata(short_name);
        let source_created_at = chrono::Utc::now();
        let kmap = self.local_kmap()?;
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
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
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
                .and_then(|value| serde_json::to_value(value).map_err(local_api_error)),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let part = multipart::Part::bytes(bytes)
                    .file_name(filename)
                    .mime_str(mime)
                    .map_err(local_api_error)?;
                self.multipart(
                    &bases.intelligence,
                    "/api/v1/audio/transcriptions",
                    multipart::Form::new()
                        .text("prompt", prompt.to_owned())
                        .text("model", model.to_owned())
                        .text("parent_operation_id", parent_operation_id.to_string())
                        .part("file", part),
                )
                .await
            }
        }
    }

    pub async fn extract_document(
        &self,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .intelligence
                .extract_document(kcode_intelligence_router::Document {
                    bytes,
                    file_name: filename,
                    content_type: mime.to_owned(),
                })
                .await
                .map_err(intelligence_error)
                .and_then(|value| serde_json::to_value(value).map_err(local_api_error)),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let part = multipart::Part::bytes(bytes)
                    .file_name(filename)
                    .mime_str(mime)
                    .map_err(local_api_error)?;
                self.multipart(
                    &bases.intelligence,
                    "/api/v1/documents/extract",
                    multipart::Form::new().part("file", part),
                )
                .await
            }
        }
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
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .intelligence
                .for_user(user_id)
                .map_err(intelligence_error)?
                .annotate(kcode_intelligence_router::AnnotationRequest {
                    prompt: prompt.to_owned(),
                    model: model.to_owned(),
                    media: media_for_annotation(bytes, filename, mime)
                        .map_err(intelligence_error)?,
                    operation_id: Uuid::new_v4(),
                    parent_operation_id: Some(parent_operation_id),
                })
                .await
                .map_err(intelligence_error)
                .and_then(|value| serde_json::to_value(value).map_err(local_api_error)),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let part = multipart::Part::bytes(bytes)
                    .file_name(filename)
                    .mime_str(mime)
                    .map_err(local_api_error)?;
                self.multipart(
                    &bases.intelligence,
                    "/api/v1/media/annotations",
                    multipart::Form::new()
                        .text("model", model.to_owned())
                        .text("prompt", prompt.to_owned())
                        .text("parent_operation_id", parent_operation_id.to_string())
                        .part("file", part),
                )
                .await
            }
        }
    }

    pub async fn generate_image(
        &self,
        user_id: &str,
        model: &str,
        prompt: &str,
        references: Vec<(Vec<u8>, String, String)>,
        parent_operation_id: Uuid,
    ) -> Result<kcode_intelligence_router::ImageResponse, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let references = references
                    .into_iter()
                    .map(|(bytes, filename, mime)| {
                        media_for_image(bytes, filename, &mime).map_err(intelligence_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                local
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
            #[cfg(test)]
            ServiceBackend::Http(_) => Err(ApiError {
                status: None,
                code: "local_service_unavailable".into(),
                message: "Image generation requires the in-process intelligence router.".into(),
            }),
        }
    }

    pub async fn synchronize_audio_ingress(&self) -> Result<(), ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .audio
                .synchronize_completed_transcripts()
                .await
                .map_err(audio_error),
            #[cfg(test)]
            ServiceBackend::Http(_) => Ok(()),
        }
    }

    fn local_kmap(&self) -> Result<&Kmap, ApiError> {
        match &self.services {
            ServiceBackend::Local(services) => Ok(&services.kmap),
            #[cfg(test)]
            ServiceBackend::Http(_) => Err(ApiError {
                status: None,
                code: "local_service_unavailable".into(),
                message: "Kmap access requires the in-process typed service.".into(),
            }),
        }
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
        match &mut self.backend {
            AgentTurnBackend::Local(turn) => match turn.next_event().await {
                Ok(event) => event.map(Ok),
                Err(error) => Some(Err(intelligence_error(error))),
            },
            #[cfg(test)]
            AgentTurnBackend::Http(turn) => turn.events.pop_front(),
        }
    }

    pub(crate) async fn respond(
        &mut self,
        call_id: &str,
        result: kcode_codex_runtime_v2::ToolResult,
    ) -> Result<(), ApiError> {
        match &mut self.backend {
            AgentTurnBackend::Local(turn) => turn
                .respond(call_id, result)
                .await
                .map_err(intelligence_error),
            #[cfg(test)]
            AgentTurnBackend::Http(_) => Ok(()),
        }
    }
}

fn kmap_error(error: kcode_kmap::Error) -> ApiError {
    let (status, code, message) = match error.kind() {
        kcode_kmap::ErrorKind::InvalidInput => (
            StatusCode::BAD_REQUEST,
            "invalid_request",
            error.to_string(),
        ),
        kcode_kmap::ErrorKind::NotFound => (StatusCode::NOT_FOUND, "not_found", error.to_string()),
        kcode_kmap::ErrorKind::Conflict => (StatusCode::CONFLICT, "conflict", error.to_string()),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected Kmap database error occurred.".into(),
        ),
    };
    ApiError {
        status: Some(status),
        code: code.into(),
        message,
    }
}

fn intelligence_error(error: kcode_intelligence_router::Error) -> ApiError {
    let status = match error.kind() {
        kcode_intelligence_router::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        kcode_intelligence_router::ErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        kcode_intelligence_router::ErrorKind::Conflict
        | kcode_intelligence_router::ErrorKind::Cancelled => StatusCode::CONFLICT,
        kcode_intelligence_router::ErrorKind::Provider => StatusCode::BAD_GATEWAY,
        kcode_intelligence_router::ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiError {
        status: Some(status),
        code: error.code().into(),
        message: error.message().into(),
    }
}

fn directory_error(error: kcode_telegram_identity::Error) -> ApiError {
    let (status, code) = match error.kind() {
        kcode_telegram_identity::ErrorKind::InvalidInput => {
            (StatusCode::BAD_REQUEST, "invalid_request")
        }
        kcode_telegram_identity::ErrorKind::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        kcode_telegram_identity::ErrorKind::Conflict => (StatusCode::CONFLICT, "state_conflict"),
        kcode_telegram_identity::ErrorKind::Storage => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
        }
    };
    ApiError {
        status: Some(status),
        code: code.into(),
        message: error.message().into(),
    }
}

#[cfg(test)]
fn managed_source_kind(name: &str) -> Option<kcode_dev_tools::ManagedSourceKind> {
    if kcode_dev_tools::RUST_LIB_TOOLS.contains(&name)
        || name == kcode_dev_tools::PREVIEW_WRITE_FILE_RUST_LIB_TOOL
    {
        Some(kcode_dev_tools::ManagedSourceKind::RustLibrary)
    } else if kcode_dev_tools::WEB_LIB_TOOLS.contains(&name)
        || name == kcode_dev_tools::PREVIEW_WRITE_FILE_WEB_LIB_TOOL
    {
        Some(kcode_dev_tools::ManagedSourceKind::WebLibrary)
    } else if kcode_dev_tools::RUST_BIN_TOOLS.contains(&name)
        || name == kcode_dev_tools::PREVIEW_WRITE_FILE_RUST_BIN_TOOL
    {
        Some(kcode_dev_tools::ManagedSourceKind::RustBinary)
    } else {
        None
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
        status: Some(error.status),
        code: error.code.into(),
        message: error.message,
    }
}

fn history_error(error: kcode_session_history::Error) -> ApiError {
    ApiError {
        status: Some(history_status(error.kind)),
        code: error.kind.code().into(),
        message: error.message,
    }
}

fn history_status(kind: kcode_session_history::ErrorKind) -> StatusCode {
    match kind {
        kcode_session_history::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        kcode_session_history::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        kcode_session_history::ErrorKind::Conflict => StatusCode::CONFLICT,
        kcode_session_history::ErrorKind::Storage => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn json_value(value: impl serde::Serialize) -> Result<Value, ApiError> {
    serde_json::to_value(value).map_err(local_api_error)
}

fn audio_error(error: crate::audio_ingress::ServiceError) -> ApiError {
    ApiError {
        status: StatusCode::from_u16(error.status).ok(),
        code: error.code.into(),
        message: error.message,
    }
}

fn trim_base(value: &str) -> String {
    value.trim_end_matches('/').to_owned()
}

fn local_api_error(error: impl std::fmt::Display) -> ApiError {
    ApiError {
        status: None,
        code: "invalid_request".into(),
        message: error.to_string(),
    }
}

async fn decode_response(response: reqwest::Response) -> Result<Value, ApiError> {
    if !response.status().is_success() {
        return Err(decode_error(response).await);
    }
    let status = response.status();
    let bytes = response.bytes().await.map_err(|error| ApiError {
        status: Some(status),
        code: "invalid_response".into(),
        message: error.to_string(),
    })?;
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).map_err(|error| ApiError {
        status: Some(status),
        code: "invalid_response".into(),
        message: format!("Backend returned invalid JSON: {error}"),
    })
}

async fn decode_error(response: reqwest::Response) -> ApiError {
    let status = response.status();
    let payload = response.json::<Value>().await.unwrap_or(Value::Null);
    let remote = payload.get("error").unwrap_or(&Value::Null);
    ApiError {
        status: Some(status),
        code: remote
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("request_failed")
            .to_owned(),
        message: remote
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("Request failed ({status}).")),
    }
}

pub(crate) fn idempotency_id() -> String {
    Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
pub(crate) fn stable_idempotency_id(namespace: &str, value: &str) -> String {
    let digest = Sha256::digest(format!("{namespace}\0{value}").as_bytes());
    hex::encode(&digest[..16])
}

pub(crate) fn encode_path(value: impl std::fmt::Display) -> String {
    urlencoding::encode(&value.to_string()).into_owned()
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

fn telegram_file_form(
    conversation_id: &str,
    file: &ResolvedObject,
    complete: bool,
    kind: Option<&str>,
) -> Result<multipart::Form, ApiError> {
    let part = multipart::Part::bytes(file.bytes.clone())
        .file_name(file.file_name.clone())
        .mime_str(&file.media_type)
        .map_err(|error| ApiError {
            status: None,
            code: "invalid_object_metadata".into(),
            message: format!("Could not encode the object's media type: {error}"),
        })?;
    let mut form = multipart::Form::new()
        .text("conversationId", conversation_id.to_owned())
        .text("fileName", file.file_name.clone())
        .text("complete", complete.to_string())
        .part("file", part);
    if let Some(kind) = kind {
        form = form.text("kind", kind.to_owned());
    }
    Ok(form)
}

fn telegram_private_file_form(
    conversation_id: &str,
    expected_conversation_id: Option<&str>,
    file: &ResolvedObject,
    kind: Option<&str>,
) -> Result<multipart::Form, ApiError> {
    let part = multipart::Part::bytes(file.bytes.clone())
        .file_name(file.file_name.clone())
        .mime_str(&file.media_type)
        .map_err(|error| ApiError {
            status: None,
            code: "invalid_object_metadata".into(),
            message: format!("Could not encode the object's media type: {error}"),
        })?;
    let mut form = multipart::Form::new()
        .text("conversationId", conversation_id.to_owned())
        .text("fileName", file.file_name.clone())
        .part("file", part);
    if let Some(expected_conversation_id) = expected_conversation_id {
        form = form.text(
            "expectedConversationId",
            expected_conversation_id.to_owned(),
        );
    }
    if let Some(kind) = kind {
        form = form.text("kind", kind.to_owned());
    }
    Ok(form)
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

    #[tokio::test]
    async fn orchestration_waits_until_the_telegram_relay_serves_health() {
        use axum::{Json, Router, routing::get};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let config = Config {
            system_prompts_directory: std::path::PathBuf::new(),
            kweb_base: base.clone(),
            intelligence_base: base.clone(),
            session_history_base: base.clone(),
            user_root_node_id: "00000001".into(),
            kennedy_root_node_id: "00000002".into(),
            telegram_relay_base: base,
            telegram_max_media_bytes: 1024,
            telegram_web_user_handle: "@test".into(),
            runtime_model: crate::orchestration::RuntimeModel::testing(),
        };
        let api = Api::new(&config).unwrap();
        let waiting = tokio::spawn(async move {
            api.wait_until_telegram_ready().await;
        });

        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        let app = Router::new().route("/health", get(|| async { Json(json!({"status":"ok"})) }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("readiness wait should finish once health is served")
            .unwrap();

        server.abort();
    }

    #[tokio::test]
    async fn rust_binary_call_arguments_and_raw_text_cross_the_test_boundary_unchanged() {
        use axum::{Json, Router, extract::State, routing::post};

        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let app = Router::new()
            .route(
                "/api/v1/rust-bins/execute",
                post(
                    |State(calls): State<std::sync::Arc<std::sync::Mutex<Vec<Value>>>>,
                     Json(body): Json<Value>| async move {
                        calls.lock().unwrap().push(body);
                        Json(json!({
                            "result":" leading space\n{\"not\":\"pretty printed\"}\nABCDEFGH"
                        }))
                    },
                ),
            )
            .with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = Config {
            system_prompts_directory: std::path::PathBuf::new(),
            kweb_base: base.clone(),
            intelligence_base: base.clone(),
            session_history_base: base.clone(),
            user_root_node_id: "00000001".into(),
            kennedy_root_node_id: "00000002".into(),
            telegram_relay_base: base.clone(),
            telegram_max_media_bytes: 1024,
            telegram_web_user_handle: "@test".into(),
            runtime_model: crate::orchestration::RuntimeModel::testing(),
        };
        let api = Api::new(&config).unwrap();
        let default_arguments = json!({
            "name":"example",
            "version":"^1",
            "input":" exact input ",
            "objectIds":["ABCDEFGH"],
        });
        let custom_arguments = json!({
            "name":"example",
            "version":"v1.2.3",
            "timeoutSeconds":7,
        });

        let first = api
            .managed_source_execute(
                "test-session",
                kcode_dev_tools::CALL_RUST_BIN_TOOL,
                default_arguments.clone(),
                Vec::new(),
            )
            .await
            .unwrap();
        let second = api
            .managed_source_execute(
                "test-session",
                kcode_dev_tools::CALL_RUST_BIN_TOOL,
                custom_arguments.clone(),
                Vec::new(),
            )
            .await
            .unwrap();

        let exact = " leading space\n{\"not\":\"pretty printed\"}\nABCDEFGH";
        assert_eq!(first.text, exact);
        assert_eq!(second.text, exact);
        assert!(first.objects.is_empty());
        assert!(first.snapshot.is_none());
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0]["arguments"], default_arguments);
        assert!(calls[0]["arguments"].get("timeoutSeconds").is_none());
        assert_eq!(calls[1]["arguments"], custom_arguments);
        assert_eq!(calls[1]["arguments"]["timeoutSeconds"], 7);
        drop(calls);
        server.abort();
    }
}
