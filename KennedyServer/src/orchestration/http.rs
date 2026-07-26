#[cfg(test)]
use std::collections::VecDeque;
use std::time::Duration;

use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use reqwest::multipart;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
#[cfg(test)]
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[cfg(test)]
use super::Config;

const ACTIVE_CONNECTION_LIMIT: usize = 8;

#[derive(Clone)]
pub(crate) struct LocalServices {
    pub kmap: crate::kmap_http::Service,
    pub intelligence: crate::intelligence::Service,
    pub history: kcode_session_history::SessionHistory,
    pub audio: crate::audio_ingress::Service,
    pub directory: std::sync::Arc<crate::telegram_identity::Directory>,
    pub rust_lib_tools: crate::rust_lib_tools::RustLibToolService,
    pub web_lib_tools: crate::web_lib_tools::WebLibToolService,
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
}

#[derive(Clone)]
enum ServiceBackend {
    Local(std::sync::Arc<LocalServices>),
    #[cfg(test)]
    Http(TestBases),
}

pub(crate) enum AgentTurn {
    Local(crate::intelligence::AgentTurn),
    #[cfg(test)]
    Http(HttpAgentTurn),
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
    audio: String,
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
                audio: trim_base(&config.audio_ingress_base),
            }),
            history_sessions,
            telegram: trim_base(&config.telegram_relay_base),
        })
    }

    pub fn local(telegram_base: &str, services: LocalServices) -> anyhow::Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("building Telegram relay HTTP client")?;
        let history_sessions = services.history.clone();
        Ok(Self {
            client,
            services: ServiceBackend::Local(std::sync::Arc::new(services)),
            history_sessions,
            telegram: trim_base(telegram_base),
        })
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

    pub async fn kmap_post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Kmap, Method::POST, path, Some(body))
            .await
            .map(normalize_kmap_mutation_response)
    }

    pub async fn kmap_get(&self, path: &str) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Kmap, Method::GET, path, None)
            .await
    }

    pub async fn kmap_node(&self, node_id: &str) -> Result<Value, ApiError> {
        self.kmap_get(&format!("/api/v1/kmap/nodes/{node_id}"))
            .await
            .map(normalize_node)
    }

    pub(crate) fn commit_kweb_session(
        &self,
        input: crate::kmap_http::SessionCommit,
    ) -> Result<crate::kmap_http::SessionCommitResult, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local.kmap.commit_session(input).map_err(kmap_error),
            #[cfg(test)]
            ServiceBackend::Http(_) => Err(ApiError {
                status: None,
                code: "local_service_unavailable".into(),
                message: "Session commits require the in-process Kweb service.".into(),
            }),
        }
    }

    pub(crate) fn kmap_file(
        &self,
        object_id: &str,
    ) -> Result<crate::kmap_http::StoredFile, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local.kmap.get_file(object_id).map_err(kmap_error),
            #[cfg(test)]
            ServiceBackend::Http(_) => Err(ApiError {
                status: None,
                code: "local_service_unavailable".into(),
                message: "Object reads require the in-process Kweb service.".into(),
            }),
        }
    }

    pub async fn intelligence_get(&self, path: &str) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Intelligence, Method::GET, path, None)
            .await
    }

    pub async fn intelligence_post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Intelligence, Method::POST, path, Some(body))
            .await
    }

    pub async fn start_agent_turn(
        &self,
        operation_id: Uuid,
        request: kcode_codex_runtime_v2::AgentRequest,
    ) -> Result<AgentTurn, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .intelligence
                .start_agent_turn(operation_id, request)
                .await
                .map(AgentTurn::Local)
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
                Ok(AgentTurn::Http(HttpAgentTurn { events }))
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

    pub async fn history_record_completion(
        &self,
        input: kcode_session_history::RecordCompletion,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => {
                let object_id = input.session_object_id.clone();
                local
                    .history
                    .record_completion(input)
                    .await
                    .map_err(history_error)?;
                Ok(json!({"sessionObjectId":object_id,"recorded":true}))
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                self.request(
                    Method::POST,
                    &bases.history,
                    "/api/v1/session-history",
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

    pub async fn audio_get(&self, path: &str) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Audio, Method::GET, path, None)
            .await
    }

    pub async fn audio_post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Audio, Method::POST, path, Some(body))
            .await
    }

    pub async fn audio_put(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Audio, Method::PUT, path, Some(body))
            .await
    }

    pub async fn directory_get(&self, path: &str) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Directory, Method::GET, path, None)
            .await
    }

    pub async fn directory_post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.service_request(ServiceKind::Directory, Method::POST, path, Some(body))
            .await
    }

    pub async fn rust_lib_execute(
        &self,
        session_id: &str,
        name: &str,
        arguments: Value,
    ) -> Result<crate::rust_lib_tools::ToolExecution, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .rust_lib_tools
                .execute_detailed(session_id.to_owned(), name.to_owned(), arguments)
                .await
                .map_err(rust_lib_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let payload = self
                    .request(
                        Method::POST,
                        &bases.kweb,
                        "/api/v1/rust-libs/execute",
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
                        message: "Rust library tool returned a non-text result.".into(),
                    })?;
                let snapshot = payload
                    .get("snapshot")
                    .and_then(Value::as_str)
                    .map(|snapshot| crate::rust_lib_tools::LibrarySnapshot {
                        name: arguments
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        text: snapshot.to_owned(),
                    })
                    .or_else(|| {
                        if name == crate::rust_lib_tools::WRITE_RUST_LIB_TOOL {
                            crate::rust_lib_tools::proposed_write_snapshot(&arguments)
                        } else if name == crate::rust_lib_tools::PREVIEW_WRITE_FILE_RUST_LIB_TOOL {
                            Some(crate::rust_lib_tools::LibrarySnapshot {
                                name: arguments
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                                text: text.clone(),
                            })
                        } else if matches!(
                            name,
                            crate::rust_lib_tools::CREATE_RUST_LIB_TOOL
                                | crate::rust_lib_tools::OPEN_RUST_LIB_TOOL
                        ) {
                            arguments.get("name").and_then(Value::as_str).map(|name| {
                                crate::rust_lib_tools::LibrarySnapshot {
                                    name: name.to_owned(),
                                    text: text.clone(),
                                }
                            })
                        } else {
                            None
                        }
                    });
                Ok(crate::rust_lib_tools::ToolExecution { text, snapshot })
            }
        }
    }

    pub async fn web_lib_execute(
        &self,
        session_id: &str,
        name: &str,
        arguments: Value,
    ) -> Result<crate::rust_lib_tools::ToolExecution, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .web_lib_tools
                .execute_detailed(session_id.to_owned(), name.to_owned(), arguments)
                .await
                .map_err(rust_lib_error),
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let payload = self
                    .request(
                        Method::POST,
                        &bases.kweb,
                        "/api/v1/web-libs/execute",
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
                        message: "Web library tool returned a non-text result.".into(),
                    })?;
                let snapshot = payload
                    .get("snapshot")
                    .and_then(Value::as_str)
                    .map(|snapshot| crate::rust_lib_tools::LibrarySnapshot {
                        name: arguments
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        text: snapshot.to_owned(),
                    })
                    .or_else(|| {
                        if name == crate::web_lib_tools::WRITE_WEB_LIB_TOOL {
                            crate::web_lib_tools::proposed_write_snapshot(&arguments)
                        } else if name == crate::web_lib_tools::PREVIEW_WRITE_FILE_WEB_LIB_TOOL {
                            Some(crate::rust_lib_tools::LibrarySnapshot {
                                name: arguments
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                                text: text.clone(),
                            })
                        } else if matches!(
                            name,
                            crate::web_lib_tools::CREATE_WEB_LIB_TOOL
                                | crate::web_lib_tools::OPEN_WEB_LIB_TOOL
                        ) {
                            arguments.get("name").and_then(Value::as_str).map(|name| {
                                crate::rust_lib_tools::LibrarySnapshot {
                                    name: name.to_owned(),
                                    text: text.clone(),
                                }
                            })
                        } else {
                            None
                        }
                    });
                Ok(crate::rust_lib_tools::ToolExecution { text, snapshot })
            }
        }
    }

    pub async fn release_rust_libs(&self, session_id: &str) {
        match &self.services {
            ServiceBackend::Local(local) => {
                let (rust_result, web_result) = tokio::join!(
                    local.rust_lib_tools.release(session_id.to_owned()),
                    local.web_lib_tools.release(session_id.to_owned()),
                );
                if let Err(error) = rust_result {
                    tracing::warn!(error=%error.message, "Rust library session release failed");
                }
                if let Err(error) = web_result {
                    tracing::warn!(error=%error.message, "Web library session release failed");
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
        file: &crate::kmap_http::StoredFile,
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

    pub async fn kmap_context(&self, node_id: &str) -> Result<Value, ApiError> {
        let requested = self
            .kmap_get(&format!("/api/v1/kmap/nodes/{node_id}"))
            .await?;
        let fixed_ids = fixed_connection_ids(&requested);
        let active_ids = active_connection_ids(&requested);
        let mut seen = std::collections::HashSet::from([node_id.to_owned()]);
        let mut fixed = Vec::with_capacity(fixed_ids.len());
        for id in fixed_ids {
            if seen.insert(id.clone()) {
                fixed.push(self.kmap_get(&format!("/api/v1/kmap/nodes/{id}")).await?);
            }
        }
        let mut active = Vec::with_capacity(active_ids.len());
        for id in active_ids {
            if seen.insert(id.clone()) {
                active.push(self.kmap_get(&format!("/api/v1/kmap/nodes/{id}")).await?);
            }
        }
        Ok(json!({
            "requested_node": normalize_node(requested),
            "fixed_connection_nodes": fixed.into_iter().map(normalize_node).collect::<Vec<_>>(),
            "active_connection_nodes": active.into_iter().map(normalize_node).collect::<Vec<_>>(),
        }))
    }

    pub async fn bootstrap_node(&self, short_name: Option<&str>) -> Result<Value, ApiError> {
        let provenance = self
            .kmap_post(
                "/api/v1/kmap/provenance",
                json!({
                    "idempotency_id": idempotency_id(),
                    "data": "Automatically provisioned blank Kmap root node.",
                    "source": "system-bootstrap",
                    "source_created_at": chrono::Utc::now().to_rfc3339(),
                }),
            )
            .await?;
        self.kmap_post(
            "/api/v1/kmap/nodes",
            json!({
                "idempotency_id": idempotency_id(),
                "provenance_id": string_at(&provenance, "id")?,
                "owner_node_id": "self",
                "model_attribution": "system-bootstrap",
                "short_name": short_name.unwrap_or("User Root"),
                "short_description": "",
                "long_description": "",
                "fixed_connections": [],
                "recent_connections": [],
            }),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transcribe_audio(
        &self,
        prompt: &str,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
        parent_operation_id: Uuid,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .intelligence
                .transcribe_audio_with_prompt_bytes(
                    prompt,
                    bytes,
                    filename,
                    mime,
                    Some(parent_operation_id),
                )
                .await
                .map_err(intelligence_error),
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
                .extract_document_bytes(bytes, filename, mime)
                .await
                .map_err(intelligence_error),
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
        provider: &str,
        prompt: &str,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
        parent_operation_id: Uuid,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local
                .intelligence
                .annotate_media_bytes(
                    provider,
                    prompt,
                    bytes,
                    filename,
                    mime,
                    Some(parent_operation_id),
                )
                .await
                .map_err(intelligence_error),
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
                        .text("provider", provider.to_owned())
                        .text("prompt", prompt.to_owned())
                        .text("parent_operation_id", parent_operation_id.to_string())
                        .part("file", part),
                )
                .await
            }
        }
    }

    pub fn next_audio_ingress(&self) -> Result<Option<Value>, ApiError> {
        match &self.services {
            ServiceBackend::Local(local) => local.audio.next_ingress_piece().map_err(audio_error),
            #[cfg(test)]
            ServiceBackend::Http(_) => Err(ApiError {
                status: None,
                code: "local_service_unavailable".into(),
                message: "The audio-ingress queue is unavailable in HTTP test mode.".into(),
            }),
        }
    }

    async fn service_request(
        &self,
        service: ServiceKind,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, ApiError> {
        match &self.services {
            ServiceBackend::Local(services) => {
                let body = body.unwrap_or(Value::Null);
                match service {
                    ServiceKind::Kmap => match method {
                        Method::GET => services.kmap.get_json(path).await,
                        Method::POST => services.kmap.post_json(path, body).await,
                        Method::PUT => services.kmap.put_json(path, body).await,
                        _ => Err(crate::kmap_http::ApiError {
                            status: StatusCode::METHOD_NOT_ALLOWED,
                            code: "method_not_allowed",
                            message: "Unsupported direct Kmap operation.".into(),
                        }),
                    }
                    .map_err(kmap_error),
                    ServiceKind::Intelligence => match method {
                        Method::GET => services.intelligence.get_json(path).await,
                        Method::POST => services.intelligence.post_json(path, body).await,
                        _ => Err(crate::intelligence::ApiError::new(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "method_not_allowed",
                            "Unsupported direct intelligence operation.",
                        )),
                    }
                    .map_err(intelligence_error),
                    ServiceKind::Audio => match method {
                        Method::GET => services.audio.get_json(path).await,
                        Method::POST => services.audio.post_json(path, body).await,
                        Method::PUT => services.audio.put_json(path, body).await,
                        _ => Err(crate::audio_ingress::ServiceError {
                            status: StatusCode::METHOD_NOT_ALLOWED.as_u16(),
                            code: "method_not_allowed",
                            message: "Unsupported direct audio operation.".into(),
                        }),
                    }
                    .map_err(audio_error),
                    ServiceKind::Directory => match method {
                        Method::GET => services.directory.get_json(path).await,
                        Method::POST => services.directory.post_json(path, body).await,
                        _ => Err(crate::telegram_identity::ApiError {
                            status: StatusCode::METHOD_NOT_ALLOWED,
                            code: "method_not_allowed",
                            message: "Unsupported direct directory operation.".into(),
                        }),
                    }
                    .map_err(directory_error),
                }
            }
            #[cfg(test)]
            ServiceBackend::Http(bases) => {
                let base = match service {
                    ServiceKind::Kmap | ServiceKind::Directory => &bases.kweb,
                    ServiceKind::Intelligence => &bases.intelligence,
                    ServiceKind::Audio => &bases.audio,
                };
                self.request(method, base, path, body).await
            }
        }
    }
}

impl AgentTurn {
    pub(crate) async fn next_event(
        &mut self,
    ) -> Option<Result<kcode_codex_runtime_v2::AgentEvent, ApiError>> {
        match self {
            Self::Local(turn) => match turn.next_event().await {
                Ok(event) => event.map(Ok),
                Err(error) => Some(Err(intelligence_error(error))),
            },
            #[cfg(test)]
            Self::Http(turn) => turn.events.pop_front(),
        }
    }

    pub(crate) async fn respond(
        &self,
        call_id: &str,
        result: kcode_codex_runtime_v2::ToolResult,
    ) -> Result<(), ApiError> {
        match self {
            Self::Local(turn) => turn
                .respond(call_id, result)
                .await
                .map_err(intelligence_error),
            #[cfg(test)]
            Self::Http(_) => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
enum ServiceKind {
    Kmap,
    Intelligence,
    Audio,
    Directory,
}

fn kmap_error(error: crate::kmap_http::ApiError) -> ApiError {
    ApiError {
        status: Some(error.status),
        code: error.code.into(),
        message: error.message,
    }
}

fn intelligence_error(error: crate::intelligence::ApiError) -> ApiError {
    ApiError {
        status: Some(error.status),
        code: error.code.into(),
        message: error.message,
    }
}

fn directory_error(error: crate::telegram_identity::ApiError) -> ApiError {
    ApiError {
        status: Some(error.status),
        code: error.code.into(),
        message: error.message,
    }
}

fn rust_lib_error(error: crate::rust_lib_tools::ToolError) -> ApiError {
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

pub(crate) fn string_at<'a>(value: &'a Value, key: &str) -> Result<&'a str, ApiError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError {
            status: None,
            code: "invalid_response".into(),
            message: format!("Backend response is missing {key}."),
        })
}

pub(crate) fn data_url(mime: &str, bytes: &[u8]) -> String {
    format!("data:{mime};base64,{}", BASE64.encode(bytes))
}

fn active_connection_ids(node: &Value) -> Vec<String> {
    node.get("recent_connections")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(ACTIVE_CONNECTION_LIMIT)
        .filter_map(|entry| {
            entry
                .as_str()
                .or_else(|| entry.get("id").and_then(Value::as_str))
                .map(str::to_owned)
        })
        .collect()
}

fn fixed_connection_ids(node: &Value) -> Vec<String> {
    node.get("fixed_connections")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            entry
                .as_str()
                .or_else(|| entry.get("id").and_then(Value::as_str))
                .map(str::to_owned)
        })
        .collect()
}

fn normalize_node(mut node: Value) -> Value {
    let summaries = node
        .get("connection_summaries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|summary| Some((summary.get("id")?.as_str()?.to_owned(), summary.clone())))
        .collect::<std::collections::HashMap<_, _>>();
    let hydrate = |entry: &Value| {
        let id = entry
            .as_str()
            .or_else(|| entry.get("id").and_then(Value::as_str))
            .unwrap_or_default();
        let mut value = summaries
            .get(id)
            .cloned()
            .unwrap_or_else(|| json!({"id": id}));
        if let (Some(target), Some(source)) = (value.as_object_mut(), entry.as_object()) {
            target.extend(source.clone());
        }
        value
    };
    let fixed = node
        .get("fixed_connections")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, entry)| {
            let mut value = hydrate(entry);
            if let Some(object) = value.as_object_mut() {
                object.entry("slot").or_insert_with(|| json!(index + 1));
            }
            value
        })
        .collect::<Vec<_>>();
    let recent = node
        .get("recent_connections")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(hydrate)
        .collect::<Vec<_>>();
    let owner = node.get("owner_node_id").cloned().unwrap_or(Value::Null);
    if let Some(object) = node.as_object_mut() {
        object.insert("owner_root_node_id".into(), owner);
        object.insert("fixed_connections".into(), json!(fixed));
        object.insert(
            "active_connections".into(),
            json!(recent.iter().take(8).cloned().collect::<Vec<_>>()),
        );
        object.insert(
            "fanout_connections".into(),
            json!(recent.iter().skip(8).cloned().collect::<Vec<_>>()),
        );
    }
    node
}

fn normalize_kmap_mutation_response(mut response: Value) -> Value {
    if let Some(node) = response.get_mut("node") {
        *node = normalize_node(std::mem::take(node));
    }
    response
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
    file: &crate::kmap_http::StoredFile,
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn mutation_nodes_are_normalized_for_immediate_agent_rendering() {
        let recent = (0..9).map(|index| format!("recent-{index}"));
        let summaries = std::iter::once(json!({
            "id":"fixed",
            "short_name":"Fixed node",
            "short_description":"Fixed summary",
        }))
        .chain((0..9).map(|index| {
            json!({
                "id":format!("recent-{index}"),
                "short_name":format!("Recent {index}"),
                "short_description":format!("Recent summary {index}"),
            })
        }))
        .collect::<Vec<_>>();
        let response = normalize_kmap_mutation_response(json!({
            "node": {
                "id":"updated",
                "owner_node_id":"root",
                "short_name":"Updated node",
                "short_description":"Updated summary",
                "long_description":"Updated details",
                "fixed_connections":["fixed"],
                "recent_connections":recent.collect::<Vec<_>>(),
                "connection_summaries":summaries,
            }
        }));
        let node = &response["node"];
        assert_eq!(node["owner_root_node_id"], "root");
        assert_eq!(node["fixed_connections"][0]["id"], "fixed");
        assert_eq!(node["fixed_connections"][0]["slot"], 1);
        assert_eq!(node["active_connections"].as_array().unwrap().len(), 8);
        assert_eq!(node["fanout_connections"].as_array().unwrap().len(), 1);
        assert_eq!(node["fanout_connections"][0]["short_name"], "Recent 8");
    }

    #[test]
    fn context_fetch_expands_only_the_first_eight_recent_connections() {
        let recent = (0..12)
            .map(|index| json!({"id":format!("recent-{index}")}))
            .collect::<Vec<_>>();
        assert_eq!(
            active_connection_ids(&json!({"recent_connections":recent})),
            (0..8)
                .map(|index| format!("recent-{index}"))
                .collect::<Vec<_>>()
        );
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
}
