use std::{
    collections::{HashMap, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, ensure};
use chrono::Utc;
use kcode_codex_runtime_v2::{
    AgentEvent, AgentRequest, CompletedTurn, DynamicToolCall, ReasoningEffort, TokenUsage,
    ToolResult,
};
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderValue},
    redirect::Policy,
};
use serde_json::{Value, json};
use tokio::sync::watch;
use uuid::Uuid;
use zeroize::Zeroizing;

const OPENAI_RESPONSES_URL: &str = "https://api.openai.com/v1/responses";
const OPENAI_MODELS_URL: &str = "https://api.openai.com/v1/models";
const GEMINI_INTERACTIONS_URL: &str =
    "https://generativelanguage.googleapis.com/v1beta/interactions";
const GEMINI_MODELS_URL: &str = "https://generativelanguage.googleapis.com/v1beta/models";
const API_CONTEXT_WINDOW_TOKENS: u64 = 1_000_000;
const API_MAX_INPUT_TOKENS: u64 = API_CONTEXT_WINDOW_TOKENS * 70 / 100;
const UNKNOWN_API_CONTEXT_WINDOW_TOKENS: u64 = 128_000;
const UNKNOWN_API_MAX_INPUT_TOKENS: u64 = UNKNOWN_API_CONTEXT_WINDOW_TOKENS * 70 / 100;
const MAX_PROVIDER_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;
const MODEL_DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Backend {
    Codex,
    OpenAi,
    Gemini,
}

#[derive(Clone, Debug)]
pub(crate) struct Model {
    pub requested: String,
    pub provider_model: String,
    pub backend: Backend,
    pub context_window_tokens: u64,
    pub max_input_tokens: u64,
}

impl Model {
    pub(crate) fn codex(
        requested: String,
        provider_model: String,
        limits: kcode_codex_runtime::ModelLimits,
    ) -> Self {
        Self {
            requested,
            provider_model,
            backend: Backend::Codex,
            context_window_tokens: limits.context_window_tokens(),
            max_input_tokens: limits.max_input_tokens(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Providers {
    client: Client,
    openai_key: Option<Arc<Zeroizing<String>>>,
    gemini_key: Option<Arc<Zeroizing<String>>>,
    openai_responses_url: Arc<str>,
    openai_models_url: Arc<str>,
    gemini_interactions_url: Arc<str>,
    gemini_models_url: Arc<str>,
    receipt_directory: Arc<PathBuf>,
    receipt_writer: Arc<Mutex<()>>,
    model_cache: Arc<Mutex<HashMap<String, Model>>>,
    active: ActiveOperations,
}

#[derive(Clone, Default)]
struct ActiveOperations {
    senders: Arc<Mutex<HashMap<Uuid, watch::Sender<bool>>>>,
}

struct ActiveOperation {
    id: Uuid,
    active: ActiveOperations,
    cancelled: watch::Receiver<bool>,
}

pub(crate) struct Turn {
    events: VecDeque<anyhow::Result<AgentEvent>>,
    pending_call_id: Option<String>,
    completed: Option<CompletedTurn>,
    operation: ActiveOperation,
}

impl Providers {
    pub(crate) fn open(
        openai_key: Option<String>,
        gemini_key: Option<String>,
        receipt_directory: PathBuf,
    ) -> anyhow::Result<Self> {
        Self::with_urls(
            openai_key,
            gemini_key,
            receipt_directory,
            OPENAI_RESPONSES_URL,
            OPENAI_MODELS_URL,
            GEMINI_INTERACTIONS_URL,
            GEMINI_MODELS_URL,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_urls(
        openai_key: Option<String>,
        gemini_key: Option<String>,
        receipt_directory: PathBuf,
        openai_responses_url: &str,
        openai_models_url: &str,
        gemini_interactions_url: &str,
        gemini_models_url: &str,
    ) -> anyhow::Result<Self> {
        fs::create_dir_all(&receipt_directory).with_context(|| {
            format!(
                "creating subagent usage receipt directory {}",
                receipt_directory.display()
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&receipt_directory, fs::Permissions::from_mode(0o700))
                .with_context(|| {
                    format!(
                        "protecting subagent usage receipt directory {}",
                        receipt_directory.display()
                    )
                })?;
        }
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .referer(false);
        #[cfg(not(test))]
        let client = client.no_proxy().https_only(true);
        let client = client.build().context("building API subagent client")?;
        Ok(Self {
            client,
            openai_key: validated_key(openai_key, "OpenAI")?,
            gemini_key: validated_key(gemini_key, "Gemini")?,
            openai_responses_url: Arc::from(openai_responses_url),
            openai_models_url: Arc::from(openai_models_url),
            gemini_interactions_url: Arc::from(gemini_interactions_url),
            gemini_models_url: Arc::from(gemini_models_url),
            receipt_directory: Arc::new(receipt_directory),
            receipt_writer: Arc::new(Mutex::new(())),
            model_cache: Arc::new(Mutex::new(HashMap::new())),
            active: ActiveOperations::default(),
        })
    }

    pub(crate) async fn resolve(&self, requested: &str) -> anyhow::Result<Model> {
        validate_public_model_name(requested)?;
        ensure!(
            !requested.starts_with("codex/"),
            "Codex model names are resolved through the Codex catalog"
        );
        if let Some(model) = self
            .model_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("subagent model cache is unavailable"))?
            .get(requested)
            .cloned()
        {
            return Ok(model);
        }
        let backend = if requested.starts_with("gemini-") {
            ensure!(
                self.gemini_key.is_some(),
                "Gemini is not configured; model {requested:?} is unavailable"
            );
            Backend::Gemini
        } else {
            ensure!(
                self.openai_key.is_some(),
                "OpenAI is not configured; model {requested:?} is unavailable"
            );
            Backend::OpenAi
        };
        let advertised_limits = self.discover_model_limits(requested, backend).await?;
        let (context_window_tokens, max_input_tokens) = advertised_limits
            .or_else(|| known_api_model_limits(requested, backend))
            .unwrap_or((
                UNKNOWN_API_CONTEXT_WINDOW_TOKENS,
                UNKNOWN_API_MAX_INPUT_TOKENS,
            ));
        let model = Model {
            requested: requested.to_owned(),
            provider_model: requested.to_owned(),
            backend,
            context_window_tokens,
            max_input_tokens,
        };
        self.model_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("subagent model cache is unavailable"))?
            .insert(requested.to_owned(), model.clone());
        Ok(model)
    }

    pub(crate) async fn start_turn(
        &self,
        user_id: &str,
        operation_id: Uuid,
        model: &Model,
        request: AgentRequest,
    ) -> anyhow::Result<Turn> {
        ensure!(
            matches!(model.backend, Backend::OpenAi | Backend::Gemini),
            "API provider turn received a non-API backend"
        );
        ensure!(
            request.model == model.provider_model,
            "subagent request model does not match its resolved provider model"
        );
        let mut operation = self.active.register(operation_id)?;
        let request_value = match model.backend {
            Backend::OpenAi => openai_request(&request),
            Backend::Gemini => gemini_request(&request),
            Backend::Codex => unreachable!("validated API backend"),
        };
        let exact = format!(
            "{}\n",
            serde_json::to_string(&request_value).expect("JSON values always serialize")
        );
        let result = tokio::select! {
            _ = operation.cancelled() => Err(anyhow::anyhow!("subagent provider call was cancelled")),
            result = self.execute(model.backend, request.timeout, request_value) => result,
        };
        let value = match result {
            Ok(value) => value,
            Err(error) => {
                self.record_receipt(user_id, model, None, None)?;
                return Err(error);
            }
        };
        let parsed = match model.backend {
            Backend::OpenAi => parse_openai_response(&value, &model.provider_model),
            Backend::Gemini => parse_gemini_response(&value, &model.provider_model),
            Backend::Codex => unreachable!("validated API backend"),
        };
        let parsed = match parsed {
            Ok(parsed) => parsed,
            Err(error) => {
                self.record_receipt(user_id, model, None, None)?;
                return Err(error);
            }
        };
        self.record_receipt(
            user_id,
            model,
            Some(&parsed),
            parsed.provider_request_id.as_deref(),
        )?;
        let mut events = VecDeque::from([Ok(AgentEvent::ProviderInput(exact))]);
        let mut pending_call_id = None;
        let completed = CompletedTurn {
            thread_id: parsed
                .provider_request_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            turn_id: Uuid::new_v4().to_string(),
            answer: parsed.answer,
            usage: parsed.usage.clone(),
        };
        if let Some(call) = parsed.tool_call {
            pending_call_id = Some(call.call_id.clone());
            events.push_back(Ok(AgentEvent::ToolCall(call)));
        } else {
            events.push_back(Ok(AgentEvent::Completed(completed.clone())));
        }
        Ok(Turn {
            events,
            pending_call_id,
            completed: Some(completed),
            operation,
        })
    }

    pub(crate) fn cancel(&self, operation_id: Uuid) -> anyhow::Result<bool> {
        self.active.cancel(operation_id)
    }

    async fn execute(
        &self,
        backend: Backend,
        timeout: std::time::Duration,
        payload: Value,
    ) -> anyhow::Result<Value> {
        let request = match backend {
            Backend::OpenAi => {
                let key = self
                    .openai_key
                    .as_ref()
                    .context("OpenAI is not configured")?;
                let mut authorization = HeaderValue::from_str(&format!("Bearer {}", key.as_str()))
                    .context("OpenAI API key is not a valid header value")?;
                authorization.set_sensitive(true);
                self.client
                    .post(self.openai_responses_url.as_ref())
                    .header(AUTHORIZATION, authorization)
            }
            Backend::Gemini => {
                let key = self
                    .gemini_key
                    .as_ref()
                    .context("Gemini is not configured")?;
                let mut value = HeaderValue::from_str(key.as_str())
                    .context("Gemini API key is not a valid header value")?;
                value.set_sensitive(true);
                self.client
                    .post(self.gemini_interactions_url.as_ref())
                    .header("x-goog-api-key", value)
            }
            Backend::Codex => anyhow::bail!("Codex does not use the API subagent client"),
        }
        .json(&payload)
        .send();
        let mut response = tokio::time::timeout(timeout, request)
            .await
            .context("subagent provider timed out")?
            .context("calling subagent provider")?;
        let status = response.status();
        ensure_response_size(&response)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("reading provider response")?
        {
            ensure!(
                bytes.len().saturating_add(chunk.len())
                    <= usize::try_from(MAX_PROVIDER_RESPONSE_BYTES).unwrap(),
                "subagent provider response exceeded 16 MiB"
            );
            bytes.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&bytes).context("decoding subagent provider JSON")?;
        ensure!(
            status.is_success(),
            "subagent provider returned HTTP {status}: {}",
            provider_error_message(&value)
        );
        Ok(value)
    }

    async fn discover_model_limits(
        &self,
        model: &str,
        backend: Backend,
    ) -> anyhow::Result<Option<(u64, u64)>> {
        let (url, key, bearer) = match backend {
            Backend::OpenAi => (
                model_metadata_url(&self.openai_models_url, model)?,
                self.openai_key
                    .as_ref()
                    .context("OpenAI is not configured")?,
                true,
            ),
            Backend::Gemini => (
                model_metadata_url(&self.gemini_models_url, model)?,
                self.gemini_key
                    .as_ref()
                    .context("Gemini is not configured")?,
                false,
            ),
            Backend::Codex => anyhow::bail!("Codex limits come from its local catalog"),
        };
        let mut request = self.client.get(url);
        if bearer {
            let mut value = HeaderValue::from_str(&format!("Bearer {}", key.as_str()))
                .context("OpenAI API key is not a valid header value")?;
            value.set_sensitive(true);
            request = request.header(AUTHORIZATION, value);
        } else {
            let mut value = HeaderValue::from_str(key.as_str())
                .context("Gemini API key is not a valid header value")?;
            value.set_sensitive(true);
            request = request.header("x-goog-api-key", value);
        }
        let mut response = tokio::time::timeout(MODEL_DISCOVERY_TIMEOUT, request.send())
            .await
            .context("provider model discovery timed out")?
            .context("resolving provider model")?;
        let status = response.status();
        ensure_response_size(&response)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("reading provider model metadata")?
        {
            ensure!(
                bytes.len().saturating_add(chunk.len())
                    <= usize::try_from(MAX_PROVIDER_RESPONSE_BYTES).unwrap(),
                "provider model metadata exceeded 16 MiB"
            );
            bytes.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&bytes).context("decoding provider model metadata")?;
        ensure!(
            status.is_success(),
            "model {model:?} is unavailable: {}",
            provider_error_message(&value)
        );
        let max_input = u64_field(
            &value,
            &[
                "max_input_tokens",
                "maxInputTokens",
                "input_token_limit",
                "inputTokenLimit",
            ],
        );
        let context = u64_field(
            &value,
            &[
                "context_window_tokens",
                "contextWindowTokens",
                "context_window",
                "contextWindow",
            ],
        );
        let Some(max_input_tokens) =
            max_input.or_else(|| context.map(|value| value.saturating_mul(70) / 100))
        else {
            return Ok(None);
        };
        let context_window_tokens = context.unwrap_or(max_input_tokens);
        ensure!(
            max_input_tokens > 0 && context_window_tokens >= max_input_tokens,
            "provider metadata for model {model:?} has invalid limits"
        );
        Ok(Some((context_window_tokens, max_input_tokens)))
    }

    fn record_receipt(
        &self,
        user_id: &str,
        model: &Model,
        parsed: Option<&ParsedResponse>,
        provider_request_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let usage = parsed
            .and_then(|parsed| parsed.usage.as_ref())
            .map(|usage| {
                kcode_intelligence_router::Metering::Tokens(kcode_intelligence_router::TokenUsage {
                    input_tokens: usage.input_tokens.saturating_sub(usage.cached_input_tokens),
                    cached_input_tokens: usage.cached_input_tokens,
                    thinking_tokens: usage.reasoning_output_tokens,
                    output_tokens: usage
                        .output_tokens
                        .saturating_sub(usage.reasoning_output_tokens),
                })
            })
            .unwrap_or(kcode_intelligence_router::Metering::Unavailable);
        let receipt = kcode_intelligence_router::UsageReceipt {
            version: 1,
            id: Uuid::new_v4(),
            recorded_at: Utc::now(),
            user_id: user_id.to_owned(),
            operation: "subagent_turn".into(),
            requested_model: model.requested.clone(),
            actual_model: parsed
                .map(|parsed| parsed.actual_model.clone())
                .unwrap_or_else(|| model.provider_model.clone()),
            provider_request_id: provider_request_id.map(str::to_owned),
            provider_thread_id: None,
            metering: usage,
        };
        write_receipt(&self.receipt_directory, &self.receipt_writer, &receipt)
    }
}

impl Turn {
    pub(crate) async fn next_event(&mut self) -> Option<anyhow::Result<AgentEvent>> {
        if *self.operation.cancelled.borrow() {
            return Some(Err(anyhow::anyhow!("subagent provider call was cancelled")));
        }
        self.events.pop_front()
    }

    pub(crate) async fn respond(
        &mut self,
        call_id: &str,
        _result: ToolResult,
    ) -> anyhow::Result<()> {
        ensure!(
            self.pending_call_id.as_deref() == Some(call_id),
            "subagent tool result does not match the pending provider call"
        );
        self.pending_call_id = None;
        if let Some(mut completed) = self.completed.take() {
            completed.answer.clear();
            self.events.push_back(Ok(AgentEvent::Completed(completed)));
        }
        Ok(())
    }
}

impl ActiveOperations {
    fn register(&self, operation_id: Uuid) -> anyhow::Result<ActiveOperation> {
        let (sender, cancelled) = watch::channel(false);
        let replaced = self
            .senders
            .lock()
            .map_err(|_| anyhow::anyhow!("subagent operation registry is unavailable"))?
            .insert(operation_id, sender);
        ensure!(
            replaced.is_none(),
            "subagent operation identifier is already active"
        );
        Ok(ActiveOperation {
            id: operation_id,
            active: self.clone(),
            cancelled,
        })
    }

    fn cancel(&self, operation_id: Uuid) -> anyhow::Result<bool> {
        let sender = self
            .senders
            .lock()
            .map_err(|_| anyhow::anyhow!("subagent operation registry is unavailable"))?
            .get(&operation_id)
            .cloned();
        Ok(sender.is_some_and(|sender| sender.send(true).is_ok()))
    }
}

impl ActiveOperation {
    async fn cancelled(&mut self) {
        if *self.cancelled.borrow() {
            return;
        }
        while self.cancelled.changed().await.is_ok() {
            if *self.cancelled.borrow() {
                return;
            }
        }
    }
}

impl Drop for ActiveOperation {
    fn drop(&mut self) {
        if let Ok(mut senders) = self.active.senders.lock() {
            senders.remove(&self.id);
        }
    }
}

struct ParsedResponse {
    actual_model: String,
    provider_request_id: Option<String>,
    answer: String,
    tool_call: Option<DynamicToolCall>,
    usage: Option<TokenUsage>,
}

fn openai_request(request: &AgentRequest) -> Value {
    let mut value = json!({
        "model":request.model,
        "input":request.input,
        "store":false,
    });
    if openai_supports_reasoning(&request.model) {
        value["reasoning"] = json!({"effort":request.reasoning_effort.as_str()});
    }
    if !request.tools.is_empty() {
        value["tools"] = json!(
            request
                .tools
                .iter()
                .map(|tool| json!({
                    "type":"function",
                    "name":tool.name,
                    "description":tool.description,
                    "parameters":tool.input_schema,
                    "strict":false,
                }))
                .collect::<Vec<_>>()
        );
        value["tool_choice"] = json!("auto");
        value["parallel_tool_calls"] = json!(false);
    }
    value
}

fn gemini_request(request: &AgentRequest) -> Value {
    let mut value = json!({
        "model":request.model,
        "input":request.input,
        "generation_config":{
            "thinking_level":gemini_thinking_level(request.reasoning_effort),
        },
        "service_tier":"standard",
        "store":false,
    });
    if !request.tools.is_empty() {
        value["tools"] = json!(
            request
                .tools
                .iter()
                .map(|tool| json!({
                    "type":"function",
                    "name":tool.name,
                    "description":tool.description,
                    "parameters":tool.input_schema,
                }))
                .collect::<Vec<_>>()
        );
    }
    value
}

fn gemini_thinking_level(effort: ReasoningEffort) -> &'static str {
    match effort {
        ReasoningEffort::None | ReasoningEffort::Minimal => "minimal",
        ReasoningEffort::Low => "low",
        ReasoningEffort::Medium => "medium",
        ReasoningEffort::High | ReasoningEffort::XHigh | ReasoningEffort::Max => "high",
    }
}

fn parse_openai_response(value: &Value, requested_model: &str) -> anyhow::Result<ParsedResponse> {
    let mut answer = Vec::new();
    let mut tool_calls = Vec::new();
    for item in value
        .get("output")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                tool_calls.push(parse_function_call(item)?);
            }
            Some("message") => collect_text(item.get("content"), &mut answer),
            Some("output_text" | "text") => {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    answer.push(text.to_owned());
                }
            }
            _ => {}
        }
    }
    if let Some(text) = value.get("output_text").and_then(Value::as_str) {
        answer.push(text.to_owned());
    }
    ensure!(
        tool_calls.len() <= 1,
        "OpenAI returned multiple Ktool calls despite parallel tool calls being disabled"
    );
    let usage = value.get("usage");
    Ok(ParsedResponse {
        actual_model: value
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(requested_model)
            .to_owned(),
        provider_request_id: value.get("id").and_then(Value::as_str).map(str::to_owned),
        answer: answer.join("\n\n"),
        tool_call: tool_calls.pop(),
        usage: usage.map(|usage| TokenUsage {
            input_tokens: usage
                .get("input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            output_tokens: usage
                .get("output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            cached_input_tokens: usage
                .pointer("/input_tokens_details/cached_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            reasoning_output_tokens: usage
                .pointer("/output_tokens_details/reasoning_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            last_input_tokens: None,
            last_output_tokens: None,
        }),
    })
}

fn parse_gemini_response(value: &Value, requested_model: &str) -> anyhow::Result<ParsedResponse> {
    let mut answer = Vec::new();
    let mut tool_calls = Vec::new();
    let steps = value
        .get("steps")
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    for step in steps {
        match step.get("type").and_then(Value::as_str) {
            Some("function_call") => tool_calls.push(parse_function_call(step)?),
            Some("model_output") => {
                for item in step
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    match item.get("type").and_then(Value::as_str) {
                        Some("function_call") => tool_calls.push(parse_function_call(item)?),
                        Some("text") => {
                            if let Some(text) = item.get("text").and_then(Value::as_str) {
                                answer.push(text.to_owned());
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    ensure!(
        tool_calls.len() <= 1,
        "Gemini returned multiple Ktool calls; subagent Ktools must run serially"
    );
    let usage = value.get("usage");
    Ok(ParsedResponse {
        actual_model: value
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(requested_model)
            .to_owned(),
        provider_request_id: value
            .get("id")
            .or_else(|| value.get("interaction_id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        answer: answer.join("\n\n"),
        tool_call: tool_calls.pop(),
        usage: usage.map(|usage| TokenUsage {
            input_tokens: u64_field(usage, &["total_input_tokens"]).unwrap_or_default(),
            output_tokens: u64_field(usage, &["total_output_tokens"]).unwrap_or_default(),
            cached_input_tokens: u64_field(usage, &["total_cached_tokens"]).unwrap_or_default(),
            reasoning_output_tokens: u64_field(usage, &["total_thought_tokens"])
                .unwrap_or_default(),
            last_input_tokens: None,
            last_output_tokens: None,
        }),
    })
}

fn parse_function_call(value: &Value) -> anyhow::Result<DynamicToolCall> {
    let arguments = match value.get("arguments") {
        Some(Value::String(value)) => {
            serde_json::from_str(value).context("provider returned invalid function arguments")?
        }
        Some(Value::Object(_)) => value["arguments"].clone(),
        _ => anyhow::bail!("provider function call omitted arguments"),
    };
    Ok(DynamicToolCall {
        call_id: value
            .get("call_id")
            .or_else(|| value.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        tool: value
            .get("name")
            .and_then(Value::as_str)
            .context("provider function call omitted its name")?
            .to_owned(),
        arguments,
    })
}

fn collect_text(value: Option<&Value>, output: &mut Vec<String>) {
    for item in value.and_then(Value::as_array).into_iter().flatten() {
        if matches!(
            item.get("type").and_then(Value::as_str),
            Some("output_text" | "text")
        ) && let Some(text) = item.get("text").and_then(Value::as_str)
        {
            output.push(text.to_owned());
        }
    }
}

fn validate_public_model_name(model: &str) -> anyhow::Result<()> {
    ensure!(
        !model.trim().is_empty()
            && model.chars().count() <= 128
            && model.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':')
            })
            && !model.starts_with("codex:"),
        "model must be an exact safe provider model identifier"
    );
    Ok(())
}

fn validated_key(
    value: Option<String>,
    provider: &str,
) -> anyhow::Result<Option<Arc<Zeroizing<String>>>> {
    value
        .map(|value| {
            let trimmed = value.trim();
            ensure!(!trimmed.is_empty(), "{provider} API key is empty");
            HeaderValue::from_str(trimmed)
                .with_context(|| format!("{provider} API key is not a valid header value"))?;
            Ok(Arc::new(Zeroizing::new(trimmed.to_owned())))
        })
        .transpose()
}

fn known_api_model_limits(model: &str, backend: Backend) -> Option<(u64, u64)> {
    match backend {
        Backend::OpenAi if model == "gpt-5.6" => {
            Some((API_CONTEXT_WINDOW_TOKENS, API_MAX_INPUT_TOKENS))
        }
        Backend::Gemini
            if matches!(
                model,
                "gemini-2.5-flash" | "gemini-3.1-flash-lite" | "gemini-3.1-pro-preview"
            ) =>
        {
            Some((API_CONTEXT_WINDOW_TOKENS, API_MAX_INPUT_TOKENS))
        }
        Backend::Codex | Backend::OpenAi | Backend::Gemini => None,
    }
}

fn openai_supports_reasoning(model: &str) -> bool {
    model.starts_with("gpt-5") || model.starts_with('o')
}

fn model_metadata_url(base: &str, model: &str) -> anyhow::Result<Url> {
    let mut url = Url::parse(base).context("provider model metadata URL is invalid")?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("provider model metadata URL cannot accept a model name"))?
        .pop_if_empty()
        .push(model);
    Ok(url)
}

fn u64_field(value: &Value, fields: &[&str]) -> Option<u64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_u64))
}

fn provider_error_message(value: &Value) -> String {
    value
        .pointer("/error/message")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("provider returned an unspecified error")
        .to_owned()
}

fn ensure_response_size(response: &reqwest::Response) -> anyhow::Result<()> {
    ensure!(
        response
            .content_length()
            .is_none_or(|length| length <= MAX_PROVIDER_RESPONSE_BYTES),
        "subagent provider response exceeded 16 MiB"
    );
    Ok(())
}

fn write_receipt(
    root: &Path,
    writer: &Mutex<()>,
    receipt: &kcode_intelligence_router::UsageReceipt,
) -> anyhow::Result<()> {
    let _guard = writer
        .lock()
        .map_err(|_| anyhow::anyhow!("subagent usage receipt writer is unavailable"))?;
    let bytes = serde_json::to_vec_pretty(receipt).context("serializing subagent usage receipt")?;
    let final_path = root.join(format!("{}.json", receipt.id));
    let temporary_path = root.join(format!(".{}.tmp", receipt.id));
    let result = (|| -> std::io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary_path, &final_path)?;
        #[cfg(unix)]
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result.with_context(|| format!("writing subagent usage receipt {}", final_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        routing::{get, post},
    };

    #[test]
    fn provider_names_keep_api_models_plain_and_codex_explicit() {
        assert!(validate_public_model_name("gpt-5.6").is_ok());
        assert!(validate_public_model_name("ft:gpt-4.1:kennedy:focused:123").is_ok());
        assert!(validate_public_model_name("gemini-3.1-pro-preview").is_ok());
        assert!(validate_public_model_name("codex/gpt-5.6-sol").is_ok());
        assert!(validate_public_model_name("codex:gpt-5.6-sol").is_err());
        let codex = super::super::RuntimeModel::testing()
            .codex_subagent_model("codex/gpt-5.6-sol")
            .unwrap()
            .unwrap();
        assert_eq!(codex.backend, Backend::Codex);
        assert_eq!(codex.provider_model, "gpt-5.6-sol");
    }

    #[test]
    fn provider_requests_preserve_model_names_and_use_compatible_tool_schemas() {
        let tool = kcode_codex_runtime_v2::DynamicTool::new(
            "call_ktool",
            "Call one Ktool.",
            json!({
                "type":"object",
                "additionalProperties":false,
                "required":["name","arguments"],
                "properties":{
                    "name":{"type":"string"},
                    "arguments":{"type":"object"}
                }
            }),
        );
        let mut request = AgentRequest::new("clean context", "gpt-4.1");
        request.tools = vec![tool.clone()];
        let openai = openai_request(&request);
        assert_eq!(openai["model"], "gpt-4.1");
        assert!(openai.get("reasoning").is_none());
        assert_eq!(openai["parallel_tool_calls"], false);
        assert_eq!(openai["tools"][0]["strict"], false);

        let mut request = AgentRequest::new("clean context", "gpt-5.6");
        request.tools = vec![tool.clone()];
        assert_eq!(
            openai_request(&request).pointer("/reasoning/effort"),
            Some(&json!("xhigh"))
        );

        let mut request = AgentRequest::new("clean context", "gemini-3.6-flash");
        request.tools = vec![tool];
        let gemini = gemini_request(&request);
        assert_eq!(gemini["model"], "gemini-3.6-flash");
        assert_eq!(gemini["tools"][0]["type"], "function");
        assert_eq!(gemini["store"], false);
    }

    #[test]
    fn openai_and_gemini_responses_normalize_final_text_and_tool_calls() {
        let openai = parse_openai_response(
            &json!({
                "id":"resp-1",
                "model":"gpt-5.6",
                "output":[{"type":"message","content":[{"type":"output_text","text":"done"}]}],
                "usage":{"input_tokens":10,"output_tokens":2},
            }),
            "gpt-5.6",
        )
        .unwrap();
        assert_eq!(openai.answer, "done");
        let gemini = parse_gemini_response(
            &json!({
                "id":"interaction-1",
                "model":"gemini-3.1-pro-preview",
                "steps":[{
                    "type":"function_call",
                    "call_id":"call-1",
                    "name":"call_ktool",
                    "arguments":{"name":"WebFetch","arguments":{"url":"https://example.com"}}
                }],
                "usage":{"total_input_tokens":12,"total_output_tokens":3},
            }),
            "gemini-3.1-pro-preview",
        )
        .unwrap();
        assert_eq!(gemini.tool_call.unwrap().call_id, "call-1");
    }

    #[tokio::test]
    async fn configured_openai_and_gemini_models_run_through_distinct_api_backends() {
        let app = Router::new()
            .route(
                "/openai-models/{model}",
                get(|| async { Json(json!({"id":"available-openai-model"})) }),
            )
            .route(
                "/gemini-models/{model}",
                get(|| async {
                    Json(json!({
                        "name":"available-gemini-model",
                        "inputTokenLimit":1_000_000
                    }))
                }),
            )
            .route(
                "/openai",
                post(|| async {
                    Json(json!({
                        "id":"response-1",
                        "model":"gpt-5.6",
                        "output":[{
                            "type":"message",
                            "content":[{"type":"output_text","text":"OpenAI final"}]
                        }],
                        "usage":{"input_tokens":20,"output_tokens":4},
                    }))
                }),
            )
            .route(
                "/gemini",
                post(|| async {
                    Json(json!({
                        "id":"interaction-1",
                        "model":"gemini-3.1-pro-preview",
                        "steps":[{
                            "type":"function_call",
                            "call_id":"gemini-call",
                            "name":"call_ktool",
                            "arguments":{"name":"WebFetch","arguments":{"url":"https://example.com"}}
                        }],
                        "usage":{"total_input_tokens":30,"total_output_tokens":5},
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let receipts =
            std::env::temp_dir().join(format!("kennedy-subagent-receipts-{}", Uuid::new_v4()));
        let providers = Providers::with_urls(
            Some("openai-test-key".into()),
            Some("gemini-test-key".into()),
            receipts.clone(),
            &format!("{base}/openai"),
            &format!("{base}/openai-models"),
            &format!("{base}/gemini"),
            &format!("{base}/gemini-models"),
        )
        .unwrap();
        let tool = kcode_codex_runtime_v2::DynamicTool::new(
            "call_ktool",
            "Call one Ktool.",
            json!({"type":"object"}),
        );

        let openai = providers.resolve("gpt-5.6").await.unwrap();
        assert_eq!(openai.backend, Backend::OpenAi);
        let openai_unknown = providers.resolve("gpt-4.1").await.unwrap();
        assert_eq!(
            openai_unknown.context_window_tokens,
            UNKNOWN_API_CONTEXT_WINDOW_TOKENS
        );
        let mut openai_request = AgentRequest::new("clean context", "gpt-5.6");
        openai_request.tools = vec![tool.clone()];
        let mut openai_turn = providers
            .start_turn("user", Uuid::new_v4(), &openai, openai_request)
            .await
            .unwrap();
        assert!(matches!(
            openai_turn.next_event().await.unwrap().unwrap(),
            AgentEvent::ProviderInput(_)
        ));
        let AgentEvent::Completed(openai_completed) =
            openai_turn.next_event().await.unwrap().unwrap()
        else {
            panic!("OpenAI should return a terminal response");
        };
        assert_eq!(openai_completed.answer, "OpenAI final");

        let gemini = providers.resolve("gemini-3.1-pro-preview").await.unwrap();
        assert_eq!(gemini.backend, Backend::Gemini);
        let mut gemini_request = AgentRequest::new("clean context", "gemini-3.1-pro-preview");
        gemini_request.tools = vec![tool];
        let mut gemini_turn = providers
            .start_turn("user", Uuid::new_v4(), &gemini, gemini_request)
            .await
            .unwrap();
        assert!(matches!(
            gemini_turn.next_event().await.unwrap().unwrap(),
            AgentEvent::ProviderInput(_)
        ));
        let AgentEvent::ToolCall(call) = gemini_turn.next_event().await.unwrap().unwrap() else {
            panic!("Gemini should return a Ktool call");
        };
        assert_eq!(call.call_id, "gemini-call");
        gemini_turn
            .respond(&call.call_id, ToolResult::success("fetched"))
            .await
            .unwrap();
        let AgentEvent::Completed(gemini_completed) =
            gemini_turn.next_event().await.unwrap().unwrap()
        else {
            panic!("Gemini tool slice should complete after its result");
        };
        assert!(gemini_completed.answer.is_empty());
        assert_eq!(fs::read_dir(&receipts).unwrap().flatten().count(), 2);

        server.abort();
        fs::remove_dir_all(receipts).unwrap();
    }
}
