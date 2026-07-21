mod defaults;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use kcode_codex_runtime::{
    CatalogCache, Codex, CodexConfig, ErrorKind as CodexErrorKind, GenerationRequest, ModelLimits,
    TokenUsage as CodexUsage, WebSearchRequest as CodexSearchRequest,
};
use kcode_doc_extraction::{DocumentExtractor, DocumentInput, ErrorKind as DocumentErrorKind};
use kcode_gemini_api::{
    Error as GeminiError, Gemini, GroundedSearchRequest, TokenUsage as GeminiUsage,
};
use kcode_openai_api::{
    AudioInput, Error as OpenAiError, OpenAi, TranscriptionRequest, TranscriptionUsage,
};
use kcode_web_fetch::{ErrorKind as WebFetchErrorKind, WebFetcher};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;
use uuid::Uuid;

use defaults::*;

#[derive(Clone)]
struct ProviderConfig {
    kind: &'static str,
    default_model: &'static str,
    models: Vec<&'static str>,
    reasoning_effort: kcode_codex_runtime::ReasoningEffort,
    timeout: Duration,
    native_audio_input_models: Vec<&'static str>,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            kind: CODEX_PROVIDER_KIND,
            default_model: DEFAULT_MODEL,
            models: vec![DEFAULT_MODEL],
            reasoning_effort: GENERATION_REASONING_EFFORT,
            timeout: GENERATION_TIMEOUT,
            native_audio_input_models: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct ProviderRuntime {
    config: ProviderConfig,
    model_limits: HashMap<&'static str, ModelLimits>,
}

#[derive(Clone)]
pub(crate) struct Service {
    default_provider: &'static str,
    providers: Arc<HashMap<&'static str, ProviderRuntime>>,
    codex: Codex,
    openai: Option<OpenAi>,
    gemini: Option<Gemini>,
    web_fetcher: WebFetcher,
    document_extractor: DocumentExtractor,
    active_operations: ActiveOperations,
    clean_codex_threads: CleanCodexThreads,
}

#[derive(Clone, Default)]
struct ActiveOperations {
    senders: Arc<Mutex<HashMap<Uuid, watch::Sender<bool>>>>,
}

struct ActiveOperation {
    id: Uuid,
    operations: ActiveOperations,
    cancellation: watch::Receiver<bool>,
}

impl ActiveOperations {
    fn register(&self, id: Uuid) -> Result<ActiveOperation, ApiError> {
        let (sender, cancellation) = watch::channel(false);
        let mut senders = self.senders.lock().map_err(|_| {
            ApiError::internal(
                "operation_registry_unavailable",
                "The operation registry is unavailable.",
            )
        })?;
        if senders.contains_key(&id) {
            return Err(ApiError::conflict(
                "operation_in_progress",
                "An operation with this identifier is already running.",
            ));
        }
        senders.insert(id, sender);
        Ok(ActiveOperation {
            id,
            operations: self.clone(),
            cancellation,
        })
    }

    fn cancel(&self, id: Uuid) -> Result<bool, ApiError> {
        let sender = self
            .senders
            .lock()
            .map_err(|_| {
                ApiError::internal(
                    "operation_registry_unavailable",
                    "The operation registry is unavailable.",
                )
            })?
            .get(&id)
            .cloned();
        Ok(sender.is_some_and(|sender| sender.send(true).is_ok()))
    }

    fn remove(&self, id: Uuid) {
        if let Ok(mut senders) = self.senders.lock() {
            senders.remove(&id);
        }
    }
}

impl ActiveOperation {
    async fn cancelled(&mut self) {
        if *self.cancellation.borrow() {
            return;
        }
        while self.cancellation.changed().await.is_ok() {
            if *self.cancellation.borrow() {
                return;
            }
        }
    }
}

impl Drop for ActiveOperation {
    fn drop(&mut self) {
        self.operations.remove(self.id);
    }
}

#[derive(Clone, Default)]
struct CleanCodexThreads {
    ids: Arc<Mutex<HashSet<String>>>,
}

impl CleanCodexThreads {
    fn require_known(&self, id: &str) -> Result<(), ApiError> {
        let ids = self.ids.lock().map_err(|_| {
            ApiError::internal(
                "thread_registry_unavailable",
                "The clean Codex thread registry is unavailable.",
            )
        })?;
        if !ids.contains(id) {
            return Err(ApiError::conflict(
                "stale_codex_thread",
                "This Codex thread predates the verified prompt boundary and must be replayed into a fresh thread.",
            ));
        }
        Ok(())
    }

    fn remember(&self, id: &str) -> Result<(), ApiError> {
        self.ids
            .lock()
            .map_err(|_| {
                ApiError::internal(
                    "thread_registry_unavailable",
                    "The clean Codex thread registry is unavailable.",
                )
            })?
            .insert(id.to_owned());
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) code: &'static str,
    pub(crate) message: String,
    request_id: Option<Uuid>,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            request_id: None,
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    fn internal(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
    }

    fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    fn cancelled(request_id: Uuid) -> Self {
        Self::conflict(
            "operation_cancelled",
            "The operation was stopped by the user.",
        )
        .with_request_id(request_id)
    }

    fn with_request_id(mut self, request_id: Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut error = json!({"code":self.code,"message":self.message});
        if let Some(request_id) = self.request_id {
            error["request_id"] = json!(request_id.to_string());
        }
        (self.status, Json(json!({"error":error}))).into_response()
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct GenerateRequest {
    provider: Option<String>,
    model: Option<String>,
    chatend: String,
    #[serde(default)]
    operation_id: Option<Uuid>,
    #[serde(default)]
    previous_response_id: Option<String>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WebSearchRequest {
    provider: Option<String>,
    model: Option<String>,
    question: String,
    #[serde(default)]
    operation_id: Option<Uuid>,
    #[serde(default)]
    mode: WebSearchMode,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum WebSearchMode {
    Fast,
    #[default]
    Balanced,
    Quality,
}

#[derive(Clone, Copy)]
enum SearchProfile {
    Gemini,
    Codex {
        model: &'static str,
        reasoning: kcode_codex_runtime::ReasoningEffort,
        context: kcode_codex_runtime::WebSearchContext,
        depth: kcode_codex_runtime::SearchDepth,
        timeout: Duration,
    },
}

impl WebSearchMode {
    fn profile(self) -> SearchProfile {
        match self {
            Self::Fast => SearchProfile::Gemini,
            Self::Balanced => SearchProfile::Codex {
                model: BALANCED_SEARCH_MODEL,
                reasoning: BALANCED_SEARCH_REASONING,
                context: BALANCED_SEARCH_CONTEXT,
                depth: BALANCED_SEARCH_DEPTH,
                timeout: BALANCED_SEARCH_TIMEOUT,
            },
            Self::Quality => SearchProfile::Codex {
                model: QUALITY_SEARCH_MODEL,
                reasoning: QUALITY_SEARCH_REASONING,
                context: QUALITY_SEARCH_CONTEXT,
                depth: QUALITY_SEARCH_DEPTH,
                timeout: QUALITY_SEARCH_TIMEOUT,
            },
        }
    }
}

#[derive(Clone, Serialize, PartialEq, Eq)]
struct WebSource {
    title: String,
    url: String,
}

#[derive(Serialize)]
struct WebSearchResponse {
    answer: String,
    sources: Vec<WebSource>,
    provider: String,
    model: String,
    mode: WebSearchMode,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct WebFetchRequest {
    url: String,
    #[serde(default)]
    operation_id: Option<Uuid>,
}

#[derive(Serialize)]
struct WebFetchResponse {
    url: String,
    title: Option<String>,
    content_type: String,
    content: String,
    truncated: bool,
    retrieved_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct OperationCancellationResponse {
    cancelled: bool,
}

#[derive(Serialize)]
struct TranscriptionResponse {
    status: String,
    provider: String,
    input_model: String,
    transcription_model: String,
    text: String,
    usage: Option<Value>,
}

#[derive(Serialize)]
struct DocumentExtractionResponse {
    status: String,
    file_name: String,
    content_type: String,
    format: String,
    text: String,
    characters: usize,
    truncated: bool,
}

#[derive(Serialize)]
struct NormalizedResponse {
    status: String,
    message: Message,
    response_id: String,
    usage: Option<Usage>,
}

#[derive(Serialize, Deserialize)]
struct Usage {
    input_tokens: u64,
    output_tokens: u64,
    cached_tokens: u64,
    cache_write_tokens: u64,
    reasoning_tokens: u64,
    cumulative: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_output_tokens: Option<u64>,
}

impl From<CodexUsage> for Usage {
    fn from(value: CodexUsage) -> Self {
        Self {
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            cached_tokens: value.cached_input_tokens,
            cache_write_tokens: 0,
            reasoning_tokens: value.reasoning_output_tokens,
            cumulative: true,
            last_input_tokens: value.last_input_tokens,
            last_output_tokens: value.last_output_tokens,
        }
    }
}

impl From<GeminiUsage> for Usage {
    fn from(value: GeminiUsage) -> Self {
        Self {
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            cached_tokens: value.cached_tokens,
            cache_write_tokens: 0,
            reasoning_tokens: value.thought_tokens,
            cumulative: false,
            last_input_tokens: None,
            last_output_tokens: None,
        }
    }
}

pub(crate) async fn open(
    openai_api_key: Option<String>,
    gemini_api_key: Option<String>,
    codex_catalog_cache: CatalogCache,
) -> anyhow::Result<Service> {
    let mut codex_config = CodexConfig::new(DEFAULT_MODEL);
    codex_config.base_instruction = KENNEDY_CODEX_BASE_INSTRUCTION.into();
    codex_config.validation_reasoning_effort = GENERATION_REASONING_EFFORT;
    let codex = Codex::open(codex_config, codex_catalog_cache)
        .await
        .context("opening Kennedy Codex runtime")?;
    let provider_config = ProviderConfig::default();
    let mut model_limits = HashMap::new();
    for model in &provider_config.models {
        let limits = codex
            .catalog()
            .model_limits(model)
            .with_context(|| format!("Codex model {model} is absent from the catalog"))?;
        model_limits.insert(*model, limits);
    }
    let providers = HashMap::from([(
        DEFAULT_PROVIDER_NAME,
        ProviderRuntime {
            config: provider_config,
            model_limits,
        },
    )]);
    let openai = openai_api_key
        .filter(|value| !value.trim().is_empty())
        .map(OpenAi::open)
        .transpose()
        .context("opening OpenAI client")?;
    let gemini = gemini_api_key
        .filter(|value| !value.trim().is_empty())
        .map(Gemini::open)
        .transpose()
        .context("opening Gemini client")?;
    Ok(Service {
        default_provider: DEFAULT_PROVIDER_NAME,
        providers: Arc::new(providers),
        codex,
        openai,
        gemini,
        web_fetcher: WebFetcher::default(),
        document_extractor: DocumentExtractor::default(),
        active_operations: ActiveOperations::default(),
        clean_codex_threads: CleanCodexThreads::default(),
    })
}

pub(crate) fn router(state: Service) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/providers", get(list_providers))
        .route("/api/v1/audio/transcriptions", post(transcribe_audio))
        .route("/api/v1/documents/extract", post(extract_document))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state)
}

impl Service {
    pub(crate) async fn get_json(&self, path: &str) -> Result<Value, ApiError> {
        match path {
            "/health" => {
                let Json(value) = health(State(self.clone())).await;
                Ok(value)
            }
            "/api/v1/providers" => {
                let Json(value) = list_providers(State(self.clone())).await;
                Ok(value)
            }
            _ => Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "Intelligence resource not found.",
            )),
        }
    }

    pub(crate) async fn post_json(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        match path {
            "/api/v1/generate" => {
                let Json(value) = generate(
                    State(self.clone()),
                    Json(
                        serde_json::from_value(body)
                            .map_err(|error| ApiError::invalid(error.to_string()))?,
                    ),
                )
                .await?;
                serde_json::to_value(value)
                    .map_err(|error| ApiError::internal("serialization_failed", error.to_string()))
            }
            "/api/v1/web/search" => {
                let Json(value) = web_search(
                    State(self.clone()),
                    Json(
                        serde_json::from_value(body)
                            .map_err(|error| ApiError::invalid(error.to_string()))?,
                    ),
                )
                .await?;
                serde_json::to_value(value)
                    .map_err(|error| ApiError::internal("serialization_failed", error.to_string()))
            }
            "/api/v1/web/fetch" => {
                let Json(value) = web_fetch(
                    State(self.clone()),
                    Json(
                        serde_json::from_value(body)
                            .map_err(|error| ApiError::invalid(error.to_string()))?,
                    ),
                )
                .await?;
                serde_json::to_value(value)
                    .map_err(|error| ApiError::internal("serialization_failed", error.to_string()))
            }
            _ if path.starts_with("/api/v1/operations/") && path.ends_with("/cancel") => {
                let id = path
                    .trim_start_matches("/api/v1/operations/")
                    .trim_end_matches("/cancel")
                    .trim_end_matches('/');
                let operation_id =
                    Uuid::parse_str(id).map_err(|_| ApiError::invalid("Invalid operation ID."))?;
                let Json(value) = cancel_operation(State(self.clone()), Path(operation_id)).await?;
                serde_json::to_value(value)
                    .map_err(|error| ApiError::internal("serialization_failed", error.to_string()))
            }
            _ => Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "Intelligence resource not found.",
            )),
        }
    }

    pub(crate) async fn extract_document_bytes(
        &self,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
    ) -> Result<Value, ApiError> {
        let request_id = Uuid::new_v4();
        let response = self
            .extract_document_input(
                DocumentInput {
                    file_name: filename,
                    content_type: mime.to_ascii_lowercase(),
                    data: bytes,
                },
                request_id,
            )
            .await?;
        serde_json::to_value(response)
            .map_err(|error| ApiError::internal("serialization_failed", error.to_string()))
    }

    pub(crate) async fn transcribe_bytes(
        &self,
        provider: &str,
        model: &str,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
    ) -> Result<Value, ApiError> {
        let request_id = Uuid::new_v4();
        let response = self
            .transcribe_input(
                Some(provider),
                Some(model),
                bytes,
                safe_audio_filename(Some(&filename), mime),
                mime.to_ascii_lowercase(),
                request_id,
            )
            .await?;
        serde_json::to_value(response)
            .map_err(|error| ApiError::internal("serialization_failed", error.to_string()))
    }

    async fn extract_document_input(
        &self,
        input: DocumentInput,
        request_id: Uuid,
    ) -> Result<DocumentExtractionResponse, ApiError> {
        let input_bytes = input.data.len();
        let started = Instant::now();
        let extractor = self.document_extractor.clone();
        let extracted = tokio::task::spawn_blocking(move || extractor.extract(input))
            .await
            .map_err(|_| {
                ApiError::internal(
                    "document_extraction_failed",
                    "The document extraction worker stopped unexpectedly.",
                )
                .with_request_id(request_id)
            })?
            .map_err(|error| document_error(error, request_id))?;
        tracing::info!(
            format = extracted.format.as_str(),
            input_bytes,
            characters = extracted.characters,
            truncated = extracted.truncated,
            duration_ms = started.elapsed().as_millis(),
            "Document extracted"
        );
        Ok(DocumentExtractionResponse {
            status: "complete".into(),
            file_name: extracted.file_name,
            content_type: extracted.content_type,
            format: extracted.format.as_str().into(),
            text: extracted.text,
            characters: extracted.characters,
            truncated: extracted.truncated,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn transcribe_input(
        &self,
        requested_provider: Option<&str>,
        requested_model: Option<&str>,
        bytes: Vec<u8>,
        file_name: String,
        content_type: String,
        request_id: Uuid,
    ) -> Result<TranscriptionResponse, ApiError> {
        let (provider_name, provider, model) =
            selected_provider(self, requested_provider, requested_model)?;
        if provider.config.native_audio_input_models.contains(&model) {
            return Err(ApiError::conflict(
                "native_audio_supported",
                "The selected model supports native audio and must receive the recording directly.",
            )
            .with_request_id(request_id));
        }
        let openai = self.openai.as_ref().ok_or_else(|| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "transcription_unavailable",
                "Audio transcription is not configured.",
            )
            .with_request_id(request_id)
        })?;
        let input = AudioInput::new(file_name, content_type, bytes)
            .map_err(|error| openai_error(error, request_id))?;
        let started = Instant::now();
        let transcription = openai
            .transcribe(TranscriptionRequest::new(input))
            .await
            .map_err(|error| openai_error(error, request_id))?;
        tracing::info!(%request_id,action="transcribe",provider=%provider_name,model=TRANSCRIPTION_MODEL,duration_ms=started.elapsed().as_millis(),"LLM call");
        Ok(TranscriptionResponse {
            status: "complete".into(),
            provider: provider_name.into(),
            input_model: model.into(),
            transcription_model: TRANSCRIPTION_MODEL.into(),
            text: transcription.text,
            usage: transcription.usage.map(transcription_usage_json),
        })
    }
}

async fn health(State(state): State<Service>) -> Json<Value> {
    Json(json!({
        "service":"kennedy",
        "status":"ok",
        "transcription":if state.openai.is_some(){"ready"}else{"unconfigured"},
        "gemini_search":if state.gemini.is_some(){"ready"}else{"unconfigured"},
    }))
}

async fn list_providers(State(state): State<Service>) -> Json<Value> {
    let providers = state
        .providers
        .iter()
        .map(|(name, provider)| {
            let limits = provider
                .model_limits
                .get(provider.config.default_model)
                .expect("startup validates the default model limit");
            let model_capabilities = provider
                .config
                .models
                .iter()
                .map(|model| {
                    let limits = provider
                        .model_limits
                        .get(model)
                        .expect("startup validates every model limit");
                    (
                        (*model).to_owned(),
                        json!({
                            "input_modalities":model_input_modalities(&provider.config,model),
                            "context_window_tokens":limits.context_window_tokens(),
                            "max_input_tokens":limits.max_input_tokens(),
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            json!({
                "name":name,
                "kind":provider.config.kind,
                "default_model":provider.config.default_model,
                "models":provider.config.models,
                "reasoning_effort":provider.config.reasoning_effort.as_str(),
                "context_window_tokens":limits.context_window_tokens(),
                "max_input_tokens":limits.max_input_tokens(),
                "input_modalities":model_input_modalities(&provider.config,provider.config.default_model),
                "model_capabilities":model_capabilities,
                "transcription_available":state.openai.is_some(),
                "fast_search_available":state.gemini.is_some(),
            })
        })
        .collect::<Vec<_>>();
    Json(json!({"default_provider":state.default_provider,"providers":providers}))
}

fn model_input_modalities(provider: &ProviderConfig, model: &str) -> Vec<&'static str> {
    let mut modalities = vec!["text"];
    if matches!(
        model,
        "gpt-5.6-sol" | "gpt-5.6" | "gpt-5.6-terra" | "gpt-5.6-luna"
    ) {
        modalities.push("image");
    }
    if provider.native_audio_input_models.contains(&model) {
        modalities.push("audio");
    }
    modalities
}

fn selected_provider<'a>(
    state: &'a Service,
    requested_provider: Option<&'a str>,
    requested_model: Option<&'a str>,
) -> Result<(&'a str, &'a ProviderRuntime, &'a str), ApiError> {
    let provider_name = requested_provider.unwrap_or(state.default_provider);
    let provider = state.providers.get(provider_name).ok_or_else(|| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "provider_not_configured",
            "Provider is not configured.",
        )
    })?;
    let model = requested_model.unwrap_or(provider.config.default_model);
    if !provider.config.models.contains(&model) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "provider_not_configured",
            "Model is not configured for this provider.",
        ));
    }
    Ok((provider_name, provider, model))
}

fn validate_generate_request(request: &GenerateRequest) -> Result<(), ApiError> {
    if request.chatend.trim().is_empty() {
        return Err(ApiError::invalid("chatend must not be empty."));
    }
    if request.chatend.chars().count() > MAX_CODEX_INPUT_CHARACTERS {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "input_too_large",
            format!("The Codex input exceeds {MAX_CODEX_INPUT_CHARACTERS} characters."),
        ));
    }
    if request
        .previous_response_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || Uuid::parse_str(value).is_err())
    {
        return Err(ApiError::invalid(
            "previous_response_id must be a Codex thread UUID.",
        ));
    }
    if request.timeout_seconds == Some(0) {
        return Err(ApiError::invalid(
            "timeout_seconds must be greater than zero.",
        ));
    }
    Ok(())
}

async fn generate(
    State(state): State<Service>,
    Json(request): Json<GenerateRequest>,
) -> Result<Json<NormalizedResponse>, ApiError> {
    validate_generate_request(&request)?;
    let (provider_name, provider, model) = selected_provider(
        &state,
        request.provider.as_deref(),
        request.model.as_deref(),
    )?;
    let request_id = request.operation_id.unwrap_or_else(Uuid::new_v4);
    if let Some(thread_id) = request.previous_response_id.as_deref() {
        state
            .clean_codex_threads
            .require_known(thread_id)
            .map_err(|error| error.with_request_id(request_id))?;
    }
    let mut operation = state.active_operations.register(request_id)?;
    let mut codex_request = GenerationRequest::new(request.chatend, model);
    codex_request.reasoning_effort = provider.config.reasoning_effort;
    codex_request.previous_thread_id = request.previous_response_id;
    codex_request.timeout = request
        .timeout_seconds
        .map(Duration::from_secs)
        .unwrap_or(provider.config.timeout)
        .min(provider.config.timeout);
    let started = Instant::now();
    let turn = tokio::select! {
        _ = operation.cancelled() => Err(ApiError::cancelled(request_id)),
        result = state.codex.generate(codex_request) => result.map_err(|error| codex_error(error, request_id)),
    }
    .inspect_err(|error| {
        tracing::warn!(%request_id,action="generate",provider=%provider_name,%model,code=%error.code,duration_ms=started.elapsed().as_millis(),"LLM call failed");
    })?;
    state.clean_codex_threads.remember(&turn.thread_id)?;
    let usage = turn.usage.map(Usage::from);
    tracing::info!(%request_id,action="generate",provider=%provider_name,%model,duration_ms=started.elapsed().as_millis(),input_tokens=?usage.as_ref().map(|value|value.input_tokens),output_tokens=?usage.as_ref().map(|value|value.output_tokens),"LLM call");
    Ok(Json(NormalizedResponse {
        status: "complete".into(),
        message: Message {
            role: "assistant".into(),
            content: turn.answer,
        },
        response_id: turn.thread_id,
        usage,
    }))
}

async fn web_search(
    State(state): State<Service>,
    Json(request): Json<WebSearchRequest>,
) -> Result<Json<WebSearchResponse>, ApiError> {
    let question = request.question.trim();
    if question.is_empty() || question.chars().count() > 4_000 {
        return Err(ApiError::invalid(
            "question must contain between 1 and 4000 characters.",
        ));
    }
    let (provider_name, _, _) = selected_provider(
        &state,
        request.provider.as_deref(),
        request.model.as_deref(),
    )?;
    let request_id = request.operation_id.unwrap_or_else(Uuid::new_v4);
    let mut operation = state.active_operations.register(request_id)?;
    let mode = request.mode;
    let started = Instant::now();
    let search = async {
        match mode.profile() {
            SearchProfile::Gemini => {
                let gemini = state.gemini.as_ref().ok_or_else(|| {
                    ApiError::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "provider_not_configured",
                        "Fast web search is not configured.",
                    )
                    .with_request_id(request_id)
                })?;
                let response = tokio::time::timeout(
                    FAST_SEARCH_TIMEOUT,
                    gemini.grounded_search(GroundedSearchRequest::new(question)),
                )
                .await
                .map_err(|_| {
                    ApiError::new(
                        StatusCode::GATEWAY_TIMEOUT,
                        "provider_timeout",
                        "Fast web search timed out.",
                    )
                    .with_request_id(request_id)
                })?
                .map_err(|error| gemini_error(error, request_id))?;
                let kcode_gemini_api::GroundedSearchResponse {
                    interaction,
                    sources,
                } = response;
                let answer = interaction
                    .text
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        ApiError::new(
                            StatusCode::BAD_GATEWAY,
                            "provider_error",
                            "Gemini search returned no answer text.",
                        )
                        .with_request_id(request_id)
                    })?;
                Ok::<_, ApiError>((
                    "gemini".to_owned(),
                    interaction.model,
                    answer,
                    sources
                        .into_iter()
                        .map(|source| WebSource {
                            title: source.title,
                            url: source.url,
                        })
                        .collect::<Vec<_>>(),
                    Some(Usage::from(interaction.usage)),
                ))
            }
            SearchProfile::Codex {
                model,
                reasoning,
                context,
                depth,
                timeout,
            } => {
                let response = state
                    .codex
                    .web_search(CodexSearchRequest {
                        question: question.to_owned(),
                        model: model.into(),
                        reasoning_effort: reasoning,
                        context,
                        depth,
                        timeout,
                    })
                    .await
                    .map_err(|error| codex_error(error, request_id))?;
                Ok((
                    provider_name.to_owned(),
                    model.to_owned(),
                    response.answer,
                    response
                        .sources
                        .into_iter()
                        .map(|source| WebSource {
                            title: source.title,
                            url: source.url,
                        })
                        .collect::<Vec<_>>(),
                    response.usage.map(Usage::from),
                ))
            }
        }
    };
    let (execution_provider, model, answer, sources, usage) = tokio::select! {
        _ = operation.cancelled() => Err(ApiError::cancelled(request_id)),
        result = search => result,
    }
    .inspect_err(|error| {
        tracing::warn!(%request_id,action="web_search",?mode,code=%error.code,duration_ms=started.elapsed().as_millis(),"LLM call failed");
    })?;
    tracing::info!(%request_id,action="web_search",provider=%execution_provider,%model,?mode,duration_ms=started.elapsed().as_millis(),source_count=sources.len(),"LLM call");
    Ok(Json(WebSearchResponse {
        answer,
        sources,
        provider: execution_provider,
        model,
        mode,
        usage,
    }))
}

async fn web_fetch(
    State(state): State<Service>,
    Json(request): Json<WebFetchRequest>,
) -> Result<Json<WebFetchResponse>, ApiError> {
    let request_id = request.operation_id.unwrap_or_else(Uuid::new_v4);
    let mut operation = state.active_operations.register(request_id)?;
    let fetched = tokio::select! {
        _ = operation.cancelled() => Err(ApiError::cancelled(request_id)),
        result = state.web_fetcher.fetch(&request.url) => result.map_err(|error| web_fetch_error(error, request_id)),
    }?;
    Ok(Json(WebFetchResponse {
        url: fetched.url,
        title: fetched.title,
        content_type: fetched.content_type,
        content: fetched.content,
        truncated: fetched.truncated,
        retrieved_at: DateTime::<Utc>::from(fetched.retrieved_at),
    }))
}

async fn extract_document(
    State(state): State<Service>,
    mut multipart: Multipart,
) -> Result<Json<DocumentExtractionResponse>, ApiError> {
    let request_id = Uuid::new_v4();
    let mut upload = None;
    while let Some(field) = multipart.next_field().await.map_err(|_| {
        ApiError::invalid("The multipart document request could not be read.")
            .with_request_id(request_id)
    })? {
        if field.name() != Some("file") {
            continue;
        }
        if upload.is_some() {
            return Err(ApiError::invalid("Exactly one document file is required.")
                .with_request_id(request_id));
        }
        let file_name = field.file_name().unwrap_or("document").to_owned();
        let content_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_ascii_lowercase();
        let data = field
            .bytes()
            .await
            .map_err(|_| ApiError::invalid("The document could not be read."))?
            .to_vec();
        upload = Some(DocumentInput {
            file_name,
            content_type,
            data,
        });
    }
    let input = upload.ok_or_else(|| {
        ApiError::invalid("One document file field named 'file' is required.")
            .with_request_id(request_id)
    })?;
    Ok(Json(state.extract_document_input(input, request_id).await?))
}

async fn transcribe_audio(
    State(state): State<Service>,
    mut multipart: Multipart,
) -> Result<Json<TranscriptionResponse>, ApiError> {
    let request_id = Uuid::new_v4();
    let mut requested_provider = None;
    let mut requested_model = None;
    let mut audio = None;
    while let Some(field) = multipart.next_field().await.map_err(|_| {
        ApiError::invalid("The multipart audio request could not be read.")
            .with_request_id(request_id)
    })? {
        let name = field.name().unwrap_or("").to_owned();
        if name == "provider" || name == "model" {
            let value = field.text().await.map_err(|_| {
                ApiError::invalid("An audio request field was not valid text.")
                    .with_request_id(request_id)
            })?;
            if name == "provider" {
                requested_provider = Some(value);
            } else {
                requested_model = Some(value);
            }
            continue;
        }
        if name != "file" {
            continue;
        }
        if audio.is_some() {
            return Err(ApiError::invalid("Exactly one audio file is required.")
                .with_request_id(request_id));
        }
        let content_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_ascii_lowercase();
        let file_name = safe_audio_filename(field.file_name(), &content_type);
        let bytes = field
            .bytes()
            .await
            .map_err(|_| ApiError::invalid("The audio file could not be read."))?
            .to_vec();
        audio = Some((bytes, file_name, content_type));
    }
    let (bytes, file_name, content_type) = audio.ok_or_else(|| {
        ApiError::invalid("One audio file field named 'file' is required.")
            .with_request_id(request_id)
    })?;
    Ok(Json(
        state
            .transcribe_input(
                requested_provider.as_deref(),
                requested_model.as_deref(),
                bytes,
                file_name,
                content_type,
                request_id,
            )
            .await?,
    ))
}

fn safe_audio_filename(value: Option<&str>, content_type: &str) -> String {
    let extension = match content_type {
        "audio/ogg" | "audio/opus" | "application/ogg" => "ogg",
        "audio/wav" | "audio/x-wav" => "wav",
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/mp4" | "video/mp4" => "mp4",
        "audio/webm" | "video/webm" => "webm",
        "audio/flac" | "audio/x-flac" => "flac",
        "audio/m4a" => "m4a",
        _ => "audio",
    };
    let cleaned = value
        .unwrap_or("")
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
        .take(120)
        .collect::<String>();
    if cleaned.is_empty() {
        format!("voice-note.{extension}")
    } else {
        cleaned
    }
}

fn transcription_usage_json(usage: TranscriptionUsage) -> Value {
    match usage {
        TranscriptionUsage::DurationSeconds(seconds) => {
            json!({"type":"duration","seconds":seconds})
        }
        TranscriptionUsage::Tokens(tokens) => json!({
            "type":"tokens",
            "input_tokens":tokens.input_tokens,
            "output_tokens":tokens.output_tokens,
            "total_tokens":tokens.total_tokens,
            "input_token_details":tokens.input_details.map(|details|json!({
                "audio_tokens":details.audio_tokens,
                "text_tokens":details.text_tokens,
            })),
        }),
    }
}

async fn cancel_operation(
    State(state): State<Service>,
    Path(operation_id): Path<Uuid>,
) -> Result<Json<OperationCancellationResponse>, ApiError> {
    Ok(Json(OperationCancellationResponse {
        cancelled: state.active_operations.cancel(operation_id)?,
    }))
}

fn codex_error(error: kcode_codex_runtime::Error, request_id: Uuid) -> ApiError {
    let (status, code) = match error.kind() {
        CodexErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_request"),
        CodexErrorKind::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "provider_unavailable"),
        CodexErrorKind::Authentication => (StatusCode::UNAUTHORIZED, "provider_auth_failed"),
        CodexErrorKind::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "provider_rate_limited"),
        CodexErrorKind::Capacity => (StatusCode::SERVICE_UNAVAILABLE, "provider_capacity"),
        CodexErrorKind::Timeout => (StatusCode::GATEWAY_TIMEOUT, "provider_timeout"),
        CodexErrorKind::InputTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "input_too_large"),
        CodexErrorKind::EmptyOutput => (StatusCode::BAD_GATEWAY, "empty_assistant_message"),
        CodexErrorKind::Protocol => (StatusCode::BAD_GATEWAY, "provider_error"),
    };
    ApiError::new(status, code, error.message()).with_request_id(request_id)
}

fn gemini_error(error: GeminiError, request_id: Uuid) -> ApiError {
    let (status, code) = match &error {
        GeminiError::InvalidApiKey => (StatusCode::SERVICE_UNAVAILABLE, "provider_not_configured"),
        GeminiError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
        GeminiError::Accounting(_) => (StatusCode::INTERNAL_SERVER_ERROR, "provider_error"),
        GeminiError::Transport(_) => (StatusCode::BAD_GATEWAY, "provider_error"),
        GeminiError::Provider { status, .. } => (
            StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY),
            "provider_error",
        ),
        GeminiError::Protocol(_) => (StatusCode::BAD_GATEWAY, "provider_error"),
        GeminiError::SpendingLimitReached { .. } => {
            (StatusCode::TOO_MANY_REQUESTS, "provider_rate_limited")
        }
    };
    ApiError::new(status, code, error.to_string()).with_request_id(request_id)
}

fn openai_error(error: OpenAiError, request_id: Uuid) -> ApiError {
    let (status, code) = match &error {
        OpenAiError::InvalidApiKey => {
            (StatusCode::SERVICE_UNAVAILABLE, "transcription_unavailable")
        }
        OpenAiError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
        OpenAiError::Transport(message) if message.contains("timed out") => {
            (StatusCode::GATEWAY_TIMEOUT, "transcription_timeout")
        }
        OpenAiError::Transport(_) => (StatusCode::BAD_GATEWAY, "transcription_failed"),
        OpenAiError::Provider { status, .. } => (
            StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY),
            "transcription_failed",
        ),
        OpenAiError::Protocol(_) => (StatusCode::BAD_GATEWAY, "transcription_failed"),
    };
    ApiError::new(status, code, error.to_string()).with_request_id(request_id)
}

fn web_fetch_error(error: kcode_web_fetch::Error, request_id: Uuid) -> ApiError {
    let (status, code) = match error.kind() {
        WebFetchErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_request"),
        WebFetchErrorKind::UnsafeDestination => (StatusCode::BAD_REQUEST, "unsafe_web_url"),
        WebFetchErrorKind::Timeout => (StatusCode::GATEWAY_TIMEOUT, "web_fetch_timeout"),
        WebFetchErrorKind::UnsupportedContent => (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_web_content",
        ),
        WebFetchErrorKind::Transport
        | WebFetchErrorKind::HttpStatus
        | WebFetchErrorKind::EmptyContent => (StatusCode::BAD_GATEWAY, "web_fetch_failed"),
    };
    ApiError::new(status, code, error.message()).with_request_id(request_id)
}

fn document_error(error: kcode_doc_extraction::Error, request_id: Uuid) -> ApiError {
    let (status, code) = match error.kind() {
        DocumentErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_request"),
        DocumentErrorKind::UnsupportedFormat => {
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_document")
        }
        DocumentErrorKind::ExtractionFailed => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "document_extraction_failed",
        ),
        DocumentErrorKind::EmptyText => (StatusCode::UNPROCESSABLE_ENTITY, "document_text_empty"),
    };
    ApiError::new(status, code, error.message()).with_request_id(request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_modes_have_fixed_provider_profiles() {
        assert!(matches!(
            WebSearchMode::Fast.profile(),
            SearchProfile::Gemini
        ));
        assert!(matches!(
            WebSearchMode::Balanced.profile(),
            SearchProfile::Codex {
                model: BALANCED_SEARCH_MODEL,
                timeout: BALANCED_SEARCH_TIMEOUT,
                ..
            }
        ));
        assert!(matches!(
            WebSearchMode::Quality.profile(),
            SearchProfile::Codex {
                model: QUALITY_SEARCH_MODEL,
                timeout: QUALITY_SEARCH_TIMEOUT,
                ..
            }
        ));
    }

    #[test]
    fn search_requests_reject_caller_selected_limits() {
        let base = json!({"question":"What changed?","mode":"fast"});
        assert!(serde_json::from_value::<WebSearchRequest>(base).is_ok());
        assert!(
            serde_json::from_value::<WebSearchRequest>(
                json!({"question":"What changed?","mode":"fast","timeout_seconds":10})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<WebSearchRequest>(
                json!({"question":"What changed?","mode":"fast","max_sources":2})
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn operations_can_be_cancelled_by_identifier() {
        let operations = ActiveOperations::default();
        let id = Uuid::new_v4();
        let mut operation = operations.register(id).unwrap();
        assert!(operations.cancel(id).unwrap());
        operation.cancelled().await;
        drop(operation);
        assert!(!operations.cancel(id).unwrap());
    }

    #[test]
    fn only_current_process_threads_can_resume() {
        let threads = CleanCodexThreads::default();
        let id = Uuid::new_v4().to_string();
        assert!(threads.require_known(&id).is_err());
        threads.remember(&id).unwrap();
        threads.require_known(&id).unwrap();
    }
}
