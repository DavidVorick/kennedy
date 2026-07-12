use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "./IntelligenceBackend/config.yaml")]
    config: PathBuf,
}

#[derive(Clone, Deserialize)]
struct Config {
    server: ServerConfig,
    default_provider: String,
    providers: HashMap<String, ProviderConfig>,
}

#[derive(Clone, Deserialize)]
struct ServerConfig {
    bind: String,
    max_request_bytes: usize,
    allowed_origins: Vec<String>,
}

#[derive(Clone, Deserialize)]
struct ProviderConfig {
    kind: String,
    api_key: String,
    base_url: String,
    default_model: String,
    models: Vec<String>,
    reasoning_effort: String,
    timeout_seconds: u64,
    #[serde(default)]
    context_window_tokens: Option<u64>,
    #[serde(default)]
    max_input_tokens: Option<u64>,
}

#[derive(Clone)]
struct ProviderRuntime {
    config: ProviderConfig,
    api_key: String,
    client: Client,
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    providers: Arc<HashMap<String, ProviderRuntime>>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    request_id: Option<Uuid>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            request_id: None,
        }
    }
    fn with_request_id(mut self, request_id: Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }
    fn provider(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "provider_not_configured", message)
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
    messages: Vec<Message>,
    #[serde(default)]
    previous_response_id: Option<String>,
    #[serde(default)]
    prompt_cache_key: Option<String>,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kennedy_intelligence=info,tower_http=info".into()),
        )
        .init();
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("installing TLS crypto provider"))?;
    let args = Args::parse();
    let raw = tokio::fs::read_to_string(&args.config)
        .await
        .with_context(|| {
            format!(
                "reading {} (copy config.example.yaml to config.yaml)",
                args.config.display()
            )
        })?;
    let config: Config = serde_yaml::from_str(&raw).context("parsing intelligence config")?;
    let providers = initialize_providers(&config)?;
    let origins = config
        .server
        .allowed_origins
        .iter()
        .map(|value| {
            value
                .parse::<HeaderValue>()
                .with_context(|| format!("invalid allowed origin {value}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([HeaderName::from_static("content-type")]);
    let state = AppState {
        config: Arc::new(config.clone()),
        providers: Arc::new(providers),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/providers", get(list_providers))
        .route("/api/v1/generate", post(generate))
        .layer(DefaultBodyLimit::max(config.server.max_request_bytes))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    tracing::info!(address=%config.server.bind,"Kennedy intelligence bridge listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn initialize_providers(config: &Config) -> anyhow::Result<HashMap<String, ProviderRuntime>> {
    let default = config
        .providers
        .get(&config.default_provider)
        .context("default_provider is not configured")?;
    if !default.models.contains(&default.default_model) {
        anyhow::bail!("default provider model is not listed in models");
    }
    let mut runtimes = HashMap::new();
    for (name, provider) in &config.providers {
        if provider.kind != "openai" {
            anyhow::bail!("unsupported provider kind '{}' for {name}", provider.kind);
        }
        if !provider.models.contains(&provider.default_model) {
            anyhow::bail!("provider {name} default_model is not listed in models");
        }
        let api_key = provider.api_key.trim().to_owned();
        if api_key.is_empty() || api_key == "replace-with-your-openai-api-key" {
            anyhow::bail!("provider {name} credential is missing from config.yaml");
        }
        if !["none", "minimal", "low", "medium", "high", "xhigh", "max"]
            .contains(&provider.reasoning_effort.as_str())
        {
            anyhow::bail!(
                "provider {name} has unsupported reasoning_effort '{}'",
                provider.reasoning_effort
            );
        }
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(provider.timeout_seconds))
            .build()?;
        runtimes.insert(
            name.clone(),
            ProviderRuntime {
                config: provider.clone(),
                api_key,
                client,
            },
        );
    }
    Ok(runtimes)
}

async fn health() -> Json<Value> {
    Json(json!({"service":"intelligence","status":"ok"}))
}

async fn list_providers(State(state): State<AppState>) -> Json<Value> {
    let providers = state
        .config
        .providers
        .iter()
        .map(|(name, p)| {
            let (context_window_tokens, max_input_tokens) = model_limits(p, &p.default_model);
            json!({
                "name": name,
                "default_model": p.default_model,
                "models": p.models,
                "context_window_tokens": context_window_tokens,
                "max_input_tokens": max_input_tokens,
            })
        })
        .collect::<Vec<_>>();
    Json(json!({"default_provider":state.config.default_provider,"providers":providers}))
}

fn model_limits(provider: &ProviderConfig, model: &str) -> (u64, u64) {
    let known = match model {
        "gpt-5.6-sol" | "gpt-5.6" => (1_050_000, 922_000),
        _ => (0, 0),
    };
    (
        provider.context_window_tokens.unwrap_or(known.0),
        provider.max_input_tokens.unwrap_or(known.1),
    )
}

fn validate_request(request: &GenerateRequest) -> Result<(), ApiError> {
    if request.messages.is_empty() {
        return Err(ApiError::invalid("messages must not be empty."));
    }
    for message in &request.messages {
        match message.role.as_str() {
            "system" | "user" | "assistant" => {}
            _ => return Err(ApiError::invalid("Unknown message role.")),
        }
        if message.content.trim().is_empty() {
            return Err(ApiError::invalid("Message content must not be empty."));
        }
    }
    if request
        .previous_response_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ApiError::invalid("previous_response_id must not be empty."));
    }
    if let Some(key) = request.prompt_cache_key.as_deref() {
        if key.trim().is_empty() || key.len() > 64 {
            return Err(ApiError::invalid(
                "prompt_cache_key must contain 1 to 64 bytes.",
            ));
        }
    }
    Ok(())
}

fn provider_request_body(request: &GenerateRequest, model: &str, reasoning_effort: &str) -> Value {
    let input = request
        .messages
        .iter()
        .map(|message| json!({"role": message.role, "content": message.content}))
        .collect::<Vec<_>>();
    let mut body = json!({
        "model": model,
        "input": input,
        "reasoning": {"effort": reasoning_effort, "context": "all_turns"},
        "store": true,
    });
    if let Some(previous_response_id) = &request.previous_response_id {
        body["previous_response_id"] = json!(previous_response_id);
    }
    if let Some(prompt_cache_key) = &request.prompt_cache_key {
        body["prompt_cache_key"] = json!(prompt_cache_key);
        body["prompt_cache_options"] = json!({"mode": "implicit", "ttl": "30m"});
    }
    body
}

async fn generate(
    State(state): State<AppState>,
    Json(request): Json<GenerateRequest>,
) -> Result<Json<NormalizedResponse>, ApiError> {
    validate_request(&request)?;
    let provider_name = request
        .provider
        .as_deref()
        .unwrap_or(&state.config.default_provider);
    let provider = state
        .providers
        .get(provider_name)
        .ok_or_else(|| ApiError::provider("Provider is not configured."))?;
    let model = request
        .model
        .as_deref()
        .unwrap_or(&provider.config.default_model);
    if !provider.config.models.iter().any(|m| m == model) {
        return Err(ApiError::provider(
            "Model is not configured for this provider.",
        ));
    }
    let body = provider_request_body(&request, model, &provider.config.reasoning_effort);
    let request_id = Uuid::new_v4();
    let started = Instant::now();
    let url = format!(
        "{}/responses",
        provider.config.base_url.trim_end_matches('/')
    );
    let response = provider
        .client
        .post(url)
        .bearer_auth(&provider.api_key)
        .json(&body)
        .send()
        .await
        .map_err(|error| map_transport_error(error).with_request_id(request_id))?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(|_| {
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider returned a response that was not valid JSON.",
        )
        .with_request_id(request_id)
    })?;
    if !status.is_success() {
        let mapped = map_provider_status(status, &payload).with_request_id(request_id);
        tracing::warn!(%request_id,provider=%provider_name,%model,status=%status,code=%mapped.code,provider_code=?provider_error_field(&payload, "code"),provider_type=?provider_error_field(&payload, "type"),provider_param=?provider_error_field(&payload, "param"),latency_ms=started.elapsed().as_millis(),"provider request failed");
        return Err(mapped);
    }
    let normalized =
        normalize_openai(payload).map_err(|error| error.with_request_id(request_id))?;
    tracing::info!(
        %request_id,
        provider=%provider_name,
        %model,
        status="ok",
        latency_ms=started.elapsed().as_millis(),
        input_tokens=?normalized.usage.as_ref().map(|u|u.input_tokens),
        output_tokens=?normalized.usage.as_ref().map(|u|u.output_tokens),
        cached_tokens=?normalized.usage.as_ref().map(|u|u.cached_tokens),
        cache_write_tokens=?normalized.usage.as_ref().map(|u|u.cache_write_tokens),
        "generation complete"
    );
    Ok(Json(normalized))
}

fn map_transport_error(error: reqwest::Error) -> ApiError {
    if error.is_timeout() {
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "provider_timeout",
            "The provider request timed out.",
        )
    } else {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider_unavailable",
            "The provider could not be reached.",
        )
    }
}

fn provider_error_field<'a>(payload: &'a Value, field: &str) -> Option<&'a str> {
    payload.get("error")?.get(field)?.as_str()
}

fn clean_provider_message(payload: &Value) -> Option<String> {
    let message = provider_error_field(payload, "message")?;
    let cleaned = message
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(500)
        .collect::<String>();
    let cleaned = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    (!cleaned.is_empty()).then_some(cleaned)
}

fn map_provider_status(status: reqwest::StatusCode, payload: &Value) -> ApiError {
    let detail = clean_provider_message(payload);
    match status.as_u16() {
        401 | 403 => {
            let message = detail
                .filter(|message| message.to_ascii_lowercase().contains("insufficient permission"))
                .map(|_| "The provider credentials do not have permission to create model responses. Update the API key permissions to allow model requests.".to_string())
                .unwrap_or_else(|| "The provider rejected its configured credentials. Check the API key and its project permissions.".to_string());
            ApiError::new(StatusCode::UNAUTHORIZED, "provider_auth_failed", message)
        }
        429 => ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "provider_rate_limited",
            detail.unwrap_or_else(|| "The provider rate limit was reached.".into()),
        ),
        _ => {
            let message = detail
                .map(|detail| format!("Provider rejected the request: {detail}"))
                .unwrap_or_else(|| format!("Provider returned HTTP {status}."));
            ApiError::new(StatusCode::BAD_GATEWAY, "provider_error", message)
        }
    }
}

fn normalize_openai(payload: Value) -> Result<NormalizedResponse, ApiError> {
    let response_id = payload
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "provider_error",
                "Provider response did not contain a response ID.",
            )
        })?
        .to_string();
    let output = payload
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "provider_error",
                "Provider response did not contain an output array.",
            )
        })?;
    let mut text = Vec::new();
    for item in output {
        if item.get("type").and_then(Value::as_str) == Some("message") {
            if let Some(parts) = item.get("content").and_then(Value::as_array) {
                text.extend(parts.iter().filter_map(|part| {
                    (part.get("type").and_then(Value::as_str) == Some("output_text"))
                        .then(|| part.get("text").and_then(Value::as_str))
                        .flatten()
                        .map(str::to_string)
                }));
            }
        }
    }
    let content = text.join("");
    if content.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider returned no text.",
        ));
    }
    let usage = payload.get("usage").and_then(|u| {
        Some(Usage {
            input_tokens: u.get("input_tokens")?.as_u64()?,
            output_tokens: u.get("output_tokens")?.as_u64()?,
            cached_tokens: u
                .get("input_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0),
            cache_write_tokens: u
                .get("input_tokens_details")
                .and_then(|details| details.get("cache_write_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0),
            reasoning_tokens: u
                .get("output_tokens_details")
                .and_then(|details| details.get("reasoning_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0),
        })
    });
    Ok(NormalizedResponse {
        status: "complete".into(),
        message: Message {
            role: "assistant".into(),
            content,
        },
        response_id,
        usage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(messages: Vec<Message>) -> GenerateRequest {
        GenerateRequest {
            provider: None,
            model: None,
            messages,
            previous_response_id: None,
            prompt_cache_key: None,
        }
    }
    fn text(role: &str, content: &str) -> Message {
        Message {
            role: role.into(),
            content: content.into(),
        }
    }
    #[test]
    fn empty_messages_fail() {
        assert!(validate_request(&request(vec![])).is_err());
    }
    #[test]
    fn non_text_transport_roles_fail() {
        assert!(validate_request(&request(vec![text("tool", "opaque")])).is_err());
    }
    #[test]
    fn complete_text_normalizes() {
        let payload = json!({
            "id":"resp_123",
            "output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}],
            "usage":{
                "input_tokens":100,
                "input_tokens_details":{"cached_tokens":80,"cache_write_tokens":10},
                "output_tokens":20,
                "output_tokens_details":{"reasoning_tokens":5}
            }
        });
        let result = normalize_openai(payload).unwrap();
        assert_eq!(result.status, "complete");
        assert_eq!(result.response_id, "resp_123");
        assert_eq!(result.message.content, "hello");
        let usage = result.usage.unwrap();
        assert_eq!(usage.cached_tokens, 80);
        assert_eq!(usage.cache_write_tokens, 10);
        assert_eq!(usage.reasoning_tokens, 5);
    }
    #[test]
    fn text_message_is_valid() {
        assert!(validate_request(&request(vec![text("user", "hi")])).is_ok());
    }
    #[test]
    fn provider_request_uses_stateful_cached_responses_shape() {
        let mut request = request(vec![text("user", "Memory tool results\nLoaded node 3")]);
        request.previous_response_id = Some("resp_previous".into());
        request.prompt_cache_key = Some("kennedy-session-1".into());
        let body = provider_request_body(&request, "gpt-5.6-sol", "xhigh");
        assert_eq!(body["model"], "gpt-5.6-sol");
        assert_eq!(body["reasoning"]["effort"], "xhigh");
        assert_eq!(body["reasoning"]["context"], "all_turns");
        assert_eq!(
            body["input"][0]["content"],
            "Memory tool results\nLoaded node 3"
        );
        assert_eq!(body["store"], true);
        assert_eq!(body["previous_response_id"], "resp_previous");
        assert_eq!(body["prompt_cache_key"], "kennedy-session-1");
        assert_eq!(
            body["prompt_cache_options"],
            json!({"mode":"implicit","ttl":"30m"})
        );
        assert!(body.get("tools").is_none());
    }
    #[test]
    fn provider_permission_error_is_actionable() {
        let error = map_provider_status(
            reqwest::StatusCode::UNAUTHORIZED,
            &json!({"error":{"message":"You have insufficient permissions for this operation."}}),
        );
        assert_eq!(error.code, "provider_auth_failed");
        assert!(
            error
                .message
                .contains("permission to create model responses")
        );
    }
    #[test]
    fn provider_validation_error_preserves_safe_detail() {
        let error = map_provider_status(
            reqwest::StatusCode::BAD_REQUEST,
            &json!({"error":{"message":"Invalid value for reasoning.effort\n","param":"reasoning.effort"}}),
        );
        assert_eq!(error.code, "provider_error");
        assert_eq!(
            error.message,
            "Provider rejected the request: Invalid value for reasoning.effort"
        );
    }
    #[test]
    fn example_config_uses_direct_api_key_and_requested_model() {
        let config: Config = serde_yaml::from_str(include_str!("../config.example.yaml")).unwrap();
        let provider = config.providers.get("primary").unwrap();
        assert_eq!(provider.api_key, "replace-with-your-openai-api-key");
        assert_eq!(provider.default_model, "gpt-5.6-sol");
        assert_eq!(provider.reasoning_effort, "xhigh");
        assert_eq!(model_limits(provider, "gpt-5.6-sol"), (1_050_000, 922_000));
    }
}
