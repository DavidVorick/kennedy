use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

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
use serde_json::{Map, Value, json};
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
    api_key_env: String,
    base_url: String,
    default_model: String,
    models: Vec<String>,
    timeout_seconds: u64,
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
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
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
        (
            self.status,
            Json(json!({"error":{"code":self.code,"message":self.message}})),
        )
            .into_response()
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Message {
    role: String,
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    tool_calls: Vec<ToolCall>,
    #[serde(default)]
    tool_call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct ToolCall {
    id: String,
    name: String,
    arguments: Map<String, Value>,
}

#[derive(Clone, Deserialize)]
struct ToolDefinition {
    name: String,
    description: String,
    input_schema: Value,
}

#[derive(Deserialize)]
struct GenerateRequest {
    provider: Option<String>,
    model: Option<String>,
    messages: Vec<Message>,
    #[serde(default)]
    tools: Vec<ToolDefinition>,
}

#[derive(Serialize)]
struct NormalizedResponse {
    status: String,
    message: Message,
    usage: Option<Usage>,
}

#[derive(Serialize, Deserialize)]
struct Usage {
    input_tokens: u64,
    output_tokens: u64,
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
        let api_key = std::env::var(&provider.api_key_env).with_context(|| {
            format!(
                "environment variable {} is required for provider {name}",
                provider.api_key_env
            )
        })?;
        if api_key.trim().is_empty() {
            anyhow::bail!("provider {name} credential is empty");
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
        .map(|(name, p)| json!({"name":name,"default_model":p.default_model,"models":p.models}))
        .collect::<Vec<_>>();
    Json(json!({"default_provider":state.config.default_provider,"providers":providers}))
}

fn validate_schema(schema: &Value, path: &str) -> Result<(), ApiError> {
    let object = schema
        .as_object()
        .ok_or_else(|| ApiError::invalid(format!("{path} must be an object schema.")))?;
    let allowed = [
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
    ];
    if let Some(key) = object.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(ApiError::invalid(format!(
            "Unsupported schema keyword {path}.{key}."
        )));
    }
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid(format!("{path}.type is required.")))?;
    if !["object", "array", "string", "integer", "number", "boolean"].contains(&kind) {
        return Err(ApiError::invalid(format!("Unsupported type at {path}.")));
    }
    if kind == "object" {
        let props = object
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| ApiError::invalid(format!("{path}.properties is required.")))?;
        let required = object
            .get("required")
            .and_then(Value::as_array)
            .ok_or_else(|| ApiError::invalid(format!("{path}.required is required.")))?;
        for entry in required {
            let name = entry.as_str().ok_or_else(|| {
                ApiError::invalid(format!("{path}.required must contain strings."))
            })?;
            if !props.contains_key(name) {
                return Err(ApiError::invalid(format!(
                    "Required property {name} is not defined."
                )));
            }
        }
        if object.get("additionalProperties").and_then(Value::as_bool) != Some(false) {
            return Err(ApiError::invalid(format!(
                "{path}.additionalProperties must be false."
            )));
        }
        for (name, child) in props {
            validate_schema(child, &format!("{path}.properties.{name}"))?;
        }
    }
    if kind == "array" {
        validate_schema(
            object
                .get("items")
                .ok_or_else(|| ApiError::invalid(format!("{path}.items is required.")))?,
            &format!("{path}.items"),
        )?;
    }
    Ok(())
}

fn validate_request(request: &GenerateRequest) -> Result<(), ApiError> {
    if request.messages.is_empty() {
        return Err(ApiError::invalid("messages must not be empty."));
    }
    let mut calls: HashMap<&str, &str> = HashMap::new();
    let mut results = HashSet::new();
    for message in &request.messages {
        match message.role.as_str() {
            "system" | "user" => {
                if !matches!(&message.content, Some(Value::String(_))) {
                    return Err(ApiError::invalid(format!(
                        "{} messages require string content.",
                        message.role
                    )));
                }
                if !message.tool_calls.is_empty() {
                    return Err(ApiError::invalid(
                        "Only assistant messages may contain tool calls.",
                    ));
                }
            }
            "assistant" => {
                if !matches!(
                    &message.content,
                    None | Some(Value::Null) | Some(Value::String(_))
                ) {
                    return Err(ApiError::invalid(
                        "Assistant content must be string or null.",
                    ));
                }
                if message.content.is_none() && message.tool_calls.is_empty() {
                    return Err(ApiError::invalid(
                        "Assistant messages require content or tool calls.",
                    ));
                }
                for call in &message.tool_calls {
                    if calls.insert(&call.id, &call.name).is_some() {
                        return Err(ApiError::invalid("Assistant tool-call IDs must be unique."));
                    }
                }
            }
            "tool" => {
                let id = message
                    .tool_call_id
                    .as_deref()
                    .ok_or_else(|| ApiError::invalid("Tool results require tool_call_id."))?;
                let name = message
                    .name
                    .as_deref()
                    .ok_or_else(|| ApiError::invalid("Tool results require name."))?;
                if calls.get(id) != Some(&name) {
                    return Err(ApiError::invalid(
                        "Tool result does not match an earlier tool call.",
                    ));
                }
                if !results.insert(id) {
                    return Err(ApiError::invalid("A tool call may have only one result."));
                }
                if message.content.is_none() {
                    return Err(ApiError::invalid("Tool results require content."));
                }
            }
            _ => return Err(ApiError::invalid("Unknown message role.")),
        }
    }
    let mut names = HashSet::new();
    for tool in &request.tools {
        if tool.name.trim().is_empty() || !names.insert(&tool.name) {
            return Err(ApiError::invalid(
                "Tool names must be non-empty and unique.",
            ));
        }
        validate_schema(&tool.input_schema, &format!("tool {}", tool.name))?;
    }
    Ok(())
}

fn provider_message(message: &Message) -> Value {
    match message.role.as_str() {
        "tool" => {
            json!({"role":"tool","tool_call_id":message.tool_call_id,"content":serde_json::to_string(message.content.as_ref().unwrap_or(&Value::Null)).unwrap_or_else(|_|"null".into())})
        }
        "assistant" if !message.tool_calls.is_empty() => {
            json!({"role":"assistant","content":message.content,"tool_calls":message.tool_calls.iter().map(|c|json!({"id":c.id,"type":"function","function":{"name":c.name,"arguments":serde_json::to_string(&c.arguments).unwrap_or_else(|_| "{}".into())}})).collect::<Vec<_>>() })
        }
        _ => {
            json!({"role":message.role,"content":message.content.as_ref().and_then(Value::as_str).unwrap_or("")})
        }
    }
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
    let mut body = json!({"model":model,"messages":request.messages.iter().map(provider_message).collect::<Vec<_>>()});
    if !request.tools.is_empty() {
        body["tools"]=Value::Array(request.tools.iter().map(|t|json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.input_schema}})).collect());
        body["tool_choice"] = json!("auto");
    }
    let request_id = Uuid::new_v4();
    let started = Instant::now();
    let url = format!(
        "{}/chat/completions",
        provider.config.base_url.trim_end_matches('/')
    );
    let response = provider
        .client
        .post(url)
        .bearer_auth(&provider.api_key)
        .json(&body)
        .send()
        .await
        .map_err(map_transport_error)?;
    let status = response.status();
    if !status.is_success() {
        let mapped = map_provider_status(status);
        tracing::warn!(%request_id,provider=%provider_name,%model,status=%status,code=%mapped.code,latency_ms=started.elapsed().as_millis(),"provider request failed");
        return Err(mapped);
    }
    let payload: Value = response.json().await.map_err(|_| {
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider returned an unusable response.",
        )
    })?;
    let normalized = normalize_openai(payload)?;
    tracing::info!(%request_id,provider=%provider_name,%model,status="ok",latency_ms=started.elapsed().as_millis(),input_tokens=?normalized.usage.as_ref().map(|u|u.input_tokens),output_tokens=?normalized.usage.as_ref().map(|u|u.output_tokens),"generation complete");
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

fn map_provider_status(status: reqwest::StatusCode) -> ApiError {
    match status.as_u16() {
        401 | 403 => ApiError::new(
            StatusCode::UNAUTHORIZED,
            "provider_auth_failed",
            "The provider rejected its configured credentials.",
        ),
        429 => ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "provider_rate_limited",
            "The provider rate limit was reached.",
        ),
        _ => ApiError::new(
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "The provider returned an error.",
        ),
    }
}

fn normalize_openai(payload: Value) -> Result<NormalizedResponse, ApiError> {
    let message = payload
        .pointer("/choices/0/message")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "provider_error",
                "Provider response did not contain a message.",
            )
        })?;
    let content = message.get("content").cloned().filter(|v| !v.is_null());
    let mut calls = Vec::new();
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for raw in tool_calls {
            let id = raw
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("call_{}", Uuid::new_v4()));
            let name = raw
                .pointer("/function/name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::BAD_GATEWAY,
                        "provider_error",
                        "Provider tool call omitted its name.",
                    )
                })?
                .to_string();
            let args = raw
                .pointer("/function/arguments")
                .and_then(Value::as_str)
                .and_then(|v| serde_json::from_str::<Value>(v).ok())
                .and_then(|v| v.as_object().cloned())
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::BAD_GATEWAY,
                        "provider_error",
                        "Provider tool arguments were not a JSON object.",
                    )
                })?;
            calls.push(ToolCall {
                id,
                name,
                arguments: args,
            });
        }
    }
    if calls.is_empty() && !matches!(&content, Some(Value::String(_))) {
        return Err(ApiError::new(
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider returned neither text nor tool calls.",
        ));
    }
    let usage = payload.get("usage").and_then(|u| {
        Some(Usage {
            input_tokens: u.get("prompt_tokens")?.as_u64()?,
            output_tokens: u.get("completion_tokens")?.as_u64()?,
        })
    });
    Ok(NormalizedResponse {
        status: if calls.is_empty() {
            "complete"
        } else {
            "tool_calls"
        }
        .into(),
        message: Message {
            role: "assistant".into(),
            content,
            tool_calls: calls,
            tool_call_id: None,
            name: None,
        },
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
            tools: vec![],
        }
    }
    fn text(role: &str, content: &str) -> Message {
        Message {
            role: role.into(),
            content: Some(json!(content)),
            tool_calls: vec![],
            tool_call_id: None,
            name: None,
        }
    }
    #[test]
    fn empty_messages_fail() {
        assert!(validate_request(&request(vec![])).is_err());
    }
    #[test]
    fn orphan_tool_result_fails() {
        let m = Message {
            role: "tool".into(),
            content: Some(json!({"ok":true})),
            tool_calls: vec![],
            tool_call_id: Some("x".into()),
            name: Some("LoadNode".into()),
        };
        assert!(validate_request(&request(vec![m])).is_err());
    }
    #[test]
    fn normalizes_multiple_calls() {
        let payload = json!({"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"a","function":{"name":"One","arguments":"{}"}},{"id":"b","function":{"name":"Two","arguments":"{\"x\":1}"}}]}}],"usage":{"prompt_tokens":10,"completion_tokens":5}});
        let result = normalize_openai(payload).unwrap();
        assert_eq!(result.status, "tool_calls");
        assert_eq!(result.message.tool_calls.len(), 2);
        assert_eq!(result.usage.unwrap().input_tokens, 10);
    }
    #[test]
    fn complete_text_normalizes() {
        let payload = json!({"choices":[{"message":{"content":"hello"}}]});
        let result = normalize_openai(payload).unwrap();
        assert_eq!(result.status, "complete");
        assert_eq!(result.message.content, Some(json!("hello")));
    }
    #[test]
    fn text_message_is_valid() {
        assert!(validate_request(&request(vec![text("user", "hi")])).is_ok());
    }
}
