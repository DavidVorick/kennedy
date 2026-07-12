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
    api_key: String,
    base_url: String,
    default_model: String,
    models: Vec<String>,
    reasoning_effort: String,
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
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    tool_calls: Vec<ToolCall>,
    #[serde(default)]
    tool_call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    provider_items: Vec<Value>,
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
                if !message.provider_items.is_empty() {
                    return Err(ApiError::invalid(
                        "Only assistant messages may contain provider items.",
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
                if !message.provider_items.is_empty() {
                    return Err(ApiError::invalid(
                        "Tool results may not contain provider items.",
                    ));
                }
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

fn provider_input_items(message: &Message) -> Vec<Value> {
    if message.role == "assistant" && !message.provider_items.is_empty() {
        return message.provider_items.clone();
    }
    match message.role.as_str() {
        "tool" => {
            let output = match message.content.as_ref().unwrap_or(&Value::Null) {
                Value::String(content) => content.clone(),
                content => serde_json::to_string(content).unwrap_or_else(|_| "null".into()),
            };
            vec![json!({
                "type": "function_call_output",
                "call_id": message.tool_call_id,
                "output": output,
            })]
        }
        "assistant" => {
            let mut items = Vec::new();
            if let Some(Value::String(content)) = &message.content {
                items.push(json!({"role":"assistant","content":content}));
            }
            items.extend(message.tool_calls.iter().map(|call| {
                json!({
                    "type": "function_call",
                    "call_id": call.id,
                    "name": call.name,
                    "arguments": serde_json::to_string(&call.arguments)
                        .unwrap_or_else(|_| "{}".into()),
                })
            }));
            items
        }
        _ => vec![json!({
            "role": message.role,
            "content": message.content.as_ref().and_then(Value::as_str).unwrap_or(""),
        })],
    }
}

fn provider_request_body(request: &GenerateRequest, model: &str, reasoning_effort: &str) -> Value {
    let input = request
        .messages
        .iter()
        .flat_map(provider_input_items)
        .collect::<Vec<_>>();
    let mut body = json!({
        "model": model,
        "input": input,
        "reasoning": {"effort": reasoning_effort},
        "store": false,
        "include": ["reasoning.encrypted_content"],
    });
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    json!({
                        "type": "function",
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                        "strict": true,
                    })
                })
                .collect(),
        );
        body["tool_choice"] = json!("auto");
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
    let mut calls = Vec::new();
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                if let Some(parts) = item.get("content").and_then(Value::as_array) {
                    text.extend(parts.iter().filter_map(|part| {
                        (part.get("type").and_then(Value::as_str) == Some("output_text"))
                            .then(|| part.get("text").and_then(Value::as_str))
                            .flatten()
                            .map(str::to_string)
                    }));
                }
            }
            Some("function_call") => {
                let id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("call_{}", Uuid::new_v4()));
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ApiError::new(
                            StatusCode::BAD_GATEWAY,
                            "provider_error",
                            "Provider tool call omitted its name.",
                        )
                    })?
                    .to_string();
                let args = item
                    .get("arguments")
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
            _ => {}
        }
    }
    let content = (!text.is_empty()).then(|| Value::String(text.join("")));
    if calls.is_empty() && !matches!(&content, Some(Value::String(_))) {
        return Err(ApiError::new(
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider returned neither text nor tool calls.",
        ));
    }
    let usage = payload.get("usage").and_then(|u| {
        Some(Usage {
            input_tokens: u.get("input_tokens")?.as_u64()?,
            output_tokens: u.get("output_tokens")?.as_u64()?,
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
            provider_items: output.clone(),
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
            provider_items: vec![],
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
            provider_items: vec![],
        };
        assert!(validate_request(&request(vec![m])).is_err());
    }
    #[test]
    fn normalizes_multiple_calls() {
        let payload = json!({
            "output": [
                {"type":"reasoning","encrypted_content":"opaque","summary":[]},
                {"type":"function_call","call_id":"a","name":"One","arguments":"{}"},
                {"type":"function_call","call_id":"b","name":"Two","arguments":"{\"x\":1}"}
            ],
            "usage":{"input_tokens":10,"output_tokens":5}
        });
        let result = normalize_openai(payload).unwrap();
        assert_eq!(result.status, "tool_calls");
        assert_eq!(result.message.tool_calls.len(), 2);
        assert_eq!(result.message.provider_items.len(), 3);
        assert_eq!(result.usage.unwrap().input_tokens, 10);
    }
    #[test]
    fn complete_text_normalizes() {
        let payload = json!({"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}]});
        let result = normalize_openai(payload).unwrap();
        assert_eq!(result.status, "complete");
        assert_eq!(result.message.content, Some(json!("hello")));
    }
    #[test]
    fn text_message_is_valid() {
        assert!(validate_request(&request(vec![text("user", "hi")])).is_ok());
    }
    #[test]
    fn provider_request_uses_stateless_responses_shape() {
        let request = request(vec![text("user", "hi")]);
        let body = provider_request_body(&request, "gpt-5.6-sol", "xhigh");
        assert_eq!(body["model"], "gpt-5.6-sol");
        assert_eq!(body["reasoning"]["effort"], "xhigh");
        assert_eq!(body["input"][0], json!({"role":"user","content":"hi"}));
        assert_eq!(body["store"], false);
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    }
    #[test]
    fn provider_request_replays_output_items_and_local_tool_result() {
        let mut assistant = text("assistant", "ignored normalized copy");
        assistant.provider_items = vec![
            json!({"type":"reasoning","encrypted_content":"opaque","summary":[]}),
            json!({"type":"function_call","call_id":"call_1","name":"LoadNode","arguments":"{\"identifier\":2}"}),
        ];
        assistant.tool_calls = vec![ToolCall {
            id: "call_1".into(),
            name: "LoadNode".into(),
            arguments: Map::from_iter([("identifier".into(), json!(2))]),
        }];
        let result = Message {
            role: "tool".into(),
            content: Some(json!("Memory load completed.\n\nNode 2: Project")),
            tool_calls: vec![],
            tool_call_id: Some("call_1".into()),
            name: Some("LoadNode".into()),
            provider_items: vec![],
        };
        let body = provider_request_body(
            &request(vec![text("user", "hi"), assistant, result]),
            "gpt-5.6-sol",
            "xhigh",
        );
        assert_eq!(body["input"][1]["type"], "reasoning");
        assert_eq!(body["input"][2]["call_id"], "call_1");
        assert_eq!(body["input"][3]["type"], "function_call_output");
        assert_eq!(
            body["input"][3]["output"],
            "Memory load completed.\n\nNode 2: Project"
        );
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
    }
}
