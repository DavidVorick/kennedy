use std::{path::Path, time::Duration};

use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use reqwest::{Client, Method, StatusCode, multipart};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::Config;

#[derive(Debug, Clone)]
pub(crate) struct ApiError {
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
    pub kweb: String,
    pub intelligence: String,
    pub history: String,
    pub telegram: String,
    pub audio: String,
}

impl Api {
    pub fn new(config: &Config) -> anyhow::Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("building reqwest client")?;
        Ok(Self {
            client,
            kweb: trim_base(&config.kweb_base),
            intelligence: trim_base(&config.intelligence_base),
            history: trim_base(&config.conversation_history_base),
            telegram: trim_base(&config.telegram_relay_base),
            audio: trim_base(&config.audio_ingress_base),
        })
    }

    pub async fn health(&self, base: &str, path: &str) -> Result<(), ApiError> {
        self.get(base, path).await.map(|_| ())
    }

    pub async fn get(&self, base: &str, path: &str) -> Result<Value, ApiError> {
        self.request(Method::GET, base, path, None).await
    }

    pub async fn post(&self, base: &str, path: &str, body: Value) -> Result<Value, ApiError> {
        self.request(Method::POST, base, path, Some(body)).await
    }

    pub async fn put(&self, base: &str, path: &str, body: Value) -> Result<Value, ApiError> {
        self.request(Method::PUT, base, path, Some(body)).await
    }

    pub async fn kmap_post(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.kmap_request(Method::POST, path, body).await
    }

    pub async fn kmap_put(&self, path: &str, body: Value) -> Result<Value, ApiError> {
        self.kmap_request(Method::PUT, path, body).await
    }

    async fn kmap_request(
        &self,
        method: Method,
        path: &str,
        body: Value,
    ) -> Result<Value, ApiError> {
        let mut last_error = None;
        for attempt in 0..3 {
            match self
                .request(method.clone(), &self.kweb, path, Some(body.clone()))
                .await
            {
                Ok(value) => return Ok(value),
                Err(error) if error.code == "network_error" && attempt < 2 => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.expect("a Kmap request retry must retain its network error"))
    }

    pub async fn delete(
        &self,
        base: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, ApiError> {
        self.request(Method::DELETE, base, path, body).await
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

    pub async fn bytes(&self, base: &str, path: &str) -> Result<(Vec<u8>, String), ApiError> {
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

    pub async fn multipart(
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

    pub async fn kmap_context(&self, node_id: &str) -> Result<Value, ApiError> {
        let requested = self
            .get(&self.kweb, &format!("/api/v1/kmap/nodes/{node_id}"))
            .await?;
        let active_ids = recent_connection_ids(&requested)
            .into_iter()
            .take(8)
            .collect::<Vec<_>>();
        let mut active = Vec::with_capacity(active_ids.len());
        for id in active_ids {
            active.push(
                self.get(&self.kweb, &format!("/api/v1/kmap/nodes/{id}"))
                    .await?,
            );
        }
        Ok(json!({
            "requested_node": normalize_node(requested),
            "active_connection_nodes": active.into_iter().map(normalize_node).collect::<Vec<_>>(),
        }))
    }

    pub async fn bootstrap_node(
        &self,
        node_id: &str,
        short_name: Option<&str>,
    ) -> Result<Value, ApiError> {
        match self
            .get(&self.kweb, &format!("/api/v1/kmap/nodes/{node_id}"))
            .await
        {
            Ok(node) => return Ok(node),
            Err(error) if error.status == Some(StatusCode::NOT_FOUND) => {}
            Err(error) => return Err(error),
        }
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
                "node_id": node_id,
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

    pub async fn create_provenance_archive(
        &self,
        idempotency: &str,
        archive: &Value,
        source: &str,
        source_created_at: &str,
    ) -> Result<Value, ApiError> {
        let mut stored = archive.clone();
        let mut artifacts = Vec::new();
        if let Some(media) = stored.get_mut("media").and_then(Value::as_array_mut) {
            for item in media {
                let Some(data_url) = item.get("dataUrl").and_then(Value::as_str) else {
                    continue;
                };
                let (mime, bytes) = decode_data_url(data_url)?;
                let filename = item
                    .get("fileName")
                    .and_then(Value::as_str)
                    .unwrap_or("provenance-media")
                    .to_owned();
                let index = artifacts.len();
                artifacts.push((filename, mime, bytes));
                if let Some(object) = item.as_object_mut() {
                    object.remove("dataUrl");
                    object.insert("provenanceArtifactIndex".into(), json!(index));
                }
            }
        }
        let safe_source = source
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                    character
                } else {
                    '-'
                }
            })
            .collect::<String>();
        let mut form = multipart::Form::new()
            .text("idempotency_id", idempotency.to_owned())
            .text("source", source.to_owned())
            .text("source_created_at", source_created_at.to_owned())
            .text("data_filename", format!("{safe_source}-archive.json"));
        for (filename, mime, bytes) in artifacts {
            let part = multipart::Part::bytes(bytes)
                .file_name(filename)
                .mime_str(&mime)
                .map_err(|error| ApiError {
                    status: None,
                    code: "invalid_media".into(),
                    message: error.to_string(),
                })?;
            form = form.part("artifact", part);
        }
        form = form.text(
            "data",
            serde_json::to_string_pretty(&stored).map_err(|error| ApiError {
                status: None,
                code: "invalid_archive".into(),
                message: error.to_string(),
            })?,
        );
        self.multipart(&self.kweb, "/api/v1/kmap/provenance-with-artifacts", form)
            .await
    }

    pub async fn release_rust_libs(&self, session_id: &str) {
        let _ = self
            .post(
                &self.kweb,
                "/api/v1/rust-libs/release",
                json!({"session_id": session_id}),
            )
            .await;
    }

    pub async fn transcribe(
        &self,
        provider: &str,
        model: &str,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
    ) -> Result<Value, ApiError> {
        let part = multipart::Part::bytes(bytes)
            .file_name(filename)
            .mime_str(mime)
            .map_err(local_api_error)?;
        self.multipart(
            &self.intelligence,
            "/api/v1/audio/transcriptions",
            multipart::Form::new()
                .text("provider", provider.to_owned())
                .text("model", model.to_owned())
                .part("file", part),
        )
        .await
    }

    pub async fn extract_document(
        &self,
        bytes: Vec<u8>,
        filename: String,
        mime: &str,
    ) -> Result<Value, ApiError> {
        let part = multipart::Part::bytes(bytes)
            .file_name(filename)
            .mime_str(mime)
            .map_err(local_api_error)?;
        self.multipart(
            &self.intelligence,
            "/api/v1/documents/extract",
            multipart::Form::new().part("file", part),
        )
        .await
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
}

pub(crate) fn data_url(mime: &str, bytes: &[u8]) -> String {
    format!("data:{mime};base64,{}", BASE64.encode(bytes))
}

fn decode_data_url(value: &str) -> Result<(String, Vec<u8>), ApiError> {
    let (header, encoded) = value.split_once(',').ok_or_else(|| ApiError {
        status: None,
        code: "invalid_media".into(),
        message: "An archived media data URL is invalid.".into(),
    })?;
    let mime = header
        .strip_prefix("data:")
        .and_then(|value| value.strip_suffix(";base64"))
        .unwrap_or("application/octet-stream")
        .to_owned();
    let bytes = BASE64.decode(encoded).map_err(|error| ApiError {
        status: None,
        code: "invalid_media".into(),
        message: format!("An archived media data URL is invalid: {error}"),
    })?;
    Ok((mime, bytes))
}

fn recent_connection_ids(node: &Value) -> Vec<String> {
    node.get("recent_connections")
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

#[allow(dead_code)]
pub(crate) fn file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("file")
        .to_owned()
}
