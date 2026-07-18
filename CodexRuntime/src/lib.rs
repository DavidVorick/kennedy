//! Shared discovery and caching for Kennedy's Codex model catalog.
//!
//! [`CatalogCache`] ensures that every Codex-backed service in one Kennedy
//! process shares one discovery. The sanitized result is also cached on disk
//! by Codex version so ordinary restarts need only a cheap version check.

#![deny(missing_docs)]

use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, ensure};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{fs, process::Command, sync::OnceCell};

/// Kennedy's sandboxed Codex launcher.
pub const DEFAULT_CODEX_EXECUTABLE: &str = "codex-safe";

const CACHE_SCHEMA: &str = "kennedy-codex-catalog-v1";
const CACHE_DIRECTORY: &str = "kennedy-codex-catalogs";

/// Effective limits advertised for one Codex model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelLimits {
    /// Effective context-window size after Codex's advertised percentage.
    pub context_window_tokens: u64,
    /// Maximum input size Kennedy may send to the model.
    pub max_input_tokens: u64,
}

impl ModelLimits {
    /// Effective context-window size after Codex's advertised percentage.
    pub fn context_window_tokens(self) -> u64 {
        self.context_window_tokens
    }

    /// Maximum input size Kennedy may send to the model.
    pub fn max_input_tokens(self) -> u64 {
        self.max_input_tokens
    }
}

/// One verified, sanitized Codex model catalog.
#[derive(Clone, Debug)]
pub struct Catalog {
    executable: Arc<str>,
    path: Arc<PathBuf>,
    limits: Arc<HashMap<String, ModelLimits>>,
    cache_key: Arc<str>,
    cache_directory: Arc<PathBuf>,
}

impl Catalog {
    /// Codex executable whose version produced this catalog.
    pub fn executable(&self) -> &str {
        &self.executable
    }

    /// Path to the sanitized JSON catalog accepted by Codex.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the advertised limits for `model` when it exists.
    pub fn model_limits(&self, model: &str) -> Option<ModelLimits> {
        self.limits.get(model).copied()
    }

    /// Reports whether a successful compatibility validation is cached.
    ///
    /// The caller must change `scope` whenever the configuration being
    /// validated changes. Catalog and Codex-version changes are incorporated
    /// automatically.
    pub async fn validation_is_cached(&self, scope: &str) -> anyhow::Result<bool> {
        let path = self.validation_path(scope);
        let expected = self.validation_record(scope);
        match fs::read(&path).await {
            Ok(actual) => Ok(actual == expected.as_bytes()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error)
                .with_context(|| format!("reading Codex validation cache {}", path.display())),
        }
    }

    /// Records that the compatibility validation identified by `scope` passed.
    pub async fn cache_validation(&self, scope: &str) -> anyhow::Result<()> {
        fs::create_dir_all(self.cache_directory.as_ref())
            .await
            .with_context(|| {
                format!(
                    "creating Codex cache directory {}",
                    self.cache_directory.display()
                )
            })?;
        let path = self.validation_path(scope);
        let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
        fs::write(&temporary, self.validation_record(scope))
            .await
            .with_context(|| {
                format!(
                    "writing temporary Codex validation cache {}",
                    temporary.display()
                )
            })?;
        fs::rename(&temporary, &path).await.with_context(|| {
            format!(
                "publishing Codex validation cache {} as {}",
                temporary.display(),
                path.display()
            )
        })?;
        Ok(())
    }

    fn validation_path(&self, scope: &str) -> PathBuf {
        let key = digest(&[self.cache_key.as_bytes(), scope.as_bytes()]);
        self.cache_directory.join(format!("validated-{key}"))
    }

    fn validation_record(&self, scope: &str) -> String {
        format!("{}\n{scope}\n", self.cache_key)
    }
}

/// A cloneable process-wide handle for lazily loading one shared catalog.
#[derive(Clone, Debug)]
pub struct CatalogCache {
    executable: Arc<str>,
    cache_directory: Arc<PathBuf>,
    catalog: Arc<OnceCell<Catalog>>,
}

impl CatalogCache {
    /// Creates an unloaded cache for `executable`.
    pub fn new(executable: impl Into<String>) -> Self {
        let cache_directory = std::env::var_os("CODEX_SAFE_CATALOG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join(CACHE_DIRECTORY));
        Self::with_directory(executable, cache_directory)
    }

    /// Creates an unloaded cache using an explicit host/container-shared path.
    pub fn with_directory(
        executable: impl Into<String>,
        cache_directory: impl Into<PathBuf>,
    ) -> Self {
        Self {
            executable: Arc::from(executable.into()),
            cache_directory: Arc::new(cache_directory.into()),
            catalog: Arc::new(OnceCell::new()),
        }
    }

    /// Loads one catalog, sharing concurrent initialization between callers.
    pub async fn load(&self) -> anyhow::Result<Catalog> {
        let executable = self.executable.clone();
        let cache_directory = self.cache_directory.clone();
        let catalog = self
            .catalog
            .get_or_try_init(|| async move { load_catalog(executable, cache_directory).await })
            .await?;
        Ok(catalog.clone())
    }
}

/// Formats the Codex configuration override for a sanitized catalog path.
pub fn model_catalog_config(path: &Path) -> String {
    format!(
        "model_catalog_json={}",
        serde_json::to_string(path.to_string_lossy().as_ref())
            .expect("serializing a path string cannot fail")
    )
}

async fn load_catalog(
    executable: Arc<str>,
    cache_directory: Arc<PathBuf>,
) -> anyhow::Result<Catalog> {
    fs::create_dir_all(cache_directory.as_ref())
        .await
        .with_context(|| format!("creating Codex catalog cache {}", cache_directory.display()))?;
    let identity = codex_identity(&executable, &cache_directory).await?;
    let cache_key = digest(&[
        CACHE_SCHEMA.as_bytes(),
        executable.as_bytes(),
        identity.as_bytes(),
    ]);
    let path = cache_directory.join(format!("models-{cache_key}.json"));

    match fs::read(&path).await {
        Ok(cached) => match verified_limits(&cached) {
            Ok(limits) => {
                tracing::info!(path=%path.display(), "Using cached sanitized Codex model catalog");
                return Ok(Catalog {
                    executable,
                    path: Arc::new(path),
                    limits: Arc::new(limits),
                    cache_key: Arc::from(cache_key),
                    cache_directory,
                });
            }
            Err(error) => {
                tracing::warn!(path=%path.display(), %error, "Ignoring invalid cached Codex model catalog");
            }
        },
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("reading Codex catalog cache {}", path.display()));
        }
    }

    let source = Command::new(executable.as_ref())
        .args(["debug", "models"])
        .env("CODEX_SAFE_CATALOG_DIR", cache_directory.as_os_str())
        .env_remove("OPENAI_API_KEY")
        .env_remove("CODEX_API_KEY")
        .output()
        .await
        .with_context(|| format!("discovering Codex models through '{executable}'"))?;
    ensure!(
        source.status.success(),
        "Codex model discovery failed through {executable}"
    );
    let source_limits = parse_model_limits(&source.stdout)?;
    let sanitized = sanitize_catalog(&source.stdout)?;
    let sanitized_limits = verified_limits(&sanitized)?;
    ensure!(
        sanitized_limits == source_limits,
        "sanitized Codex catalog changed advertised model limits"
    );

    let temporary = path.with_extension(format!("tmp-{}.json", std::process::id()));
    fs::write(&temporary, &sanitized).await.with_context(|| {
        format!(
            "writing temporary sanitized Codex catalog {}",
            temporary.display()
        )
    })?;
    let probe = Command::new(executable.as_ref())
        .arg("-c")
        .arg(model_catalog_config(&temporary))
        .args(["debug", "models"])
        .env("CODEX_SAFE_CATALOG_DIR", cache_directory.as_os_str())
        .env_remove("OPENAI_API_KEY")
        .env_remove("CODEX_API_KEY")
        .output()
        .await
        .with_context(|| format!("probing sanitized Codex catalog through '{executable}'"))?;
    let verification = (|| -> anyhow::Result<()> {
        ensure!(
            probe.status.success(),
            "{executable} cannot read {} inside its sandbox",
            temporary.display()
        );
        ensure!(
            verified_limits(&probe.stdout)? == source_limits,
            "Codex changed model limits while loading the sanitized catalog"
        );
        Ok(())
    })();
    if let Err(error) = verification {
        let _ = fs::remove_file(&temporary).await;
        return Err(error);
    }
    fs::rename(&temporary, &path).await.with_context(|| {
        format!(
            "publishing sanitized Codex catalog {} as {}",
            temporary.display(),
            path.display()
        )
    })?;
    tracing::info!(path=%path.display(), "Discovered and cached sanitized Codex model catalog");
    Ok(Catalog {
        executable,
        path: Arc::new(path),
        limits: Arc::new(source_limits),
        cache_key: Arc::from(cache_key),
        cache_directory,
    })
}

async fn codex_identity(executable: &str, cache_directory: &Path) -> anyhow::Result<String> {
    let output = Command::new(executable)
        .arg("--version")
        .env("CODEX_SAFE_CATALOG_DIR", cache_directory.as_os_str())
        .env_remove("OPENAI_API_KEY")
        .env_remove("CODEX_API_KEY")
        .output()
        .await
        .with_context(|| format!("reading Codex version through '{executable}'"))?;
    ensure!(
        output.status.success(),
        "Codex version check failed through {executable}"
    );
    let version = String::from_utf8(output.stdout)
        .context("Codex returned a non-UTF-8 version")?
        .trim()
        .to_owned();
    ensure!(!version.is_empty(), "Codex returned an empty version");
    Ok(version)
}

fn digest(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    hex::encode(digest.finalize())
}

fn parse_model_limits(output: &[u8]) -> anyhow::Result<HashMap<String, ModelLimits>> {
    let catalog: Value =
        serde_json::from_slice(output).context("Codex returned an invalid model catalog")?;
    let models = catalog
        .get("models")
        .and_then(Value::as_array)
        .context("Codex model catalog has no models array")?;
    let mut limits = HashMap::new();
    for model in models {
        let slug = model
            .get("slug")
            .and_then(Value::as_str)
            .context("Codex model has no slug")?;
        let context_window = model
            .get("context_window")
            .and_then(Value::as_u64)
            .context("Codex model has no context window")?;
        let effective_percent = model
            .get("effective_context_window_percent")
            .and_then(Value::as_u64)
            .context("Codex model has no effective context percentage")?;
        ensure!(
            (1..=100).contains(&effective_percent),
            "Codex model {slug} has an invalid effective context percentage"
        );
        let effective = context_window
            .checked_mul(effective_percent)
            .context("Codex model context limit overflowed")?
            / 100;
        ensure!(effective > 0, "Codex model {slug} has an empty context");
        limits.insert(
            slug.to_owned(),
            ModelLimits {
                context_window_tokens: effective,
                max_input_tokens: effective,
            },
        );
    }
    Ok(limits)
}

fn sanitize_catalog(output: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut catalog: Value =
        serde_json::from_slice(output).context("Codex returned an invalid model catalog")?;
    let models = catalog
        .get_mut("models")
        .and_then(Value::as_array_mut)
        .context("Codex model catalog has no models array")?;
    for model in models {
        let model = model
            .as_object_mut()
            .context("Codex model catalog contains a non-object model")?;
        model.remove("tool_mode");
        model.remove("multi_agent_version");
        model.remove("apply_patch_tool_type");
        model.remove("model_messages");
        model.insert("base_instructions".into(), Value::String(String::new()));
        model.insert(
            "include_skills_usage_instructions".into(),
            Value::Bool(false),
        );
    }
    serde_json::to_vec(&catalog).context("serializing the sanitized Codex model catalog")
}

fn verified_limits(output: &[u8]) -> anyhow::Result<HashMap<String, ModelLimits>> {
    let catalog: Value =
        serde_json::from_slice(output).context("Codex returned an invalid model catalog")?;
    let models = catalog
        .get("models")
        .and_then(Value::as_array)
        .context("Codex model catalog has no models array")?;
    for model in models {
        let model = model
            .as_object()
            .context("Codex model catalog contains a non-object model")?;
        let slug = model
            .get("slug")
            .and_then(Value::as_str)
            .unwrap_or("unknown model");
        ensure!(
            model.get("base_instructions").and_then(Value::as_str) == Some(""),
            "Codex retained base instructions for {slug}"
        );
        ensure!(
            !model.contains_key("model_messages"),
            "Codex retained model messages for {slug}"
        );
        ensure!(
            model
                .get("include_skills_usage_instructions")
                .and_then(Value::as_bool)
                == Some(false),
            "Codex retained skill usage instructions for {slug}"
        );
    }
    parse_model_limits(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: &[u8] = br#"{
        "models":[{
            "slug":"gpt-5.6-sol",
            "context_window":272000,
            "effective_context_window_percent":95,
            "base_instructions":"provider instructions",
            "model_messages":{"instructions_template":"more provider instructions"},
            "include_skills_usage_instructions":true,
            "tool_mode":"code_mode_only",
            "multi_agent_version":"v2",
            "apply_patch_tool_type":"freeform",
            "unrelated":"preserved"
        }]
    }"#;

    #[test]
    fn sanitization_removes_hidden_prompts_and_preserves_limits() {
        let sanitized = sanitize_catalog(SOURCE).unwrap();
        assert_eq!(
            parse_model_limits(&sanitized).unwrap(),
            parse_model_limits(SOURCE).unwrap()
        );
        verified_limits(&sanitized).unwrap();
        let catalog: Value = serde_json::from_slice(&sanitized).unwrap();
        let model = catalog["models"][0].as_object().unwrap();
        for removed in [
            "tool_mode",
            "multi_agent_version",
            "apply_patch_tool_type",
            "model_messages",
        ] {
            assert!(!model.contains_key(removed));
        }
        assert_eq!(model["base_instructions"], "");
        assert_eq!(model["include_skills_usage_instructions"], false);
        assert_eq!(model["unrelated"], "preserved");
    }

    #[test]
    fn advertised_context_uses_the_effective_percentage() {
        let limits = parse_model_limits(SOURCE).unwrap();
        assert_eq!(
            limits["gpt-5.6-sol"],
            ModelLimits {
                context_window_tokens: 258_400,
                max_input_tokens: 258_400,
            }
        );
    }

    #[test]
    fn catalog_configuration_quotes_paths() {
        assert_eq!(
            model_catalog_config(Path::new("/tmp/a catalog.json")),
            "model_catalog_json=\"/tmp/a catalog.json\""
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn concurrent_callers_share_discovery_and_restarts_use_the_disk_cache() {
        use std::{
            os::unix::fs::PermissionsExt,
            time::{SystemTime, UNIX_EPOCH},
        };

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "kennedy-codex-runtime-test-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let executable = directory.join("fake-codex");
        let calls = directory.join("calls");
        let sanitized_catalog = r#"{"models":[{"slug":"gpt-5.6-sol","context_window":272000,"effective_context_window_percent":95,"base_instructions":"","include_skills_usage_instructions":false}]}"#;
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$1\" >> '{}'\nif [ \"$1\" = '--version' ]; then\n  printf '%s\\n' 'fake-codex 1.0'\nelse\n  printf '%s' '{}'\nfi\n",
            calls.display(),
            sanitized_catalog
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let cache = CatalogCache::with_directory(
            executable.to_string_lossy().into_owned(),
            directory.join("catalog-cache"),
        );
        let (left, right) = tokio::join!(cache.load(), cache.load());
        let first = left.unwrap();
        assert_eq!(first.path(), right.unwrap().path());
        let cache_path = first.path().to_owned();
        assert!(!first.validation_is_cached("prompt-v1").await.unwrap());
        first.cache_validation("prompt-v1").await.unwrap();
        assert!(first.validation_is_cached("prompt-v1").await.unwrap());
        assert!(!first.validation_is_cached("prompt-v2").await.unwrap());

        let restarted = CatalogCache::with_directory(
            executable.to_string_lossy().into_owned(),
            directory.join("catalog-cache"),
        );
        assert_eq!(restarted.load().await.unwrap().path(), cache_path);
        assert_eq!(
            std::fs::read_to_string(&calls)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec!["--version", "debug", "-c", "--version"]
        );

        std::fs::remove_file(cache_path).unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
