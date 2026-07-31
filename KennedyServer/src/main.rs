mod audio_ingress;
mod kmap_http;
mod orchestration;
mod session_history_http;

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use kcode_credential_vault::{CredentialVault, ExposeSecret, SecretString};
use kcode_kweb_db::{Config as KwebConfig, NoopGossip, WriterId};
use kcode_speech_classification::SpeechClassifier;
use zeroize::{Zeroize, Zeroizing};

const OPENAI_API_KEY_SECRET: &str = "openai-api-key";
const GEMINI_API_KEY_SECRET: &str = "gemini-api-key";
const TELEGRAM_BOT_TOKEN_SECRET: &str = "telegram-bot-token";
const CRATES_IO_KEY_SECRET: &str = "cratesio-key";
const KWEB_WRITER_SIGNING_KEY_SECRET: &str = "kweb-writer-signing-key";
const KWEB_WRITERS_SECRET: &str = "kweb-writers-by-priority";
const SPEECH_CLASSIFICATION_DATABASE_PATH: &str = "./data/kennedy-speech-classification.sqlite3";

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, global = true, default_value = "./data/kennedy-secrets.age")]
    vault_path: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, global = true, default_value = "127.0.0.1:4321")]
    kweb_bind: String,
    #[arg(long, global = true, default_value = "./data/kweb")]
    kweb_root: PathBuf,
    #[arg(
        long,
        global = true,
        default_value = "./data/kennedy-conversations.sqlite3"
    )]
    conversation_history_database: PathBuf,
    #[arg(long, global = true, default_value = "./data/sessions/in-progress")]
    session_directory: PathBuf,
    #[arg(long, global = true, default_value = "./data/session-history.txt")]
    session_history_file: PathBuf,
    #[arg(long, global = true, default_value = "./data/kennedy-telegram.sqlite3")]
    telegram_database: PathBuf,
    #[arg(long, global = true, default_value = "./data/kennedy-users.sqlite3")]
    user_database: PathBuf,
    #[arg(
        long,
        alias = "audio-ingress-database",
        global = true,
        default_value = "./data/kennedy-audio.sqlite3",
        help = "Optional pre-library AudioIngress database used only for one-time migration"
    )]
    legacy_audio_ingress_database: PathBuf,
    #[arg(
        long,
        alias = "audio-ingress-media",
        global = true,
        default_value = "./data/audio-ingress-media",
        help = "AudioIngress-owned persistence root (database and original audio)"
    )]
    audio_ingress_directory: PathBuf,
    #[arg(
        long,
        global = true,
        default_value = "./data/intelligence-usage",
        help = "One-file-per-call intelligence usage receipt directory"
    )]
    intelligence_usage_directory: PathBuf,
    #[arg(long, default_value = "./data/kcode/kcode-rust-libs")]
    rust_libs_root: PathBuf,
    #[arg(long, default_value = "./data/kcode/kcode-web-libs")]
    web_libs_root: PathBuf,
    #[arg(long, default_value = "./data/kcode/kcode-web-libs-published")]
    web_libs_published_root: PathBuf,
    #[arg(long, default_value = "./data/kcode/kcode-rust-bins")]
    rust_bins_root: PathBuf,
    #[arg(long, default_value = "./data/kcode/kcode-rust-bin-artifacts")]
    rust_bin_artifacts_root: PathBuf,
    #[arg(long, default_value = "./KennedyServer/runtime/system-prompts")]
    system_prompts_dir: PathBuf,
    #[arg(long, default_value = "@taek42")]
    telegram_bootstrap_username: String,
    #[arg(long, default_value_t = 20 * 1024 * 1024)]
    telegram_max_voice_bytes: usize,
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    audio_ingress_max_upload_bytes: usize,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create and manage generic named secrets in Kennedy's encrypted vault.
    Secrets {
        #[command(subcommand)]
        command: SecretsCommand,
    },
    /// Estimate the token footprint of all current Kmap node text.
    KmapSize,
}

#[derive(Subcommand, Debug)]
enum SecretsCommand {
    /// Prompt for and store a named secret, replacing any previous value.
    Set { name: String },
    /// Remove a named secret without displaying its value.
    Remove { name: String },
    /// List configured secret names without displaying their values.
    List,
    /// Re-encrypt the vault with a new passphrase.
    ChangePassphrase,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "kennedy_server=info,kcode_kweb_db=info,kcode_codex_runtime=info,kcode_session_history=info,kcode_tg_kennedy_bot=info,tower_http=info".into()
            }),
        )
        .init();
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("installing TLS crypto provider"))?;
    let mut args = Args::parse();
    let vault_path = args.vault_path.clone();
    match args.command.take() {
        Some(Command::Secrets { command }) => {
            let _maintenance_guard = tokio::net::TcpListener::bind(&args.kweb_bind)
                .await
                .with_context(|| {
                    format!(
                        "binding maintenance lock {}; stop the running Kennedy server before changing its credential vault",
                        args.kweb_bind
                    )
                })?;
            manage_secrets(command, &vault_path)
        }
        Some(Command::KmapSize) => {
            let _maintenance_guard =
                maintenance_guard(&args.kweb_bind, "measuring the Kweb").await?;
            let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
            let vault = CredentialVault::unlock(&vault_path, passphrase)?;
            let size = kcode_kmap_size::measure(&args.kweb_root, kweb_config(&vault)?)?;
            println!("{}", kcode_kmap_size::render(&size));
            Ok(())
        }
        None => run_server(args, vault_path).await,
    }
}

async fn run_server(args: Args, vault_path: PathBuf) -> anyhow::Result<()> {
    // Bind the public Kennedy address before opening any persistent state.
    // Offline maintenance checks this address before copying the data tree.
    let kweb_listener = tokio::net::TcpListener::bind(&args.kweb_bind)
        .await
        .with_context(|| format!("binding Kweb listener {}", args.kweb_bind))?;
    ensure_runtime_parent_directories(&args, &vault_path)?;
    let vault = if vault_path.exists() {
        let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
        CredentialVault::unlock(&vault_path, passphrase)?
    } else {
        tracing::warn!(path=%vault_path.display(), "Kennedy credential vault does not exist; secret-backed features are unavailable");
        CredentialVault::empty()
    };
    let openai_api_key = resolve_optional_secret(
        &vault,
        OPENAI_API_KEY_SECRET,
        "OpenAI transcription, media annotation, agents, and image generation/editing",
    )?;
    let gemini_api_key = resolve_optional_secret(
        &vault,
        GEMINI_API_KEY_SECRET,
        "Gemini search, media annotation, agents, audio transcription, and image generation/editing",
    )?;
    let telegram_bot_token =
        resolve_optional_secret(&vault, TELEGRAM_BOT_TOKEN_SECRET, "Telegram relay")?
            .map(kcode_tg_kennedy_bot::BotToken::new)
            .transpose()?;
    let crates_io_key =
        resolve_required_secret(&vault, CRATES_IO_KEY_SECRET, "Rust library publication")?;
    let kweb_config = kweb_config(&vault)?;
    let codex_catalog_cache =
        kcode_codex_runtime::CatalogCache::new(kcode_codex_runtime::DEFAULT_CODEX_EXECUTABLE);
    let (kmap, system_roots) =
        kmap_http::initialize(&args.kweb_root, kweb_config, &args.user_database)?;
    let speech_classifier = SpeechClassifier::open(SPEECH_CLASSIFICATION_DATABASE_PATH)
        .with_context(|| {
            format!("opening speaker-classification database {SPEECH_CLASSIFICATION_DATABASE_PATH}")
        })?;
    let speech_classifier = Arc::new(speech_classifier);
    let dev_tools = kcode_dev_tools::Service::open(kcode_dev_tools::Config {
        rust_libraries_root: args.rust_libs_root.clone(),
        web_libraries_root: args.web_libs_root.clone(),
        web_publications_root: args.web_libs_published_root.clone(),
        rust_binaries_root: args.rust_bins_root.clone(),
        rust_binary_publications_root: args.rust_bin_artifacts_root.clone(),
        crates_io_registry_token: crates_io_key,
    })
    .map_err(anyhow::Error::new)
    .with_context(|| {
        format!(
            "opening managed Kcode development roots under {}",
            args.rust_libs_root
                .parent()
                .unwrap_or(Path::new("."))
                .display()
        )
    })?;
    let web_lib_router = kcode_web_semver_routing::router(dev_tools.web_publications_root());
    let telegram_identity = std::sync::Arc::new(kcode_telegram_identity::Directory::open(
        &args.user_database,
        &args.telegram_bootstrap_username,
    )?);
    let history_service =
        kcode_session_history::SessionHistory::open(kcode_session_history::Config {
            directory: args.session_directory,
            completed_list: args.session_history_file,
            provider_cost_compatibility: Some(kcode_session_history::ProviderCostCompatibility {
                session_model: legacy_session_provider_model,
                estimator: legacy_provider_cost,
            }),
        })?;
    let (intelligence_service, intelligence_runtime) =
        kcode_intelligence_router::open(kcode_intelligence_router::Config {
            openai_api_key,
            gemini_api_key,
            codex_catalog_cache,
            receipt_directory: args.intelligence_usage_directory,
        })
        .await?;
    let agent_runtime = kcode_agent_runtime::AgentRuntime::new(intelligence_service.clone());
    let telegram_runtime = kcode_tg_kennedy_bot::open(kcode_tg_kennedy_bot::Config {
        database: args.telegram_database,
        bot_token: telegram_bot_token,
        identity_sink: telegram_identity.clone(),
        max_voice_bytes: args.telegram_max_voice_bytes,
    })
    .await?;
    let telegram_service = telegram_runtime.service();
    let kmap_service = kmap_http::Service::new(
        kmap.clone(),
        system_roots,
        telegram_service.clone(),
        history_service.clone(),
    );
    let chunk_intelligence = intelligence_service.clone();
    let transcribe_chunk: kcode_audio_ingress::AudioChunkCall = Arc::new(move |request| {
        let intelligence = chunk_intelligence.clone();
        Box::pin(async move {
            let user = intelligence
                .for_user(request.user_id)
                .map_err(audio_intelligence_error)?;
            let media = kcode_intelligence_router::Media::audio(
                request.audio_ogg,
                "audio-chunk.ogg",
                "audio/ogg",
            )
            .map_err(audio_intelligence_error)?;
            user.analyze_audio(kcode_intelligence_router::AudioAnalysisRequest {
                operation: "transcribe_chunk".into(),
                prompt: request.prompt,
                model: request.model,
                media,
                schema: request.schema,
                max_output_tokens: request.max_output_tokens,
                operation_id: uuid::Uuid::new_v4(),
                parent_operation_id: None,
            })
            .await
            .map(|response| response.text)
            .map_err(audio_intelligence_error)
        })
    });
    let text_intelligence = intelligence_service.clone();
    let generate_text: kcode_audio_ingress::TextGenerationCall = Arc::new(move |request| {
        let intelligence = text_intelligence.clone();
        Box::pin(async move {
            let reasoning_effort = match request.reasoning_effort.as_str() {
                "xhigh" => kcode_intelligence_router::ReasoningEffort::XHigh,
                _ => {
                    return Err(kcode_audio_ingress::IntelligenceError::new(
                        "AudioIngress requested an unsupported reasoning effort.",
                        false,
                    ));
                }
            };
            let user = intelligence
                .for_user(request.user_id)
                .map_err(audio_intelligence_error)?;
            user.generate_text(kcode_intelligence_router::TextGenerationRequest {
                operation: request.operation,
                prompt: request.prompt,
                model: request.model,
                reasoning_effort,
                timeout: request.timeout,
                operation_id: uuid::Uuid::new_v4(),
                parent_operation_id: None,
            })
            .await
            .map(|response| response.text)
            .map_err(audio_intelligence_error)
        })
    });
    let audio_transcriber =
        kcode_audio_ingress::AudioTranscriber::new(transcribe_chunk, generate_text);
    let audio_state_database = args.audio_ingress_directory.join("state.sqlite3");
    migrate_audio_ingress_database(&args.legacy_audio_ingress_database, &audio_state_database)?;
    let audio = kcode_audio_ingress::AudioIngress::open(
        &args.audio_ingress_directory,
        audio_transcriber,
        Arc::clone(&speech_classifier),
    )
    .await?;
    let audio_coordinator = kcode_audio_session_ingress::Coordinator::new(
        audio,
        history_service.clone(),
        kcode_audio_session_ingress::Config {
            user_id: system_roots.user.to_string(),
            effective_context_tokens: intelligence_runtime.context_window_tokens,
        },
    )?;
    let audio_service = audio_ingress::Service::open(
        audio_coordinator.clone(),
        args.audio_ingress_max_upload_bytes,
    )?;
    let audio_ingress_router = audio_ingress::router(audio_service.clone());
    let orchestration = orchestration::Config {
        system_prompts_directory: args.system_prompts_dir.clone(),
        user_root_node_id: system_roots.user.to_string(),
        kennedy_root_node_id: system_roots.kennedy.to_string(),
        telegram_max_media_bytes: args.telegram_max_voice_bytes,
        telegram_web_user_handle: args.telegram_bootstrap_username,
        runtime_model: orchestration::RuntimeModel::from_intelligence(intelligence_runtime),
    };
    let orchestration_api = orchestration::Api::new(
        &orchestration,
        orchestration::LocalServices {
            kmap,
            intelligence: intelligence_service,
            agents: agent_runtime,
            history: history_service.clone(),
            audio: audio_coordinator,
            speech_classifier,
            directory: telegram_identity.clone(),
            dev_tools,
            telegram: telegram_service,
        },
    );
    let orchestration_worker = orchestration::build(orchestration, orchestration_api);
    let history_router =
        session_history_http::router(history_service, orchestration_worker.clone());
    tokio::try_join!(
        kmap_http::serve_with_listener(
            kmap_service,
            kmap_http::MergedRouters::new(history_router, audio_ingress_router, web_lib_router),
            kweb_listener,
        ),
        telegram_runtime.run(),
        orchestration::run(orchestration_worker),
    )?;
    Ok(())
}

fn audio_intelligence_error(
    error: kcode_intelligence_router::Error,
) -> kcode_audio_ingress::IntelligenceError {
    let retryable = error.retryable();
    kcode_audio_ingress::IntelligenceError::new(error.message(), retryable)
}

fn ensure_runtime_parent_directories(args: &Args, vault_path: &Path) -> anyhow::Result<()> {
    for path in [
        vault_path,
        &args.kweb_root,
        &args.conversation_history_database,
        &args.session_directory,
        &args.session_history_file,
        &args.telegram_database,
        &args.user_database,
        Path::new(SPEECH_CLASSIFICATION_DATABASE_PATH),
        &args.legacy_audio_ingress_database,
        &args.audio_ingress_directory,
        &args.intelligence_usage_directory,
        &args.rust_libs_root,
        &args.web_libs_root,
        &args.web_libs_published_root,
        &args.rust_bins_root,
        &args.rust_bin_artifacts_root,
    ] {
        let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) else {
            continue;
        };
        if parent.exists() {
            continue;
        }
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(parent)
            .with_context(|| format!("creating runtime data directory {}", parent.display()))?;
    }
    Ok(())
}

fn migrate_audio_ingress_database(legacy: &Path, current: &Path) -> anyhow::Result<()> {
    if current.exists() || !legacy.exists() {
        return Ok(());
    }
    if let Some(parent) = current.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating AudioIngress root {}", parent.display()))?;
    }
    let source = rusqlite::Connection::open(legacy)
        .with_context(|| format!("opening legacy AudioIngress database {}", legacy.display()))?;
    source
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .context("checkpointing legacy AudioIngress database")?;
    source
        .backup(rusqlite::MAIN_DB, current, None)
        .context("copying legacy AudioIngress database into its persistence root")?;
    let destination = rusqlite::Connection::open(current)
        .with_context(|| format!("opening AudioIngress database {}", current.display()))?;
    destination
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .context("syncing migrated AudioIngress database")?;
    tracing::info!(
        source = %legacy.display(),
        destination = %current.display(),
        "Migrated AudioIngress database into its owned persistence root"
    );
    Ok(())
}

async fn maintenance_guard(bind: &str, purpose: &str) -> anyhow::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(bind).await.with_context(|| {
        format!("binding maintenance lock {bind}; stop the running Kennedy server before {purpose}")
    })
}

fn kweb_config(vault: &CredentialVault) -> anyhow::Result<KwebConfig> {
    let encoded_key = resolve_required_secret(
        vault,
        KWEB_WRITER_SIGNING_KEY_SECRET,
        "Kweb mutation signing",
    )?;
    let mut signing_key = Zeroizing::new([0_u8; 32]);
    let decoded = hex::decode(encoded_key.trim())
        .context("Kweb writer signing key must be 64 lowercase hexadecimal characters")?;
    *signing_key = decoded
        .try_into()
        .map_err(|_| anyhow::anyhow!("Kweb writer signing key must decode to exactly 32 bytes"))?;
    let encoded_writers =
        resolve_required_secret(vault, KWEB_WRITERS_SECRET, "Kweb writer authorization")?;
    let writers_by_priority = encoded_writers
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(WriterId::from_str)
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)
        .context("decoding the ordered Kweb writer whitelist")?;
    anyhow::ensure!(
        !writers_by_priority.is_empty(),
        "the Kweb writer whitelist is empty"
    );
    Ok(KwebConfig {
        signing_key: *signing_key,
        writers_by_priority,
        gossip: Arc::new(NoopGossip),
    })
}

fn resolve_optional_secret(
    vault: &CredentialVault,
    configured_name: &str,
    purpose: &str,
) -> anyhow::Result<Option<String>> {
    let name = configured_name.trim();
    if name.is_empty() {
        return Ok(None);
    }
    let secret = vault.secret(name)?;
    if secret.is_none() {
        tracing::warn!(secret_name=name, %purpose, "configured Kennedy secret is not present in the vault");
    }
    Ok(secret.map(|value| value.expose_secret().to_owned()))
}

fn resolve_required_secret(
    vault: &CredentialVault,
    configured_name: &str,
    purpose: &str,
) -> anyhow::Result<String> {
    let name = configured_name.trim();
    if name.is_empty() {
        anyhow::bail!("{purpose} requires a configured Kennedy secret name");
    }
    vault
        .secret(name)?
        .map(|value| value.expose_secret().to_owned())
        .with_context(|| {
            format!(
                "{purpose} requires Kennedy secret '{name}'; store it with `kennedy-server secrets set {name}`"
            )
        })
}

fn manage_secrets(command: SecretsCommand, vault_path: &Path) -> anyhow::Result<()> {
    match command {
        SecretsCommand::Set { name } => {
            let (mut vault, passphrase) = unlock_for_edit(vault_path)?;
            let value = prompt_confirmed_value(&format!("Value for {name}: "))?;
            vault.set(&name, value)?;
            vault.save(vault_path, &passphrase)?;
            println!("Stored Kennedy secret '{name}'.");
        }
        SecretsCommand::Remove { name } => {
            if !vault_path.exists() {
                println!("No Kennedy credential vault exists yet.");
                return Ok(());
            }
            let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
            let mut vault = CredentialVault::unlock(vault_path, passphrase.clone())?;
            if vault.remove(&name)? {
                vault.save(vault_path, &passphrase)?;
                println!("Removed Kennedy secret '{name}'.");
            } else {
                println!("Kennedy secret '{name}' was not configured.");
            }
        }
        SecretsCommand::List => {
            if !vault_path.exists() {
                println!("No Kennedy credential vault exists yet.");
                return Ok(());
            }
            let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
            let vault = CredentialVault::unlock(vault_path, passphrase)?;
            let names = vault.names().collect::<Vec<_>>();
            if names.is_empty() {
                println!("The Kennedy credential vault contains no secrets.");
            } else {
                println!("Configured Kennedy secrets:");
                for name in names {
                    println!("- {name}");
                }
            }
        }
        SecretsCommand::ChangePassphrase => {
            if !vault_path.exists() {
                println!("No Kennedy credential vault exists yet.");
                return Ok(());
            }
            let old = prompt_passphrase("Unlock Kennedy credential vault: ")?;
            let vault = CredentialVault::unlock(vault_path, old)?;
            let new = prompt_new_vault_passphrase()?;
            vault.save(vault_path, &new)?;
            println!("Changed the Kennedy credential vault passphrase.");
        }
    }
    Ok(())
}

fn unlock_for_edit(path: &Path) -> anyhow::Result<(CredentialVault, SecretString)> {
    if path.exists() {
        let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
        let vault = CredentialVault::unlock(path, passphrase.clone())?;
        Ok((vault, passphrase))
    } else {
        let passphrase = prompt_new_vault_passphrase()?;
        Ok((CredentialVault::empty(), passphrase))
    }
}

fn prompt_passphrase(prompt: &str) -> anyhow::Result<SecretString> {
    let mut value = rpassword::prompt_password(prompt)?;
    if value.is_empty() {
        value.zeroize();
        anyhow::bail!("the credential vault passphrase cannot be empty");
    }
    Ok(SecretString::from(value))
}

fn prompt_new_vault_passphrase() -> anyhow::Result<SecretString> {
    let mut first = rpassword::prompt_password("Create Kennedy credential vault passphrase: ")?;
    let mut second = rpassword::prompt_password("Confirm credential vault passphrase: ")?;
    if first.is_empty() || first != second {
        first.zeroize();
        second.zeroize();
        anyhow::bail!("credential vault passphrases were empty or did not match");
    }
    second.zeroize();
    Ok(SecretString::from(first))
}

fn prompt_confirmed_value(prompt: &str) -> anyhow::Result<String> {
    let mut first = rpassword::prompt_password(prompt)?;
    let mut second = rpassword::prompt_password("Confirm secret value: ")?;
    if first.is_empty() || first != second {
        first.zeroize();
        second.zeroize();
        anyhow::bail!("secret values were empty or did not match");
    }
    second.zeroize();
    Ok(first)
}

fn legacy_session_provider_model(state: &serde_json::Value) -> Option<String> {
    if let Some(model) = state
        .get("providerModel")
        .and_then(serde_json::Value::as_str)
        .filter(|model| !model.trim().is_empty())
    {
        return Some(model.strip_prefix("codex/").unwrap_or(model).to_owned());
    }
    let attribution = state
        .get("commitAuthor")
        .and_then(serde_json::Value::as_str)?;
    let model = ["-minimal", "-low", "-medium", "-high", "-xhigh", "-max"]
        .iter()
        .find_map(|suffix| attribution.strip_suffix(suffix))
        .unwrap_or(attribution);
    (!model.trim().is_empty()).then(|| model.strip_prefix("codex/").unwrap_or(model).to_owned())
}

fn legacy_provider_cost(
    model: &str,
    metering: &kcode_session_history::chatend::ProviderMetering,
) -> Option<kcode_session_history::chatend::ProviderCostEstimate> {
    let metering = match metering {
        kcode_session_history::chatend::ProviderMetering::Tokens(usage) => {
            kcode_intelligence_router::Metering::Tokens(kcode_intelligence_router::TokenUsage {
                input_tokens: usage.input_tokens,
                cached_input_tokens: usage.cached_input_tokens,
                thinking_tokens: usage.thinking_tokens,
                output_tokens: usage.output_tokens,
            })
        }
        kcode_session_history::chatend::ProviderMetering::DurationSeconds { seconds } => {
            kcode_intelligence_router::Metering::DurationSeconds { seconds: *seconds }
        }
        kcode_session_history::chatend::ProviderMetering::Unavailable => return None,
    };
    let cost = kcode_intelligence_router::estimate_cost(model, &metering)?;
    Some(kcode_session_history::chatend::ProviderCostEstimate {
        usd_nanos: cost.usd_nanos,
        accuracy: serde_json::to_value(cost.accuracy).ok()?,
        pricing_version: cost.pricing_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_names_are_stable_code_defaults() {
        assert_eq!(OPENAI_API_KEY_SECRET, "openai-api-key");
        assert_eq!(GEMINI_API_KEY_SECRET, "gemini-api-key");
        assert_eq!(TELEGRAM_BOT_TOKEN_SECRET, "telegram-bot-token");
        assert_eq!(CRATES_IO_KEY_SECRET, "cratesio-key");
        assert_eq!(KWEB_WRITER_SIGNING_KEY_SECRET, "kweb-writer-signing-key");
        assert_eq!(KWEB_WRITERS_SECRET, "kweb-writers-by-priority");
    }

    #[test]
    fn legacy_session_model_prefers_exact_model_and_understands_old_attribution() {
        assert_eq!(
            legacy_session_provider_model(&serde_json::json!({
                "providerModel":"gpt-5.6-terra",
                "commitAuthor":"gpt-5.6-sol-xhigh"
            }))
            .as_deref(),
            Some("gpt-5.6-terra")
        );
        assert_eq!(
            legacy_session_provider_model(&serde_json::json!({
                "commitAuthor":"gpt-5.6-sol-xhigh"
            }))
            .as_deref(),
            Some("gpt-5.6-sol")
        );
    }

    #[test]
    fn legacy_provider_cost_uses_the_intelligence_catalog() {
        let cost = legacy_provider_cost(
            "gpt-5.6-sol",
            &kcode_session_history::chatend::ProviderMetering::Tokens(
                kcode_session_history::chatend::ProviderTokenUsage {
                    input_tokens: 10,
                    cached_input_tokens: 20,
                    thinking_tokens: 3,
                    output_tokens: 4,
                },
            ),
        )
        .unwrap();
        assert_eq!(cost.usd_nanos, 270_000);
        assert_eq!(cost.pricing_version, "kennedy-provider-pricing-2026-07-30");
    }

    #[test]
    fn persistent_path_defaults_are_under_data() {
        let args = Args::try_parse_from(["kennedy-server"]).unwrap();
        for path in [
            &args.vault_path,
            &args.kweb_root,
            &args.conversation_history_database,
            &args.session_directory,
            &args.session_history_file,
            &args.telegram_database,
            &args.user_database,
            &args.legacy_audio_ingress_database,
            &args.audio_ingress_directory,
            &args.intelligence_usage_directory,
            &args.rust_libs_root,
            &args.web_libs_root,
            &args.web_libs_published_root,
            &args.rust_bins_root,
            &args.rust_bin_artifacts_root,
        ] {
            assert!(
                path.starts_with("./data"),
                "persistent default is outside data/: {}",
                path.display()
            );
        }
        for path in [
            &args.rust_libs_root,
            &args.web_libs_root,
            &args.web_libs_published_root,
            &args.rust_bins_root,
            &args.rust_bin_artifacts_root,
        ] {
            assert!(
                path.starts_with("./data/kcode"),
                "managed Kcode default is outside data/kcode/: {}",
                path.display()
            );
        }
        assert_eq!(
            args.system_prompts_dir,
            PathBuf::from("./KennedyServer/runtime/system-prompts")
        );
        orchestration::Manuals::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("runtime/system-prompts"),
        )
        .unwrap();
    }

    #[test]
    fn codex_launcher_preserves_catalog_and_runtime_boundaries() {
        let launcher = include_str!("../../scripts/codex-safe");
        let runtime_builder = include_str!("../../scripts/build-codex-safe-runtime");
        let default_catalog = "${TMPDIR:-/tmp}/kcode-codex-catalogs";

        assert!(launcher.contains(default_catalog));
        assert!(!launcher.contains("kennedy-codex-catalogs"));
        assert!(launcher.contains("CODEX_SAFE_CARGO_CACHE_DIR"));
        assert!(launcher.contains("CODEX_IMAGE:-local/codex-dev"));
        assert!(runtime_builder.contains("CODEX_SAFE_RUNTIME_IMAGE:-local/codex-dev"));
        assert!(runtime_builder.contains("chromium"));
        assert!(runtime_builder.contains("--security-opt=no-new-privileges"));
        assert!(runtime_builder.contains("--cap-drop=all"));
        assert!(runtime_builder.contains("--cap-add=SYS_CHROOT"));
    }

    #[test]
    fn native_orchestration_remains_a_rust_backend_concern() {
        let worker = include_str!("orchestration/worker.rs");
        let session = include_str!("orchestration/session.rs");
        assert!(worker.contains("Native Rust orchestration worker ready"));
        assert!(session.contains("struct Session"));
    }

    #[tokio::test]
    async fn published_router_serves_the_floating_html_ui_contract() {
        let root = std::env::temp_dir().join(format!(
            "kennedy-web-routing-integration-test-{}",
            uuid::Uuid::new_v4()
        ));
        for version in ["0.1.0", "0.1.2"] {
            let version_root = root.join(format!("module/kcode-kui-loader/v{version}"));
            std::fs::create_dir_all(&version_root).unwrap();
            std::fs::write(
                version_root.join("kcode-web.json"),
                format!(
                    r#"{{"name":"kcode-kui-loader","version":"{version}","entry":"index.js","tests":"tests.js"}}"#
                ),
            )
            .unwrap();
            std::fs::write(
                version_root.join("index.html"),
                format!("<!doctype html><title>Kennedy {version}</title>\n"),
            )
            .unwrap();
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router_root = root.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, kcode_web_semver_routing::router(router_root))
                .await
                .unwrap();
        });
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        let floating = client
            .get(format!(
                "http://{address}/lib/kcode-kui-loader/v0.1/index.html"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(floating.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            floating.headers()[reqwest::header::LOCATION],
            "/lib/kcode-kui-loader/v0.1.2/index.html"
        );
        assert_eq!(
            floating.headers()[reqwest::header::CACHE_CONTROL],
            "no-store, max-age=0"
        );

        let exact = client
            .get(format!(
                "http://{address}/lib/kcode-kui-loader/v0.1.2/index.html"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(exact.status(), reqwest::StatusCode::OK);
        assert_eq!(
            exact.headers()[reqwest::header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(
            exact.headers()[reqwest::header::CACHE_CONTROL],
            "public, max-age=31536000, immutable"
        );
        assert!(exact.text().await.unwrap().contains("Kennedy 0.1.2"));

        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn unified_dev_tools_service_opens_all_roots_and_routes_three_source_kinds() {
        let directory = std::env::temp_dir().join(format!(
            "kennedy-dev-tools-open-test-{}",
            uuid::Uuid::new_v4()
        ));
        let rust_libraries = directory.join("kcode-rust-libs");
        let web_libraries = directory.join("kcode-web-libs");
        let web_publications = directory.join("kcode-web-libs-published");
        let rust_binaries = directory.join("kcode-rust-bins");
        let rust_binary_artifacts = directory.join("kcode-rust-bin-artifacts");
        let service = kcode_dev_tools::Service::open(kcode_dev_tools::Config {
            rust_libraries_root: rust_libraries.clone(),
            web_libraries_root: web_libraries.clone(),
            web_publications_root: web_publications.clone(),
            rust_binaries_root: rust_binaries.clone(),
            rust_binary_publications_root: rust_binary_artifacts.clone(),
            crates_io_registry_token: "test-token".into(),
        })
        .unwrap();

        assert_eq!(
            service.web_libraries_root(),
            std::fs::canonicalize(&web_libraries).unwrap()
        );
        assert_eq!(
            service.web_publications_root(),
            std::fs::canonicalize(&web_publications).unwrap()
        );
        for path in [
            rust_libraries,
            web_libraries,
            web_publications,
            rust_binaries,
            rust_binary_artifacts,
        ] {
            assert!(
                path.is_dir(),
                "managed root was not created: {}",
                path.display()
            );
        }
        for (create, open, write, name, path, kind) in [
            (
                kcode_dev_tools::CREATE_RUST_LIB_TOOL,
                kcode_dev_tools::OPEN_RUST_LIB_TOOL,
                kcode_dev_tools::WRITE_FILE_FREEFORM_RUST_LIB_TOOL,
                "kennedy-test-lib",
                "src/extra.rs",
                kcode_dev_tools::ManagedSourceKind::RustLibrary,
            ),
            (
                kcode_dev_tools::CREATE_WEB_LIB_TOOL,
                kcode_dev_tools::OPEN_WEB_LIB_TOOL,
                kcode_dev_tools::WRITE_FILE_FREEFORM_WEB_LIB_TOOL,
                "kennedy-test-web",
                "extra.js",
                kcode_dev_tools::ManagedSourceKind::WebLibrary,
            ),
            (
                kcode_dev_tools::CREATE_RUST_BIN_TOOL,
                kcode_dev_tools::OPEN_RUST_BIN_TOOL,
                kcode_dev_tools::WRITE_FILE_FREEFORM_RUST_BIN_TOOL,
                "kennedy-test-bin",
                "src/extra.rs",
                kcode_dev_tools::ManagedSourceKind::RustBinary,
            ),
        ] {
            let created = service
                .execute(
                    "create-session",
                    create,
                    serde_json::json!({"name":name}),
                    Vec::new(),
                )
                .await
                .unwrap();
            assert_eq!(created.snapshot.unwrap().kind, kind);
            let written = service
                .execute(
                    "create-session",
                    write,
                    serde_json::json!({
                        "name":name,
                        "path":path,
                        "contents":"// Kennedy managed source\n",
                    }),
                    Vec::new(),
                )
                .await
                .unwrap();
            assert_eq!(written.snapshot.unwrap().kind, kind);

            let open_result = service
                .execute(
                    "open-session",
                    open,
                    serde_json::json!({"name":name}),
                    Vec::new(),
                )
                .await
                .unwrap();
            assert_eq!(open_result.snapshot.unwrap().kind, kind);
        }
        let asset = service
            .execute(
                "create-session",
                kcode_dev_tools::ATTACH_OBJECT_WEB_LIB_TOOL,
                serde_json::json!({
                    "name":"kennedy-test-web",
                    "path":"assets/fonts/display.woff2",
                    "objectId":"pending:1",
                }),
                vec![vec![0, 159, 146, 150, 255]],
            )
            .await
            .unwrap();
        let snapshot = asset.snapshot.unwrap();
        assert_eq!(
            snapshot.kind,
            kcode_dev_tools::ManagedSourceKind::WebLibrary
        );
        assert!(snapshot.text.contains("Asset: assets/fonts/display.woff2"));
        assert!(snapshot.text.contains("Bytes: 5"));
        assert!(!snapshot.text.contains("SHA-256:"));
        assert_eq!(service.release("create-session").await.unwrap(), 3);
        assert_eq!(service.release("open-session").await.unwrap(), 3);
        drop(service);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_optional_secret_disables_only_its_feature() {
        let vault = CredentialVault::empty();
        assert!(
            resolve_optional_secret(&vault, "openai-api-key", "transcription")
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_optional_secret(&vault, "", "disabled")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn required_secret_must_be_present() {
        let mut vault = CredentialVault::empty();
        let error =
            resolve_required_secret(&vault, CRATES_IO_KEY_SECRET, "publication").unwrap_err();
        assert!(error.to_string().contains(CRATES_IO_KEY_SECRET));

        vault
            .set(CRATES_IO_KEY_SECRET, "test-crates-io-key".into())
            .unwrap();
        assert_eq!(
            resolve_required_secret(&vault, CRATES_IO_KEY_SECRET, "publication").unwrap(),
            "test-crates-io-key"
        );
    }

    #[test]
    fn legacy_audio_database_is_copied_once_into_the_persistence_root() {
        let directory = std::env::temp_dir().join(format!(
            "kennedy-audio-migration-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let legacy = directory.join("legacy.sqlite3");
        let current = directory.join("audio-ingress/state.sqlite3");
        let source = rusqlite::Connection::open(&legacy).unwrap();
        source
            .execute_batch("CREATE TABLE marker(value TEXT NOT NULL);")
            .unwrap();
        source
            .execute("INSERT INTO marker(value) VALUES('legacy')", [])
            .unwrap();
        drop(source);

        migrate_audio_ingress_database(&legacy, &current).unwrap();
        let migrated = rusqlite::Connection::open(&current).unwrap();
        let value: String = migrated
            .query_row("SELECT value FROM marker", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, "legacy");
        migrated
            .execute("UPDATE marker SET value='current'", [])
            .unwrap();
        drop(migrated);

        migrate_audio_ingress_database(&legacy, &current).unwrap();
        let value: String = rusqlite::Connection::open(&current)
            .unwrap()
            .query_row("SELECT value FROM marker", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, "current");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn occupied_kweb_address_prevents_server_from_opening_persistent_state() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-server-lock-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap().to_string();
        let vault = directory.join("vault.age");
        let kmap = directory.join("kweb");
        let conversations = directory.join("conversations.sqlite3");
        let telegram = directory.join("telegram.sqlite3");
        let users = directory.join("users.sqlite3");
        let audio = directory.join("audio.sqlite3");
        let audio_media = directory.join("audio-media");
        let args = Args {
            vault_path: vault.clone(),
            command: None,
            kweb_bind: bind,
            kweb_root: kmap.clone(),
            conversation_history_database: conversations.clone(),
            session_directory: directory.join("sessions"),
            session_history_file: directory.join("session-history.txt"),
            telegram_database: telegram.clone(),
            user_database: users.clone(),
            legacy_audio_ingress_database: audio.clone(),
            audio_ingress_directory: audio_media.clone(),
            intelligence_usage_directory: directory.join("intelligence-usage"),
            rust_libs_root: directory.join("rust-libs"),
            web_libs_root: directory.join("kcode-web-libs"),
            web_libs_published_root: directory.join("kcode-web-libs-published"),
            rust_bins_root: directory.join("kcode-rust-bins"),
            rust_bin_artifacts_root: directory.join("kcode-rust-bin-artifacts"),
            system_prompts_dir: directory.join("prompts"),
            telegram_bootstrap_username: "@test".to_owned(),
            telegram_max_voice_bytes: 1024,
            audio_ingress_max_upload_bytes: 1024,
        };

        let error = run_server(args, vault.clone()).await.unwrap_err();
        assert!(error.to_string().contains("binding Kweb listener"));
        assert!(!vault.exists());
        assert!(!kmap.exists());
        assert!(!conversations.exists());
        assert!(!telegram.exists());
        assert!(!users.exists());
        assert!(!audio.exists());
        assert!(!audio_media.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
