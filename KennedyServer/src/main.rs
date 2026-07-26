mod audio_ingress;
mod backup;
mod credentials;
mod intelligence;
mod kmap_http;
mod kmap_size;
mod kweb_writer;
mod orchestration;
mod rust_lib_tools;
mod telegram_identity;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use age::secrecy::{ExposeSecret, SecretString};
use anyhow::Context;
use backup::BackupOptions;
use clap::{Parser, Subcommand};
use credentials::CredentialVault;
use kcode_kweb_db::{Config as KwebConfig, NoopGossip, WriterId};
use zeroize::{Zeroize, Zeroizing};

const OPENAI_API_KEY_SECRET: &str = "openai-api-key";
const GEMINI_API_KEY_SECRET: &str = "gemini-api-key";
const TELEGRAM_BOT_TOKEN_SECRET: &str = "telegram-bot-token";
const CRATES_IO_KEY_SECRET: &str = "cratesio-key";
const KWEB_WRITER_SIGNING_KEY_SECRET: &str = "kweb-writer-signing-key";
const KWEB_WRITERS_SECRET: &str = "kweb-writers-by-priority";
const RUST_LIBS_ROOT: &str = "/home/user/dev/kennedy/kcode/kcode-rust-libs";
#[derive(Parser, Debug)]
struct Args {
    #[arg(long, global = true, default_value = "./data/kennedy-secrets.age")]
    vault_path: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, global = true, default_value = "127.0.0.1:4321")]
    kweb_bind: String,
    #[arg(long, default_value = "127.0.0.1:4324")]
    telegram_bind: String,
    #[arg(long, default_value = "http://127.0.0.1:4321")]
    frontend_origin: String,
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
        long = "memory-ingress-database",
        global = true,
        default_value = "./data/kennedy-memory-ingress.sqlite3",
        help = "Kennedy-owned audio transcript memory-ingress queue"
    )]
    audio_memory_ingress_database: PathBuf,
    #[arg(
        long,
        alias = "audio-ingress-media",
        global = true,
        default_value = "./data/audio-ingress-media",
        help = "AudioIngress-owned persistence root (database and original audio)"
    )]
    audio_ingress_directory: PathBuf,
    #[arg(long, default_value = "./Frontend/public")]
    frontend_dir: PathBuf,
    #[arg(long, default_value = "./Frontend/SystemPrompts")]
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
    /// Create a verified offline archive of all Kennedy-owned persistent data.
    Backup {
        /// Directory in which to create the timestamped .tar.gz archive.
        #[arg(long, default_value = "./data/backups")]
        backup_dir: PathBuf,
        /// Omit immutable Kweb objects while retaining nodes and transaction metadata.
        #[arg(long)]
        lightweight_kweb: bool,
    },
    /// Estimate the token footprint of all current Kmap node text.
    KmapSize,
    /// Generate Kennedy's permanent Kweb key inside the encrypted vault.
    ProvisionKwebWriter,
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
                "kennedy_server=info,kcode_kweb_db=info,kcode_codex_runtime=info,kennedy_conversation_history=info,kcode_tg_kennedy_bot=info,tower_http=info".into()
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
        Some(Command::Backup {
            backup_dir,
            lightweight_kweb,
        }) => {
            let path = backup::run(BackupOptions {
                bind: args.kweb_bind,
                backup_dir,
                kweb_root: args.kweb_root,
                include_kweb_objects: !lightweight_kweb,
                conversation_database: args.conversation_history_database,
                session_directory: args.session_directory,
                session_history_file: args.session_history_file,
                telegram_database: args.telegram_database,
                user_database: args.user_database,
                legacy_audio_database: Some(args.legacy_audio_ingress_database),
                audio_memory_ingress_database: args.audio_memory_ingress_database,
                audio_ingress_directory: args.audio_ingress_directory,
                vault: vault_path,
            })
            .await?;
            println!("Created Kennedy backup {}", path.display());
            Ok(())
        }
        Some(Command::KmapSize) => {
            let _maintenance_guard =
                maintenance_guard(&args.kweb_bind, "measuring the Kweb").await?;
            let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
            let vault = CredentialVault::unlock(&vault_path, passphrase)?;
            let size = kmap_size::measure(&args.kweb_root, kweb_config(&vault)?)?;
            println!("{}", kmap_size::render(&size));
            Ok(())
        }
        Some(Command::ProvisionKwebWriter) => {
            let _maintenance_guard =
                maintenance_guard(&args.kweb_bind, "provisioning the Kweb writer").await?;
            provision_kweb_writer(&args.kweb_root, &vault_path)
        }
        None => run_server(args, vault_path).await,
    }
}

async fn run_server(args: Args, vault_path: PathBuf) -> anyhow::Result<()> {
    // Bind the public Kennedy address before opening any persistent state. The
    // backup command owns this same address for its entire run, making the port
    // an inter-process offline lock rather than only a late startup check.
    let kweb_listener = tokio::net::TcpListener::bind(&args.kweb_bind)
        .await
        .with_context(|| format!("binding Kweb listener {}", args.kweb_bind))?;
    ensure_runtime_parent_directories(&args, &vault_path)?;
    let orchestration_telegram_base = telegram_relay_http_base(&args.telegram_bind);
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
        "OpenAI transcription and media annotation",
    )?;
    let gemini_api_key = resolve_optional_secret(
        &vault,
        GEMINI_API_KEY_SECRET,
        "Gemini search, media annotation, and audio transcription",
    )?;
    let gemini = gemini_api_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(kcode_gemini_api::Gemini::open)
        .transpose()
        .context("opening shared Gemini client")?;
    // AudioIngress still publishes a Gemini 0.1 constructor. Keep its
    // compatibility client isolated until that library adopts Gemini 0.2.
    let audio_gemini = gemini_api_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(kcode_gemini_api_legacy::Gemini::open)
        .transpose()
        .context("opening AudioIngress Gemini compatibility client")?;
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
    let kmap_service = kmap_http::Service::new(kmap, system_roots, &args.user_database)?;
    let rust_lib_tools = rust_lib_tools::RustLibToolService::new(RUST_LIBS_ROOT, crates_io_key)
        .with_context(|| format!("opening managed Rust libraries root {RUST_LIBS_ROOT}"))?;
    let telegram_identity = std::sync::Arc::new(telegram_identity::Directory::open(
        &args.user_database,
        &args.telegram_bootstrap_username,
    )?);
    let history_service =
        kennedy_conversation_history::open(kennedy_conversation_history::Config {
            directory: args.session_directory,
            completed_list: args.session_history_file,
            max_request_bytes: 32 * 1024 * 1024 * 1024,
        })?;
    let history_router = kennedy_conversation_history::router(history_service.clone());
    let intelligence_service =
        intelligence::open(openai_api_key, gemini.clone(), codex_catalog_cache.clone()).await?;
    let intelligence_router = intelligence::router(intelligence_service.clone());
    let telegram = kcode_tg_kennedy_bot::Config {
        bind: args.telegram_bind,
        database: args.telegram_database,
        allowed_origins: vec![args.frontend_origin.clone()],
        bot_token: telegram_bot_token,
        identity_sink: telegram_identity.clone(),
        max_voice_bytes: args.telegram_max_voice_bytes,
    };
    let audio_gemini = audio_gemini.context(
        "AudioIngress requires the gemini-api-key credential; configure it before starting Kennedy",
    )?;
    let mut config =
        kcode_codex_runtime::CodexConfig::new(kcode_audio_ingress::RECONCILIATION_MODEL);
    config.validation_reasoning_effort = kcode_codex_runtime::ReasoningEffort::XHigh;
    let codex = kcode_codex_runtime::Codex::open(config, codex_catalog_cache)
        .await
        .context("opening Codex audio-reconciliation runtime")?;
    let audio_transcriber = kcode_audio_ingress::AudioTranscriber::new(audio_gemini, codex);
    let audio_state_database = args.audio_ingress_directory.join("state.sqlite3");
    migrate_audio_ingress_database(&args.legacy_audio_ingress_database, &audio_state_database)?;
    let audio =
        kcode_audio_ingress::AudioIngress::open(&args.audio_ingress_directory, audio_transcriber)
            .await?;
    let audio_service = audio_ingress::Service::open(
        audio,
        &args.audio_memory_ingress_database,
        Some(&audio_state_database),
        args.audio_ingress_max_upload_bytes,
    )?;
    let audio_ingress_router = audio_ingress::router(audio_service.clone());
    let orchestration = orchestration::Config {
        system_prompts_directory: args.system_prompts_dir.clone(),
        telegram_relay_base: orchestration_telegram_base,
        telegram_max_media_bytes: args.telegram_max_voice_bytes,
        #[cfg(test)]
        kweb_base: String::new(),
        #[cfg(test)]
        intelligence_base: String::new(),
        #[cfg(test)]
        conversation_history_base: String::new(),
        #[cfg(test)]
        audio_ingress_base: String::new(),
        telegram_web_user_handle: args.telegram_bootstrap_username,
    };
    let orchestration_api = orchestration::Api::local(
        &orchestration.telegram_relay_base,
        orchestration::LocalServices {
            kmap: kmap_service.clone(),
            intelligence: intelligence_service,
            history: history_service,
            audio: audio_service,
            directory: telegram_identity.clone(),
            rust_lib_tools,
        },
    )?;
    tokio::try_join!(
        kmap_http::serve_with_listener(
            kmap_service,
            args.frontend_dir,
            kmap_http::MergedRouters::new(
                intelligence_router,
                history_router,
                audio_ingress_router,
            ),
            kweb_listener,
        ),
        kcode_tg_kennedy_bot::serve(telegram),
        orchestration::run(orchestration, orchestration_api),
    )?;
    Ok(())
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
        &args.legacy_audio_ingress_database,
        &args.audio_memory_ingress_database,
        &args.audio_ingress_directory,
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

fn telegram_relay_http_base(bind: &str) -> String {
    let bind = bind.trim();
    let Ok(address) = bind.parse::<SocketAddr>() else {
        return format!("http://{bind}");
    };
    let ip = if address.ip().is_unspecified() {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        address.ip()
    };
    match ip {
        IpAddr::V4(ip) => format!("http://{ip}:{}", address.port()),
        IpAddr::V6(ip) => format!("http://[{ip}]:{}", address.port()),
    }
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

fn provision_kweb_writer(kweb_root: &Path, vault_path: &Path) -> anyhow::Result<()> {
    let (mut vault, passphrase) = unlock_for_edit(vault_path)?;
    let mut signing_key = if let Some(existing) = vault.secret(KWEB_WRITER_SIGNING_KEY_SECRET)? {
        let decoded = hex::decode(existing.expose_secret().trim())
            .context("decoding the existing Kweb writer signing key")?;
        Zeroizing::new(
            decoded
                .try_into()
                .map_err(|_| anyhow::anyhow!("the existing Kweb signing key is not 32 bytes"))?,
        )
    } else {
        let generated = Zeroizing::new(rand::random::<[u8; 32]>());
        vault.set(KWEB_WRITER_SIGNING_KEY_SECRET, hex::encode(*generated))?;
        vault.save(vault_path, &passphrase)?;
        generated
    };
    let permanent_writer = WriterId::from_signing_key(&signing_key);
    let writers = if kweb_root.exists() {
        kweb_writer::install_permanent_writer(kweb_root, permanent_writer)?
    } else {
        vec![permanent_writer]
    };
    vault.set(
        KWEB_WRITERS_SECRET,
        writers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
    )?;
    vault.save(vault_path, &passphrase)?;
    signing_key.zeroize();
    println!(
        "Provisioned Kennedy Kweb writer {permanent_writer}. The private key was written only to the encrypted vault."
    );
    Ok(())
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
        assert_eq!(
            RUST_LIBS_ROOT,
            "/home/user/dev/kennedy/kcode/kcode-rust-libs"
        );
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
    fn telegram_relay_urls_are_valid_for_wildcard_and_ipv6_binds() {
        assert_eq!(
            telegram_relay_http_base("0.0.0.0:4321"),
            "http://127.0.0.1:4321"
        );
        assert_eq!(
            telegram_relay_http_base("[::]:4322"),
            "http://127.0.0.1:4322"
        );
        assert_eq!(telegram_relay_http_base("[::1]:9876"), "http://[::1]:9876");
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
        let memory_ingress = directory.join("memory-ingress.sqlite3");
        let audio_media = directory.join("audio-media");
        let args = Args {
            vault_path: vault.clone(),
            command: None,
            kweb_bind: bind,
            telegram_bind: "127.0.0.1:0".to_owned(),
            frontend_origin: "http://127.0.0.1:4321".to_owned(),
            kweb_root: kmap.clone(),
            conversation_history_database: conversations.clone(),
            session_directory: directory.join("sessions"),
            session_history_file: directory.join("session-history.txt"),
            telegram_database: telegram.clone(),
            user_database: users.clone(),
            legacy_audio_ingress_database: audio.clone(),
            audio_memory_ingress_database: memory_ingress.clone(),
            audio_ingress_directory: audio_media.clone(),
            frontend_dir: directory.join("frontend"),
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
        assert!(!memory_ingress.exists());
        assert!(!audio_media.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
