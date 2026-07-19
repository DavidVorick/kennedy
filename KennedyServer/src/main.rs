mod backup;
mod credentials;
mod kmap_http;
mod kmap_size;
mod rust_lib_tools;

use std::path::{Path, PathBuf};

use age::secrecy::{ExposeSecret, SecretString};
use anyhow::Context;
use backup::BackupOptions;
use clap::{Parser, Subcommand};
use credentials::CredentialVault;
use zeroize::Zeroize;

const OPENAI_API_KEY_SECRET: &str = "openai-api-key";
const GEMINI_API_KEY_SECRET: &str = "gemini-api-key";
const TELEGRAM_BOT_TOKEN_SECRET: &str = "telegram-bot-token";
const RUST_LIBS_ROOT: &str = "/home/user/dev/kennedy/kcode/kcode-rust-libs";

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, global = true, default_value = "./kennedy-secrets.age")]
    vault_path: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, global = true, default_value = "127.0.0.1:4321")]
    kweb_bind: String,
    #[arg(long, default_value = "127.0.0.1:4322")]
    intelligence_bind: String,
    #[arg(long, default_value = "127.0.0.1:4323")]
    conversation_history_bind: String,
    #[arg(long, default_value = "127.0.0.1:4324")]
    telegram_bind: String,
    #[arg(long, default_value = "127.0.0.1:4325")]
    audio_ingress_bind: String,
    #[arg(long, default_value = "http://127.0.0.1:4321")]
    frontend_origin: String,
    #[arg(long, global = true, default_value = "./kweb-db-core.sqlite3")]
    kweb_database: PathBuf,
    #[arg(long, global = true, default_value = "./kweb-provenance-artifacts")]
    kweb_provenance_artifacts: PathBuf,
    #[arg(long, global = true, default_value = "./kennedy-conversations.sqlite3")]
    conversation_history_database: PathBuf,
    #[arg(long, global = true, default_value = "./kennedy-telegram.sqlite3")]
    telegram_database: PathBuf,
    #[arg(long, global = true, default_value = "./kennedy-users.sqlite3")]
    user_database: PathBuf,
    #[arg(long, global = true, default_value = "./kennedy-audio.sqlite3")]
    audio_ingress_database: PathBuf,
    #[arg(long, global = true, default_value = "./kennedy-audio-ingress")]
    audio_ingress_media: PathBuf,
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
        #[arg(long, default_value = "./backups")]
        backup_dir: PathBuf,
        /// Omit large Kweb provenance artifacts while retaining the Kweb database and artifact metadata.
        #[arg(long)]
        lightweight_kweb: bool,
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
                "kennedy_server=info,kweb_db_core=info,kennedy_intelligence=info,kennedy_conversation_history=info,kennedy_telegram_relay=info,tower_http=info".into()
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
                kmap_database: args.kweb_database,
                kmap_artifact_directory: args.kweb_provenance_artifacts,
                include_kmap_artifacts: !lightweight_kweb,
                conversation_database: args.conversation_history_database,
                telegram_database: args.telegram_database,
                user_database: args.user_database,
                audio_database: args.audio_ingress_database,
                audio_media_directory: args.audio_ingress_media,
                vault: vault_path,
            })
            .await?;
            println!("Created Kennedy backup {}", path.display());
            Ok(())
        }
        Some(Command::KmapSize) => {
            let size = kmap_size::measure(&args.kweb_database, &args.kweb_provenance_artifacts)?;
            println!("{}", kmap_size::render(&size));
            Ok(())
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
    let vault = if vault_path.exists() {
        let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
        CredentialVault::unlock(&vault_path, passphrase)?
    } else {
        tracing::warn!(path=%vault_path.display(), "Kennedy credential vault does not exist; configured secret-backed features are disabled");
        CredentialVault::empty()
    };
    let transcription_api_key =
        resolve_optional_secret(&vault, OPENAI_API_KEY_SECRET, "OpenAI transcription")?;
    let gemini_api_key = resolve_optional_secret(
        &vault,
        GEMINI_API_KEY_SECRET,
        "Gemini search and audio transcription",
    )?;
    let telegram_bot_token =
        resolve_optional_secret(&vault, TELEGRAM_BOT_TOKEN_SECRET, "Telegram relay")?;
    let codex_catalog_cache =
        kennedy_codex_runtime::CatalogCache::new(kennedy_codex_runtime::DEFAULT_CODEX_EXECUTABLE);
    let (kmap, system_roots) = kmap_http::initialize(
        &args.kweb_database,
        &args.kweb_provenance_artifacts,
        &args.user_database,
    )?;
    let rust_lib_tools = rust_lib_tools::RustLibToolService::new(RUST_LIBS_ROOT)
        .with_context(|| format!("opening managed Rust libraries root {RUST_LIBS_ROOT}"))?;
    let history = kennedy_conversation_history::Config {
        bind: args.conversation_history_bind,
        database: args.conversation_history_database,
        allowed_origins: vec![args.frontend_origin.clone()],
        max_request_bytes: 128 * 1024 * 1024,
    };
    let intelligence = kennedy_intelligence::ServeOptions {
        bind: args.intelligence_bind,
        allowed_origins: vec![args.frontend_origin.clone()],
    };
    let telegram = kennedy_telegram_relay::Config {
        bind: args.telegram_bind,
        database: args.telegram_database,
        user_database: args.user_database,
        allowed_origins: vec![args.frontend_origin.clone()],
        bot_token: telegram_bot_token,
        bootstrap_usernames: vec![args.telegram_bootstrap_username],
        max_voice_bytes: args.telegram_max_voice_bytes,
    };
    let audio_ingress = kennedy_audio_ingress::Config {
        bind: args.audio_ingress_bind,
        database: args.audio_ingress_database,
        media_directory: args.audio_ingress_media,
        allowed_origins: vec![args.frontend_origin],
        max_upload_bytes: args.audio_ingress_max_upload_bytes,
        gemini_api_key: gemini_api_key.clone(),
    };
    tokio::try_join!(
        kmap_http::serve_with_listener(
            kmap,
            system_roots,
            args.frontend_dir,
            args.system_prompts_dir,
            rust_lib_tools,
            kweb_listener,
        ),
        kennedy_intelligence::serve(
            intelligence,
            transcription_api_key,
            gemini_api_key,
            codex_catalog_cache.clone(),
        ),
        kennedy_conversation_history::serve(history),
        kennedy_telegram_relay::serve(telegram),
        kennedy_audio_ingress::serve(audio_ingress, codex_catalog_cache),
    )?;
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

    #[tokio::test]
    async fn occupied_kweb_address_prevents_server_from_opening_persistent_state() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-server-lock-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap().to_string();
        let vault = directory.join("vault.age");
        let kmap = directory.join("kmap.sqlite3");
        let conversations = directory.join("conversations.sqlite3");
        let telegram = directory.join("telegram.sqlite3");
        let users = directory.join("users.sqlite3");
        let audio = directory.join("audio.sqlite3");
        let audio_media = directory.join("audio-media");
        let args = Args {
            vault_path: vault.clone(),
            command: None,
            kweb_bind: bind,
            intelligence_bind: "127.0.0.1:0".to_owned(),
            conversation_history_bind: "127.0.0.1:0".to_owned(),
            telegram_bind: "127.0.0.1:0".to_owned(),
            audio_ingress_bind: "127.0.0.1:0".to_owned(),
            frontend_origin: "http://127.0.0.1:4321".to_owned(),
            kweb_database: kmap.clone(),
            kweb_provenance_artifacts: directory.join("kweb-provenance-artifacts"),
            conversation_history_database: conversations.clone(),
            telegram_database: telegram.clone(),
            user_database: users.clone(),
            audio_ingress_database: audio.clone(),
            audio_ingress_media: audio_media.clone(),
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
        assert!(!audio_media.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
