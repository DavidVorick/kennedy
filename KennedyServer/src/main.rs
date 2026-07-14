mod credentials;

use std::path::{Path, PathBuf};

use age::secrecy::{ExposeSecret, SecretString};
use clap::{Parser, Subcommand};
use credentials::{CredentialVault, resolve_vault_path};
use serde::Deserialize;
use zeroize::Zeroize;

#[derive(Deserialize, Default)]
struct SharedConfig {
    #[serde(default)]
    credentials: CredentialsConfig,
    #[serde(default)]
    audio: AudioSecretConfig,
    #[serde(default)]
    telegram: TelegramConfig,
}

#[derive(Deserialize)]
struct CredentialsConfig {
    #[serde(default = "default_vault_path")]
    vault_path: PathBuf,
}

impl Default for CredentialsConfig {
    fn default() -> Self {
        Self {
            vault_path: default_vault_path(),
        }
    }
}

#[derive(Deserialize)]
struct AudioSecretConfig {
    #[serde(default = "default_openai_secret_name")]
    api_key_secret: String,
}

impl Default for AudioSecretConfig {
    fn default() -> Self {
        Self {
            api_key_secret: default_openai_secret_name(),
        }
    }
}

#[derive(Deserialize)]
struct TelegramConfig {
    #[serde(default = "default_telegram_secret_name")]
    bot_token_secret: String,
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self {
            bot_token_secret: default_telegram_secret_name(),
        }
    }
}

fn default_vault_path() -> PathBuf {
    PathBuf::from("./kennedy-secrets.age")
}

fn default_openai_secret_name() -> String {
    "openai-api-key".into()
}

fn default_telegram_secret_name() -> String {
    "telegram-bot-token".into()
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, global = true, default_value = "./config.yaml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, default_value = "127.0.0.1:4321")]
    kweb_bind: String,
    #[arg(long, default_value = "127.0.0.1:4323")]
    conversation_history_bind: String,
    #[arg(long, default_value = "127.0.0.1:4324")]
    telegram_bind: String,
    #[arg(long, default_value = "http://127.0.0.1:4321")]
    frontend_origin: String,
    #[arg(long, default_value = "./kennedy.sqlite3")]
    kweb_database: PathBuf,
    #[arg(long, default_value = "./kennedy-conversations.sqlite3")]
    conversation_history_database: PathBuf,
    #[arg(long, default_value = "./kennedy-telegram.sqlite3")]
    telegram_database: PathBuf,
    #[arg(long, default_value = "./Frontend/public")]
    frontend_dir: PathBuf,
    #[arg(long, default_value = "./Frontend/SystemPrompts")]
    system_prompts_dir: PathBuf,
    #[arg(long, default_value_t = 12)]
    active_limit: usize,
    #[arg(long, default_value_t = 60)]
    fanout_limit: usize,
    #[arg(long, default_value = "@taek42")]
    telegram_bootstrap_username: String,
    #[arg(long, default_value_t = 20 * 1024 * 1024)]
    telegram_max_voice_bytes: usize,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create and manage generic named secrets in Kennedy's encrypted vault.
    Secrets {
        #[command(subcommand)]
        command: SecretsCommand,
    },
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
                "kennedy_server=info,kennedy_kweb=info,kennedy_intelligence=info,kennedy_conversation_history=info,kennedy_telegram_relay=info,tower_http=info".into()
            }),
        )
        .init();
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("installing TLS crypto provider"))?;
    let args = Args::parse();
    let shared_config = load_shared_config(&args.config).await?;
    let vault_path = resolve_vault_path(&args.config, &shared_config.credentials.vault_path);
    if let Some(Command::Secrets { command }) = args.command {
        return manage_secrets(command, &vault_path);
    }
    run_server(args, shared_config, vault_path).await
}

async fn run_server(
    args: Args,
    shared_config: SharedConfig,
    vault_path: PathBuf,
) -> anyhow::Result<()> {
    let vault = if vault_path.exists() {
        let passphrase = prompt_passphrase("Unlock Kennedy credential vault: ")?;
        CredentialVault::unlock(&vault_path, passphrase)?
    } else {
        tracing::warn!(path=%vault_path.display(), "Kennedy credential vault does not exist; configured secret-backed features are disabled");
        CredentialVault::empty()
    };
    let transcription_api_key = resolve_optional_secret(
        &vault,
        &shared_config.audio.api_key_secret,
        "OpenAI transcription",
    )?;
    let telegram_bot_token = resolve_optional_secret(
        &vault,
        &shared_config.telegram.bot_token_secret,
        "Telegram relay",
    )?;
    let kweb = kennedy_kweb::Config {
        bind: args.kweb_bind,
        database: args.kweb_database,
        frontend_dir: args.frontend_dir,
        system_prompts_dir: args.system_prompts_dir,
        active_limit: args.active_limit,
        fanout_limit: args.fanout_limit,
    };
    let history = kennedy_conversation_history::Config {
        bind: args.conversation_history_bind,
        database: args.conversation_history_database,
        allowed_origins: vec![args.frontend_origin.clone()],
        max_request_bytes: 128 * 1024 * 1024,
    };
    let telegram = kennedy_telegram_relay::Config {
        bind: args.telegram_bind,
        database: args.telegram_database,
        allowed_origins: vec![args.frontend_origin],
        bot_token: telegram_bot_token,
        bootstrap_usernames: vec![args.telegram_bootstrap_username],
        max_voice_bytes: args.telegram_max_voice_bytes,
    };
    let config_path = args.config;
    tokio::try_join!(
        kennedy_kweb::serve(kweb),
        kennedy_intelligence::serve(config_path, transcription_api_key),
        kennedy_conversation_history::serve(history),
        kennedy_telegram_relay::serve(telegram),
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

async fn load_shared_config(path: &Path) -> anyhow::Result<SharedConfig> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .map_err(|error| anyhow::anyhow!("reading {}: {error}", path.display()))?;
    serde_yaml::from_str(&raw)
        .map_err(|error| anyhow::anyhow!("parsing {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_config_uses_generic_vault_secret_references() {
        let config: SharedConfig = serde_yaml::from_str(include_str!("../../config.yaml")).unwrap();
        assert_eq!(
            config.credentials.vault_path,
            PathBuf::from("./kennedy-secrets.age")
        );
        assert_eq!(config.audio.api_key_secret, "openai-api-key");
        assert_eq!(config.telegram.bot_token_secret, "telegram-bot-token");
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
}
