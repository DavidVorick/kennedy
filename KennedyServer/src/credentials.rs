use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use age::secrecy::SecretString;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

const VAULT_VERSION: u32 = 1;

#[derive(Deserialize)]
struct VaultPayload {
    version: u32,
    secrets: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct VaultPayloadRef<'a> {
    version: u32,
    secrets: &'a BTreeMap<String, String>,
}

pub(crate) struct CredentialVault {
    secrets: BTreeMap<String, String>,
}

impl CredentialVault {
    pub(crate) fn empty() -> Self {
        Self {
            secrets: BTreeMap::new(),
        }
    }

    pub(crate) fn unlock(path: &Path, passphrase: SecretString) -> anyhow::Result<Self> {
        let ciphertext = fs::read(path)
            .with_context(|| format!("reading credential vault {}", path.display()))?;
        let decryptor = age::Decryptor::new(&ciphertext[..])
            .with_context(|| format!("reading encrypted credential vault {}", path.display()))?;
        let identity = age::scrypt::Identity::new(passphrase);
        let mut reader = decryptor
            .decrypt(std::iter::once(&identity as &dyn age::Identity))
            .context("unlocking credential vault; the passphrase may be incorrect")?;
        let mut plaintext = Zeroizing::new(Vec::new());
        reader
            .read_to_end(&mut plaintext)
            .context("decrypting credential vault")?;
        let payload: VaultPayload =
            serde_json::from_slice(&plaintext).context("parsing decrypted credential vault")?;
        if payload.version != VAULT_VERSION {
            anyhow::bail!(
                "credential vault version {} is unsupported",
                payload.version
            );
        }
        Ok(Self {
            secrets: payload.secrets,
        })
    }

    pub(crate) fn save(&self, path: &Path, passphrase: &SecretString) -> anyhow::Result<()> {
        let payload = VaultPayloadRef {
            version: VAULT_VERSION,
            secrets: &self.secrets,
        };
        let plaintext =
            Zeroizing::new(serde_json::to_vec(&payload).context("serializing credential vault")?);
        let encryptor = age::Encryptor::with_user_passphrase(passphrase.clone());
        let mut ciphertext = Vec::new();
        {
            let mut writer = encryptor
                .wrap_output(&mut ciphertext)
                .context("starting credential vault encryption")?;
            writer
                .write_all(&plaintext)
                .context("encrypting credential vault")?;
            writer
                .finish()
                .context("finishing credential vault encryption")?;
        }
        write_private_atomic(path, &ciphertext)
    }

    pub(crate) fn set(&mut self, name: &str, value: String) -> anyhow::Result<()> {
        validate_secret_name(name)?;
        if value.is_empty() {
            anyhow::bail!("secret values cannot be empty");
        }
        if let Some(mut previous) = self.secrets.insert(name.to_owned(), value) {
            previous.zeroize();
        }
        Ok(())
    }

    pub(crate) fn remove(&mut self, name: &str) -> anyhow::Result<bool> {
        validate_secret_name(name)?;
        if let Some(mut value) = self.secrets.remove(name) {
            value.zeroize();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn secret(&self, name: &str) -> anyhow::Result<Option<SecretString>> {
        validate_secret_name(name)?;
        Ok(self.secrets.get(name).cloned().map(SecretString::from))
    }

    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.secrets.keys().map(String::as_str)
    }
}

impl Drop for CredentialVault {
    fn drop(&mut self) {
        for value in self.secrets.values_mut() {
            value.zeroize();
        }
    }
}

fn validate_secret_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        anyhow::bail!(
            "secret names must contain 1-128 ASCII letters, numbers, dots, dashes, or underscores"
        );
    }
    Ok(())
}

fn write_private_atomic(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("creating credential vault directory {}", parent.display()))?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("kennedy-secrets.age");
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let result = (|| -> anyhow::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("creating temporary vault {}", temporary.display()))?;
        file.write_all(contents)
            .context("writing encrypted credential vault")?;
        file.sync_all()
            .context("syncing encrypted credential vault")?;
        fs::rename(&temporary, path)
            .with_context(|| format!("installing credential vault {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).with_context(|| {
                format!("setting private permissions on vault {}", path.display())
            })?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use age::secrecy::ExposeSecret;

    fn passphrase(value: &str) -> SecretString {
        SecretString::from(value.to_owned())
    }

    #[test]
    fn encrypted_vault_round_trips_generic_named_secrets_without_plaintext() {
        let directory = std::env::temp_dir().join(format!("kennedy-vault-{}", Uuid::new_v4()));
        let path = directory.join("secrets.age");
        let password = passphrase("correct horse battery staple");
        let mut vault = CredentialVault::empty();
        vault
            .set("openai-api-key", "sk-test-private".into())
            .unwrap();
        vault
            .set("telegram-bot-token", "123456:private".into())
            .unwrap();
        vault.save(&path, &password).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let ciphertext = fs::read(&path).unwrap();
        assert!(
            !ciphertext
                .windows(b"sk-test-private".len())
                .any(|window| window == b"sk-test-private")
        );
        let restored = CredentialVault::unlock(&path, password).unwrap();
        assert_eq!(
            restored
                .secret("openai-api-key")
                .unwrap()
                .unwrap()
                .expose_secret(),
            "sk-test-private"
        );
        assert_eq!(
            restored.names().collect::<Vec<_>>(),
            vec!["openai-api-key", "telegram-bot-token"]
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn wrong_passphrase_cannot_unlock_vault() {
        let directory = std::env::temp_dir().join(format!("kennedy-vault-{}", Uuid::new_v4()));
        let path = directory.join("secrets.age");
        CredentialVault::empty()
            .save(&path, &passphrase("right passphrase"))
            .unwrap();
        let error = CredentialVault::unlock(&path, passphrase("wrong passphrase"))
            .err()
            .unwrap();
        assert!(error.to_string().contains("unlocking credential vault"));
        fs::remove_dir_all(directory).unwrap();
    }
}
