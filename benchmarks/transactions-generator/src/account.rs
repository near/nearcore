use anyhow::Context;
use near_crypto::{InMemorySigner, PublicKey, SecretKey, Signer};
use near_primitives::types::AccountId;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::atomic::AtomicU64;

#[derive(Serialize, Deserialize, Debug)]
pub struct Account {
    #[serde(rename = "account_id")]
    pub id: AccountId,
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
    // New transaction must have a nonce bigger than this.
    pub nonce: AtomicU64,
}

impl Account {
    pub fn new(id: AccountId, secret_key: SecretKey, nonce: u64) -> Self {
        Self { id, public_key: secret_key.public_key(), secret_key, nonce: nonce.into() }
    }

    pub fn from_file(path: &Path) -> anyhow::Result<Account> {
        let content = fs::read_to_string(path)?;
        let account = serde_json::from_str(&content)
            .with_context(|| format!("failed reading file {path:?} as 'Account'"))?;
        Ok(account)
    }

    pub fn write_to_dir(&self, dir: &Path) -> anyhow::Result<()> {
        if !dir.exists() {
            std::fs::create_dir(dir)?;
        }

        let json = serde_json::to_string(self)?;
        let mut file_name = self.id.to_string();
        file_name.push_str(".json");
        let file_path = dir.join(file_name);
        fs::write(file_path, json)?;
        Ok(())
    }

    pub fn as_signer(&self) -> Signer {
        Signer::from(InMemorySigner::from_secret_key(self.id.clone(), self.secret_key.clone()))
    }
}

/// Tries to deserialize all json files in `path` as [`Account`].
/// If `path` is a file, tries to deserialize it as a list of [`Account`].
pub fn accounts_from_path(path: &Path) -> anyhow::Result<Vec<Account>> {
    if path.is_file() {
        let content = fs::read_to_string(path)?;
        let accounts: Vec<Account> = serde_json::from_str(&content)
            .with_context(|| format!("failed reading file {path:?} as a list of 'Account'"))?;
        return Ok(accounts);
    }

    // Otherwise, proceed with the original directory reading logic
    let mut accounts = vec![];
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let file_extension = path.extension();
        if file_extension.is_none() || file_extension.unwrap() != "json" {
            continue;
        }
        match Account::from_file(&path) {
            Ok(account) => accounts.push(account),
            Err(err) => tracing::debug!("{err}"),
        }
    }

    Ok(accounts)
}
