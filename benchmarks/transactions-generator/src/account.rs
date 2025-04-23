use anyhow::Context;
use near_crypto::{InMemorySigner, SecretKey, Signer};
use near_primitives::types::AccountId;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::atomic::AtomicU64;

#[derive(Serialize, Deserialize, Debug)]
pub struct Account {
    #[serde(flatten)]
    pub signer: InMemorySigner,
    #[serde(skip)]
    pub nonce: AtomicU64,
}

impl Account {
    pub fn new(account_id: AccountId, secret_key: SecretKey, nonce: u64) -> Self {
        Self { 
            signer: InMemorySigner{
                account_id, 
                public_key: secret_key.public_key(), 
                secret_key,
            },
            nonce: nonce.into(),
        }
    }

    pub fn from_file(path: &Path) -> anyhow::Result<Account> {
        Ok (Self {
            signer: InMemorySigner::from_file(path)?,
            nonce: 0.into(),
        })
    }

    pub fn write_to_dir(&self, dir: &Path) -> anyhow::Result<()> {
        if !dir.exists() {
            std::fs::create_dir(dir)?;
        }

        let json = serde_json::to_string(self)?;
        let mut file_name = self.signer.account_id.to_string();
        file_name.push_str(".json");
        let file_path = dir.join(file_name);
        fs::write(file_path, json)?;
        Ok(())
    }

    pub fn as_signer(&self) -> Signer {
        Signer::from(self.signer.clone())
    }
}

/// Tries to deserialize all json files in `path` as [`Account`].
/// If `path` is a file, tries to deserialize it as a list of [`Account`].
pub fn accounts_from_path(path: &Path) -> anyhow::Result<Vec<Account>> {
    if path.is_file() {
        let content = fs::read_to_string(path)?;
        let accounts: Vec<Account> = serde_json::from_str(&content)
            .with_context(|| format!("failed reading file {path:?} as a list of 'Account'"))?;
        Ok(accounts)
    } else if path.is_dir() {
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
    } else {
        Ok(vec![])
    }
}
