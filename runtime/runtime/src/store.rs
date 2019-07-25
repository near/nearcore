use std::convert::TryFrom;

use near_primitives::account::{AccessKey, Account};
use near_primitives::crypto::signature::PublicKey;
use near_primitives::transaction::Callback;
use near_primitives::types::{AccountId, ReadablePublicKey};

/// Record in the state storage.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StateRecord {
    /// Account information.
    Account { account_id: AccountId, account: Account },
    /// Data records inside the contract, encoded in base64.
    Data { key: String, value: String },
    /// Contract code encoded in base64.
    Contract { account_id: AccountId, code: String },
    /// Access key associated with some account.
    AccessKey { account_id: AccountId, public_key: ReadablePublicKey, access_key: AccessKey },
    /// Callback.
    Callback { id: Vec<u8>, callback: Callback },
}

impl StateRecord {
    pub fn account(account_id: &str, public_key: &str, amount: u128, staked: u128) -> Self {
        StateRecord::Account {
            account_id: account_id.to_string(),
            account: Account {
                public_keys: vec![PublicKey::try_from(public_key).unwrap()],
                nonce: 0,
                amount,
                staked,
                code_hash: Default::default(),
                storage_usage: 0,
                storage_paid_at: 0,
            },
        }
    }
}
