use near_primitives::hash::CryptoHash;
use near_primitives::rpc::{AccessKeyView, AccountView, PublicKeyView};
use near_primitives::types::AccountId;

/// Record in the state storage.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StateRecord {
    /// Account information.
    Account { account_id: AccountId, account: AccountView },
    /// Data records inside the contract, encoded in base64.
    Data { key: String, value: String },
    /// Contract code encoded in base64.
    Contract { account_id: AccountId, code: String },
    /// Access key associated with some account.
    AccessKey { account_id: AccountId, public_key: PublicKeyView, access_key: AccessKeyView },
    // TODO: DATA
}

impl StateRecord {
    pub fn account(account_id: &str, amount: u128, staked: u128) -> Self {
        StateRecord::Account {
            account_id: account_id.to_string(),
            account: AccountView {
                amount,
                staked,
                code_hash: CryptoHash::default().into(),
                storage_usage: 0,
                storage_paid_at: 0,
            },
        }
    }
}
