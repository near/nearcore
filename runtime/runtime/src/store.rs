use near_primitives::account::{AccessKey, Account};
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
    // TODO: DATA
}
