use serde::{Deserialize, Serialize};

use near_crypto::PublicKey;

use crate::hash::CryptoHash;
use crate::serialize::option_base64_format;
use crate::types::AccountId;
use crate::views::{AccessKeyView, AccountView, ReceiptView};

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
    AccessKey { account_id: AccountId, public_key: PublicKey, access_key: AccessKeyView },
    /// Postponed Action Receipt.
    PostponedReceipt(Box<ReceiptView>),
    /// Received data from DataReceipt encoded in base64 for the given account_id and data_id.
    ReceivedData {
        account_id: AccountId,
        data_id: CryptoHash,
        #[serde(with = "option_base64_format")]
        data: Option<Vec<u8>>,
    },
}
