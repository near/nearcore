use std::collections::HashMap;
use std::convert::TryFrom;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::account::{AccessKey, Account};
use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::serialize::{u128_dec_format, vec_base_format};
use crate::types::{AccountId, Balance, BlockIndex, MerkleHash, Nonce, StorageUsage, Version};

/// A view of the account
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct AccountView {
    #[serde(with = "vec_base_format")]
    pub public_keys: Vec<PublicKey>,
    pub nonce: Nonce,
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub staked: Balance,
    pub code_hash: CryptoHash,
    pub storage_usage: StorageUsage,
    pub storage_paid_at: BlockIndex,
}

impl From<Account> for AccountView {
    fn from(account: Account) -> Self {
        AccountView {
            public_keys: account.public_keys,
            nonce: account.nonce,
            amount: account.amount,
            staked: account.staked,
            code_hash: account.code_hash,
            storage_usage: account.storage_usage,
            storage_paid_at: account.storage_paid_at,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct AccessKeyView {
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    pub balance_owner: Option<AccountId>,
    pub contract_id: Option<AccountId>,
    pub method_name: Option<Vec<u8>>,
}

impl From<AccessKey> for AccessKeyView {
    fn from(access_key: AccessKey) -> Self {
        Self {
            amount: access_key.amount,
            balance_owner: access_key.balance_owner,
            contract_id: access_key.contract_id,
            method_name: access_key.method_name,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewStateResult {
    pub values: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CallResult {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryError {
    pub error: String,
    pub logs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccessKeyInfo {
    pub public_key: PublicKey,
    pub access_key: AccessKeyView,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum QueryResponse {
    ViewAccount(AccountView),
    ViewState(ViewStateResult),
    CallResult(CallResult),
    Error(QueryError),
    AccessKey(Option<AccessKeyView>),
    AccessKeyList(Vec<AccessKeyInfo>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusSyncInfo {
    pub latest_block_hash: CryptoHash,
    pub latest_block_height: BlockIndex,
    pub latest_state_root: MerkleHash,
    pub latest_block_time: DateTime<Utc>,
    pub syncing: bool,
}

// TODO: add more information to ValidatorInfo
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ValidatorInfo {
    pub account_id: AccountId,
    pub is_slashed: bool,
}

// TODO: add more information to status.
#[derive(Serialize, Deserialize, Debug)]
pub struct StatusResponse {
    /// Binary version.
    pub version: Version,
    /// Unique chain id.
    pub chain_id: String,
    /// Address for RPC server.
    pub rpc_addr: String,
    /// Current epoch validators.
    pub validators: Vec<ValidatorInfo>,
    /// Sync status of the node.
    pub sync_info: StatusSyncInfo,
}

impl TryFrom<QueryResponse> for AccountView {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response {
            QueryResponse::ViewAccount(acc) => Ok(acc),
            _ => Err("Invalid type of response".into()),
        }
    }
}

impl TryFrom<QueryResponse> for ViewStateResult {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response {
            QueryResponse::ViewState(vs) => Ok(vs),
            _ => Err("Invalid type of response".into()),
        }
    }
}

impl TryFrom<QueryResponse> for Option<AccessKeyView> {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response {
            QueryResponse::AccessKey(access_key) => Ok(access_key),
            _ => Err("Invalid type of response".into()),
        }
    }
}
