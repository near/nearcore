use std::collections::HashMap;
use std::convert::TryFrom;

use chrono::{DateTime, Utc};

use crate::account::AccessKey;
use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::serialize::{base_format, u128_dec_format, vec_base_format};
use crate::types::{AccountId, Balance, BlockIndex, MerkleHash, Nonce, Version};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct AccountViewCallResult {
    pub account_id: AccountId,
    pub nonce: Nonce,
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub stake: Balance,
    #[serde(with = "vec_base_format")]
    pub public_keys: Vec<PublicKey>,
    #[serde(with = "base_format")]
    pub code_hash: CryptoHash,
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
#[serde(untagged)]
pub enum QueryResponse {
    ViewAccount(AccountViewCallResult),
    ViewState(ViewStateResult),
    CallResult(CallResult),
    Error(QueryError),
    AccessKey(Option<AccessKey>),
    AccessKeyList(Vec<(PublicKey, AccessKey)>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusSyncInfo {
    #[serde(with = "base_format")]
    pub latest_block_hash: CryptoHash,
    pub latest_block_height: BlockIndex,
    #[serde(with = "base_format")]
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

impl TryFrom<QueryResponse> for AccountViewCallResult {
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

impl TryFrom<QueryResponse> for Option<AccessKey> {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response {
            QueryResponse::AccessKey(access_key) => Ok(access_key),
            _ => Err("Invalid type of response".into()),
        }
    }
}
