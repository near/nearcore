use std::collections::HashMap;
use std::convert::TryFrom;

use serde::{Deserialize, Serialize};

use crate::account::AccessKey;
use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::serialize::{base_format, u128_hex_format};
use crate::types::{AccountId, Balance, Nonce};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct AccountViewCallResult {
    pub account_id: AccountId,
    pub nonce: Nonce,
    #[serde(with = "u128_hex_format")]
    pub amount: Balance,
    #[serde(with = "u128_hex_format")]
    pub stake: Balance,
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