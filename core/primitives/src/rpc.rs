use std::collections::HashMap;
use std::convert::TryFrom;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::account::{AccessKey, Account};
use crate::crypto::signature::{PublicKey, SecretKey, Signature};
use crate::hash::CryptoHash;
use crate::serialize::{from_base, to_base, u128_dec_format, vec_base_format};
use crate::types::{AccountId, Balance, BlockIndex, MerkleHash, Nonce, StorageUsage, Version};

/// Number of nano seconds in one second.
//const NS_IN_SECOND: u64 = 1_000_000_000;

#[derive(Debug, Eq, PartialEq)]
pub struct PublicKeyView(Vec<u8>);

impl Serialize for PublicKeyView {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_base(&self.0))
    }
}

impl<'de> Deserialize<'de> for PublicKeyView {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_base(&s)
            .map(|v| PublicKeyView(v))
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl From<PublicKey> for PublicKeyView {
    fn from(public_key: PublicKey) -> Self {
        Self(public_key.0.as_ref().to_vec())
    }
}

#[derive(Debug)]
pub struct SecretKeyView(Vec<u8>);

impl Serialize for SecretKeyView {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_base(&self.0))
    }
}

impl<'de> Deserialize<'de> for SecretKeyView {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_base(&s)
            .map(|v| SecretKeyView(v))
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl From<SecretKey> for SecretKeyView {
    fn from(secret_key: SecretKey) -> Self {
        Self(secret_key.0[..].to_vec())
    }
}

#[derive(Debug)]
pub struct SignatureView(Vec<u8>);

impl Serialize for SignatureView {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_base(&self.0))
    }
}

impl<'de> Deserialize<'de> for SignatureView {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_base(&s)
            .map(|v| SignatureView(v))
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl From<Signature> for SignatureView {
    fn from(signature: Signature) -> Self {
        Self(signature.0.as_ref().to_vec())
    }
}

/// A view of the account
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct AccountView {
    pub public_keys: Vec<PublicKeyView>,
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
    fn from(mut account: Account) -> Self {
        AccountView {
            public_keys: account
                .public_keys
                .drain(..)
                .map(|public_key| public_key.into())
                .collect(),
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
    pub public_key: PublicKeyView,
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
