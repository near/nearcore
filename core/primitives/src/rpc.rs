use std::collections::HashMap;
use std::convert::TryFrom;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::account::{AccessKey, Account};
use crate::block::{Block, BlockHeader, BlockHeaderInner};
use crate::crypto::signature::{PublicKey, SecretKey, Signature};
use crate::hash::CryptoHash;
use crate::serialize::{from_base, to_base, u128_dec_format};
use crate::transaction::SignedTransaction;
use crate::types::{AccountId, Balance, BlockIndex, Nonce, StorageUsage, ValidatorStake, Version};

#[derive(Debug, Eq, PartialEq, Clone)]
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

impl From<PublicKeyView> for PublicKey {
    fn from(view: PublicKeyView) -> Self {
        Self::try_from(view.0).expect("Failed to get PublicKey from PublicKeyView")
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

#[derive(Debug, Clone)]
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

impl From<SignatureView> for Signature {
    fn from(view: SignatureView) -> Self {
        Signature::try_from(view.0).expect("Failed to get Signature from SignatureView")
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CryptoHashView(Vec<u8>);

impl Serialize for CryptoHashView {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_base(&self.0))
    }
}

impl<'de> Deserialize<'de> for CryptoHashView {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_base(&s)
            .map(|v| CryptoHashView(v))
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl From<CryptoHash> for CryptoHashView {
    fn from(hash: CryptoHash) -> Self {
        CryptoHashView(hash.as_ref().to_vec())
    }
}

impl From<CryptoHashView> for CryptoHash {
    fn from(view: CryptoHashView) -> Self {
        CryptoHash::try_from(view.0).expect("Failed to convert CryptoHashView to CryptoHash")
    }
}

/// A view of the account
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct AccountView {
    pub public_keys: Vec<PublicKeyView>,
    pub nonce: Nonce,
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub staked: Balance,
    pub code_hash: CryptoHashView,
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
            code_hash: account.code_hash.into(),
            storage_usage: account.storage_usage,
            storage_paid_at: account.storage_paid_at,
        }
    }
}

impl From<AccountView> for Account {
    fn from(mut view: AccountView) -> Self {
        Self {
            public_keys: view.public_keys.drain(..).map(|public_key| public_key.into()).collect(),
            nonce: view.nonce,
            amount: view.amount,
            staked: view.staked,
            code_hash: view.code_hash.into(),
            storage_usage: view.storage_usage,
            storage_paid_at: view.storage_paid_at,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
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

impl From<AccessKeyView> for AccessKey {
    fn from(view: AccessKeyView) -> Self {
        Self {
            amount: view.amount,
            balance_owner: view.balance_owner,
            contract_id: view.contract_id,
            method_name: view.method_name,
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
    pub latest_block_hash: CryptoHashView,
    pub latest_block_height: BlockIndex,
    pub latest_state_root: CryptoHashView,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockHeaderView {
    pub height: BlockIndex,
    pub epoch_hash: CryptoHash,
    pub prev_hash: CryptoHashView,
    pub prev_state_root: CryptoHashView,
    pub tx_root: CryptoHashView,
    pub timestamp: u64,
    pub approval_mask: Vec<bool>,
    pub approval_sigs: Vec<SignatureView>,
    pub total_weight: u64,
    pub validator_proposals: Vec<ValidatorStake>,
    pub signature: SignatureView,
}

impl From<BlockHeader> for BlockHeaderView {
    fn from(mut header: BlockHeader) -> Self {
        Self {
            height: header.inner.height,
            epoch_hash: header.inner.epoch_hash,
            prev_hash: header.inner.prev_hash.into(),
            prev_state_root: header.inner.prev_state_root.into(),
            tx_root: header.inner.tx_root.into(),
            timestamp: header.inner.timestamp,
            approval_mask: header.inner.approval_mask,
            approval_sigs: header
                .inner
                .approval_sigs
                .drain(..)
                .map(|signature| signature.into())
                .collect(),
            total_weight: header.inner.total_weight.to_num(),
            validator_proposals: header.inner.validator_proposals,
            signature: header.signature.into(),
        }
    }
}

impl From<BlockHeaderView> for BlockHeader {
    fn from(mut view: BlockHeaderView) -> Self {
        let mut header = Self {
            inner: BlockHeaderInner {
                height: view.height,
                epoch_hash: view.epoch_hash.into(),
                prev_hash: view.prev_hash.into(),
                prev_state_root: view.prev_state_root.into(),
                tx_root: view.tx_root.into(),
                timestamp: view.timestamp,
                approval_mask: view.approval_mask,
                approval_sigs: view
                    .approval_sigs
                    .drain(..)
                    .map(|signature| signature.into())
                    .collect(),
                total_weight: view.total_weight.into(),
                validator_proposals: view.validator_proposals,
            },
            signature: view.signature.into(),
            hash: CryptoHash::default(),
        };
        header.init();
        header
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockView {
    pub header: BlockHeaderView,
    pub transactions: Vec<SignedTransactionView>,
}

impl From<Block> for BlockView {
    fn from(mut block: Block) -> Self {
        BlockView {
            header: block.header.into(),
            transactions: block.transactions.drain(..).map(|tx| tx.into()).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SignedTransactionView {}

impl From<SignedTransaction> for SignedTransactionView {
    fn from(signed_tx: SignedTransaction) -> Self {
        // MOO
        SignedTransactionView {}
    }
}
