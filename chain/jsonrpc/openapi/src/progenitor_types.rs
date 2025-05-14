use near_jsonrpc_primitives::errors::RpcRequestValidationErrorKind;
/// This module is used when generating the OpenAPI spec version which is compatible with [progenitor](https://github.com/oxidecomputer/progenitor/) tool. So that the types are generated correctly.
use near_primitives::hash::CryptoHash;

use serde_with::{base64::Base64, serde_as};

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RpcStateChangesInBlockResponse {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub changes: Vec<StateChangeWithCauseView>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct StateChangeWithCauseView {
    pub cause: near_primitives::views::StateChangeCauseView,
    #[serde(rename = "type")]
    pub value: StateChangeValueViewType,
    pub change: StateChangeValueViewContent,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StateChangeValueViewType {
    AccountUpdate,
    AccountDeletion,
    AccessKeyUpdate,
    DataUpdate,
    DataDeletion,
    ContractCodeUpdate,
    ContractCodeDeletion,
}

#[serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum StateChangeValueViewContent {
    AccountUpdate {
        account_id: near_primitives::types::AccountId,
        #[serde(flatten)]
        account: near_primitives::views::AccountView,
    },
    AccountDeletion {
        account_id: near_primitives::types::AccountId,
    },
    AccessKeyUpdate {
        account_id: near_primitives::types::AccountId,
        public_key: near_crypto::PublicKey,
        access_key: near_primitives::views::AccessKeyView,
    },
    AccessKeyDeletion {
        account_id: near_primitives::types::AccountId,
        public_key: near_crypto::PublicKey,
    },
    DataUpdate {
        account_id: near_primitives::types::AccountId,
        #[serde(rename = "key_base64")]
        key: near_primitives::types::StoreKey,
        #[serde(rename = "value_base64")]
        value: near_primitives::types::StoreValue,
    },
    DataDeletion {
        account_id: near_primitives::types::AccountId,
        #[serde(rename = "key_base64")]
        key: near_primitives::types::StoreKey,
    },
    ContractCodeUpdate {
        account_id: near_primitives::types::AccountId,
        #[serde(rename = "code_base64")]
        #[serde_as(as = "Base64")]
        #[schemars(with = "String")]
        code: Vec<u8>,
    },
    ContractCodeDeletion {
        account_id: near_primitives::types::AccountId,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, schemars::JsonSchema)]
#[serde(untagged)]
pub enum CauseRpcErrorKind {
    RequestValidationError(RpcRequestValidationErrorKind),
    HandlerError(serde_json::Value),
    InternalError(serde_json::Value),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NameRpcErrorKind {
    RequestValidationError,
    HandlerError,
    InternalError,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TypeTransactionOrReceiptId {
    Transaction,
    Receipt,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, schemars::JsonSchema)]
#[serde(untagged)]
pub enum TransactionOrReceiptId {
    Transaction { transaction_hash: CryptoHash, sender_id: near_primitives::types::AccountId },
    Receipt { receipt_id: CryptoHash, receiver_id: near_primitives::types::AccountId },
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RpcLightClientExecutionProofRequest {
    #[serde(flatten)]
    pub id: TransactionOrReceiptId,
    #[serde(rename = "type")]
    pub type_transaction_or_receipt_id: TypeTransactionOrReceiptId,
    pub light_client_head: near_primitives::hash::CryptoHash,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct RpcError {
    pub name: Option<NameRpcErrorKind>,
    pub cause: Option<CauseRpcErrorKind>,
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}
