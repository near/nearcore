use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo};
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, ShardId};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ReceiptReference {
    pub receipt_id: CryptoHash,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptRequest {
    #[serde(flatten)]
    pub receipt_reference: ReceiptReference,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptResponse {
    #[serde(flatten)]
    pub receipt_view: near_primitives::views::ReceiptView,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcReceiptError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    #[error("Receipt with id {receipt_id} has never been observed on this node")]
    UnknownReceipt { receipt_id: CryptoHash },
}

impl From<RpcReceiptError> for crate::errors::RpcError {
    fn from(error: RpcReceiptError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcReceiptError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptToTxRequest {
    #[serde(flatten)]
    pub receipt_reference: ReceiptReference,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptToTxResponse {
    pub transaction_hash: CryptoHash,
    pub sender_account_id: AccountId,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcReceiptToTxError {
    #[error("Receipt with id {receipt_id} has never been observed on this node")]
    UnknownReceipt { receipt_id: CryptoHash },
    #[error("depth limit {limit} exceeded when resolving receipt {receipt_id}")]
    DepthExceeded { receipt_id: CryptoHash, limit: u32 },
    #[error("this node does not support receipt-to-tx lookup: {error_message}")]
    Unsupported { error_message: String },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcReceiptToTxError> for crate::errors::RpcError {
    fn from(error: RpcReceiptToTxError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcReceiptToTxError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptParentByHintRequest {
    pub receipt_id: CryptoHash,
    pub block_height: BlockHeight,
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<BlockHeightDelta>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReceiptOriginView {
    FromTransaction { tx_hash: CryptoHash, sender_account_id: AccountId },
    FromReceipt { parent_receipt_id: CryptoHash, parent_predecessor_id: AccountId },
}

impl From<&ReceiptOrigin> for ReceiptOriginView {
    fn from(origin: &ReceiptOrigin) -> Self {
        match origin {
            ReceiptOrigin::FromTransaction(o) => Self::FromTransaction {
                tx_hash: o.tx_hash,
                sender_account_id: o.sender_account_id.clone(),
            },
            ReceiptOrigin::FromReceipt(o) => Self::FromReceipt {
                parent_receipt_id: o.parent_receipt_id,
                parent_predecessor_id: o.parent_predecessor_id.clone(),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptParentByHintResponse {
    pub origin: ReceiptOriginView,
    pub receiver_account_id: AccountId,
    pub shard_id: ShardId,
}

impl RpcReceiptParentByHintResponse {
    pub fn from_info(info: ReceiptToTxInfo) -> Self {
        let ReceiptToTxInfo::V1(v1) = info;
        Self {
            origin: ReceiptOriginView::from(&v1.origin),
            receiver_account_id: v1.receiver_account_id,
            shard_id: v1.shard_id,
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcReceiptParentByHintError {
    #[error(
        "receipt {receipt_id} not found in hint window of ±{effective_window} around height {block_height} on shard {shard_id}"
    )]
    ReceiptNotFoundInHintWindow {
        receipt_id: CryptoHash,
        block_height: BlockHeight,
        shard_id: ShardId,
        effective_window: BlockHeightDelta,
    },
    #[error("execution outcomes are not stored on this node (save_tx_outcomes=false)")]
    OutcomesNotStored,
    #[error("this node does not track shard {shard_id}")]
    ShardNotTracked { shard_id: ShardId },
    #[error("requested window {requested} exceeds maximum {maximum}")]
    WindowTooLarge { requested: BlockHeightDelta, maximum: BlockHeightDelta },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcReceiptParentByHintError> for crate::errors::RpcError {
    fn from(error: RpcReceiptParentByHintError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcReceiptParentByHintError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}
