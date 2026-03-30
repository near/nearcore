use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;

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
