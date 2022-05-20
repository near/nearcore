use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptReference {
    pub receipt_id: near_primitives::hash::CryptoHash,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcReceiptRequest {
    #[serde(flatten)]
    pub receipt_reference: ReceiptReference,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcReceiptResponse {
    #[serde(flatten)]
    pub receipt_view: near_primitives::views::ReceiptView,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcReceiptError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    #[error("Receipt with id {receipt_id} has never been observed on this node")]
    UnknownReceipt { receipt_id: near_primitives::hash::CryptoHash },
}

impl From<ReceiptReference> for near_client_primitives::types::GetReceipt {
    fn from(receipt_reference: ReceiptReference) -> Self {
        Self { receipt_id: receipt_reference.receipt_id }
    }
}

#[cfg(feature = "server")]
impl crate::RpcRequest for RpcReceiptRequest {
    fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        let receipt_reference = crate::utils::parse_params::<ReceiptReference>(value)?;
        Ok(Self { receipt_reference })
    }
}

impl From<near_client_primitives::types::GetReceiptError> for RpcReceiptError {
    fn from(error: near_client_primitives::types::GetReceiptError) -> Self {
        match error {
            near_client_primitives::types::GetReceiptError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetReceiptError::UnknownReceipt(hash) => {
                Self::UnknownReceipt { receipt_id: hash }
            }
            near_client_primitives::types::GetReceiptError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcReceiptError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcReceiptError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcReceiptError> for crate::errors::RpcError {
    fn from(error: RpcReceiptError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcReceiptError: {:?}", err),
                )
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}
