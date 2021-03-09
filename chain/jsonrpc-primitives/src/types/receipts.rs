use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptReference {
    pub receipt_id: near_primitives::hash::CryptoHash,
}

#[derive(Serialize, Deserialize)]
pub struct RpcReceiptRequest {
    #[serde(flatten)]
    pub receipt_reference: ReceiptReference,
}

#[derive(Serialize, Deserialize)]
pub struct RpcReceiptResponse {
    #[serde(flatten)]
    pub receipt_view: near_primitives::views::ReceiptView,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcReceiptError {
    #[error("The node reached its limits. Try again later. More details: {0}")]
    InternalError(String),
    #[error("Receipt with id {0} has never been observed on this node")]
    UnknownReceipt(near_primitives::hash::CryptoHash),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<ReceiptReference> for near_client_primitives::types::GetReceipt {
    fn from(receipt_reference: ReceiptReference) -> Self {
        Self { receipt_id: receipt_reference.receipt_id }
    }
}

impl RpcReceiptRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        let receipt_reference = crate::utils::parse_params::<ReceiptReference>(value)?;
        Ok(Self { receipt_reference })
    }
}

impl From<near_client_primitives::types::GetReceiptError> for RpcReceiptError {
    fn from(error: near_client_primitives::types::GetReceiptError) -> Self {
        match error {
            near_client_primitives::types::GetReceiptError::IOError(s) => Self::InternalError(s),
            near_client_primitives::types::GetReceiptError::UnknownReceipt(hash) => {
                Self::UnknownReceipt(hash)
            }
            near_client_primitives::types::GetReceiptError::Unreachable(error_message) => {
                error!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcReceiptError", &error_message],
                );
                Self::Unreachable(error_message)
            }
        }
    }
}

impl From<actix::MailboxError> for RpcReceiptError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError(error.to_string())
    }
}

impl From<RpcReceiptError> for crate::errors::RpcError {
    fn from(error: RpcReceiptError) -> Self {
        let error_data = Some(Value::String(error.to_string()));

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}
