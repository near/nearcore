use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStatusResponse {
    #[serde(flatten)]
    pub status_response: near_primitives::views::StatusResponse,
}

#[derive(Debug, Serialize)]
pub struct RpcHealthResponse;

#[derive(thiserror::Error, Debug)]
pub enum RpcStatusError {
    #[error("Node is syncing")]
    NodeIsSyncing,
    #[error("No blocks for {elapsed:?}")]
    NoNewBlocks { elapsed: std::time::Duration },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_primitives::views::StatusResponse> for RpcStatusResponse {
    fn from(status_response: near_primitives::views::StatusResponse) -> Self {
        Self { status_response }
    }
}

impl From<near_primitives::views::StatusResponse> for RpcHealthResponse {
    fn from(_status_response: near_primitives::views::StatusResponse) -> Self {
        Self {}
    }
}

impl From<near_client_primitives::types::StatusError> for RpcStatusError {
    fn from(error: near_client_primitives::types::StatusError) -> Self {
        match error {
            near_client_primitives::types::StatusError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::StatusError::NodeIsSyncing => Self::NodeIsSyncing,
            near_client_primitives::types::StatusError::NoNewBlocks { elapsed } => {
                Self::NoNewBlocks { elapsed }
            }
            near_client_primitives::types::StatusError::EpochOutOfBounds { epoch_id } => {
                Self::EpochOutOfBounds { epoch_id }
            }
            near_client_primitives::types::StatusError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcStatusError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcStatusError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcStatusError> for crate::errors::RpcError {
    fn from(error: RpcStatusError) -> Self {
        let error_data = Some(Value::String(error.to_string()));

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}
