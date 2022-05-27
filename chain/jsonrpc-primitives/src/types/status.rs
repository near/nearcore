use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStatusResponse {
    #[serde(flatten)]
    pub status_response: near_primitives::views::StatusResponse,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcHealthResponse;

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcStatusError {
    #[error("Node is syncing")]
    NodeIsSyncing,
    #[error("No blocks for {elapsed:?}")]
    NoNewBlocks { elapsed: std::time::Duration },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
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
            near_client_primitives::types::StatusError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStatusError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl From<RpcStatusError> for crate::errors::RpcError {
    fn from(error: RpcStatusError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcStateChangesError: {:?}", err),
                )
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}
