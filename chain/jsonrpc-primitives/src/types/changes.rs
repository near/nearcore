use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesInBlockRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesInBlockResponse {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub changes: near_primitives::views::StateChangesView,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesInBlockByTypeRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    #[serde(flatten)]
    pub state_changes_request: near_primitives::views::StateChangesRequestView,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesInBlockByTypeResponse {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub changes: near_primitives::views::StateChangesKindsView,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcStateChangesError {
    #[error("Block not found: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
    },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<near_client_primitives::types::GetBlockError> for RpcStateChangesError {
    fn from(error: near_client_primitives::types::GetBlockError) -> Self {
        match error {
            near_client_primitives::types::GetBlockError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetBlockError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetBlockError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetBlockError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStateChangesError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl From<near_client_primitives::types::GetStateChangesError> for RpcStateChangesError {
    fn from(error: near_client_primitives::types::GetStateChangesError) -> Self {
        match error {
            near_client_primitives::types::GetStateChangesError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetStateChangesError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetStateChangesError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetStateChangesError::Unreachable {
                ref error_message,
            } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStateChangesError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl From<RpcStateChangesError> for crate::errors::RpcError {
    fn from(error: RpcStateChangesError) -> Self {
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
