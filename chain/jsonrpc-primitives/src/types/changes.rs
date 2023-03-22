#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcStateChangesInBlockRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcStateChangesInBlockResponse {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub changes: near_primitives::views::StateChangesView,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcStateChangesInBlockByTypeRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    #[serde(flatten)]
    pub state_changes_request: near_primitives::views::StateChangesRequestView,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcStateChangesInBlockByTypeResponse {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub changes: near_primitives::views::StateChangesKindsView,
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
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
