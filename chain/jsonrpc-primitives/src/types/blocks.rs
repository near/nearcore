use serde_json::Value;

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcBlockError {
    #[error("Block not found: {error_message}")]
    UnknownBlock {
        // We are skipping this field for now
        // until we can provide useful struct like block_height or block_hash
        // that was requested
        #[serde(skip_serializing)]
        error_message: String,
    },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

#[derive(Debug, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary)]
pub struct RpcBlockRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcBlockResponse {
    #[serde(flatten)]
    pub block_view: near_primitives::views::BlockView,
}

impl From<RpcBlockError> for crate::errors::RpcError {
    fn from(error: RpcBlockError) -> Self {
        let error_data = match &error {
            RpcBlockError::UnknownBlock { error_message } => Some(Value::String(format!(
                "DB Not Found Error: {} \n Cause: Unknown",
                error_message
            ))),
            RpcBlockError::NotSyncedYet | RpcBlockError::InternalError { .. } => {
                Some(Value::String(error.to_string()))
            }
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcBlockError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}
