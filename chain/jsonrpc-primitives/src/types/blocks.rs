use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockReference {
    BlockId(near_primitives::types::BlockId),
    Finality(near_primitives::types::Finality),
    SyncCheckpoint(near_primitives::types::SyncCheckpoint),
}

#[derive(thiserror::Error, Debug, Serialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcBlockRequest {
    #[serde(flatten)]
    pub block_reference: BlockReference,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcBlockResponse {
    #[serde(flatten)]
    pub block_view: near_primitives::views::BlockView,
}

// near_client_primitives::types::GetBlock wants BlockReference from near_primitives
// that's why this impl exists
impl From<BlockReference> for near_primitives::types::BlockReference {
    fn from(block_reference: BlockReference) -> Self {
        match block_reference {
            BlockReference::BlockId(block_id) => Self::BlockId(block_id),
            BlockReference::Finality(finality) => Self::Finality(finality),
            BlockReference::SyncCheckpoint(sync_checkpoint) => {
                Self::SyncCheckpoint(sync_checkpoint)
            }
        }
    }
}

impl From<near_client_primitives::types::GetBlockError> for RpcBlockError {
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
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcBlockError"],
                );
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcBlockError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
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

impl RpcBlockRequest {
    pub fn parse(value: Option<Value>) -> Result<RpcBlockRequest, crate::errors::RpcParseError> {
        let block_reference = if let Ok((block_id,)) =
            crate::utils::parse_params::<(near_primitives::types::BlockId,)>(value.clone())
        {
            BlockReference::BlockId(block_id)
        } else {
            crate::utils::parse_params::<BlockReference>(value)?
        };
        Ok(RpcBlockRequest { block_reference })
    }
}
