use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockReference {
    BlockId(near_primitives::types::BlockId),
    Finality(near_primitives::types::Finality),
    SyncCheckpoint(near_primitives::types::SyncCheckpoint),
}

#[derive(thiserror::Error, Debug)]
pub enum RpcBlockError {
    #[error("Block not found: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
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
            near_client_primitives::types::GetBlockError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcBlockError"],
                );
                Self::Unreachable { error_message }
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
        let error_data = match error {
            RpcBlockError::UnknownBlock { error_message } => Some(Value::String(format!(
                "DB Not Found Error: {} \n Cause: Unknown",
                error_message
            ))),
            RpcBlockError::Unreachable { error_message } => Some(Value::String(error_message)),
            RpcBlockError::NotSyncedYet | RpcBlockError::InternalError { .. } => {
                Some(Value::String(error.to_string()))
            }
        };

        Self::new(-32_000, "Server error".to_string(), error_data)
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
