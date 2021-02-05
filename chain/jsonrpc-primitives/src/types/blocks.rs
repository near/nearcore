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
    #[error("Block `{0}` is missing")]
    BlockMissing(near_primitives::hash::CryptoHash),
    #[error("Block not found")]
    BlockNotFound(String),
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    #[error("The node reached its limits. Try again later. More details: {0}")]
    InternalError(String),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

#[derive(Serialize, Deserialize)]
pub struct RpcBlockRequest {
    #[serde(flatten)]
    pub block_reference: BlockReference,
}

#[derive(Serialize, Deserialize)]
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
            near_client_primitives::types::GetBlockError::BlockMissing(block_hash) => {
                Self::BlockMissing(block_hash)
            }
            near_client_primitives::types::GetBlockError::BlockNotFound(s) => {
                Self::BlockNotFound(s)
            }
            near_client_primitives::types::GetBlockError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetBlockError::IOError(s) => Self::InternalError(s),
            near_client_primitives::types::GetBlockError::Unreachable(s) => {
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcBlockError", &s],
                );
                Self::Unreachable(s)
            }
        }
    }
}

impl From<actix::MailboxError> for RpcBlockError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError(error.to_string())
    }
}

impl From<RpcBlockError> for crate::errors::RpcError {
    fn from(error: RpcBlockError) -> Self {
        let error_data = match error {
            RpcBlockError::BlockMissing(hash) => Some(Value::String(format!(
                "Block Missing (unavailable on the node): {} \n Cause: Unknown",
                hash.to_string()
            ))),
            RpcBlockError::BlockNotFound(s) => {
                Some(Value::String(format!("DB Not Found Error: {} \n Cause: Unknown", s)))
            }
            RpcBlockError::Unreachable(s) => Some(Value::String(s)),
            RpcBlockError::NotSyncedYet | RpcBlockError::InternalError(_) => {
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
