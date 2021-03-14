use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChunkReference {
    BlockShardId {
        block_id: near_primitives::types::BlockId,
        shard_id: near_primitives::types::ShardId,
    },
    ChunkHash {
        chunk_id: near_primitives::hash::CryptoHash,
    },
}

#[derive(Serialize, Deserialize)]
pub struct RpcChunkRequest {
    #[serde(flatten)]
    pub chunk_reference: ChunkReference,
}

#[derive(Serialize, Deserialize)]
pub struct RpcChunkResponse {
    #[serde(flatten)]
    pub chunk_view: near_primitives::views::ChunkView,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcChunkError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("Shard id {shard_id} does not exist")]
    InvalidShardId { shard_id: u64 },
    #[error("Chunk with hash {chunk_hash:?} has never been observed on this node")]
    UnknownChunk { chunk_hash: near_primitives::sharding::ChunkHash },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<ChunkReference> for near_client_primitives::types::GetChunk {
    fn from(chunk_reference: ChunkReference) -> Self {
        match chunk_reference {
            ChunkReference::BlockShardId { block_id, shard_id } => match block_id {
                near_primitives::types::BlockId::Height(height) => Self::Height(height, shard_id),
                near_primitives::types::BlockId::Hash(block_hash) => {
                    Self::BlockHash(block_hash.into(), shard_id)
                }
            },
            ChunkReference::ChunkHash { chunk_id } => Self::ChunkHash(chunk_id.into()),
        }
    }
}

impl RpcChunkRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        // Try to parse legacy positioned args and if it fails parse newer named args
        let chunk_reference = if let Ok((chunk_id,)) =
            crate::utils::parse_params::<(near_primitives::hash::CryptoHash,)>(value.clone())
        {
            ChunkReference::ChunkHash { chunk_id }
        } else if let Ok(((block_id, shard_id),)) = crate::utils::parse_params::<((
            near_primitives::types::BlockId,
            near_primitives::types::ShardId,
        ),)>(value.clone())
        {
            ChunkReference::BlockShardId { block_id, shard_id }
        } else {
            crate::utils::parse_params::<ChunkReference>(value)?
        };
        Ok(Self { chunk_reference })
    }
}

impl From<near_client_primitives::types::GetChunkError> for RpcChunkError {
    fn from(error: near_client_primitives::types::GetChunkError) -> Self {
        match error {
            near_client_primitives::types::GetChunkError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetChunkError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetChunkError::InvalidShardId { shard_id } => {
                Self::InvalidShardId { shard_id }
            }
            near_client_primitives::types::GetChunkError::UnknownChunk { chunk_hash } => {
                Self::UnknownChunk { chunk_hash }
            }
            near_client_primitives::types::GetChunkError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcChunkError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcChunkError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcChunkError> for crate::errors::RpcError {
    fn from(error: RpcChunkError) -> Self {
        let error_data = match error {
            RpcChunkError::InternalError { .. } => Some(Value::String(error.to_string())),
            RpcChunkError::UnknownBlock { error_message } => Some(Value::String(format!(
                "DB Not Found Error: {} \n Cause: Unknown",
                error_message
            ))),
            RpcChunkError::InvalidShardId { .. } => Some(Value::String(error.to_string())),
            RpcChunkError::UnknownChunk { chunk_hash } => Some(Value::String(format!(
                "Chunk Missing (unavailable on the node): ChunkHash(`{}`) \n Cause: Unknown",
                chunk_hash.0.to_string()
            ))),
            RpcChunkError::Unreachable { error_message } => Some(Value::String(error_message)),
        };

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}
