use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChunkReference {
    BlockShardId(near_primitives::types::BlockId, near_primitives::types::ShardId),
    BlockIdShardIdStruct(BlockShardId),
    Hash(near_primitives::hash::CryptoHash),
    HashStruct(ChunkId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockShardId {
    pub block_id: near_primitives::types::BlockId,
    pub shard_id: near_primitives::types::ShardId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkId {
    pub chunk_id: near_primitives::hash::CryptoHash,
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
    #[error("The node reached its limits. Try again later. More details: {0}")]
    InternalError(String),
    #[error("Block not found")]
    UnknownBlock(String),
    #[error("Block `{0}` is unavailable on the node")]
    UnavailableBlock(near_primitives::hash::CryptoHash),
    #[error("Shard id {0} does not exist")]
    InvalidShardId(u64),
    #[error("Chunk {0:?} is missing")]
    UnknownChunk(near_primitives::sharding::ChunkHash),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<ChunkReference> for near_client_primitives::types::GetChunk {
    fn from(chunk_reference: ChunkReference) -> Self {
        match chunk_reference {
            ChunkReference::BlockShardId(block_id, shard_id) => match block_id {
                near_primitives::types::BlockId::Height(height) => Self::Height(height, shard_id),
                near_primitives::types::BlockId::Hash(block_hash) => {
                    Self::BlockHash(block_hash.into(), shard_id)
                }
            },
            ChunkReference::Hash(chunk_hash) => Self::ChunkHash(chunk_hash.into()),
            ChunkReference::BlockIdShardIdStruct(block_shard_struct) => {
                match block_shard_struct.block_id {
                    near_primitives::types::BlockId::Height(height) => {
                        Self::Height(height, block_shard_struct.shard_id)
                    }
                    near_primitives::types::BlockId::Hash(block_hash) => {
                        Self::BlockHash(block_hash.into(), block_shard_struct.shard_id)
                    }
                }
            }
            ChunkReference::HashStruct(chunk_id) => Self::ChunkHash(chunk_id.chunk_id.into()),
        }
    }
}

impl RpcChunkRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        let chunk_reference = if let Ok((chunk_id,)) =
            crate::utils::parse_params::<(ChunkReference,)>(value.clone())
        {
            chunk_id
        } else {
            crate::utils::parse_params::<ChunkReference>(value)?
        };
        Ok(Self { chunk_reference })
    }
}

impl From<near_primitives::views::ChunkView> for RpcChunkResponse {
    fn from(chunk_view: near_primitives::views::ChunkView) -> Self {
        Self { chunk_view }
    }
}

impl From<near_client_primitives::types::GetChunkError> for RpcChunkError {
    fn from(error: near_client_primitives::types::GetChunkError) -> Self {
        match error {
            near_client_primitives::types::GetChunkError::IOError(s) => Self::InternalError(s),
            near_client_primitives::types::GetChunkError::UnknownBlock(s) => Self::UnknownBlock(s),
            near_client_primitives::types::GetChunkError::UnavailableBlock(hash) => {
                Self::UnavailableBlock(hash)
            }
            near_client_primitives::types::GetChunkError::InvalidShardId(shard_id) => {
                Self::InvalidShardId(shard_id)
            }
            near_client_primitives::types::GetChunkError::UnknownChunk(hash) => {
                Self::UnknownChunk(hash)
            }
            near_client_primitives::types::GetChunkError::Unreachable(s) => {
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcChunkError", &s],
                );
                Self::Unreachable(s)
            }
        }
    }
}

impl From<actix::MailboxError> for RpcChunkError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError(error.to_string())
    }
}

impl From<RpcChunkError> for crate::errors::RpcError {
    fn from(error: RpcChunkError) -> Self {
        let error_data = match error {
            RpcChunkError::InternalError(_) => Some(Value::String(error.to_string())),
            RpcChunkError::UnavailableBlock(hash) => Some(Value::String(format!(
                "DB Not Found Error: {} \n Cause: Unknown",
                hash.to_string()
            ))),
            RpcChunkError::UnknownBlock(s) => {
                Some(Value::String(format!("DB Not Found Error: {} \n Cause: Unknown", s)))
            }
            RpcChunkError::InvalidShardId(_) => Some(Value::String(error.to_string())),
            RpcChunkError::UnknownChunk(hash) => Some(Value::String(format!(
                "Chunk Missing (unavailable on the node): ChunkHash(`{}`) \n Cause: Unknown",
                hash.0.to_string()
            ))),
            RpcChunkError::Unreachable(s) => Some(Value::String(s)),
        };

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}
