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

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcChunkRequest {
    #[serde(flatten)]
    pub chunk_reference: ChunkReference,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcChunkResponse {
    #[serde(flatten)]
    pub chunk_view: near_primitives::views::ChunkView,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcChunkError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
    },
    #[error("Shard id {shard_id} does not exist")]
    InvalidShardId { shard_id: u64 },
    #[error("Chunk with hash {chunk_hash:?} has never been observed on this node")]
    UnknownChunk { chunk_hash: near_primitives::sharding::ChunkHash },
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
            near_client_primitives::types::GetChunkError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcChunkError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
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
        let error_data = match &error {
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
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcStateChangesError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}
