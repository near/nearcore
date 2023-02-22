use serde_json::Value;

use near_client_primitives::types::{GetChunk, GetChunkError};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::chunks::{ChunkReference, RpcChunkError, RpcChunkRequest};
use near_primitives::types::BlockId;

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcChunkRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        // params can be:
        // - chunk_reference         (an object),
        // - [[block_id, shard_id]]  (a one-element array with array element) or
        // - [chunk_id]              (a one-element array with hash element).
        let chunk_reference = Params::new(value)
            .try_singleton(|value: Value| {
                if value.is_array() {
                    let (block_id, shard_id) = Params::parse(value)?;
                    Ok(ChunkReference::BlockShardId { block_id, shard_id })
                } else {
                    let chunk_id = Params::parse(value)?;
                    Ok(ChunkReference::ChunkHash { chunk_id })
                }
            })
            .unwrap_or_parse()?;
        Ok(Self { chunk_reference })
    }
}

impl RpcFrom<actix::MailboxError> for RpcChunkError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<ChunkReference> for GetChunk {
    fn rpc_from(chunk_reference: ChunkReference) -> Self {
        match chunk_reference {
            ChunkReference::BlockShardId { block_id, shard_id } => match block_id {
                BlockId::Height(height) => Self::Height(height, shard_id),
                BlockId::Hash(block_hash) => Self::BlockHash(block_hash, shard_id),
            },
            ChunkReference::ChunkHash { chunk_id } => Self::ChunkHash(chunk_id.into()),
        }
    }
}

impl RpcFrom<GetChunkError> for RpcChunkError {
    fn rpc_from(error: GetChunkError) -> Self {
        match error {
            GetChunkError::IOError { error_message } => Self::InternalError { error_message },
            GetChunkError::UnknownBlock { error_message } => Self::UnknownBlock { error_message },
            GetChunkError::InvalidShardId { shard_id } => Self::InvalidShardId { shard_id },
            GetChunkError::UnknownChunk { chunk_hash } => Self::UnknownChunk { chunk_hash },
            GetChunkError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcChunkError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
