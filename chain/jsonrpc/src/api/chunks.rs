use serde_json::Value;

use near_client_primitives::types::{GetChunk, GetChunkError};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::chunks::{ChunkReference, RpcChunkError, RpcChunkRequest};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockId, ShardId};

use super::{parse_params, RpcFrom, RpcRequest};

impl RpcRequest for RpcChunkRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        // Try to parse legacy positioned args and if it fails parse newer named args
        let chunk_reference = if let Ok((chunk_id,)) = parse_params::<(CryptoHash,)>(value.clone())
        {
            ChunkReference::ChunkHash { chunk_id }
        } else if let Ok(((block_id, shard_id),)) =
            parse_params::<((BlockId, ShardId),)>(value.clone())
        {
            ChunkReference::BlockShardId { block_id, shard_id }
        } else {
            parse_params::<ChunkReference>(value)?
        };
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
