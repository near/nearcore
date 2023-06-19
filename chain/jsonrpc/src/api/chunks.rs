use serde_json::Value;

use near_client_primitives::types::{GetChunk, GetChunkError};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::chunks::{RpcChunkError, RpcChunkRequest};
use near_primitives::types::ChunkReference;

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcChunkRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        // params can be:
        // - chunk_reference         (an object),
        // - [[block_id, shard_ref]] (a one-element array with array element) or
        // - [chunk_id]              (a one-element array with hash element).
        let chunk_reference =
            Params::new(value).try_singleton(parse_chunk_reference).unwrap_or_parse()?;
        Ok(Self { chunk_reference })
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ShardReference {
    ShardId(near_primitives::types::ShardId),
    AccountId(near_primitives::types::AccountId),
}

fn parse_chunk_reference(value: Value) -> Result<ChunkReference, RpcParseError> {
    // value can be:
    // - [block_id, shard_ref] (a two-element array) or
    // - chunk_id              (a hash value).

    if value.is_array() {
        let (block_id, shard_ref) = Params::parse(value)?;
        Ok(match shard_ref {
            ShardReference::ShardId(shard_id) => {
                ChunkReference::BlockShardId { block_id, shard_id }
            }
            ShardReference::AccountId(account_id) => {
                ChunkReference::BlockAccountId { block_id, account_id }
            }
        })
    } else {
        Ok(ChunkReference::ChunkHash { chunk_id: Params::parse(value)? })
    }
}

impl RpcFrom<actix::MailboxError> for RpcChunkError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<ChunkReference> for GetChunk {
    fn rpc_from(chunk_reference: ChunkReference) -> Self {
        Self(chunk_reference)
    }
}

impl RpcFrom<GetChunkError> for RpcChunkError {
    fn rpc_from(error: GetChunkError) -> Self {
        match error {
            GetChunkError::IOError { error_message } => Self::InternalError { error_message },
            GetChunkError::UnknownBlock { error_message } => Self::UnknownBlock { error_message },
            GetChunkError::InvalidShardId { shard_id } => Self::InvalidShardId { shard_id },
            GetChunkError::UnknownChunk { chunk_hash } => Self::UnknownChunk { chunk_hash },
            GetChunkError::EpochOutOfBounds { epoch_id } => Self::EpochOutOfBounds { epoch_id },
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
