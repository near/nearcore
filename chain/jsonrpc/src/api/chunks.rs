use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::chunks::{ChunkReference, RpcChunkRequest};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockId, ShardId};

use super::{parse_params, RpcRequest};

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
