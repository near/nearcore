use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::block::BlockHeader;
use crate::hash::CryptoHash;
use crate::sharding::ShardChunkHeader;
use crate::types::ShardId;

pub type PartialState = HashMap<CryptoHash, Vec<u8>>;

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum Challenge {
    /// Double signed block.
    BlockDoubleSign { left_block_header: BlockHeader, right_block_header: BlockHeader },
    /// Double signed chunk within the same block.
    ChunkDoubleSign { left_chunk_header: ShardChunkHeader, right_chunk_header: ShardChunkHeader },
    /// Invalid chunk header (proofs are invalid).
    ChunkProofs { chunk_header: ShardChunkHeader },
    /// Doesn't match post-{state root, outgoing receipts, gas used, etc} results after applying previous chunk.
    ChunkState {
        chunk_header: ShardChunkHeader,
        block_hash: CryptoHash,
        shard_id: ShardId,
        partial_state: PartialState,
    },
}
