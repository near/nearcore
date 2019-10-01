use borsh::{BorshDeserialize, BorshSerialize};

use crate::block::BlockHeader;
use crate::hash::CryptoHash;
use crate::sharding::{ShardChunk, ShardChunkHeader};
use crate::types::ShardId;

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum Challenge {
    /// Double signed block.
    BlockDoubleSign { left_block_header: BlockHeader, right_block_header: BlockHeader },
    /// Invalid chunk header (proofs are invalid).
    ChunkProofs { chunk: ShardChunk },
    /// Doesn't match post-{state root, outgoing receipts, gas used, etc} results after applying previous chunk.
    ChunkState { prev_chunk: ShardChunk, chunk_header: ShardChunkHeader },
}

pub type Challenges = Vec<Challenge>;
