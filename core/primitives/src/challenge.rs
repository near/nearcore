use borsh::{BorshDeserialize, BorshSerialize};

use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::sharding::{EncodedShardChunk, ShardChunk, ShardChunkHeader};

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct StateItem {
    key: CryptoHash,
    value: Vec<u8>,
}

pub type PartialState = Vec<StateItem>;

/// Double signed block.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct BlockDoubleSign {
    pub left_block_header: Vec<u8>,
    pub right_block_header: Vec<u8>,
}

/// Invalid chunk header (body of the chunk doesn't match proofs or invalid encoding).
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct ChunkProofs {
    /// Block header that contains invalid chunk.
    pub block_header: Vec<u8>,
    /// Merkle proofs that this chunk is included in the block.
    pub merkle_proof: MerklePath,
    /// Invalid chunk in encoded form.
    pub chunk: EncodedShardChunk,
}

/// Doesn't match post-{state root, outgoing receipts, gas used, etc} results after applying previous chunk.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct ChunkState {
    /// Block header that contains invalid chunk.
    pub block_header: Vec<u8>,
    /// Previous chunk that contains transactions.
    pub prev_chunk: ShardChunk,
    /// Invalid chunk header.
    pub chunk_header: ShardChunkHeader,
    /// Partial state that was affected by transactions of given chunk.
    pub partial_state: PartialState,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub enum Challenge {
    BlockDoubleSign(BlockDoubleSign),
    ChunkProofs(ChunkProofs),
    ChunkState(ChunkState),
}

pub type Challenges = Vec<Challenge>;
