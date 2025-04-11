use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::sharding::{EncodedShardChunk, ShardChunk, ShardChunkHeader};
use crate::state::PartialState;
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_primitives_core::types::BlockHeight;
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::Debug;

/// Double signed block.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct BlockDoubleSign {
    pub left_block_header: Vec<u8>,
    pub right_block_header: Vec<u8>,
}

/// Invalid chunk (body of the chunk doesn't match proofs or invalid encoding).
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct ChunkProofs {
    /// Encoded block header that contains invalid chunk.
    pub block_header: Vec<u8>,
    /// Merkle proof of inclusion of this chunk.
    pub merkle_proof: MerklePath,
    /// Invalid chunk in an encoded form or in a decoded form.
    pub chunk: Box<MaybeEncodedShardChunk>,
}

/// Either `EncodedShardChunk` or `ShardChunk`. Used for `ChunkProofs`.
/// `Decoded` is used to avoid re-encoding an already decoded chunk to construct a challenge.
/// `Encoded` is still needed in case a challenge challenges an invalid encoded chunk that can't be
/// decoded.
#[allow(clippy::large_enum_variant)] // both variants are large
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub enum MaybeEncodedShardChunk {
    Encoded(EncodedShardChunk),
    Decoded(ShardChunk),
}

/// Doesn't match post-{state root, outgoing receipts, gas used, etc} results after applying previous chunk.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct ChunkState {
    /// Encoded prev block header.
    pub prev_block_header: Vec<u8>,
    /// Block height.
    /// TODO: block header is likely to be needed if we ever want to support
    /// challenges fully.
    pub block_height: BlockHeight,
    /// Merkle proof in inclusion of prev chunk.
    pub prev_merkle_proof: MerklePath,
    /// Previous chunk that contains transactions.
    pub prev_chunk: ShardChunk,
    /// Merkle proof of inclusion of this chunk.
    pub merkle_proof: MerklePath,
    /// Invalid chunk header.
    pub chunk_header: ShardChunkHeader,
    /// Partial state that was affected by transactions of given chunk.
    pub partial_state: PartialState,
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum ChallengeBody {
    BlockDoubleSign(BlockDoubleSign),
    ChunkProofs(ChunkProofs),
    ChunkState(ChunkState),
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
#[borsh(init=init)]
pub struct Challenge {
    pub body: ChallengeBody,
    pub account_id: AccountId,
    pub signature: Signature,

    #[borsh(skip)]
    pub hash: CryptoHash,
}

impl Challenge {
    pub fn init(&mut self) {
        self.hash = CryptoHash::hash_borsh(&self.body);
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
pub struct SlashedValidator {
    pub account_id: AccountId,
    pub is_double_sign: bool,
}
