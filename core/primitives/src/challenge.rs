use borsh::{BorshDeserialize, BorshSerialize};

use crate::hash::{hash, CryptoHash};
use crate::sharding::{EncodedShardChunk, ShardChunk, ShardChunkHeader};
use near_crypto::{BlsSignature, BlsSigner};

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

/// Invalid chunk (body of the chunk doesn't match proofs or invalid encoding).
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct ChunkProofs {
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
pub enum ChallengeBody {
    BlockDoubleSign(BlockDoubleSign),
    ChunkProofs(ChunkProofs),
    ChunkState(ChunkState),
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
#[borsh_init(init)]
pub struct Challenge {
    pub body: ChallengeBody,
    pub signature: BlsSignature,

    #[borsh_skip]
    hash: CryptoHash,
}

impl Challenge {
    pub fn init(&mut self) {
        self.hash = hash(&self.body.try_to_vec().expect("Failed to serialize"));
    }

    pub fn produce(body: ChallengeBody, signer: &dyn BlsSigner) -> Self {
        let hash = hash(&body.try_to_vec().expect("Failed to serialize"));
        let signature = signer.sign(hash.as_ref());
        Self { body, signature, hash }
    }
}

pub type Challenges = Vec<Challenge>;
