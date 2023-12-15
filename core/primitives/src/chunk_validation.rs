use crate::sharding::{ChunkHash, ShardChunkHeader};
use crate::types::StateRoot;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_primitives_core::types::AccountId;

/// The state witness for a chunk; proves the state transition that the
/// chunk attests to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkStateWitness {
    // TODO(#10265): Is the entire header necessary?
    pub chunk_header: ShardChunkHeader,
    // TODO(#10265): Replace this with fields for the actual witness.
    pub state_root: StateRoot,
}

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsement {
    pub inner: ChunkEndorsementInner,
    pub account_id: AccountId,
    pub signature: Signature,
}

/// This is the part of the chunk endorsement that is actually being signed.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsementInner {
    pub chunk_hash: ChunkHash,
    /// An arbitrary static string to make sure that this struct cannot be
    /// serialized to look identical to another serialized struct. For chunk
    /// production we are signing a chunk hash, so we need to make sure that
    /// this signature means something different.
    ///
    /// This is a messy workaround until we know what to do with NEP 483.
    signature_differentiator: String,
}

impl ChunkEndorsementInner {
    pub fn new(chunk_hash: ChunkHash) -> Self {
        Self { chunk_hash, signature_differentiator: "ChunkEndorsement".to_owned() }
    }
}

/// Message intended for the network layer to send a chunk endorsement.
/// It just includes an additional target account ID to send it to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsementMessage {
    pub endorsement: ChunkEndorsement,
    pub target: AccountId,
}
