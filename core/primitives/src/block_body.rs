use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::vrf::{Proof, Value};
use near_crypto::Signature;
use near_primitives_core::checked_feature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ProtocolVersion;

use crate::challenge::Challenges;
use crate::sharding::ShardChunkHeader;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockBodyV1 {
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: Value,
    pub vrf_proof: Proof,
}

impl BlockBodyV1 {
    // Required as we have raw reference to BlockBodyV1 in BlockV3
    pub fn compute_hash(&self) -> CryptoHash {
        CryptoHash::hash_borsh(self)
    }
}

/// V1 -> V2: added chunk_endorsements from chunk_validators
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub struct BlockBodyV2 {
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: Value,
    pub vrf_proof: Proof,

    // Chunk endorsements
    // These are structured as a vector of Signatures from all ordered chunk_validators
    // for each shard got from fn get_ordered_chunk_validators
    // chunk_endorsements[shard_id][chunk_validator_index] is the signature (if present).
    // If the chunk_validator did not endorse the chunk, the signature is None.
    // For cases of missing chunk, we include the chunk endorsements from the previous
    // block just like we do for chunks.
    pub chunk_endorsements: Vec<ChunkEndorsementSignatures>,
}

pub type ChunkEndorsementSignatures = Vec<Option<Box<Signature>>>;

// For now, we only have one version of block body.
// Eventually with ChunkValidation, we would include ChunkEndorsement in BlockBodyV2
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub enum BlockBody {
    V1(BlockBodyV1),
    V2(BlockBodyV2),
}

impl BlockBody {
    pub fn new(
        protocol_version: ProtocolVersion,
        chunks: Vec<ShardChunkHeader>,
        challenges: Challenges,
        vrf_value: Value,
        vrf_proof: Proof,
        chunk_endorsements: Vec<ChunkEndorsementSignatures>,
    ) -> Self {
        if !checked_feature!("stable", StatelessValidationV0, protocol_version) {
            BlockBody::V1(BlockBodyV1 { chunks, challenges, vrf_value, vrf_proof })
        } else {
            BlockBody::V2(BlockBodyV2 {
                chunks,
                challenges,
                vrf_value,
                vrf_proof,
                chunk_endorsements,
            })
        }
    }

    #[inline]
    pub fn chunks(&self) -> &[ShardChunkHeader] {
        match self {
            BlockBody::V1(body) => &body.chunks,
            BlockBody::V2(body) => &body.chunks,
        }
    }

    #[inline]
    pub fn challenges(&self) -> &Challenges {
        match self {
            BlockBody::V1(body) => &body.challenges,
            BlockBody::V2(body) => &body.challenges,
        }
    }

    #[inline]
    pub fn vrf_value(&self) -> &Value {
        match self {
            BlockBody::V1(body) => &body.vrf_value,
            BlockBody::V2(body) => &body.vrf_value,
        }
    }

    #[inline]
    pub fn vrf_proof(&self) -> &Proof {
        match self {
            BlockBody::V1(body) => &body.vrf_proof,
            BlockBody::V2(body) => &body.vrf_proof,
        }
    }

    #[inline]
    pub fn chunk_endorsements(&self) -> &[ChunkEndorsementSignatures] {
        match self {
            BlockBody::V1(_) => &[],
            BlockBody::V2(body) => &body.chunk_endorsements,
        }
    }

    pub fn compute_hash(&self) -> CryptoHash {
        // From BlockBodyV2 onwards, we hash the entire body including version.
        match self {
            BlockBody::V1(body) => body.compute_hash(),
            _ => CryptoHash::hash_borsh(self),
        }
    }
}
