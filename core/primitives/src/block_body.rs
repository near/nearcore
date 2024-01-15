use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::vrf::{Proof, Value};
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
    pub fn compute_hash(&self) -> CryptoHash {
        CryptoHash::hash_borsh(self)
    }
}

// For now, we only have one version of block body.
// Eventually with ChunkValidation, we would include ChunkEndorsement in BlockBodyV2
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub enum BlockBody {
    V1(BlockBodyV1),
}

impl BlockBody {
    pub fn new(
        _protocol_version: ProtocolVersion,
        chunks: Vec<ShardChunkHeader>,
        challenges: Challenges,
        vrf_value: Value,
        vrf_proof: Proof,
    ) -> Self {
        // Eventually we will have different versions of block body, but for now we only have V1.
        BlockBody::V1(BlockBodyV1 { chunks, challenges, vrf_value, vrf_proof })
    }

    #[inline]
    pub fn chunks(&self) -> &[ShardChunkHeader] {
        match self {
            BlockBody::V1(body) => &body.chunks,
        }
    }

    #[inline]
    pub fn challenges(&self) -> &Challenges {
        match self {
            BlockBody::V1(body) => &body.challenges,
        }
    }

    #[inline]
    pub fn vrf_value(&self) -> &Value {
        match self {
            BlockBody::V1(body) => &body.vrf_value,
        }
    }

    #[inline]
    pub fn vrf_proof(&self) -> &Proof {
        match self {
            BlockBody::V1(body) => &body.vrf_proof,
        }
    }

    pub fn compute_hash(&self) -> CryptoHash {
        match self {
            BlockBody::V1(body) => body.compute_hash(),
        }
    }
}
