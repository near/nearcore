use crate::challenge::Challenge;
use crate::sharding::ShardChunkHeader;
use crate::stateless_validation::spice_chunk_endorsement::SpiceEndorsementCoreStatement;
use crate::types::{ChunkExecutionResult, SpiceChunkId};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_crypto::vrf::{Proof, Value};
use near_primitives_core::hash::CryptoHash;
use near_schema_checker_lib::ProtocolSchema;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockBodyV1 {
    pub chunks: Vec<ShardChunkHeader>,
    #[deprecated]
    pub challenges: Vec<Challenge>,

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
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockBodyV2 {
    pub chunks: Vec<ShardChunkHeader>,
    #[deprecated]
    pub challenges: Vec<Challenge>,

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

/// V2 -> V3: block body for spice:
/// - replacing endorsements with core statements.
/// - removing deprecated challenges.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct SpiceBlockBodyV3 {
    pub chunks: Vec<ShardChunkHeader>,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: Value,
    pub vrf_proof: Proof,

    pub core_statements: SpiceCoreStatements,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub enum SpiceCoreStatement {
    Endorsement(SpiceEndorsementCoreStatement),
    ChunkExecutionResult { chunk_id: SpiceChunkId, execution_result: ChunkExecutionResult },
}

impl SpiceCoreStatement {
    pub fn chunk_id(&self) -> &SpiceChunkId {
        match self {
            SpiceCoreStatement::Endorsement(endorsement) => endorsement.chunk_id(),
            SpiceCoreStatement::ChunkExecutionResult { chunk_id, .. } => &chunk_id,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct SpiceCoreStatements(Vec<SpiceCoreStatement>);

impl SpiceCoreStatements {
    pub fn empty() -> &'static Self {
        static EMPTY: SpiceCoreStatements = SpiceCoreStatements(Vec::new());
        &EMPTY
    }

    pub fn new(statements: Vec<SpiceCoreStatement>) -> Self {
        Self(statements)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, SpiceCoreStatement> {
        self.0.iter()
    }

    pub fn iter_execution_results(
        &self,
    ) -> impl Iterator<Item = (&SpiceChunkId, &ChunkExecutionResult)> {
        self.0.iter().filter_map(|s| match s {
            SpiceCoreStatement::ChunkExecutionResult { chunk_id, execution_result } => {
                Some((chunk_id, execution_result))
            }
            _ => None,
        })
    }

    pub fn iter_endorsements(&self) -> impl Iterator<Item = &SpiceEndorsementCoreStatement> {
        self.0.iter().filter_map(|s| match s {
            SpiceCoreStatement::Endorsement(e) => Some(e),
            _ => None,
        })
    }
}

impl<'a> IntoIterator for &'a SpiceCoreStatements {
    type Item = &'a SpiceCoreStatement;
    type IntoIter = std::slice::Iter<'a, SpiceCoreStatement>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum BlockBody {
    V1(BlockBodyV1) = 0,
    V2(BlockBodyV2) = 1,
    V3(SpiceBlockBodyV3) = 2,
}

impl BlockBody {
    pub fn new(
        chunks: Vec<ShardChunkHeader>,
        vrf_value: Value,
        vrf_proof: Proof,
        chunk_endorsements: Vec<ChunkEndorsementSignatures>,
    ) -> Self {
        #[allow(deprecated)]
        BlockBody::V2(BlockBodyV2 {
            chunks,
            challenges: vec![],
            vrf_value,
            vrf_proof,
            chunk_endorsements,
        })
    }

    pub fn new_for_spice(
        chunks: Vec<ShardChunkHeader>,
        vrf_value: Value,
        vrf_proof: Proof,
        core_statements: SpiceCoreStatements,
    ) -> Self {
        BlockBody::V3(SpiceBlockBodyV3 { chunks, vrf_value, vrf_proof, core_statements })
    }

    #[inline]
    pub fn chunks(&self) -> &[ShardChunkHeader] {
        match self {
            BlockBody::V1(body) => &body.chunks,
            BlockBody::V2(body) => &body.chunks,
            BlockBody::V3(body) => &body.chunks,
        }
    }

    #[inline]
    pub fn vrf_value(&self) -> &Value {
        match self {
            BlockBody::V1(body) => &body.vrf_value,
            BlockBody::V2(body) => &body.vrf_value,
            BlockBody::V3(body) => &body.vrf_value,
        }
    }

    #[inline]
    pub fn vrf_proof(&self) -> &Proof {
        match self {
            BlockBody::V1(body) => &body.vrf_proof,
            BlockBody::V2(body) => &body.vrf_proof,
            BlockBody::V3(body) => &body.vrf_proof,
        }
    }

    #[inline]
    pub fn chunk_endorsements(&self) -> &[ChunkEndorsementSignatures] {
        match self {
            BlockBody::V1(_) => &[],
            BlockBody::V2(body) => &body.chunk_endorsements,
            BlockBody::V3(_) => &[],
        }
    }

    #[inline]
    pub fn spice_core_statements(&self) -> &SpiceCoreStatements {
        match self {
            BlockBody::V1(_) | BlockBody::V2(_) => SpiceCoreStatements::empty(),
            BlockBody::V3(body) => &body.core_statements,
        }
    }

    #[inline]
    pub fn is_spice_block(&self) -> bool {
        match self {
            BlockBody::V1(_) | BlockBody::V2(_) => false,
            BlockBody::V3(_) => true,
        }
    }

    pub fn compute_hash(&self) -> CryptoHash {
        // From BlockBodyV2 onwards, we hash the entire body including version.
        match self {
            BlockBody::V1(body) => body.compute_hash(),
            BlockBody::V2(_) | BlockBody::V3(_) => CryptoHash::hash_borsh(self),
        }
    }
}
