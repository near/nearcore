use super::item::{AssembledDataError, SpiceData};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::partial_data::SpiceDataIdentifier;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::types::{ShardId, SpiceChunkId};
use std::iter;

/// Identity of one piece of distributed data the engine serves or tracks.
/// Methods depend only on the id and the data it names, never on the chain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataId {
    /// Produced by `source`'s producers, needed by next-block producers of `to_shard`.
    ReceiptProof(ReceiptProofId),
    /// The state witness of a chunk. Produced by the chunk's producers, needed by its
    /// validators.
    Witness(SpiceChunkId),
}

/// Identity of the receipts one chunk's execution sends to one shard.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReceiptProofId {
    /// The chunk whose execution produced the receipts.
    pub source: SpiceChunkId,
    /// The destination shard of the receipts.
    pub to_shard: ShardId,
}

/// Decoded data with the id it was checked against.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Decoded {
    ReceiptProof(ReceiptProofId, ReceiptProof),
    Witness { chunk_id: SpiceChunkId, witness: Box<SpiceChunkStateWitness> },
}

impl ReceiptProofId {
    pub fn new(block_hash: CryptoHash, from_shard_id: ShardId, to_shard_id: ShardId) -> Self {
        Self { source: SpiceChunkId { block_hash, shard_id: from_shard_id }, to_shard: to_shard_id }
    }
}

impl DataId {
    pub fn receipt_proof(
        block_hash: CryptoHash,
        from_shard_id: ShardId,
        to_shard_id: ShardId,
    ) -> Self {
        Self::ReceiptProof(ReceiptProofId::new(block_hash, from_shard_id, to_shard_id))
    }

    /// The chunk whose producers produce the data.
    pub(crate) fn producing_chunk(&self) -> &SpiceChunkId {
        match self {
            DataId::ReceiptProof(id) => &id.source,
            DataId::Witness(chunk_id) => chunk_id,
        }
    }

    /// Hash of the block the data belongs to.
    pub(crate) fn block_hash(&self) -> &CryptoHash {
        &self.producing_chunk().block_hash
    }

    /// The shards the id names.
    pub(crate) fn shard_ids(&self) -> impl Iterator<Item = ShardId> {
        let to_shard = match self {
            DataId::ReceiptProof(id) => Some(id.to_shard),
            DataId::Witness(_) => None,
        };
        iter::once(self.producing_chunk().shard_id).chain(to_shard)
    }

    /// Pairs decoded `data` with this id, if it is the data the id names.
    pub(crate) fn verify_data(&self, data: SpiceData) -> Result<Decoded, AssembledDataError> {
        match (self, data) {
            (DataId::ReceiptProof(id), SpiceData::ReceiptProof(proof)) => {
                if proof.1.to_shard_id != id.to_shard {
                    return Err(AssembledDataError::InvalidToShardId);
                }
                if proof.1.from_shard_id != id.source.shard_id {
                    return Err(AssembledDataError::InvalidFromShardId);
                }
                Ok(Decoded::ReceiptProof(id.clone(), proof))
            }
            (DataId::Witness(chunk_id), SpiceData::StateWitness(witness)) => {
                if witness.chunk_id() != chunk_id {
                    return Err(AssembledDataError::InvalidWitnessChunkId);
                }
                Ok(Decoded::Witness { chunk_id: chunk_id.clone(), witness })
            }
            (DataId::ReceiptProof(_), SpiceData::StateWitness(_))
            | (DataId::Witness(_), SpiceData::ReceiptProof(_)) => {
                Err(AssembledDataError::IdAndDataMismatch)
            }
        }
    }
}

impl From<&SpiceDataIdentifier> for DataId {
    fn from(id: &SpiceDataIdentifier) -> Self {
        match id {
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                Self::receipt_proof(*block_hash, *from_shard_id, *to_shard_id)
            }
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                Self::Witness(SpiceChunkId { block_hash: *block_hash, shard_id: *shard_id })
            }
        }
    }
}

impl From<&DataId> for SpiceDataIdentifier {
    fn from(id: &DataId) -> Self {
        match id {
            DataId::ReceiptProof(ReceiptProofId { source, to_shard }) => {
                SpiceDataIdentifier::ReceiptProof {
                    block_hash: source.block_hash,
                    from_shard_id: source.shard_id,
                    to_shard_id: *to_shard,
                }
            }
            DataId::Witness(chunk_id) => SpiceDataIdentifier::Witness {
                block_hash: chunk_id.block_hash,
                shard_id: chunk_id.shard_id,
            },
        }
    }
}
