use super::item::{AssembledDataError, SpiceData};
use near_primitives::hash::CryptoHash;
use near_primitives::spice::partial_data::SpiceDataIdentifier;
use near_primitives::types::{ShardId, SpiceChunkId};

/// Identity of one piece of distributed data the engine tracks.
/// Methods depend only on the id and the data it names, never on the chain.
// TODO(spice-data-distribution): witnesses and contract code move here when their
// paths switch to the engine (#16275).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataId {
    /// `source` is the chunk whose execution produced the receipts; `to_shard` is their
    /// destination. Produced by `source`'s producers, needed by next-block producers of
    /// `to_shard`.
    ReceiptProof { source: SpiceChunkId, to_shard: ShardId },
}

impl DataId {
    pub fn receipt_proof(
        block_hash: CryptoHash,
        from_shard_id: ShardId,
        to_shard_id: ShardId,
    ) -> Self {
        Self::ReceiptProof {
            source: SpiceChunkId { block_hash, shard_id: from_shard_id },
            to_shard: to_shard_id,
        }
    }

    /// Checks that decoded data is the data this id names.
    pub(crate) fn verify_data(&self, data: &SpiceData) -> Result<(), AssembledDataError> {
        let DataId::ReceiptProof { source, to_shard } = self;
        let SpiceData::ReceiptProof(proof) = data else {
            return Err(AssembledDataError::IdAndDataMismatch);
        };
        if &proof.1.to_shard_id != to_shard {
            return Err(AssembledDataError::InvalidToShardId);
        }
        if proof.1.from_shard_id != source.shard_id {
            return Err(AssembledDataError::InvalidFromShardId);
        }
        Ok(())
    }
}

impl From<&DataId> for SpiceDataIdentifier {
    fn from(id: &DataId) -> Self {
        match id {
            DataId::ReceiptProof { source, to_shard } => SpiceDataIdentifier::ReceiptProof {
                block_hash: source.block_hash,
                from_shard_id: source.shard_id,
                to_shard_id: *to_shard,
            },
        }
    }
}
