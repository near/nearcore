use super::item::DataId;
use crate::spice::chunk_executor_actor::receipt_proof_exists;
use crate::spice::data_manager::SpiceData;
use near_chain::Block;
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;

/// Policy to query a fetchable data type's properties (like relevance, doneness, and
/// validity of assembled data).
/// Implementations carry the engine's only chain dependencies.
/// Everything else about fetching is generic.
pub(crate) trait DataPolicy {
    /// Whether this node needs the item. Consulted once, at seed time; afterwards the
    /// lifecycle advances by events. `block` is the id's block.
    // TODO(spice-data-distribution): becomes tri-state (`Interest`) when the first pull
    // starter lands (#16275).
    fn classify_at_seed(&self, id: &DataId, block: &Block) -> Result<bool, near_chain::Error>;

    /// Checks the assembled data against its id. Runs at delivery, after the tracker's
    /// decode and committed-hash check; a failure is attributable to the part senders.
    fn verify_assembled(&self, id: &DataId, data: &SpiceData) -> Result<(), AssembledDataError>;

    /// Whether the durable artifact this item exists to obtain is already in the store.
    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AssembledDataError {
    #[error("decoded data doesn't match id")]
    IdAndDataMismatch,
    #[error("decoded receipt proof to_shard_id is invalid")]
    InvalidToShardId,
    #[error("decoded receipt proof from_shard_id is invalid")]
    InvalidFromShardId,
}

/// Receipt proofs: produced by the source chunk's producers, needed by nodes that apply
/// the destination shard in the next block; done once the proof is persisted.
pub(crate) struct ReceiptProofPolicy {
    chain_store: ChainStoreAdapter,
    shard_tracker: ShardTracker,
}

impl ReceiptProofPolicy {
    pub(crate) fn new(chain_store: ChainStoreAdapter, shard_tracker: ShardTracker) -> Self {
        Self { chain_store, shard_tracker }
    }
}

impl DataPolicy for ReceiptProofPolicy {
    fn classify_at_seed(&self, id: &DataId, block: &Block) -> Result<bool, near_chain::Error> {
        let DataId::ReceiptProof { source, to_shard } = id;
        debug_assert_eq!(&source.block_hash, block.hash());
        // Applying the source shard ourselves produces the proof locally; this is
        // also why a producer never fetches its own proof.
        if self.shard_tracker.should_apply_chunk(
            ApplyChunksMode::IsCaughtUp,
            block.header().prev_hash(),
            source.shard_id,
        ) {
            return Ok(false);
        }
        // The proof feeds applying `to_shard` in the next block.
        Ok(self.shard_tracker.should_apply_chunk(
            ApplyChunksMode::IsCaughtUp,
            block.hash(),
            *to_shard,
        ))
    }

    fn verify_assembled(&self, id: &DataId, data: &SpiceData) -> Result<(), AssembledDataError> {
        let DataId::ReceiptProof { source, to_shard } = id;
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

    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error> {
        let DataId::ReceiptProof { source, to_shard } = id;
        Ok(receipt_proof_exists(
            &self.chain_store.store(),
            &source.block_hash,
            *to_shard,
            source.shard_id,
        ))
    }
}
