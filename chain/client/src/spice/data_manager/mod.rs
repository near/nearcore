//! Fetch-engine state for SPICE data distribution.

mod fetchable;
mod item;

pub(crate) use fetchable::DataPolicy;
use fetchable::ReceiptProofPolicy;
pub use item::DataId;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, Item, PartInsertResult};
use near_async::time::Clock;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DataManagerError {
    #[error("no item tracks this data")]
    UnknownItem,
    #[error("commitment decoded to garbage: {0}")]
    GarbageCommitment(AssembledDataError),
    #[error("item is not collecting")]
    NotCollecting,
    #[error("item is not waiting for a verification result")]
    NotDelivered,
    #[error("part merkle proof does not verify against the commitment root")]
    InvalidMerkleProof,
    #[error("part ordinal is out of range")]
    InvalidOrdinal,
    #[error("part was verified against a different total parts count")]
    WrongTotalParts,
    #[error("part length does not match the commitment's encoded length")]
    WrongPartLength,
    #[error("sender already backed another commitment")]
    ConflictingCommitment,
    #[error("commitment was rejected after validation")]
    BannedCommitment,
}

/// Outcome of accepting parts for an item.
#[must_use]
#[derive(Debug)]
pub(crate) enum ReceivedParts {
    /// Parts accepted; the item is still collecting.
    Collecting,
    /// The parts completed the item; the data is handed to the consumer.
    Complete(SpiceData),
    /// No item tracks the id, or the item is past collecting (delivered or processed).
    NotWanted,
}

/// The per-data-type policies, one per [`DataId`] variant.
pub(crate) struct Policies {
    receipt_proofs: ReceiptProofPolicy,
}

impl Policies {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self { receipt_proofs: ReceiptProofPolicy::new(chain_store, epoch_manager, shard_tracker) }
    }

    fn for_id(&self, id: &DataId) -> &dyn DataPolicy {
        match id {
            DataId::ReceiptProof { .. } => &self.receipt_proofs,
        }
    }
}

/// Fans out per-block queries over every policy; dispatches per-id calls to the policy
/// of `id`'s data type.
impl DataPolicy for Policies {
    fn needed_ids(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        self.receipt_proofs.needed_ids(block)
    }

    fn is_done(&self, id: &DataId) -> Result<bool, Error> {
        self.for_id(id).is_done(id)
    }
}

/// Owns the per-item fetch lifecycle: what this node still needs, the parts received so
/// far and who sent them, and when an item stops being relevant.
/// Validation results are reported back via [`Self::on_verified`]/[`Self::on_failed`].
// TODO(spice-data-distribution): only receipt proofs route here; witnesses still live
// on the old actor path (#16275).
pub(crate) struct SpiceDataManager {
    clock: Clock,
    encoders: ReedSolomonEncoderCache,
    policies: Policies,
    /// All tracked items, in any state.
    items: HashMap<DataId, Item>,
    /// Ids of tracked items, indexed by their block's height as captured when first tracked
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,
    /// Highest final execution head reported; `None` until the first report. Items at
    /// or below it can never be applied.
    final_execution_head: Option<BlockHeight>,
}

impl SpiceDataManager {
    pub(crate) fn new(clock: Clock, data_parts_ratio: f64, policies: Policies) -> Self {
        Self {
            clock,
            encoders: ReedSolomonEncoderCache::new(data_parts_ratio),
            policies,
            items: HashMap::new(),
            items_by_height: BTreeMap::new(),
            final_execution_head: None,
        }
    }

    /// Whether an item for `id` exists, in any state.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, id: &DataId) -> bool {
        self.items.contains_key(id)
    }

    /// Starts tracking every item this node needs from `block` and doesn't already have or track. Idempotent.
    pub(crate) fn on_block(&mut self, block: &BlockHeader) -> Result<(), Error> {
        let height = block.height();
        // The chain is past the block, so its data can never be applied.
        if self.final_execution_head.is_some_and(|head| height <= head) {
            return Ok(());
        }
        for id in self.policies.needed_ids(block)? {
            if self.items.contains_key(&id) || self.policies.is_done(&id)? {
                continue;
            }
            self.items_by_height.entry(height).or_default().push(id.clone());
            self.items.insert(id, Item::Fetch(FetchItem::waiting_for_push(height)));
        }
        Ok(())
    }

    /// The only insert path for received units. Verifies each part against the
    /// commitment and inserts it. A completing insert checks the decoded data against
    /// the committed hash and the id: a failure bans the commitment, a match returns the
    /// data for handoff to the consumer, leaving the item parked until the consumer's
    /// (local) verification result arrives. Errors are attributable to the sender.
    pub(crate) fn on_parts_received(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        total_parts: usize,
    ) -> Result<ReceivedParts, DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Ok(ReceivedParts::NotWanted);
        };
        let encoder = self.encoders.entry(total_parts);
        // TODO(spice-data-distribution): verify every part before inserting any; today
        // the first bad part aborts the loop without undoing earlier inserts (#16275).
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            let verified =
                VerifiedCodedPart::verify(commitment, total_parts, part_ord, part, &merkle_proof)?;
            match item.insert_part(&self.clock, &encoder, id, sender, verified) {
                Ok(PartInsertResult::Complete(data)) => return Ok(ReceivedParts::Complete(data)),
                Ok(PartInsertResult::Garbage { contributors, error }) => {
                    tracing::debug!(target: "spice_data_distribution", ?id, ?contributors, "commitment decoded to garbage");
                    return Err(DataManagerError::GarbageCommitment(error));
                }
                Ok(PartInsertResult::Accepted | PartInsertResult::Duplicate) => {}
                Err(DataManagerError::NotCollecting) => return Ok(ReceivedParts::NotWanted),
                Err(err) => return Err(err),
            }
        }
        Ok(ReceivedParts::Collecting)
    }

    /// Consumer validated and persisted the delivered data (so `is_done` holds for it from now on).
    /// A verification result for an expired item is rejected without effect.
    pub(crate) fn on_verified(&mut self, id: &DataId) -> Result<(), DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Err(DataManagerError::UnknownItem);
        };
        item.mark_verified()?;
        Ok(())
    }

    /// Consumer rejected the delivered data: the delivered commitment is banned and
    /// collecting resumes from the remaining trackers. A verification result for an
    /// expired item is rejected without effect.
    pub(crate) fn on_failed(&mut self, id: &DataId) -> Result<(), DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Err(DataManagerError::UnknownItem);
        };
        // TODO(spice-data-distribution): feed the contributors into reputation (#16275).
        let contributors = item.mark_failed()?;
        tracing::debug!(target: "spice_data_distribution", ?id, ?contributors, "delivered data failed consumer validation");
        Ok(())
    }

    /// The final execution head advanced: the chain is past every item at or below it,
    /// so their data can no longer be applied. Removes them, and [`Self::on_block`] refuses
    /// them from now on.
    pub(crate) fn on_final_execution_head(&mut self, height: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(height));
        let Some(next_height) = height.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = std::mem::replace(&mut self.items_by_height, live);
        for (bucket_height, ids) in expired {
            for id in ids {
                let Item::Fetch(item) =
                    self.items.get(&id).expect("index entry names a tracked item");
                assert_eq!(item.height, bucket_height, "index entry height matches its item");
                self.items.remove(&id);
            }
        }
    }
}
