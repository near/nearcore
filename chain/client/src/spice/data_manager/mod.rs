//! Fetch-engine state for SPICE data distribution.

mod fetchable;
mod item;

pub(crate) use fetchable::{AssembledDataError, DataPolicy, ReceiptProofPolicy};
pub use item::DataId;
pub(crate) use item::{AssemblyError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, Item, PartInsertResult};
use near_async::time::Clock;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight};
use std::collections::{BTreeMap, HashMap};

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DataManagerError {
    #[error("no item tracks this data")]
    UnknownItem,
    #[error("commitment decoded to garbage")]
    GarbageCommitment,
    #[error(transparent)]
    Assembly(#[from] AssemblyError),
    #[error("assembled data doesn't match its id: {0}")]
    AssembledData(#[from] AssembledDataError),
}

/// The per-data-type policies, one per [`DataId`] variant.
pub(crate) struct Policies {
    pub(crate) receipt_proofs: ReceiptProofPolicy,
}

/// Each method dispatches to the policy of `id`'s data type; see [`DataPolicy`] for
/// the contracts.
impl Policies {
    fn for_id(&self, id: &DataId) -> &dyn DataPolicy {
        match id {
            DataId::ReceiptProof { .. } => &self.receipt_proofs,
        }
    }

    fn classify_at_seed(
        &self,
        id: &DataId,
        block: &near_chain::Block,
    ) -> Result<bool, near_chain::Error> {
        self.for_id(id).classify_at_seed(id, block)
    }

    fn verify_assembled(&self, id: &DataId, data: &SpiceData) -> Result<(), AssembledDataError> {
        self.for_id(id).verify_assembled(id, data)
    }

    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error> {
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
    /// Ids of tracked items, indexed by their block's height as captured at seed time
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

    /// Starts tracking `id` if this node needs it and doesn't already have or track it.
    /// Idempotent. `block` is the id's block.
    pub(crate) fn seed(
        &mut self,
        id: DataId,
        block: &near_chain::Block,
    ) -> Result<(), near_chain::Error> {
        if self.items.contains_key(&id) {
            return Ok(());
        }
        let height = block.header().height();
        // The chain is past the block, so the data can never be applied.
        if self.final_execution_head.is_some_and(|head| height <= head) {
            return Ok(());
        }
        if !self.policies.classify_at_seed(&id, block)? || self.policies.is_done(&id)? {
            return Ok(());
        }
        self.items_by_height.entry(height).or_default().push(id.clone());
        self.items.insert(id, Item::Fetch(FetchItem::waiting_for_push(height)));
        Ok(())
    }

    /// The only insert path for received units. Verifies each part against the
    /// commitment, inserts, and on a completing insert checks the assembled data
    /// against its id: a mismatch bans the commitment on the spot, a match returns the
    /// data for handoff to the consumer, leaving the item parked until the consumer's
    /// (local) verification result arrives.
    pub(crate) fn on_data_received(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        total_parts: usize,
    ) -> Result<Option<SpiceData>, DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Err(DataManagerError::UnknownItem);
        };
        let encoder = self.encoders.entry(total_parts);
        let mut decoded = None;
        // TODO(spice-data-distribution): verify every part before inserting any; today
        // the first bad part aborts the loop without undoing earlier inserts (#16275).
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            let verified =
                VerifiedCodedPart::verify(commitment, total_parts, part_ord, part, &merkle_proof)?;
            match item.insert_part(&self.clock, &encoder, sender, verified)? {
                PartInsertResult::Complete(data) => {
                    decoded = Some(data);
                    break;
                }
                PartInsertResult::Garbage { contributors } => {
                    tracing::debug!(target: "spice_data_distribution", ?id, ?contributors, "commitment decoded to garbage");
                    return Err(DataManagerError::GarbageCommitment);
                }
                PartInsertResult::Accepted | PartInsertResult::Duplicate => {}
            }
        }
        let Some(data) = decoded else {
            return Ok(None);
        };

        if let Err(err) = self.policies.verify_assembled(id, &data) {
            let contributors = item
                .mark_failed(commitment)
                .expect("a completing insert leaves the item delivered");
            tracing::debug!(target: "spice_data_distribution", ?id, ?contributors, "assembled data doesn't match its id");
            return Err(err.into());
        }
        Ok(Some(data))
    }

    /// Consumer validated and persisted the delivered data. A verification result for an expired
    /// item or a commitment other than the delivered one is rejected without effect.
    pub(crate) fn on_verified(
        &mut self,
        id: &DataId,
        commitment: &SpiceDataCommitment,
    ) -> Result<(), DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Err(DataManagerError::UnknownItem);
        };
        item.mark_verified(commitment)?;
        Ok(())
    }

    /// Consumer rejected the delivered data: the decoded commitment is banned and
    /// collecting resumes from the remaining trackers. A verification result for an expired item or
    /// a commitment other than the delivered one is rejected without effect.
    pub(crate) fn on_failed(
        &mut self,
        id: &DataId,
        commitment: &SpiceDataCommitment,
    ) -> Result<(), DataManagerError> {
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return Err(DataManagerError::UnknownItem);
        };
        // TODO(spice-data-distribution): feed the contributors into reputation (#16275).
        let contributors = item.mark_failed(commitment)?;
        tracing::debug!(target: "spice_data_distribution", ?id, ?contributors, "delivered data failed consumer validation");
        Ok(())
    }

    /// The final execution head advanced: the chain is past every item at or below it,
    /// so their data can no longer be applied. Removes them, and [`Self::seed`] refuses
    /// them from now on. Index entries whose item is gone or lives at another height
    /// are stale and skipped.
    pub(crate) fn on_final_execution_head(&mut self, height: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(height));
        let Some(next_height) = height.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = std::mem::replace(&mut self.items_by_height, live);
        for (bucket_height, ids) in expired {
            for id in ids {
                // TODO(spice-data-distribution): expire produce items here too once they
                // exist — skipping them while their index bucket is discarded would leak
                // them (#16275).
                let Some(Item::Fetch(item)) = self.items.get(&id) else {
                    continue;
                };
                if item.height != bucket_height {
                    continue;
                }
                self.items.remove(&id);
            }
        }
    }
}
