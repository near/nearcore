//! Fetch-engine state for SPICE data distribution.

mod data_id;
mod fetchable;
mod item;

pub use data_id::DataId;
pub(crate) use fetchable::DataPolicy;
use fetchable::ReceiptProofPolicy;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
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

/// What the signed message got wrong. Every variant is attributable to its sender.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SenderFault {
    #[error("commitment settled as garbage: {0}")]
    GarbageCommitment(AssembledDataError),
    #[error("part merkle proof does not verify against the commitment root")]
    InvalidMerkleProof,
    #[error("sender already backed another commitment")]
    ConflictingCommitment,
}

/// Outcome of accepting parts for an item.
#[must_use]
#[derive(Debug)]
pub(crate) enum PartsOutcome {
    /// Parts accepted; no commitment decoded.
    Collecting,
    /// A commitment decoded to this data, which matches the committed hash and the id.
    Decoded(SpiceData),
    /// The commitment was already decoded; a late or re-sent part.
    Settled,
    /// No item tracks the id.
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
// TODO(spice-data-distribution): only receipt proofs route here; witnesses still live
// on the old actor path (#16275).
pub(crate) struct SpiceDataManager {
    encoders: ReedSolomonEncoderCache,
    policies: Policies,
    /// All tracked items, in any state.
    items: HashMap<DataId, FetchItem>,
    /// Ids of tracked items, indexed by their block's height as captured when first tracked
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,
    /// Highest final execution head reported; `None` until the first report.
    final_execution_head: Option<BlockHeight>,
}

impl SpiceDataManager {
    pub(crate) fn new(data_parts_ratio: f64, policies: Policies) -> Self {
        Self {
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
    pub(crate) fn track_block(&mut self, block: &BlockHeader) -> Result<(), Error> {
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
            self.items.insert(id, FetchItem::new(height));
        }
        Ok(())
    }

    /// The only insert path for received units. Verifies every part against the
    /// commitment before inserting any; one failing part rejects the whole message and
    /// leaves the item untouched. A decoding insert checks the decoded data against the
    /// committed hash and the id, settles the commitment either way, and returns matching
    /// data.
    pub(crate) fn on_parts_received(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        total_parts: usize,
    ) -> Result<PartsOutcome, SenderFault> {
        let Some(item) = self.items.get_mut(id) else {
            return Ok(PartsOutcome::NotWanted);
        };
        let mut verified = Vec::with_capacity(parts.len());
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            let part =
                VerifiedCodedPart::verify(commitment, total_parts, part_ord, part, &merkle_proof)
                    .ok_or(SenderFault::InvalidMerkleProof)?;
            verified.push(part);
        }
        let encoder = self.encoders.entry(total_parts);
        for part in verified {
            match item.insert_part(&encoder, id, sender, part) {
                PartInsertResult::Decoded(data) => {
                    return Ok(PartsOutcome::Decoded(data));
                }
                PartInsertResult::Garbage(error) => {
                    return Err(SenderFault::GarbageCommitment(error));
                }
                PartInsertResult::ConflictingCommitment => {
                    return Err(SenderFault::ConflictingCommitment);
                }
                PartInsertResult::Settled => return Ok(PartsOutcome::Settled),
                PartInsertResult::Accepted | PartInsertResult::Duplicate => {}
            }
        }
        Ok(PartsOutcome::Collecting)
    }

    /// The final execution head advanced: the chain is past every item at or below it,
    /// so their data can no longer be applied. Removes them, and [`Self::track_block`] refuses
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
                let item = self.items.get(&id).expect("index entry names a tracked item");
                assert_eq!(item.height, bucket_height, "index entry height matches its item");
                self.items.remove(&id);
            }
        }
    }
}
