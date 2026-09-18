//! Fetch-engine state for SPICE data distribution.

mod data_id;
mod fetchable;
mod item;
mod pull;

pub use data_id::DataId;
pub(crate) use fetchable::DataPolicy;
use fetchable::{CertifiedFrontier, ReceiptProofPolicy};
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use near_chain::Error;
use near_chain::spice::core::SpiceCoreReader;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::chain_store::ChainStoreAdapter;
pub(crate) use pull::{PullConfig, PullRequest, rotated_source_index};
use std::collections::{BTreeMap, HashMap};
use std::mem::replace;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DataManagerError {
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
        core_reader: SpiceCoreReader,
    ) -> Self {
        Self {
            receipt_proofs: ReceiptProofPolicy::new(
                chain_store,
                epoch_manager,
                shard_tracker,
                core_reader,
            ),
        }
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

    fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.for_id(id).sources(id)
    }

    fn certified_frontier(&self, block: &BlockHeader) -> Result<CertifiedFrontier, Error> {
        self.receipt_proofs.certified_frontier(block)
    }

    fn is_pull_open(&self, id: &DataId, height: BlockHeight, frontier: &CertifiedFrontier) -> bool {
        self.for_id(id).is_pull_open(id, height, frontier)
    }
}

/// Owns the per-item fetch state: what this node still needs, the parts received so far
/// and who sent them, the pull requests outstanding, and when an item stops being
/// relevant.
// TODO(spice-data-distribution): only receipt proofs route here; witnesses still live
// on the old actor path (#16275).
pub(crate) struct SpiceDataManager<P: DataPolicy = Policies> {
    pull_config: PullConfig,
    encoders: ReedSolomonEncoderCache,
    policies: P,
    /// All tracked items, in any state.
    items: HashMap<DataId, FetchItem>,
    /// Ids of tracked items, indexed by their block's height as captured when first tracked
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,
    /// Highest final execution head reported; `None` until the first report.
    final_execution_head: Option<BlockHeight>,
}

impl<P: DataPolicy> SpiceDataManager<P> {
    pub(crate) fn new(pull_config: PullConfig, data_parts_ratio: f64, policies: P) -> Self {
        Self {
            pull_config,
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

    /// `block` was processed: expires the items at or below `final_execution_head`, tracks
    /// the items needed from `block`, retires the open ones already in the store, and
    /// returns the requests for the rest, grouped by producer. Without a `requester`
    /// nothing is requested and the items stay.
    pub(crate) fn on_new_block(
        &mut self,
        block: &BlockHeader,
        final_execution_head: BlockHeight,
        requester: Option<&AccountId>,
    ) -> Result<Vec<PullRequest>, Error> {
        self.expire_at_or_below(final_execution_head);
        self.track_block(block)?;
        let frontier = self.policies.certified_frontier(block)?;
        self.retire_done_items(&frontier);
        Ok(self.pull_requests(block.height(), &frontier, requester))
    }

    /// The only insert path for received units. Verifies each part against the
    /// commitment and inserts it. A decoding insert checks the decoded data against the
    /// committed hash and the id, settles the commitment either way, and returns matching
    /// data.
    /// Any `Err` outcome is a sender's fault.
    pub(crate) fn on_parts_received(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        total_parts: usize,
    ) -> Result<PartsOutcome, DataManagerError> {
        let Some(item) = self.items.get_mut(id) else {
            return Ok(PartsOutcome::NotWanted);
        };
        item.note_answer_from(sender);
        let encoder = self.encoders.entry(total_parts);
        // TODO(spice-data-distribution): verify every part before inserting any; today
        // the first bad part aborts the loop without undoing earlier inserts (#16275).
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            let verified =
                VerifiedCodedPart::verify(commitment, total_parts, part_ord, part, &merkle_proof)?;
            match item.insert_part(&encoder, id, sender, verified) {
                PartInsertResult::Decoded(data) => {
                    return Ok(PartsOutcome::Decoded(data));
                }
                PartInsertResult::Garbage(error) => {
                    return Err(DataManagerError::GarbageCommitment(error));
                }
                PartInsertResult::ConflictingCommitment => {
                    return Err(DataManagerError::ConflictingCommitment);
                }
                PartInsertResult::Settled => return Ok(PartsOutcome::Settled),
                PartInsertResult::Accepted | PartInsertResult::Duplicate => {}
            }
        }
        Ok(PartsOutcome::Collecting)
    }

    fn remove_item(&mut self, id: &DataId) {
        let Some(item) = self.items.remove(id) else {
            return;
        };
        let ids = self.items_by_height.get_mut(&item.height).expect("tracked item is indexed");
        ids.retain(|indexed| indexed != id);
        if ids.is_empty() {
            self.items_by_height.remove(&item.height);
        }
    }

    /// The chain is past every item at or below the final execution head, so their data
    /// can no longer be applied. Removes them, and [`Self::track_block`] refuses them from
    /// now on.
    fn expire_at_or_below(&mut self, final_execution_head: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(final_execution_head));
        let Some(next_height) = final_execution_head.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = replace(&mut self.items_by_height, live);
        for (bucket_height, ids) in expired {
            for id in ids {
                let item = self.items.get(&id).expect("index entry names a tracked item");
                assert_eq!(item.height, bucket_height, "index entry height matches its item");
                self.items.remove(&id);
            }
        }
    }
}
