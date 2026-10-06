//! Fetch-engine state for SPICE data distribution.

mod data_id;
mod fetchable;
mod item;
mod pending;
mod pull;

pub use data_id::DataId;
pub(crate) use fetchable::DataPolicy;
use fetchable::ReceiptProofPolicy;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use near_async::time::Instant;
use near_chain::{Block, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::chain_store::ChainStoreAdapter;
pub(crate) use pending::PendingPartialData;
pub(crate) use pull::{PullConfig, PullRequest};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::mem;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// What the signed message got wrong. Every variant is attributable to its sender.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SenderFault {
    #[error("message carries no parts")]
    EmptyMessage,
    #[error("message carries more parts than the commitment has")]
    TooManyParts,
    #[error("message carries the same part ordinal twice")]
    DuplicateOrdinal,
    #[error("commitment settled as garbage: {0}")]
    GarbageCommitment(AssembledDataError),
    #[error("part merkle proof does not verify against the commitment root")]
    InvalidMerkleProof,
    #[error("sender already backed another commitment")]
    ConflictingCommitment,
    #[error("sender is not a producer of the item")]
    NotAProducer,
}

/// Outcome of accepting parts for an item.
#[must_use]
#[derive(Debug)]
pub(crate) enum PartsOutcome {
    /// Parts accepted; no commitment decoded.
    Collecting,
    /// A commitment decoded to this data, which matches the committed hash and the id.
    Decoded(SpiceData),
    /// The commitment was already settled, to data or to garbage; a late or re-sent part.
    AlreadySettled,
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
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error> {
        self.receipt_proofs.needed_items(block)
    }

    fn is_done(&self, id: &DataId) -> bool {
        self.for_id(id).is_done(id)
    }

    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        self.receipt_proofs.made_pullable_by(block)
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
    chain_store: ChainStoreAdapter,
    policies: P,
    /// All tracked items, in any state.
    items: HashMap<DataId, FetchItem>,
    /// Ids of tracked items, indexed by their block's height as captured when first tracked
    items_by_height: BTreeMap<BlockHeight, BTreeSet<DataId>>,
    /// Ids of tracked items that may be pulled, by their block's height.
    pullable: BTreeMap<BlockHeight, BTreeSet<DataId>>,
    // TODO(spice-data-distribution): keep these only for blocks whose tracking failed, with
    // the orphan pool.
    /// Ids made pullable before they were tracked, with the height of the block that made
    /// them pullable; tracking one makes it pullable at once.
    pullable_before_tracking: HashMap<DataId, BlockHeight>,
    /// Ids of tracked items whose decode was handed to the consumer. Only their data can be
    /// in the store.
    delivered: HashSet<DataId>,
    /// Highest final execution head reported; `None` until the first report.
    final_execution_head: Option<BlockHeight>,
}

impl<P: DataPolicy> SpiceDataManager<P> {
    pub(crate) fn new(
        pull_config: PullConfig,
        data_parts_ratio: f64,
        chain_store: ChainStoreAdapter,
        policies: P,
    ) -> Self {
        Self {
            pull_config,
            encoders: ReedSolomonEncoderCache::new(data_parts_ratio),
            chain_store,
            policies,
            items: HashMap::new(),
            items_by_height: BTreeMap::new(),
            pullable: BTreeMap::new(),
            pullable_before_tracking: HashMap::new(),
            delivered: HashSet::new(),
            final_execution_head: None,
        }
    }

    /// Whether an item for `id` exists, in any state.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, id: &DataId) -> bool {
        self.items.contains_key(id)
    }

    /// Starts tracking every item this node needs from `block` and doesn't already have or
    /// track, then marks the tracked items `block` makes pullable. Idempotent.
    pub(crate) fn track_block(&mut self, block: &Block) -> Result<(), Error> {
        self.track_block_items(block.header())?;
        self.mark_pullable_by(block)
    }

    /// Marks the items `block` makes pullable; an item not tracked yet is marked when tracked.
    fn mark_pullable_by(&mut self, block: &Block) -> Result<(), Error> {
        let height = block.header().height();
        for id in self.policies.made_pullable_by(block)? {
            match self.items.get(&id) {
                Some(item) => {
                    self.pullable.entry(item.height).or_default().insert(id);
                }
                None => {
                    let marked_at = self.pullable_before_tracking.entry(id).or_insert(height);
                    *marked_at = (*marked_at).max(height);
                }
            }
        }
        Ok(())
    }

    // TODO(spice-data-distribution): Fold back into `track_block` once data for a block not
    // yet processed is parked until the block is processed; `on_parts_received` then drops
    // parts for untracked items without reading the chain.
    /// Starts tracking every item this node needs from `block` and doesn't already have or
    /// track. Idempotent.
    pub(crate) fn track_block_items(&mut self, block: &BlockHeader) -> Result<(), Error> {
        let height = block.height();
        // The chain is past the block, so its data can never be applied.
        if self.final_execution_head.is_some_and(|head| height <= head) {
            return Ok(());
        }
        for (id, producers) in self.policies.needed_items(block)? {
            if self.items.contains_key(&id) || self.policies.is_done(&id) {
                continue;
            }
            self.items_by_height.entry(height).or_default().insert(id.clone());
            if self.pullable_before_tracking.remove(&id).is_some() {
                self.pullable.entry(height).or_default().insert(id.clone());
            }
            self.items.insert(id, FetchItem::new(height, producers));
        }
        Ok(())
    }

    /// The block was processed at `now`: expires the items at or below the final execution
    /// head, tracks the items needed from the block, marks the items the block makes pullable,
    /// removes the delivered items already in the store, and returns the requests for the
    /// pullable rest, grouped by producer. A failed chain read is logged and skips only its
    /// own step.
    pub(crate) fn on_block_processed(
        &mut self,
        block_hash: &CryptoHash,
        now: Instant,
    ) -> Vec<PullRequest> {
        match self.final_execution_head_height() {
            Ok(height) => self.expire_at_or_below(height),
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, "failed to read the final execution head");
            }
        }
        match self.chain_store.get_block(block_hash) {
            Ok(block) => {
                if let Err(err) = self.track_block_items(block.header()) {
                    tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failed to track the block");
                }
                if let Err(err) = self.mark_pullable_by(&block) {
                    tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failed to mark the items the block makes pullable");
                }
            }
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failed to read the block");
            }
        }
        self.remove_done_items();
        self.pull_requests(now)
    }

    /// Height of the final execution head; the genesis height before the first one is recorded.
    fn final_execution_head_height(&self) -> Result<BlockHeight, Error> {
        match self.chain_store.spice_final_execution_head() {
            Ok(head) => Ok(head.height),
            Err(Error::DBNotFoundErr(_)) => Ok(self.chain_store.get_genesis_height()),
            Err(err) => Err(err),
        }
    }

    /// Handles incoming parts: verifies every part's proof against the commitment before
    /// inserting any; a part failing its proof rejects the whole message and leaves the item
    /// untouched, as does an empty message, one with more than `total_parts` parts, one
    /// repeating an ordinal, or one from a sender that is not a producer of the item. A verified message counts as the sender's answer to any request
    /// outstanding to it. A decoding insert checks the decoded data against the committed hash
    /// and the id, settles the commitment either way, and returns matching data.
    pub(crate) fn on_parts_received(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        total_parts: usize,
    ) -> Result<PartsOutcome, SenderFault> {
        if parts.is_empty() {
            return Err(SenderFault::EmptyMessage);
        }
        if parts.len() > total_parts {
            return Err(SenderFault::TooManyParts);
        }
        let Some(item) = self.items.get_mut(id) else {
            return Ok(PartsOutcome::NotWanted);
        };
        let Some(producer_index) = item.producers.iter().position(|(account, _)| account == sender)
        else {
            return Err(SenderFault::NotAProducer);
        };
        let mut ordinals = HashSet::with_capacity(parts.len());
        let mut verified = Vec::with_capacity(parts.len());
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            if !ordinals.insert(part_ord) {
                return Err(SenderFault::DuplicateOrdinal);
            }
            let part =
                VerifiedCodedPart::verify(commitment, total_parts, part_ord, part, &merkle_proof)
                    .ok_or(SenderFault::InvalidMerkleProof)?;
            verified.push(part);
        }
        item.note_pull_response(sender);
        let encoder = self.encoders.entry(total_parts);
        for part in verified {
            match item.insert_part(&encoder, id, producer_index, part) {
                PartInsertResult::Decoded(data) => {
                    self.delivered.insert(id.clone());
                    return Ok(PartsOutcome::Decoded(data));
                }
                PartInsertResult::Garbage(error) => {
                    return Err(SenderFault::GarbageCommitment(error));
                }
                PartInsertResult::ConflictingCommitment => {
                    return Err(SenderFault::ConflictingCommitment);
                }
                PartInsertResult::AlreadySettled => return Ok(PartsOutcome::AlreadySettled),
                PartInsertResult::Accepted | PartInsertResult::Duplicate => {}
            }
        }
        Ok(PartsOutcome::Collecting)
    }

    /// Stops tracking items at or below `height`.
    fn expire_at_or_below(&mut self, height: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(height));
        self.pullable_before_tracking.retain(|_, marked_at| *marked_at > height);
        let Some(next_height) = height.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = mem::replace(&mut self.items_by_height, live);
        self.pullable = self.pullable.split_off(&next_height);
        for id in expired.into_values().flatten() {
            self.items.remove(&id).expect("index entry names a tracked item");
            self.delivered.remove(&id);
        }
    }

    /// Removes the items whose delivered data is in the store.
    fn remove_done_items(&mut self) {
        let done: Vec<DataId> =
            self.delivered.iter().filter(|id| self.policies.is_done(id)).cloned().collect();
        for id in done {
            self.remove_item(&id);
        }
    }

    fn remove_item(&mut self, id: &DataId) {
        let Some(item) = self.items.remove(id) else {
            return;
        };
        remove_indexed(&mut self.items_by_height, item.height, id);
        remove_indexed(&mut self.pullable, item.height, id);
        self.delivered.remove(id);
    }
}

/// Removes `id` from `index` under `height`, dropping the height once it holds no id.
fn remove_indexed(
    index: &mut BTreeMap<BlockHeight, BTreeSet<DataId>>,
    height: BlockHeight,
    id: &DataId,
) {
    if let Some(ids) = index.get_mut(&height) {
        ids.remove(id);
        if ids.is_empty() {
            index.remove(&height);
        }
    }
}
