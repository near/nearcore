//! Fetch engine and serve side for SPICE data distribution.

mod data_id;
mod encode;
mod item;
mod pending;
mod policy;
mod pull;
mod serve;

pub use data_id::DataId;
pub(crate) use encode::{EncodedData, encode};
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use near_async::time::Instant;
use near_chain::{Block, Error};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, epoch_chunk_producers_for_shard};
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
pub(crate) use pending::PendingPartialData;
use policy::receipt_proof;
pub(crate) use policy::{ServePolicies, ServePolicy};
use pull::Pulls;
pub(crate) use pull::{Config as PullConfig, PullRequest};
pub use serve::MalformedDataRequest;
pub(crate) use serve::{Push, ServeError, ServeOutcome, SpiceDataServer};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::ops::Bound;
use std::sync::Arc;

/// Share of the parts an item is encoded into that suffice to decode it.
pub const DATA_PARTS_RATIO: f64 = 0.6;

/// Max total number of parts a single batched request may carry, and so a single message
/// answering it. Must stay above the largest per-shard chunk-producer set.
// TODO(spice): Bound the outbound bytes per (data_id, requester) too.
pub(crate) const MAX_REQUESTED_PARTS: usize = 256;

#[cfg(test)]
mod tests;

/// What the signed message got wrong. Every variant is attributable to its sender.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SenderFault {
    #[error("message carries no parts")]
    EmptyMessage,
    #[error("message carries more parts than a message may, or than the item has producers")]
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

/// The producers of `id` in the epoch of `epoch_info` and `shard_layout`, in the parts' encoding
/// order. Fetching and serving both take part ordinals from this list.
fn producers_in_epoch(
    epoch_info: &EpochInfo,
    shard_layout: &ShardLayout,
    id: &DataId,
) -> Result<Vec<AccountId>, Error> {
    match id {
        DataId::ReceiptProof { source, .. } => {
            Ok(epoch_chunk_producers_for_shard(epoch_info, shard_layout, source.shard_id)?)
        }
    }
}

/// Owns the per-item fetch state: what this node still needs, the parts received so far
/// and who sent them, the pull requests outstanding, and when an item stops being
/// relevant.
// TODO(spice-data-distribution): only receipt proofs route here; witnesses still live
// on the old actor path (#16275).
pub(crate) struct SpiceDataManager {
    encoders: ReedSolomonEncoderCache,
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    /// All tracked items, in any state.
    items: HashMap<DataId, FetchItem>,
    /// Ids of tracked items, indexed by their block's height as captured when first tracked
    items_by_height: BTreeMap<BlockHeight, HashSet<DataId>>,
    /// Ids of tracked items whose decode was handed to the consumer. Only their data can be
    /// in the store.
    delivered_items: HashSet<DataId>,
    /// Pulls open and in flight, and the config pacing and packing new ones.
    pulls: Pulls,
    /// Highest final execution head reported; `None` until the first report.
    final_execution_head: Option<BlockHeight>,
    /// Highest consensus final head height seen at a trigger.`None` until the first trigger.
    final_height: Option<BlockHeight>,
}

impl SpiceDataManager {
    pub(crate) fn new(
        pull_config: PullConfig,
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self {
            pulls: Pulls::new(pull_config),
            encoders: ReedSolomonEncoderCache::new(DATA_PARTS_RATIO),
            chain_store,
            epoch_manager,
            shard_tracker,
            items: HashMap::new(),
            items_by_height: BTreeMap::new(),
            delivered_items: HashSet::new(),
            final_execution_head: None,
            final_height: None,
        }
    }

    /// The ids of every data kind this node needs from `block`.
    // TODO(spice-data-distribution): each id comes with a tri-state `Interest` once
    // witnesses move onto the engine (#16275).
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        receipt_proof::needed_items(self.epoch_manager.as_ref(), &self.shard_tracker, block)
    }

    /// Whether the item is completed as far as data distribution is concerned.
    fn is_done(&self, id: &DataId) -> bool {
        match id {
            DataId::ReceiptProof { source, to_shard } => {
                receipt_proof::is_done(&self.chain_store.store(), source, *to_shard)
            }
        }
    }

    /// The ids of every data kind that `block` makes pullable, tracked or not.
    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        receipt_proof::made_pullable_by(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            block,
        )
    }

    /// Whether an item for `id` exists, in any state.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, id: &DataId) -> bool {
        self.items.contains_key(id)
    }

    /// Starts tracking every item this node needs from `block` and doesn't already have or
    /// track, then adds the tracked items `block` makes pullable to `pulls`.
    /// Idempotent.
    pub(crate) fn track_block(&mut self, block: &Block) -> Result<(), Error> {
        self.track_block_items(block.header())?;
        self.mark_pullable_by(block)
    }

    /// Adds the tracked items `block` makes pullable to `pulls`.
    fn mark_pullable_by(&mut self, block: &Block) -> Result<(), Error> {
        for id in self.made_pullable_by(block)? {
            if let Some(item) = self.items.get(&id) {
                self.pulls.mark_pullable(id, item.height);
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
        // A block off the canonical chain at or below the final head can never be applied.
        if self.final_height.is_some_and(|final_height| height <= final_height)
            && self.canonical_block_hash(height)? != Some(*block.hash())
        {
            return Ok(());
        }
        let epoch_info = self.epoch_manager.get_epoch_info(block.epoch_id())?;
        let shard_layout = self.epoch_manager.get_shard_layout(block.epoch_id())?;
        let new_items_and_producers = self
            .needed_items(block)?
            .into_iter()
            .filter(|id| !self.items.contains_key(id) && !self.is_done(id))
            .map(|id| {
                let producers = producers_in_epoch(&epoch_info, &shard_layout, &id)?;
                Ok((id, producers))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        for (id, producers) in new_items_and_producers {
            self.items_by_height.entry(height).or_default().insert(id.clone());
            self.items.insert(id, FetchItem::new(height, producers));
        }
        Ok(())
    }

    /// The block was processed at `now`: expires the items at or below the final execution
    /// head, drops the items off the canonical chain at or below the consensus final head,
    /// tracks the items needed from the block, adds the tracked items the block makes
    /// pullable to `pulls`, removes the delivered items already in the store, and
    /// returns the requests for the pullable rest, grouped by producer. A failed chain read is
    /// logged and skips only its own step.
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
        match self.chain_store.final_head() {
            Ok(final_head) => self.drop_forks_at_or_below(final_head.height),
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, "failed to read the final head");
            }
        }
        match self.chain_store.get_block(block_hash) {
            Ok(block) => {
                if let Err(err) = self.track_block_items(block.header()) {
                    tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failed to track the block");
                }
                if let Err(err) = self.mark_pullable_by(&block) {
                    tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failed to add the items the block makes pullable");
                }
            }
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failed to read the block");
            }
        }
        self.remove_done_items();
        self.pulls.to_issue(now, &self.items)
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
    /// untouched, as does an empty message, one with more than `MAX_REQUESTED_PARTS` parts or
    /// more parts than the item has producers, one repeating an ordinal, or one from a sender
    /// that is not a producer of the item. A verified message counts as the sender's answer to
    /// any request outstanding to it. A decoding insert checks the decoded data against the
    /// committed hash and the id, settles the commitment either way, and returns matching data.
    pub(crate) fn on_parts_received(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
    ) -> Result<PartsOutcome, SenderFault> {
        if parts.is_empty() {
            return Err(SenderFault::EmptyMessage);
        }
        if parts.len() > MAX_REQUESTED_PARTS {
            return Err(SenderFault::TooManyParts);
        }
        let Some(item) = self.items.get_mut(id) else {
            return Ok(PartsOutcome::NotWanted);
        };
        let total_parts = item.producers.len();
        if parts.len() > total_parts {
            return Err(SenderFault::TooManyParts);
        }
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
        self.pulls.record_answer(id, sender);
        let encoder = self.encoders.entry(total_parts);
        for part in verified {
            match item.insert_part(&encoder, id, producer_index, part) {
                PartInsertResult::Decoded(data) => {
                    self.delivered_items.insert(id.clone());
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
        let Some(next_height) = height.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = mem::replace(&mut self.items_by_height, live);
        self.pulls.forget_below(next_height);
        for id in expired.into_values().flatten() {
            self.items.remove(&id).expect("index entry names a tracked item");
            self.delivered_items.remove(&id);
        }
    }

    /// Removes the items off the canonical chain at heights newly at or below `final_height`.
    fn drop_forks_at_or_below(&mut self, final_height: BlockHeight) {
        let from = match self.final_height {
            Some(checked) if checked >= final_height => return,
            Some(checked) => Bound::Excluded(checked),
            None => Bound::Unbounded,
        };
        self.final_height = Some(final_height);
        let mut forks = Vec::new();
        for (&height, ids) in self.items_by_height.range((from, Bound::Included(final_height))) {
            let canonical = match self.canonical_block_hash(height) {
                Ok(canonical) => canonical,
                Err(err) => {
                    tracing::error!(target: "spice_data_distribution", ?err, height, "failed to read the canonical block");
                    continue;
                }
            };
            forks.extend(ids.iter().filter(|id| Some(*id.block_hash()) != canonical).cloned());
        }
        for id in forks {
            self.remove_item(&id);
        }
    }

    /// Hash of the canonical block at `height`; `None` if the canonical chain has none there.
    fn canonical_block_hash(&self, height: BlockHeight) -> Result<Option<CryptoHash>, Error> {
        match self.chain_store.get_block_hash_by_height(height) {
            Ok(hash) => Ok(Some(hash)),
            Err(Error::DBNotFoundErr(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Removes the items whose delivered data is in the store.
    fn remove_done_items(&mut self) {
        let done: Vec<DataId> =
            self.delivered_items.iter().filter(|id| self.is_done(id)).cloned().collect();
        for id in done {
            self.remove_item(&id);
        }
    }

    fn remove_item(&mut self, id: &DataId) {
        let Some(item) = self.items.remove(id) else {
            return;
        };
        remove_indexed(&mut self.items_by_height, item.height, id);
        self.pulls.forget(id, item.height);
        self.delivered_items.remove(id);
    }
}

/// Removes `id` from `index` under `height`, dropping the height once it holds no id.
fn remove_indexed(
    index: &mut BTreeMap<BlockHeight, HashSet<DataId>>,
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
