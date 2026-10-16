//! Fetch-engine state for SPICE data distribution.

mod data_id;
mod encode;
mod fetchable;
mod item;
mod pending;
mod pull;
mod serve;

pub use data_id::DataId;
pub(crate) use encode::{EncodedData, encode};
pub(crate) use fetchable::{DataPolicy, Policies};
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use near_async::time::Instant;
use near_chain::{Block, Error};
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::{ReedSolomonEncoder, ReedSolomonEncoderCache};
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::chain_store::ChainStoreAdapter;
pub(crate) use pending::PendingPartialData;
use pull::Pulls;
pub(crate) use pull::{PullConfig, PullRequest};
pub(crate) use serve::{Push, ServeError, Served};
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
    pullable_items: BTreeMap<BlockHeight, BTreeSet<DataId>>,
    /// Ids of tracked items whose decode was handed to the consumer. Only their data can be
    /// in the store.
    delivered_items: HashSet<DataId>,
    /// Pulls in flight, and per producer the pullable items it is a source for.
    pulls: Pulls,
    /// Highest final execution head reported; `None` until the first report.
    final_execution_head: Option<BlockHeight>,
    /// Items the pull traversal visited, over all triggers.
    #[cfg(test)]
    items_visited_by_traversal: usize,
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
            pullable_items: BTreeMap::new(),
            delivered_items: HashSet::new(),
            pulls: Pulls::default(),
            final_execution_head: None,
            #[cfg(test)]
            items_visited_by_traversal: 0,
        }
    }

    /// Whether an item for `id` exists, in any state.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, id: &DataId) -> bool {
        self.items.contains_key(id)
    }

    /// Starts tracking every item this node needs from `block` and doesn't already have or
    /// track, then adds the tracked items `block` makes pullable to `pullable_items`.
    /// Idempotent.
    pub(crate) fn track_block(&mut self, block: &Block) -> Result<(), Error> {
        self.track_block_items(block.header())?;
        self.mark_pullable_by(block)
    }

    /// Adds the tracked items `block` makes pullable to `pullable_items`.
    fn mark_pullable_by(&mut self, block: &Block) -> Result<(), Error> {
        for id in self.policies.made_pullable_by(block)? {
            if let Some(item) = self.items.get(&id) {
                if self.pullable_items.entry(item.height).or_default().insert(id) {
                    self.pulls.add_sources(item.pull_sources());
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
            self.items.insert(id, FetchItem::new(height, producers));
        }
        Ok(())
    }

    /// The block was processed at `now`: expires the items at or below the final execution
    /// head, tracks the items needed from the block, adds the tracked items the block makes
    /// pullable to `pullable_items`, removes the delivered items already in the store, and
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
        self.pulls_to_issue(now)
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
        self.pulls.clear(id, sender);
        let is_pullable = self.pullable_items.get(&item.height).is_some_and(|ids| ids.contains(id));
        let sources_before = if is_pullable { item.pull_sources() } else { HashSet::new() };
        let encoder = self.encoders.entry(total_parts);
        let outcome = insert_verified_parts(item, &encoder, id, producer_index, verified);
        if is_pullable {
            let sources_after = item.pull_sources();
            let dropped: Vec<AccountId> =
                sources_before.difference(&sources_after).cloned().collect();
            for producer in &dropped {
                self.pulls.clear(id, producer);
            }
            self.pulls.remove_sources(dropped);
        }
        if let Ok(PartsOutcome::Decoded(_)) = &outcome {
            self.delivered_items.insert(id.clone());
        }
        outcome
    }

    /// Stops tracking items at or below `height`.
    fn expire_at_or_below(&mut self, height: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(height));
        let Some(next_height) = height.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = mem::replace(&mut self.items_by_height, live);
        let live_pullable = self.pullable_items.split_off(&next_height);
        let expired_pullable: HashSet<DataId> =
            mem::replace(&mut self.pullable_items, live_pullable).into_values().flatten().collect();
        for id in expired.into_values().flatten() {
            let item = self.items.remove(&id).expect("index entry names a tracked item");
            self.release_pulls(&id, &item, expired_pullable.contains(&id));
            self.delivered_items.remove(&id);
        }
    }

    /// Removes the items whose delivered data is in the store.
    fn remove_done_items(&mut self) {
        let done: Vec<DataId> =
            self.delivered_items.iter().filter(|id| self.policies.is_done(id)).cloned().collect();
        for id in done {
            self.remove_item(&id);
        }
    }

    fn remove_item(&mut self, id: &DataId) {
        let Some(item) = self.items.remove(id) else {
            return;
        };
        remove_indexed(&mut self.items_by_height, item.height, id);
        let was_pullable = remove_indexed(&mut self.pullable_items, item.height, id);
        self.release_pulls(id, &item, was_pullable);
        self.delivered_items.remove(id);
    }

    /// Forgets the requests in flight for `item`, just removed, and, if it was pullable, its
    /// sources.
    fn release_pulls(&mut self, id: &DataId, item: &FetchItem, was_pullable: bool) {
        for (producer, _) in &item.producers {
            self.pulls.clear(id, producer);
        }
        if was_pullable {
            self.pulls.remove_sources(item.pull_sources());
        }
    }
}

/// Inserts `verified` parts from the producer at `producer_index` until one settles a
/// commitment or fails.
fn insert_verified_parts(
    item: &mut FetchItem,
    encoder: &Arc<ReedSolomonEncoder>,
    id: &DataId,
    producer_index: usize,
    verified: Vec<VerifiedCodedPart>,
) -> Result<PartsOutcome, SenderFault> {
    for part in verified {
        match item.insert_part(encoder, id, producer_index, part) {
            PartInsertResult::Decoded(data) => return Ok(PartsOutcome::Decoded(data)),
            PartInsertResult::Garbage(error) => return Err(SenderFault::GarbageCommitment(error)),
            PartInsertResult::ConflictingCommitment => {
                return Err(SenderFault::ConflictingCommitment);
            }
            PartInsertResult::AlreadySettled => return Ok(PartsOutcome::AlreadySettled),
            PartInsertResult::Accepted | PartInsertResult::Duplicate => {}
        }
    }
    Ok(PartsOutcome::Collecting)
}

/// Removes `id` from `index` under `height`, dropping the height once it holds no id.
/// Returns whether `id` was there.
fn remove_indexed(
    index: &mut BTreeMap<BlockHeight, BTreeSet<DataId>>,
    height: BlockHeight,
    id: &DataId,
) -> bool {
    let Some(ids) = index.get_mut(&height) else {
        return false;
    };
    let removed = ids.remove(id);
    if ids.is_empty() {
        index.remove(&height);
    }
    removed
}
