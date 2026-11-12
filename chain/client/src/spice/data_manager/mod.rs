//! Fetch engine and serve side for SPICE data distribution.

mod data_id;
mod encode;
mod heights;
mod item;
mod orphans;
mod policy;
mod pull;
mod sender;
mod serve;

use crate::spice::chunk_executor_actor::may_still_apply_receipts;
pub use data_id::{DataId, ReceiptProofId};
pub(crate) use encode::{EncodedData, encode};
use heights::ChainHeights;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use lru::LruCache;
use near_async::time::Instant;
use near_chain::{Block, Error};
use near_crypto::PublicKey;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, epoch_chunk_producers_for_shard};
use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataPart, SpicePartialData, SpiceVerifiedPartialData,
};
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use orphans::ParkedParts;
pub(crate) use policy::ServePolicies;
use policy::receipt_proof;
use pull::Pulls;
pub(crate) use pull::{Config as PullConfig, PullRequest};
use sender::AuthenticatedSender;
pub use serve::MalformedDataRequest;
pub(crate) use serve::{
    CheckedDataRequest, Push, RequestedItem, ServeError, ServeOutcome, SpiceDataServer,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::Arc;

/// Share of the parts an item is encoded into that suffice to decode it.
pub const DATA_PARTS_RATIO: f64 = 0.6;

/// Max total number of parts a single batched request (and response) may carry.
/// For performance better stay above the largest per-shard chunk-producer set.
// TODO(spice): Bound the outbound bytes per (data_id, requester) too.
pub(crate) const MAX_REQUESTED_PARTS: usize = 128;

/// Max number of entries `(data_id, ordinals)` a single batched request may carry.
/// This caps the encodes (CPU).
pub(crate) const MAX_REQUESTED_DATA_IDS: usize = 32;

/// Blocks parts are parked for.
pub(crate) const MAX_ORPHAN_BLOCKS: NonZeroUsize = NonZeroUsize::new(10).unwrap();

#[cfg(test)]
mod tests;

/// What the message got wrong.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SenderFault {
    #[error("message carries no parts")]
    EmptyMessage,
    #[error("message carries more parts than allowed")]
    TooManyParts,
    #[error("message carries the same part ordinal twice")]
    DuplicateOrdinal,
    #[error("commitment settled as garbage: {0}")]
    GarbageCommitment(AssembledDataError),
    #[error("part merkle proof does not verify against the commitment root")]
    InvalidMerkleProof,
    #[error("sender already sent parts under another commitment")]
    ConflictingCommitment,
    #[error("sender is not a producer of the item")]
    NotAProducer,
    #[error("message is not signed with a key the sender may use for the item")]
    InvalidSignature,
}

/// Outcome of accepting parts for an item.
#[must_use]
#[derive(Debug)]
pub(crate) enum PartsOutcome {
    /// Parts accepted; no commitment decoded.
    Collecting,
    /// A commitment decoded to this data, which matches the committed hash and the id.
    Decoded(SpiceData),
    /// The item is done, or the commitment was already settled, to data or to garbage; The parts
    /// were probably OK, but we don't need them any more.
    AlreadySettled,
    /// The part that we do not need. Too old or shouldn't ended up here at all.
    NotWanted,
}

/// The producers of `id` in the epoch of `epoch_info` and `shard_layout`, in the parts' encoding
/// order. Fetching and serving both take part ordinals from this list.
fn producers_in_epoch(
    epoch_info: &EpochInfo,
    shard_layout: &ShardLayout,
    id: &DataId,
) -> Result<Vec<AccountId>, Error> {
    Ok(epoch_chunk_producers_for_shard(epoch_info, shard_layout, id.producing_chunk().shard_id)?)
}

/// What a processed block yields.
#[must_use]
#[derive(Debug, Default)]
pub(crate) struct ProcessedBlockOutput {
    /// Requests for the pullable items, grouped by producer.
    pub(crate) pull_requests: Vec<PullRequest>,
    /// Data decoded from the parts parked for the block.
    pub(crate) decoded: Vec<(DataId, SpiceData)>,
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
    /// Tracked items, indexed by their block's height as captured when first tracked
    items_by_height: BTreeMap<BlockHeight, HashSet<DataId>>,
    /// Items whose decode was handed to the consumer. Only their data can be "done".
    delivered_items: HashSet<DataId>,
    /// Items that are done. Their parts are settled.
    done_items: HashSet<DataId>,
    /// Handles pull recovery.
    pulls: Pulls,
    /// Tracks the heights observed.
    heights: ChainHeights,
    /// Parts for blocks not processed yet, by block hash; the most recently parked blocks.
    // TODO(spice-data-distribution): bound the data parked per sender.
    orphans: LruCache<CryptoHash, Vec<ParkedParts>>,
    /// Processed blocks above the final execution head, with their heights.
    processed_blocks: HashMap<CryptoHash, BlockHeight>,
    /// Epoch of the final execution head. The items kept below that head wait on it to change, see
    /// [`Self::is_past`].
    final_execution_epoch: Option<EpochId>,
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
            done_items: HashSet::new(),
            heights: ChainHeights::default(),
            orphans: LruCache::new(MAX_ORPHAN_BLOCKS),
            processed_blocks: HashMap::new(),
            final_execution_epoch: None,
        }
    }

    /// The ids of every data kind this node needs from `block`.
    // TODO(spice-data-distribution): each id comes with a tri-state `Interest` once
    // witnesses move onto the engine (#16275).
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        receipt_proof::needed_items(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            block,
        )
    }

    /// Whether the item is completed as far as data distribution is concerned.
    fn is_done(&self, id: &DataId) -> bool {
        match id {
            DataId::ReceiptProof(id) => {
                receipt_proof::is_done(&self.chain_store.store(), &id.source, id.to_shard)
            }
            DataId::Witness(_) => unreachable!("the data manager tracks no witnesses"),
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

    /// The keys the producer `account_id` of `item` may sign its parts with.
    fn signing_keys(&self, item: &FetchItem, account_id: &AccountId) -> Vec<PublicKey> {
        self.epoch_manager
            .get_validator_signing_keys(&item.epoch_id, &item.next_epoch_id, account_id)
            .expect("a producer of a tracked item should be a validator in its block's epoch")
    }

    /// Index of the producer of `item` that `sender` is, if it signed with one of its keys.
    fn producer_signed_by(
        &self,
        item: &FetchItem,
        sender: &AuthenticatedSender,
    ) -> Result<usize, SenderFault> {
        let index = item.producer_index(sender.account_id()).ok_or(SenderFault::NotAProducer)?;
        if !self.signing_keys(item, sender.account_id()).contains(sender.key()) {
            return Err(SenderFault::InvalidSignature);
        }
        Ok(index)
    }

    /// Whether an item for `id` exists, in any state.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, id: &DataId) -> bool {
        self.items.contains_key(id)
    }

    /// Tracks the items a processed block needs or makes pullable. Returns the data decoded from
    /// parts that waited for it. Idempotent. Fails only on a read of the block's stored chain or
    /// epoch data.
    fn track_block(&mut self, block: &Block) -> Result<Vec<(DataId, SpiceData)>, Error> {
        self.track_needed_items(block)?;
        self.mark_pullable_by(block)?;
        Ok(self.release_parked(block.header()))
    }

    /// Starts tracking every item this node needs from `block` and doesn't already have or track.
    fn track_needed_items(&mut self, block: &Block) -> Result<(), Error> {
        let block = block.header();
        let height = block.height();
        // A block off the canonical chain at or below the final head can never be applied.
        if self.heights.at_or_below_final_head(height)
            && self.canonical_block_hash(height)? != Some(*block.hash())
        {
            return Ok(());
        }
        let epoch_info = self.epoch_manager.get_epoch_info(block.epoch_id())?;
        let shard_layout = self.epoch_manager.get_shard_layout(block.epoch_id())?;
        let final_execution_head = self.final_execution_head()?;
        let new_items_and_producers = self
            .needed_items(block)?
            .into_iter()
            .filter(|id| {
                !self.items.contains_key(id)
                    && !self.is_past(id, height, final_execution_head.as_deref())
                    && !self.is_done(id)
            })
            .map(|id| {
                let producers = producers_in_epoch(&epoch_info, &shard_layout, &id)?;
                Ok((id, producers))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        for (id, producers) in new_items_and_producers {
            self.items_by_height.entry(height).or_default().insert(id.clone());
            let item = FetchItem::new(height, *block.epoch_id(), *block.next_epoch_id(), producers);
            self.items.insert(id, item);
        }
        Ok(())
    }

    /// Adds the tracked items `block` makes pullable to `pulls`.
    fn mark_pullable_by(&mut self, block: &Block) -> Result<(), Error> {
        for id in self.made_pullable_by(block)? {
            if self.done_items.contains(&id) {
                continue;
            }
            if let Some(item) = self.items.get(&id) {
                self.pulls.mark_pullable(id, item.height);
            }
        }
        Ok(())
    }

    /// Handles a processed block. Returns the pull requests to send now and the data decoded
    /// from parts that waited for the block.
    pub(crate) fn on_block_processed(
        &mut self,
        block_hash: &CryptoHash,
        now: Instant,
    ) -> ProcessedBlockOutput {
        match self.final_execution_head() {
            Ok(final_execution_head) => {
                self.on_final_execution_head(final_execution_head.as_deref())
            }
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, "failed to read the final execution head");
            }
        }
        match self.chain_store.final_head() {
            Ok(final_head) => self.on_final_head(final_head.height),
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, "failed to read the final head");
            }
        }
        let decoded = match self.chain_store.get_block(block_hash) {
            Ok(block) => self
                .track_block(&block)
                .expect("chain and epoch data of a processed block should be readable"),
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failed to read the block");
                Vec::new()
            }
        };
        self.handle_done_items();
        ProcessedBlockOutput { pull_requests: self.pulls.to_issue(now, &self.items), decoded }
    }

    /// The final execution head; `None` before the first one is recorded.
    fn final_execution_head(&self) -> Result<Option<Arc<Tip>>, Error> {
        match self.chain_store.spice_final_execution_head() {
            Ok(head) => Ok(Some(head)),
            Err(Error::DBNotFoundErr(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Handles a signed message of parts. Parts for a block not processed yet wait for it.
    /// Otherwise the whole message is checked before any part is inserted, so a bad message
    /// leaves the item untouched. A checked message counts as the sender's answer to its pending
    /// request. Decoded data is checked against the commitment and the id before it is returned.
    pub(crate) fn on_parts_received(
        &mut self,
        data: SpicePartialData,
    ) -> Result<PartsOutcome, SenderFault> {
        if data.num_parts() == 0 {
            return Err(SenderFault::EmptyMessage);
        }
        if data.num_parts() > MAX_REQUESTED_PARTS {
            return Err(SenderFault::TooManyParts);
        }
        let id = DataId::from(data.id());
        let Some(item) = self.items.get(&id) else {
            return self.handle_untracked_item(id, data);
        };
        let producer_index = item.producer_index(data.sender()).ok_or(SenderFault::NotAProducer)?;
        let keys = self.signing_keys(item, data.sender());
        let (_, SpiceVerifiedPartialData { commitment, parts, .. }) =
            AuthenticatedSender::verify(data, &keys).ok_or(SenderFault::InvalidSignature)?;
        self.add_parts(&id, producer_index, &commitment, parts)
    }

    /// Adds the parts the producer at `producer_index` signed to the tracked item `id`.
    fn add_parts(
        &mut self,
        id: &DataId,
        producer_index: usize,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
    ) -> Result<PartsOutcome, SenderFault> {
        let item = self.items.get_mut(id).expect("parts are added only to a tracked item");
        let total_parts = item.producers.len();
        if parts.len() > total_parts {
            return Err(SenderFault::TooManyParts);
        }
        if self.done_items.contains(id) {
            return Ok(PartsOutcome::AlreadySettled);
        }
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
        self.pulls.record_answer(id, &item.producers[producer_index].account_id);
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

    /// Stops tracking the items at or below the final execution head that are done or past, see
    /// [`Self::is_past`]. Before the first head is recorded, the genesis height stands in for it.
    /// Only the heights the head newly covers are looked at, all of them once its epoch changes.
    fn on_final_execution_head(&mut self, final_execution_head: Option<&Tip>) {
        let height = final_execution_head
            .map_or_else(|| self.chain_store.get_genesis_height(), |head| head.height);
        let Some(mut from) = self.heights.update_final_execution_head(height) else {
            return;
        };
        self.processed_blocks.retain(|_, block_height| *block_height > height);
        let epoch_id = final_execution_head.map(|head| head.epoch_id);
        if mem::replace(&mut self.final_execution_epoch, epoch_id) != epoch_id {
            from = Bound::Unbounded;
        }
        let expired: Vec<DataId> = self
            .items_by_height
            .range((from, Bound::Included(height)))
            .flat_map(|(&item_height, ids)| ids.iter().map(move |id| (item_height, id)))
            .filter(|&(item_height, id)| {
                self.done_items.contains(id)
                    || self.is_past(id, item_height, final_execution_head)
                    || self.is_done(id)
            })
            .map(|(_, id)| id.clone())
            .collect();
        for id in expired {
            self.remove_item(&id);
        }
    }

    /// Whether this node no longer needs `id`, of the block at `height`: it either applied the
    /// data or never will, see [`may_still_apply_receipts`]. Without a recorded final execution
    /// head, the height decides.
    fn is_past(
        &self,
        id: &DataId,
        height: BlockHeight,
        final_execution_head: Option<&Tip>,
    ) -> bool {
        let Some(final_execution_head) = final_execution_head else {
            return self.heights.at_or_below_final_execution(height);
        };
        match id {
            DataId::ReceiptProof(ReceiptProofId { source, to_shard }) => !may_still_apply_receipts(
                self.epoch_manager.as_ref(),
                &self.shard_tracker,
                final_execution_head,
                &source.block_hash,
                height,
                *to_shard,
            ),
            // FIXME(spice-state-sync): decide what to do in this case
            DataId::Witness(_spice_chunk_id) => todo!(),
        }
    }

    /// Removes the items off the canonical chain at heights newly at or below the final head at
    /// `final_height`.
    fn on_final_head(&mut self, final_height: BlockHeight) {
        let Some(from) = self.heights.update_final_head(final_height) else {
            return;
        };
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

    /// Marks the delivered items that are done, and stops pulling them. An item at or below the
    /// final execution head was kept only until it is done, so it is dropped instead.
    fn handle_done_items(&mut self) {
        let done: Vec<DataId> =
            self.delivered_items.iter().filter(|id| self.is_done(id)).cloned().collect();
        for id in done {
            let height = self.items[&id].height;
            if self.heights.at_or_below_final_execution(height) {
                self.remove_item(&id);
                continue;
            }
            self.delivered_items.remove(&id);
            self.pulls.forget(&id, height);
            self.done_items.insert(id);
        }
    }

    fn remove_item(&mut self, id: &DataId) {
        let Some(item) = self.items.remove(id) else {
            return;
        };
        remove_indexed(&mut self.items_by_height, item.height, id);
        self.pulls.forget(id, item.height);
        self.delivered_items.remove(id);
        self.done_items.remove(id);
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
