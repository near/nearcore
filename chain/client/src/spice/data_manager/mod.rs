//! Fetch engine and serve side for SPICE data distribution.

mod data_id;
mod encode;
mod heights;
mod item;
mod items;
mod orphans;
mod policy;
mod pull;
mod sender;
mod serve;

pub use data_id::{DataId, ReceiptProofId};
pub(crate) use encode::{EncodedData, encode};
use heights::ChainHeights;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use items::Items;
use lru::LruCache;
use near_async::time::Instant;
use near_chain::{Block, Error};
use near_chain_configs::MutableValidatorSigner;
use near_crypto::PublicKey;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, epoch_chunk_producers_for_shard};
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataPart, SpicePartialData, SpiceVerifiedPartialData,
};
use near_primitives::types::{AccountId, BlockHeight, ShardId, SpiceChunkId};
use near_primitives::utils::get_execution_results_key;
use near_store::DBCol;
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

/// What an event handled by the manager yields.
#[must_use]
#[derive(Debug, Default)]
pub(crate) struct DataManagerOutput {
    /// Requests for the pullable items, grouped by producer.
    pub(crate) pull_requests: Vec<PullRequest>,
    /// Data decoded from parts parked for a block.
    pub(crate) decoded: Vec<(DataId, SpiceData)>,
}

/// A change in which receipt proofs into one shard the chunk executor needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NeedChange {
    /// The proofs from these chunks are missing.
    Needed(Vec<SpiceChunkId>),
    /// No proof from a block below this height is needed.
    NoLongerNeededBelow(BlockHeight),
    /// No proof is needed: the shard is no longer applied.
    NoLongerNeeded,
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
    validator_signer: MutableValidatorSigner,
    /// All tracked items, in any state.
    items: Items,
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
    /// Processed blocks at or above the final execution head, with their heights.
    processed_blocks: HashMap<CryptoHash, BlockHeight>,
}

impl SpiceDataManager {
    pub(crate) fn new(
        pull_config: PullConfig,
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        validator_signer: MutableValidatorSigner,
    ) -> Self {
        Self {
            pulls: Pulls::new(pull_config),
            encoders: ReedSolomonEncoderCache::new(DATA_PARTS_RATIO),
            chain_store,
            epoch_manager,
            shard_tracker,
            validator_signer,
            items: Items::default(),
            delivered_items: HashSet::new(),
            done_items: HashSet::new(),
            heights: ChainHeights::default(),
            orphans: LruCache::new(MAX_ORPHAN_BLOCKS),
            processed_blocks: HashMap::new(),
        }
    }

    fn me(&self) -> Option<AccountId> {
        self.validator_signer.get().map(|signer| signer.validator_id().clone())
    }

    /// The ids of every data kind pushed to this node from `block`.
    // TODO(spice-data-distribution): each id comes with a tri-state `Interest` once
    // witnesses move onto the engine (#16275).
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        receipt_proof::needed_items(
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            self.me().as_ref(),
            block,
        )
    }

    /// Whether `id` is pushed to this node.
    fn is_pushed_to_me(&self, id: &DataId) -> Result<bool, Error> {
        match id {
            DataId::ReceiptProof(ReceiptProofId { source, to_shard }) => {
                let Some(me) = self.me() else {
                    return Ok(false);
                };
                let recipients = receipt_proof::recipients(
                    self.epoch_manager.as_ref(),
                    &source.block_hash,
                    *to_shard,
                )?;
                Ok(recipients.contains(&me))
            }
            DataId::Witness(_) => Ok(true),
        }
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

    /// The ids pushed to this node that `block` makes pullable, tracked or not.
    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        receipt_proof::made_pullable_by(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            self.me().as_ref(),
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
        self.items.contains(id)
    }

    /// Tracks the items a processed block needs or makes pullable. Returns the data decoded from
    /// parts that waited for it. Idempotent. Fails only on a read of the block's stored chain or
    /// epoch data.
    fn track_block(&mut self, block: &Block) -> Result<Vec<(DataId, SpiceData)>, Error> {
        self.track_needed_items(block)?;
        self.mark_pullable_by(block)?;
        Ok(self.release_parked(block.header()))
    }

    /// Starts tracking every item pushed to this node from `block` that it doesn't already have or
    /// track.
    fn track_needed_items(&mut self, block: &Block) -> Result<(), Error> {
        let block = block.header();
        let height = block.height();
        if self.is_fork_at_or_below_final_head(block)? || self.heights.below_final_execution(height)
        {
            return Ok(());
        }
        let new_items = self
            .needed_items(block)?
            .into_iter()
            .filter(|id| !self.items.contains(id) && !self.is_done(id))
            .map(|id| Ok((self.new_item(block, &id)?, id)))
            .collect::<Result<Vec<_>, Error>>()?;
        for (item, id) in new_items {
            self.items.insert_push(id, item);
        }
        Ok(())
    }

    /// Whether `block` is off the canonical chain at or below the final head, so it can never be
    /// applied.
    fn is_fork_at_or_below_final_head(&self, block: &BlockHeader) -> Result<bool, Error> {
        Ok(self.heights.at_or_below_final_head(block.height())
            && self.canonical_block_hash(block.height())? != Some(*block.hash()))
    }

    /// A new item for `id` from `block`.
    fn new_item(&self, block: &BlockHeader, id: &DataId) -> Result<FetchItem, Error> {
        let epoch_info = self.epoch_manager.get_epoch_info(block.epoch_id())?;
        let shard_layout = self.epoch_manager.get_shard_layout(block.epoch_id())?;
        let producers = producers_in_epoch(&epoch_info, &shard_layout, id)?;
        Ok(FetchItem::new(block.height(), *block.epoch_id(), *block.next_epoch_id(), producers))
    }

    /// Applies a change in the receipt proofs into `to_shard` the chunk executor needs, and stops
    /// pulling the delivered items now saved. Returns the pull requests to send now.
    pub(crate) fn on_receipt_proof_needs(
        &mut self,
        to_shard: ShardId,
        change: NeedChange,
        now: Instant,
    ) -> DataManagerOutput {
        match change {
            NeedChange::Needed(sources) => {
                for source in sources {
                    let id = DataId::ReceiptProof(ReceiptProofId { source, to_shard });
                    if let Err(err) = self.track_missing_item(&id) {
                        tracing::error!(target: "spice_data_distribution", ?err, ?id, "failed to track a receipt proof the chunk executor misses");
                    }
                }
            }
            NeedChange::NoLongerNeededBelow(height) => {
                for id in self.items.pull_only_ids_in(to_shard, ..height) {
                    self.remove_item(&id);
                }
            }
            NeedChange::NoLongerNeeded => {
                for id in self.items.pull_only_ids_in(to_shard, ..) {
                    self.remove_item(&id);
                }
            }
        }
        self.handle_done_items();
        DataManagerOutput {
            pull_requests: self.pulls.to_issue(now, &self.items),
            decoded: Vec::new(),
        }
    }

    /// Tracks `id`, which the chunk executor misses, unless it is tracked or done, and makes it
    /// pullable. An item pushed to this node is skipped below the final execution head, where it
    /// expires.
    fn track_missing_item(&mut self, id: &DataId) -> Result<(), Error> {
        if self.items.contains(id) || self.is_done(id) {
            return Ok(());
        }
        let block = self.chain_store.get_block_header(id.block_hash())?;
        if self.is_fork_at_or_below_final_head(&block)? {
            return Ok(());
        }
        let pushed = self.is_pushed_to_me(id)?;
        if pushed && self.heights.below_final_execution(block.height()) {
            return Ok(());
        }
        let source = id.producing_chunk();
        debug_assert!(
            !block.is_spice()
                || self.chain_store.store().exists(
                    DBCol::execution_results(),
                    &get_execution_results_key(&source.block_hash, source.shard_id),
                ),
            "the chunk executor misses only receipt proofs from certified chunks",
        );
        let item = self.new_item(&block, id)?;
        if pushed {
            self.items.insert_push(id.clone(), item);
        } else {
            self.items.insert_pull_only(id.clone(), item);
        }
        let delivery = self.items.delivery(id).expect("the item was just tracked");
        self.pulls.mark_pullable(id.clone(), block.height(), delivery);
        Ok(())
    }

    /// Adds the tracked items `block` makes pullable to `pulls`.
    fn mark_pullable_by(&mut self, block: &Block) -> Result<(), Error> {
        for id in self.made_pullable_by(block)? {
            if self.done_items.contains(&id) {
                continue;
            }
            if let (Some(item), Some(delivery)) = (self.items.get(&id), self.items.delivery(&id)) {
                self.pulls.mark_pullable(id, item.height, delivery);
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
    ) -> DataManagerOutput {
        match self.final_execution_head_height() {
            Ok(height) => self.on_final_execution_head(height),
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
        DataManagerOutput { pull_requests: self.pulls.to_issue(now, &self.items), decoded }
    }

    /// Height of the final execution head; the genesis height before the first one is recorded.
    fn final_execution_head_height(&self) -> Result<BlockHeight, Error> {
        match self.chain_store.spice_final_execution_head() {
            Ok(head) => Ok(head.height),
            Err(Error::DBNotFoundErr(_)) => Ok(self.chain_store.get_genesis_height()),
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

    /// Records the final execution head at `height` and stops tracking the items pushed to this node
    /// below it. Below it, processed blocks are forgotten and parts for untracked items are no longer
    /// wanted.
    fn on_final_execution_head(&mut self, height: BlockHeight) {
        let Some(from) = self.heights.update_final_execution_head(height) else {
            return;
        };
        self.processed_blocks.retain(|_, block_height| *block_height >= height);
        // Pushed items are not tracked below the head, so the heights below `from` hold none.
        let expired: Vec<DataId> =
            self.items.push_ids_in((from, Bound::Excluded(height))).cloned().collect();
        for id in expired {
            self.remove_item(&id);
        }
    }

    /// Removes the items at heights newly at or below the final head at `final_height` that are
    /// off the canonical chain, which can never be applied, and every done item at or below it. A
    /// done item is kept only to settle late parts, and the head of the shard it goes to need not
    /// pass it: the shard may stop being applied.
    fn on_final_head(&mut self, final_height: BlockHeight) {
        let Some(from) = self.heights.update_final_head(final_height) else {
            return;
        };
        let mut canonical_hashes = HashMap::new();
        let mut removed: Vec<DataId> = self
            .done_items
            .iter()
            .filter(|id| self.items.get(id).expect("a done item is tracked").height <= final_height)
            .cloned()
            .collect();
        for id in self.items.ids_in((from, Bound::Included(final_height))) {
            let height = self.items.get(id).expect("indexed ids are tracked").height;
            let canonical = match canonical_hashes.get(&height) {
                Some(canonical) => *canonical,
                None => match self.canonical_block_hash(height) {
                    Ok(canonical) => *canonical_hashes.entry(height).or_insert(canonical),
                    Err(err) => {
                        tracing::error!(target: "spice_data_distribution", ?err, height, "failed to read the canonical block");
                        continue;
                    }
                },
            };
            if Some(*id.block_hash()) != canonical {
                removed.push(id.clone());
            }
        }
        for id in removed {
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

    /// Marks the delivered items that are done, and stops pulling them.
    fn handle_done_items(&mut self) {
        let done: Vec<DataId> =
            self.delivered_items.iter().filter(|id| self.is_done(id)).cloned().collect();
        for id in done {
            let height = self.items.get(&id).expect("a delivered item is tracked").height;
            let delivery = self.items.delivery(&id).expect("a delivered item is tracked");
            self.delivered_items.remove(&id);
            self.pulls.forget(&id, height, delivery);
            self.done_items.insert(id);
        }
    }

    fn remove_item(&mut self, id: &DataId) {
        let Some(delivery) = self.items.delivery(id) else {
            return;
        };
        let item = self.items.remove(id).expect("an item with a delivery is tracked");
        self.pulls.forget(id, item.height, delivery);
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
