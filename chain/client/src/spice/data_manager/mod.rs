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

pub(crate) use data_id::Decoded;
pub use data_id::{DataId, ReceiptProofId};
pub(crate) use encode::{EncodedData, encode};
use heights::{ChainHeights, ExpiredHeights, LowestNeededHeights};
use item::VerifiedCodedPart;
pub(crate) use item::{AssembledDataError, SpiceData};
use item::{FetchItem, PartInsertResult};
use items::Items;
use lru::LruCache;
use near_async::time::Instant;
use near_chain::{Block, Error};
use near_chain_configs::MutableValidatorSigner;
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
use near_primitives::types::{AccountId, BlockHeight, ShardId, SpiceChunkId};
use near_primitives::utils::get_execution_results_key;
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use orphans::ParkedParts;
pub(crate) use policy::ServePolicies;
#[cfg(test)]
pub(crate) use policy::witness::FALLBACK_TRACKING_LOOKAHEAD;
use policy::{receipt_proof, witness};
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

/// Max number of witness entries a single batched request may carry. Serving one encodes a
/// witness of up to 64 MiB.
pub(crate) const MAX_REQUESTED_WITNESSES: usize = 2;

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
    Decoded(Decoded),
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
    pub(crate) decoded: Vec<Decoded>,
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
    /// Handles pull recovery.
    pulls: Pulls,
    /// Tracks the heights observed.
    heights: ChainHeights,
    /// Parts for blocks not processed yet, by block hash; the most recently parked blocks.
    // TODO(spice-data-distribution): bound the data parked per sender.
    orphans: LruCache<CryptoHash, Vec<ParkedParts>>,
    /// Processed blocks at or above the lowest needed height of any kind, with their heights.
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
            heights: ChainHeights::default(),
            orphans: LruCache::new(MAX_ORPHAN_BLOCKS),
            processed_blocks: HashMap::new(),
        }
    }

    fn me(&self) -> Option<AccountId> {
        self.validator_signer.get().map(|signer| signer.validator_id().clone())
    }

    /// The ids of every data kind pushed to this node from `block`.
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        let epoch_manager = self.epoch_manager.as_ref();
        let me = self.me();
        let mut ids =
            receipt_proof::needed_items(epoch_manager, &self.shard_tracker, me.as_ref(), block)?;
        ids.extend(witness::needed_items(epoch_manager, &self.shard_tracker, me.as_ref(), block)?);
        Ok(ids)
    }

    /// Whether `id` is pushed to this node.
    fn is_pushed_to_me(&self, id: &DataId) -> Result<bool, Error> {
        let me = self.me();
        match id {
            DataId::ReceiptProof(id) => {
                receipt_proof::is_pushed_to(self.epoch_manager.as_ref(), me.as_ref(), id)
            }
            DataId::Witness(_) => Ok(witness::is_pushed_to(me.as_ref())),
        }
    }

    /// Whether the item is completed as far as data distribution is concerned.
    fn is_done(&self, id: &DataId) -> bool {
        let store = self.chain_store.store();
        match id {
            DataId::ReceiptProof(id) => receipt_proof::is_done(&store, &id.source, id.to_shard),
            DataId::Witness(chunk_id) => witness::is_done(&store, self.me().as_ref(), chunk_id),
        }
    }

    /// The ids `block` makes pullable, tracked or not.
    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        let epoch_manager = self.epoch_manager.as_ref();
        let mut ids = receipt_proof::made_pullable_by(&self.chain_store, epoch_manager, block)?;
        if let Some(me) = &self.me() {
            ids.extend(witness::made_pullable_by(&self.chain_store, epoch_manager, me, block)?);
        }
        Ok(ids)
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

    /// Tracks the items a processed block needs or makes pullable, and drops the witnesses of
    /// the chunks it certifies. Returns the data decoded from parts that waited for it.
    /// Idempotent. Fails only on a read of the block's stored chain or epoch data.
    fn track_block(&mut self, block: &Block) -> Result<Vec<Decoded>, Error> {
        self.track_needed_items(block)?;
        self.apply_core_statements(block)?;
        Ok(self.release_parked(block.header()))
    }

    /// Opens the tracked items `block` makes pullable and marks done the witnesses of the chunks it
    /// certifies.
    fn apply_core_statements(&mut self, block: &Block) -> Result<(), Error> {
        self.mark_pullable_by(block)?;
        for id in witness::certified_by(block) {
            self.mark_done(&id);
        }
        Ok(())
    }

    /// Starts tracking the items pushed to this node from `block`, and the witnesses of earlier
    /// chunks whose fallback push to this node `block` brings near.
    fn track_needed_items(&mut self, block: &Block) -> Result<(), Error> {
        let ids = self.needed_items(block.header())?;
        self.track_pushed(block.header(), ids)?;
        let me = self.me();
        for (header, id) in witness::nearing_fallback(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            me.as_ref(),
            block,
        )? {
            self.track_pushed(&header, vec![id])?;
        }
        Ok(())
    }

    /// Starts tracking each of `ids`, pushed to this node from `block`, that it doesn't already have
    /// or track.
    fn track_pushed(&mut self, block: &BlockHeader, ids: Vec<DataId>) -> Result<(), Error> {
        let height = block.height();
        if self.is_fork_at_or_below_final_head(block)? {
            return Ok(());
        }
        let new_items = ids
            .into_iter()
            .filter(|id| {
                !self.heights.expired(id, height) && !self.items.contains(id) && !self.is_done(id)
            })
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
    /// pulling the delivered items now saved. Returns the receipt-proof pull requests to send now.
    pub(crate) fn on_receipt_proof_needs(
        &mut self,
        to_shard: ShardId,
        change: NeedChange,
        now: Instant,
    ) -> Vec<PullRequest> {
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
        self.pulls.receipt_proofs_to_issue(now, &self.items)
    }

    /// Tracks `id`, which the chunk executor misses, unless it is tracked or done, and makes it
    /// pullable. An item pushed to this node is skipped below its kind's lowest needed height,
    /// where it expires.
    fn track_missing_item(&mut self, id: &DataId) -> Result<(), Error> {
        if self.items.contains(id) || self.is_done(id) {
            return Ok(());
        }
        let block = self.chain_store.get_block_header(id.block_hash())?;
        if self.is_fork_at_or_below_final_head(&block)? {
            return Ok(());
        }
        let pushed = self.is_pushed_to_me(id)?;
        if pushed && self.heights.expired(id, block.height()) {
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
        self.items.mark_pullable(id);
        Ok(())
    }

    /// Makes the tracked items `block` makes pullable open to pulling.
    fn mark_pullable_by(&mut self, block: &Block) -> Result<(), Error> {
        for id in self.made_pullable_by(block)? {
            if !self.items.is_marked_done(&id) {
                self.items.mark_pullable(&id);
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
        match self.lowest_needed_heights() {
            Ok(lowest_needed) => self.on_lowest_needed(lowest_needed),
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, "failed to read the lowest needed heights");
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

    /// The lowest height whose pushed items of each kind may still be needed.
    fn lowest_needed_heights(&self) -> Result<LowestNeededHeights, Error> {
        Ok(LowestNeededHeights {
            receipt_proofs: receipt_proof::lowest_needed_height(&self.chain_store)?,
            witnesses: witness::lowest_needed_height(&self.chain_store)?,
        })
    }

    /// The block from which the processed blocks are walked on startup: the lower of the final
    /// execution head and the last block certified as of the final head, raised so that every
    /// canonical block from it up to the final execution head has its uncertified chunks. After
    /// state sync, the blocks below the sync block's grandparent have none. Fails before the
    /// first final execution head is recorded.
    pub(crate) fn startup_cutoff(&self) -> Result<Tip, Error> {
        let final_execution_head = self.chain_store.spice_final_execution_head()?;
        let last_certified = witness::last_certified_as_of_final_head(&self.chain_store)?;
        let lowest = final_execution_head.height.min(last_certified.height());
        let mut cutoff = Arc::unwrap_or_clone(final_execution_head);
        for height in (lowest..cutoff.height).rev() {
            let block_hash = match self.chain_store.get_block_hash_by_height(height) {
                Ok(block_hash) => block_hash,
                Err(Error::DBNotFoundErr(_)) => continue,
                Err(err) => return Err(err),
            };
            let header = self.chain_store.get_block_header(&block_hash)?;
            let has_uncertified_chunks = header.is_genesis()
                || !header.is_spice()
                || self
                    .chain_store
                    .store()
                    .exists(DBCol::uncertified_chunks(), block_hash.as_ref());
            if !has_uncertified_chunks {
                break;
            }
            cutoff = Tip::from_header(&header);
        }
        Ok(cutoff)
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
        let done = self.items.is_marked_done(id);
        let item = self.items.get_mut(id).expect("parts are added only to a tracked item");
        let total_parts = item.producers.len();
        if parts.len() > total_parts {
            return Err(SenderFault::TooManyParts);
        }
        if done {
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

    /// Records the lowest needed heights and stops tracking the items pushed to this node below
    /// their kind's. Below the lowest one, processed blocks are forgotten.
    fn on_lowest_needed(&mut self, lowest_needed: LowestNeededHeights) {
        let ExpiredHeights { receipt_proofs, witnesses } =
            self.heights.update_lowest_needed(lowest_needed);
        if receipt_proofs.is_none() && witnesses.is_none() {
            return;
        }
        let expired_receipt_proofs = receipt_proofs.into_iter().flat_map(|heights| {
            self.items.push_ids_in(heights).filter(|id| matches!(id, DataId::ReceiptProof(_)))
        });
        let expired_witnesses = witnesses.into_iter().flat_map(|heights| {
            self.items.push_ids_in(heights).filter(|id| matches!(id, DataId::Witness(_)))
        });
        let expired: Vec<DataId> =
            expired_receipt_proofs.chain(expired_witnesses).cloned().collect();
        for id in expired {
            self.remove_item(&id);
        }
        if let Some(lowest) = self.heights.lowest_needed() {
            self.processed_blocks.retain(|_, block_height| *block_height >= lowest);
        }
    }

    /// Removes the items at heights newly at or below the final head at `final_height` that are
    /// off the canonical chain, which can never be applied, and every done receipt proof at or
    /// below it. A done receipt proof is kept only to settle late parts, and the head of the shard
    /// it goes to need not pass it: the shard may stop being applied. A done witness stays until
    /// it expires: a fork above the final head can still list its chunk as uncertified.
    fn on_final_head(&mut self, final_height: BlockHeight) {
        let Some(from) = self.heights.update_final_head(final_height) else {
            return;
        };
        let mut removed: Vec<DataId> = self
            .items
            .done_ids_in(..=final_height)
            .filter(|id| matches!(id, DataId::ReceiptProof(_)))
            .cloned()
            .collect();
        for height in self.items.heights_in((from, Bound::Included(final_height))) {
            let canonical = match self.canonical_block_hash(height) {
                Ok(canonical) => canonical,
                Err(err) => {
                    tracing::error!(target: "spice_data_distribution", ?err, height, "failed to read the canonical block");
                    continue;
                }
            };
            removed.extend(
                self.items
                    .ids_in(height..=height)
                    .filter(|(_, id)| Some(*id.block_hash()) != canonical)
                    .map(|(_, id)| id.clone()),
            );
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
            self.mark_done(&id);
        }
    }

    /// Stops pulling the tracked item `id` and settles its later parts; does nothing for an
    /// untracked one.
    fn mark_done(&mut self, id: &DataId) {
        if !self.items.contains(id) {
            return;
        }
        self.delivered_items.remove(id);
        self.items.stop_pulling(id);
        self.pulls.forget_requests(id);
        self.items.mark_done(id);
    }

    fn remove_item(&mut self, id: &DataId) {
        if self.items.remove(id).is_none() {
            return;
        }
        self.pulls.forget_requests(id);
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
