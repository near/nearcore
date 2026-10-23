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

pub use data_id::DataId;
pub(crate) use encode::{EncodedData, encode};
use heights::ChainHeights;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use near_async::time::Instant;
use near_chain::{Block, Error};
use near_crypto::PublicKey;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, epoch_chunk_producers_for_shard};
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataPart, SpicePartialData, SpiceVerifiedPartialData,
};
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
pub(crate) use orphans::OrphanPool;
use policy::receipt_proof;
pub(crate) use policy::{ServePolicies, ServePolicy};
use pull::Pulls;
pub(crate) use pull::{Config as PullConfig, PullRequest};
use sender::AuthenticatedSender;
pub use serve::MalformedDataRequest;
pub(crate) use serve::{Push, ServeError, ServeOutcome, SpiceDataServer};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::Arc;

/// Share of the parts an item is encoded into that suffice to decode it.
pub const DATA_PARTS_RATIO: f64 = 0.6;

/// Max total number of parts a single batched request may carry, and so a single message
/// answering it. Must stay above the largest per-shard chunk-producer set.
// TODO(spice): Bound the outbound bytes per (data_id, requester) too.
pub(crate) const MAX_REQUESTED_PARTS: usize = 256;

/// Blocks an orphan pool holds data for.
pub(crate) const MAX_ORPHAN_BLOCKS: NonZeroUsize = NonZeroUsize::new(10).unwrap();

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
    match id {
        DataId::ReceiptProof { source, .. } => {
            Ok(epoch_chunk_producers_for_shard(epoch_info, shard_layout, source.shard_id)?)
        }
    }
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

/// Parts received for an item whose block was not processed yet.
struct ParkedParts {
    sender: AuthenticatedSender,
    id: DataId,
    commitment: SpiceDataCommitment,
    parts: Vec<SpiceDataPart>,
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
    /// Parts for blocks not processed yet.
    orphans: OrphanPool<ParkedParts>,
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
            orphans: OrphanPool::new(MAX_ORPHAN_BLOCKS),
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

    /// Handles a processed block. Returns the data decoded from parts that waited for it.
    /// Idempotent. Fails only on a read of the block's stored chain or epoch data.
    pub(crate) fn track_block(&mut self, block: &Block) -> Result<Vec<(DataId, SpiceData)>, Error> {
        self.track_needed_items(block)?;
        self.mark_pullable_by(block)?;
        Ok(self.release_parked(block.header()))
    }

    /// Feeds the parts parked for the block to its items. Returns the data decoded.
    fn release_parked(&mut self, block: &BlockHeader) -> Vec<(DataId, SpiceData)> {
        let mut decoded = Vec::new();
        for ParkedParts { sender, id, commitment, parts } in
            self.orphans.release(block.height(), block.hash())
        {
            let Some(item) = self.items.get(&id) else {
                continue;
            };
            let producer_index = self.producer_signed_by(item, &sender);
            let result =
                producer_index.and_then(|index| self.add_parts(&id, index, &commitment, parts));
            match result {
                Ok(PartsOutcome::Decoded(data)) => decoded.push((id, data)),
                Ok(
                    PartsOutcome::Collecting
                    | PartsOutcome::AlreadySettled
                    | PartsOutcome::NotWanted,
                ) => {}
                Err(fault) => {
                    tracing::debug!(target: "spice_data_distribution", ?fault, ?id, ?sender, "rejected parked parts");
                }
            }
        }
        decoded
    }

    /// Starts tracking every item this node needs from `block` and doesn't already have or track.
    fn track_needed_items(&mut self, block: &Block) -> Result<(), Error> {
        let block = block.header();
        let height = block.height();
        // The chain is past the block, so its data can never be applied.
        if self.heights.at_or_below_final_execution(height) {
            return Ok(());
        }
        // A block off the canonical chain at or below the final head can never be applied.
        if self.heights.at_or_below_final_head(height)
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
        ProcessedBlockOutput { pull_requests: self.pulls.to_issue(now, &self.items), decoded }
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
        let id = DataId::from_identifier(data.id())
            .expect("only data the engine tracks should reach the data manager");
        let Some(item) = self.items.get(&id) else {
            return self.park(id, data);
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
        if self.done_items.contains(id) || item.is_settled(commitment) {
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

    /// Parks parts for an untracked item until its block's trigger.
    fn park(&mut self, id: DataId, data: SpicePartialData) -> Result<PartsOutcome, SenderFault> {
        let header = self.chain_store.get_block_header_if_exists(id.block_hash());
        let block_epochs = match &header {
            Some(header) => vec![(*header.epoch_id(), *header.next_epoch_id())],
            None => {
                let header_head =
                    self.chain_store.header_head().expect("header head should be stored");
                // The epoch after the next is not known yet, so a block in the next epoch takes
                // the next epoch's keys only.
                vec![
                    (header_head.epoch_id, header_head.next_epoch_id),
                    (header_head.next_epoch_id, header_head.next_epoch_id),
                ]
            }
        };
        let keys = self
            .sender_keys(&id, data.sender(), &block_epochs)
            .expect("producers and keys of known epochs should be readable");
        if keys.is_empty() {
            // Without the header the block's epoch is a guess, so a miss is no proof of a fault.
            return match header {
                Some(_) => Err(SenderFault::NotAProducer),
                None => Ok(PartsOutcome::NotWanted),
            };
        }
        let (sender, SpiceVerifiedPartialData { commitment, parts, .. }) =
            AuthenticatedSender::verify(data, &keys).ok_or(SenderFault::InvalidSignature)?;
        if header.is_some_and(|header| self.heights.at_or_below_final_head(header.height())) {
            return Ok(PartsOutcome::NotWanted);
        }
        let block_hash = *id.block_hash();
        if !self.orphans.insert(block_hash, ParkedParts { sender, id, commitment, parts }) {
            return Ok(PartsOutcome::NotWanted);
        }
        Ok(PartsOutcome::Collecting)
    }

    /// The keys `sender` may sign `id`'s parts with if it produces `id` for a block in one of
    /// `block_epochs`, each a block's epoch and the next. An epoch whose layout lacks the item's
    /// shard adds none.
    fn sender_keys(
        &self,
        id: &DataId,
        sender: &AccountId,
        block_epochs: &[(EpochId, EpochId)],
    ) -> Result<Vec<PublicKey>, Error> {
        let [source_shard, _] = id.shard_endpoints();
        let mut keys = Vec::new();
        for (epoch_id, next_epoch_id) in block_epochs {
            // The validator lookup comes first, so a non-validator costs no producer list.
            let sender_keys = match self.epoch_manager.get_validator_signing_keys(
                epoch_id,
                next_epoch_id,
                sender,
            ) {
                Ok(sender_keys) => sender_keys,
                Err(EpochError::NotAValidator(..)) => continue,
                Err(err) => return Err(err.into()),
            };
            let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
            if shard_layout.get_shard_index(source_shard).is_err() {
                continue;
            }
            let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
            if !producers_in_epoch(&epoch_info, &shard_layout, id)?.contains(sender) {
                continue;
            }
            for key in sender_keys {
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
        Ok(keys)
    }

    /// Stops tracking items at or below the final execution head at `height`.
    fn on_final_execution_head(&mut self, height: BlockHeight) {
        self.heights.update_final_execution_head(height);
        let Some(next_height) = height.checked_add(1) else {
            return;
        };
        let live = self.items_by_height.split_off(&next_height);
        let expired = mem::replace(&mut self.items_by_height, live);
        self.pulls.forget_below(next_height);
        for id in expired.into_values().flatten() {
            self.items.remove(&id).expect("index entry names a tracked item");
            self.delivered_items.remove(&id);
            self.done_items.remove(&id);
        }
    }

    /// Forgets the released blocks at or below the final head at `final_height`, and removes the
    /// items off the canonical chain at heights newly at or below it.
    fn on_final_head(&mut self, final_height: BlockHeight) {
        let Some(from) = self.heights.update_final_head(final_height) else {
            return;
        };
        self.orphans.prune_at_or_below(final_height);
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

    /// Marks the delivered items that are done, and stops pulling them.
    fn handle_done_items(&mut self) {
        let done: Vec<DataId> =
            self.delivered_items.iter().filter(|id| self.is_done(id)).cloned().collect();
        for id in done {
            self.delivered_items.remove(&id);
            self.pulls.forget(&id, self.items[&id].height);
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
