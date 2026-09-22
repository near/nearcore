//! Fetch-engine state for SPICE data distribution.

mod data_id;
mod fetchable;
mod item;
mod pull;

pub use data_id::DataId;
pub(crate) use fetchable::ChainView;
pub(crate) use fetchable::DataPolicy;
use fetchable::ReceiptProofPolicy;
pub(crate) use item::{AssembledDataError, SpiceData, VerifiedCodedPart};
use item::{FetchItem, PartInsertResult};
use near_async::time::Instant;
use near_chain::Error;
use near_chain::spice::core::SpiceCoreReader;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_store::adapter::chain_store::ChainStoreAdapter;
pub(crate) use pull::{PullConfig, PullRequest, rotated_source_index};
use std::collections::{BTreeMap, HashMap};
use std::mem::replace;
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
    chain_store: ChainStoreAdapter,
    core_reader: SpiceCoreReader,
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
                chain_store.clone(),
                epoch_manager,
                shard_tracker,
            ),
            chain_store,
            core_reader,
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

    fn is_pullable(
        &self,
        id: &DataId,
        height: BlockHeight,
        certified_frontier: &HashMap<ShardId, BlockHeight>,
    ) -> bool {
        self.for_id(id).is_pullable(id, height, certified_frontier)
    }
}

impl ChainView for Policies {
    fn block_header(&self, block_hash: &CryptoHash) -> Result<Arc<BlockHeader>, Error> {
        self.chain_store.get_block_header(block_hash)
    }

    fn final_execution_head_height(&self) -> Result<BlockHeight, Error> {
        match self.chain_store.spice_final_execution_head() {
            Ok(head) => Ok(head.height),
            Err(Error::DBNotFoundErr(_)) => Ok(self.chain_store.get_genesis_height()),
            Err(err) => Err(err),
        }
    }

    fn certified_frontier(
        &self,
        block: &BlockHeader,
    ) -> Result<HashMap<ShardId, BlockHeight>, Error> {
        self.core_reader.highest_certified_heights(block)
    }
}

/// Owns the per-item fetch state: what this node still needs, the parts received so far
/// and who sent them, the pull requests outstanding, and when an item stops being
/// relevant.
// TODO(spice-data-distribution): only receipt proofs route here; witnesses still live
// on the old actor path (#16275).
pub(crate) struct SpiceDataManager<P: DataPolicy + ChainView = Policies> {
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

impl<P: DataPolicy + ChainView> SpiceDataManager<P> {
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
            let sources = self.policies.sources(&id)?;
            self.items_by_height.entry(height).or_default().push(id.clone());
            self.items.insert(id, FetchItem::new(height, sources));
        }
        Ok(())
    }

    /// The block was processed at `now`: expires the items at or below the final execution
    /// head, tracks the items needed from the block, retires the pullable ones already in
    /// the store, and returns the requests for the rest, grouped by producer. Without a
    /// `requester` nothing is requested and the items stay.
    pub(crate) fn on_new_block(
        &mut self,
        block_hash: &CryptoHash,
        requester: Option<&AccountId>,
        now: Instant,
    ) -> Result<Vec<PullRequest>, Error> {
        let block = self.policies.block_header(block_hash)?;
        let block = block.as_ref();
        self.expire_at_or_below(self.policies.final_execution_head_height()?);
        self.track_block(block)?;
        let certified_frontier = self.policies.certified_frontier(block)?;
        self.retire_done_items(&certified_frontier);
        Ok(self.pull_requests(now, &certified_frontier, requester))
    }

    /// The only insert path for received units. Verifies every part against the
    /// commitment first and inserts the ones that verify; a message with at least one
    /// verifying part counts as the sender's answer to any request outstanding to it. A
    /// decoding insert checks the decoded data against the committed hash and the id,
    /// settles the commitment either way, and returns matching data.
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
        let mut rejected = 0;
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            match VerifiedCodedPart::verify(commitment, total_parts, part_ord, part, &merkle_proof)
            {
                Some(part) => verified.push(part),
                None => rejected += 1,
            }
        }
        // A message with no verifying part does not bind the sender. That costs at worst one extra
        // pull request per sender. The alternative is either Item-local set of banned, or better manager-wide
        // reputation - an option for later.
        if verified.is_empty() {
            return Err(SenderFault::InvalidMerkleProof);
        }
        if rejected > 0 {
            tracing::debug!(target: "spice_data_distribution", ?id, ?sender, rejected, "parts failed their merkle proof");
        }
        item.note_answer_from(sender);
        let encoder = self.encoders.entry(total_parts);
        for part in verified {
            match item.insert_part(&encoder, id, sender, part) {
                PartInsertResult::Decoded(data) => {
                    item.delivered = true;
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

    /// Stops tracking items at or below `height`.
    fn expire_at_or_below(&mut self, height: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(height));
        let Some(next_height) = height.checked_add(1) else {
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
