//! Fetch-engine state for SPICE data distribution.

mod data_id;
mod fetchable;
mod item;
mod pending;

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
pub(crate) use pending::PendingPartialData;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    fn needed_ids(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        self.receipt_proofs.needed_ids(block)
    }

    fn is_done(&self, id: &DataId) -> Result<bool, Error> {
        self.for_id(id).is_done(id)
    }

    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.for_id(id).producers(id)
    }
}

/// Owns the per-item fetch lifecycle: what this node still needs, the parts received so
/// far and who sent them, and when an item stops being relevant.
// TODO(spice-data-distribution): only receipt proofs route here; witnesses still live
// on the old actor path (#16275).
pub(crate) struct SpiceDataManager<P: DataPolicy = Policies> {
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
    pub(crate) fn new(data_parts_ratio: f64, policies: P) -> Self {
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

    /// Starts tracking every item this node needs from `block` and doesn't already have or
    /// track. Idempotent.
    pub(crate) fn track_block(&mut self, block: &BlockHeader) -> Result<(), Error> {
        let height = block.height();
        // The chain is past the block, so its data can never be applied.
        if self.final_execution_head.is_some_and(|head| height <= head) {
            return Ok(());
        }
        let mut new_ids = Vec::new();
        for id in self.policies.needed_ids(block)? {
            if !self.items.contains_key(&id) && !self.policies.is_done(&id)? {
                new_ids.push(id);
            }
        }
        if new_ids.is_empty() {
            return Ok(());
        }
        let mut resolved = Vec::with_capacity(new_ids.len());
        for id in new_ids {
            let producers = self.policies.producers(&id)?;
            resolved.push((id, producers));
        }
        for (id, producers) in resolved {
            self.items_by_height.entry(height).or_default().push(id.clone());
            self.items.insert(id, FetchItem::new(height, producers));
        }
        Ok(())
    }

    /// Handles incoming parts: verifies every part's proof against the commitment before
    /// inserting any; a part failing its proof rejects the whole message and leaves the item
    /// untouched, as does an empty message, one with more than `total_parts` parts, one
    /// repeating an ordinal, or one from a sender that is not a producer of the item. A
    /// decoding insert checks the decoded data against the committed hash and the id, settles
    /// the commitment either way, and returns matching data.
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
        let Some(producer) = item.producers.iter().position(|(account, _)| account == sender)
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
        let encoder = self.encoders.entry(total_parts);
        for part in verified {
            match item.insert_part(&encoder, id, producer, part) {
                PartInsertResult::Decoded(data) => {
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
