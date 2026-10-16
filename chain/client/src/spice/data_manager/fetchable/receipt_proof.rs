use super::DataPolicy;
use crate::spice::chunk_executor_actor::{get_receipt_proof, receipt_proof_exists};
use crate::spice::data_manager::{DataId, SpiceData};
use near_chain::spice::boundary::shards_applied_itself;
use near_chain::{Block, Error};
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Receipt proofs: produced by the source chunk's producers, needed by nodes that apply
/// the destination shard in the next block; done once the proof is persisted.
pub(crate) struct ReceiptProofPolicy {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
}

impl ReceiptProofPolicy {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self { chain_store, epoch_manager, shard_tracker }
    }

    fn needed_shards(&self, block: &BlockHeader) -> Result<NeededShards, Error> {
        let shard_layout = self.epoch_manager.get_shard_layout(block.epoch_id())?;
        // Applying the source shard ourselves produces the proof locally; this is
        // also why a producer never fetches its own proof.
        let applied_itself =
            shards_applied_itself(&self.shard_tracker, self.epoch_manager.as_ref(), block)?;
        let sources = shard_layout
            .shard_ids()
            .filter(|shard_id| !applied_itself.contains(shard_id))
            .collect();
        // The proof feeds applying the destination shard in the next block. `block` is a
        // spice or last pre-spice block, so the next one is a spice block: no boundary case.
        let destinations = shard_layout
            .shard_ids()
            .filter(|shard_id| {
                self.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::IsCaughtUp,
                    block.hash(),
                    *shard_id,
                )
            })
            .collect();
        Ok(NeededShards { sources, destinations })
    }

    fn in_layout(&self, epoch_id: &EpochId, shard_id: ShardId) -> Result<bool, Error> {
        Ok(self.epoch_manager.get_shard_layout(epoch_id)?.shard_ids().any(|id| id == shard_id))
    }
}

/// The shards of a block whose receipt proofs this node needs.
struct NeededShards {
    /// Source shards this node doesn't apply itself.
    sources: Vec<ShardId>,
    /// Destination shards this node applies in the next block.
    destinations: Vec<ShardId>,
}

impl DataPolicy for ReceiptProofPolicy {
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error> {
        let NeededShards { sources, destinations } = self.needed_shards(block)?;
        // TODO(spice-resharding): Handle resharding
        let mut items = Vec::with_capacity(sources.len() * destinations.len());
        for from_shard in sources {
            let producers = self
                .epoch_manager
                .get_epoch_chunk_producers_for_shard(block.epoch_id(), from_shard)?;
            for to_shard in &destinations {
                let id = DataId::receipt_proof(*block.hash(), from_shard, *to_shard);
                items.push((id, producers.clone()));
            }
        }
        Ok(items)
    }

    fn is_done(&self, id: &DataId) -> bool {
        let DataId::ReceiptProof { source, to_shard } = id;
        receipt_proof_exists(
            &self.chain_store.store(),
            &source.block_hash,
            *to_shard,
            source.shard_id,
        )
    }

    /// The needed proofs from every chunk `block` certifies.
    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        let mut needed_by_source_block: HashMap<CryptoHash, NeededShards> = HashMap::new();
        let mut ids = Vec::new();
        for (source, _) in block.spice_core_statements().iter_execution_results() {
            let needed = match needed_by_source_block.entry(source.block_hash) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let header = self.chain_store.get_block_header(&source.block_hash)?;
                    entry.insert(self.needed_shards(&header)?)
                }
            };
            if !needed.sources.contains(&source.shard_id) {
                continue;
            }
            for to_shard in &needed.destinations {
                ids.push(DataId::ReceiptProof { source: source.clone(), to_shard: *to_shard });
            }
        }
        Ok(ids)
    }

    /// The source shard is read against the source block's layout, the destination shard
    /// against the next block's.
    fn names_known_shards(&self, id: &DataId) -> Result<bool, Error> {
        let DataId::ReceiptProof { source, to_shard } = id;
        let header = self.chain_store.get_block_header(&source.block_hash)?;
        let next_block_epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&source.block_hash)?;
        Ok(self.in_layout(header.epoch_id(), source.shard_id)?
            && self.in_layout(&next_block_epoch_id, *to_shard)?)
    }

    // TODO(spice-resharding): validate whether the source and destination shards are correct
    // when resharding.
    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        let DataId::ReceiptProof { source, .. } = id;
        let header = self.chain_store.get_block_header(&source.block_hash)?;
        Ok(self
            .epoch_manager
            .get_epoch_chunk_producers_for_shard(header.epoch_id(), source.shard_id)?)
    }

    /// The destination shard's chunk producers for the next block.
    fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error> {
        let DataId::ReceiptProof { source, to_shard } = id;
        let next_block_epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&source.block_hash)?;
        let producers = self.producers(id)?;
        Ok(self
            .epoch_manager
            .get_epoch_chunk_producers_for_shard(&next_block_epoch_id, *to_shard)?
            .into_iter()
            .filter(|account| !producers.contains(account))
            .collect())
    }

    fn stored_data(&self, id: &DataId) -> Option<SpiceData> {
        let DataId::ReceiptProof { source, to_shard } = id;
        get_receipt_proof(
            self.chain_store.store_ref(),
            &source.block_hash,
            *to_shard,
            source.shard_id,
        )
        .map(SpiceData::ReceiptProof)
    }
}
