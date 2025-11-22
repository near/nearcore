use std::collections::{HashMap, HashSet, VecDeque};

use near_chain::spice_core::get_uncertified_chunks;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::ShardId;
use near_store::ShardUId;
use near_store::adapter::chain_store::ChainStoreAdapter;

pub struct ShardedPendingTransactionQueue {
    queues: HashMap<ShardUId, PendingTransactionQueue>,
}

impl ShardedPendingTransactionQueue {
    pub fn new() -> Self {
        Self { queues: HashMap::new() }
    }

    pub fn get_queue_mut(&mut self, shard_uid: ShardUId) -> &mut PendingTransactionQueue {
        self.queues.entry(shard_uid).or_insert_with(PendingTransactionQueue::new)
    }

    pub fn reset_from_uncertified_chunks(
        &mut self,
        head: &CryptoHash,
        store: &ChainStoreAdapter,
        shard_tracker: &ShardTracker,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        self.queues.clear();
        let chunk_infos = get_uncertified_chunks(store, head)?;
        let mut shard_ids_by_block_hash: HashMap<CryptoHash, HashSet<ShardId>> = HashMap::new();
        for chunk_info in chunk_infos {
            let block_hash = chunk_info.chunk_id.block_hash;
            let shard_id = chunk_info.chunk_id.shard_id;
            shard_ids_by_block_hash.entry(block_hash).or_default().insert(shard_id);
        }
        for (block_hash, shard_ids) in shard_ids_by_block_hash {
            let block = store.get_block(&block_hash)?;
            // Spice does not have missing chunks so we can use iter_new here.
            for chunk_header in block.chunks().iter_new() {
                let shard_id = chunk_header.shard_id();
                // TODO(spice-resharding): Is this correct?
                if !shard_tracker.cares_about_shard(block.header().prev_hash(), shard_id) {
                    continue;
                }
                if !shard_ids.contains(&chunk_header.shard_id()) {
                    continue;
                }
                let epoch_id = block.header().epoch_id();
                let chunk = store.get_chunk(&chunk_header.chunk_hash())?;
                self.get_queue_mut(shard_id_to_uid(epoch_manager, shard_id, epoch_id)?)
                    .add_transactions(block_hash, chunk.into_transactions());
            }
        }
        Ok(())
    }
}

/// A queue that holds transactions that are included in blocks,
/// but not yet executed for a given shard.
pub struct PendingTransactionQueue {
    pending_txs: VecDeque<(CryptoHash, Vec<SignedTransaction>)>,
}

impl PendingTransactionQueue {
    fn new() -> Self {
        Self { pending_txs: VecDeque::new() }
    }

    pub fn add_transactions(&mut self, block_hash: CryptoHash, txs: Vec<SignedTransaction>) {
        self.pending_txs.push_back((block_hash, txs));
    }

    pub fn remove_transactions(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Option<Vec<SignedTransaction>> {
        if let Some(pos) = self.pending_txs.iter().position(|(hash, _)| hash == block_hash) {
            let (_, txs) = self.pending_txs.remove(pos).unwrap();
            Some(txs)
        } else {
            None
        }
    }
}
