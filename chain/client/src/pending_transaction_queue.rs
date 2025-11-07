use std::collections::{HashMap, VecDeque};

use near_primitives::sharding::ChunkHash;
use near_primitives::transaction::SignedTransaction;
use near_store::ShardUId;

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
}

/// A queue that holds transactions that are included in blocks,
/// but not yet executed for a given shard.
pub struct PendingTransactionQueue {
    pending_txs: VecDeque<(ChunkHash, Vec<SignedTransaction>)>,
}

impl PendingTransactionQueue {
    fn new() -> Self {
        Self { pending_txs: VecDeque::new() }
    }

    /// Add transactions for a specific chunk hash.
    pub fn add_transactions(&mut self, chunk_hash: ChunkHash, txs: Vec<SignedTransaction>) {
        self.pending_txs.push_back((chunk_hash, txs));
    }

    /// Remove and return transactions for the specified chunk hash.
    pub fn remove_transactions(
        &mut self,
        chunk_hash: &ChunkHash,
    ) -> Option<Vec<SignedTransaction>> {
        if let Some(pos) = self.pending_txs.iter().position(|(hash, _)| hash == chunk_hash) {
            let (_, txs) = self.pending_txs.remove(pos).unwrap();
            Some(txs)
        } else {
            None
        }
    }
}
