use std::collections::HashMap;
use std::sync::Arc;

use near_chain::types::{HasContract, PendingConstraints, PendingTxCheckResult};
use near_parameters::RuntimeConfig;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::Balance;
use parking_lot::Mutex;

/// Maps ShardUId -> per-shard PendingTransactionQueue.
pub struct ShardedPendingTransactionQueue {
    queues: HashMap<ShardUId, PendingTransactionQueue>,
}

impl ShardedPendingTransactionQueue {
    pub fn new() -> Self {
        Self { queues: HashMap::new() }
    }

    pub fn get_or_create(&mut self, shard_uid: ShardUId) -> &mut PendingTransactionQueue {
        self.queues.entry(shard_uid).or_insert_with(PendingTransactionQueue::new)
    }

    pub fn get(&self, shard_uid: &ShardUId) -> Option<&PendingTransactionQueue> {
        self.queues.get(shard_uid)
    }

    pub fn get_mut(&mut self, shard_uid: &ShardUId) -> Option<&mut PendingTransactionQueue> {
        self.queues.get_mut(shard_uid)
    }

    pub fn remove_certified_block(&mut self, block_hash: &CryptoHash) {
        for queue in self.queues.values_mut() {
            queue.remove_certified_chunk(block_hash);
        }
    }

    pub fn clear(&mut self) {
        self.queues.clear();
    }
}

/// Per-shard pending transaction queue. Tracks transactions that have been
/// included in blocks but not yet certified (executed).
///
/// This is a no-op stub. The real implementation will maintain per-account
/// balance/nonce/count aggregates across uncertified chunks.
pub struct PendingTransactionQueue;

impl PendingTransactionQueue {
    pub fn new() -> Self {
        Self
    }

    pub fn is_empty(&self) -> bool {
        true
    }

    pub fn add_chunk_transactions(
        &mut self,
        _block_hash: CryptoHash,
        _transactions: &[SignedTransaction],
        _config: &RuntimeConfig,
        _gas_price: Balance,
    ) {
    }

    pub fn remove_certified_chunk(&mut self, _block_hash: &CryptoHash) {}

    pub fn clear(&mut self) {}

    pub fn get_pending_constraints(&self, _tx: &SignedTransaction) -> PendingConstraints {
        PendingConstraints::default()
    }
}

/// Per-chunk-production session that combines pending transaction queue state
/// with session-local tracking for P_MAX / deploy exclusivity constraints.
///
/// This is a no-op stub that always admits transactions.
pub struct PendingTxSession {
    _pending_transaction_queue: Arc<Mutex<ShardedPendingTransactionQueue>>,
    _shard_uid: ShardUId,
}

impl PendingTxSession {
    pub fn new(
        pending_transaction_queue: Arc<Mutex<ShardedPendingTransactionQueue>>,
        shard_uid: ShardUId,
    ) -> Self {
        Self { _pending_transaction_queue: pending_transaction_queue, _shard_uid: shard_uid }
    }

    pub fn check_pending(
        &mut self,
        _tx: &SignedTransaction,
        _has_contract: HasContract,
    ) -> PendingTxCheckResult {
        PendingTxCheckResult::Admit(PendingConstraints::default())
    }
}
