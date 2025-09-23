use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use near_async::time::Duration;
use near_chain::types::{
    PrepareTransactionsBlockContext, PreparedTransactions, RuntimeAdapter, SkippedTransactions,
};
use near_chunks::client::ShardedTransactionPool;
use near_client_primitives::types::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::CachedShardUpdateKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::{ShardUId, TrieUpdate};
use parking_lot::Mutex;

pub struct PrepareTransactionsJobInputs {
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub state: TrieUpdate,
    pub shard_id: ShardId,
    pub shard_uid: ShardUId,
    pub prev_block_context: PrepareTransactionsBlockContext,
    pub tx_pool: Arc<Mutex<ShardedTransactionPool>>,
    pub tx_validity_period_check: Box<dyn Fn(&SignedTransaction) -> bool + Send + 'static>,
    pub prev_chunk_tx_hashes: HashSet<CryptoHash>,
    pub time_limit: Option<Duration>,
}

enum PrepareTransactionsJobState {
    NotStarted(PrepareTransactionsJobInputs),
    Running, // Temporary state to use with std::mem::replace, not visible when locked
    Finished(Arc<Result<PreparedTransactions, Error>>),
}

pub struct PrepareTransactionsJob {
    state: Mutex<PrepareTransactionsJobState>,
    cancel: Arc<AtomicBool>,
}

impl PrepareTransactionsJob {
    fn new(inputs: PrepareTransactionsJobInputs) -> Self {
        Self {
            state: Mutex::new(PrepareTransactionsJobState::NotStarted(inputs)),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    fn cancel(&self) {
        self.cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    // Returns true if the job was cancelled.
    fn cancel_if_not_started(&self) -> bool {
        let mut cancelled = false;
        self.state.try_lock().map(|mut state| {
            if let PrepareTransactionsJobState::NotStarted(_) = *state {
                *state = PrepareTransactionsJobState::Finished(Arc::new(Err(
                    Error::ChunkProducer("Job was cancelled before starting".to_string()),
                )));
                cancelled = true;
            }
        });
        cancelled
    }

    pub fn wait(&self) -> Arc<Result<PreparedTransactions, Error>> {
        let mut state = self.state.lock();
        match std::mem::replace(&mut *state, PrepareTransactionsJobState::Running) {
            PrepareTransactionsJobState::Finished(result) => {
                // Put the result back since we took it
                *state = PrepareTransactionsJobState::Finished(result.clone());
                result
            }
            PrepareTransactionsJobState::NotStarted(inputs) => {
                let result = Arc::new(self.run_not_started(inputs));
                *state = PrepareTransactionsJobState::Finished(result.clone());
                result
            }
            PrepareTransactionsJobState::Running => {
                unreachable!("not reachable due to locking")
            }
        }
    }

    fn run_not_started(
        &self,
        inputs: PrepareTransactionsJobInputs,
    ) -> Result<PreparedTransactions, Error> {
        let mut pool_guard = inputs.tx_pool.lock();

        if let Ok(_hash) = inputs
            .runtime_adapter
            .store()
            .chain_store()
            .get_block_hash_by_height(inputs.prev_block_context.height)
        {
            return Err(Error::ChunkProducer(
                "Block was already postprocessed before prepare_transactions job ran, skipping"
                    .to_string(),
            ));
        }

        let (prepared, skipped) =
            if let Some(mut iter) = pool_guard.get_pool_iterator(inputs.shard_uid) {
                inputs.runtime_adapter.prepare_transactions_extra(
                    inputs.state,
                    inputs.shard_id,
                    inputs.prev_block_context,
                    &mut iter,
                    &inputs.tx_validity_period_check,
                    inputs.prev_chunk_tx_hashes,
                    inputs.time_limit,
                    Some(self.cancel.clone()),
                )?
            } else {
                (
                    PreparedTransactions { transactions: Vec::new(), limited_by: None },
                    SkippedTransactions(Vec::new()),
                )
            };
        pool_guard.reintroduce_transactions(inputs.shard_uid, prepared.transactions.clone());
        pool_guard.reintroduce_transactions(inputs.shard_uid, skipped.0);
        Ok(prepared)
    }
}

#[derive(PartialEq, Eq)]
pub struct PrepareTransactionsJobKey {
    pub shard_id: ShardId,
    pub shard_uid: ShardUId,
    pub shard_update_key: CachedShardUpdateKey,
    pub prev_block_context: PrepareTransactionsBlockContext,
}

pub struct PrepareTransactionsManager {
    jobs: lru::LruCache<
        (BlockHeight, ShardId), // Only one job per (height, shard_id) is allowed
        (PrepareTransactionsJobKey, Arc<PrepareTransactionsJob>),
    >,
}

impl PrepareTransactionsManager {
    pub fn new() -> Self {
        Self { jobs: lru::LruCache::new(NonZeroUsize::new(64).unwrap()) }
    }

    pub fn push(
        &mut self,
        key: PrepareTransactionsJobKey,
        inputs: PrepareTransactionsJobInputs,
    ) -> Arc<PrepareTransactionsJob> {
        let height = key.prev_block_context.height;
        let shard_id = key.shard_id;
        assert!(key.prev_block_context == inputs.prev_block_context);

        // Cancel the existing job if it exists
        if let Some((_, job)) = self.jobs.pop(&(height, shard_id)) {
            job.cancel();
            job.wait();
        }

        let job = Arc::new(PrepareTransactionsJob::new(inputs));
        self.jobs.push((height, shard_id), (key, job.clone()));
        job
    }

    pub fn pop_job_result(
        &mut self,
        key: PrepareTransactionsJobKey,
    ) -> Option<Arc<Result<PreparedTransactions, Error>>> {
        let height = key.prev_block_context.height;
        let shard_id = key.shard_id;

        let Some((job_key, job)) = self.jobs.pop(&(height, shard_id)) else {
            return None;
        };
        if job_key != key {
            job.cancel();
            job.wait();
            return None;
        }
        // If the job is not started by now, cancel it so prepare_transactions
        // runs with the latest postprocessed block.
        if job.cancel_if_not_started() {
            return None;
        }
        Some(job.wait())
    }
}
