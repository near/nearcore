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

/// Inputs required to create a `PrepareTransactionsJob`.
pub struct PrepareTransactionsJobInputs {
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub state: TrieUpdate,
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
    Finished(Option<Result<PreparedTransactions, Error>>),
}

/// A job that prepares transactions for inclusion in a chunk.
/// Used for early transaction preparation.
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

    /// Wait for the job to complete. Run the job in place if it hasn't started yet.
    pub fn wait(&self) {
        let mut state = self.state.lock();
        match std::mem::replace(&mut *state, PrepareTransactionsJobState::Running) {
            PrepareTransactionsJobState::Finished(result) => {
                // Put the result back since we took it
                *state = PrepareTransactionsJobState::Finished(result);
            }
            PrepareTransactionsJobState::NotStarted(inputs) => {
                let result = self.run_not_started(inputs);
                *state = PrepareTransactionsJobState::Finished(result.transpose());
            }
            PrepareTransactionsJobState::Running => {
                unreachable!("not reachable due to locking")
            }
        }
    }

    fn take(&self) -> Option<Result<PreparedTransactions, Error>> {
        let mut state = self.state.lock();
        if let PrepareTransactionsJobState::Finished(result) = &mut *state {
            return result.take();
        }
        None
    }

    fn run_not_started(
        &self,
        inputs: PrepareTransactionsJobInputs,
    ) -> Result<Option<PreparedTransactions>, Error> {
        let mut pool_guard = inputs.tx_pool.lock();

        // Usually the prepare transactions job runs in parallel with chunk application, before the
        // block that contains the applied chunk is postprocessed.
        //
        // However in rare cases (weird thread scheduling, testloop reordering things) it might
        // happen that the job starts after the block is postprocessed.
        //
        // In such cases it's better to discard the job and prepare transactions the normal way, in
        // `produce_chunk` which happens right after postprocessing the block.
        //
        // This is because the job is not aware of the block that was just postprocessed. If we run
        // the job, there's a risk that transactions that were created using the latest
        // postprocessed block will be rejected because the job isn't aware of the latest block.
        // This happens is some testloop tests and makes them fail.
        if let Ok(_hash) = inputs
            .runtime_adapter
            .store()
            .chain_store()
            .get_block_hash_by_height(inputs.prev_block_context.height)
        {
            return Ok(None);
        }

        let (prepared, skipped) =
            if let Some(mut iter) = pool_guard.get_pool_iterator(inputs.shard_uid) {
                inputs.runtime_adapter.prepare_transactions_extra(
                    inputs.state,
                    inputs.shard_uid.shard_id(),
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
        Ok(Some(prepared))
    }
}

/// Key which uniquely identifies a preparation job with specific inputs. This key is used when
/// fetching job result, any mismatch in the inputs will cause the job to be discarded.
#[derive(PartialEq, Eq)]
pub struct PrepareTransactionsJobKey {
    pub shard_uid: ShardUId,
    pub shard_update_key: CachedShardUpdateKey,
    pub prev_block_context: PrepareTransactionsBlockContext,
}

/// Manages multiple `PrepareTransactionsJob`s, ensuring that only one job per (height, shard_id)
/// is active at a time. If a new job is pushed for the same (height, shard_id), the existing job is
/// cancelled and replaced.
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
        let shard_id = key.shard_uid.shard_id();
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
    ) -> Option<Result<PreparedTransactions, Error>> {
        let height = key.prev_block_context.height;
        let shard_id = key.shard_uid.shard_id();

        let Some((job_key, job)) = self.jobs.pop(&(height, shard_id)) else {
            return None;
        };
        if job_key != key {
            job.cancel();
            job.wait();
            return None;
        }
        job.wait();
        job.take()
    }
}
