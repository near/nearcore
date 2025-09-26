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

impl PrepareTransactionsJobInputs {
    #[cfg(test)]
    pub fn new_for_test(
        runtime: Arc<dyn RuntimeAdapter>,
        state: TrieUpdate,
        shard_uid: ShardUId,
        tx_pool: Arc<Mutex<ShardedTransactionPool>>,
    ) -> Self {
        use near_primitives::congestion_info::BlockCongestionInfo;
        use near_primitives::types::{Balance, EpochId};

        Self {
            runtime_adapter: runtime,
            state,
            shard_uid,
            prev_block_context: PrepareTransactionsBlockContext {
                next_gas_price: Balance::ZERO,
                height: 0,
                next_epoch_id: EpochId::default(),
                congestion_info: BlockCongestionInfo::default(),
            },
            tx_pool,
            tx_validity_period_check: Box::new(|_| true),
            prev_chunk_tx_hashes: HashSet::new(),
            time_limit: None,
        }
    }
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
#[derive(PartialEq, Eq, Clone)]
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use near_chain::runtime::NightshadeRuntime;
    use near_chain::types::{PrepareTransactionsBlockContext, PrepareTransactionsLimit};
    use near_chain_configs::test_genesis::TestGenesisBuilder;
    use near_chunks::client::ShardedTransactionPool;
    use near_epoch_manager::EpochManager;
    use near_epoch_manager::test_utils::TEST_SEED;
    use near_pool::InsertTransactionResult;
    use near_primitives::hash::CryptoHash;
    use near_primitives::optimistic_block::CachedShardUpdateKey;
    use near_primitives::test_utils::create_test_signer;
    use near_primitives::transaction::{SignedTransaction, ValidatedTransaction};
    use near_primitives::types::{AccountId, Balance, BlockHeight};
    use near_store::genesis::initialize_genesis_state;
    use near_store::test_utils::{TestTriesBuilder, create_test_store};
    use near_store::{ShardUId, TrieUpdate, get_genesis_state_roots};
    use parking_lot::Mutex;

    use crate::prepare_transactions::{
        PrepareTransactionsJob, PrepareTransactionsJobInputs, PrepareTransactionsJobKey,
        PrepareTransactionsManager,
    };

    fn insert_tx(
        pool: &mut ShardedTransactionPool,
        shard_uid: ShardUId,
        pub_key: near_crypto::PublicKey,
        account_id: AccountId,
        nonce: u64,
    ) {
        let signed_tx = SignedTransaction::new(
            near_crypto::Signature::empty(near_crypto::KeyType::ED25519),
            near_primitives::transaction::Transaction::new_v1(
                account_id,
                pub_key,
                "other".parse().unwrap(),
                nonce,
                CryptoHash::default(),
                0,
            ),
        );
        let validated_tx = ValidatedTransaction::new_for_test(signed_tx);
        assert_eq!(
            InsertTransactionResult::Success,
            pool.insert_transaction(shard_uid, validated_tx)
        );
    }

    // Sets up store, genesis, runtime, tries and state for a single account,
    // Returns a Nightshade runtime and a TrieUpdate on top of the genesis state,
    // suitable for creating PrepareTransactionsJob.
    fn setup_state(account_id: AccountId) -> (Arc<NightshadeRuntime>, TrieUpdate) {
        let store = create_test_store();
        let genesis = TestGenesisBuilder::new()
            .add_user_account_simple(account_id, Balance::from_near(1))
            .build();
        let tempdir = tempfile::tempdir().unwrap();
        initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let runtime =
            NightshadeRuntime::test(tempdir.path(), store.clone(), &genesis.config, epoch_manager);
        let roots = get_genesis_state_roots(&store)
            .expect("Error getting genesis state roots")
            .expect("Genesis state roots must exist");
        let root = roots.iter().next().expect("Genesis state root for shard must exist");
        let tries = TestTriesBuilder::new().with_store(store).build();
        (runtime, tries.new_trie_update(ShardUId::single_shard(), *root))
    }

    // Sets up a transaction pool with `num_txs` transactions from `account_id`
    fn setup_pool(
        account_id: AccountId,
        shard_uid: ShardUId,
        num_txs: usize,
    ) -> Arc<Mutex<ShardedTransactionPool>> {
        let signer = create_test_signer(account_id.as_str());
        let tx_pool = Arc::new(Mutex::new(ShardedTransactionPool::new(TEST_SEED, None)));
        let mut pool_guard = tx_pool.lock();
        for nonce in 1..=num_txs as u64 {
            insert_tx(&mut pool_guard, shard_uid, signer.public_key(), account_id.clone(), nonce);
        }
        drop(pool_guard);
        tx_pool
    }

    #[test]
    fn test_prepare_transactions_job() {
        let account_id: AccountId = "test".parse().unwrap();
        let shard_uid = ShardUId::single_shard();
        let (runtime, state) = setup_state(account_id.clone());
        let tx_pool = setup_pool(account_id, shard_uid, 1);

        let inputs = PrepareTransactionsJobInputs::new_for_test(runtime, state, shard_uid, tx_pool);
        let job = PrepareTransactionsJob::new(inputs);
        job.wait();
        let result =
            job.take().expect("result must be available after wait").expect("job must succeed");
        assert_eq!(None, result.limited_by);
        assert_eq!(1, result.transactions.len());

        // Taking again returns None
        let result = job.take();
        assert!(result.is_none());
    }

    fn assert_cancelled(job: &PrepareTransactionsJob) {
        job.wait();
        let result =
            job.take().expect("result must be available after wait").expect("job must succeed");
        assert_eq!(Some(PrepareTransactionsLimit::Cancelled), result.limited_by);
        assert!(result.transactions.is_empty());
    }

    #[test]
    fn test_prepare_transactions_job_cancel() {
        let account_id: AccountId = "test".parse().unwrap();
        let shard_uid = ShardUId::single_shard();
        let (runtime, state) = setup_state(account_id.clone());
        let tx_pool = setup_pool(account_id, shard_uid, 1);
        let inputs = PrepareTransactionsJobInputs::new_for_test(runtime, state, shard_uid, tx_pool);
        let job = PrepareTransactionsJob::new(inputs);
        job.cancel();
        assert_cancelled(&job);
    }

    fn make_key(
        height: BlockHeight,
        shard_uid: ShardUId,
        shard_update_key_inner: CryptoHash,
    ) -> PrepareTransactionsJobKey {
        PrepareTransactionsJobKey {
            shard_uid,
            shard_update_key: CachedShardUpdateKey::new(shard_update_key_inner),
            prev_block_context: PrepareTransactionsBlockContext {
                next_gas_price: Balance::ZERO,
                height,
                next_epoch_id: Default::default(),
                congestion_info: Default::default(),
            },
        }
    }

    #[test]
    fn test_prepare_transactions_manager() {
        let account_id: AccountId = "test".parse().unwrap();
        let shard_uid = ShardUId::single_shard();
        let (runtime, state) = setup_state(account_id.clone());
        let tx_pool = setup_pool(account_id, shard_uid, 1);

        let inputs = PrepareTransactionsJobInputs::new_for_test(runtime, state, shard_uid, tx_pool);
        let key = make_key(0, shard_uid, CryptoHash::default());
        let mut manager = PrepareTransactionsManager::new();
        let _ = manager.push(key.clone(), inputs);
        let result =
            manager.pop_job_result(key).expect("should get result").expect("job must succeed");
        assert_eq!(1, result.transactions.len());
    }

    #[test]
    fn test_prepare_transactions_manager_pop_job_with_different_key() {
        let account_id: AccountId = "test".parse().unwrap();
        let shard_uid = ShardUId::single_shard();
        let (runtime, state) = setup_state(account_id.clone());
        let tx_pool = setup_pool(account_id, shard_uid, 1);
        let inputs = PrepareTransactionsJobInputs::new_for_test(runtime, state, shard_uid, tx_pool);
        let key = make_key(0, shard_uid, CryptoHash::default());
        let mut manager = PrepareTransactionsManager::new();
        let job = manager.push(key, inputs);
        // Try to pop with a different key
        let different_key = make_key(0, shard_uid, CryptoHash::hash_bytes(b"different"));
        assert!(manager.pop_job_result(different_key).is_none());
        assert_cancelled(&job);
    }

    #[test]
    fn test_prepare_transactions_manager_push_same_height_and_shard() {
        let account_id: AccountId = "test".parse().unwrap();
        let shard_uid = ShardUId::single_shard();
        let (runtime, state) = setup_state(account_id.clone());
        let tx_pool = setup_pool(account_id, shard_uid, 1);
        let inputs1 = PrepareTransactionsJobInputs::new_for_test(
            runtime.clone(),
            state.clone_for_tx_preparation(), // This is good enough for the test
            shard_uid,
            tx_pool.clone(),
        );
        let key1 = make_key(0, shard_uid, CryptoHash::default());
        let mut manager = PrepareTransactionsManager::new();
        let job1 = manager.push(key1, inputs1);

        // Create a second job with the same (height, shard_id) but different key
        let inputs2 =
            PrepareTransactionsJobInputs::new_for_test(runtime, state, shard_uid, tx_pool);
        let key2 = make_key(0, shard_uid, CryptoHash::hash_bytes(b"different"));
        let _job2 = manager.push(key2.clone(), inputs2);

        // The first job should be cancelled
        assert_cancelled(&job1);

        // The second job should complete successfully
        let result =
            manager.pop_job_result(key2).expect("should get result").expect("job must succeed");
        assert_eq!(1, result.transactions.len());
    }
}
