use crate::Provenance;
use crate::chain::BlockMissingChunks;
use crate::near_chain_primitives::error::BlockKnownError::KnownInProcessing;
use crate::orphan::OrphanMissingChunks;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::time::Instant;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::{BlockToApply, CachedShardUpdateKey, OptimisticBlock};
use near_primitives::sharding::{ReceiptProof, ShardChunkHeader, StateSyncInfo};
use near_primitives::types::{BlockHeight, ShardId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Max number of blocks that can be in the pool at once.
/// This number will likely never be hit unless there are many forks in the chain.
pub(crate) const MAX_PROCESSING_BLOCKS: usize = 5;

/// Contains information from preprocessing a block
pub(crate) struct BlockPreprocessInfo {
    /// This field has two related but actually different meanings. For the first block of an
    /// epoch, this will be set to false if we need to download state for shards we'll track in
    /// the future but don't track currently. This implies the first meaning, which is that if
    /// this is true, then we are ready to apply all chunks and update flat state for shards
    /// we'll track in this and the next epoch. This comes into play when we decide what ApplyChunksMode
    /// to pass to Chain::apply_chunks_preprocessing().
    /// The other meaning is that the catchup code should process this block. When the state sync sync_hash
    /// is the first block of the epoch, these two meanings are the same. But if the sync_hash is moved forward
    /// in order to sync the current epoch's state instead of last epoch's, this field being false no longer implies
    /// that we want to apply this block during catchup, so some care is needed to ensure we start catchup at the right
    /// point in Client::run_catchup()
    pub(crate) is_caught_up: bool,
    pub(crate) state_sync_info: Option<StateSyncInfo>,
    pub(crate) incoming_receipts: HashMap<ShardId, Vec<ReceiptProof>>,
    pub(crate) provenance: Provenance,
    /// Used to get notified when the applying chunks of a block finishes.
    pub(crate) apply_chunks_done_waiter: ApplyChunksDoneWaiter,
    /// Used to calculate block processing time metric.
    pub(crate) block_start_processing_time: Instant,
}

pub(crate) struct OptimisticBlockInfo {
    /// Used to get notified when the applying chunks of a block finishes.
    pub(crate) apply_chunks_done_waiter: ApplyChunksDoneWaiter,
    /// Used to calculate processing time metric.
    pub(crate) block_start_processing_time: Instant,
    /// Shard update keys for the processed chunks.
    pub(crate) shard_update_keys: Vec<CachedShardUpdateKey>,
}

/// Blocks which finished pre-processing and are now being applied asynchronously
pub(crate) struct BlocksInProcessing {
    // A map that stores all blocks in processing
    preprocessed_blocks: HashMap<CryptoHash, (Arc<Block>, BlockPreprocessInfo)>,
    optimistic_blocks: HashMap<BlockHeight, (OptimisticBlock, OptimisticBlockInfo)>,
}

#[derive(Debug)]
pub(crate) enum AddError {
    ExceedingPoolSize,
    BlockAlreadyInPool,
}

impl From<AddError> for near_chain_primitives::Error {
    fn from(err: AddError) -> Self {
        match err {
            AddError::ExceedingPoolSize => near_chain_primitives::Error::TooManyProcessingBlocks,
            AddError::BlockAlreadyInPool => {
                near_chain_primitives::Error::BlockKnown(KnownInProcessing)
            }
        }
    }
}

/// Results from processing a block that are useful for client and client actor to use
/// for steps after a block is processed that can't be finished inside Chain after a block is processed
/// (for example, sending requests for missing chunks or challenges).
/// This struct is passed to Chain::process_block as an argument instead of returned as Result,
/// because the information stored here need to returned whether process_block succeeds or returns an error.
#[derive(Default)]
pub struct BlockProcessingArtifact {
    pub orphans_missing_chunks: Vec<OrphanMissingChunks>,
    pub blocks_missing_chunks: Vec<BlockMissingChunks>,
    pub invalid_chunks: Vec<ShardChunkHeader>,
}

#[derive(Debug)]
pub struct BlockNotInPoolError;

impl BlocksInProcessing {
    pub(crate) fn new() -> Self {
        BlocksInProcessing {
            preprocessed_blocks: HashMap::new(),
            optimistic_blocks: HashMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.preprocessed_blocks.len() + self.optimistic_blocks.len()
    }

    /// Add a preprocessed block to the pool. Return Error::ExceedingPoolSize if the pool already
    /// reaches its max size.
    pub(crate) fn add(
        &mut self,
        block: Arc<Block>,
        preprocess_info: BlockPreprocessInfo,
    ) -> Result<(), AddError> {
        self.add_dry_run(&BlockToApply::Normal(*block.hash()))?;

        self.preprocessed_blocks.insert(*block.hash(), (block, preprocess_info));
        Ok(())
    }

    pub(crate) fn add_optimistic(
        &mut self,
        block: OptimisticBlock,
        preprocess_info: OptimisticBlockInfo,
    ) -> Result<(), AddError> {
        self.add_dry_run(&BlockToApply::Optimistic(block.height()))?;

        self.optimistic_blocks.insert(block.height(), (block, preprocess_info));
        Ok(())
    }

    pub(crate) fn contains(&self, block_to_apply: &BlockToApply) -> bool {
        match block_to_apply {
            BlockToApply::Normal(block_hash) => self.preprocessed_blocks.contains_key(block_hash),
            BlockToApply::Optimistic(block_height) => {
                self.optimistic_blocks.contains_key(block_height)
            }
        }
    }

    pub(crate) fn remove(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Option<(Arc<Block>, BlockPreprocessInfo)> {
        self.preprocessed_blocks.remove(block_hash)
    }

    pub(crate) fn remove_optimistic(
        &mut self,
        block_height: &BlockHeight,
    ) -> Option<(OptimisticBlock, OptimisticBlockInfo)> {
        self.optimistic_blocks.remove(block_height)
    }

    /// This function does NOT add the block, it simply checks if the block can be added
    pub(crate) fn add_dry_run(&self, block_to_apply: &BlockToApply) -> Result<(), AddError> {
        // We set a limit to the max number of blocks that we will be processing at the same time.
        // Since processing a block requires that the its previous block is processed, this limit
        // is likely never hit, unless there are many forks in the chain.
        // In this case, we will simply drop the block.
        if self.len() >= MAX_PROCESSING_BLOCKS {
            Err(AddError::ExceedingPoolSize)
        } else if self.contains(block_to_apply) {
            Err(AddError::BlockAlreadyInPool)
        } else {
            Ok(())
        }
    }

    /// Check if there is an optimistic block in processing for the given
    /// height and shard update keys.
    pub fn has_optimistic_block_with(
        &self,
        block_height: BlockHeight,
        shard_update_keys: &[&CachedShardUpdateKey],
    ) -> bool {
        let Some((_, optimistic_block_info)) = self.optimistic_blocks.get(&block_height) else {
            return false;
        };
        let info_keys: Vec<&CachedShardUpdateKey> =
            optimistic_block_info.shard_update_keys.iter().collect();
        shard_update_keys == info_keys.as_slice()
    }

    pub(crate) fn has_blocks_to_catch_up(&self, prev_hash: &CryptoHash) -> bool {
        self.preprocessed_blocks
            .iter()
            .any(|(_, (block, _))| block.header().prev_hash() == prev_hash)
    }

    /// This function waits until apply_chunks_done is marked as true for all blocks in the pool
    /// Returns true if new blocks are done applying chunks
    pub(crate) fn wait_for_all_blocks(&self) -> bool {
        for (_, (_, block_preprocess_info)) in &self.preprocessed_blocks {
            let _ = block_preprocess_info.apply_chunks_done_waiter.wait();
        }
        for (_, (_, optimistic_block_info)) in &self.optimistic_blocks {
            let _ = optimistic_block_info.apply_chunks_done_waiter.wait();
        }
        !self.preprocessed_blocks.is_empty() || !self.optimistic_blocks.is_empty()
    }

    /// This function waits until apply_chunks_done is marked as true for block `block_hash`
    pub(crate) fn wait_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(), BlockNotInPoolError> {
        let _ = self
            .preprocessed_blocks
            .get(block_hash)
            .ok_or(BlockNotInPoolError)?
            .1
            .apply_chunks_done_waiter
            .wait();
        Ok(())
    }
}

/// The waiter's wait() will block until the corresponding ApplyChunksStillApplying is dropped.
#[derive(Clone)]
pub struct ApplyChunksDoneWaiter(Arc<tokio::sync::Mutex<()>>);
pub struct ApplyChunksStillApplying {
    // We're using tokio's mutex guard, because the std one is not Send.
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl ApplyChunksDoneWaiter {
    pub fn new() -> (Self, ApplyChunksStillApplying) {
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        // Use try_lock_owned() rather than blocking_lock_owned(), because otherwise
        // this causes a panic if we do this on a tokio runtime.
        let guard = lock.clone().try_lock_owned().expect("should succeed on a fresh mutex");
        (ApplyChunksDoneWaiter(lock), ApplyChunksStillApplying { _guard: guard })
    }

    pub fn wait(&self) {
        // TODO(#14005): Unfortunately we cannot block here because we may be in a tokio runtime.
        // We also cannot use futures::executor::block_on to cheat around this, because a locking
        // operation may actually block forever in certain situations. So we use this not very
        // great approach.
        // This is fine though, because this is only used when shutting down the node, and in
        // TestEnv-based integration tests.
        let start = Instant::now();
        for i in 1u64.. {
            // This would only go through if the guard has been dropped.
            if let Ok(_) = self.0.try_lock() {
                return;
            }
            if i % 1000 == 0 {
                // If a node or test is somehow deadlocked on this for some reason, log it to help debugging.
                tracing::error!("still waiting for chunks application to complete");
                debug_assert!(
                    start.elapsed().as_secs() < 30,
                    "Chunk application didn't complete in 30 seconds; is there a deadlock?"
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use near_async::futures::StdThreadAsyncComputationSpawner;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn test_apply_chunks_with_multiple_waiters() {
        let shared_value: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        let (waiter, still_applying) = ApplyChunksDoneWaiter::new();
        let waiter1 = waiter.clone();
        let waiter2 = waiter.clone();
        let waiter3 = waiter;

        let (results_sender, results_receiver) = std::sync::mpsc::channel();

        // Spawn waiter tasks
        for waiter in [waiter1, waiter2, waiter3] {
            let current_sender = results_sender.clone();
            let current_shared_value = shared_value.clone();
            std::thread::spawn(move || {
                waiter.wait();
                let read_value = current_shared_value.load(Ordering::Relaxed);
                current_sender.send(read_value).unwrap();
            });
        }

        // Wait 300ms then set the shared_value to true, and notify the waiters.
        std::thread::sleep(Duration::from_millis(300));
        shared_value.store(true, Ordering::Relaxed);
        drop(still_applying);

        // Check values that waiters read
        for _ in 0..3 {
            let waiter_value = results_receiver.recv().unwrap();
            assert_eq!(waiter_value, true);
        }
    }

    type TestResult = Result<String, Error>;

    #[test]
    fn pending_shard_jobs_results_in_spawn_order() {
        let (tx, rx) = mpsc::channel();
        let pending = PendingShardJobs::<u32, TestResult>::new(
            "test",
            Arc::new(StdThreadAsyncComputationSpawner),
            3,
            move |results| tx.send(results).unwrap(),
        );
        // Task 0 sleeps longest, task 2 returns immediately. Results must
        // still arrive in spawn order (0, 1, 2).
        pending.spawn(0, || {
            std::thread::sleep(Duration::from_millis(20));
            Ok("task 0".to_string())
        });
        pending.spawn(1, || {
            std::thread::sleep(Duration::from_millis(10));
            Ok("task 1".to_string())
        });
        pending.spawn(2, || Ok("task 2".to_string()));

        let results = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(results.len(), 3);
        for (i, (key, result)) in results.into_iter().enumerate() {
            assert_eq!(key, i as u32);
            assert_eq!(result.unwrap(), format!("task {i}"));
        }
    }

    #[test]
    fn pending_shard_jobs_panic_produces_error() {
        let (tx, rx) = mpsc::channel();
        let pending = PendingShardJobs::<u32, TestResult>::new(
            "test",
            Arc::new(StdThreadAsyncComputationSpawner),
            2,
            move |results| tx.send(results).unwrap(),
        );
        pending.spawn(0, || Ok("ok".to_string()));
        pending.spawn(1, || panic!("deliberate panic"));

        let results = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let [(key_0, result_0), (key_1, result_1)] = results.try_into().unwrap();

        assert_eq!(key_0, 0);
        assert_eq!(result_0.unwrap(), "ok");

        assert_eq!(key_1, 1);
        let err = result_1.unwrap_err();
        assert!(err.to_string().contains("deliberate panic"), "{err}");
    }

    #[test]
    fn pending_shard_jobs_zero_count() {
        let (tx, rx) = mpsc::channel();
        PendingShardJobs::<u32, TestResult>::new(
            "test",
            Arc::new(StdThreadAsyncComputationSpawner),
            0,
            move |results| tx.send(results).unwrap(),
        );
        let results = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(results.is_empty());
    }
}

/// Produce a fallback value when a spawned task panics.
pub trait FromPanic {
    fn from_panic(message: String) -> Self;
}

impl<T> FromPanic for Result<T, crate::Error> {
    fn from_panic(message: String) -> Self {
        Err(crate::Error::Other(message))
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast::<String>()
        .map(|s| *s)
        .or_else(|p| p.downcast::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|_| "unknown panic".to_string())
}

/// Spawns `count` tasks on spawner and collects their results in spawn order.
/// When the last task completes, `on_done` is called with all results. If
/// `count` is zero, `on_done` is invoked asynchronously via the spawner.
pub struct PendingShardJobs<K: Send + 'static, R: FromPanic + Send + 'static> {
    name: &'static str,
    spawner: Arc<dyn AsyncComputationSpawner>,
    next_index: AtomicUsize,
    remaining: AtomicUsize,
    results: parking_lot::Mutex<Vec<Option<(K, R)>>>,
    on_done: parking_lot::Mutex<Option<Box<dyn FnOnce(Vec<(K, R)>) + Send>>>,
}

impl<K: Send + 'static, R: FromPanic + Send + 'static> PendingShardJobs<K, R> {
    pub fn new(
        name: &'static str,
        spawner: Arc<dyn AsyncComputationSpawner>,
        count: usize,
        on_done: impl FnOnce(Vec<(K, R)>) + Send + 'static,
    ) -> Arc<Self> {
        let results: Vec<Option<(K, R)>> = (0..count).map(|_| None).collect();
        let pending = Arc::new(Self {
            name,
            spawner,
            next_index: AtomicUsize::new(0),
            remaining: AtomicUsize::new(count),
            results: parking_lot::Mutex::new(results),
            on_done: parking_lot::Mutex::new(Some(Box::new(on_done))),
        });
        if count == 0 {
            // Invoke on_done via the spawner to keep result delivery asynchronous.
            // Some test-loops depend on receiving results as a separate event.
            let pending_clone = pending.clone();
            pending.spawner.spawn(pending.name, move || {
                pending_clone.invoke_on_done();
            });
        }
        pending
    }

    /// Spawn a task. Results are delivered to `on_done` in the order `spawn`
    /// is called, regardless of which task finishes first.
    ///
    /// If `task` panics, `R::from_panic` produces a fallback result, keeping
    /// `on_done` delivery intact.
    pub fn spawn(self: &Arc<Self>, key: K, task: impl FnOnce() -> R + Send + 'static) {
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        let pending = self.clone();
        self.spawner.spawn(self.name, move || {
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)) {
                Ok(value) => value,
                Err(payload) => {
                    let message = panic_payload_to_string(payload);
                    tracing::error!("{}: task panicked: {}", pending.name, message);
                    R::from_panic(message)
                }
            };
            pending.set_result(index, (key, result));
        });
    }

    fn set_result(&self, index: usize, result: (K, R)) {
        self.results.lock()[index] = Some(result);
        if self.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.invoke_on_done();
        }
    }

    fn invoke_on_done(&self) {
        let results =
            std::mem::take(&mut *self.results.lock()).into_iter().map(|r| r.unwrap()).collect();
        let on_done = self.on_done.lock().take().unwrap();
        on_done(results);
    }
}

/// Test-only gate to pause block processing in the spawned apply-chunks task.
/// The gate blocks the task before `do_apply_chunks` runs, preventing results
/// from reaching the channel until `resume` is called.
#[cfg(feature = "test_features")]
#[derive(Default)]
pub struct TestPausedBlocks {
    gates: HashMap<CryptoHash, Arc<std::sync::OnceLock<()>>>,
}

#[cfg(feature = "test_features")]
impl TestPausedBlocks {
    /// Returns the gate for a block, if one is set. The caller passes it into
    /// the spawned task to block before applying chunks.
    pub fn get_gate(&self, block_hash: &CryptoHash) -> Option<Arc<std::sync::OnceLock<()>>> {
        self.gates.get(block_hash).cloned()
    }

    pub fn pause(&mut self, block_hash: &CryptoHash) {
        let prev = self.gates.insert(*block_hash, Arc::new(std::sync::OnceLock::new()));
        assert!(prev.is_none(), "block {block_hash} is already paused");
    }

    pub fn resume(&mut self, block_hash: &CryptoHash) {
        let gate = self.gates.remove(block_hash).expect("block was not paused");
        let _ = gate.set(());
    }

    /// Resumes all paused blocks. Returns true if any were paused.
    pub fn resume_all(&mut self) -> bool {
        let had_paused = !self.gates.is_empty();
        for (_, gate) in self.gates.drain() {
            let _ = gate.set(());
        }
        had_paused
    }
}
