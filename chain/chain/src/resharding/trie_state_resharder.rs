use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_store::adapter::trie_store::{TrieStoreUpdateAdapter, get_shard_uid_mapping};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::db::TRIE_STATE_RESHARDING_STATUS_KEY;
use near_store::metrics::resharding::trie_state_metrics;
use near_store::{DBCol, StorageError};

use crate::resharding::event_type::ReshardingSplitShardParams;
use crate::types::RuntimeAdapter;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_o11y::metrics::IntGauge;
use near_primitives::shard_layout::ShardUId;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
/// Represents the status of one child shard during trie state resharding.
struct TrieStateReshardingChildStatus {
    shard_uid: ShardUId,
    /// The post-state root of the child shard after the resharding block.
    state_root: CryptoHash,
    /// The key to start the next batch from.
    next_key: Option<Vec<u8>>,
    #[borsh(skip)]
    metrics: Option<TrieStateResharderMetrics>,
}

impl PartialEq for TrieStateReshardingChildStatus {
    fn eq(&self, other: &Self) -> bool {
        // ignores metrics for equality check
        self.shard_uid == other.shard_uid
            && self.state_root == other.state_root
            && self.next_key == other.next_key
    }
}
impl Eq for TrieStateReshardingChildStatus {}

impl TrieStateReshardingChildStatus {
    fn new(shard_uid: ShardUId, state_root: CryptoHash) -> Self {
        Self { shard_uid, state_root, next_key: None, metrics: None }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
/// Represents the status of an ongoing trie state resharding.
/// It is used to resume the resharding process after a crash or restart.
struct TrieStateReshardingStatus {
    parent_shard_uid: ShardUId,
    left: Option<TrieStateReshardingChildStatus>,
    right: Option<TrieStateReshardingChildStatus>,
}

impl TrieStateReshardingStatus {
    fn with_metrics(mut self) -> Self {
        for child in [&mut self.left, &mut self.right] {
            child.as_mut().map(|child| {
                child.metrics = Some(TrieStateResharderMetrics::new(&child.shard_uid));
            });
        }
        self
    }

    fn done(&self) -> bool {
        self.left.is_none() && self.right.is_none()
    }
}

/// TrieStateResharder is responsible for handling state resharding operations.
pub struct TrieStateResharder {
    runtime: Arc<dyn RuntimeAdapter>,
    /// Controls cancellation of background processing.
    handle: ReshardingHandle,
    /// Configuration for resharding.
    resharding_config: MutableConfigValue<ReshardingConfig>,
}

impl TrieStateResharder {
    pub fn new(
        runtime: Arc<dyn RuntimeAdapter>,
        handle: ReshardingHandle,
        resharding_config: MutableConfigValue<ReshardingConfig>,
    ) -> Self {
        Self { runtime, handle, resharding_config }
    }

    // Processes one batch of a trie state resharding and updates the status,
    // also persisting the status to the store.
    fn process_batch_and_update_status(
        &self,
        status: &mut TrieStateReshardingStatus,
    ) -> Result<(), Error> {
        let batch_delay = self.resharding_config.get().batch_delay.unsigned_abs();

        let child_ref = if status.left.is_some() { &mut status.left } else { &mut status.right };
        let Some(child) = child_ref else {
            // No more children to process.
            return Ok(());
        };

        // Sleep between batches in order to throttle resharding and leave some resource for the
        // regular node operation.
        std::thread::sleep(batch_delay);
        let _span = tracing::debug_span!(
            target: "resharding",
            "TrieStateResharder::process_batch_and_update_status",
            parent_shard_uid = ?status.parent_shard_uid,
            child_shard_uid = ?child.shard_uid,
        );

        let mut store_update = self.runtime.store().store_update();
        let next_key = self.next_batch(
            child.shard_uid,
            child.state_root,
            child.next_key.clone(),
            &mut store_update.trie_store_update(),
        )?;

        if let Some(metrics) = &child.metrics {
            metrics.inc_processed_batches();
        }
        if let Some(next_key) = next_key {
            child.next_key = Some(next_key);
        } else {
            // No more keys to process for this child shard.
            store_update.trie_store_update().delete_shard_uid_mapping(child.shard_uid);
            *child_ref = None;
        };

        // Commit the changes to the store, along with the status.
        if status.done() {
            store_update.delete(DBCol::Misc, TRIE_STATE_RESHARDING_STATUS_KEY);
        } else {
            store_update.set(
                DBCol::Misc,
                TRIE_STATE_RESHARDING_STATUS_KEY,
                &borsh::to_vec(status)?,
            );
        }
        store_update.commit()?;
        Ok(())
    }

    fn next_batch(
        &self,
        child_shard_uid: ShardUId,
        state_root: CryptoHash,
        seek_key: Option<Vec<u8>>,
        store_update: &mut TrieStoreUpdateAdapter,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let tries = self.runtime.get_tries();
        let trie =
            tries.get_trie_for_shard(child_shard_uid, state_root).recording_reads_new_recorder();

        // Assert that the child shard has memtries loaded during resharding
        debug_assert!(
            trie.has_memtries(),
            "Child shard {:?} should have memtries loaded during trie state resharding",
            child_shard_uid
        );

        let next_key = {
            // If the child shard does not have memtries, we cannot proceed with resharding.
            let locked = trie.lock_for_iter();
            let mut iter = locked.iter()?;
            if let Some(seek_key) = seek_key {
                // If seek_key is provided, this will prepare the iterator to continue from where
                // it left off. Note this will not record any trie nodes to the recorder.
                iter.seek(Bound::Excluded(seek_key))?;
            }

            // During iteration, the trie nodes will be recorded to the recorder, so we
            // don't need to care about the value explicitly. If we reach the batch
            // size, we stop iterating, and remember the key to continue from in the
            // next batch.
            let batch_size = self.resharding_config.get().batch_size.as_u64() as usize;
            let mut next_key: Option<Vec<u8>> = None;
            for item in iter {
                let (key, _val) = item?; // Handle StorageError
                let stats = trie.recorder_stats().expect("trie recorder stats should be available");
                if stats.total_size >= batch_size {
                    next_key = Some(key);
                    break;
                }
            }
            next_key
        };

        // Take the recorded trie changes and apply them to the State column of the child shard.
        let trie_changes =
            trie.recorded_trie_changes(state_root).expect("trie changes should be available");
        tries.apply_all(&trie_changes, child_shard_uid, store_update);
        Ok(next_key)
    }

    fn load_status(&self) -> Result<Option<TrieStateReshardingStatus>, Error> {
        Ok(self
            .runtime
            .store()
            .get_ser::<TrieStateReshardingStatus>(DBCol::Misc, TRIE_STATE_RESHARDING_STATUS_KEY)?)
    }

    /// Start a resharding operation by iterating the memtries of each child shard,
    /// writing the result to the `State` column of the respective shard.
    pub fn start_resharding_blocking(
        &self,
        event: &ReshardingSplitShardParams,
    ) -> Result<(), Error> {
        if let Some(status) = self.load_status()? {
            panic!(
                "TrieStateReshardingStatus already exists for shard {}, cannot start a new resharding operation. Run resume_resharding to continue.",
                status.parent_shard_uid
            );
        }

        // Get state root from the chunk extra of the child shard.
        let block_hash = event.resharding_block.hash;
        let store = self.runtime.store().chain_store();
        let left_state_root =
            *store.get_chunk_extra(&block_hash, &event.left_child_shard)?.state_root();
        let right_state_root =
            *store.get_chunk_extra(&block_hash, &event.right_child_shard)?.state_root();
        tracing::debug!(
            target: "resharding",
            ?left_state_root,
            ?right_state_root,
            ?event.left_child_shard,
            ?event.right_child_shard,
            "TrieStateResharding: child state roots"
        );

        // If the child shard_uid mapping doesn't exist, it means we are not tracking the child shard.
        let store = self.runtime.store();
        let left_child =
            if get_shard_uid_mapping(store, event.left_child_shard) != event.left_child_shard {
                Some(TrieStateReshardingChildStatus::new(event.left_child_shard, left_state_root))
            } else {
                None
            };
        let right_child =
            if get_shard_uid_mapping(store, event.right_child_shard) != event.right_child_shard {
                Some(TrieStateReshardingChildStatus::new(event.right_child_shard, right_state_root))
            } else {
                None
            };

        let mut status = TrieStateReshardingStatus {
            parent_shard_uid: event.parent_shard,
            left: left_child,
            right: right_child,
        }
        .with_metrics();
        self.resharding_blocking_impl(&mut status)
    }

    /// Resume an interrupted resharding operation.
    pub fn resume(&self, parent_shard_uid: ShardUId) -> Result<(), Error> {
        let Some(status) = self.load_status()? else {
            tracing::info!(target: "resharding", "Resharding status not found, nothing to resume.");
            return Ok(());
        };

        if status.parent_shard_uid != parent_shard_uid {
            return Err(Error::ReshardingError(format!(
                "Resharding status shard UID {} does not match the provided shard UID {}.",
                status.parent_shard_uid, parent_shard_uid
            )));
        }
        let mut status = status.with_metrics();
        self.resharding_blocking_impl(&mut status)
    }

    fn resharding_blocking_impl(
        &self,
        status: &mut TrieStateReshardingStatus,
    ) -> Result<(), Error> {
        while !status.done() && !self.handle.is_cancelled() {
            self.process_batch_and_update_status(status)?;
        }

        Ok(())
    }
}

impl Debug for TrieStateResharder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrieStateResharder").field("handle", &self.handle).finish()
    }
}

/// Metrics for tracking store column update during resharding.
#[derive(Debug)]
struct TrieStateResharderMetrics {
    processed_batches: IntGauge,
}

impl TrieStateResharderMetrics {
    pub fn new(shard_uid: &ShardUId) -> Self {
        let processed_batches = trie_state_metrics::STATE_COL_RESHARDING_PROCESSED_BATCHES
            .with_label_values(&[&shard_uid.to_string()]);
        Self { processed_batches }
    }

    pub fn inc_processed_batches(&self) {
        self.processed_batches.inc();
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use itertools::Itertools;
    use near_async::time::Clock;
    use near_chain_configs::Genesis;
    use near_epoch_manager::EpochManager;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::trie_key::TrieKey;
    use near_store::Trie;
    use near_store::test_utils::{
        TestTriesBuilder, create_test_store, simplify_changes, test_populate_trie,
    };
    use near_store::trie::ops::resharding::RetainMode;

    use crate::runtime::NightshadeRuntime;
    use crate::types::ChainConfig;

    use super::*;

    type KeyValues = Vec<(Vec<u8>, Option<Vec<u8>>)>;
    struct TestSetup {
        runtime: Arc<dyn RuntimeAdapter>,
        initial: KeyValues,
        parent_shard: ShardUId,
        left_shard: ShardUId,
        right_shard: ShardUId,
        left_root: CryptoHash,
        right_root: CryptoHash,
    }

    impl TestSetup {
        fn as_status(&self) -> TrieStateReshardingStatus {
            TrieStateReshardingStatus {
                parent_shard_uid: self.parent_shard,
                left: Some(TrieStateReshardingChildStatus::new(self.left_shard, self.left_root)),
                right: Some(TrieStateReshardingChildStatus::new(self.right_shard, self.right_root)),
            }
        }
    }

    fn setup_test() -> TestSetup {
        let shard_layout = ShardLayout::single_shard();
        let genesis = Genesis::from_accounts(
            Clock::real(),
            vec!["aa".parse().unwrap()],
            1,
            shard_layout.clone(),
        );
        let tempdir = tempfile::tempdir().unwrap();
        let store = create_test_store();

        // This enables the flat storage and in-memory tries for testing.
        // It sets FlatStorageReadyStatus and creates an empty ChunkExtra.
        let _tries = TestTriesBuilder::new()
            .with_shard_layout(shard_layout.clone())
            .with_store(store.clone())
            .with_flat_storage(true)
            .with_in_memory_tries(true)
            .build();

        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let runtime =
            NightshadeRuntime::test(tempdir.path(), store, &genesis.config, epoch_manager);

        let parent_shard = ShardUId::single_shard();
        let tries = runtime.get_tries();
        tries.load_memtrie(&parent_shard, None, false).unwrap();

        let make_account_key = |i: usize| {
            let account_str = format!("account-{:02}", i);
            TrieKey::Account { account_id: account_str.parse().unwrap() }
        };

        // Using `TrieKey::Account` to create the trie keys. This is so in
        // resharding, the keys can be split by `AccountId`. Deliberately
        // contains duplicate values to test reference counting
        let initial =
            (0..1000).map(|i| (make_account_key(i).to_vec(), Some(vec![i as u8]))).collect_vec();
        let initial = simplify_changes(&initial); // Sorts & deduplicates the changes

        let parent_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, parent_shard, initial.clone());

        // Create tries to represent the post-state root at the end of the resharding block.
        // Each child shard will have its post-state. Here the test uses the midpoint of the
        // initial keys to split the parent trie to two roughly equally sized children.
        let boundary_account =
            make_account_key(initial.len() / 2 as usize).get_account_id().unwrap();
        let new_layout = ShardLayout::derive_shard_layout(&shard_layout, boundary_account.clone());
        let children = new_layout.get_children_shards_uids(parent_shard.shard_id()).unwrap();
        assert_eq!(2, children.len());

        let block_height = 1;
        let parent_trie = tries.get_trie_for_shard(parent_shard, parent_root);
        let (left_shard, right_shard) = (children[0], children[1]);

        let mut store_update = runtime.store().trie_store().store_update();
        let [left_root, right_root] = [RetainMode::Left, RetainMode::Right].map(|retain_mode| {
            let trie_changes =
                parent_trie.retain_split_shard(&boundary_account, retain_mode).unwrap();
            tries.apply_insertions(&trie_changes, parent_shard, &mut store_update);
            tries.apply_memtrie_changes(&trie_changes, parent_shard, block_height);
            trie_changes.new_root
        });
        // Add a mapping from the child shards to the parent shard. This is not
        // needed as TrieStateResharder will iterate the memtrie, not consult
        // disk. Adding here just to test they will be removed.
        store_update.set_shard_uid_mapping(left_shard, parent_shard);
        store_update.set_shard_uid_mapping(right_shard, parent_shard);
        store_update.commit().unwrap();

        tries.freeze_parent_memtrie(parent_shard, children).unwrap();

        TestSetup { runtime, initial, parent_shard, left_shard, right_shard, left_root, right_root }
    }

    #[test]
    fn test_trie_state_resharder() {
        let test = setup_test();

        let config = ChainConfig::test().resharding_config;
        let resharder =
            TrieStateResharder::new(test.runtime.clone(), ReshardingHandle::new(), config);

        let mut update_status = test.as_status();
        resharder.resharding_blocking_impl(&mut update_status).unwrap();
        // The resharding status should be None after completion.
        assert!(resharder.load_status().unwrap().is_none());
        check_child_tries_contain_all_keys(&test);
        // StateShardUIdMapping should be removed after resharding.
        assert_eq!(0, test.runtime.store().iter(DBCol::StateShardUIdMapping).count());
    }

    #[test]
    fn test_trie_state_resharder_interrupt_and_resume() {
        let test = setup_test();

        let config = ChainConfig::test().resharding_config;
        let resharder =
            TrieStateResharder::new(test.runtime.clone(), ReshardingHandle::new(), config);

        // Set the batch size to 1, this should stop iteration after the first key.
        resharder
            .resharding_config
            .update(ReshardingConfig { batch_size: ByteSize(1), ..ReshardingConfig::test() });
        let mut update_status = test.as_status();
        resharder.process_batch_and_update_status(&mut update_status).unwrap();

        let got_status = resharder
            .load_status()
            .unwrap()
            .expect("status should not be empty after processing one batch");
        assert_eq!(update_status, got_status);
        // The persisted status should indicate continuing from the expected next key,
        // which is the first key in the left child.
        assert_eq!(
            &test.initial.first().unwrap().0,
            update_status.left.as_ref().unwrap().next_key.as_ref().unwrap()
        );
        // StateShardUIdMapping should still be present after processing one batch.
        assert_eq!(2, test.runtime.store().iter(DBCol::StateShardUIdMapping).count());

        // Test cancelling the resharding operation.
        resharder.handle.stop();
        resharder.resharding_blocking_impl(&mut update_status).unwrap();
        // The resharding status should not have changed after cancellation.
        assert_eq!(got_status, update_status);

        // Test resuming the resharding operation.
        let config = ChainConfig::test().resharding_config;
        let resharder =
            TrieStateResharder::new(test.runtime.clone(), ReshardingHandle::new(), config);
        resharder.resume(test.parent_shard).expect("resume should succeed");

        // The resharding status should be None after completion.
        assert!(resharder.load_status().unwrap().is_none());
        check_child_tries_contain_all_keys(&test);
        // StateShardUIdMapping should be removed after resharding.
        assert_eq!(0, test.runtime.store().iter(DBCol::StateShardUIdMapping).count());
    }

    fn check_child_tries_contain_all_keys(test: &TestSetup) {
        let tries = test.runtime.get_tries();
        // Using view_trie to bypass the memtrie and read from disk.
        let left_trie = tries.get_view_trie_for_shard(test.left_shard, test.left_root);
        let left_kvs = left_trie.lock_for_iter().iter().unwrap().map(Result::unwrap).collect_vec();
        let right_trie = tries.get_view_trie_for_shard(test.right_shard, test.right_root);
        let right_kvs =
            right_trie.lock_for_iter().iter().unwrap().map(Result::unwrap).collect_vec();
        assert!(!left_trie.has_memtries() && !right_trie.has_memtries());

        // Iterate the child shards and check we can access the key/values.
        // Note `test.initial` was already sorted and deduplicated, and iteration
        // over the child tries should yield the key/values in sorted order.
        let expected_keys = &test.initial;
        expected_keys.iter().zip_eq(left_kvs.iter().chain(right_kvs.iter())).for_each(
            |(expected, got)| {
                assert_eq!(expected.0, got.0);
                assert_eq!(expected.1.as_ref().unwrap(), &got.1);
            },
        );
    }
}
