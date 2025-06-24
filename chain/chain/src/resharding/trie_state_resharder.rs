use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::trie_store::{TrieStoreUpdateAdapter, get_shard_uid_mapping};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::db::TRIE_STATE_RESHARDING_STATUS_KEY;
use near_store::metrics::resharding::trie_state_metrics;
use near_store::trie::ops::resharding::RetainMode;
use near_store::{DBCol, StorageError};

use crate::resharding::event_type::ReshardingSplitShardParams;
use crate::types::RuntimeAdapter;
use itertools::Itertools;
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
    boundary_account: AccountId,
    resharding_block_height: BlockHeight,
    parent_state_root: CryptoHash,
}

impl TrieStateReshardingStatus {
    fn new(
        parent_shard_uid: ShardUId,
        left: Option<TrieStateReshardingChildStatus>,
        right: Option<TrieStateReshardingChildStatus>,
        boundary_account: AccountId,
        resharding_block_height: BlockHeight,
        parent_state_root: CryptoHash,
    ) -> Self {
        Self {
            parent_shard_uid,
            left,
            right,
            boundary_account,
            resharding_block_height,
            parent_state_root,
        }
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeAllowed {
    Yes,
    No,
}

impl TrieStateResharder {
    pub fn new(
        runtime: Arc<dyn RuntimeAdapter>,
        handle: ReshardingHandle,
        resharding_config: MutableConfigValue<ReshardingConfig>,
        resume_allowed: ResumeAllowed,
    ) -> Self {
        let resharder = Self { runtime, handle, resharding_config };
        if resume_allowed == ResumeAllowed::No {
            // Load the status to check if resharding is in progress
            if let Some(status) = resharder.load_status().unwrap() {
                panic!(
                    "TrieStateReshardingStatus already exists for shard {}, must run resume_resharding to continue interrupted resharding operation before starting node.",
                    status.parent_shard_uid
                )
            }
        }
        resharder
    }

    // Processes one batch of a trie state resharding and updates the status,
    // also persisting the status to the store.
    fn process_batch_and_update_status(
        &self,
        status: &mut TrieStateReshardingStatus,
        expect_memtries: bool,
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
            expect_memtries,
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
        expect_memtries: bool,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let tries = self.runtime.get_tries();
        let trie =
            tries.get_trie_for_shard(child_shard_uid, state_root).recording_reads_new_recorder();

        // Assert that the child shard has memtries loaded during resharding
        debug_assert!(
            trie.has_memtries() || !expect_memtries,
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

    /// Initializes the trie state resharding status for a new resharding operation.
    /// This is done by the resharding actor at the beginning of a resharding operation, before starting
    /// flat storage resharding.
    /// This is needed so that any future calls to resume_resharding will split the parent shard
    pub fn initialize_trie_state_resharding_status(
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

        // We need the parent state root for memtrie recreation.
        let parent_state_root =
            *store.get_chunk_extra(&block_hash, &event.parent_shard)?.state_root();

        tracing::debug!(
            target: "resharding",
            ?left_state_root,
            ?right_state_root,
            ?parent_state_root,
            ?event.left_child_shard,
            ?event.right_child_shard,
            ?event.parent_shard,
            "TrieStateResharding: child and parent state roots"
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

        let status = TrieStateReshardingStatus::new(
            event.parent_shard,
            left_child,
            right_child,
            event.boundary_account.clone(),
            event.resharding_block.height,
            parent_state_root,
        );

        let mut store_update = self.runtime.store().store_update();
        store_update.set(DBCol::Misc, TRIE_STATE_RESHARDING_STATUS_KEY, &borsh::to_vec(&status)?);
        store_update.commit()?;

        Ok(())
    }

    /// Start a resharding operation by iterating the memtries of each child shard,
    /// writing the result to the `State` column of the respective shard.
    pub fn start_resharding_blocking(
        &self,
        event: &ReshardingSplitShardParams,
    ) -> Result<(), Error> {
        let Some(status) = self.load_status()? else {
            panic!(
                "TrieStateReshardingStatus not found. Have we called initialize_trie_state_resharding_status?"
            );
        };

        tracing::info!(target: "resharding", ?status, ?event, "start_resharding_blocking");

        let mut status = status.with_metrics();
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

        // Before resuming, ensure child memtries exist for both children.
        self.ensure_child_memtries_exist(&status)?;

        let mut status = status.with_metrics();
        self.resharding_blocking_impl(&mut status)
    }

    /// Ensures that child memtries exist for both children, recreating them if necessary.
    fn ensure_child_memtries_exist(&self, status: &TrieStateReshardingStatus) -> Result<(), Error> {
        let tries = self.runtime.get_tries();

        let children_missing_memtrie = [
            status.left.as_ref().map(|child| (child.shard_uid, RetainMode::Left)),
            status.right.as_ref().map(|child| (child.shard_uid, RetainMode::Right)),
        ]
        .into_iter()
        .flatten()
        .filter(|(shard_uid, _)| tries.get_memtries(*shard_uid).is_none())
        .collect_vec();

        if !children_missing_memtrie.is_empty() {
            self.recreate_child_memtries(status, children_missing_memtrie)?;
        }

        Ok(())
    }

    fn recreate_child_memtries(
        &self,
        status: &TrieStateReshardingStatus,
        missing_children: Vec<(ShardUId, RetainMode)>,
    ) -> Result<(), Error> {
        let tries = self.runtime.get_tries();
        let parent_shard_uid = status.parent_shard_uid;
        let boundary_account = &status.boundary_account;
        let block_height = status.resharding_block_height;

        // Parent memtrie must be loaded before proceeding.
        if tries.get_memtries(parent_shard_uid).is_none() {
            tracing::info!(
                target: "resharding",
                ?parent_shard_uid,
                parent_state_root = ?status.parent_state_root,
                "Parent memtrie not loaded, loading it now"
            );
            tries.load_memtrie(&parent_shard_uid, None, true).map_err(|e| {
                Error::Other(format!(
                    "Failed to load parent memtrie for shard {parent_shard_uid}: {e}"
                ))
            })?;
        }

        let parent_trie = tries.get_trie_for_shard(parent_shard_uid, status.parent_state_root);

        if !parent_trie.has_memtries() {
            return Err(Error::Other(format!(
                "Parent memtrie for shard {parent_shard_uid} does not exist or is not loaded"
            )));
        }

        let mut store_update = self.runtime.store().trie_store().store_update();

        // Recreate each missing child memtrie.
        for (child_shard_uid, retain_mode) in &missing_children {
            tracing::info!(
                target: "resharding",
                ?child_shard_uid,
                ?parent_shard_uid,
                ?boundary_account,
                ?retain_mode,
                ?block_height,
                "Recreating child memtrie from parent"
            );

            // Perform the shard split operation.
            let trie_changes = parent_trie.retain_split_shard(boundary_account, *retain_mode)?;

            tries.apply_insertions(&trie_changes, parent_shard_uid, &mut store_update);
            tries.apply_memtrie_changes(&trie_changes, parent_shard_uid, block_height);

            tracing::info!(
                target: "resharding",
                ?child_shard_uid,
                new_root = ?trie_changes.new_root,
                "Successfully recreated child memtrie from parent"
            );
        }

        store_update.commit().unwrap();

        // After creating all the child memtries, freeze the parent.
        let children_shard_uids = missing_children.into_iter().map(|(uid, _)| uid).collect_vec();
        tries.freeze_parent_memtrie(parent_shard_uid, children_shard_uids)?;

        Ok(())
    }

    fn resharding_blocking_impl(
        &self,
        status: &mut TrieStateReshardingStatus,
    ) -> Result<(), Error> {
        while !status.done() && !self.handle.is_cancelled() {
            self.process_batch_and_update_status(status, true)?;
        }

        // If resharding completed successfully, clean up parent flat storage.
        if status.done() {
            self.cleanup_parent_flat_storage(status.parent_shard_uid);
        }

        Ok(())
    }

    /// Cleans up parent flat storage after trie state resharding is complete.
    fn cleanup_parent_flat_storage(&self, parent_shard_uid: ShardUId) {
        tracing::info!(
            target: "resharding",
            ?parent_shard_uid,
            "Trie state resharding complete, cleaning up parent flat storage"
        );

        let flat_store = self.runtime.store().flat_store();
        let mut store_update = flat_store.store_update();

        if !self
            .runtime
            .get_flat_storage_manager()
            .remove_flat_storage_for_shard(parent_shard_uid, &mut store_update)
            .unwrap()
        {
            store_update.remove_flat_storage(parent_shard_uid);
        }

        store_update.commit().unwrap();
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
    use near_primitives::shard_layout::{ShardLayout, get_block_shard_uid};
    use near_primitives::trie_key::TrieKey;
    use near_store::Trie;
    use near_store::test_utils::{
        TestTriesBuilder, create_test_store, simplify_changes, test_populate_trie,
    };
    use near_store::trie::ops::resharding::RetainMode;

    use crate::runtime::NightshadeRuntime;
    use crate::types::ChainConfig;

    use super::*;
    use near_primitives::bandwidth_scheduler::BandwidthRequests;
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::state::FlatStateValue;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_store::flat::{FlatStorageReadyStatus, FlatStorageStatus};

    type KeyValues = Vec<(Vec<u8>, Option<Vec<u8>>)>;
    struct TestSetup {
        runtime: Arc<dyn RuntimeAdapter>,
        initial: KeyValues,
        parent_shard: ShardUId,
        left_shard: ShardUId,
        right_shard: ShardUId,
        parent_root: CryptoHash,
        left_root: CryptoHash,
        right_root: CryptoHash,
        boundary_account: AccountId,
        resharding_block_height: BlockHeight,
    }

    impl TestSetup {
        fn as_status(&self) -> TrieStateReshardingStatus {
            TrieStateReshardingStatus::new(
                self.parent_shard,
                Some(TrieStateReshardingChildStatus::new(self.left_shard, self.left_root)),
                Some(TrieStateReshardingChildStatus::new(self.right_shard, self.right_root)),
                self.boundary_account.clone(),
                self.resharding_block_height,
                self.parent_root,
            )
        }
    }

    /// Sets up a test environment for trie state resharding.
    ///
    /// # Parameters
    ///
    /// * `create_memtries`:
    ///   - `true`: Creates and keeps all memtries (parent + children) in memory.
    ///     This simulates the normal case where memtries are available during resharding.
    ///   - `false`: Skips creation of children memtries, unloads the parent memtrie and
    ///     sets up flat storage instead. This simulates resharding interruption and recovery
    ///     where memtries need to be loaded from flat storage during resume().
    fn setup_test(create_memtries: bool) -> TestSetup {
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
            if create_memtries {
                tries.apply_memtrie_changes(&trie_changes, parent_shard, block_height);
            }
            trie_changes.new_root
        });
        // Add a mapping from the child shards to the parent shard. This is not
        // needed as TrieStateResharder will iterate the memtrie, not consult
        // disk. Adding here just to test they will be removed.
        store_update.set_shard_uid_mapping(left_shard, parent_shard);
        store_update.set_shard_uid_mapping(right_shard, parent_shard);
        store_update.commit().unwrap();

        if create_memtries {
            tries.freeze_parent_memtrie(parent_shard, children).unwrap();
        } else {
            // If not creating memtries, we need to set up flat storage properly
            // so that the parent memtrie can be loaded later during resume.

            // First, create flat storage for the parent shard.
            runtime.get_flat_storage_manager().create_flat_storage_for_shard(parent_shard).unwrap();

            // Second, populate flat storage with the trie data.
            let flat_store = runtime.store().flat_store();
            let mut store_update = flat_store.store_update();
            let parent_trie = tries.get_trie_for_shard(parent_shard, parent_root);
            let iter = parent_trie.lock_for_iter();
            for item in iter.iter().unwrap() {
                let (key, value) = item.unwrap();
                store_update.set(parent_shard, key, Some(FlatStateValue::Inlined(value)));
            }

            // Third, set up flat storage status to Ready with the parent state root.
            store_update.set_flat_storage_status(
                parent_shard,
                FlatStorageStatus::Ready(FlatStorageReadyStatus {
                    flat_head: near_store::flat::BlockInfo {
                        hash: parent_root,
                        height: block_height,
                        prev_hash: CryptoHash::default(),
                    },
                }),
            );
            store_update.commit().unwrap();

            // Fourth, create ChunkExtra for the flat storage head so load_memtrie can find the state root.
            let mut chain_store_update = runtime.store().store_update();
            let chunk_extra = ChunkExtra::new(
                &parent_root,
                CryptoHash::default(),
                Vec::new(),
                0,
                0,
                0,
                Some(CongestionInfo::default()),
                BandwidthRequests::empty(),
            );
            let block_shard_uid = get_block_shard_uid(&parent_root, &parent_shard);
            chain_store_update
                .set_ser(near_store::DBCol::ChunkExtra, &block_shard_uid, &chunk_extra)
                .unwrap();
            chain_store_update.commit().unwrap();

            // Now unload the parent memtrie, so the test can verify
            // it will get loaded correctly during resume.
            tries.unload_memtrie(&parent_shard);
        }

        TestSetup {
            runtime,
            initial,
            parent_shard,
            left_shard,
            right_shard,
            parent_root,
            left_root,
            right_root,
            boundary_account,
            resharding_block_height: block_height,
        }
    }

    #[test]
    fn test_trie_state_resharder() {
        let test = setup_test(true);

        let config = ChainConfig::test().resharding_config;
        let resharder = TrieStateResharder::new(
            test.runtime.clone(),
            ReshardingHandle::new(),
            config,
            ResumeAllowed::No,
        );

        let mut update_status = test.as_status();
        resharder.resharding_blocking_impl(&mut update_status).unwrap();
        // The resharding status should be None after completion.
        assert!(resharder.load_status().unwrap().is_none());
        check_child_tries_contain_all_keys(&test);
        // StateShardUIdMapping should be removed after resharding.
        assert_eq!(0, test.runtime.store().iter(DBCol::StateShardUIdMapping).count());
    }

    fn test_trie_state_resharder_interrupt_and_resume_impl(missing_memtries: bool) {
        let test = setup_test(!missing_memtries);

        let config = ChainConfig::test().resharding_config;
        let resharder = TrieStateResharder::new(
            test.runtime.clone(),
            ReshardingHandle::new(),
            config,
            ResumeAllowed::No,
        );

        // Set the batch size to 1, this should stop iteration after the first key.
        resharder
            .resharding_config
            .update(ReshardingConfig { batch_size: ByteSize(1), ..ReshardingConfig::test() });
        let mut update_status = test.as_status();

        if missing_memtries {
            // Verify all memtries don't exist.
            let tries = test.runtime.get_tries();
            assert!(
                tries.get_memtries(test.parent_shard).is_none(),
                "Parent memtrie should not exist"
            );
            assert!(
                tries.get_memtries(test.left_shard).is_none(),
                "Left child memtrie should not exist"
            );
            assert!(
                tries.get_memtries(test.right_shard).is_none(),
                "Right child memtrie should not exist"
            );
        }

        resharder.process_batch_and_update_status(&mut update_status, !missing_memtries).unwrap();

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
        let resharder = TrieStateResharder::new(
            test.runtime.clone(),
            ReshardingHandle::new(),
            config,
            ResumeAllowed::Yes,
        );
        resharder.resume(test.parent_shard).expect("resume should succeed");

        if missing_memtries {
            // Verify that all memtries were loaded/recreated during resume.
            let tries = test.runtime.get_tries();
            assert!(
                tries.get_memtries(test.parent_shard).is_some(),
                "Parent memtrie should be loaded"
            );
            assert!(
                tries.get_memtries(test.left_shard).is_some(),
                "Left child memtrie should be recreated"
            );
            assert!(
                tries.get_memtries(test.right_shard).is_some(),
                "Right child memtrie should be recreated"
            );
        }

        // The resharding status should be None after completion.
        assert!(resharder.load_status().unwrap().is_none());
        check_child_tries_contain_all_keys(&test);
        // StateShardUIdMapping should be removed after resharding.
        assert_eq!(0, test.runtime.store().iter(DBCol::StateShardUIdMapping).count());
    }

    #[test]
    fn test_trie_state_resharder_interrupt_and_resume() {
        test_trie_state_resharder_interrupt_and_resume_impl(false);
    }

    #[test]
    fn test_trie_state_resharder_with_missing_memtries() {
        test_trie_state_resharder_interrupt_and_resume_impl(true);
    }

    #[test]
    #[should_panic(expected = "TrieStateReshardingStatus already exists")]
    fn test_trie_state_resharder_panic_on_implicit_resume() {
        let test = setup_test(true);

        let config = ChainConfig::test().resharding_config;
        let resharder = TrieStateResharder::new(
            test.runtime.clone(),
            ReshardingHandle::new(),
            config,
            ResumeAllowed::No,
        );

        // Set the batch size to 1, this should stop iteration after the first key.
        resharder
            .resharding_config
            .update(ReshardingConfig { batch_size: ByteSize(1), ..ReshardingConfig::test() });
        let mut update_status = test.as_status();
        resharder.process_batch_and_update_status(&mut update_status, true).unwrap();

        // Implicitly resuming the resharding operation should panic,
        // as the status is not None and we are not allowed to resume.
        let config = ChainConfig::test().resharding_config;
        let _resharder = TrieStateResharder::new(
            test.runtime.clone(),
            ReshardingHandle::new(),
            config,
            ResumeAllowed::No,
        );
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
