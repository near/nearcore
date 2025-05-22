use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_store::adapter::trie_store::TrieStoreUpdateAdapter;
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

#[derive(BorshSerialize, BorshDeserialize)]
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

impl TrieStateReshardingChildStatus {
    fn new(shard_uid: ShardUId, state_root: CryptoHash) -> Self {
        Self { shard_uid, state_root, next_key: None, metrics: None }
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
/// Represents the status of an ongoing trie state resharding.
/// It is used to resume the resharding process after a crash or restart.
struct TrieStateReshardingStatus {
    parent_shard_uid: ShardUId,
    left: Option<TrieStateReshardingChildStatus>,
    right: Option<TrieStateReshardingChildStatus>,
}

impl TrieStateReshardingStatus {
    fn new(
        parent_shard_uid: ShardUId,
        left: TrieStateReshardingChildStatus,
        right: TrieStateReshardingChildStatus,
    ) -> Self {
        Self { parent_shard_uid, left: Some(left), right: Some(right) }
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
        let locked = trie.lock_for_iter();
        let mut iter = locked.iter()?;
        if let Some(seek_key) = seek_key {
            // If seek_key is provided, this will prepare the iterator to continue from where it left off.
            // Note this will not record any trie nodes to the recorder.
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
        tracing::info!(
            target: "resharding",
            ?left_state_root,
            ?right_state_root,
            ?event.left_child_shard,
            ?event.right_child_shard,
            "TrieStateResharding: child state roots"
        );

        let mut status = TrieStateReshardingStatus::new(
            event.parent_shard,
            TrieStateReshardingChildStatus::new(event.left_child_shard, left_state_root),
            TrieStateReshardingChildStatus::new(event.right_child_shard, right_state_root),
        )
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
