use std::fmt::Debug;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_store::adapter::trie_store::TrieStoreUpdateAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::db::TRIE_STATE_RESHARDING_STATUS_KEY;
use near_store::metrics::resharding::trie_state_metrics;
use near_store::trie::iterator::RangeBound;
use near_store::{DBCol, ShardTries, StorageError};

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
    next_key: Vec<u8>,

    #[borsh(skip)]
    metrics: Option<TrieStateResharderMetrics>,
}

impl TrieStateReshardingChildStatus {
    fn new(shard_uid: ShardUId, state_root: CryptoHash) -> Self {
        Self { shard_uid, state_root, next_key: vec![], metrics: None }
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
/// Represents the status of an ongoing trie state resharding.
/// It is used to resume the resharding process after a crash or restart.
struct TrieStateReshardingStatus {
    parent_shard_uid: ShardUId,

    /// The child shards that still need to be processed.
    children: Vec<TrieStateReshardingChildStatus>,
}

impl TrieStateReshardingStatus {
    fn new(
        parent_shard_uid: ShardUId,
        left: TrieStateReshardingChildStatus,
        right: TrieStateReshardingChildStatus,
    ) -> Self {
        Self { parent_shard_uid, children: vec![left, right] }
    }

    fn with_metrics(mut self) -> Self {
        for child in &mut self.children {
            child.metrics = Some(TrieStateResharderMetrics::new(&child.shard_uid));
        }
        self
    }

    fn done(&self) -> bool {
        self.children.is_empty()
    }
}

/// TrieStateResharder is responsible for handling state resharding operations.
pub struct TrieStateResharder {
    runtime: Arc<dyn RuntimeAdapter>,
    /// Controls cancellation of background processing.
    pub handle: ReshardingHandle,
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
        let batch_size = self.resharding_config.get().batch_size.as_u64() as usize;
        let batch_delay = self.resharding_config.get().batch_delay.unsigned_abs();
        let Some(child) = status.children.first_mut() else {
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
        let next_key = next_batch(
            self.runtime.get_tries(),
            child.shard_uid,
            child.state_root,
            child.next_key.clone(),
            batch_size,
            &mut store_update.trie_store_update(),
        )?;

        if let Some(metrics) = &child.metrics {
            metrics.inc_processed_batches();
        }
        if let Some(next_key) = next_key {
            child.next_key = next_key;
        } else {
            // No more keys to process for this child shard.
            store_update.trie_store_update().delete_shard_uid_mapping(child.shard_uid);
            status.children.remove(0);
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

fn next_batch(
    tries: ShardTries,
    child_shard_uid: ShardUId,
    state_root: CryptoHash,
    seek_key: Vec<u8>,
    batch_size: usize,
    store_update: &mut TrieStoreUpdateAdapter,
) -> Result<Option<Vec<u8>>, StorageError> {
    let trie = tries.get_trie_for_shard(child_shard_uid, state_root).recording_reads_new_recorder();
    let locked = trie.lock_for_iter();
    let mut iter = locked.iter()?;
    if !seek_key.is_empty() {
        iter.seek(seek_key, RangeBound::Exclusive)?;
    }

    let mut next_key: Option<Vec<u8>> = None;
    for item in iter {
        let (key, _val) = item?; // Handle StorageError
        let stats = trie.recorder_stats().expect("trie recorder stats should be available");
        if stats.total_size >= batch_size {
            next_key = Some(key);
            break;
        }
    }

    let trie_changes =
        trie.recorded_trie_changes(state_root).expect("trie changes should be available");
    tracing::info!(
        target: "resharding",
        ?child_shard_uid,
        ?state_root,
        ?next_key,
        ?trie_changes,
        "TrieStateResharding: next batch"
    );
    tries.apply_all(&trie_changes, child_shard_uid, store_update);
    Ok(next_key)
}
