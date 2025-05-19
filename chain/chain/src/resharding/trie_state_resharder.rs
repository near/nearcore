use std::fmt::Debug;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_store::adapter::trie_store::TrieStoreUpdateAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::db::TRIE_STATE_RESHARDING_STATUS_KEY;
use near_store::metrics::trie_state_metrics;
use near_store::{DBCol, ShardTries, StorageError};

use crate::resharding::event_type::ReshardingSplitShardParams;
use crate::types::RuntimeAdapter;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_o11y::metrics::IntGauge;
use near_primitives::shard_layout::ShardUId;

#[derive(BorshSerialize, BorshDeserialize)]
struct TrieStateReshardingChildStatus {
    shard_uid: ShardUId,
    state_root: CryptoHash,
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
struct TrieStateReshardingStatus {
    shard_uid: ShardUId,
    children: Vec<TrieStateReshardingChildStatus>,
}

impl TrieStateReshardingStatus {
    fn new(
        shard_uid: ShardUId,
        left: TrieStateReshardingChildStatus,
        right: TrieStateReshardingChildStatus,
    ) -> Self {
        Self { shard_uid, children: vec![left, right] }
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

    //
    fn process_batch_and_update_status(
        &self,
        status: &mut TrieStateReshardingStatus,
    ) -> Result<(), Error> {
        let batch_size = self.resharding_config.get().batch_size.as_u64() as usize;
        while let Some(child) = status.children.first_mut() {
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
        }

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
            tracing::error!(
                target: "resharding", status_shard_uid=?status.shard_uid,
                "TrieStateReshardingStatus already exists, cannot start a new resharding operation. Run resume_resharding to continue.");
            panic!(
                "TrieStateReshardingStatus already exists, cannot start a new resharding operation. Run resume_resharding to continue."
            );
        }

        // Get state root from the chunk extra of the child shard.
        let block_hash = event.resharding_block.hash;
        let store = self.runtime.store().chain_store();
        let left_state_root =
            *store.get_chunk_extra(&block_hash, &event.left_child_shard)?.state_root();
        let right_state_root =
            *store.get_chunk_extra(&block_hash, &event.right_child_shard)?.state_root();

        let mut status = TrieStateReshardingStatus::new(
            event.parent_shard,
            TrieStateReshardingChildStatus::new(event.left_child_shard, left_state_root),
            TrieStateReshardingChildStatus::new(event.right_child_shard, right_state_root),
        )
        .with_metrics();
        while !status.done() && !self.handle.is_cancelled() {
            // Process the batch and update the status.
            self.process_batch_and_update_status(&mut status)?;
        }

        Ok(())
    }

    /// Resume an interrupted resharding operation.
    pub fn resume(&self, shard_uid: ShardUId) -> Result<(), Error> {
        let Some(status) = self.load_status()? else {
            tracing::info!(target: "resharding", "Resharding status not found, nothing to resume.");
            return Ok(());
        };

        if status.shard_uid != shard_uid {
            tracing::error!(
                target: "resharding", status_shard_uid=?status.shard_uid, ?shard_uid,
                "Resharding status shard UID does not match the provided shard UID.");
            return Err(Error::ReshardingError(format!(
                "Resharding status shard UID {} does not match the provided shard UID {}.",
                status.shard_uid, shard_uid
            )));
        }
        let mut status = status.with_metrics();
        while !status.done() && !self.handle.is_cancelled() {
            // Process the batch and update the status.
            self.process_batch_and_update_status(&mut status)?;
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
    shard_uid: ShardUId,
    state_root: CryptoHash,
    seek_key: Vec<u8>,
    batch_size: usize,
    store_update: &mut TrieStoreUpdateAdapter,
) -> Result<Option<Vec<u8>>, StorageError> {
    let trie = tries.get_trie_for_shard(shard_uid, state_root).recording_reads_new_recorder();
    let locked = trie.lock_for_iter();
    let mut iter = locked.iter()?;
    iter.seek(seek_key, true)?;

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
    tries.apply_all(&trie_changes, shard_uid, store_update);
    Ok(next_key)
}
