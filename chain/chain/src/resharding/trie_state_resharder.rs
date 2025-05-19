#![allow(unused_variables)]
#![allow(dead_code)]

use std::fmt::Debug;
use std::sync::Arc;

use itertools::Itertools;
use near_store::adapter::StoreAdapter;
use tracing::{error, info};

use crate::resharding::event_type::ReshardingSplitShardParams;
use crate::types::RuntimeAdapter;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::metrics::IntGauge;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, StateRoot};
use near_store::flat::{
    FlatStorageReadyStatus, FlatStorageReshardingStatus, FlatStorageStatus, ParentSplitParameters,
};

/// TrieStateResharder is responsible for handling state resharding operations.
pub struct TrieStateResharder {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    /// Controls cancellation of background processing.
    pub handle: ReshardingHandle,
    /// Configuration for resharding.
    resharding_config: MutableConfigValue<ReshardingConfig>,
}

impl TrieStateResharder {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        handle: ReshardingHandle,
        resharding_config: MutableConfigValue<ReshardingConfig>,
    ) -> Self {
        Self { epoch_manager, runtime, handle, resharding_config }
    }

    /// Start a resharding operation by iterating the memtries of each child shard,
    /// writing the result to the `State` column of the respective shard.
    pub fn start_resharding_blocking(
        &self,
        event: &ReshardingSplitShardParams,
    ) -> Result<(), Error> {
        let status = self.runtime.store().flat_store().get_flat_storage_status(event.parent_shard);
        let Ok(FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head })) = status else {
            error!(target: "resharding", ?status, ?event, "flat storage shard split task: parent shard is not ready");
            panic!("impossible to recover from a flat storage split shard failure!");
        };

        // Get the new shard layout right after resharding.
        let next_epoch_id = self.epoch_manager.get_next_epoch_id(&event.resharding_block.hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&next_epoch_id)?;

        let split_params = ParentSplitParameters {
            left_child_shard: event.left_child_shard,
            right_child_shard: event.right_child_shard,
            shard_layout,
            resharding_blocks: vec![event.resharding_block],
            flat_head,
        };
        self.start_resharding_blocking_impl(event.parent_shard, split_params);

        Ok(())
    }

    fn start_resharding_blocking_impl(
        &self,
        parent_shard: ShardUId,
        split_params: ParentSplitParameters,
    ) {
        let left_child_shard = split_params.left_child_shard;
        let right_child_shard = split_params.right_child_shard;
        match self.split_shard_task_blocking(parent_shard, split_params) {
            // All good.
            FlatStorageReshardingTaskResult::Successful { .. } => {}
            // The task has been cancelled. Nothing else to do.
            FlatStorageReshardingTaskResult::Cancelled => {
                return;
            }
            FlatStorageReshardingTaskResult::Failed => {
                tracing::error!(target: "resharding", "impossible to recover from a state shard split failure!");
                panic!("impossible to recover from a state split shard failure!")
            }
        }

        for child_shard in [left_child_shard, right_child_shard] {
            match self.shard_catchup_task_blocking(child_shard) {
                // All good.
                FlatStorageReshardingTaskResult::Successful { .. } => {}
                // The task has been cancelled. Nothing else to do.
                FlatStorageReshardingTaskResult::Cancelled => {}
                FlatStorageReshardingTaskResult::Failed => {
                    panic!("impossible to recover from a state shard catchup failure!")
                }
            }
        }
    }

    /// Resume an interrupted resharding operation.
    pub fn resume(&self, shard_uid: ShardUId) -> Result<(), Error> {
        let status = self.runtime.get_flat_storage_manager().get_flat_storage_status(shard_uid);
        let resharding_status = match status {
            FlatStorageStatus::Disabled
            | FlatStorageStatus::Empty
            | FlatStorageStatus::Creation(_)
            | FlatStorageStatus::Ready(_) => {
                tracing::info!(target: "resharding", ?shard_uid, ?status, "did not resume resharding");
                return Ok(());
            }
            // We only need to resume resharding if the status is `Resharding`.
            FlatStorageStatus::Resharding(status) => status,
        };

        match resharding_status {
            FlatStorageReshardingStatus::CreatingChild => {
                // Nothing to do here because the parent will take care of resuming work.
            }
            FlatStorageReshardingStatus::SplittingParent(status) => {
                let parent_shard_uid = shard_uid;
                info!(target: "resharding", ?parent_shard_uid, ?status, "resuming state shard split");
                // On resume, flat storage status is already set correctly and read from DB.
                // Thus, we don't need to care about cancelling other existing resharding events.
                // However, we don't know the current state of children shards,
                // so it's better to clean them.
                self.clean_children_shards(&status)?;
                self.start_resharding_blocking_impl(parent_shard_uid, status);
            }
            FlatStorageReshardingStatus::CatchingUp(_) => {
                info!(target: "resharding", ?shard_uid, ?resharding_status, "resuming state shard catchup");
                match self.shard_catchup_task_blocking(shard_uid) {
                    // All good.
                    FlatStorageReshardingTaskResult::Successful { .. } => {}
                    FlatStorageReshardingTaskResult::Failed => {
                        panic!("impossible to recover from a state shard catchup failure!")
                    }
                    // The task has been cancelled. Nothing else to do.
                    FlatStorageReshardingTaskResult::Cancelled => {}
                }
            }
        }
        Ok(())
    }

    /// Clean up any data in the children shards.
    fn clean_children_shards(&self, status: &ParentSplitParameters) -> Result<(), Error> {
        let ParentSplitParameters { left_child_shard, right_child_shard, .. } = status;
        info!(target: "resharding", ?left_child_shard, ?right_child_shard, "cleaning up children shards state's content");
        let mut store_update = self.runtime.store().flat_store().store_update();
        for child in [left_child_shard, right_child_shard] {
            store_update.remove_all_values(*child);
        }
        store_update.commit()?;
        Ok(())
    }

    /// Execute the shard split task.
    fn split_shard_task_blocking(
        &self,
        parent_shard: ShardUId,
        split_params: ParentSplitParameters,
    ) -> FlatStorageReshardingTaskResult {
        info!(target: "resharding", "state shard split task execution");

        let metrics = FlatStorageReshardingShardSplitMetrics::new(
            parent_shard,
            split_params.left_child_shard,
            split_params.right_child_shard,
        );

        self.split_shard_task_preprocessing(parent_shard, &split_params, &metrics);

        let task_status =
            self.split_shard_task_blocking_impl(parent_shard, &split_params, &metrics);
        self.split_shard_task_postprocessing(parent_shard, split_params, &metrics, task_status);
        info!(target: "resharding", ?task_status, "state shard split task finished");
        task_status
    }

    /// Prepare for the shard split operation.
    fn split_shard_task_preprocessing(
        &self,
        _parent_shard: ShardUId,
        _split_params: &ParentSplitParameters,
        _metrics: &FlatStorageReshardingShardSplitMetrics,
    ) {
        // Implementation would go here
    }

    /// Execute the core shard split processing.
    fn split_shard_task_blocking_impl(
        &self,
        parent_shard: ShardUId,
        split_params: &ParentSplitParameters,
        metrics: &FlatStorageReshardingShardSplitMetrics,
    ) -> FlatStorageReshardingTaskResult {
        // Exit early if the task has already been cancelled.
        if self.handle.is_cancelled() {
            return FlatStorageReshardingTaskResult::Cancelled;
        }

        // Determines after how many bytes worth of key-values the process stops to commit changes
        // and to check cancellation.
        let batch_size = self.resharding_config.get().batch_size.as_u64() as usize;
        metrics.set_split_shard_batch_size(batch_size);
        // Delay between every batch.

        // Implementation would continue here

        FlatStorageReshardingTaskResult::Successful { num_batches_done: 0 }
    }

    /// Finalize the shard split operation.
    fn split_shard_task_postprocessing(
        &self,
        parent_shard: ShardUId,
        split_params: ParentSplitParameters,
        metrics: &FlatStorageReshardingShardSplitMetrics,
        task_status: FlatStorageReshardingTaskResult,
    ) {
        info!(target: "resharding", ?parent_shard, ?task_status, ?split_params, "state shard split task: post-processing");

        let ParentSplitParameters {
            left_child_shard,
            right_child_shard,
            flat_head,
            resharding_blocks,
            ..
        } = split_params;
        let resharding_block = resharding_blocks.into_iter().exactly_one().unwrap();

        // Implementation would continue here
    }

    /// Execute the shard catchup task.
    fn shard_catchup_task_blocking(&self, shard_uid: ShardUId) -> FlatStorageReshardingTaskResult {
        // Exit early if the task has already been cancelled.
        if self.handle.is_cancelled() {
            return FlatStorageReshardingTaskResult::Cancelled;
        }

        info!(target: "resharding", ?shard_uid, "state shard catchup task started");
        let metrics = FlatStorageReshardingShardCatchUpMetrics::new(&shard_uid);

        // Apply deltas and then create the storage.
        let (num_batches_done, flat_head) = match self
            .shard_catchup_apply_deltas_blocking(shard_uid, &metrics)
        {
            Ok(ShardCatchupApplyDeltasOutcome::Succeeded(num_batches_done, tip)) => {
                (num_batches_done, tip)
            }
            Ok(ShardCatchupApplyDeltasOutcome::Cancelled) => {
                return FlatStorageReshardingTaskResult::Cancelled;
            }
            Err(err) => {
                error!(target: "resharding", ?shard_uid, ?err, "state shard catchup delta application failed!");
                return FlatStorageReshardingTaskResult::Failed;
            }
        };

        // Implementation would continue here

        FlatStorageReshardingTaskResult::Successful { num_batches_done }
    }

    /// Apply deltas during shard catchup.
    fn shard_catchup_apply_deltas_blocking(
        &self,
        shard_uid: ShardUId,
        metrics: &FlatStorageReshardingShardCatchUpMetrics,
    ) -> Result<ShardCatchupApplyDeltasOutcome, Error> {
        // How many block heights of deltas are applied in a single commit.
        let catch_up_blocks = self.resharding_config.get().catch_up_blocks;
        // Delay between every batch.
        let batch_delay = self.resharding_config.get().batch_delay.unsigned_abs();

        info!(target: "resharding", ?shard_uid, ?batch_delay, ?catch_up_blocks, "state shard catchup: starting delta apply");

        // Implementation would continue here

        Ok(ShardCatchupApplyDeltasOutcome::Succeeded(0, StateRoot::default()))
    }
}

impl Debug for TrieStateResharder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrieStateResharder").field("handle", &self.handle).finish()
    }
}

/// Result of a flat storage resharding task.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
enum FlatStorageReshardingTaskResult {
    Successful { num_batches_done: usize },
    Failed,
    Cancelled,
}

/// Outcome of applying deltas during shard catchup.
enum ShardCatchupApplyDeltasOutcome {
    Succeeded(usize, StateRoot),
    Cancelled,
}

/// Metrics for tracking shard split operations.
struct FlatStorageReshardingShardSplitMetrics {
    status: IntGauge,
    // Other metrics fields would be here
}

impl FlatStorageReshardingShardSplitMetrics {
    fn new(
        parent_shard: ShardUId,
        left_child_shard: ShardUId,
        right_child_shard: ShardUId,
    ) -> Self {
        Self {
            status: IntGauge::new("status", "Status of resharding").unwrap(),
            // Initialize other metrics
        }
    }

    fn set_split_shard_batch_size(&self, batch_size: usize) {
        // Implementation
    }
}

/// Metrics for tracking shard catchup operations.
struct FlatStorageReshardingShardCatchUpMetrics {
    status: IntGauge,
    head_height: IntGauge,
}

impl FlatStorageReshardingShardCatchUpMetrics {
    pub fn new(shard_uid: &ShardUId) -> Self {
        Self {
            status: IntGauge::new("status", "Status of resharding").unwrap(),
            head_height: IntGauge::new("head_height", "Height of the flat storage head").unwrap(),
        }
    }

    pub fn set_status(&self, status: &FlatStorageStatus) {
        // Implementation
    }

    pub fn set_head_height(&self, height: BlockHeight) {
        // Implementation
    }
}
