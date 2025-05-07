//! Logic for resharding flat storage in parallel to chain processing.
//!
//! See [FlatStorageResharder] for more details about how the resharding takes place.

use std::num::NonZero;
use std::sync::Arc;

use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;

use near_store::adapter::trie_store::TrieStoreAdapter;
use tracing::{debug, error, info, warn};

use crate::resharding::event_type::{ReshardingEventType, ReshardingSplitShardParams};
use crate::resharding::types::{
    FlatStorageShardCatchupRequest, FlatStorageSplitShardRequest, MemtrieReloadRequest,
    ReshardingSender,
};
use crate::types::RuntimeAdapter;
use crate::{ChainStore, ChainStoreAccess};
use itertools::Itertools;
use near_primitives::block::Tip;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::col::{self};
use near_primitives::trie_key::trie_key_parsers::{
    parse_account_id_from_access_key_key, parse_account_id_from_account_key,
    parse_account_id_from_contract_code_key, parse_account_id_from_contract_data_key,
    parse_account_id_from_received_data_key, parse_account_id_from_trie_key_with_separator,
};
#[cfg(feature = "test_features")]
use near_primitives::types::BlockHeightDelta;
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::flat_store::{FlatStoreAdapter, FlatStoreUpdateAdapter};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::flat::{
    BlockInfo, FlatStateChanges, FlatStorageError, FlatStorageReadyStatus,
    FlatStorageReshardingShardCatchUpMetrics, FlatStorageReshardingShardSplitMetrics,
    FlatStorageReshardingStatus, FlatStorageStatus, ParentSplitParameters,
};
use near_store::{ShardUId, StorageError};
use parking_lot::Mutex;
use std::fmt::{Debug, Formatter};
use std::iter;

/// `FlatStorageResharder` takes care of updating flat storage when a resharding event happens.
///
/// On an high level, the events supported are:
/// - #### Shard splitting
///     Parent shard must be split into two children. This operation does not explicitly freeze the
///     shard, but instead relies on the fact that no new chunk will be processed for it. Children
///     shards are created empty and the key-values of the parent will be copied into them, in a
///     background task.
///
///     After the copy is finished, the children shards will have their state at the height of the
///     last block of the old shard layout. It'll be necessary to perform catchup before their flat
///     storages can be put in Ready state. The parent shard storage is not needed anymore and
///     can be removed.
///
/// The resharder has also the following properties:
/// - Background processing: the bulk of resharding is done in separate tasks, see
///   [FlatStorageResharder::split_shard_task] and [FlatStorageResharder::shard_catchup_task].
/// - Interruptible: a reshard operation can be cancelled through a
///   [FlatStorageResharderController].
///     - In the case of event `Split` the state of flat storage will go back to what it was
///       previously.
///     - Children shard catchup can be cancelled and will resume from the point where it left.
/// - Resilience to chain forks.
///     - Resharding events will perform changes on the state only after their resharding block
///       becomes final.
#[derive(Clone)]
pub struct FlatStorageResharder {
    runtime: Arc<dyn RuntimeAdapter>,
    /// The current active resharding event.
    resharding_event: Arc<Mutex<Option<FlatStorageReshardingEventStatus>>>,
    /// Sender responsible to convey requests to the dedicated resharding actor.
    sender: ReshardingSender,
    /// Controls cancellation of background processing.
    pub controller: FlatStorageResharderController,
    /// Configuration for resharding.
    resharding_config: MutableConfigValue<ReshardingConfig>,
    #[cfg(feature = "test_features")]
    /// TEST ONLY.
    /// If non zero, the start of scheduled tasks (such as split parent) will be postponed by
    /// the specified number of blocks.
    pub adv_task_delay_by_blocks: BlockHeightDelta,
}

impl FlatStorageResharder {
    /// Creates a new `FlatStorageResharder`.
    ///
    /// # Args:
    /// * `runtime`: runtime adapter
    /// * `sender`: component used to schedule the background tasks
    /// * `controller`: manages the execution of the background tasks
    /// * `resharding_config`: configuration options
    pub fn new(
        runtime: Arc<dyn RuntimeAdapter>,
        sender: ReshardingSender,
        controller: FlatStorageResharderController,
        resharding_config: MutableConfigValue<ReshardingConfig>,
    ) -> Self {
        let resharding_event = Arc::new(Mutex::new(None));
        Self {
            runtime,
            resharding_event,
            sender,
            controller,
            resharding_config,
            #[cfg(feature = "test_features")]
            adv_task_delay_by_blocks: 0,
        }
    }

    /// Starts a resharding event.
    ///
    /// For now, only splitting a shard is supported.
    ///
    /// # Args:
    /// * `event_type`: the type of resharding event
    /// * `shard_layout`: the new shard layout
    pub fn start_resharding(
        &self,
        event_type: ReshardingEventType,
        shard_layout: &ShardLayout,
    ) -> Result<(), Error> {
        match event_type {
            ReshardingEventType::SplitShard(params) => self.split_shard(params, shard_layout),
        }
    }

    /// Resumes a resharding event that was interrupted.
    ///
    /// Flat-storage resharding will resume upon a node crash.
    ///
    /// # Args:
    /// * `shard_uid`: UId of the shard
    /// * `status`: resharding status of the shard
    pub fn resume(
        &self,
        shard_uid: ShardUId,
        status: &FlatStorageReshardingStatus,
    ) -> Result<(), Error> {
        match status {
            FlatStorageReshardingStatus::CreatingChild => {
                // Nothing to do here because the parent will take care of resuming work.
            }
            FlatStorageReshardingStatus::SplittingParent(status) => {
                let parent_shard_uid = shard_uid;
                info!(target: "resharding", ?parent_shard_uid, ?status, "resuming flat storage shard split");
                self.check_new_event_is_allowed()?;
                // On resume, flat storage status is already set correctly and read from DB.
                // Thus, we don't need to care about cancelling other existing resharding events.
                // However, we don't know the current state of children shards,
                // so it's better to clean them.
                self.clean_children_shards(&status)?;
                self.schedule_split_shard(parent_shard_uid, &status);
            }
            FlatStorageReshardingStatus::CatchingUp(_) => {
                info!(target: "resharding", ?shard_uid, ?status, "resuming flat storage shard catchup");
                // Send a request to schedule the execution of `shard_catchup_task` for this shard.
                self.sender
                    .flat_storage_shard_catchup_sender
                    .send(FlatStorageShardCatchupRequest { resharder: self.clone(), shard_uid });
            }
        }
        Ok(())
    }

    /// Starts the event of splitting a parent shard flat storage into two children.
    ///
    /// This method is resilient to chain forks and concurrent resharding requests. First of all, if
    /// a resharding event has started, no other split shard events will be accepted. Second, if a
    /// resharding event is already scheduled, the new event will extend the old one (if they are
    /// the same type) or replace it.
    fn split_shard(
        &self,
        split_params: ReshardingSplitShardParams,
        shard_layout: &ShardLayout,
    ) -> Result<(), Error> {
        let ReshardingSplitShardParams {
            parent_shard,
            left_child_shard,
            right_child_shard,
            resharding_block,
            ..
        } = split_params;
        info!(target: "resharding", ?split_params, "initiating flat storage shard split");

        self.check_new_event_is_allowed()?;

        // Get all resharding blocks, if any, from the existing scheduled split shard,
        // and merge them with the new resharding block.
        let mut resharding_blocks = vec![resharding_block];
        if let Some(FlatStorageReshardingEventStatus::SplitShard(parent, params, _)) =
            self.resharding_event()
        {
            if parent_shard == parent
                && params.left_child_shard == left_child_shard
                && params.right_child_shard == right_child_shard
                && params.shard_layout == *shard_layout
            {
                resharding_blocks.extend(params.resharding_blocks);
            }
        };

        // Cancel any scheduled, not yet started event.
        self.cancel_scheduled_event();

        // Change parent and children shards flat storage status.
        let store = self.runtime.store().flat_store();
        let mut store_update = store.store_update();
        let flat_head = retrieve_shard_flat_head(parent_shard, &store)?;
        let split_params = ParentSplitParameters {
            left_child_shard,
            right_child_shard,
            shard_layout: shard_layout.clone(),
            resharding_blocks,
            flat_head,
        };
        store_update.set_flat_storage_status(
            parent_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::SplittingParent(
                split_params.clone(),
            )),
        );
        // Do not update parent flat head, to avoid overriding the resharding status.
        // In any case, at the end of resharding the parent shard will completely disappear.
        self.runtime
            .get_flat_storage_manager()
            .get_flat_storage_for_shard(parent_shard)
            .expect("flat storage of the parent shard must exist!")
            .set_flat_head_update_mode(false);
        store_update.set_flat_storage_status(
            left_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_update.set_flat_storage_status(
            right_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_update.commit()?;

        self.schedule_split_shard(parent_shard, &split_params);
        Ok(())
    }

    /// Returns `Ok` if:
    /// - no resharding event exists.
    /// - a resharding event already exists, but it's not in progress yet.
    ///
    /// Returns `Err` if:
    /// - a resharding event is in progress.
    fn check_new_event_is_allowed(&self) -> Result<(), StorageError> {
        let Some(current_event) = self.resharding_event() else {
            return Ok(());
        };
        if current_event.has_started() {
            error!(target: "resharding", "trying to start a new flat storage resharding event while one is already in progress!");
            return Err(StorageError::FlatStorageReshardingAlreadyInProgress);
        }
        Ok(())
    }

    fn set_resharding_event(&self, event: FlatStorageReshardingEventStatus) {
        *self.resharding_event.lock() = Some(event);
    }

    /// Returns the current in-progress resharding event, if any.
    pub fn resharding_event(&self) -> Option<FlatStorageReshardingEventStatus> {
        self.resharding_event.lock().clone()
    }

    /// Schedules a task to split a shard.
    fn schedule_split_shard(&self, parent_shard: ShardUId, split_params: &ParentSplitParameters) {
        let event = FlatStorageReshardingEventStatus::SplitShard(
            parent_shard,
            split_params.clone(),
            TaskExecutionStatus::NotStarted,
        );
        self.set_resharding_event(event);

        let metrics = FlatStorageReshardingShardSplitMetrics::new(
            parent_shard,
            split_params.left_child_shard,
            split_params.right_child_shard,
        );
        metrics.update_shards_status(&self.runtime.get_flat_storage_manager());

        info!(target: "resharding", ?parent_shard, ?split_params, "scheduling flat storage shard split");
        let resharder = self.clone();
        // Send a request to schedule the execution of `split_shard_task`, to do the bulk of the
        // splitting work.
        self.sender
            .flat_storage_split_shard_sender
            .send(FlatStorageSplitShardRequest { resharder });
    }

    /// Cleans up children shards flat storage's content (status and deltas are excluded).
    #[tracing::instrument(
        level = "info",
        target = "resharding",
        "FlatStorageResharder::clean_children_shards",
        skip_all,
        fields(left_child_shard = ?status.left_child_shard, right_child_shard = ?status.right_child_shard)
    )]
    fn clean_children_shards(&self, status: &ParentSplitParameters) -> Result<(), Error> {
        let ParentSplitParameters { left_child_shard, right_child_shard, .. } = status;
        info!(target: "resharding", ?left_child_shard, ?right_child_shard, "cleaning up children shards flat storage's content");
        let mut store_update = self.runtime.store().flat_store().store_update();
        for child in [left_child_shard, right_child_shard] {
            store_update.remove_all_values(*child);
        }
        store_update.commit()?;
        Ok(())
    }

    /// Task to perform the actual split of a flat storage shard. This may be a long operation
    /// time-wise.
    ///
    /// Conceptually it simply copies each key-value pair from the parent shard to the correct
    /// child. This task may get cancelled or postponed.
    pub fn split_shard_task(&self, chain_store: &ChainStore) -> FlatStorageReshardingTaskResult {
        info!(target: "resharding", "flat storage shard split task execution");

        let (resharding_block, parent_shard, split_params) = match self
            .split_shard_check_readiness(chain_store)
        {
            SplitShardSchedulingStatus::CanStart(block, shard_uid, parent_split_parameters) => {
                (block, shard_uid, parent_split_parameters)
            }
            SplitShardSchedulingStatus::Cancelled => {
                info!(target: "resharding", "flat storage shard split task has been cancelled");
                return FlatStorageReshardingTaskResult::Cancelled;
            }
            SplitShardSchedulingStatus::Postponed => {
                info!(target: "resharding", "flat storage shard split task has been postponed");
                return FlatStorageReshardingTaskResult::Postponed;
            }
            SplitShardSchedulingStatus::Failed => {
                // It's important to cancel the scheduled event in case of failure.
                self.cancel_scheduled_event();
                error!(target: "resharding", "flat storage shard split task failed during scheduling!");
                return FlatStorageReshardingTaskResult::Failed;
            }
        };

        let metrics = FlatStorageReshardingShardSplitMetrics::new(
            parent_shard,
            split_params.left_child_shard,
            split_params.right_child_shard,
        );

        let task_status =
            self.split_shard_task_impl(parent_shard, &split_params, &resharding_block, &metrics);
        self.split_shard_task_postprocessing(
            parent_shard,
            split_params,
            &resharding_block,
            &metrics,
            task_status,
        );
        info!(target: "resharding", ?task_status, "flat storage shard split task finished");
        task_status
    }

    /// Checks if the resharder is ready to split a shard. In such case it returns the parameters needed for the split.
    fn split_shard_check_readiness(&self, chain_store: &ChainStore) -> SplitShardSchedulingStatus {
        info!(target: "resharding", "checking readiness of flat storage shard split task");
        // The current resharding event must exist and be compatible with the operation of
        // splitting a shard. All these checks must happen while the resharding event is under
        // lock, to prevent multiple parallel resharding attempts (due to forks) to interfere
        // with each other .
        let mut event_lock_guard = self.resharding_event.lock();
        let Some(event) = event_lock_guard.as_mut() else {
            info!(target: "resharding", "flat storage shard split task cancelled: no resharding event");
            return SplitShardSchedulingStatus::Cancelled;
        };

        if event.has_started() {
            info!(target: "resharding", "flat storage shard split task cancelled: resharding already in progress");
            return SplitShardSchedulingStatus::Cancelled;
        }

        let (resharding_blocks, parent_shard, split_params, execution_status) = match event {
            FlatStorageReshardingEventStatus::SplitShard(
                parent_shard,
                split_params,
                execution_status,
            ) => (
                split_params.resharding_blocks.clone(),
                *parent_shard,
                split_params.clone(),
                execution_status,
            ),
        };

        // Wait until one of the candidate resharding blocks becomes final.
        match self.compute_scheduled_task_status(&resharding_blocks, chain_store) {
            TaskSchedulingStatus::CanStart(block) => {
                info!(target: "resharding", "flat storage shard split task ready to perform bulk processing");

                #[cfg(feature = "test_features")]
                {
                    if self.adv_should_delay_task(&block.hash, chain_store) {
                        info!(target: "resharding", "flat storage shard split task has been artificially postponed!");
                        return SplitShardSchedulingStatus::Postponed;
                    }
                }

                // We must mark the task as 'Started'.
                *execution_status = TaskExecutionStatus::Started;
                SplitShardSchedulingStatus::CanStart(block, parent_shard, split_params)
            }
            TaskSchedulingStatus::Failed => SplitShardSchedulingStatus::Failed,
            TaskSchedulingStatus::Postponed => SplitShardSchedulingStatus::Postponed,
        }
    }

    /// Performs the bulk of [split_shard_task]. This method splits the flat storage of the parent shard.
    ///
    /// Returns `true` if the routine completed successfully.
    fn split_shard_task_impl(
        &self,
        parent_shard: ShardUId,
        split_params: &ParentSplitParameters,
        resharding_block: &BlockInfo,
        metrics: &FlatStorageReshardingShardSplitMetrics,
    ) -> FlatStorageReshardingTaskResult {
        // Exit early if the task has already been cancelled.
        if self.controller.is_cancelled() {
            return FlatStorageReshardingTaskResult::Cancelled;
        }

        // Determines after how many bytes worth of key-values the process stops to commit changes
        // and to check cancellation.
        let batch_size = self.resharding_config.get().batch_size.as_u64() as usize;
        metrics.set_split_shard_batch_size(batch_size);
        // Delay between every batch.
        let batch_delay = self.resharding_config.get().batch_delay.unsigned_abs();

        info!(target: "resharding", ?parent_shard, ?split_params, ?resharding_block, ?batch_delay, ?batch_size, "flat storage shard split task: starting key-values copy");

        // Prepare the store object for commits and the iterator over parent's flat storage.
        let flat_store = self.runtime.store().flat_store();
        let mut iter = match self.flat_storage_iterator(
            &flat_store,
            &parent_shard,
            &resharding_block.hash,
        ) {
            Ok(iter) => iter,
            Err(err) => {
                error!(target: "resharding", ?parent_shard, block_hash=?resharding_block.hash, ?err, "failed to build flat storage iterator");
                return FlatStorageReshardingTaskResult::Failed;
            }
        };

        let mut num_batches_done: usize = 0;
        metrics.set_split_shard_processed_bytes(0);
        let mut iter_exhausted = false;

        loop {
            let _span = tracing::debug_span!(
                target: "resharding",
                "split_shard_task_impl/batch",
                batch_id = ?num_batches_done)
            .entered();
            let store = flat_store.trie_store();
            let mut store_update = flat_store.store_update();
            let mut processed_size = 0;

            // Process a `batch_size` worth of key value pairs.
            while processed_size < batch_size && !iter_exhausted {
                match iter.next() {
                    // Stop iterating and commit the batch.
                    Some(FlatStorageAndDeltaIterItem::CommitPoint) => break,
                    Some(FlatStorageAndDeltaIterItem::Entry(Ok((key, value)))) => {
                        processed_size += key.len() + value.as_ref().map_or(0, |v| v.size());
                        if let Err(err) = shard_split_handle_key_value(
                            key,
                            value,
                            &store,
                            &mut store_update,
                            &split_params,
                        ) {
                            error!(target: "resharding", ?err, "failed to handle flat storage key");
                            return FlatStorageReshardingTaskResult::Failed;
                        }
                    }
                    Some(FlatStorageAndDeltaIterItem::Entry(Err(err))) => {
                        error!(target: "resharding", ?err, "failed to read flat storage value from parent shard");
                        return FlatStorageReshardingTaskResult::Failed;
                    }
                    None => {
                        iter_exhausted = true;
                    }
                }
            }

            // Make a pause to commit and check if the routine should stop.
            if let Err(err) = store_update.commit() {
                error!(target: "resharding", ?err, "failed to commit store update");
                return FlatStorageReshardingTaskResult::Failed;
            }

            num_batches_done += 1;
            metrics.set_split_shard_processed_batches(num_batches_done);
            metrics.inc_split_shard_processed_bytes_by(processed_size);

            // If `iter`` is exhausted we can exit after the store commit.
            if iter_exhausted {
                return FlatStorageReshardingTaskResult::Successful { num_batches_done };
            }
            if self.controller.is_cancelled() {
                return FlatStorageReshardingTaskResult::Cancelled;
            }

            // Sleep between batches in order to throttle resharding and leave some resource for the
            // regular node operation.
            std::thread::sleep(batch_delay);
        }
    }

    /// Performs post-processing of shard splitting after all key-values have been moved from parent to
    /// children. `success` indicates whether or not the previous phase was successful.
    #[tracing::instrument(
        level = "info",
        target = "resharding",
        "FlatStorageResharder::split_shard_task_postprocessing",
        skip_all
    )]
    fn split_shard_task_postprocessing(
        &self,
        parent_shard: ShardUId,
        split_params: ParentSplitParameters,
        resharding_block: &BlockInfo,
        metrics: &FlatStorageReshardingShardSplitMetrics,
        task_status: FlatStorageReshardingTaskResult,
    ) {
        let ParentSplitParameters { left_child_shard, right_child_shard, flat_head, .. } =
            split_params;
        let flat_store = self.runtime.store().flat_store();
        info!(target: "resharding", ?parent_shard, ?task_status, ?split_params, ?resharding_block, "flat storage shard split task: post-processing");

        let mut store_update = flat_store.store_update();
        match task_status {
            FlatStorageReshardingTaskResult::Successful { .. } => {
                // Split shard completed successfully.
                // Parent flat storage can be deleted from the FlatStoreManager.
                // If FlatStoreManager has no reference to the shard, delete it manually.
                if !self
                    .runtime
                    .get_flat_storage_manager()
                    .remove_flat_storage_for_shard(parent_shard, &mut store_update)
                    .unwrap()
                {
                    store_update.remove_flat_storage(parent_shard);
                }
                // Children must perform catchup.
                for child_shard in [left_child_shard, right_child_shard] {
                    store_update.set_flat_storage_status(
                        child_shard,
                        FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(
                            *resharding_block,
                        )),
                    );
                    // Catchup will happen in a separate task, so send a request to schedule the
                    // execution of `shard_catchup_task` for the child shard.
                    self.sender.flat_storage_shard_catchup_sender.send(
                        FlatStorageShardCatchupRequest {
                            resharder: self.clone(),
                            shard_uid: child_shard,
                        },
                    );
                }
            }
            FlatStorageReshardingTaskResult::Failed => {
                // Reset parent.
                store_update.set_flat_storage_status(
                    parent_shard,
                    FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }),
                );
                self.runtime
                    .get_flat_storage_manager()
                    .get_flat_storage_for_shard(parent_shard)
                    .map(|flat_storage| flat_storage.set_flat_head_update_mode(true));
                // Remove children shards entirely.
                for child_shard in [left_child_shard, right_child_shard] {
                    store_update.remove_flat_storage(child_shard);
                }
            }
            FlatStorageReshardingTaskResult::Cancelled => {
                // Remove children shards leftovers, but keep intact their current status and deltas
                // plus the current status of the parent, so resharding can resume later.
                for child_shard in [left_child_shard, right_child_shard] {
                    store_update.remove_all_values(child_shard);
                }
            }
            FlatStorageReshardingTaskResult::Postponed => {
                panic!("can't finalize processing of a postponed split task!");
            }
        }
        store_update.commit().unwrap();
        self.remove_resharding_event();
        metrics.update_shards_status(&self.runtime.get_flat_storage_manager());
    }

    /// Returns an iterator over a shard's flat storage at the given block hash. This
    /// iterator contains both flat storage values and deltas.
    fn flat_storage_iterator<'a>(
        &self,
        flat_store: &'a FlatStoreAdapter,
        shard_uid: &ShardUId,
        block_hash: &CryptoHash,
    ) -> Result<Box<FlatStorageAndDeltaIter<'a>>, Error> {
        let mut iter: Box<FlatStorageAndDeltaIter<'a>> = Box::new(
            flat_store
                .iter(*shard_uid)
                // Get the flat storage iter and wrap the value in Optional::Some to
                // match the delta iterator so that they can be chained.
                .map_ok(|(key, value)| (key, Some(value)))
                // Wrap the iterator's item into an Entry.
                .map(|entry| FlatStorageAndDeltaIterItem::Entry(entry)),
        );

        // Get all the blocks from flat head to the wanted block hash.
        let flat_storage = self
            .runtime
            .get_flat_storage_manager()
            .get_flat_storage_for_shard(*shard_uid)
            .expect("the flat storage undergoing resharding must exist!");
        // Must reverse the result because we want ascending block heights.
        let mut blocks_to_head = flat_storage.get_blocks_to_head(block_hash).map_err(|err| {
            StorageError::StorageInconsistentState(format!(
                "failed to find path from block {block_hash} to flat storage head ({err})"
            ))
        })?;
        blocks_to_head.reverse();
        debug!(target = "resharding", "flat storage blocks to head len = {}", blocks_to_head.len());

        // Get all the delta iterators and wrap the items in Result to match the flat
        // storage iter so that they can be chained.
        for block in blocks_to_head {
            let deltas = flat_store.get_delta(*shard_uid, block).map_err(|err| {
                StorageError::StorageInconsistentState(format!(
                    "can't retrieve deltas for flat storage at {block}/{shard_uid:?}({err})"
                ))
            })?;
            let Some(deltas) = deltas else {
                continue;
            };
            // Chain the iterators effectively adding a block worth of deltas.
            // Before doing so insert a commit point to separate changes to the same key in different transactions.
            iter = Box::new(iter.chain(iter::once(FlatStorageAndDeltaIterItem::CommitPoint)));
            let deltas_iter = deltas.0.into_iter();
            let deltas_iter = deltas_iter.map(|item| FlatStorageAndDeltaIterItem::Entry(Ok(item)));
            iter = Box::new(iter.chain(deltas_iter));
        }

        Ok(iter)
    }

    /// Task to perform catchup and creation of a flat storage shard spawned from a previous
    /// resharding operation. May be a long operation time-wise. This task can't be cancelled
    /// nor postponed.
    pub fn shard_catchup_task(
        &self,
        shard_uid: ShardUId,
        chain_store: &ChainStore,
    ) -> FlatStorageReshardingTaskResult {
        // Exit early if the task has already been cancelled.
        if self.controller.is_cancelled() {
            return FlatStorageReshardingTaskResult::Cancelled;
        }
        info!(target: "resharding", ?shard_uid, "flat storage shard catchup task started");
        let metrics = FlatStorageReshardingShardCatchUpMetrics::new(&shard_uid);
        // Apply deltas and then create the flat storage.
        let (num_batches_done, flat_head) = match self.shard_catchup_apply_deltas(
            shard_uid,
            chain_store,
            &metrics,
        ) {
            Ok(ShardCatchupApplyDeltasOutcome::Succeeded(num_batches_done, tip)) => {
                (num_batches_done, tip)
            }
            Ok(ShardCatchupApplyDeltasOutcome::Cancelled) => {
                return FlatStorageReshardingTaskResult::Cancelled;
            }
            Ok(ShardCatchupApplyDeltasOutcome::Postponed) => {
                return FlatStorageReshardingTaskResult::Postponed;
            }
            Err(err) => {
                error!(target: "resharding", ?shard_uid, ?err, "flat storage shard catchup delta application failed!");
                return FlatStorageReshardingTaskResult::Failed;
            }
        };
        match self.shard_catchup_finalize_storage(shard_uid, &flat_head, &metrics) {
            Ok(_) => {
                let task_status = FlatStorageReshardingTaskResult::Successful { num_batches_done };
                info!(target: "resharding", ?shard_uid, ?task_status, "flat storage shard catchup task finished");
                // At this point we can trigger the reload of memtries.
                self.sender.memtrie_reload_sender.send(MemtrieReloadRequest { shard_uid });
                task_status
            }
            Err(err) => {
                error!(target: "resharding", ?shard_uid, ?err, "flat storage shard catchup finalize failed!");
                FlatStorageReshardingTaskResult::Failed
            }
        }
    }

    /// checks whether there's a snapshot in progress. Returns true if we've already applied all deltas up
    /// to the desired snapshot height, and should no longer continue to give the state snapshot
    /// code a chance to finish first.
    fn coordinate_snapshot(&self, height: BlockHeight) -> bool {
        let manager = self.runtime.get_flat_storage_manager();
        let Some(min_chunk_prev_height) = manager.snapshot_height_wanted() else {
            return false;
        };
        height >= min_chunk_prev_height
    }

    /// Applies flat storage deltas in batches on a shard that is in catchup status.
    ///
    /// This function can either:
    /// - Finish successfully, returning the number of delta batches applied and the final tip of the flat storage.
    /// - Be cancelled
    /// - Be postponed, to let other operations run on this intermediate state of flat storage (example state sync snapshot).
    fn shard_catchup_apply_deltas(
        &self,
        shard_uid: ShardUId,
        chain_store: &ChainStore,
        metrics: &FlatStorageReshardingShardCatchUpMetrics,
    ) -> Result<ShardCatchupApplyDeltasOutcome, Error> {
        // How many block heights of deltas are applied in a single commit.
        let catch_up_blocks = self.resharding_config.get().catch_up_blocks;
        // Delay between every batch.
        let batch_delay = self.resharding_config.get().batch_delay.unsigned_abs();

        info!(target: "resharding", ?shard_uid, ?batch_delay, ?catch_up_blocks, "flat storage shard catchup: starting delta apply");

        let mut num_batches_done: usize = 0;

        let status = self
            .runtime
            .store()
            .flat_store()
            .get_flat_storage_status(shard_uid)
            .map_err(|e| Into::<StorageError>::into(e))?;
        let FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(mut flat_head)) =
            status
        else {
            return Err(Error::Other(format!(
                "unexpected resharding catchup flat storage status for {}: {:?}",
                shard_uid, &status
            )));
        };

        loop {
            let _span = tracing::debug_span!(
                target: "resharding",
                "shard_catchup_apply_deltas/batch",
                ?shard_uid,
                ?flat_head,
                batch_id = ?num_batches_done)
            .entered();
            let chain_final_head = chain_store.final_head()?;

            // If we reached the desired new flat head, we can terminate the delta application step.
            if is_flat_head_on_par_with_chain(&flat_head.hash, &chain_final_head) {
                return Ok(ShardCatchupApplyDeltasOutcome::Succeeded(
                    num_batches_done,
                    Tip::from_header(&chain_store.get_block_header(&flat_head.hash)?),
                ));
            }

            let mut merged_changes = FlatStateChanges::default();
            let store = self.runtime.store().flat_store();
            let mut store_update = store.store_update();
            let mut postpone = false;

            // Merge deltas from the next blocks until we reach chain final head.
            for _ in 0..catch_up_blocks {
                debug_assert!(
                    flat_head.height <= chain_final_head.height,
                    "flat head: {:?}",
                    &flat_head,
                );
                // Stop if we reached the desired new flat head.
                if is_flat_head_on_par_with_chain(&flat_head.hash, &chain_final_head) {
                    break;
                }
                if self.coordinate_snapshot(flat_head.height) {
                    postpone = true;
                    break;
                }
                let next_hash = chain_store.get_next_block_hash(&flat_head.hash)?;
                let next_header = chain_store.get_block_header(&next_hash)?;
                flat_head = BlockInfo {
                    hash: *next_header.hash(),
                    height: next_header.height(),
                    prev_hash: *next_header.prev_hash(),
                };
                if let Some(changes) = store
                    .get_delta(shard_uid, flat_head.hash)
                    .map_err(|err| Into::<StorageError>::into(err))?
                {
                    merged_changes.merge(changes);
                    store_update.remove_delta(shard_uid, flat_head.hash);
                }
            }

            // Commit all changes to store.
            merged_changes.apply_to_flat_state(&mut store_update, shard_uid);
            store_update.set_flat_storage_status(
                shard_uid,
                FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(flat_head)),
            );
            store_update.commit()?;

            num_batches_done += 1;
            metrics.set_head_height(flat_head.height);

            if postpone {
                return Ok(ShardCatchupApplyDeltasOutcome::Postponed);
            }
            if self.controller.is_cancelled() {
                return Ok(ShardCatchupApplyDeltasOutcome::Cancelled);
            }

            // Sleep between batches in order to throttle resharding and leave some resource for the
            // regular node operation.
            std::thread::sleep(batch_delay);
        }
    }

    /// Creates a flat storage entry for a shard that completed catchup. Also clears leftover data.
    #[tracing::instrument(
        level = "info",
        target = "resharding",
        "FlatStorageResharder::shard_catchup_finalize_storage",
        skip_all,
        fields(?shard_uid)
    )]
    fn shard_catchup_finalize_storage(
        &self,
        shard_uid: ShardUId,
        flat_head: &Tip,
        metrics: &FlatStorageReshardingShardCatchUpMetrics,
    ) -> Result<(), Error> {
        // GC deltas from forks which could have appeared on chain during catchup.
        let store = self.runtime.store().flat_store();
        let mut store_update = store.store_update();
        // Deltas must exist because we applied them previously.
        let deltas_metadata = store.get_all_deltas_metadata(shard_uid).unwrap_or_else(|_| {
            panic!("Cannot read flat state deltas metadata for shard {shard_uid} from storage")
        });
        let mut deltas_gc_count = 0;
        for delta_metadata in deltas_metadata {
            if delta_metadata.block.height <= flat_head.height {
                store_update.remove_delta(shard_uid, delta_metadata.block.hash);
                deltas_gc_count += 1;
            }
        }
        // Set the flat storage status to `Ready`.
        let flat_storage_status = FlatStorageStatus::Ready(FlatStorageReadyStatus {
            flat_head: BlockInfo {
                hash: flat_head.last_block_hash,
                prev_hash: flat_head.prev_block_hash,
                height: flat_head.height,
            },
        });
        store_update.set_flat_storage_status(shard_uid, flat_storage_status.clone());
        store_update.commit()?;
        metrics.set_status(&flat_storage_status);
        info!(target: "resharding", ?shard_uid, %deltas_gc_count, "garbage collected flat storage deltas");
        // Create the flat storage entry for this shard in the manager.
        self.runtime.get_flat_storage_manager().create_flat_storage_for_shard(shard_uid)?;
        info!(target: "resharding", ?shard_uid, ?flat_head, "flat storage creation done");
        Ok(())
    }

    /// Cancels the current event, if it exists and it hasn't started yet.
    fn cancel_scheduled_event(&self) {
        let Some(current_event) = self.resharding_event() else {
            return;
        };
        info!(target: "resharding", ?current_event, "cancelling current scheduled resharding event");
        debug_assert!(!current_event.has_started());
        // Clean up the database state.
        match current_event {
            FlatStorageReshardingEventStatus::SplitShard(parent_shard, split_status, ..) => {
                let flat_store = self.runtime.store().flat_store();
                let mut store_update = flat_store.store_update();
                // Parent go back to Ready state.
                store_update.set_flat_storage_status(
                    parent_shard,
                    FlatStorageStatus::Ready(FlatStorageReadyStatus {
                        flat_head: split_status.flat_head,
                    }),
                );
                self.runtime
                    .get_flat_storage_manager()
                    .get_flat_storage_for_shard(parent_shard)
                    .map(|flat_storage| flat_storage.set_flat_head_update_mode(true));
                // Remove children shards status.
                for child_shard in [split_status.left_child_shard, split_status.right_child_shard] {
                    store_update.remove_status(child_shard);
                }
                store_update.commit().unwrap();
            }
        }
        // Clean up the resharding event.
        self.remove_resharding_event();
    }

    /// Computes the scheduling status of a task waiting to be started, taking into account all possible
    /// resharding blocks across all forks.
    /// The task will be ready to start if one resharding block has become final.
    fn compute_scheduled_task_status(
        &self,
        resharding_blocks: &[BlockInfo],
        chain_store: &ChainStore,
    ) -> TaskSchedulingStatus {
        use TaskSchedulingStatus::*;
        let mut at_least_one_postponed = false;
        for block_info in resharding_blocks {
            match self.compute_scheduled_task_status_at_block(&block_info, chain_store) {
                CanStart(block_info) => return CanStart(block_info),
                Failed => {} // Nothing to do on failure. This can happen during forks. It's normal for all but one fork to fail.
                Postponed => at_least_one_postponed = true,
            }
        }
        if at_least_one_postponed {
            // One resharding block can still become final in the future.
            Postponed
        } else {
            // The whole task failed.
            Failed
        }
    }

    /// Computes the scheduling status of a task at a given resharding block.
    fn compute_scheduled_task_status_at_block(
        &self,
        resharding_block_info: &BlockInfo,
        chain_store: &ChainStore,
    ) -> TaskSchedulingStatus {
        let resharding_hash = &resharding_block_info.hash;
        // Retrieve the height of the resharding block.
        let chain_final_head = chain_store.final_head().unwrap();
        let resharding_height_result = chain_store.get_block_height(resharding_hash);
        let Ok(resharding_height) = resharding_height_result else {
            warn!(target: "resharding", ?resharding_hash, err = ?resharding_height_result.unwrap_err(), "can't get resharding block height!");
            return TaskSchedulingStatus::Failed;
        };
        // If the resharding block is beyond the chain final block, try again later.
        let chain_final_height = chain_final_head.height;
        if resharding_height > chain_final_height {
            info!(
                target = "resharding",
                ?resharding_height,
                ?chain_final_height,
                "resharding block height is higher than final block height: postponing task"
            );
            return TaskSchedulingStatus::Postponed;
        }
        // If the resharding block is not in the canonical chain this task has failed.
        match chain_store.get_block_hash_by_height(resharding_height) {
            Ok(hash) if hash == *resharding_hash => {
                TaskSchedulingStatus::CanStart(*resharding_block_info)
            }
            Ok(hash) => {
                info!(target: "resharding", ?resharding_height, ?resharding_hash, ?hash, "resharding block not in canonical chain!");
                TaskSchedulingStatus::Failed
            }
            Err(err) => {
                warn!(target: "resharding", ?resharding_height, ?resharding_hash, ?err, "can't find resharding block hash by height!");
                TaskSchedulingStatus::Failed
            }
        }
    }

    fn remove_resharding_event(&self) {
        *self.resharding_event.lock() = None;
    }

    #[cfg(feature = "test_features")]
    /// Returns true if a task should be "artificially" delayed. This behavior is configured through
    /// `adv_task_delay_by_blocks`.`
    fn adv_should_delay_task(
        &self,
        resharding_hash: &CryptoHash,
        chain_store: &ChainStore,
    ) -> bool {
        let blocks_delay = self.adv_task_delay_by_blocks;
        if blocks_delay == 0 {
            return false;
        }
        // Unwrap freely since this function is only used in tests.
        let chain_final_height = chain_store.final_head().unwrap().height;
        let resharding_height = chain_store.get_block_height(resharding_hash).unwrap();
        debug!(
            target = "resharding",
            resharding_height, chain_final_height, blocks_delay, "checking adversarial task delay"
        );
        return resharding_height + blocks_delay > chain_final_height;
    }
}

/// Enum used to wrap the `Item` of iterators over flat storage contents or flat storage deltas. Its
/// purpose is to insert a marker to force store commits during iteration over all entries. This is
/// necessary because otherwise deltas might set again the value of a flat storage entry inside the
/// same transaction.
enum FlatStorageAndDeltaIterItem {
    Entry(Result<(Vec<u8>, Option<FlatStateValue>), FlatStorageError>),
    CommitPoint,
}

type FlatStorageAndDeltaIter<'a> = dyn Iterator<Item = FlatStorageAndDeltaIterItem> + 'a;

impl Debug for FlatStorageResharder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatStorageResharder")
            .field("event", &self.resharding_event())
            .field("controller", &self.controller)
            .finish()
    }
}

/// Retrieves the flat head of the given `shard`.
/// The shard must be in [FlatStorageStatus::Ready] state otherwise this method returns an error.
fn retrieve_shard_flat_head(shard: ShardUId, store: &FlatStoreAdapter) -> Result<BlockInfo, Error> {
    let status =
        store.get_flat_storage_status(shard).map_err(|err| Into::<StorageError>::into(err))?;
    if let FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }) = status {
        Ok(flat_head)
    } else {
        let err_msg = "flat storage shard status is not ready!";
        error!(target: "resharding", ?shard, ?status, err_msg);
        Err(Error::ReshardingError(err_msg.to_owned()))
    }
}

/// Handles the inheritance of a key-value pair from parent shard to children shards.
fn shard_split_handle_key_value(
    key: Vec<u8>,
    value: Option<FlatStateValue>,
    store: &TrieStoreAdapter,
    store_update: &mut FlatStoreUpdateAdapter,
    split_params: &ParentSplitParameters,
) -> Result<(), Error> {
    if key.is_empty() {
        panic!("flat storage key is empty!")
    }
    let key_column_prefix = key[0];

    match key_column_prefix {
        col::ACCOUNT => copy_kv_to_child(
            &split_params,
            key,
            value,
            store_update,
            parse_account_id_from_account_key,
        )?,
        col::CONTRACT_DATA => copy_kv_to_child(
            &split_params,
            key,
            value,
            store_update,
            parse_account_id_from_contract_data_key,
        )?,
        col::CONTRACT_CODE => copy_kv_to_child(
            &split_params,
            key,
            value,
            store_update,
            parse_account_id_from_contract_code_key,
        )?,
        col::ACCESS_KEY => copy_kv_to_child(
            &split_params,
            key,
            value,
            store_update,
            parse_account_id_from_access_key_key,
        )?,
        col::RECEIVED_DATA => copy_kv_to_child(
            &split_params,
            key,
            value,
            store_update,
            parse_account_id_from_received_data_key,
        )?,
        col::POSTPONED_RECEIPT_ID
        | col::PENDING_DATA_COUNT
        | col::POSTPONED_RECEIPT
        | col::PROMISE_YIELD_RECEIPT => {
            copy_kv_to_child(&split_params, key, value, store_update, |raw_key: &[u8]| {
                parse_account_id_from_trie_key_with_separator(
                    key_column_prefix,
                    raw_key,
                    &format!("col at index {}", key_column_prefix),
                )
            })?
        }
        col::DELAYED_RECEIPT_OR_INDICES
        | col::PROMISE_YIELD_INDICES
        | col::PROMISE_YIELD_TIMEOUT
        | col::BANDWIDTH_SCHEDULER_STATE
        | col::GLOBAL_CONTRACT_CODE => {
            copy_kv_to_all_children(&split_params, key, value.clone(), store_update);
            let Some(flat_state_value) = value else {
                return Ok(());
            };
            // Doesn't matter which child ShardUId we use, as they both map to the parent.
            let shard_uid = split_params.left_child_shard;
            let (node_hash, node_value) = match flat_state_value {
                FlatStateValue::Inlined(node_value) => {
                    let node_hash = hash(&node_value);
                    (node_hash, node_value)
                }
                FlatStateValue::Ref(value_ref) => {
                    let node_hash = value_ref.hash;
                    let node_value = store.get(shard_uid, &node_hash)?.to_vec();
                    (node_hash, node_value)
                }
            };
            let refcount_increment = NonZero::new(1).unwrap();
            store_update.trie_store_update().increment_refcount_by(
                shard_uid,
                &node_hash,
                &node_value,
                refcount_increment,
            );
        }
        col::BUFFERED_RECEIPT_INDICES
        | col::BUFFERED_RECEIPT
        | col::BUFFERED_RECEIPT_GROUPS_QUEUE_DATA
        | col::BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM => {
            copy_kv_to_left_child(&split_params, key, value, store_update)
        }
        _ => unreachable!("key: {:?} should not appear in flat store!", key),
    }
    Ok(())
}

/// Copies a key-value pair to the correct child shard by matching the account-id to the provided shard layout.
fn copy_kv_to_child(
    split_params: &ParentSplitParameters,
    key: Vec<u8>,
    value: Option<FlatStateValue>,
    store_update: &mut FlatStoreUpdateAdapter,
    account_id_parser: impl FnOnce(&[u8]) -> Result<AccountId, std::io::Error>,
) -> Result<(), Error> {
    let ParentSplitParameters { left_child_shard, right_child_shard, shard_layout, .. } =
        &split_params;
    // Derive the shard uid for this account in the new shard layout.
    let account_id = account_id_parser(&key)?;
    let new_shard_id = shard_layout.account_id_to_shard_id(&account_id);
    let mut new_shard_uid = ShardUId::from_shard_id_and_layout(new_shard_id, &shard_layout);

    // Sanity check we are truly writing to one of the expected children shards.
    if new_shard_uid != *left_child_shard && new_shard_uid != *right_child_shard {
        // TODO(resharding): replace the code below with an assertion once the root cause is fixed.
        // The problem is that forknet state contains implicit accounts out of shard boundaries.
        let left_child_shard_boundary_account = &shard_layout.boundary_accounts()[shard_layout
            .get_shard_index(left_child_shard.shard_id())
            .expect("left child shard must exist!")];
        let closer_shard_uid = if account_id < *left_child_shard_boundary_account {
            left_child_shard
        } else {
            right_child_shard
        };

        let err_msg = "account id doesn't map to any child shard! - copying to the closer child";
        warn!(target: "resharding", ?new_shard_uid, ?closer_shard_uid, ?left_child_shard, ?right_child_shard, ?shard_layout, ?account_id, err_msg);

        new_shard_uid = *closer_shard_uid;
    }
    // Add the new flat store entry.
    store_update.set(new_shard_uid, key, value);
    Ok(())
}

/// Copies a key-value pair to both children.
fn copy_kv_to_all_children(
    split_params: &ParentSplitParameters,
    key: Vec<u8>,
    value: Option<FlatStateValue>,
    store_update: &mut FlatStoreUpdateAdapter,
) {
    store_update.set(split_params.left_child_shard, key.clone(), value.clone());
    store_update.set(split_params.right_child_shard, key, value);
}

/// Copies a key-value pair to the child on the left of the account boundary (also called 'first child').
fn copy_kv_to_left_child(
    split_params: &ParentSplitParameters,
    key: Vec<u8>,
    value: Option<FlatStateValue>,
    store_update: &mut FlatStoreUpdateAdapter,
) {
    store_update.set(split_params.left_child_shard, key, value);
}

/// Returns `true` if a flat head at `flat_head_block_hash` has reached the necessary height to be
/// considered in sync with the chain.
///
///  Observations:
/// - as a result of delta application during parent split, if the resharding is extremely fast the
///   flat head might be already on the last final block.
/// - the new flat head candidate is the previous block hash of the final head as stated in
///   `Chain::get_new_flat_storage_head`.
/// - this method assumes the flat head is never beyond the final chain.
fn is_flat_head_on_par_with_chain(
    flat_head_block_hash: &CryptoHash,
    chain_final_head: &Tip,
) -> bool {
    *flat_head_block_hash == chain_final_head.prev_block_hash
        || *flat_head_block_hash == chain_final_head.last_block_hash
}

/// Struct to describe, perform and track progress of a flat storage resharding.
#[derive(Clone, Debug)]
pub enum FlatStorageReshardingEventStatus {
    /// Split a shard. Includes the parent shard uid, the detailed information about the split
    /// operation (`ParentSplitParameters`) and the execution status of the task that is performing
    /// the split.
    SplitShard(ShardUId, ParentSplitParameters, TaskExecutionStatus),
}

impl FlatStorageReshardingEventStatus {
    /// Returns `true` if the resharding event has started processing.
    fn has_started(&self) -> bool {
        match self {
            FlatStorageReshardingEventStatus::SplitShard(_, _, execution_status) => {
                matches!(execution_status, TaskExecutionStatus::Started)
            }
        }
    }
}

/// All different states of task execution for [FlatStorageReshardingEventStatus].
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum TaskExecutionStatus {
    Started,
    NotStarted,
}

/// Result of a scheduled flat storage resharding task.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum FlatStorageReshardingTaskResult {
    Successful { num_batches_done: usize },
    Failed,
    Cancelled,
    Postponed,
}

/// Status of scheduling of a flat storage resharding tasks.
/// It is useful to know whether or not a task can start or has to be delayed.
enum TaskSchedulingStatus {
    CanStart(BlockInfo),
    Failed,
    Postponed,
}

/// Scheduling status of split shard task. It's an extension of [TaskSchedulingStatus].
enum SplitShardSchedulingStatus {
    CanStart(BlockInfo, ShardUId, ParentSplitParameters),
    Cancelled,
    Postponed,
    Failed,
}

/// Outcome of the task that applies deltas during shard catchup.
enum ShardCatchupApplyDeltasOutcome {
    /// Contains the number of delta batches applied and the final tip of the flat storage.
    Succeeded(usize, Tip),
    Cancelled,
    Postponed,
}

/// Helps control the flat storage resharder background operations. This struct wraps
/// [ReshardingHandle] and gives better meaning request to stop any processing when applied to flat
/// storage. In flat storage resharding there's a slight difference between interrupt and cancel.
/// Interruption happens when the node crashes whilst cancellation is an on demand request. An
/// interrupted flat storage resharding will resume on node restart, a cancelled one won't.
#[derive(Clone, Debug)]
pub struct FlatStorageResharderController {
    /// Resharding handle to control cancellation.
    handle: ReshardingHandle,
}

impl FlatStorageResharderController {
    /// Creates a new `FlatStorageResharderController` with its own handle.
    pub fn new() -> Self {
        let handle = ReshardingHandle::new();
        Self { handle }
    }

    pub fn from_resharding_handle(handle: ReshardingHandle) -> Self {
        Self { handle }
    }

    /// Returns whether or not background task is cancelled.
    pub fn is_cancelled(&self) -> bool {
        !self.handle.get()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use near_async::time::Clock;
    use near_chain_configs::{Genesis, MutableConfigValue, TrackedShardsConfig};
    use near_epoch_manager::{EpochManager, shard_tracker::ShardTracker};
    use near_o11y::testonly::init_test_logger;
    use near_primitives::{
        hash::CryptoHash,
        shard_layout::ShardLayout,
        state::FlatStateValue,
        test_utils::{TestBlockBuilder, create_test_signer},
        trie_key::TrieKey,
        types::{
            AccountId, BlockHeight, RawStateChange, RawStateChangesWithTrieKey, ShardId,
            StateChangeCause,
        },
    };
    use near_store::{
        flat::{BlockInfo, FlatStorageManager, FlatStorageReadyStatus},
        genesis::initialize_genesis_state,
        test_utils::create_test_store,
    };

    use crate::{
        Chain, ChainGenesis, DoomslugThresholdMode, rayon_spawner::RayonAsyncComputationSpawner,
        runtime::NightshadeRuntime, types::ChainConfig,
    };

    use super::*;
    use assert_matches::assert_matches;
    use more_asserts::assert_gt;
    use near_async::messaging::{CanSend, IntoMultiSender};
    use near_crypto::{KeyType, PublicKey};

    /// Shorthand to create account ID.
    macro_rules! account {
        ($str:expr) => {
            $str.parse::<AccountId>().unwrap()
        };
    }

    trait TestSender:
        CanSend<FlatStorageSplitShardRequest>
        + CanSend<FlatStorageShardCatchupRequest>
        + CanSend<MemtrieReloadRequest>
    {
        fn new(chain_store: ChainStore) -> Self;
    }

    /// Simple sender to execute tasks immediately on the same thread where they are issued.
    struct SimpleSender {
        chain_store: Arc<Mutex<ChainStore>>,
    }

    impl TestSender for SimpleSender {
        fn new(chain_store: ChainStore) -> Self {
            Self { chain_store: Arc::new(Mutex::new(chain_store)) }
        }
    }

    impl CanSend<FlatStorageSplitShardRequest> for SimpleSender {
        fn send(&self, msg: FlatStorageSplitShardRequest) {
            msg.resharder.split_shard_task(&self.chain_store.lock());
        }
    }

    impl CanSend<FlatStorageShardCatchupRequest> for SimpleSender {
        fn send(&self, msg: FlatStorageShardCatchupRequest) {
            msg.resharder.shard_catchup_task(msg.shard_uid, &self.chain_store.lock());
        }
    }

    impl CanSend<MemtrieReloadRequest> for SimpleSender {
        fn send(&self, _: MemtrieReloadRequest) {}
    }

    /// A sender that doesn't execute tasks immediately. Tasks execution must be invoked
    /// manually.
    struct DelayedSender {
        chain_store: Arc<Mutex<ChainStore>>,
        split_shard_request: Mutex<Option<FlatStorageSplitShardRequest>>,
        shard_catchup_requests: Mutex<Vec<FlatStorageShardCatchupRequest>>,
        memtrie_reload_requests: Mutex<Vec<ShardUId>>,
    }

    impl TestSender for DelayedSender {
        fn new(chain_store: ChainStore) -> Self {
            Self {
                chain_store: Arc::new(Mutex::new(chain_store)),
                split_shard_request: Default::default(),
                shard_catchup_requests: Default::default(),
                memtrie_reload_requests: Default::default(),
            }
        }
    }

    impl DelayedSender {
        fn call_split_shard_task(&self) -> FlatStorageReshardingTaskResult {
            let request = self.split_shard_request.lock();
            request.as_ref().unwrap().resharder.split_shard_task(&self.chain_store.lock())
        }

        fn call_shard_catchup_tasks(&self) -> Vec<FlatStorageReshardingTaskResult> {
            self.shard_catchup_requests
                .lock()
                .iter()
                .map(|request| {
                    request
                        .resharder
                        .shard_catchup_task(request.shard_uid, &self.chain_store.lock())
                })
                .collect()
        }

        fn clear(&self) {
            *self.split_shard_request.lock() = None;
            self.shard_catchup_requests.lock().clear();
            self.memtrie_reload_requests.lock().clear();
        }

        fn memtrie_reload_requests(&self) -> Vec<ShardUId> {
            self.memtrie_reload_requests.lock().clone()
        }
    }

    impl CanSend<FlatStorageSplitShardRequest> for DelayedSender {
        fn send(&self, msg: FlatStorageSplitShardRequest) {
            *self.split_shard_request.lock() = Some(msg);
        }
    }

    impl CanSend<FlatStorageShardCatchupRequest> for DelayedSender {
        fn send(&self, msg: FlatStorageShardCatchupRequest) {
            self.shard_catchup_requests.lock().push(msg);
        }
    }

    impl CanSend<MemtrieReloadRequest> for DelayedSender {
        fn send(&self, msg: MemtrieReloadRequest) {
            self.memtrie_reload_requests.lock().push(msg.shard_uid);
        }
    }

    impl FlatStorageResharder {
        /// Retrieves parent shard UIds and the split shard parameters, only if a resharding event
        /// is in progress and of type `Split`.
        fn get_parent_shard_and_split_params(&self) -> Option<(ShardUId, ParentSplitParameters)> {
            let event = self.resharding_event.lock();
            match event.as_ref() {
                Some(FlatStorageReshardingEventStatus::SplitShard(
                    parent_shard,
                    split_params,
                    ..,
                )) => Some((*parent_shard, split_params.clone())),
                None => None,
            }
        }
    }

    impl FlatStorageReshardingEventStatus {
        fn set_execution_status(&mut self, new_status: TaskExecutionStatus) {
            match self {
                FlatStorageReshardingEventStatus::SplitShard(_, _, execution_status) => {
                    *execution_status = new_status
                }
            }
        }
    }

    /// Simple shard layout with two shards.
    fn simple_shard_layout() -> ShardLayout {
        let s0 = ShardId::new(0);
        let s1 = ShardId::new(1);
        let shards_split_map = BTreeMap::from([(s0, vec![s0]), (s1, vec![s1])]);
        ShardLayout::v2(vec![account!("ff")], vec![s0, s1], Some(shards_split_map))
    }

    /// Derived from [simple_shard_layout] by splitting the second shard.
    fn shard_layout_after_split() -> ShardLayout {
        ShardLayout::derive_shard_layout(&simple_shard_layout(), "pp".parse().unwrap())
    }

    /// Shard layout with three shards.
    fn shards_layout_three_shards() -> ShardLayout {
        let s0 = ShardId::new(0);
        let s1 = ShardId::new(1);
        let s2 = ShardId::new(2);
        let shards_split_map = BTreeMap::from([(s0, vec![s0]), (s1, vec![s1]), (s2, vec![s2])]);
        ShardLayout::v2(
            vec![account!("ff"), account!("pp")],
            vec![s0, s1, s2],
            Some(shards_split_map),
        )
    }

    /// Derived from [shards_layout_three_shards] by splitting the second shard.
    fn shards_layout_three_shards_after_split() -> ShardLayout {
        ShardLayout::derive_shard_layout(&simple_shard_layout(), "yy".parse().unwrap())
    }

    /// Generic test setup. It creates an instance of chain, a FlatStorageResharder and a sender.
    fn create_chain_resharder_sender<T: TestSender>(
        shard_layout: ShardLayout,
    ) -> (Chain, FlatStorageResharder, Arc<T>) {
        let genesis = Genesis::from_accounts(
            Clock::real(),
            vec![account!("aa"), account!("mm"), account!("vv")],
            1,
            shard_layout.clone(),
        );
        let tempdir = tempfile::tempdir().unwrap();
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let shard_tracker =
            ShardTracker::new(TrackedShardsConfig::AllShards, epoch_manager.clone());
        let runtime = NightshadeRuntime::test(
            tempdir.path(),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let sender = Arc::new(T::new(ChainStore::new(
            store,
            false,
            chain_genesis.transaction_validity_period,
        )));
        let chain = Chain::new(
            Clock::real(),
            epoch_manager,
            shard_tracker,
            runtime,
            &chain_genesis,
            DoomslugThresholdMode::NoApprovals,
            ChainConfig::test(),
            None,
            Arc::new(RayonAsyncComputationSpawner),
            MutableConfigValue::new(None, "validator_signer"),
            sender.as_multi_sender(),
        )
        .unwrap();
        for shard_uid in shard_layout.shard_uids() {
            chain
                .runtime_adapter
                .get_flat_storage_manager()
                .create_flat_storage_for_shard(shard_uid)
                .unwrap();
        }
        let resharder = chain.resharding_manager.flat_storage_resharder.clone();
        (chain, resharder, sender)
    }

    /// Utility function to derive the resharding event type from chain and shard layout.
    fn event_type_from_chain_and_layout(
        chain: &Chain,
        new_shard_layout: &ShardLayout,
    ) -> ReshardingEventType {
        ReshardingEventType::from_shard_layout(&new_shard_layout, chain.head().unwrap().into())
            .unwrap()
            .unwrap()
    }

    enum PreviousBlockHeight {
        ChainHead,
        Fixed(u64),
    }

    enum NextBlockHeight {
        ChainHeadPlusOne,
        Fixed(u64),
    }

    /// Utility to add blocks on top of a chain.
    fn add_blocks_to_chain(
        chain: &mut Chain,
        num_blocks: u64,
        on_top_of_height: PreviousBlockHeight,
        next_height: NextBlockHeight,
    ) {
        assert_gt!(num_blocks, 0);
        let signer = Arc::new(create_test_signer("aa"));
        let mut prev_block_height = match on_top_of_height {
            PreviousBlockHeight::ChainHead => chain.head().unwrap().height,
            PreviousBlockHeight::Fixed(height) => height,
        };
        let next_block_height = match next_height {
            NextBlockHeight::ChainHeadPlusOne => chain.head().unwrap().height + 1,
            NextBlockHeight::Fixed(height) => height,
        };
        for height in next_block_height..next_block_height + num_blocks {
            // Take the previous block hash by navigating all block hashes at `prev_block_height` across all forks.
            // We pick the first available hash.
            let prev_block_hash = *chain
                .chain_store()
                .get_all_block_hashes_by_height(prev_block_height)
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .1
                .iter()
                .next()
                .unwrap();
            let prev_block = chain.get_block(&prev_block_hash).unwrap();
            let block = TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone())
                .height(height)
                .build();
            chain.process_block_test(&None, block).unwrap();
            prev_block_height = height;
        }
        assert_eq!(chain.head().unwrap().height, next_block_height + num_blocks - 1);
    }

    /// Verify that a new incompatible resharding event can't be triggered if another one has already started.
    #[test]
    fn concurrent_reshardings_are_disallowed() {
        init_test_logger();
        let (chain, resharder, _) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let controller = FlatStorageResharderController::new();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);

        assert!(
            resharder.start_resharding(resharding_event_type.clone(), &new_shard_layout).is_ok()
        );

        // Immediately cancel the resharding and call the resharding task.
        controller.handle.stop();
        let (parent_shard, split_params) = resharder.get_parent_shard_and_split_params().unwrap();
        let metrics = FlatStorageReshardingShardSplitMetrics::new(
            parent_shard,
            split_params.left_child_shard,
            split_params.right_child_shard,
        );
        resharder.split_shard_task_impl(
            parent_shard,
            &split_params,
            &split_params.resharding_blocks[0],
            &metrics,
        );
        resharder
            .resharding_event
            .lock()
            .as_mut()
            .map(|event| event.set_execution_status(TaskExecutionStatus::Started));

        assert!(resharder.resharding_event().is_some());
        assert_matches!(
            resharder.start_resharding(resharding_event_type, &new_shard_layout),
            Err(_)
        );
    }

    /// Flat storage shard status should be set correctly upon starting a shard split.
    #[test]
    fn flat_storage_split_status_set() {
        init_test_logger();
        let (chain, resharder, _) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let flat_store = resharder.runtime.store().flat_store();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);

        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());

        let resharding_event = resharder.resharding_event();
        match resharding_event.unwrap() {
            FlatStorageReshardingEventStatus::SplitShard(parent, status, exec_status) => {
                assert_eq!(
                    flat_store.get_flat_storage_status(parent),
                    Ok(FlatStorageStatus::Resharding(
                        FlatStorageReshardingStatus::SplittingParent(status.clone())
                    ))
                );
                assert_eq!(
                    flat_store.get_flat_storage_status(status.left_child_shard),
                    Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild))
                );
                assert_eq!(
                    flat_store.get_flat_storage_status(status.right_child_shard),
                    Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild))
                );
                assert_eq!(exec_status, TaskExecutionStatus::NotStarted);
            }
        }
    }

    /// In this test we write some dirty state into children shards and then try to resume a shard split.
    /// Verify that the dirty writes are cleaned up correctly.
    #[test]
    fn resume_split_starts_from_clean_state() {
        init_test_logger();
        let (chain, resharder, _) =
            create_chain_resharder_sender::<SimpleSender>(simple_shard_layout());
        let flat_store = resharder.runtime.store().flat_store();
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard, left_child_shard, right_child_shard, ..
        } = match resharding_event_type {
            ReshardingEventType::SplitShard(params) => params,
        };

        let mut store_update = flat_store.store_update();

        // Write some random key-values in children shards.
        let dirty_key: Vec<u8> = vec![1, 2, 3, 4];
        let dirty_value = Some(FlatStateValue::Inlined(dirty_key.clone()));
        for child_shard in [left_child_shard, right_child_shard] {
            store_update.set(child_shard, dirty_key.clone(), dirty_value.clone());
        }

        // Set parent state to ShardSplitting, manually, to simulate a forcibly cancelled resharding attempt.
        let resharding_status =
            FlatStorageReshardingStatus::SplittingParent(ParentSplitParameters {
                // Values don't matter.
                left_child_shard,
                right_child_shard,
                shard_layout: new_shard_layout,
                resharding_blocks: vec![BlockInfo {
                    hash: CryptoHash::default(),
                    height: 2,
                    prev_hash: CryptoHash::default(),
                }],
                flat_head: BlockInfo {
                    hash: CryptoHash::default(),
                    height: 1,
                    prev_hash: CryptoHash::default(),
                },
            });
        store_update.set_flat_storage_status(
            parent_shard,
            FlatStorageStatus::Resharding(resharding_status.clone()),
        );

        store_update.commit().unwrap();

        // Resume resharding.
        resharder.resume(parent_shard, &resharding_status).unwrap();

        // Children should not contain the random keys written before.
        for child_shard in [left_child_shard, right_child_shard] {
            assert_eq!(flat_store.get(child_shard, &dirty_key), Ok(None));
        }
    }

    /// Tests a simple split shard scenario.
    ///
    /// Old layout:
    /// shard 0 -> accounts [aa]
    /// shard 1 -> accounts [mm, vv]
    ///
    /// New layout:
    /// shard 0 -> accounts [aa]
    /// shard 2 -> accounts [mm]
    /// shard 3 -> accounts [vv]
    ///
    /// Shard to split is shard 1.
    #[test]
    fn simple_split_shard() {
        init_test_logger();
        let (mut chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let left_child = ShardUId { version: 3, shard_id: 2 };
        let right_child = ShardUId { version: 3, shard_id: 3 };
        let flat_store = resharder.runtime.store().flat_store();

        // Add two blocks on top of genesis. This will make the resharding block (height 0) final.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Perform resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        sender.call_split_shard_task();

        // Check final status of parent flat storage.
        let parent = ShardUId { version: 3, shard_id: 1 };
        assert_eq!(flat_store.get_flat_storage_status(parent), Ok(FlatStorageStatus::Empty));
        assert_eq!(flat_store.iter(parent).count(), 0);
        assert!(
            resharder
                .runtime
                .get_flat_storage_manager()
                .get_flat_storage_for_shard(parent)
                .is_none()
        );

        // Check intermediate status of children flat storages.
        for child in [left_child, right_child] {
            assert_eq!(
                flat_store.get_flat_storage_status(child),
                Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(
                    chain.final_head().unwrap().into()
                )))
            );
        }

        // Perform children catchup.
        sender.call_shard_catchup_tasks();

        // Check flat storages of children contain the correct accounts and access keys.
        let account_mm_key = TrieKey::Account { account_id: account!("mm") };
        let account_vv_key = TrieKey::Account { account_id: account!("vv") };
        assert!(
            flat_store.get(left_child, &account_mm_key.to_vec()).is_ok_and(|val| val.is_some())
        );
        assert!(
            flat_store.get(right_child, &account_vv_key.to_vec()).is_ok_and(|val| val.is_some())
        );
        let account_mm_access_key = TrieKey::AccessKey {
            account_id: account!("mm"),
            public_key: PublicKey::from_seed(KeyType::ED25519, account!("mm").as_str()),
        };
        let account_vv_access_key = TrieKey::AccessKey {
            account_id: account!("vv"),
            public_key: PublicKey::from_seed(KeyType::ED25519, account!("vv").as_str()),
        };
        assert!(
            flat_store
                .get(left_child, &account_mm_access_key.to_vec())
                .is_ok_and(|val| val.is_some())
        );
        assert!(
            flat_store
                .get(right_child, &account_vv_access_key.to_vec())
                .is_ok_and(|val| val.is_some())
        );

        // Check final status of children flat storages.
        for child in [left_child, right_child] {
            assert_eq!(
                flat_store.get_flat_storage_status(child),
                Ok(FlatStorageStatus::Ready(FlatStorageReadyStatus {
                    flat_head: BlockInfo {
                        hash: chain.final_head().unwrap().last_block_hash,
                        height: chain.final_head().unwrap().height,
                        prev_hash: chain.final_head().unwrap().prev_block_hash
                    }
                }))
            );
        }
    }

    /// Split shard task should run in batches.
    #[test]
    fn split_shard_batching() {
        init_test_logger();
        let (chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);

        // Tweak the resharding config to make smaller batches.
        let mut config = resharder.resharding_config.get();
        config.batch_size = bytesize::ByteSize(1);
        resharder.resharding_config.update(config);

        // Perform resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());

        // Check that more than one batch has been processed.
        let FlatStorageReshardingTaskResult::Successful { num_batches_done } =
            sender.call_split_shard_task()
        else {
            assert!(false);
            return;
        };
        assert_gt!(num_batches_done, 1);
    }

    #[test]
    fn cancel_split_shard() {
        init_test_logger();
        let (chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);

        // Perform resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        let (parent_shard, split_params) = resharder.get_parent_shard_and_split_params().unwrap();
        let ParentSplitParameters { left_child_shard, right_child_shard, .. } = split_params;

        // Cancel the task before it starts.
        resharder.controller.handle.stop();

        // Run the task.
        sender.call_split_shard_task();

        // Check that the resharding task was effectively cancelled.
        // Note that resharding as a whole is not cancelled: it should resume if the node restarts.
        let flat_store = resharder.runtime.store().flat_store();
        assert_matches!(
            flat_store.get_flat_storage_status(parent_shard),
            Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::SplittingParent(_)))
        );
        for child_shard in [left_child_shard, right_child_shard] {
            assert_eq!(
                flat_store.get_flat_storage_status(child_shard),
                Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild))
            );
            assert_eq!(flat_store.iter(child_shard).count(), 0);
        }
    }

    #[test]
    fn cancel_shard_catchup() {
        init_test_logger();
        let (chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);

        // Perform resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        let (_, split_params) = resharder.get_parent_shard_and_split_params().unwrap();
        let ParentSplitParameters { left_child_shard, right_child_shard, .. } = split_params;

        // Run the split task.
        assert_matches!(
            sender.call_split_shard_task(),
            FlatStorageReshardingTaskResult::Successful { num_batches_done: _ }
        );

        // Cancel resharding.
        resharder.controller.handle.stop();

        // Run the catchup tasks.
        assert_eq!(
            sender.call_shard_catchup_tasks(),
            vec![
                FlatStorageReshardingTaskResult::Cancelled,
                FlatStorageReshardingTaskResult::Cancelled
            ]
        );

        // Check that resharding was effectively cancelled.
        let flat_store = resharder.runtime.store().flat_store();
        for child_shard in [left_child_shard, right_child_shard] {
            assert_matches!(
                flat_store.get_flat_storage_status(child_shard),
                Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(_)))
            );
        }
    }

    /// A shard can't be split if it isn't in ready state.
    #[test]
    fn reject_split_shard_if_parent_is_not_ready() {
        let (chain, resharder, _) =
            create_chain_resharder_sender::<SimpleSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);

        // Make flat storage of parent shard not ready.
        let parent_shard = ShardUId { version: 3, shard_id: 1 };
        let flat_store = resharder.runtime.store().flat_store();
        let mut store_update = flat_store.store_update();
        store_update.set_flat_storage_status(parent_shard, FlatStorageStatus::Empty);
        store_update.commit().unwrap();

        // Trigger resharding and it should fail.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_err());
    }

    /// Verify the correctness of a shard split in the presence of flat storage deltas in the parent
    /// shard.
    #[test]
    fn split_shard_parent_flat_store_with_deltas() {
        init_test_logger();
        let (mut chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();

        // In order to have flat state deltas we must bring the chain forward by adding blocks.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard, left_child_shard, right_child_shard, ..
        } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };

        // Bring chain forward in order to make the resharding block (height 2) final.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        let manager = chain.runtime_adapter.get_flat_storage_manager();

        // Manually add deltas on top of parent's flat storage.
        // Pick different kind of keys and operations in order to maximize test coverage.
        // List of all keys and their values:
        let account_vv_key = TrieKey::Account { account_id: account!("vv") };
        let account_vv_value = Some("vv-update".as_bytes().to_vec());
        let account_oo_key = TrieKey::Account { account_id: account!("oo") };
        let account_oo_value = Some("oo".as_bytes().to_vec());
        let account_mm_key = TrieKey::Account { account_id: account!("mm") };
        let delayed_receipt_0_key = TrieKey::DelayedReceipt { index: 0 };
        let delayed_receipt_0_value_0 = Some("delayed0-0".as_bytes().to_vec());
        let delayed_receipt_0_value_1 = Some("delayed0-1".as_bytes().to_vec());
        let delayed_receipt_1_key = TrieKey::DelayedReceipt { index: 1 };
        let delayed_receipt_1_value = Some("delayed1".as_bytes().to_vec());
        let buffered_receipt_0_key =
            TrieKey::BufferedReceipt { receiving_shard: ShardId::new(0), index: 0 };
        let buffered_receipt_0_value_0 = Some("buffered0-0".as_bytes().to_vec());
        let buffered_receipt_0_value_1 = Some("buffered0-1".as_bytes().to_vec());
        let buffered_receipt_1_key =
            TrieKey::BufferedReceipt { receiving_shard: ShardId::new(0), index: 1 };
        let buffered_receipt_1_value = Some("buffered1".as_bytes().to_vec());

        // First set of deltas.
        let height = 1;
        let prev_hash = *chain.get_block_by_height(height).unwrap().header().prev_hash();
        let block_hash = *chain.get_block_by_height(height).unwrap().hash();
        let state_changes = vec![
            // Change: add account.
            RawStateChangesWithTrieKey {
                trie_key: account_oo_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: account_oo_value.clone(),
                }],
            },
            // Change: update account.
            RawStateChangesWithTrieKey {
                trie_key: account_vv_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: account_vv_value.clone(),
                }],
            },
            // Change: add two delayed receipts.
            RawStateChangesWithTrieKey {
                trie_key: delayed_receipt_0_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: delayed_receipt_0_value_0,
                }],
            },
            RawStateChangesWithTrieKey {
                trie_key: delayed_receipt_1_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: delayed_receipt_1_value,
                }],
            },
            // Change: update delayed receipt.
            RawStateChangesWithTrieKey {
                trie_key: delayed_receipt_0_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: delayed_receipt_0_value_1.clone(),
                }],
            },
            // Change: add two buffered receipts.
            RawStateChangesWithTrieKey {
                trie_key: buffered_receipt_0_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: buffered_receipt_0_value_0,
                }],
            },
            RawStateChangesWithTrieKey {
                trie_key: buffered_receipt_1_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: buffered_receipt_1_value,
                }],
            },
            // Change: update buffered receipt.
            RawStateChangesWithTrieKey {
                trie_key: buffered_receipt_0_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: buffered_receipt_0_value_1.clone(),
                }],
            },
        ];
        manager
            .save_flat_state_changes(block_hash, prev_hash, height, parent_shard, &state_changes)
            .unwrap()
            .commit()
            .unwrap();

        // Second set of deltas.
        let height = 2;
        let prev_hash = *chain.get_block_by_height(height).unwrap().header().prev_hash();
        let block_hash = *chain.get_block_by_height(height).unwrap().hash();
        let state_changes = vec![
            // Change: remove account.
            RawStateChangesWithTrieKey {
                trie_key: account_mm_key,
                changes: vec![RawStateChange { cause: StateChangeCause::InitialState, data: None }],
            },
            // Change: remove delayed receipt.
            RawStateChangesWithTrieKey {
                trie_key: delayed_receipt_1_key.clone(),
                changes: vec![RawStateChange { cause: StateChangeCause::InitialState, data: None }],
            },
            // Change: remove buffered receipt.
            RawStateChangesWithTrieKey {
                trie_key: buffered_receipt_1_key.clone(),
                changes: vec![RawStateChange { cause: StateChangeCause::InitialState, data: None }],
            },
        ];
        manager
            .save_flat_state_changes(block_hash, prev_hash, height, parent_shard, &state_changes)
            .unwrap()
            .commit()
            .unwrap();

        // Do resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        assert_eq!(
            sender.call_split_shard_task(),
            FlatStorageReshardingTaskResult::Successful { num_batches_done: 3 }
        );

        // Validate integrity of children shards.
        let flat_store = resharder.runtime.store().flat_store();
        // Account 'oo' should exist only in the left child.
        assert_eq!(
            flat_store.get(left_child_shard, &account_oo_key.to_vec()),
            Ok(account_oo_value.map(|val| FlatStateValue::inlined(&val)))
        );
        assert_eq!(flat_store.get(right_child_shard, &account_oo_key.to_vec()), Ok(None));
        // Account 'vv' should exist with updated value only in the right child.
        assert_eq!(flat_store.get(left_child_shard, &account_vv_key.to_vec()), Ok(None));
        assert_eq!(
            flat_store.get(right_child_shard, &account_vv_key.to_vec()),
            Ok(account_vv_value.map(|val| FlatStateValue::inlined(&val)))
        );
        // Delayed receipt '1' shouldn't exist.
        // Delayed receipt '0' should exist with updated value in both children.
        for child in [left_child_shard, right_child_shard] {
            assert_eq!(
                flat_store.get(child, &delayed_receipt_0_key.to_vec()),
                Ok(delayed_receipt_0_value_1.clone().map(|val| FlatStateValue::inlined(&val)))
            );

            assert_eq!(flat_store.get(child, &delayed_receipt_1_key.to_vec()), Ok(None));
        }
        // Buffered receipt '0' should exist with updated value only in the left child.
        assert_eq!(
            flat_store.get(left_child_shard, &buffered_receipt_0_key.to_vec()),
            Ok(buffered_receipt_0_value_1.map(|val| FlatStateValue::inlined(&val)))
        );
        assert_eq!(flat_store.get(right_child_shard, &buffered_receipt_0_key.to_vec()), Ok(None));
        // Buffered receipt '1' shouldn't exist.
        for child in [left_child_shard, right_child_shard] {
            assert_eq!(flat_store.get(child, &buffered_receipt_1_key.to_vec()), Ok(None));
        }
    }

    /// Tests the split of "account-id based" keys that are not covered in [simple_split_shard].
    ///
    /// Old layout:
    /// shard 0 -> accounts [aa]
    /// shard 1 -> accounts [mm, vv]
    ///
    /// New layout:
    /// shard 0 -> accounts [aa]
    /// shard 2 -> accounts [mm]
    /// shard 3 -> accounts [vv]
    #[test]
    fn split_shard_handle_account_id_keys() {
        init_test_logger();
        let (chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard, left_child_shard, right_child_shard, ..
        } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        let flat_store = resharder.runtime.store().flat_store();

        let mut store_update = flat_store.store_update();
        let test_value = Some(FlatStateValue::Inlined(vec![0]));

        // Helper closure to create all test keys for a given account. Returns the created keys.
        let mut inject = |account: AccountId| -> Vec<Vec<u8>> {
            let mut keys = vec![];

            // Inject contract data.
            let key = TrieKey::ContractData { account_id: account.clone(), key: vec![] }.to_vec();
            store_update.set(parent_shard, key.clone(), test_value.clone());
            keys.push(key);

            // Inject contract code.
            let key = TrieKey::ContractCode { account_id: account.clone() }.to_vec();
            store_update.set(parent_shard, key.clone(), test_value.clone());
            keys.push(key);

            // Inject received_data.
            let key = TrieKey::ReceivedData {
                receiver_id: account.clone(),
                data_id: CryptoHash::default(),
            }
            .to_vec();
            store_update.set(parent_shard, key.clone(), test_value.clone());
            keys.push(key);

            // Inject postponed receipt.
            let key = TrieKey::PostponedReceiptId {
                receiver_id: account.clone(),
                data_id: CryptoHash::default(),
            }
            .to_vec();
            store_update.set(parent_shard, key.clone(), test_value.clone());
            keys.push(key);
            let key = TrieKey::PendingDataCount {
                receiver_id: account.clone(),
                receipt_id: CryptoHash::default(),
            }
            .to_vec();
            store_update.set(parent_shard, key.clone(), test_value.clone());
            keys.push(key);
            let key = TrieKey::PostponedReceipt {
                receiver_id: account,
                receipt_id: CryptoHash::default(),
            }
            .to_vec();
            store_update.set(parent_shard, key.clone(), test_value.clone());
            keys.push(key);

            keys
        };

        let account_mm_keys = inject(account!("mm"));
        let account_vv_keys = inject(account!("vv"));
        store_update.commit().unwrap();

        // Do resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        sender.call_split_shard_task();

        // Check each child has the correct keys assigned to itself.
        for key in &account_mm_keys {
            assert_eq!(flat_store.get(left_child_shard, key), Ok(test_value.clone()));
            assert_eq!(flat_store.get(right_child_shard, key), Ok(None));
        }
        for key in &account_vv_keys {
            assert_eq!(flat_store.get(left_child_shard, key), Ok(None));
            assert_eq!(flat_store.get(right_child_shard, key), Ok(test_value.clone()));
        }
    }

    /// Tests the split of delayed receipts.
    #[test]
    fn split_shard_handle_delayed_receipts() {
        init_test_logger();
        let (chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard, left_child_shard, right_child_shard, ..
        } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        let flat_store = resharder.runtime.store().flat_store();

        // Inject a delayed receipt into the parent flat storage.
        let mut store_update = flat_store.store_update();

        let delayed_receipt_indices_key = TrieKey::DelayedReceiptIndices.to_vec();
        let delayed_receipt_indices_value = Some(FlatStateValue::Inlined(vec![0]));
        store_update.set(
            parent_shard,
            delayed_receipt_indices_key.clone(),
            delayed_receipt_indices_value.clone(),
        );

        let delayed_receipt_key = TrieKey::DelayedReceipt { index: 0 }.to_vec();
        let delayed_receipt_value = Some(FlatStateValue::Inlined(vec![1]));
        store_update.set(parent_shard, delayed_receipt_key.clone(), delayed_receipt_value.clone());

        store_update.commit().unwrap();

        // Do resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        sender.call_split_shard_task();

        // Check that flat storages of both children contain the delayed receipt.
        for child_shard in [left_child_shard, right_child_shard] {
            assert_eq!(
                flat_store.get(child_shard, &delayed_receipt_indices_key),
                Ok(delayed_receipt_indices_value.clone())
            );
            assert_eq!(
                flat_store.get(child_shard, &delayed_receipt_key),
                Ok(delayed_receipt_value.clone())
            );
        }
    }

    /// Tests the split of promise yield receipts.
    #[test]
    fn split_shard_handle_promise_yield() {
        init_test_logger();
        let (chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard, left_child_shard, right_child_shard, ..
        } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        let flat_store = resharder.runtime.store().flat_store();

        // Inject two promise yield receipts into the parent flat storage.
        let mut store_update = flat_store.store_update();

        let promise_yield_indices_key = TrieKey::PromiseYieldIndices.to_vec();
        let promise_yield_indices_value = Some(FlatStateValue::Inlined(vec![0]));
        store_update.set(
            parent_shard,
            promise_yield_indices_key.clone(),
            promise_yield_indices_value.clone(),
        );

        let promise_yield_timeout_key = TrieKey::PromiseYieldTimeout { index: 0 }.to_vec();
        let promise_yield_timeout_value = Some(FlatStateValue::Inlined(vec![1]));
        store_update.set(
            parent_shard,
            promise_yield_timeout_key.clone(),
            promise_yield_timeout_value.clone(),
        );

        let promise_yield_receipt_mm_key = TrieKey::PromiseYieldReceipt {
            receiver_id: account!("mm"),
            data_id: CryptoHash::default(),
        }
        .to_vec();
        let promise_yield_receipt_vv_key = TrieKey::PromiseYieldReceipt {
            receiver_id: account!("vv"),
            data_id: CryptoHash::default(),
        }
        .to_vec();
        let promise_yield_receipt_value = Some(FlatStateValue::Inlined(vec![2]));
        store_update.set(
            parent_shard,
            promise_yield_receipt_mm_key.clone(),
            promise_yield_receipt_value.clone(),
        );
        store_update.set(
            parent_shard,
            promise_yield_receipt_vv_key.clone(),
            promise_yield_receipt_value.clone(),
        );

        store_update.commit().unwrap();

        // Do resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        sender.call_split_shard_task();

        // Check that flat storages of both children contain the promise yield timeout and indices.
        for child_shard in [left_child_shard, right_child_shard] {
            assert_eq!(
                flat_store.get(child_shard, &promise_yield_indices_key),
                Ok(promise_yield_indices_value.clone())
            );
            assert_eq!(
                flat_store.get(child_shard, &promise_yield_timeout_key),
                Ok(promise_yield_timeout_value.clone())
            );
        }
        // Receipts work differently: these should be split depending on the account.
        assert_eq!(
            flat_store.get(left_child_shard, &promise_yield_receipt_mm_key),
            Ok(promise_yield_receipt_value.clone())
        );
        assert_eq!(flat_store.get(left_child_shard, &promise_yield_receipt_vv_key), Ok(None));
        assert_eq!(flat_store.get(right_child_shard, &promise_yield_receipt_mm_key), Ok(None));
        assert_eq!(
            flat_store.get(right_child_shard, &promise_yield_receipt_vv_key),
            Ok(promise_yield_receipt_value)
        );
    }

    /// Tests the split of buffered receipts.
    #[test]
    fn split_shard_handle_buffered_receipts() {
        init_test_logger();
        let (chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard, left_child_shard, right_child_shard, ..
        } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        let flat_store = resharder.runtime.store().flat_store();

        // Inject a buffered receipt into the parent flat storage.
        let mut store_update = flat_store.store_update();

        let buffered_receipt_indices_key = TrieKey::BufferedReceiptIndices.to_vec();
        let buffered_receipt_indices_value = Some(FlatStateValue::Inlined(vec![0]));
        store_update.set(
            parent_shard,
            buffered_receipt_indices_key.clone(),
            buffered_receipt_indices_value.clone(),
        );

        let receiving_shard = ShardId::new(0);
        let buffered_receipt_key = TrieKey::BufferedReceipt { receiving_shard, index: 0 }.to_vec();
        let buffered_receipt_value = Some(FlatStateValue::Inlined(vec![1]));
        store_update.set(
            parent_shard,
            buffered_receipt_key.clone(),
            buffered_receipt_value.clone(),
        );

        store_update.commit().unwrap();

        // Do resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        sender.call_split_shard_task();

        // Check that only the first child contain the buffered receipt.
        assert_eq!(
            flat_store.get(left_child_shard, &buffered_receipt_indices_key),
            Ok(buffered_receipt_indices_value)
        );
        assert_eq!(flat_store.get(right_child_shard, &buffered_receipt_indices_key), Ok(None));
        assert_eq!(
            flat_store.get(left_child_shard, &buffered_receipt_key),
            Ok(buffered_receipt_value)
        );
        assert_eq!(flat_store.get(right_child_shard, &buffered_receipt_key), Ok(None));
    }

    /// Base test scenario for testing children catchup.
    fn children_catchup_base(with_restart: bool) {
        init_test_logger();
        let (mut chain, mut resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard,
            left_child_shard,
            right_child_shard,
            resharding_block,
            ..
        } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        let manager = chain.runtime_adapter.get_flat_storage_manager();

        // Do resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());

        // Trigger the task to perform the parent split.
        sender.call_split_shard_task();

        // Simulate the chain going forward by seven blocks.
        // Note that the last two blocks won't be yet 'final'.
        const NUM_BLOCKS: u64 = 5;
        let signer = Arc::new(create_test_signer("aa"));
        for height in 1..NUM_BLOCKS + 1 {
            let prev_block = chain.get_block_by_height(height - 1).unwrap();
            let block = TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone())
                .height(height)
                .build();
            chain.process_block_test(&None, block).unwrap();
        }
        assert_eq!(chain.head().unwrap().height, NUM_BLOCKS);

        // Manually add deltas into the children shards.
        // For simplicity, add one new account at every height.
        for height in 1..NUM_BLOCKS + 1 {
            let prev_hash = *chain.get_block_by_height(height).unwrap().header().prev_hash();
            let block_hash = *chain.get_block_by_height(height).unwrap().hash();
            create_new_account_through_deltas(
                &manager,
                account!(format!("oo{}", height)),
                block_hash,
                prev_hash,
                height,
                left_child_shard,
            );
            create_new_account_through_deltas(
                &manager,
                account!(format!("zz{}", height)),
                block_hash,
                prev_hash,
                height,
                right_child_shard,
            );
        }

        // If this test is checking a node restart scenario: rebuild the resharder from scratch.
        if with_restart {
            sender.clear();
            resharder = FlatStorageResharder::new(
                resharder.runtime,
                resharder.sender,
                resharder.controller,
                resharder.resharding_config,
            );
            assert!(
                resharder
                    .resume(
                        left_child_shard,
                        &FlatStorageReshardingStatus::CatchingUp(resharding_block)
                    )
                    .is_ok()
            );
            assert!(
                resharder
                    .resume(
                        right_child_shard,
                        &FlatStorageReshardingStatus::CatchingUp(resharding_block)
                    )
                    .is_ok()
            );
        }

        // Trigger the catchup tasks.
        assert_eq!(
            sender.call_shard_catchup_tasks(),
            vec![
                FlatStorageReshardingTaskResult::Successful { num_batches_done: 1 },
                FlatStorageReshardingTaskResult::Successful { num_batches_done: 1 }
            ]
        );

        // Check shards flat storage status.
        let flat_store = resharder.runtime.store().flat_store();
        let prev_last_final_block = chain.get_block_by_height(NUM_BLOCKS - 3).unwrap();
        assert_eq!(flat_store.get_flat_storage_status(parent_shard), Ok(FlatStorageStatus::Empty));
        for child_shard in [left_child_shard, right_child_shard] {
            assert_eq!(
                flat_store.get_flat_storage_status(child_shard),
                Ok(FlatStorageStatus::Ready(FlatStorageReadyStatus {
                    flat_head: BlockInfo {
                        hash: *prev_last_final_block.hash(),
                        height: prev_last_final_block.header().height(),
                        prev_hash: *prev_last_final_block.header().prev_hash()
                    }
                }))
            );
            assert!(
                resharder
                    .runtime
                    .get_flat_storage_manager()
                    .get_flat_storage_for_shard(child_shard)
                    .is_some()
            );
        }
        // Children flat storages should contain the new accounts created through the deltas
        // application.
        // Flat store will only contain changes until the previous final block.
        for height in 1..NUM_BLOCKS - 2 {
            let new_account_left_child = account!(format!("oo{}", height));
            assert_eq!(
                flat_store.get(
                    left_child_shard,
                    &TrieKey::Account { account_id: new_account_left_child.clone() }.to_vec()
                ),
                Ok(Some(FlatStateValue::inlined(new_account_left_child.as_bytes())))
            );
            let new_account_right_child = account!(format!("zz{}", height));
            assert_eq!(
                flat_store.get(
                    right_child_shard,
                    &TrieKey::Account { account_id: new_account_right_child.clone() }.to_vec()
                ),
                Ok(Some(FlatStateValue::inlined(new_account_right_child.as_bytes())))
            );
        }
        // All changes can be retrieved through the flat store chunk view.
        let left_child_chunk_view =
            manager.chunk_view(left_child_shard, chain.head().unwrap().last_block_hash).unwrap();
        let right_child_chunk_view =
            manager.chunk_view(right_child_shard, chain.head().unwrap().last_block_hash).unwrap();
        for height in 1..NUM_BLOCKS + 1 {
            let new_account_left_child = account!(format!("oo{}", height));
            assert_eq!(
                left_child_chunk_view
                    .get_value(
                        &TrieKey::Account { account_id: new_account_left_child.clone() }.to_vec()
                    )
                    .map(|result| result.map(|option| option.to_value_ref())),
                Ok(Some(FlatStateValue::inlined(new_account_left_child.as_bytes()).to_value_ref()))
            );
            let new_account_right_child = account!(format!("zz{}", height));
            assert_eq!(
                right_child_chunk_view
                    .get_value(
                        &TrieKey::Account { account_id: new_account_right_child.clone() }.to_vec()
                    )
                    .map(|result| result.map(|option| option.to_value_ref())),
                Ok(Some(
                    FlatStateValue::inlined(new_account_right_child.as_bytes()).to_value_ref()
                ))
            );
        }
        // In the end there should be two requests for memtrie reloading.
        assert_eq!(sender.memtrie_reload_requests(), vec![left_child_shard, right_child_shard]);
    }

    /// Creates a new account through a state change saved as flat storage deltas.
    fn create_new_account_through_deltas(
        manager: &FlatStorageManager,
        account: AccountId,
        block_hash: CryptoHash,
        prev_hash: CryptoHash,
        height: BlockHeight,
        shard_uid: ShardUId,
    ) {
        let state_changes = vec![RawStateChangesWithTrieKey {
            trie_key: TrieKey::Account { account_id: account.clone() },
            changes: vec![RawStateChange {
                cause: StateChangeCause::InitialState,
                data: Some(account.as_bytes().to_vec()),
            }],
        }];
        manager
            .save_flat_state_changes(block_hash, prev_hash, height, shard_uid, &state_changes)
            .unwrap()
            .commit()
            .unwrap();
    }

    /// Tests the correctness of children catchup operation after a shard split.
    #[test]
    fn children_catchup_after_split() {
        children_catchup_base(false);
    }

    /// Checks that children can perform catchup correctly even if the node has been restarted in
    /// the middle of the process.
    #[test]
    fn children_catchup_after_restart() {
        children_catchup_base(true);
    }

    /// The split of a parent shard shouldn't happen until the resharding block has become final.
    #[test]
    fn shard_split_should_wait_final_block() {
        init_test_logger();
        let (mut chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let flat_store = resharder.runtime.store().flat_store();

        // Add two blocks on top of genesis.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Trigger resharding at block 2 and it shouldn't split the parent shard.
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams { parent_shard, .. } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        assert_eq!(sender.call_split_shard_task(), FlatStorageReshardingTaskResult::Postponed);
        assert_gt!(flat_store.iter(parent_shard).count(), 0);

        // Move the chain final head to the resharding block height (2).
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Trigger resharding again and now it should split the parent shard.
        assert_eq!(
            sender.call_split_shard_task(),
            FlatStorageReshardingTaskResult::Successful { num_batches_done: 3 }
        );
        assert_eq!(flat_store.iter(parent_shard).count(), 0);
    }

    /// Test to verify that a resharding event not yet started can be replaced by a newer resharding
    /// event on a different resharding hash. This property is useful to have in the presence of
    /// chain forks. For instance, the chain may wants to split a shard at some block B; there's a
    /// chance B never becomes final and instead a new split is triggered at block B'. The latter
    /// shouldn't be blocked by the presence of an earlier resharding event.
    #[test]
    fn resharding_event_not_started_can_be_replaced() {
        init_test_logger();
        let (mut chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let flat_store = resharder.runtime.store().flat_store();

        // Add two blocks on top of genesis.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Trigger resharding at block 2. Parent shard shouldn't get split yet.
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams { parent_shard, .. } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        assert_eq!(sender.call_split_shard_task(), FlatStorageReshardingTaskResult::Postponed);
        assert_gt!(flat_store.iter(parent_shard).count(), 0);

        // Add two blocks on top of the first block (simulate a fork).
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::Fixed(1),
            NextBlockHeight::Fixed(3),
        );

        // Get the new resharding event and re-trigger the shard split.
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams { parent_shard, .. } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        assert_eq!(sender.call_split_shard_task(), FlatStorageReshardingTaskResult::Postponed);
        assert_gt!(flat_store.iter(parent_shard).count(), 0);

        // Add two additional blocks on the fork to make the resharding block (height 1) final.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::Fixed(4),
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Now the second resharding event should take place.
        assert_matches!(
            sender.call_split_shard_task(),
            FlatStorageReshardingTaskResult::Successful { .. }
        );

        assert_eq!(flat_store.iter(parent_shard).count(), 0);
    }

    /// In this test we make sure that after a task whose scheduling has failed the cleanup logic is
    /// executed correctly.
    #[test]
    fn scheduled_task_failure_is_handled_correctly() {
        init_test_logger();
        let (mut chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();

        // Add two blocks on top of genesis.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Trigger resharding at block 2.
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        let (parent_shard, split_params) = resharder.get_parent_shard_and_split_params().unwrap();
        let ParentSplitParameters { flat_head, .. } = split_params;
        assert_eq!(sender.call_split_shard_task(), FlatStorageReshardingTaskResult::Postponed);

        // Fork the chain before the resharding block and make it final, but don't update the
        // resharding block hash.
        add_blocks_to_chain(
            &mut chain,
            3,
            PreviousBlockHeight::Fixed(1),
            NextBlockHeight::Fixed(3),
        );

        // Scheduling of the shard split should fail.
        assert_eq!(sender.call_split_shard_task(), FlatStorageReshardingTaskResult::Failed);
        assert!(resharder.resharding_event().is_none());
        let flat_store = resharder.runtime.store().flat_store();
        assert_eq!(
            flat_store.get_flat_storage_status(parent_shard),
            Ok(FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }))
        );
    }

    /// This test asserts that resharding doesn't fail if flat storage iteration goes over an account
    /// which is not part of any children shards after the shard layout changes.
    #[test]
    fn unrelated_account_do_not_fail_splitting() {
        init_test_logger();
        let (mut chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(shards_layout_three_shards());
        let new_shard_layout = shards_layout_three_shards_after_split();
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams {
            parent_shard, left_child_shard, right_child_shard, ..
        } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        let flat_store = resharder.runtime.store().flat_store();

        // Add two blocks on top of genesis. This will make the resharding block (height 0) final.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Inject an account which doesn't belong to the parent shard into its flat storage.
        let mut store_update = flat_store.store_update();
        let test_value = Some(FlatStateValue::Inlined(vec![0]));
        let key_before_left_child = TrieKey::Account { account_id: account!("ab") };
        let key_after_right_child = TrieKey::Account { account_id: account!("zz") };
        store_update.set(parent_shard, key_before_left_child.to_vec(), test_value.clone());
        store_update.set(parent_shard, key_after_right_child.to_vec(), test_value);
        store_update.commit().unwrap();

        // Perform resharding.
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        sender.call_split_shard_task();

        // Check final status of parent flat storage.
        let parent = ShardUId { version: 3, shard_id: 1 };
        assert_eq!(flat_store.get_flat_storage_status(parent), Ok(FlatStorageStatus::Empty));
        assert_eq!(flat_store.iter(parent).count(), 0);
        assert!(
            resharder
                .runtime
                .get_flat_storage_manager()
                .get_flat_storage_for_shard(parent)
                .is_none()
        );

        // Check intermediate status of children flat storages.
        // If children reached the catching up state, it means that the split task succeeded.
        for child in [left_child_shard, right_child_shard] {
            assert_eq!(
                flat_store.get_flat_storage_status(child),
                Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(
                    chain.final_head().unwrap().into()
                )))
            );
        }
        // The unrelated accounts should end up in the 'closer' child.
        assert!(
            flat_store
                .get(left_child_shard, &key_before_left_child.to_vec())
                .is_ok_and(|val| val.is_some())
        );
        assert!(
            flat_store
                .get(right_child_shard, &key_after_right_child.to_vec())
                .is_ok_and(|val| val.is_some())
        );
    }

    /// Test to validate that split shard resharding works if the chain undergoes a fork across the
    /// resharding boundary. In this specific test we want to assert that the whole operation
    /// succeed even if the older fork (chronologically) ends up becoming part of the canonical
    /// chain.
    /// The complementary test to this is `resharding_event_not_started_can_be_replaced`, which tests
    /// forks where the newest path is finalized.
    ///
    /// Timeline:
    /// ------- Fork 1 -------> canonical chain
    ///    |
    ///     -------- Fork 2 --> discarded
    #[test]
    fn split_shard_in_forks_when_older_branch_is_finalized() {
        init_test_logger();
        let (mut chain, resharder, sender) =
            create_chain_resharder_sender::<DelayedSender>(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let flat_store = resharder.runtime.store().flat_store();

        // Add two blocks on top of genesis.
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::ChainHead,
            NextBlockHeight::ChainHeadPlusOne,
        );

        // Trigger resharding at block 2. Parent shard shouldn't get split yet.
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams { parent_shard, .. } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        assert_eq!(sender.call_split_shard_task(), FlatStorageReshardingTaskResult::Postponed);
        assert_gt!(flat_store.iter(parent_shard).count(), 0);

        // Add two blocks on top of the first block (simulate a fork).
        add_blocks_to_chain(
            &mut chain,
            2,
            PreviousBlockHeight::Fixed(1),
            NextBlockHeight::Fixed(3),
        );

        // Get the new resharding event and re-trigger the shard split.
        let resharding_event_type = event_type_from_chain_and_layout(&chain, &new_shard_layout);
        let ReshardingSplitShardParams { parent_shard, .. } = match resharding_event_type.clone() {
            ReshardingEventType::SplitShard(params) => params,
        };
        assert!(resharder.start_resharding(resharding_event_type, &new_shard_layout).is_ok());
        assert_eq!(sender.call_split_shard_task(), FlatStorageReshardingTaskResult::Postponed);
        assert_gt!(flat_store.iter(parent_shard).count(), 0);

        // Add three additional blocks on the fork to make the resharding block (height 2) final.
        add_blocks_to_chain(
            &mut chain,
            3,
            PreviousBlockHeight::Fixed(2),
            NextBlockHeight::Fixed(5),
        );

        // Now the second resharding event should take place.
        assert_matches!(
            sender.call_split_shard_task(),
            FlatStorageReshardingTaskResult::Successful { .. }
        );

        assert_eq!(flat_store.iter(parent_shard).count(), 0);
    }
}
