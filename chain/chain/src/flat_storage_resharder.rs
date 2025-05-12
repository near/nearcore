//! Logic for resharding flat storage in parallel to chain processing.
//!
//! See [FlatStorageResharder] for more details about how the resharding takes place.

use std::fmt::{Debug, Formatter};
use std::iter;
use std::num::NonZero;
use std::sync::Arc;

use crate::resharding::event_type::ReshardingSplitShardParams;
use crate::types::RuntimeAdapter;
use itertools::Itertools;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Tip;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::col::{self};
use near_primitives::trie_key::trie_key_parsers::{
    parse_account_id_from_access_key_key, parse_account_id_from_account_key,
    parse_account_id_from_contract_code_key, parse_account_id_from_contract_data_key,
    parse_account_id_from_received_data_key, parse_account_id_from_trie_key_with_separator,
};
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::flat_store::{FlatStoreAdapter, FlatStoreUpdateAdapter};
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::flat::{
    BlockInfo, FlatStateChanges, FlatStorageError, FlatStorageReadyStatus,
    FlatStorageReshardingShardCatchUpMetrics, FlatStorageReshardingShardSplitMetrics,
    FlatStorageReshardingStatus, FlatStorageStatus, ParentSplitParameters,
};
use near_store::{ShardUId, StorageError};
use tracing::{debug, error, info, warn};

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
///   [ReshardingHandle].
///     - In the case of event `Split` the state of flat storage will go back to what it was
///       previously.
///     - Children shard catchup can be cancelled and will resume from the point where it left.
#[derive(Clone)]
pub struct FlatStorageResharder {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    /// Controls cancellation of background processing.
    pub handle: ReshardingHandle,
    /// Configuration for resharding.
    resharding_config: MutableConfigValue<ReshardingConfig>,
}

impl FlatStorageResharder {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        controller: ReshardingHandle,
        resharding_config: MutableConfigValue<ReshardingConfig>,
    ) -> Self {
        Self { epoch_manager, runtime, handle: controller, resharding_config }
    }

    /// Main function to start resharding. This function is a long running cancellable task.
    /// This is called from the resharding actor and is blocking till the resharding of flat storage
    /// is completed.
    pub fn start_resharding(&self, event: &ReshardingSplitShardParams) -> Result<(), Error> {
        let status = self.runtime.store().flat_store().get_flat_storage_status(event.parent_shard);
        let Ok(FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head })) = status else {
            error!(target: "resharding", ?event, "flat storage shard split task: parent shard is not ready");
            panic!("impossible to recover from a flat storage split shard failure!");
        };

        let next_epoch_id =
            self.epoch_manager.get_next_epoch_id_from_prev_block(&event.resharding_block.hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&next_epoch_id)?;

        let split_params = ParentSplitParameters {
            left_child_shard: event.left_child_shard,
            right_child_shard: event.right_child_shard,
            shard_layout,
            resharding_blocks: vec![event.resharding_block],
            flat_head,
        };
        self.start_resharding_impl(event.parent_shard, split_params);

        Ok(())
    }

    fn start_resharding_impl(&self, parent_shard: ShardUId, split_params: ParentSplitParameters) {
        let left_child_shard = split_params.left_child_shard;
        let right_child_shard = split_params.right_child_shard;
        match self.split_shard_task(parent_shard, split_params) {
            // All good.
            FlatStorageReshardingTaskResult::Successful { .. } => {}
            FlatStorageReshardingTaskResult::Failed => {
                panic!("impossible to recover from a flat storage split shard failure!")
            }
            // The task has been cancelled. Nothing else to do.
            FlatStorageReshardingTaskResult::Cancelled => {
                return;
            }
        }

        for child_shard in [left_child_shard, right_child_shard] {
            match self.shard_catchup_task(child_shard) {
                // All good.
                FlatStorageReshardingTaskResult::Successful { .. } => {}
                FlatStorageReshardingTaskResult::Failed => {
                    panic!("impossible to recover from a flat storage shard catchup failure!")
                }
                // The task has been cancelled. Nothing else to do.
                FlatStorageReshardingTaskResult::Cancelled => {}
            }
        }
    }

    /// Resumes a resharding event that was interrupted.
    ///
    /// Flat-storage resharding will resume upon a node crash.
    ///
    /// # Args:
    /// * `shard_uid`: UId of the shard
    pub fn resume(&self, shard_uid: ShardUId) -> Result<(), Error> {
        let resharding_status =
            match self.runtime.get_flat_storage_manager().get_flat_storage_status(shard_uid) {
                FlatStorageStatus::Disabled
                | FlatStorageStatus::Empty
                | FlatStorageStatus::Creation(_)
                | FlatStorageStatus::Ready(_) => return Ok(()),
                // We only need to resume resharding if the status is `Resharding`.
                FlatStorageStatus::Resharding(status) => status,
            };

        match resharding_status {
            FlatStorageReshardingStatus::CreatingChild => {
                // Nothing to do here because the parent will take care of resuming work.
            }
            FlatStorageReshardingStatus::SplittingParent(status) => {
                let parent_shard_uid = shard_uid;
                info!(target: "resharding", ?parent_shard_uid, ?status, "resuming flat storage shard split");
                // On resume, flat storage status is already set correctly and read from DB.
                // Thus, we don't need to care about cancelling other existing resharding events.
                // However, we don't know the current state of children shards,
                // so it's better to clean them.
                self.clean_children_shards(&status)?;
                self.start_resharding_impl(parent_shard_uid, status);
            }
            FlatStorageReshardingStatus::CatchingUp(_) => {
                info!(target: "resharding", ?shard_uid, ?resharding_status, "resuming flat storage shard catchup");
                match self.shard_catchup_task(shard_uid) {
                    // All good.
                    FlatStorageReshardingTaskResult::Successful { .. } => {}
                    FlatStorageReshardingTaskResult::Failed => {
                        panic!("impossible to recover from a flat storage shard catchup failure!")
                    }
                    // The task has been cancelled. Nothing else to do.
                    FlatStorageReshardingTaskResult::Cancelled => {}
                }
            }
        }
        Ok(())
    }

    /// The preprocessing step sets the appropriate flat storage status for the parent and children shards.
    fn split_shard_task_preprocessing(
        &self,
        parent_shard: ShardUId,
        split_params: &ParentSplitParameters,
        metrics: &FlatStorageReshardingShardSplitMetrics,
    ) {
        // Change parent and children shards flat storage status.
        let mut store_update = self.runtime.store().flat_store().store_update();
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
            split_params.left_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_update.set_flat_storage_status(
            split_params.right_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_update.commit().unwrap();

        metrics.update_shards_status(&self.runtime.get_flat_storage_manager());
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
    /// child. This task may get cancelled.
    fn split_shard_task(
        &self,
        parent_shard: ShardUId,
        split_params: ParentSplitParameters,
    ) -> FlatStorageReshardingTaskResult {
        info!(target: "resharding", "flat storage shard split task execution");

        let metrics = FlatStorageReshardingShardSplitMetrics::new(
            parent_shard,
            split_params.left_child_shard,
            split_params.right_child_shard,
        );

        self.split_shard_task_preprocessing(parent_shard, &split_params, &metrics);

        let task_status = self.split_shard_task_impl(parent_shard, &split_params, &metrics);
        self.split_shard_task_postprocessing(parent_shard, split_params, &metrics, task_status);
        info!(target: "resharding", ?task_status, "flat storage shard split task finished");
        task_status
    }

    /// Performs the bulk of [split_shard_task]. This method splits the flat storage of the parent shard.
    ///
    /// Returns `true` if the routine completed successfully.
    fn split_shard_task_impl(
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
        let batch_delay = self.resharding_config.get().batch_delay.unsigned_abs();

        info!(target: "resharding", ?parent_shard, ?split_params, ?batch_delay, ?batch_size, "flat storage shard split task: starting key-values copy");

        assert!(split_params.resharding_blocks.len() == 1);
        let resharding_block = split_params.resharding_blocks[0];

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
            if self.handle.is_cancelled() {
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
        metrics: &FlatStorageReshardingShardSplitMetrics,
        task_status: FlatStorageReshardingTaskResult,
    ) {
        info!(target: "resharding", ?parent_shard, ?task_status, ?split_params, "flat storage shard split task: post-processing");

        assert!(split_params.resharding_blocks.len() == 1);
        let resharding_block = split_params.resharding_blocks[0];
        let flat_head = split_params.flat_head;

        let flat_store = self.runtime.store().flat_store();
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
                for child_shard in [split_params.left_child_shard, split_params.right_child_shard] {
                    store_update.set_flat_storage_status(
                        child_shard,
                        FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(
                            resharding_block,
                        )),
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
                for child_shard in [split_params.left_child_shard, split_params.right_child_shard] {
                    store_update.remove_flat_storage(child_shard);
                }
            }
            FlatStorageReshardingTaskResult::Cancelled => {
                // Remove children shards leftovers, but keep intact their current status and deltas
                // plus the current status of the parent, so resharding can resume later.
                for child_shard in [split_params.left_child_shard, split_params.right_child_shard] {
                    store_update.remove_all_values(child_shard);
                }
            }
        }
        store_update.commit().unwrap();
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
    fn shard_catchup_task(&self, shard_uid: ShardUId) -> FlatStorageReshardingTaskResult {
        // Exit early if the task has already been cancelled.
        if self.handle.is_cancelled() {
            return FlatStorageReshardingTaskResult::Cancelled;
        }
        info!(target: "resharding", ?shard_uid, "flat storage shard catchup task started");
        let metrics = FlatStorageReshardingShardCatchUpMetrics::new(&shard_uid);
        // Apply deltas and then create the flat storage.
        let (num_batches_done, flat_head) = match self
            .shard_catchup_apply_deltas(shard_uid, &metrics)
        {
            Ok(ShardCatchupApplyDeltasOutcome::Succeeded(num_batches_done, tip)) => {
                (num_batches_done, tip)
            }
            Ok(ShardCatchupApplyDeltasOutcome::Cancelled) => {
                return FlatStorageReshardingTaskResult::Cancelled;
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

        let chain_store = self.runtime.store().chain_store();
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

            if self.handle.is_cancelled() {
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
        f.debug_struct("FlatStorageResharder").field("controller", &self.handle).finish()
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

/// Result of a scheduled flat storage resharding task.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
enum FlatStorageReshardingTaskResult {
    Successful { num_batches_done: usize },
    Failed,
    Cancelled,
}

/// Outcome of the task that applies deltas during shard catchup.
enum ShardCatchupApplyDeltasOutcome {
    /// Contains the number of delta batches applied and the final tip of the flat storage.
    Succeeded(usize, Tip),
    Cancelled,
}
