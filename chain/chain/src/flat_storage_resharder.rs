//! Logic for resharding flat storage in parallel to chain processing.
//!
//! See [FlatStorageResharder] for more details about how the resharding takes place.

use std::sync::{Arc, Mutex};

use crossbeam_channel::{Receiver, Sender};
use near_chain_configs::ReshardingHandle;
use near_chain_primitives::Error;

use tracing::{debug, error, info, warn};

use crate::resharding::event_type::{ReshardingEventType, ReshardingSplitShardParams};
use crate::types::RuntimeAdapter;
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::col::{self, ALL_COLUMNS_WITH_NAMES};
use near_primitives::trie_key::trie_key_parsers::{
    parse_account_id_from_access_key_key, parse_account_id_from_account_key,
    parse_account_id_from_contract_code_key, parse_account_id_from_contract_data_key,
    parse_account_id_from_received_data_key, parse_account_id_from_trie_key_with_separator,
};
use near_primitives::types::AccountId;
use near_store::adapter::flat_store::{FlatStoreAdapter, FlatStoreUpdateAdapter};
use near_store::adapter::StoreAdapter;
use near_store::flat::{
    BlockInfo, FlatStorageReadyStatus, FlatStorageReshardingStatus, FlatStorageStatus,
    SplittingParentStatus,
};
use near_store::{ShardUId, StorageError};

/// `FlatStorageResharder` takes care of updating flat storage when a resharding event happens.
///
/// On an high level, the events supported are:
/// - #### Shard splitting
///     Parent shard must be split into two children. The entire operation freezes the flat storage
///     for the involved shards. Children shards are created empty and the key-values of the parent
///     will be copied into one of them, in the background.
///
///     After the copy is finished the children shard will have the correct state at some past block
///     height. It'll be necessary to perform catchup before the flat storage can be put again in
///     Ready state. The parent shard storage is not needed anymore and can be removed.
///
/// The resharder has also the following properties:
/// - Background processing: the bulk of resharding is done in a separate task, see
///   [FlatStorageResharderScheduler]
/// - Interruptible: a reshard operation can be cancelled through a
///   [FlatStorageResharderController].
///     - In the case of event `Split` the state of flat storage will go back to what it was
///       previously.
#[derive(Clone)]
pub struct FlatStorageResharder {
    runtime: Arc<dyn RuntimeAdapter>,
    resharding_event: Arc<Mutex<Option<FlatStorageReshardingEventStatus>>>,
}

impl FlatStorageResharder {
    /// Creates a new `FlatStorageResharder`.
    pub fn new(runtime: Arc<dyn RuntimeAdapter>) -> Self {
        let resharding_event = Arc::new(Mutex::new(None));
        Self { runtime, resharding_event }
    }

    /// Starts a resharding event.
    ///
    /// For now, only splitting a shard is supported.
    ///
    /// # Args:
    /// * `event_type`: the type of resharding event
    /// * `shard_layout`: the new shard layout
    /// * `scheduler`: component used to schedule the background tasks
    /// * `controller`: manages the execution of the background tasks
    pub fn start_resharding(
        &self,
        event_type: ReshardingEventType,
        shard_layout: &ShardLayout,
        scheduler: &dyn FlatStorageResharderScheduler,
        controller: FlatStorageResharderController,
    ) -> Result<(), Error> {
        match event_type {
            ReshardingEventType::SplitShard(params) => {
                self.split_shard(params, shard_layout, scheduler, controller)
            }
        }
    }

    /// Resumes a resharding event that was interrupted.
    ///
    /// Flat-storage resharding will resume upon a node crash.
    ///
    /// # Args:
    /// * `shard_uid`: UId of the shard
    /// * `status`: resharding status of the shard
    /// * `scheduler`: component used to schedule the background tasks
    /// * `controller`: manages the execution of the background tasks
    pub fn resume(
        &self,
        shard_uid: ShardUId,
        status: &FlatStorageReshardingStatus,
        scheduler: &dyn FlatStorageResharderScheduler,
        controller: FlatStorageResharderController,
    ) -> Result<(), Error> {
        match status {
            FlatStorageReshardingStatus::CreatingChild => {
                // Nothing to do here because the parent will take care of resuming work.
            }
            FlatStorageReshardingStatus::SplittingParent(status) => {
                let parent_shard_uid = shard_uid;
                info!(target: "resharding", ?parent_shard_uid, ?status, "resuming flat storage shard split");
                self.check_no_resharding_in_progress()?;
                // On resume flat storage status is already set.
                // However, we don't know the current state of children shards,
                // so it's better to clean them.
                self.clean_children_shards(&status)?;
                self.schedule_split_shard(parent_shard_uid, &status, scheduler, controller);
            }
            FlatStorageReshardingStatus::CatchingUp(_) => {
                info!(target: "resharding", ?shard_uid, ?status, "resuming flat storage shard catchup");
                // TODO(Trisfald): implement child catch up
                todo!()
            }
        }
        Ok(())
    }

    /// Starts the event of splitting a parent shard flat storage into two children.
    fn split_shard(
        &self,
        split_params: ReshardingSplitShardParams,
        shard_layout: &ShardLayout,
        scheduler: &dyn FlatStorageResharderScheduler,
        controller: FlatStorageResharderController,
    ) -> Result<(), Error> {
        let ReshardingSplitShardParams {
            parent_shard,
            left_child_shard,
            right_child_shard,
            block_hash,
            prev_block_hash,
            ..
        } = split_params;
        info!(target: "resharding", ?split_params, "initiating flat storage shard split");
        self.check_no_resharding_in_progress()?;

        // Change parent and children shards flat storage status.
        let store = self.runtime.store().flat_store();
        let mut store_update = store.store_update();
        let flat_head = retrieve_shard_flat_head(parent_shard, &store)?;
        let status = SplittingParentStatus {
            left_child_shard,
            right_child_shard,
            shard_layout: shard_layout.clone(),
            block_hash,
            prev_block_hash,
            flat_head,
        };
        store_update.set_flat_storage_status(
            parent_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::SplittingParent(
                status.clone(),
            )),
        );
        store_update.set_flat_storage_status(
            left_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_update.set_flat_storage_status(
            right_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_update.commit()?;

        self.schedule_split_shard(parent_shard, &status, scheduler, controller);
        Ok(())
    }

    /// Returns an error if a resharding event is in progress.
    fn check_no_resharding_in_progress(&self) -> Result<(), StorageError> {
        // Do not allow multiple resharding events in parallel.
        if self.resharding_event().is_some() {
            error!(target: "resharding", "trying to start a new flat storage resharding event while one is already in progress!");
            Err(StorageError::FlatStorageReshardingAlreadyInProgress)
        } else {
            Ok(())
        }
    }

    fn set_resharding_event(&self, event: FlatStorageReshardingEventStatus) {
        *self.resharding_event.lock().unwrap() = Some(event);
    }

    /// Returns the current in-progress resharding event, if any.
    pub fn resharding_event(&self) -> Option<FlatStorageReshardingEventStatus> {
        self.resharding_event.lock().unwrap().clone()
    }

    /// Schedules a task to split a shard.
    fn schedule_split_shard(
        &self,
        parent_shard: ShardUId,
        status: &SplittingParentStatus,
        scheduler: &dyn FlatStorageResharderScheduler,
        controller: FlatStorageResharderController,
    ) {
        let event = FlatStorageReshardingEventStatus::SplitShard(parent_shard, status.clone());
        self.set_resharding_event(event);
        info!(target: "resharding", ?parent_shard, ?status,"scheduling flat storage shard split");

        let resharder = self.clone();
        let task = Box::new(move || split_shard_task(resharder, controller));
        scheduler.schedule(task);
    }

    /// Cleans up children shards flat storage's content (status is excluded).
    fn clean_children_shards(&self, status: &SplittingParentStatus) -> Result<(), Error> {
        let SplittingParentStatus { left_child_shard, right_child_shard, .. } = status;
        debug!(target: "resharding", ?left_child_shard, ?right_child_shard, "cleaning up children shards flat storage's content");
        let mut store_update = self.runtime.store().flat_store().store_update();
        for child in [left_child_shard, right_child_shard] {
            store_update.remove_all_deltas(*child);
            store_update.remove_all_values(*child);
        }
        store_update.commit()?;
        Ok(())
    }

    /// Retrieves parent shard UIds and current resharding event status, only if a resharding event
    /// is in progress and of type `Split`.
    fn get_parent_shard_and_status(&self) -> Option<(ShardUId, SplittingParentStatus)> {
        let event = self.resharding_event.lock().unwrap();
        match event.as_ref() {
            Some(FlatStorageReshardingEventStatus::SplitShard(parent_shard, status)) => {
                Some((*parent_shard, status.clone()))
            }
            None => None,
        }
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

/// Task to perform the actual split of a flat storage shard. This may be a long operation time-wise.
///
/// Conceptually it simply copies each key-value pair from the parent shard to the correct child.
fn split_shard_task(resharder: FlatStorageResharder, controller: FlatStorageResharderController) {
    let task_status = split_shard_task_impl(resharder.clone(), controller.clone());
    split_shard_task_postprocessing(resharder, task_status);
    info!(target: "resharding", ?task_status, "flat storage shard split task finished");
    if let Err(err) = controller.completion_sender.send(task_status) {
        warn!(target: "resharding", ?err, "error notifying completion of flat storage shard split task")
    };
}

/// Performs the bulk of [split_shard_task].
///
/// Returns `true` if the routine completed successfully.
fn split_shard_task_impl(
    resharder: FlatStorageResharder,
    controller: FlatStorageResharderController,
) -> FlatStorageReshardingTaskStatus {
    if controller.is_interrupted() {
        return FlatStorageReshardingTaskStatus::Cancelled;
    }

    /// Determines after how many key-values the process stops to
    /// commit changes and to check interruptions.
    const BATCH_SIZE: usize = 10_000;

    let (parent_shard, status) = resharder
        .get_parent_shard_and_status()
        .expect("flat storage resharding event must be Split!");
    info!(target: "resharding", ?parent_shard, ?status, "flat storage shard split task: starting key-values copy");

    // Parent shard flat storage head must be on block height just before the new shard layout kicks
    // in. This guarantees that all deltas have been applied and thus the state of all key-values is
    // up to date.
    // TODO(trisfald): do this check, maybe call update_flat_storage_for_shard
    let _parent_flat_head = status.flat_head;

    // Prepare the store object for commits and the iterator over parent's flat storage.
    let flat_store = resharder.runtime.store().flat_store();
    let mut iter = flat_store.iter(parent_shard);

    loop {
        let mut store_update = flat_store.store_update();

        // Process a `BATCH_SIZE` worth of key value pairs.
        let mut iter_exhausted = false;
        for _ in 0..BATCH_SIZE {
            match iter.next() {
                Some(Ok((key, value))) => {
                    if let Err(err) =
                        shard_split_handle_key_value(key, value, &mut store_update, &status)
                    {
                        error!(target: "resharding", ?err, "failed to handle flat storage key");
                        return FlatStorageReshardingTaskStatus::Failed;
                    }
                }
                Some(Err(err)) => {
                    error!(target: "resharding", ?err, "failed to read flat storage value from parent shard");
                    return FlatStorageReshardingTaskStatus::Failed;
                }
                None => {
                    iter_exhausted = true;
                }
            }
        }

        // Make a pause to commit and check if the routine should stop.
        if let Err(err) = store_update.commit() {
            error!(target: "resharding", ?err, "failed to commit store update");
            return FlatStorageReshardingTaskStatus::Failed;
        }

        // TODO(Trisfald): metrics and logs

        // If `iter`` is exhausted we can exit after the store commit.
        if iter_exhausted {
            break;
        }
        if controller.is_interrupted() {
            return FlatStorageReshardingTaskStatus::Cancelled;
        }
    }
    FlatStorageReshardingTaskStatus::Successful
}

/// Handles the inheritance of a key-value pair from parent shard to children shards.
fn shard_split_handle_key_value(
    key: Vec<u8>,
    value: FlatStateValue,
    store_update: &mut FlatStoreUpdateAdapter,
    status: &SplittingParentStatus,
) -> Result<(), Error> {
    if key.is_empty() {
        panic!("flat storage key is empty!")
    }
    let key_column_prefix = key[0];

    match key_column_prefix {
        col::ACCOUNT => {
            copy_kv_to_child(&status, key, value, store_update, parse_account_id_from_account_key)?
        }
        col::CONTRACT_DATA => copy_kv_to_child(
            &status,
            key,
            value,
            store_update,
            parse_account_id_from_contract_data_key,
        )?,
        col::CONTRACT_CODE => copy_kv_to_child(
            &status,
            key,
            value,
            store_update,
            parse_account_id_from_contract_code_key,
        )?,
        col::ACCESS_KEY => copy_kv_to_child(
            &status,
            key,
            value,
            store_update,
            parse_account_id_from_access_key_key,
        )?,
        col::RECEIVED_DATA => copy_kv_to_child(
            &status,
            key,
            value,
            store_update,
            parse_account_id_from_received_data_key,
        )?,
        col::POSTPONED_RECEIPT_ID | col::PENDING_DATA_COUNT | col::POSTPONED_RECEIPT => {
            copy_kv_to_child(&status, key, value, store_update, |raw_key: &[u8]| {
                parse_account_id_from_trie_key_with_separator(
                    key_column_prefix,
                    raw_key,
                    ALL_COLUMNS_WITH_NAMES[key_column_prefix as usize].1,
                )
            })?
        }
        col::DELAYED_RECEIPT_OR_INDICES
        | col::PROMISE_YIELD_INDICES
        | col::PROMISE_YIELD_TIMEOUT
        | col::PROMISE_YIELD_RECEIPT
        | col::BUFFERED_RECEIPT_INDICES
        | col::BUFFERED_RECEIPT => {
            // TODO(trisfald): implement logic and remove error log
            let col_name = ALL_COLUMNS_WITH_NAMES[key_column_prefix as usize].1;
            error!(target: "resharding", "flat storage resharding of {col_name} is not implemented yet!");
        }
        _ => unreachable!(),
    }
    Ok(())
}

/// Performs post-processing of shard splitting after all key-values have been moved from parent to
/// children. `success` indicates whether or not the previous phase was successful.
fn split_shard_task_postprocessing(
    resharder: FlatStorageResharder,
    task_status: FlatStorageReshardingTaskStatus,
) {
    let (parent_shard, split_status) = resharder
        .get_parent_shard_and_status()
        .expect("flat storage resharding event must be Split!");
    let SplittingParentStatus { left_child_shard, right_child_shard, flat_head, .. } = split_status;
    let flat_store = resharder.runtime.store().flat_store();
    info!(target: "resharding", ?parent_shard, ?task_status, ?split_status, "flat storage shard split task: post-processing");

    let mut store_update = flat_store.store_update();
    match task_status {
        FlatStorageReshardingTaskStatus::Successful => {
            // Split shard completed successfully.
            // Parent flat storage can be deleted from the FlatStoreManager.
            resharder
                .runtime
                .get_flat_storage_manager()
                .remove_flat_storage_for_shard(parent_shard, &mut store_update)
                .unwrap();
            store_update.remove_flat_storage(parent_shard);
            // Children must perform catchup.
            for child_shard in [left_child_shard, right_child_shard] {
                store_update.set_flat_storage_status(
                    child_shard,
                    FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(
                        flat_head.hash,
                    )),
                );
            }
            // TODO(trisfald): trigger catchup
        }
        FlatStorageReshardingTaskStatus::Failed | FlatStorageReshardingTaskStatus::Cancelled => {
            // We got an error or an interrupt request.
            // Reset parent.
            store_update.set_flat_storage_status(
                parent_shard,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }),
            );
            // Remove children shards leftovers.
            for child_shard in [left_child_shard, right_child_shard] {
                store_update.remove_flat_storage(child_shard);
            }
        }
    }
    store_update.commit().unwrap();
    // Terminate the resharding event.
    *resharder.resharding_event.lock().unwrap() = None;
}

/// Copies a key-value pair to the correct child shard by matching the account-id to the provided shard layout.
fn copy_kv_to_child(
    status: &SplittingParentStatus,
    key: Vec<u8>,
    value: FlatStateValue,
    store_update: &mut FlatStoreUpdateAdapter,
    account_id_parser: impl FnOnce(&[u8]) -> Result<AccountId, std::io::Error>,
) -> Result<(), Error> {
    let SplittingParentStatus { left_child_shard, right_child_shard, shard_layout, .. } = &status;
    // Derive the shard uid for this account in the new shard layout.
    let account_id = account_id_parser(&key)?;
    let new_shard_id = account_id_to_shard_id(&account_id, shard_layout);
    let new_shard_uid = ShardUId::from_shard_id_and_layout(new_shard_id, &shard_layout);

    // Sanity check we are truly writing to one of the expected children shards.
    if new_shard_uid != *left_child_shard && new_shard_uid != *right_child_shard {
        let err_msg = "account id doesn't map to any child shard!";
        error!(target: "resharding", ?new_shard_uid, ?left_child_shard, ?right_child_shard, ?shard_layout, ?account_id, err_msg);
        return Err(Error::ReshardingError(err_msg.to_string()));
    }
    // Add the new flat store entry.
    store_update.set(new_shard_uid, key, Some(value));
    Ok(())
}

/// Struct to describe, perform and track progress of a flat storage resharding.
#[derive(Clone, Debug)]
pub enum FlatStorageReshardingEventStatus {
    /// Split a shard.
    /// Includes the parent shard uid and the operation' status.
    SplitShard(ShardUId, SplittingParentStatus),
}

/// Status of a flat storage resharding task.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum FlatStorageReshardingTaskStatus {
    Successful,
    Failed,
    Cancelled,
}

/// Helps control the flat storage resharder operation. More specifically,
/// it has a way to know when the background task is done or to interrupt it.
#[derive(Clone)]
pub struct FlatStorageResharderController {
    /// Resharding handle to control interruption.
    handle: ReshardingHandle,
    /// This object will be used to signal when the background task is completed.
    completion_sender: Sender<FlatStorageReshardingTaskStatus>,
    /// Corresponding receiver for `completion_sender`.
    pub completion_receiver: Receiver<FlatStorageReshardingTaskStatus>,
}

impl FlatStorageResharderController {
    /// Creates a new `FlatStorageResharderController` with its own handle.
    pub fn new() -> Self {
        let (completion_sender, completion_receiver) = crossbeam_channel::bounded(1);
        let handle = ReshardingHandle::new();
        Self { handle, completion_sender, completion_receiver }
    }

    pub fn from_resharding_handle(handle: ReshardingHandle) -> Self {
        let (completion_sender, completion_receiver) = crossbeam_channel::bounded(1);
        Self { handle, completion_sender, completion_receiver }
    }

    pub fn handle(&self) -> &ReshardingHandle {
        &self.handle
    }

    /// Returns whether or not background task is interrupted.
    pub fn is_interrupted(&self) -> bool {
        !self.handle.get()
    }
}

/// Represent the capability of scheduling the background tasks spawned by flat storage resharding.
pub trait FlatStorageResharderScheduler {
    fn schedule(&self, f: Box<dyn FnOnce()>);
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap, time::Duration};

    use near_async::time::Clock;
    use near_chain_configs::{Genesis, MutableConfigValue};
    use near_epoch_manager::{shard_tracker::ShardTracker, EpochManager};
    use near_o11y::testonly::init_test_logger;
    use near_primitives::{
        hash::CryptoHash, shard_layout::ShardLayout, state::FlatStateValue, trie_key::TrieKey,
        types::AccountId,
    };
    use near_store::{
        flat::{BlockInfo, FlatStorageReadyStatus},
        genesis::initialize_genesis_state,
        test_utils::create_test_store,
    };

    use crate::{
        rayon_spawner::RayonAsyncComputationSpawner, runtime::NightshadeRuntime,
        types::ChainConfig, Chain, ChainGenesis, DoomslugThresholdMode,
    };

    use super::*;

    /// Shorthand to create account ID.
    macro_rules! account {
        ($str:expr) => {
            $str.parse::<AccountId>().unwrap()
        };
    }

    struct TestScheduler {}

    impl FlatStorageResharderScheduler for TestScheduler {
        fn schedule(&self, f: Box<dyn FnOnce()>) {
            f();
        }
    }

    #[derive(Default)]
    struct DelayedScheduler {
        callable: RefCell<Option<Box<dyn FnOnce()>>>,
    }

    impl DelayedScheduler {
        fn call(&self) {
            self.callable.take().unwrap()();
        }
    }

    impl FlatStorageResharderScheduler for DelayedScheduler {
        fn schedule(&self, f: Box<dyn FnOnce()>) {
            *self.callable.borrow_mut() = Some(f);
        }
    }

    /// Simple shard layout with two shards.
    fn simple_shard_layout() -> ShardLayout {
        let shards_split_map = BTreeMap::from([(0, vec![0]), (1, vec![1])]);
        ShardLayout::v2(vec![account!("ff")], vec![0, 1], Some(shards_split_map))
    }

    /// Derived from [simple_shard_layout] by splitting the second shard.
    fn shard_layout_after_split() -> ShardLayout {
        let shards_split_map = BTreeMap::from([(0, vec![0]), (1, vec![2, 3])]);
        ShardLayout::v2(vec![account!("ff"), account!("pp")], vec![0, 2, 3], Some(shards_split_map))
    }

    /// Generic test setup.
    fn create_fs_resharder(shard_layout: ShardLayout) -> (Chain, FlatStorageResharder) {
        let num_shards = shard_layout.shard_ids().count();
        let genesis = Genesis::test_with_seeds(
            Clock::real(),
            vec![account!("aa"), account!("mm"), account!("vv")],
            1,
            vec![1; num_shards],
            shard_layout,
        );
        let tempdir = tempfile::tempdir().unwrap();
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
        let runtime =
            NightshadeRuntime::test(tempdir.path(), store, &genesis.config, epoch_manager.clone());
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let chain = Chain::new(
            Clock::real(),
            epoch_manager,
            shard_tracker,
            runtime.clone(),
            &chain_genesis,
            DoomslugThresholdMode::NoApprovals,
            ChainConfig::test(),
            None,
            Arc::new(RayonAsyncComputationSpawner),
            MutableConfigValue::new(None, "validator_signer"),
        )
        .unwrap();
        (chain, FlatStorageResharder::new(runtime))
    }

    /// Verify that another resharding can't be triggered if one is ongoing.
    #[test]
    fn concurrent_reshardings_are_disallowed() {
        init_test_logger();
        let (chain, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = DelayedScheduler::default();
        let controller = FlatStorageResharderController::new();
        let resharding_event_type = ReshardingEventType::from_shard_layout(
            &new_shard_layout,
            chain.head().unwrap().last_block_hash,
            chain.head().unwrap().prev_block_hash,
        )
        .unwrap()
        .unwrap();

        assert!(resharder
            .start_resharding(
                resharding_event_type.clone(),
                &new_shard_layout,
                &scheduler,
                controller.clone()
            )
            .is_ok());

        // Immediately interrupt the resharding.
        controller.handle().stop();

        assert!(resharder.resharding_event().is_some());
        assert!(resharder
            .start_resharding(resharding_event_type, &new_shard_layout, &scheduler, controller)
            .is_err());
    }

    /// Flat storage shard status should be set correctly upon starting a shard split.
    #[test]
    fn flat_storage_split_status_set() {
        init_test_logger();
        let (chain, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = DelayedScheduler::default();
        let controller = FlatStorageResharderController::new();
        let flat_store = resharder.runtime.store().flat_store();
        let resharding_event_type = ReshardingEventType::from_shard_layout(
            &new_shard_layout,
            chain.head().unwrap().last_block_hash,
            chain.head().unwrap().prev_block_hash,
        )
        .unwrap()
        .unwrap();

        assert!(resharder
            .start_resharding(resharding_event_type, &new_shard_layout, &scheduler, controller)
            .is_ok());

        let resharding_event = resharder.resharding_event();
        match resharding_event.unwrap() {
            FlatStorageReshardingEventStatus::SplitShard(parent, status) => {
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
            }
        }
    }

    /// In this test we write some dirty state into children shards and then try to resume a shard split.
    /// Verify that the dirty writes are cleaned up correctly.
    #[test]
    fn resume_split_starts_from_clean_state() {
        init_test_logger();
        let (chain, resharder) = create_fs_resharder(simple_shard_layout());
        let flat_store = resharder.runtime.store().flat_store();
        let new_shard_layout = shard_layout_after_split();
        let resharding_event_type = ReshardingEventType::from_shard_layout(
            &new_shard_layout,
            chain.head().unwrap().last_block_hash,
            chain.head().unwrap().prev_block_hash,
        )
        .unwrap()
        .unwrap();
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
            FlatStorageReshardingStatus::SplittingParent(SplittingParentStatus {
                // Values don't matter.
                left_child_shard,
                right_child_shard,
                shard_layout: new_shard_layout,
                block_hash: CryptoHash::default(),
                prev_block_hash: CryptoHash::default(),
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
        let scheduler = TestScheduler {};
        let controller = FlatStorageResharderController::new();
        resharder.resume(parent_shard, &resharding_status, &scheduler, controller).unwrap();

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
        // Perform resharding.
        let (chain, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = TestScheduler {};
        let controller = FlatStorageResharderController::new();
        let resharding_event_type = ReshardingEventType::from_shard_layout(
            &new_shard_layout,
            chain.head().unwrap().last_block_hash,
            chain.head().unwrap().prev_block_hash,
        )
        .unwrap()
        .unwrap();

        assert!(resharder
            .start_resharding(
                resharding_event_type,
                &new_shard_layout,
                &scheduler,
                controller.clone()
            )
            .is_ok());

        // Check flat storages of children contain the correct accounts.
        let left_child = ShardUId { version: 3, shard_id: 2 };
        let right_child = ShardUId { version: 3, shard_id: 3 };
        let flat_store = resharder.runtime.store().flat_store();
        let account_mm_key = TrieKey::Account { account_id: account!("mm") };
        let account_vv_key = TrieKey::Account { account_id: account!("vv") };
        assert!(flat_store
            .get(left_child, &account_mm_key.to_vec())
            .is_ok_and(|val| val.is_some()));
        assert!(flat_store
            .get(right_child, &account_vv_key.to_vec())
            .is_ok_and(|val| val.is_some()));

        // Controller should signal that resharding ended.
        assert_eq!(
            controller.completion_receiver.recv_timeout(Duration::from_secs(1)),
            Ok(FlatStorageReshardingTaskStatus::Successful)
        );

        // Check final status of parent flat storage.
        let parent = ShardUId { version: 3, shard_id: 1 };
        assert_eq!(flat_store.get_flat_storage_status(parent), Ok(FlatStorageStatus::Empty));
        assert_eq!(flat_store.iter(parent).count(), 0);
        assert!(resharder
            .runtime
            .get_flat_storage_manager()
            .get_flat_storage_for_shard(parent)
            .is_none());

        // Check final status of children flat storages.
        let last_hash = chain.head().unwrap().last_block_hash;
        assert_eq!(
            flat_store.get_flat_storage_status(left_child),
            Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(last_hash)))
        );
        assert_eq!(
            flat_store.get_flat_storage_status(left_child),
            Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(last_hash)))
        );
    }

    #[test]
    fn interrupt_split_shard() {
        init_test_logger();
        // Perform resharding.
        let (chain, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = DelayedScheduler::default();
        let controller = FlatStorageResharderController::new();
        let resharding_event_type = ReshardingEventType::from_shard_layout(
            &new_shard_layout,
            chain.head().unwrap().last_block_hash,
            chain.head().unwrap().prev_block_hash,
        )
        .unwrap()
        .unwrap();

        assert!(resharder
            .start_resharding(
                resharding_event_type,
                &new_shard_layout,
                &scheduler,
                controller.clone()
            )
            .is_ok());
        let (parent_shard, status) = resharder.get_parent_shard_and_status().unwrap();
        let SplittingParentStatus { left_child_shard, right_child_shard, flat_head, .. } = status;

        // Interrupt the task before it starts.
        controller.handle().stop();

        // Run the task.
        scheduler.call();

        // Check that resharding was effectively cancelled.
        let flat_store = resharder.runtime.store().flat_store();
        assert_eq!(
            controller.completion_receiver.recv_timeout(Duration::from_secs(1)),
            Ok(FlatStorageReshardingTaskStatus::Cancelled)
        );
        assert_eq!(
            flat_store.get_flat_storage_status(parent_shard),
            Ok(FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }))
        );
        for child_shard in [left_child_shard, right_child_shard] {
            assert_eq!(
                flat_store.get_flat_storage_status(status.left_child_shard),
                Ok(FlatStorageStatus::Empty)
            );
            assert_eq!(flat_store.iter(child_shard).count(), 0);
        }
    }

    /// A shard can't be split if it isn't in ready state.
    #[test]
    fn reject_split_shard_if_parent_is_not_ready() {
        let (chain, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = TestScheduler {};
        let controller = FlatStorageResharderController::new();
        let resharding_event_type = ReshardingEventType::from_shard_layout(
            &new_shard_layout,
            chain.head().unwrap().last_block_hash,
            chain.head().unwrap().prev_block_hash,
        )
        .unwrap()
        .unwrap();

        // Make flat storage of parent shard not ready.
        let parent_shard = ShardUId { version: 3, shard_id: 1 };
        let flat_store = resharder.runtime.store().flat_store();
        let mut store_update = flat_store.store_update();
        store_update.set_flat_storage_status(parent_shard, FlatStorageStatus::Empty);
        store_update.commit().unwrap();

        // Trigger resharding and it should fail.
        assert!(resharder
            .start_resharding(resharding_event_type, &new_shard_layout, &scheduler, controller)
            .is_err());
    }

    /// Verify that a shard can be split correctly even if its flat head is lagging behind the expected
    /// block height.
    #[test]
    fn split_shard_parent_flat_store_lagging_behind() {
        // TODO(Trisfald): implement
    }
}
