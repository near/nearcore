//! Logic for resharding flat storage in parallel to chain processing.
//!
//! See [FlatStorageResharder] for more details about how the resharding takes place.

use std::sync::{Arc, Mutex};

use crossbeam_channel::{Receiver, Sender};
use near_chain_configs::ReshardingHandle;
use near_chain_primitives::Error;

use tracing::{debug, error, info, warn};

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
use near_store::adapter::flat_store::FlatStoreUpdateAdapter;
use near_store::adapter::StoreAdapter;
use near_store::flat::{
    FlatStorageReadyStatus, FlatStorageReshardingStatus, FlatStorageStatus, SplittingParentStatus,
};
use near_store::{ShardUId, StorageError};

/// `FlatStorageResharder` takes care of updating flat storage when a resharding event
/// happens.
///
/// On an high level, the events supported are:
/// - #### Shard splitting
///     Parent shard must be split into two children. The entire operation freezes the flat storage
///     for the involved shards.
///     Children shards are created empty and the key-values of the parent will be copied into one of them,
///     in the background.
///
///     After the copy is finished the children shard will have the correct state at some past block height.
///     It'll be necessary to perform catchup before the flat storage can be put again in Ready state.
///     The parent shard storage is not needed anymore and can be removed.
///
/// The resharder has also the following properties:
/// - Background processing: the bulk of resharding is done in a separate task, see [FlatStorageResharderScheduler]
/// - Interruptible: a reshard operation can be interrupted through a [FlatStorageResharderController].
///     - In the case of event `Split` the state of flat storage will go back to what it was previously.
pub struct FlatStorageResharder {
    inner: FlatStorageResharderInner,
}

/// Inner clonable object to make sharing internal state easier.
#[derive(Clone)]
struct FlatStorageResharderInner {
    runtime: Arc<dyn RuntimeAdapter>,
    resharding_event: Arc<Mutex<Option<FlatStorageReshardingEvent>>>,
}

impl FlatStorageResharder {
    /// Creates a new `FlatStorageResharder`.
    pub fn new(runtime: Arc<dyn RuntimeAdapter>) -> Self {
        let resharding_event = Arc::new(Mutex::new(None));
        let inner = FlatStorageResharderInner { runtime, resharding_event };
        Self { inner }
    }

    /// Resumes a resharding event that was in progress.
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

    /// Starts a resharding event deduced from the new shard layout provided.
    ///
    /// For now, only splitting a shard is supported.
    ///
    /// # Args:
    /// * `shard_layout`: the new shard layout, it must contain a layout change or an error is returned
    /// * `scheduler`: component used to schedule the background tasks
    /// * `controller`: manages the execution of the background tasks
    pub fn start_resharding_from_new_shard_layout(
        &self,
        shard_layout: &ShardLayout,
        scheduler: &dyn FlatStorageResharderScheduler,
        controller: FlatStorageResharderController,
    ) -> Result<(), Error> {
        match event_type_from_shard_layout(&shard_layout)? {
            ReshardingEventType::Split(params) => {
                self.split_shard(params, shard_layout, scheduler, controller)?
            }
        };
        Ok(())
    }

    /// Starts the event of splitting a parent shard flat storage into two children.
    fn split_shard(
        &self,
        split_params: ReshardingSplitParams,
        shard_layout: &ShardLayout,
        scheduler: &dyn FlatStorageResharderScheduler,
        controller: FlatStorageResharderController,
    ) -> Result<(), Error> {
        let ReshardingSplitParams { parent_shard, left_child_shard, right_child_shard } =
            split_params;
        info!(target: "resharding", ?parent_shard, ?left_child_shard, ?right_child_shard, "initiating flat storage shard split");
        self.check_no_resharding_in_progress()?;

        // Parent shard must be in ready state.
        let store = self.inner.runtime.store().flat_store();
        let flat_head = if let FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }) =
            store
                .get_flat_storage_status(parent_shard)
                .map_err(|err| Into::<StorageError>::into(err))?
        {
            flat_head
        } else {
            let err_msg = "flat storage parent shard is not ready!";
            error!(target: "resharding", ?parent_shard, err_msg);
            return Err(Error::ReshardingError(err_msg.to_owned()));
        };

        // Change parent and children shards flat storage status.
        let mut store_update = store.store_update();
        let status = SplittingParentStatus {
            left_child_shard,
            right_child_shard,
            shard_layout: shard_layout.clone(),
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

    fn set_resharding_event(&self, event: FlatStorageReshardingEvent) {
        *self.inner.resharding_event.lock().unwrap() = Some(event);
    }

    /// Returns the current in-progress resharding event, if any.
    pub fn resharding_event(&self) -> Option<FlatStorageReshardingEvent> {
        self.inner.resharding_event.lock().unwrap().clone()
    }

    /// Schedules a task to split a shard.
    fn schedule_split_shard(
        &self,
        parent_shard: ShardUId,
        status: &SplittingParentStatus,
        scheduler: &dyn FlatStorageResharderScheduler,
        controller: FlatStorageResharderController,
    ) {
        let event = FlatStorageReshardingEvent::Split(parent_shard, status.clone());
        self.set_resharding_event(event);
        info!(target: "resharding", ?parent_shard, ?status,"scheduling flat storage shard split");

        let resharder = self.inner.clone();
        let task = Box::new(move || split_shard_task(resharder, controller));
        scheduler.schedule(task);
    }

    /// Cleans up children shards flat storage's content (status is excluded).
    fn clean_children_shards(&self, status: &SplittingParentStatus) -> Result<(), Error> {
        let SplittingParentStatus { left_child_shard, right_child_shard, .. } = status;
        debug!(target: "resharding", ?left_child_shard, ?right_child_shard, "cleaning up children shards flat storage's content");
        let mut store_update = self.inner.runtime.store().flat_store().store_update();
        for child in [left_child_shard, right_child_shard] {
            store_update.remove_all_deltas(*child);
            store_update.remove_all(*child);
        }
        store_update.commit()?;
        Ok(())
    }
}

/// Struct used to destructure a new shard layout definition into the resulting resharding event.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
enum ReshardingEventType {
    /// Split of a shard.
    Split(ReshardingSplitParams),
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
struct ReshardingSplitParams {
    // Shard being split.
    parent_shard: ShardUId,
    // Child to the left of the account boundary.
    left_child_shard: ShardUId,
    // Child to the right of the account boundary.
    right_child_shard: ShardUId,
}

/// Takes as input a [ShardLayout] definition and deduces which kind of resharding operation must be
/// performed.
///
/// Returns an error if there isn't any change in the shard layout that would require resharding.
fn event_type_from_shard_layout(shard_layout: &ShardLayout) -> Result<ReshardingEventType, Error> {
    // Resharding V3 supports shard layout V2 onwards.
    match shard_layout {
        ShardLayout::V0(_) | ShardLayout::V1(_) => {
            let err_msg = "unsupported shard layout!";
            error!(target: "resharding", ?shard_layout, err_msg);
            return Err(Error::ReshardingError(err_msg.to_owned()));
        }
        ShardLayout::V2(_) => {
            // Supported.
        }
    }

    let mut event = None;

    let error_two_reshardings = || {
        let err_msg = "can't perform two reshardings at the same time!";
        error!(target: "resharding", ?shard_layout, err_msg);
        return Err(Error::ReshardingError(err_msg.to_owned()));
    };

    for shard_id in shard_layout.shard_ids() {
        // Look for a shard having exactly two children, to detect a split.
        // - retrieve the parent shard
        // - if the parent has two children create the split event
        let parent_shard_id = shard_layout.get_parent_shard_id(shard_id)?;
        if let Some(children) = shard_layout.get_children_shards_uids(parent_shard_id) {
            if children.len() == 2 {
                match &event {
                    None => {
                        let parent_shard = ShardUId {
                            version: shard_layout.version(),
                            shard_id: parent_shard_id as u32,
                        };
                        let left_child_shard = children[0];
                        let right_child_shard = children[1];
                        event = Some(ReshardingEventType::Split(ReshardingSplitParams {
                            parent_shard,
                            left_child_shard,
                            right_child_shard,
                        }))
                    }
                    Some(ReshardingEventType::Split(split)) => {
                        // It's fine only if this shard is already a child of the existing event.
                        if split.left_child_shard.shard_id() != shard_id
                            && split.right_child_shard.shard_id() != shard_id
                        {
                            return error_two_reshardings();
                        }
                    }
                }
            }
        }
    }
    // We must have found at least one resharding event by now.
    event.ok_or_else(|| {
        let err_msg = "no supported shard layout change found!";
        error!(target: "resharding", ?shard_layout, err_msg);
        Error::ReshardingError(err_msg.to_owned())
    })
}

/// Task to perform the actual split of a flat storage shard. This may be a long operation time-wise.
///
/// Conceptually it simply copies each key-value pair from the parent shard to the correct child.
fn split_shard_task(
    resharder: FlatStorageResharderInner,
    controller: FlatStorageResharderController,
) {
    let success = split_shard_task_impl(resharder.clone(), controller.clone());
    split_shard_task_postprocessing(resharder, success);
    info!(target: "resharding", "flat storage shard split task finished, success: {success}");
    if let Err(err) = controller.completion_sender.send(success) {
        warn!(target: "resharding", ?err, "error notifying completion of flat storage shard split task")
    };
}

/// Retrieve parent shard UIds and current resharding event status.
/// Resharding event must be of type "Split".
fn get_parent_shard_and_status(
    resharder: &FlatStorageResharderInner,
) -> (ShardUId, SplittingParentStatus) {
    let event = resharder.resharding_event.lock().unwrap();
    match event.as_ref() {
        Some(FlatStorageReshardingEvent::Split(parent_shard, status)) => {
            (*parent_shard, status.clone())
        }
        None => panic!("a resharding event must exist!"),
    }
}

/// Performs the bulk of [split_shard_task].
///
/// Returns `true` if the routine completed successfully.
fn split_shard_task_impl(
    resharder: FlatStorageResharderInner,
    controller: FlatStorageResharderController,
) -> bool {
    if controller.is_interrupted() {
        return false;
    }

    /// Determines after how many key-values the process stops to
    /// commit changes and to check interruptions.
    const BATCH_SIZE: usize = 10_000;

    let (parent_shard, status) = get_parent_shard_and_status(&resharder);

    info!(target: "resharding", ?parent_shard, "flat storage shard split task: starting key-values copy");

    // Prepare the store object for commits and the iterator over parent's flat storage.
    let flat_store = resharder.runtime.store().flat_store();
    let mut iter = flat_store.iter(parent_shard);

    loop {
        let mut store_update = flat_store.store_update();

        // Process a `BATCH_SIZE` worth of key value pairs.
        let mut iter_exhausted = false;
        for _ in 0..BATCH_SIZE {
            match iter.next() {
                Some(Ok(kv)) => {
                    if let Err(err) = shard_split_handle_key_value(kv, &mut store_update, &status) {
                        error!(target: "resharding", ?err, "failed to handle flat storage key");
                        return false;
                    }
                }
                Some(Err(err)) => {
                    error!(target: "resharding", ?err, "failed to read flat storage value from parent shard");
                    return false;
                }
                None => {
                    iter_exhausted = true;
                }
            }
        }

        // Make a pause to commit and check if the routine should stop.
        if let Err(err) = store_update.commit() {
            error!(target: "resharding", ?err, "failed to commit store update");
            return false;
        }

        // TODO(Trisfald): metrics and logs

        // If `iter`` is exhausted we can exit after the store commit.
        if iter_exhausted {
            break;
        }
        if controller.is_interrupted() {
            return false;
        }
    }
    true
}

/// Handles the inheritance of a key-value pair from parent shard to children shards.
fn shard_split_handle_key_value(
    kv: (Vec<u8>, FlatStateValue),
    store_update: &mut FlatStoreUpdateAdapter,
    status: &SplittingParentStatus,
) -> Result<(), std::io::Error> {
    let (key, value) = kv;
    if key.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "flat storage key is empty",
        ));
    }
    let key_column_prefix = key[0];
    let SplittingParentStatus { left_child_shard, right_child_shard, shard_layout, .. } = status;

    // Copies a key value pair to the correct child by matching the account id to the new shard.
    let copy_kv_to_child =
        |account_id_parser: fn(&[u8]) -> Result<AccountId, _>| -> Result<(), std::io::Error> {
            // Derive the shard uid for this account in the new shard layout.
            let account_id = account_id_parser(&key)?;
            let new_shard_id = account_id_to_shard_id(&account_id, shard_layout);
            let new_shard_uid = ShardUId::from_shard_id_and_layout(new_shard_id, &shard_layout);

            // Sanity check we are truly writing to one of the expected children shards.
            if new_shard_uid != *left_child_shard && new_shard_uid != *right_child_shard {
                let err_msg = "account id doesn't map to any child shard!";
                error!(target: "resharding", ?new_shard_uid, ?left_child_shard, ?right_child_shard, ?shard_layout, err_msg);
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, err_msg));
            }
            // Add the new flat store entry.
            store_update.set(new_shard_uid, key, Some(value));
            Ok(())
        };

    match key_column_prefix {
        col::ACCOUNT => copy_kv_to_child(parse_account_id_from_account_key)?,
        col::CONTRACT_DATA => copy_kv_to_child(parse_account_id_from_contract_data_key)?,
        col::CONTRACT_CODE => copy_kv_to_child(parse_account_id_from_contract_code_key)?,
        col::ACCESS_KEY => copy_kv_to_child(parse_account_id_from_access_key_key)?,
        col::RECEIVED_DATA => copy_kv_to_child(parse_account_id_from_received_data_key)?,
        col::POSTPONED_RECEIPT_ID => copy_kv_to_child(|raw_key: &[u8]| {
            parse_account_id_from_trie_key_with_separator(
                col::POSTPONED_RECEIPT_ID,
                raw_key,
                ALL_COLUMNS_WITH_NAMES[col::POSTPONED_RECEIPT_ID as usize].1,
            )
        })?,
        col::PENDING_DATA_COUNT => copy_kv_to_child(|raw_key: &[u8]| {
            parse_account_id_from_trie_key_with_separator(
                col::PENDING_DATA_COUNT,
                raw_key,
                ALL_COLUMNS_WITH_NAMES[col::PENDING_DATA_COUNT as usize].1,
            )
        })?,
        col::POSTPONED_RECEIPT => copy_kv_to_child(|raw_key: &[u8]| {
            parse_account_id_from_trie_key_with_separator(
                col::POSTPONED_RECEIPT,
                raw_key,
                ALL_COLUMNS_WITH_NAMES[col::POSTPONED_RECEIPT as usize].1,
            )
        })?,
        col::DELAYED_RECEIPT_OR_INDICES => todo!(),
        col::PROMISE_YIELD_INDICES => todo!(),
        col::PROMISE_YIELD_TIMEOUT => todo!(),
        col::PROMISE_YIELD_RECEIPT => todo!(),
        col::BUFFERED_RECEIPT_INDICES => todo!(),
        col::BUFFERED_RECEIPT => todo!(),
        _ => unreachable!(),
    }
    Ok(())
}

/// Performs post-processing of shard splitting after all key-values have been moved from parent to children.
/// `success` indicates whether or not the previous phase was successful.
fn split_shard_task_postprocessing(resharder: FlatStorageResharderInner, success: bool) {
    let (parent_shard, status) = get_parent_shard_and_status(&resharder);
    let SplittingParentStatus { left_child_shard, right_child_shard, flat_head, .. } = status;
    let flat_store = resharder.runtime.store().flat_store();
    info!(target: "resharding", ?parent_shard, "flat storage shard split task: post-processing");

    let mut store_update = flat_store.store_update();
    if success {
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
    } else {
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
    store_update.commit().unwrap();
    // Terminate the resharding event.
    *resharder.resharding_event.lock().unwrap() = None;
}

/// Struct to describe, perform and track progress of a flat storage resharding.
#[derive(Clone, Debug)]
pub enum FlatStorageReshardingEvent {
    /// Split a shard.
    /// Includes the parent shard uid and the operation' status.
    Split(ShardUId, SplittingParentStatus),
}

/// Helps control the flat storage resharder operation. More specifically,
/// it has a way to know when the background task is done or to interrupt it.
#[derive(Clone)]
pub struct FlatStorageResharderController {
    /// Resharding handle to control interruption.
    handle: ReshardingHandle,
    /// This object will be used to signal when the background task is completed.
    /// A value of `true` means that the operation completed successfully.
    completion_sender: Sender<bool>,
    /// Corresponding receiver for `completion_sender`.
    pub completion_receiver: Receiver<bool>,
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

    /// Verify that the correct type of resharding is deduced from a new shard layout.
    #[test]
    fn parse_event_type_from_shard_layout() {
        // No resharding is not ok.
        assert!(event_type_from_shard_layout(&simple_shard_layout()).is_err());

        // Shard layouts V0 and V1 are rejected.
        assert!(event_type_from_shard_layout(&ShardLayout::v0_single_shard()).is_err());
        assert!(event_type_from_shard_layout(&ShardLayout::v1_test()).is_err());

        // Single split shard is ok.
        let layout = shard_layout_after_split();
        let event_type = event_type_from_shard_layout(&layout).unwrap();
        assert_eq!(
            event_type,
            ReshardingEventType::Split(ReshardingSplitParams {
                parent_shard: ShardUId { version: 3, shard_id: 1 },
                left_child_shard: ShardUId { version: 3, shard_id: 2 },
                right_child_shard: ShardUId { version: 3, shard_id: 3 }
            })
        );

        // Double split shard is not ok.
        let shards_split_map = BTreeMap::from([(0, vec![2, 3]), (1, vec![4, 5])]);
        let layout = ShardLayout::v2(
            vec![account!("ff"), account!("pp"), account!("ss")],
            vec![2, 3, 4, 5],
            Some(shards_split_map),
        );
        assert!(event_type_from_shard_layout(&layout).is_err());
    }

    /// Verify that another resharding can't be triggered if one is ongoing.
    #[test]
    fn concurrent_reshardings_are_disallowed() {
        init_test_logger();
        let (_, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = DelayedScheduler::default();
        let controller = FlatStorageResharderController::new();

        assert!(resharder
            .start_resharding_from_new_shard_layout(
                &new_shard_layout,
                &scheduler,
                controller.clone()
            )
            .is_ok());

        // Immediately interrupt the resharding.
        controller.handle().stop();

        assert!(resharder.resharding_event().is_some());
        assert!(resharder
            .start_resharding_from_new_shard_layout(&new_shard_layout, &scheduler, controller)
            .is_err());
    }

    /// Flat storage shard status should be set correctly upon starting a shard split.
    #[test]
    fn flat_storage_split_status_set() {
        init_test_logger();
        let (_, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = DelayedScheduler::default();
        let controller = FlatStorageResharderController::new();
        let flat_store = resharder.inner.runtime.store().flat_store();

        assert!(resharder
            .start_resharding_from_new_shard_layout(&new_shard_layout, &scheduler, controller)
            .is_ok());

        let resharding_event = resharder.resharding_event();
        match resharding_event.unwrap() {
            FlatStorageReshardingEvent::Split(parent, status) => {
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
        let (_, resharder) = create_fs_resharder(simple_shard_layout());
        let flat_store = resharder.inner.runtime.store().flat_store();
        let shard_layout = shard_layout_after_split();
        let resharding_type = event_type_from_shard_layout(&shard_layout).unwrap();
        let ReshardingSplitParams { parent_shard, left_child_shard, right_child_shard } =
            match resharding_type {
                ReshardingEventType::Split(params) => params,
            };

        let mut store_update = flat_store.store_update();

        // Write some random key-values in children shards.
        let dirty_key: Vec<u8> = vec![1, 2, 3, 4];
        let dirty_value = Some(FlatStateValue::Inlined(dirty_key.clone()));
        for child_shard in [left_child_shard, right_child_shard] {
            store_update.set(child_shard, dirty_key.clone(), dirty_value.clone());
        }

        // Set parent state to ShardSplitting, manually, to simulate a forcibly interrupted resharding attempt.
        let resharding_status =
            FlatStorageReshardingStatus::SplittingParent(SplittingParentStatus {
                left_child_shard,
                right_child_shard,
                shard_layout,
                // Values don't matter.
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

        let result = resharder.split_shard(
            ReshardingSplitParams {
                parent_shard: ShardUId { version: 3, shard_id: 1 },
                left_child_shard: ShardUId { version: 3, shard_id: 2 },
                right_child_shard: ShardUId { version: 3, shard_id: 3 },
            },
            &new_shard_layout,
            &scheduler,
            controller.clone(),
        );
        // TODO(Trisfald): replace the above with this simple call
        // let result =
        //     resharder.start_resharding_from_new_shard_layout(&new_shard_layout, &scheduler);
        assert!(result.is_ok());

        // Check flat storages of children contain the correct accounts.
        let left_child = ShardUId { version: 3, shard_id: 2 };
        let right_child = ShardUId { version: 3, shard_id: 3 };
        let flat_store = resharder.inner.runtime.store().flat_store();
        let account_mm_key = TrieKey::Account { account_id: account!("mm") };
        let account_vv_key = TrieKey::Account { account_id: account!("vv") };
        assert!(flat_store
            .get(left_child, &account_mm_key.to_vec())
            .is_ok_and(|val| val.is_some()));
        assert!(flat_store
            .get(right_child, &account_vv_key.to_vec())
            .is_ok_and(|val| val.is_some()));

        // Controller should signal that resharding ended.
        assert_eq!(controller.completion_receiver.recv_timeout(Duration::from_secs(1)), Ok(true));

        // Check final status of parent flat storage.
        let parent = ShardUId { version: 3, shard_id: 1 };
        assert_eq!(flat_store.get_flat_storage_status(parent), Ok(FlatStorageStatus::Empty));
        assert_eq!(flat_store.iter(parent).count(), 0);
        assert!(resharder
            .inner
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
        let (_, resharder) = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        let scheduler = DelayedScheduler::default();
        let controller = FlatStorageResharderController::new();

        assert!(resharder
            .start_resharding_from_new_shard_layout(
                &new_shard_layout,
                &scheduler,
                controller.clone()
            )
            .is_ok());
        let (parent_shard, status) = get_parent_shard_and_status(&resharder.inner);
        let SplittingParentStatus { left_child_shard, right_child_shard, flat_head, .. } = status;

        // Interrupt the task before it starts.
        controller.handle().stop();

        // Run the task.
        scheduler.call();

        // Check that resharding was effectively interrupted.
        let flat_store = resharder.inner.runtime.store().flat_store();
        assert_eq!(controller.completion_receiver.recv_timeout(Duration::from_secs(1)), Ok(false));
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
}
