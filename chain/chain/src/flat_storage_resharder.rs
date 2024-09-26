//! Logic for resharding flat storage in parallel to chain processing.
//!
//! See [FlatStorageResharder] for more details about how the resharding takes place.

use std::sync::{Arc, Mutex};

use near_chain_primitives::Error;
use near_primitives::shard_layout::ShardLayout;
use near_store::{
    flat::{store_helper, FlatStorageReshardingStatus, FlatStorageStatus, SplittingParentStatus},
    ShardUId, StorageError,
};
use tracing::{debug, error, info};

use crate::types::RuntimeAdapter;

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
                self.check_no_resharding_in_progress()?;
                // On resume flat storage status is already set.
                // However, we don't know the current state of children shards,
                // so it's better to clean them.
                self.clean_children_shards(&status)?;
                self.split_shard_impl(parent_shard_uid, &status);
            }
            FlatStorageReshardingStatus::CatchingUp(_) => {
                info!(target: "resharding", ?shard_uid, ?status, "resuming flat storage shard catchup");
                // TODO(Trisfald): implement child catch up
                todo!()
            }
            FlatStorageReshardingStatus::ToBeDeleted => {
                // Parent shard's content has been previously copied to the children.
                // Nothing else to do.
            }
        }
        Ok(())
    }

    /// Starts a resharding event deduced from the new shard layout provided.
    ///
    /// For now, only splitting a shard is supported.
    pub fn start_resharding_from_new_shard_layout(
        &self,
        shard_layout: &ShardLayout,
    ) -> Result<(), Error> {
        match event_type_from_shard_layout(&shard_layout)? {
            ReshardingEventType::Split(parent_shard, left_child_shard, right_child_shard) => {
                self.split_shard(parent_shard, left_child_shard, right_child_shard, shard_layout)
            }
        }
    }

    /// Starts the event of splitting a parent shard flat storage into two children.
    pub fn split_shard(
        &self,
        parent_shard: ShardUId,
        left_child_shard: ShardUId,
        right_child_shard: ShardUId,
        shard_layout: &ShardLayout,
    ) -> Result<(), Error> {
        info!(target: "resharding", ?parent_shard, ?left_child_shard, ?right_child_shard, "initiating flat storage split");
        self.check_no_resharding_in_progress()?;

        // Change parent and children shards flat storage status.
        let mut store_update = self.inner.runtime.store().store_update();
        let status = SplittingParentStatus {
            left_child_shard,
            right_child_shard,
            shard_layout: shard_layout.clone(),
        };
        store_helper::set_flat_storage_status(
            &mut store_update,
            parent_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::SplittingParent(
                status.clone(),
            )),
        );
        store_helper::set_flat_storage_status(
            &mut store_update,
            left_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_helper::set_flat_storage_status(
            &mut store_update,
            right_child_shard,
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild),
        );
        store_update.commit()?;

        self.split_shard_impl(parent_shard, &status);
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

    fn split_shard_impl(&self, parent_shard: ShardUId, status: &SplittingParentStatus) {
        let event = FlatStorageReshardingEvent::Split(parent_shard, status.clone());
        self.set_resharding_event(event);
        debug!(target: "resharding", ?parent_shard, "starting flat storage split: copy of key-value pairs");

        // TODO(Trisfald): start `split_shard_task`
    }

    /// Cleans up children shards flat storage's content (status is excluded).
    fn clean_children_shards(&self, status: &SplittingParentStatus) -> Result<(), Error> {
        let mut store_update = self.inner.runtime.store().store_update();
        for child in [status.left_child_shard, status.right_child_shard] {
            store_helper::remove_all_deltas(&mut store_update, child);
            store_helper::remove_all_flat_state_values(&mut store_update, child);
        }
        store_update.commit()?;
        Ok(())
    }
}

/// Takes as input a [ShardLayout] definition and deduces which kind of resharding operation must be
/// performed.
///
/// Returns an error if there isn't any change in the shard layout that would require resharding.
fn event_type_from_shard_layout(shard_layout: &ShardLayout) -> Result<ReshardingEventType, Error> {
    // Resharding V3 supports shard layout V2 onwards.
    match shard_layout {
        ShardLayout::V0(_) | ShardLayout::V1(_) => {
            error!(target: "resharding", ?shard_layout, "unsupported shard layout!");
            return Err(Error::Other("resharding: unsupported shard layout".to_string()));
        }
    }

    let event = None;
    // Look for a shard having exactly two children, to trigger a split.
    for shard in shard_layout.shard_ids() {
        let parent = shard_layout.get_parent_shard_id(shard)?;
        if let Some(children) = shard_layout.get_children_shards_uids(parent) {
            if children.len() == 2 {
                if event.is_none() {
                    event = Some(ReshardingEventType::Split(
                        ShardUId::from_shard_id_and_layout(parent, &shard_layout),
                        children[0],
                        children[1],
                    ))
                } else {
                    error!(target: "resharding", ?shard_layout, "two reshards can't be performed at the same time!");
                    return Err(Error::Other(
                        "resharding: new shard layout requires two reshards".to_string(),
                    ));
                }
            }
        }
    }
    event.ok_or_else(|| {
        error!(target: "resharding", ?shard_layout, "no supported shard layout change found!");
        Error::Other(
            "resharding: shard layout doesn't contain any supported shard layout change"
                .to_string(),
        )
    })
}

/// Task to perform the actual split of a flat storage shard. This may be a long operation time-wise.
///
/// Conceptually it simply copies each key-value pair from the parent shard to the correct child.
#[allow(unused)] // TODO(Trisfald): remove annotation
fn split_shard_task(_resharder: FlatStorageResharderInner) {
    // TODO(Trisfald): implement logic
    // store_helper::iter_flat_state_entries
    todo!()
}

/// Struct used to destructure a new shard layout definition into the resulting resharding event.
#[cfg_attr(test, derive(PartialEq, Eq))]
enum ReshardingEventType {
    /// Split a shard.
    /// Includes: `parent_shard`, `left_child_shard` and `right_child_shard`.
    Split(ShardUId, ShardUId, ShardUId),
}

/// Struct to describe, perform and track progress of a flat storage resharding.
#[derive(Clone, Debug)]
pub enum FlatStorageReshardingEvent {
    /// Split a shard.
    /// Includes the parent shard uid and the operation' status.
    Split(ShardUId, SplittingParentStatus),
}

#[cfg(test)]
mod tests {
    use near_async::time::Clock;
    use near_chain_configs::Genesis;
    use near_epoch_manager::EpochManager;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::{shard_layout::ShardLayout, state::FlatStateValue, types::AccountId};
    use near_store::{genesis::initialize_genesis_state, test_utils::create_test_store};

    use crate::runtime::NightshadeRuntime;

    use super::*;

    /// Shorthand to create account ID.
    macro_rules! account {
        ($str:expr) => {
            $str.parse::<AccountId>().unwrap()
        };
    }

    /// Simple shard layout with two shards.
    fn simple_shard_layout() -> ShardLayout {
        // TODO(Trisfald): use shard layout v2
        ShardLayout::v1(vec![account!("ff")], None, 3)
    }

    /// Derived from [simple_shard_layout] by splitting the second shard.
    fn shard_layout_after_split() -> ShardLayout {
        // TODO(Trisfald): use shard layout v2
        ShardLayout::v1(vec![account!("ff"), account!("pp")], Some(vec![vec![0], vec![1, 2]]), 3)
    }

    /// Generic test setup.
    fn create_fs_resharder(shard_layout: ShardLayout) -> FlatStorageResharder {
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
        let runtime =
            NightshadeRuntime::test(tempdir.path(), store, &genesis.config, epoch_manager);
        FlatStorageResharder::new(runtime)
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
        let _event_type = event_type_from_shard_layout(&layout);
        // TODO(Trisfald): it won't work until we have shard layout v2.
        // assert_eq!(event_type, ReshardingEventType::Split(...));

        // Double split shard is not ok.
        // TODO(Trisfald): use shard layout v2
        let layout = ShardLayout::v1(
            vec![account!("ff"), account!("pp"), account!("ss")],
            Some(vec![vec![0, 2], vec![1, 2]]),
            3,
        );
        assert!(event_type_from_shard_layout(&layout).is_err());
    }

    /// Verify that another resharding can't be triggered if one is ongoing.
    #[test]
    fn concurrent_reshardings_are_disallowed() {
        init_test_logger();
        let _resharder = create_fs_resharder(simple_shard_layout());
        let _new_shard_layout = shard_layout_after_split();

        // TODO(Trisfald): it won't work until we have shard layout v2.

        // assert!(resharder.start_resharding_from_new_shard_layout(&new_shard_layout).is_ok());
        // TODO(Trisfald): find a way to make sure first resharding doesn't finish immediately
        // assert!(resharder.resharding_event.lock().unwrap().is_some());
        // assert!(resharder.start_resharding_from_new_shard_layout(&new_shard_layout).is_err());
    }

    /// Flat storage shard status should be set correctly upon starting a shard split.
    #[test]
    fn flat_storage_split_status_set() {
        init_test_logger();
        let resharder = create_fs_resharder(simple_shard_layout());
        let _new_shard_layout = shard_layout_after_split();

        // TODO(Trisfald): it won't work until we have shard layout v2.

        // assert!(resharder.start_resharding_from_new_shard_layout(&new_shard_layout).is_ok());

        let resharding_event = resharder.resharding_event();
        match resharding_event.unwrap() {
            FlatStorageReshardingEvent::Split(parent, status) => {
                assert_eq!(
                    store_helper::get_flat_storage_status(resharder.inner.runtime.store(), parent),
                    Ok(FlatStorageStatus::Resharding(
                        FlatStorageReshardingStatus::SplittingParent(status.clone())
                    ))
                );
                assert_eq!(
                    store_helper::get_flat_storage_status(
                        resharder.inner.runtime.store(),
                        status.left_child_shard
                    ),
                    Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild))
                );
                assert_eq!(
                    store_helper::get_flat_storage_status(
                        resharder.inner.runtime.store(),
                        status.right_child_shard
                    ),
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
        let resharder = create_fs_resharder(simple_shard_layout());
        let store = resharder.inner.runtime.store();
        let shard_layout = shard_layout_after_split();
        let resharding_type = event_type_from_shard_layout(&shard_layout).unwrap();
        let (parent, left_child_shard, right_child_shard) = match resharding_type {
            ReshardingEventType::Split(parent, left_child, right_child) => {
                (parent, left_child, right_child)
            }
        };

        let mut store_update = store.store_update();

        // Write some random key-values in children shards.
        let dirty_key: Vec<u8> = vec![1, 2, 3, 4];
        let dirty_value = Some(FlatStateValue::Inlined(dirty_key.clone()));
        for child_shard in [left_child_shard, right_child_shard] {
            store_helper::set_flat_state_value(
                &mut store_update,
                child_shard,
                dirty_key.clone(),
                dirty_value.clone(),
            );
        }

        // Set parent state to ShardSplitting, manually, to simulate a forcibly interrupted resharding attempt.
        let resharding_status =
            FlatStorageReshardingStatus::SplittingParent(SplittingParentStatus {
                left_child_shard,
                right_child_shard,
                shard_layout,
            });
        store_helper::set_flat_storage_status(
            &mut store_update,
            parent,
            FlatStorageStatus::Resharding(resharding_status.clone()),
        );

        store_update.commit().unwrap();

        // Resume resharding.
        resharder.resume(parent, &resharding_status).unwrap();

        // Children should not contain the random keys written before.
        for child_shard in [left_child_shard, right_child_shard] {
            assert_eq!(
                store_helper::get_flat_state_value(&store, child_shard, &dirty_key),
                Ok(None)
            );
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
        let resharder = create_fs_resharder(simple_shard_layout());
        let new_shard_layout = shard_layout_after_split();
        assert!(resharder.start_resharding_from_new_shard_layout(&new_shard_layout).is_ok());

        // Check flat storages of children contain the correct accounts.
        let left_child = ShardUId::from_shard_id_and_layout(2, &new_shard_layout);
        let right_child = ShardUId::from_shard_id_and_layout(3, &new_shard_layout);
        let store = resharder.inner.runtime.store();
        let account_mm = account!("mm");
        let account_vv = account!("vv");
        assert!(store_helper::get_flat_state_value(&store, left_child, account_mm.as_bytes())
            .is_ok_and(|val| val.is_some()));
        assert!(store_helper::get_flat_state_value(&store, right_child, account_vv.as_bytes())
            .is_ok_and(|val| val.is_some()));
    }
}
