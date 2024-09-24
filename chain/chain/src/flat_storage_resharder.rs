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
    runtime: Arc<dyn RuntimeAdapter>,
    resharding_event: Mutex<Option<ReshardingEvent>>,
}

impl FlatStorageResharder {
    /// Creates a new `FlatStorageResharder`.
    pub fn new(runtime: Arc<dyn RuntimeAdapter>) -> Self {
        let resharding_event = Mutex::new(None);
        Self { runtime, resharding_event }
    }

    /// Resumes a resharding event that was in progress.
    pub fn resume(
        &self,
        shard_uid: &ShardUId,
        status: &FlatStorageReshardingStatus,
    ) -> Result<(), StorageError> {
        match status {
            FlatStorageReshardingStatus::CreatingChild => {
                // Nothing to do here because the parent will take care of resuming work.
            }
            FlatStorageReshardingStatus::SplittingParent(status) => {
                let parent_shard_uid = shard_uid;
                info!(target: "resharding", ?parent_shard_uid, ?status, "resuming flat storage resharding");
                self.check_no_resharding_in_progress()?;
                // On resume flat storage status is already set.
                self.split_shard_impl(*parent_shard_uid, &status);
            }
            FlatStorageReshardingStatus::CatchingUp(_) => {
                // TODO(Trisfald): implement catch up
                todo!()
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
        match Self::event_params_from_shard_layout(&shard_layout)? {
            ReshardingEventParams::Split(parent_shard, left_child_shard, right_child_shard) => {
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
        // TODO(trisfald): add metrics
        let mut store_update = self.runtime.store().store_update();
        let status = SplittingParentStatus {
            left_child_shard,
            right_child_shard,
            shard_layout: shard_layout.clone(),
            latest_account_moved: None,
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

    fn check_no_resharding_in_progress(&self) -> Result<(), StorageError> {
        // Do not allow multiple resharding events in parallel.
        if self.resharding_event.lock().unwrap().is_some() {
            error!(target: "resharding", "trying to start a new flat storage resharding event while one is already in progress!");
            Err(StorageError::FlatStorageReshardingAlreadyInProgress)
        } else {
            Ok(())
        }
    }

    fn set_resharding_event(&self, event: ReshardingEvent) {
        *self.resharding_event.lock().unwrap() = Some(event);
    }

    /// Returns the current in-progress resharding event, if any.
    pub fn resharding_event(&self) -> Option<ReshardingEvent> {
        self.resharding_event.lock().unwrap().clone()
    }

    fn split_shard_impl(&self, parent_shard: ShardUId, status: &SplittingParentStatus) {
        let event = ReshardingEvent::Split(parent_shard, status.clone());
        self.set_resharding_event(event);
        debug!(target: "resharding", ?parent_shard, "starting flat storage split: copy of key-value pairs");

        // TODO(Trisfald): implement copy of keys from parent to children
    }

    fn event_params_from_shard_layout(
        shard_layout: &ShardLayout,
    ) -> Result<ReshardingEventParams, Error> {
        // Resharder supports shard layout V2 onwards.
        match shard_layout {
            ShardLayout::V0(_) | ShardLayout::V1(_) => {
                error!(target: "resharding", ?shard_layout, "unsupported shard layout!");
                return Err(Error::Other(
                    "flat storage resharding: unsupported shard layout".to_string(),
                ));
            }
        }

        // Look for a shard having exactly two children, to trigger a split.
        for shard in shard_layout.shard_ids() {
            if let Ok(parent) = shard_layout.get_parent_shard_id(shard) {
                if let Some(children) = shard_layout.get_children_shards_uids(parent) {
                    if children.len() == 2 {
                        return Ok(ReshardingEventParams::Split(
                            ShardUId::from_shard_id_and_layout(parent, &shard_layout),
                            children[0],
                            children[1],
                        ));
                    }
                }
            }
        }
        error!(target: "resharding", ?shard_layout, "no supported shard layout change found!");
        Err(Error::Other("flat storage resharding: shard layout doesn't contain any supported shard layout change".to_string()))
    }
}

/// Struct used to destructure a new shard layout definition into the resulting resharding event.
#[cfg_attr(test, derive(PartialEq, Eq))]
enum ReshardingEventParams {
    /// Split a shard.
    /// Includes: `parent_shard`, `left_child_shard` and `right_child_shard`.
    Split(ShardUId, ShardUId, ShardUId),
}

/// Struct to describe, perform and track progress of a flat storage resharding.
#[derive(Clone, Debug)]
pub enum ReshardingEvent {
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
    use near_primitives::shard_layout::ShardLayout;
    use near_store::{genesis::initialize_genesis_state, test_utils::create_test_store};

    use crate::runtime::NightshadeRuntime;

    use super::*;

    /// Shorthand to create account ID.
    macro_rules! account {
        ($str:expr) => {
            $str.parse().unwrap()
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
        let runtime = NightshadeRuntime::test(
            tempdir.path(),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        FlatStorageResharder::new(runtime)
    }

    /// Verify that the correct type of resharding is deduced from a new shard layout.
    #[test]
    fn parse_event_params_from_shard_layout() {
        // No resharding specified.
        assert!(
            FlatStorageResharder::event_params_from_shard_layout(&simple_shard_layout()).is_err()
        );

        // Shard layouts V0 and V1 are rejected.
        assert!(FlatStorageResharder::event_params_from_shard_layout(
            &ShardLayout::v0_single_shard()
        )
        .is_err());
        assert!(
            FlatStorageResharder::event_params_from_shard_layout(&ShardLayout::v1_test()).is_err()
        );

        // Split shard.
        {
            let layout = shard_layout_after_split();
            let _event_params = FlatStorageResharder::event_params_from_shard_layout(&layout);

            // TODO(Trisfald): it won't work until we have shard layout v2.
            // assert_eq!(event_params, ReshardingEventParams::Split(...));
        }
    }

    /// Verify that another resharding can't be triggered if one is ongoing.
    #[test]
    fn concurrent_reshardings_are_disallowed() {
        init_test_logger();
        let _resharder = create_fs_resharder(simple_shard_layout());
        let _new_shard_layout = shard_layout_after_split();

        // TODO(Trisfald): it won't work until we have shard layout v2.

        // assert!(resharder.start_resharding_from_new_shard_layout(&new_shard_layout).is_ok());
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
            ReshardingEvent::Split(parent, status) => {
                assert_eq!(
                    store_helper::get_flat_storage_status(resharder.runtime.store(), parent),
                    Ok(FlatStorageStatus::Resharding(
                        FlatStorageReshardingStatus::SplittingParent(status.clone())
                    ))
                );
                assert_eq!(
                    store_helper::get_flat_storage_status(
                        resharder.runtime.store(),
                        status.left_child_shard
                    ),
                    Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild))
                );
                assert_eq!(
                    store_helper::get_flat_storage_status(
                        resharder.runtime.store(),
                        status.right_child_shard
                    ),
                    Ok(FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CreatingChild))
                );
            }
        }
    }
}
