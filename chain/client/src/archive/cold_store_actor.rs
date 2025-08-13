use std::sync::{Arc, atomic::AtomicBool};

use near_async::actix::wrapper::spawn_actix_actor;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
use near_async::time::Duration;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::types::BlockHeight;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::{CloudStorage, update_cloud_storage};
use near_store::archive::cold_storage::{
    CopyAllDataToColdStatus, copy_all_data_to_cold, update_cold_db, update_cold_head,
};
use near_store::config::SplitStorageConfig;
use near_store::db::ColdDB;
use near_store::db::metadata::DbKind;
use near_store::{NodeStorage, Store};

use crate::archive::types::{
    ColdStoreCopyResult, ColdStoreError, ColdStoreMigrationResult, cold_store_copy_result_to_string,
};
use crate::metrics;

pub struct ColdStoreActor {
    split_storage_config: SplitStorageConfig,
    genesis_height: BlockHeight,
    hot_store: Store,
    cold_db: Arc<ColdDB>,
    cloud_storage: Option<Arc<CloudStorage>>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    keep_going: Arc<AtomicBool>,
}

impl Actor for ColdStoreActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target : "cold_store", "Starting the cold store actor");
        self.cold_store_migration_loop(ctx);
    }
}

impl ColdStoreActor {
    pub fn new(
        split_storage_config: SplitStorageConfig,
        genesis_height: BlockHeight,
        hot_store: Store,
        cold_db: Arc<ColdDB>,
        cloud_storage: Option<Arc<CloudStorage>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        keep_going: Arc<AtomicBool>,
    ) -> Self {
        let cold_store_actor = ColdStoreActor {
            split_storage_config,
            genesis_height,
            hot_store,
            cold_db,
            cloud_storage,
            epoch_manager,
            shard_tracker,
            keep_going,
        };

        // Perform the sanity check before starting the actor.
        // If the check fails when the node is starting it's better to just fail
        // fast and crash the node immediately.
        let (cold_head_height, hot_final_head_height, hot_tail_height) =
            cold_store_actor.get_heights();
        sanity_check(cold_head_height, hot_final_head_height, hot_tail_height).unwrap();
        debug_assert!(cold_store_actor.shard_tracker.is_valid_for_archival());

        cold_store_actor
    }

    /// Return (cold head height, hot final head height, hot tail height).
    fn get_heights(&self) -> (BlockHeight, BlockHeight, BlockHeight) {
        let hot_store = self.hot_store.chain_store();
        let cold_store = self.cold_db.as_store().chain_store();

        // If HEAD is not set for cold storage we default it to genesis_height.
        let cold_head_height =
            cold_store.head().map(|tip| tip.height).unwrap_or(self.genesis_height);

        // If FINAL_HEAD is not set for hot storage we default it to genesis_height.
        let hot_final_head_height =
            hot_store.final_head().map(|tip| tip.height).unwrap_or(self.genesis_height);

        // If TAIL is not set for hot storage we default it to genesis_height.
        // TAIL not being set is not an error.
        // Archive dbs don't have TAIL, that means that they have all data from genesis_height.
        let hot_tail_height = hot_store.tail().unwrap_or(self.genesis_height);

        (cold_head_height, hot_final_head_height, hot_tail_height)
    }

    fn cold_store_migration_loop(&self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if !self.keep_going.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!(target: "cold_store", "stopping the initial migration loop");
            return;
        }

        match self.cold_store_migration() {
            Err(err) => {
                // We can either stop the cold store thread or hope that next time migration will not fail.
                // Here we pick the second option.
                let dur =
                    self.split_storage_config.cold_store_initial_migration_loop_sleep_duration;
                tracing::error!(target: "cold_store", ?err, ?dur, "Migration failed. Sleeping and trying again.");
                ctx.run_later("cold_store_migration_loop", dur, move |actor, ctx| {
                    actor.cold_store_migration_loop(ctx);
                });
            }
            Ok(migration_status) => {
                match migration_status {
                    ColdStoreMigrationResult::MigrationInterrupted => {
                        tracing::info!(target: "cold_store", "Cold storage migration was interrupted");
                        return;
                    }
                    ColdStoreMigrationResult::NoNeedForMigration
                    | ColdStoreMigrationResult::SuccessfulMigration => {
                        // Migration was successful, we can start the cold store loop.
                        tracing::info!(target : "cold_store", "Starting the cold store loop");
                        self.cold_store_loop(ctx);
                    }
                }
            }
        }
    }

    fn cold_store_loop(&self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if !self.keep_going.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!(target : "cold_store", "Stopping the cold store loop");
            return;
        }

        let result = self.cold_store_loop_impl();

        // A block older than the final head was copied. We should continue copying
        // until cold head reaches final head.
        let dur = if let Ok(ColdStoreCopyResult::OtherBlockCopied) = result {
            Duration::ZERO
        } else {
            self.split_storage_config.cold_store_loop_sleep_duration
        };

        ctx.run_later("cold_store_loop", dur, move |actor, ctx| {
            actor.cold_store_loop(ctx);
        });
    }

    /// This function performs migration to cold storage if needed.
    /// Migration can be interrupted via `keep_going` flag.
    ///
    /// Migration is performed if cold storage does not have a head set.
    /// New head is determined based on hot storage DBKind.
    /// - If hot storage is of type `Archive`, we need to perform initial migration from legacy archival node.
    ///   This process will take a long time. Cold head will be set to hot final head BEFORE the migration started.
    /// - If hot storage is of type `Hot`, this node was just created in split storage mode from genesis.
    ///   Genesis data is written to hot storage before node can join the chain. Cold storage remains empty during that time.
    ///   Thus, when cold loop is spawned, we need to perform migration of genesis data to cold storage.
    ///   Cold head will be set to genesis height.
    /// - Other kinds of hot storage are indicative of configuration error.
    /// New cold head is written only after the migration is fully finished.
    ///
    /// After cold head is determined this function
    /// 1. performs migration
    /// 2. updates cold head
    ///
    /// Any Ok status means that this function should not be retried:
    /// - either migration was performed (now or earlier)
    /// - or migration was interrupted, which means the `keep_going` flag was set to `false`
    ///   which means that everything cold store thread related has to stop
    ///
    /// Error status means that for some reason migration cannot be performed.
    fn cold_store_migration(&self) -> anyhow::Result<ColdStoreMigrationResult> {
        // Migration is only needed if cold storage is not properly initialized,
        // i.e. if cold head is not set.
        if self.cold_db.as_store().chain_store().head().is_ok() {
            tracing::info!(target: "cold_store", "Cold store already has a head set. No migration needed.");
            return Ok(ColdStoreMigrationResult::NoNeedForMigration);
        }

        tracing::info!(target: "cold_store", "Starting population of cold store.");
        let new_cold_height = match self.hot_store.get_db_kind()? {
            None => {
                tracing::error!(target: "cold_store", "Hot store DBKind not set.");
                return Err(anyhow::anyhow!("Hot store DBKind is not set"));
            }
            Some(DbKind::Hot) => {
                tracing::info!(target: "cold_store", "Hot store DBKind is Hot.");
                self.genesis_height
            }
            Some(DbKind::Archive) => {
                tracing::info!(target: "cold_store", "Hot store DBKind is Archive.");
                self.hot_store.chain_store().final_head()?.height
            }
            Some(kind) => {
                tracing::error!(target: "cold_store", ?kind, "Hot store DbKind not supported.");
                return Err(anyhow::anyhow!(format!("Hot store DBKind not {kind:?}.")));
            }
        };

        tracing::info!(target: "cold_store", new_cold_height, "Determined cold storage head height after migration");

        let batch_size = self.split_storage_config.cold_store_initial_migration_batch_size;
        match copy_all_data_to_cold(
            self.cold_db.clone(),
            &self.hot_store,
            batch_size,
            &self.keep_going,
        )? {
            CopyAllDataToColdStatus::EverythingCopied => {
                tracing::info!(target: "cold_store", new_cold_height, "Cold storage population was successful, writing cold head.");
                update_cold_head(self.cold_db.as_ref(), &self.hot_store, &new_cold_height)?;
                Ok(ColdStoreMigrationResult::SuccessfulMigration)
            }
            CopyAllDataToColdStatus::Interrupted => {
                tracing::info!(target: "cold_store", "Cold storage population was interrupted");
                Ok(ColdStoreMigrationResult::MigrationInterrupted)
            }
        }
    }

    fn cold_store_loop_impl(&self) -> anyhow::Result<ColdStoreCopyResult, ColdStoreError> {
        let instant = std::time::Instant::now();
        let result = self.cold_store_copy();
        let duration = instant.elapsed();

        let result_string = cold_store_copy_result_to_string(&result);
        metrics::COLD_STORE_COPY_RESULT.with_label_values(&[result_string]).inc();
        if duration > std::time::Duration::from_secs(1) {
            tracing::debug!(target : "cold_store", "cold_store_copy took {}s", duration.as_secs_f64());
        }

        match &result {
            Err(err) => {
                tracing::error!(target : "cold_store", error = format!("{err:#?}"), "cold_store_copy failed");
            }
            Ok(copy_result) => match copy_result {
                ColdStoreCopyResult::NoBlockCopied => {
                    tracing::trace!(target: "cold_store", "No block was copied - cold head is up to date");
                }
                ColdStoreCopyResult::LatestBlockCopied => {
                    tracing::trace!(target: "cold_store", "Latest block was copied");
                }
                ColdStoreCopyResult::OtherBlockCopied => {
                    tracing::trace!(target: "cold_store", "Other block was copied - more copying needed");
                }
            },
        }

        result
    }

    /// Checks if cold store head is behind the final head and if so copies data
    /// for the next available produced block after current cold store head.
    /// Updates cold store head after.
    fn cold_store_copy(&self) -> anyhow::Result<ColdStoreCopyResult, ColdStoreError> {
        let (cold_head_height, hot_final_head_height, hot_tail_height) = self.get_heights();
        let _span = tracing::debug_span!(target: "cold_store", "cold_store_copy", cold_head_height, hot_final_head_height, hot_tail_height).entered();

        sanity_check(cold_head_height, hot_final_head_height, hot_tail_height)?;

        if cold_head_height >= hot_final_head_height {
            return Ok(ColdStoreCopyResult::NoBlockCopied);
        }

        let mut next_height = cold_head_height + 1;
        let next_height_block_hash = loop {
            if next_height > hot_final_head_height {
                return Err(ColdStoreError::SkippedBlocksBetweenColdHeadAndNextHeightError {
                    cold_head_height,
                    next_height,
                    hot_final_head_height,
                });
            }
            // Here it should be sufficient to just read from hot storage.
            // Because BlockHeight is never garbage collectable and is not even copied to cold.
            if let Ok(next_height_block_hash) =
                self.hot_store.chain_store().get_block_hash_by_height(next_height)
            {
                break next_height_block_hash;
            }
            next_height = next_height + 1;
        };

        // The next block hash exists in hot store so we can use it to get epoch id.
        let epoch_id = self.epoch_manager.get_epoch_id(&next_height_block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let tracked_shards =
            self.shard_tracker.get_tracked_shards_for_non_validator_in_epoch(&epoch_id)?;
        let block_info = self.epoch_manager.get_block_info(&next_height_block_hash)?;
        let is_resharding_boundary =
            self.epoch_manager.is_resharding_boundary(block_info.prev_hash())?;

        update_cold_db(
            &self.cold_db,
            &self.hot_store,
            &shard_layout,
            &tracked_shards,
            &next_height,
            is_resharding_boundary,
            self.split_storage_config.num_cold_store_read_threads,
        )?;

        if let Some(cloud_storage) = &self.cloud_storage {
            update_cloud_storage(cloud_storage, &self.hot_store, &next_height)?;
        }

        update_cold_head(&self.cold_db, &self.hot_store, &next_height)?;

        let result = if next_height >= hot_final_head_height {
            Ok(ColdStoreCopyResult::LatestBlockCopied)
        } else {
            Ok(ColdStoreCopyResult::OtherBlockCopied)
        };

        tracing::trace!(target: "cold_store", ?result, "ending");
        result
    }
}

// Check some basic sanity conditions.
// * cold head <= hot final head
// * cold head >= hot tail
fn sanity_check(
    cold_head_height: BlockHeight,
    hot_final_head_height: BlockHeight,
    hot_tail_height: BlockHeight,
) -> anyhow::Result<(), ColdStoreError> {
    // We should only copy final blocks to cold storage.
    if cold_head_height > hot_final_head_height {
        return Err(ColdStoreError::ColdHeadAheadOfFinalHeadError {
            cold_head_height,
            hot_final_head_height,
        });
    }

    // Cold and Hot storages need to overlap. Without this check we would skip
    // blocks from cold_head_height to hot_tail_height. This will result in
    // corrupted cold storage.
    if cold_head_height < hot_tail_height {
        return Err(ColdStoreError::ColdHeadBehindHotTailError {
            cold_head_height,
            hot_tail_height,
        });
    }
    Ok(())
}

/// Spawns the cold store actor in a background thread and returns handle.
/// If cold store is not configured it does nothing and returns None.
/// This replaces the original `spawn_cold_store_loop` function.
pub fn spawn_cold_store_actor(
    split_storage_config: &SplitStorageConfig,
    genesis_height: BlockHeight,
    storage: &NodeStorage,
    epoch_manager: Arc<EpochManagerHandle>,
    shard_tracker: ShardTracker,
) -> anyhow::Result<Option<Arc<AtomicBool>>> {
    let hot_store = storage.get_hot_store();
    let cold_db = match storage.cold_db() {
        Some(cold_db) => cold_db.clone(),
        None => {
            tracing::debug!(target : "cold_store", "Not spawning the cold store actor because cold store is not configured");
            return Ok(None);
        }
    };

    // TODO(cloud_archival) Move this to a separate cloud storage loop.
    let cloud_storage = storage.get_cloud_storage().cloned();

    let keep_going = Arc::new(AtomicBool::new(true));
    let keep_going_clone = keep_going.clone();

    // Perform the sanity check before spawning the actor.
    // If the check fails when the node is starting it's better to just fail
    // fast and crash the node immediately.
    let cold_store = cold_db.as_store().chain_store();
    let cold_head_height = cold_store.head().map(|tip| tip.height).unwrap_or(genesis_height);
    let hot_final_head_height =
        hot_store.chain_store().final_head().map(|tip| tip.height).unwrap_or(genesis_height);
    let hot_tail_height = hot_store.chain_store().tail().unwrap_or(genesis_height);

    sanity_check(cold_head_height, hot_final_head_height, hot_tail_height)?;
    debug_assert!(shard_tracker.is_valid_for_archival());

    tracing::info!(target : "cold_store", "Spawning the cold store actor");

    let actor = ColdStoreActor::new(
        split_storage_config.clone(),
        genesis_height,
        hot_store,
        cold_db,
        cloud_storage,
        epoch_manager,
        shard_tracker,
        keep_going_clone,
    );

    let (_cold_store_actor_addr, _cold_store_arbiter) = spawn_actix_actor(actor);

    Ok(Some(keep_going))
}
