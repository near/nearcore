use std::sync::{Arc, atomic::AtomicBool};

use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
use near_async::time::Duration;
use near_chain::types::Tip;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::errors::EpochError;
use near_primitives::types::BlockHeight;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::{CloudStorage, update_cloud_storage};
use near_store::archive::cold_storage::{
    CopyAllDataToColdStatus, copy_all_data_to_cold, get_cold_head, update_cold_db, update_cold_head,
};
use near_store::config::SplitStorageConfig;
use near_store::db::ColdDB;
use near_store::db::metadata::DbKind;
use near_store::{DBCol, FINAL_HEAD_KEY, NodeStorage, Store, TAIL_KEY, set_genesis_height};

use crate::metrics;

/// The ColdStoreCopyResult indicates if and what block was copied.
#[derive(Debug)]
enum ColdStoreCopyResult {
    // No block was copied. The cold head is up to date with the final head.
    NoBlockCopied,
    /// The final head block was copied. This is the latest block
    /// that could be copied until new block is finalized.
    LatestBlockCopied,
    /// A block older than the final head block was copied. There
    /// are more blocks that can be copied immediately.
    OtherBlockCopied,
}

/// The ColdStoreError indicates what errors were encountered while copying a blocks and running sanity checks.
#[derive(thiserror::Error, Debug)]
pub enum ColdStoreError {
    #[error(
        "Cold head is ahead of final head. cold head height: {cold_head_height} final head height {hot_final_head_height}"
    )]
    ColdHeadAheadOfFinalHeadError { cold_head_height: u64, hot_final_head_height: u64 },
    #[error(
        "Cold head is behind hot tail. cold head height: {cold_head_height} hot tail height {hot_tail_height}"
    )]
    ColdHeadBehindHotTailError { cold_head_height: u64, hot_tail_height: u64 },
    #[error(
        "All blocks between cold head and next height were skipped, but next height > hot final head. cold head {cold_head_height} next height to copy: {next_height} final head height {hot_final_head_height}"
    )]
    SkippedBlocksBetweenColdHeadAndNextHeightError {
        cold_head_height: u64,
        next_height: u64,
        hot_final_head_height: u64,
    },
    #[error("Failed to read the cold head hash at height {cold_head_height}")]
    ColdHeadHashReadError { cold_head_height: u64 },
    #[error("Cold store copy error: {e}")]
    EpochError { e: EpochError },
    #[error("Cold store copy error: {message}")]
    Error { message: String },
}

impl From<std::io::Error> for ColdStoreError {
    fn from(error: std::io::Error) -> Self {
        ColdStoreError::Error { message: error.to_string() }
    }
}

impl From<EpochError> for ColdStoreError {
    fn from(error: EpochError) -> Self {
        ColdStoreError::EpochError { e: error }
    }
}

fn cold_store_copy_result_to_string(
    result: &anyhow::Result<ColdStoreCopyResult, ColdStoreError>,
) -> &str {
    match result {
        Err(ColdStoreError::ColdHeadBehindHotTailError { .. }) => "cold_head_behind_hot_tail_error",
        Err(ColdStoreError::ColdHeadAheadOfFinalHeadError { .. }) => {
            "cold_head_ahead_of_final_head_error"
        }
        Err(ColdStoreError::SkippedBlocksBetweenColdHeadAndNextHeightError { .. }) => {
            "skipped_blocks_between_cold_head_and_next_height_error"
        }
        Err(ColdStoreError::ColdHeadHashReadError { .. }) => "cold_head_hash_read_error",
        Err(ColdStoreError::EpochError { .. }) => "epoch_error",
        Err(ColdStoreError::Error { .. }) => "error",
        Ok(ColdStoreCopyResult::NoBlockCopied) => "no_block_copied",
        Ok(ColdStoreCopyResult::LatestBlockCopied) => "latest_block_copied",
        Ok(ColdStoreCopyResult::OtherBlockCopied) => "other_block_copied",
    }
}

#[derive(Debug)]
enum ColdStoreMigrationResult {
    /// Cold storage was already initialized
    NoNeedForMigration,
    /// Performed a successful cold storage migration
    SuccessfulMigration,
    /// Migration was interrupted by keep_going flag
    MigrationInterrupted,
}

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
        // Note that we start with the cold store migration loop which later spawns the cold store loop.
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
        debug_assert!(shard_tracker.is_valid_for_archival());
        ColdStoreActor {
            split_storage_config,
            genesis_height,
            hot_store,
            cold_db,
            cloud_storage,
            epoch_manager,
            shard_tracker,
            keep_going,
        }
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

    /// Runs a loop that tries to copy all data from hot store to cold (do migration).
    /// If migration fails sleeps for 30s and tries again.
    /// If migration returned any successful status (including interruption status) breaks the loop.
    fn cold_store_migration_loop(&self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if !self.keep_going.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!(target: "cold_store", "stopping the initial migration loop");
            return;
        }

        match self.cold_store_migration() {
            Err(err) => {
                // We can either stop the cold store actor or hope that next time migration will not fail.
                // Here we pick the second option.
                let duration =
                    self.split_storage_config.cold_store_initial_migration_loop_sleep_duration;
                tracing::error!(target: "cold_store", ?err, ?duration, "Migration failed. Sleeping and trying again.");
                ctx.run_later("cold_store_migration_loop", duration, move |actor, ctx| {
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

    // This method will copy data from hot storage to cold storage in a loop.
    // It will try to copy blocks as fast as possible up until cold head = final head.
    // Once the cold head reaches the final head it will sleep for one second before
    // trying to copy data at the next height.
    fn cold_store_loop(&self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if !self.keep_going.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!(target : "cold_store", "Stopping the cold store loop");
            return;
        }

        let result = self.cold_store_loop_impl();

        // A block older than the final head was copied. We should continue copying
        // until cold head reaches final head.
        let duration = if let Ok(ColdStoreCopyResult::OtherBlockCopied) = result {
            Duration::ZERO
        } else {
            self.split_storage_config.cold_store_loop_sleep_duration
        };

        ctx.run_later("cold_store_loop", duration, move |actor, ctx| {
            actor.cold_store_loop(ctx);
        });
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
        // If HEAD is not set for cold storage we default it to genesis_height.
        let cold_head = get_cold_head(&self.cold_db)?;
        let cold_head_height = cold_head.map_or(self.genesis_height, |tip| tip.height);

        // If FINAL_HEAD is not set for hot storage we default it to genesis_height.
        let hot_final_head = self.hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
        let hot_final_head_height = hot_final_head.map_or(self.genesis_height, |tip| tip.height);

        // If TAIL is not set for hot storage we default it to genesis_height.
        // TAIL not being set is not an error.
        // Archive dbs don't have TAIL, that means that they have all data from genesis_height.
        let hot_tail = self.hot_store.get_ser::<u64>(DBCol::BlockMisc, TAIL_KEY)?;
        let hot_tail_height = hot_tail.unwrap_or(self.genesis_height);

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

/// Creates the cold store actor and keep_going handle if cold store is configured.
/// Returns None if cold store is not configured or trie changes are not saved.
/// This replaces the original `spawn_cold_store_loop` function.
pub fn create_cold_store_actor(
    save_trie_changes: Option<bool>,
    split_storage_config: &SplitStorageConfig,
    genesis_height: BlockHeight,
    storage: &NodeStorage,
    epoch_manager: Arc<EpochManagerHandle>,
    shard_tracker: ShardTracker,
) -> anyhow::Result<Option<(ColdStoreActor, Arc<AtomicBool>)>> {
    if save_trie_changes != Some(true) {
        tracing::debug!(target:"cold_store", "Not creating cold store actor because TrieChanges are not saved");
        return Ok(None);
    }
    let hot_store = storage.get_hot_store();
    let cold_db = match storage.cold_db() {
        Some(cold_db) => cold_db.clone(),
        None => {
            tracing::debug!(target : "cold_store", "Not creating the cold store actor because cold store is not configured");
            return Ok(None);
        }
    };

    // TODO(cloud_archival) Move this to a separate cloud storage loop.
    let cloud_storage = storage.get_cloud_storage().cloned();

    let keep_going = Arc::new(AtomicBool::new(true));
    let keep_going_clone = keep_going.clone();

    // Save the genesis height to cold storage
    let mut store_update = cold_db.as_store().store_update();
    set_genesis_height(&mut store_update, &genesis_height);
    store_update.commit()?;

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

    tracing::info!(target : "cold_store", "Creating the cold store actor");

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

    Ok(Some((actor, keep_going)))
}

// Check some basic sanity conditions.
// * cold head <= hot final head
// * cold head >= hot tail
fn sanity_check(
    cold_head_height: u64,
    hot_final_head_height: u64,
    hot_tail_height: u64,
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
