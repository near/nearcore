use std::sync::{Arc, atomic::AtomicBool};

use near_chain::types::Tip;
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::errors::EpochError;
use near_primitives::{hash::CryptoHash, types::BlockHeight};
use near_store::config::SplitStorageConfig;
use near_store::db::metadata::DbKind;
use near_store::{
    DBCol, FINAL_HEAD_KEY, NodeStorage, Store, TAIL_KEY,
    archive::cold_storage::{
        CopyAllDataToColdStatus, copy_all_data_to_cold, get_cold_head, update_cold_db,
        update_cold_head,
    },
    db::ColdDB,
};

use crate::{NearConfig, metrics};

/// A handle that keeps the state of the cold store loop and can be used to stop it.
pub struct ColdStoreLoopHandle {
    join_handle: std::thread::JoinHandle<()>,
    keep_going: Arc<AtomicBool>,
}

impl ColdStoreLoopHandle {
    pub fn stop(self) {
        self.keep_going.store(false, std::sync::atomic::Ordering::Relaxed);
        match self.join_handle.join() {
            Ok(_) => {
                tracing::debug!(target : "cold_store", "Joined the cold store loop thread");
            }
            Err(_) => {
                tracing::error!(target : "cold_store", "Failed to join the cold store loop thread");
            }
        }
    }
}

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

/// Checks if cold store head is behind the final head and if so copies data
/// for the next available produced block after current cold store head.
/// Updates cold store head after.
fn cold_store_copy(
    hot_store: &Store,
    cold_db: &ColdDB,
    genesis_height: BlockHeight,
    epoch_manager: &EpochManagerHandle,
    num_threads: usize,
) -> anyhow::Result<ColdStoreCopyResult, ColdStoreError> {
    // If HEAD is not set for cold storage we default it to genesis_height.
    let cold_head = get_cold_head(cold_db)?;
    let cold_head_height = cold_head.map_or(genesis_height, |tip| tip.height);

    // If FINAL_HEAD is not set for hot storage we default it to genesis_height.
    let hot_final_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
    let hot_final_head_height = hot_final_head.map_or(genesis_height, |tip| tip.height);

    // If TAIL is not set for hot storage we default it to genesis_height.
    // TAIL not being set is not an error.
    // Archive dbs don't have TAIL, that means that they have all data from genesis_height.
    let hot_tail = hot_store.get_ser::<u64>(DBCol::BlockMisc, TAIL_KEY)?;
    let hot_tail_height = hot_tail.unwrap_or(genesis_height);

    let _span = tracing::debug_span!(target: "cold_store", "cold_store_copy", cold_head_height, hot_final_head_height, hot_tail_height).entered();

    sanity_check_impl(cold_head_height, hot_final_head_height, hot_tail_height)?;

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
        let next_height_block_hash =
            hot_store.get_ser::<CryptoHash>(DBCol::BlockHeight, &next_height.to_le_bytes())?;
        if let Some(next_height_block_hash) = next_height_block_hash {
            break next_height_block_hash;
        }
        next_height = next_height + 1;
    };
    // The next block hash exists in hot store so we can use it to get epoch id.
    let epoch_id = epoch_manager.get_epoch_id(&next_height_block_hash)?;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    let is_last_block_in_epoch =
        epoch_manager.is_next_block_epoch_start(&next_height_block_hash)?;
    update_cold_db(
        cold_db,
        hot_store,
        &shard_layout,
        &next_height,
        is_last_block_in_epoch,
        num_threads,
    )?;
    update_cold_head(cold_db, hot_store, &next_height)?;

    let result = if next_height >= hot_final_head_height {
        Ok(ColdStoreCopyResult::LatestBlockCopied)
    } else {
        Ok(ColdStoreCopyResult::OtherBlockCopied)
    };

    tracing::trace!(target: "cold_store", ?result, "ending");
    result
}

// Check some basic sanity conditions.
// * cold head <= hot final head
// * cold head >= hot tail
fn sanity_check(
    hot_store: &Store,
    cold_db: &ColdDB,
    genesis_height: BlockHeight,
) -> anyhow::Result<()> {
    let cold_head = get_cold_head(cold_db)?;
    let cold_head_height = cold_head.map_or(genesis_height, |tip| tip.height);

    let hot_final_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
    let hot_final_head_height = hot_final_head.map_or(genesis_height, |tip| tip.height);

    let hot_tail = hot_store.get_ser::<u64>(DBCol::BlockMisc, TAIL_KEY)?;
    let hot_tail_height = hot_tail.unwrap_or(genesis_height);

    sanity_check_impl(cold_head_height, hot_final_head_height, hot_tail_height)?;

    Ok(())
}

fn sanity_check_impl(
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
fn cold_store_migration(
    split_storage_config: &SplitStorageConfig,
    keep_going: &Arc<AtomicBool>,
    genesis_height: BlockHeight,
    hot_store: &Store,
    cold_db: Arc<ColdDB>,
) -> anyhow::Result<ColdStoreMigrationResult> {
    // Migration is only needed if cold storage is not properly initialized,
    // i.e. if cold head is not set.
    if get_cold_head(cold_db.as_ref())?.is_some() {
        return Ok(ColdStoreMigrationResult::NoNeedForMigration);
    }

    tracing::info!(target: "cold_store", "Starting population of cold store.");
    let new_cold_height = match hot_store.get_db_kind()? {
        None => {
            tracing::error!(target: "cold_store", "Hot store DBKind not set.");
            return Err(anyhow::anyhow!("Hot store DBKind is not set"));
        }
        Some(DbKind::Hot) => {
            tracing::info!(target: "cold_store", "Hot store DBKind is Hot.");
            genesis_height
        }
        Some(DbKind::Archive) => {
            tracing::info!(target: "cold_store", "Hot store DBKind is Archive.");
            hot_store
                .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
                .ok_or_else(|| anyhow::anyhow!("FINAL_HEAD not found in hot storage"))?
                .height
        }
        Some(kind) => {
            tracing::error!(target: "cold_store", ?kind, "Hot store DbKind not supported.");
            return Err(anyhow::anyhow!(format!("Hot store DBKind not {kind:?}.")));
        }
    };

    tracing::info!(target: "cold_store", new_cold_height, "Determined cold storage head height after migration");

    let batch_size = split_storage_config.cold_store_initial_migration_batch_size;
    match copy_all_data_to_cold(cold_db.clone(), hot_store, batch_size, keep_going)? {
        CopyAllDataToColdStatus::EverythingCopied => {
            tracing::info!(target: "cold_store", new_cold_height, "Cold storage population was successful, writing cold head.");
            update_cold_head(cold_db.as_ref(), hot_store, &new_cold_height)?;
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
fn cold_store_migration_loop(
    split_storage_config: &SplitStorageConfig,
    keep_going: &Arc<AtomicBool>,
    genesis_height: BlockHeight,
    hot_store: &Store,
    cold_db: Arc<ColdDB>,
) {
    tracing::info!(target: "cold_store", "starting initial migration loop");
    loop {
        if !keep_going.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!(target: "cold_store", "stopping the initial migration loop");
            break;
        }
        match cold_store_migration(
            split_storage_config,
            keep_going,
            genesis_height,
            hot_store,
            cold_db.clone(),
        ) {
            // We can either stop the cold store thread or hope that next time migration will not fail.
            // Here we pick the second option.
            Err(err) => {
                let sleep_duration = split_storage_config
                    .cold_store_initial_migration_loop_sleep_duration
                    .unsigned_abs();
                let sleep_duration_in_secs = split_storage_config
                    .cold_store_initial_migration_loop_sleep_duration
                    .whole_seconds();
                tracing::error!(target: "cold_store", ?err, ?sleep_duration_in_secs, "Migration failed. Sleeping and trying again.", );
                std::thread::sleep(sleep_duration);
            }
            // Any Ok status from `cold_store_initial_migration` function means that we can proceed to regular run.
            Ok(migration_status) => {
                tracing::info!(target: "cold_store", ?migration_status, "Moving on.");
                break;
            }
        }
    }
}

// This method will copy data from hot storage to cold storage in a loop.
// It will try to copy blocks as fast as possible up until cold head = final head.
// Once the cold head reaches the final head it will sleep for one second before
// trying to copy data at the next height.
// TODO clean up the interface, currently we need to pass hot store, cold store and
// cold_db which is redundant.
fn cold_store_loop(
    split_storage_config: &SplitStorageConfig,
    keep_going: &Arc<AtomicBool>,
    hot_store: Store,
    cold_db: Arc<ColdDB>,
    genesis_height: BlockHeight,
    epoch_manager: &EpochManagerHandle,
) {
    tracing::info!(target : "cold_store", "Starting the cold store loop");

    loop {
        if !keep_going.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!(target : "cold_store", "Stopping the cold store loop");
            break;
        }

        let instant = std::time::Instant::now();
        let result = cold_store_copy(
            &hot_store,
            cold_db.as_ref(),
            genesis_height,
            epoch_manager,
            split_storage_config.num_cold_store_read_threads,
        );
        let duration = instant.elapsed();

        let result_string = cold_store_copy_result_to_string(&result);
        metrics::COLD_STORE_COPY_RESULT.with_label_values(&[result_string]).inc();
        if duration > std::time::Duration::from_secs(1) {
            tracing::debug!(target : "cold_store", "cold_store_copy took {}s", duration.as_secs_f64());
        }

        let sleep_duration = split_storage_config.cold_store_loop_sleep_duration;
        match result {
            Err(err) => {
                tracing::error!(target : "cold_store", error = format!("{err:#?}"), "cold_store_copy failed");
                std::thread::sleep(sleep_duration.unsigned_abs());
            }
            // If no block was copied the cold head is up to date with final head and
            // this loop should sleep while waiting for a new block to get finalized.
            Ok(ColdStoreCopyResult::NoBlockCopied) => {
                std::thread::sleep(sleep_duration.unsigned_abs());
            }
            // The final head block was copied. There are no more blocks to be copied now
            // this loop should sleep while waiting for a new block to get finalized.
            Ok(ColdStoreCopyResult::LatestBlockCopied) => {
                std::thread::sleep(sleep_duration.unsigned_abs());
            }
            // A block older than the final head was copied. We should continue copying
            // until cold head reaches final head.
            Ok(ColdStoreCopyResult::OtherBlockCopied) => {
                continue;
            }
        }
    }
}

/// Spawns the cold store loop in a background thread and returns ColdStoreLoopHandle.
/// If cold store is not configured it does nothing and returns None.
/// The cold store loop is spawned in a rust native thread because it's quite heavy
/// and it is not suitable for async frameworks such as actix or tokio. It's not suitable
/// for async because RocksDB itself doesn't support async. Running this in an async
/// environment would just hog a thread while synchronously waiting for the IO operations
/// to finish.
pub fn spawn_cold_store_loop(
    config: &NearConfig,
    storage: &NodeStorage,
    epoch_manager: Arc<EpochManagerHandle>,
) -> anyhow::Result<Option<ColdStoreLoopHandle>> {
    if config.config.save_trie_changes != Some(true) {
        tracing::debug!(target:"cold_store", "Not spawning cold store because TrieChanges are not saved");
        return Ok(None);
    }

    let hot_store = storage.get_hot_store();
    let cold_db = match storage.cold_db() {
        Some(cold_db) => cold_db.clone(),
        None => {
            tracing::debug!(target : "cold_store", "Not spawning the cold store loop because cold store is not configured");
            return Ok(None);
        }
    };

    let genesis_height = config.genesis.config.genesis_height;
    let keep_going = Arc::new(AtomicBool::new(true));
    let keep_going_clone = keep_going.clone();

    // Perform the sanity check before spawning the thread.
    // If the check fails when the node is starting it's better to just fail
    // fast and crash the node immediately.
    sanity_check(&hot_store, cold_db.as_ref(), genesis_height)?;

    let split_storage_config = config.config.split_storage.clone().unwrap_or_default();

    tracing::info!(target : "cold_store", "Spawning the cold store loop");
    let join_handle =
        std::thread::Builder::new().name("cold_store_copy".to_string()).spawn(move || {
            cold_store_migration_loop(
                &split_storage_config,
                &keep_going_clone,
                genesis_height,
                &hot_store,
                cold_db.clone(),
            );
            cold_store_loop(
                &split_storage_config,
                &keep_going_clone,
                hot_store,
                cold_db,
                genesis_height,
                epoch_manager.as_ref(),
            )
        })?;

    Ok(Some(ColdStoreLoopHandle { join_handle, keep_going }))
}
