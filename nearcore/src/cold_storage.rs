use std::sync::{atomic::AtomicBool, Arc};

use near_chain::types::Tip;
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::{hash::CryptoHash, types::BlockHeight};
use near_store::cold_storage::{copy_all_data_to_cold, CopyAllDataToColdStatus};
use near_store::{
    cold_storage::{update_cold_db, update_cold_head},
    db::ColdDB,
    DBCol, NodeStorage, Store, FINAL_HEAD_KEY, HEAD_KEY, TAIL_KEY,
};

use crate::config::SplitStorageConfig;
use crate::{metrics, NearConfig};

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

/// Checks if cold store head is behind the final head and if so copies data
/// for the next available produced block after current cold store head.
/// Updates cold store head after.
fn cold_store_copy(
    hot_store: &Store,
    cold_store: &Store,
    cold_db: &Arc<ColdDB>,
    genesis_height: BlockHeight,
    epoch_manager: &EpochManagerHandle,
) -> anyhow::Result<ColdStoreCopyResult> {
    // If COLD_HEAD is not set for hot storage we default it to genesis_height.
    let cold_head = cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
    let cold_head_height = cold_head.map_or(genesis_height, |tip| tip.height);

    // If FINAL_HEAD is not set for hot storage we default it to genesis_height.
    let hot_final_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
    let hot_final_head_height = hot_final_head.map_or(genesis_height, |tip| tip.height);

    // If TAIL is not set for hot storage we default it to genesis_height.
    // TAIL not being set is not an error.
    // Archive dbs don't have TAIL, that means that they have all data from genesis_height.
    let hot_tail = hot_store.get_ser::<u64>(DBCol::BlockMisc, TAIL_KEY)?;
    let hot_tail_height = hot_tail.unwrap_or(genesis_height);

    tracing::debug!(target: "cold_store", "cold store loop, cold_head {}, hot_final_head {}, hot_tail {}", cold_head_height, hot_final_head_height, hot_tail_height);

    if cold_head_height > hot_final_head_height {
        return Err(anyhow::anyhow!(
            "Cold head is ahead of final head. cold head height: {} final head height {}",
            cold_head_height,
            hot_final_head_height
        ));
    }

    // Cold and Hot storages need to overlap.
    // Without this check we would skip blocks from cold_head_height to hot_tail_height.
    // This will result in corrupted cold storage.
    if cold_head_height < hot_tail_height {
        return Err(anyhow::anyhow!(
            "Cold head is behind hot tail. cold head height: {} hot tail height {}",
            cold_head_height,
            hot_tail_height
        ));
    }

    if cold_head_height >= hot_final_head_height {
        return Ok(ColdStoreCopyResult::NoBlockCopied);
    }

    // Here it should be sufficient to just read from hot storage.
    // Because BlockHeight is never garbage collectable and is not even copied to cold.
    let cold_head_hash =
        hot_store.get_ser::<CryptoHash>(DBCol::BlockHeight, &cold_head_height.to_le_bytes())?;
    let cold_head_hash = cold_head_hash
        .ok_or(anyhow::anyhow!("Failed to read the cold head hash at height {cold_head_height}"))?;

    // The previous block is the cold head so we can use it to get epoch id.
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&cold_head_hash)?;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;

    let mut next_height = cold_head_height + 1;
    while !update_cold_db(cold_db, hot_store, &shard_layout, &next_height)? {
        next_height += 1;
        if next_height > hot_final_head_height {
            return Err(anyhow::anyhow!(
                "All blocks between cold head and next height were skipped, but next height > hot final head. cold head {} next height to copy: {} final head height {}",
                cold_head_height,
                next_height,
                hot_final_head_height
            ));
        }
    }

    update_cold_head(cold_db, hot_store, &next_height)?;

    if next_height >= hot_final_head_height {
        Ok(ColdStoreCopyResult::LatestBlockCopied)
    } else {
        Ok(ColdStoreCopyResult::OtherBlockCopied)
    }
}

fn cold_store_copy_result_to_string(result: &anyhow::Result<ColdStoreCopyResult>) -> &str {
    match result {
        Err(_) => "error",
        Ok(ColdStoreCopyResult::NoBlockCopied) => "no_block_copied",
        Ok(ColdStoreCopyResult::LatestBlockCopied) => "latest_block_copied",
        Ok(ColdStoreCopyResult::OtherBlockCopied) => "other_block_copied",
    }
}

#[derive(Debug)]
enum ColdStoreInitialMigrationResult {
    /// Cold storage was already initialized
    NoNeedForMigration,
    /// Performed a successful cold storage migration
    SuccessfulMigration,
    /// Migration was interrupted by keep_going flag
    MigrationInterrupted,
}

/// This function performs initial population of cold storage if needed.
/// Migration can be interrupted via `keep_going` flag.
///
/// First, checks that hot store is of kind `Archive`. If not, no migration needed.
/// Then, captures hot final head BEFORE the migration, as migration is performed during normal neard run.
/// If hot final head is not set, returns Err.
/// Otherwise:
/// 1. performed migration
/// 2. updates head to saved hot final head
///
/// Any Ok status means that this function should not be retried:
/// - either migration was performed (now or earlier)
/// - or migration was interrupted, which means the `keep_going` flag was set to `false`
///   which means that everything cold store thread related has to stop
///
/// Error status means that for some reason migration cannot be performed.
fn cold_store_initial_migration(
    split_storage_config: &SplitStorageConfig,
    keep_going: &Arc<AtomicBool>,
    hot_store: &Store,
    cold_store: &Store,
    cold_db: &Arc<ColdDB>,
) -> anyhow::Result<ColdStoreInitialMigrationResult> {
    // We only need to perform the migration if hot store is of kind Archive and cold store doesn't have a head yet
    if hot_store.get_db_kind()? != Some(near_store::metadata::DbKind::Archive)
        || cold_store.get(DBCol::BlockMisc, HEAD_KEY)?.is_some()
    {
        return Ok(ColdStoreInitialMigrationResult::NoNeedForMigration);
    }

    tracing::info!(target: "cold_store", "Starting initial population of cold store");

    // If FINAL_HEAD is not set for hot storage something isn't right and we will probably fail in `update_cold_head`.
    // Let's fail early.
    let hot_final_head = hot_store
        .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
        .ok_or_else(|| anyhow::anyhow!("FINAL_HEAD not found in hot storage"))?;
    let hot_final_head_height = hot_final_head.height;

    let batch_size = split_storage_config.cold_store_initial_migration_batch_size;
    match copy_all_data_to_cold(cold_db.clone(), hot_store, batch_size, keep_going)? {
        CopyAllDataToColdStatus::EverythingCopied => {
            tracing::info!(target: "cold_store", "Initial population was successful, writing cold head of height {}", hot_final_head_height);
            update_cold_head(cold_db, hot_store, &hot_final_head_height)?;
            Ok(ColdStoreInitialMigrationResult::SuccessfulMigration)
        }
        CopyAllDataToColdStatus::Interrupted => {
            tracing::info!(target: "cold_store", "Initial population was interrupted");
            Ok(ColdStoreInitialMigrationResult::MigrationInterrupted)
        }
    }
}

/// Runs a loop that tries to copy all data from hot store to cold (do initial migration).
/// If migration fails sleeps for 30s and tries again.
/// If migration returned any successful status (including interruption status) breaks the loop.
fn cold_store_initial_migration_loop(
    split_storage_config: &SplitStorageConfig,
    keep_going: &Arc<AtomicBool>,
    hot_store: &Store,
    cold_store: &Store,
    cold_db: Arc<ColdDB>,
) {
    tracing::info!(target: "cold_store", "starting initial migration loop");
    loop {
        if !keep_going.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!(target: "cold_store", "stopping the initial migration loop");
            break;
        }
        match cold_store_initial_migration(
            split_storage_config,
            keep_going,
            hot_store,
            cold_store,
            &cold_db,
        ) {
            // We can either stop the cold store thread or hope that next time migration will not fail.
            // Here we pick the second option.
            Err(err) => {
                let dur = split_storage_config.cold_store_initial_migration_loop_sleep_duration;
                tracing::error!(target: "cold_store", "initial migration failed with error {}, sleeping {}s and trying again", err, dur.as_secs());
                std::thread::sleep(dur);
            }
            // Any Ok status from `cold_store_initial_migration` function means that we can proceed to regular run.
            Ok(status) => {
                tracing::info!(target: "cold_store", "Initial migration status: {:?}. Moving on.", status);
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
    cold_store: Store,
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
        let result =
            cold_store_copy(&hot_store, &cold_store, &cold_db, genesis_height, epoch_manager);

        metrics::COLD_STORE_COPY_RESULT
            .with_label_values(&[cold_store_copy_result_to_string(&result)])
            .inc();

        let sleep_duration = split_storage_config.cold_store_loop_sleep_duration;
        match result {
            Err(err) => {
                tracing::error!(target : "cold_store", error = format!("{err:#?}"), "cold_store_copy failed");
                std::thread::sleep(sleep_duration);
            }
            // If no block was copied the cold head is up to date with final head and
            // this loop should sleep while waiting for a new block to get finalized.
            Ok(ColdStoreCopyResult::NoBlockCopied) => {
                std::thread::sleep(sleep_duration);
            }
            // The final head block was copied. There are no more blocks to be copied now
            // this loop should sleep while waiting for a new block to get finalized.
            Ok(ColdStoreCopyResult::LatestBlockCopied) => {
                std::thread::sleep(sleep_duration);
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
    let cold_store = match storage.get_cold_store() {
        Some(cold_store) => cold_store,
        None => {
            tracing::debug!(target : "cold_store", "Not spawning the cold store loop because cold store is not configured");
            return Ok(None);
        }
    };
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

    let split_storage_config = config.config.split_storage.clone().unwrap_or_default();

    tracing::info!(target : "cold_store", "Spawning the cold store loop");
    let join_handle =
        std::thread::Builder::new().name("cold_store_copy".to_string()).spawn(move || {
            cold_store_initial_migration_loop(
                &split_storage_config,
                &keep_going_clone,
                &hot_store,
                &cold_store,
                cold_db.clone(),
            );
            cold_store_loop(
                &split_storage_config,
                &keep_going_clone,
                hot_store,
                cold_store,
                cold_db,
                genesis_height,
                epoch_manager.as_ref(),
            )
        })?;

    Ok(Some(ColdStoreLoopHandle { join_handle, keep_going }))
}
