use std::sync::{atomic::AtomicBool, Arc};

use near_chain::types::Tip;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{hash::CryptoHash, types::BlockHeight};
use near_store::{
    cold_storage::{update_cold_db, update_cold_head},
    db::ColdDB,
    DBCol, NodeStorage, Store, FINAL_HEAD_KEY, HEAD_KEY,
};

use crate::{metrics, NearConfig, NightshadeRuntime};

/// A handle that keeps the state of the cold store loop and can be used to stop it.
pub struct ColdStoreLoopHandle {
    join_handle: std::thread::JoinHandle<()>,
    keep_going: Arc<AtomicBool>,
}

impl ColdStoreLoopHandle {
    pub fn stop(self) {
        self.keep_going.store(false, std::sync::atomic::Ordering::SeqCst);
        match self.join_handle.join() {
            Ok(_) => {
                tracing::debug!(target:"cold_store", "Joined the cold store loop thread");
            }
            Err(_) => {
                tracing::error!(target:"cold_store", "Failed to join the cold store loop thread");
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
    runtime: &Arc<NightshadeRuntime>,
) -> anyhow::Result<ColdStoreCopyResult> {
    // If COLD_HEAD is not set for hot storage we default it to genesis_height.
    let cold_head = cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
    let cold_head_height = cold_head.map_or(genesis_height, |tip| tip.height);

    // If FINAL_HEAD is not set for hot storage we default it to genesis_height.
    let hot_final_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
    let hot_final_head_height = hot_final_head.map_or(genesis_height, |tip| tip.height);

    tracing::debug!(target: "cold_store", "cold store loop, cold_head {}, hot_final_head {}", cold_head_height, hot_final_head_height);

    if cold_head_height > hot_final_head_height {
        return Err(anyhow::anyhow!(
            "Cold head is ahead of final head. cold head height: {} final head height {}",
            cold_head_height,
            hot_final_head_height
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
    let epoch_id = &runtime.get_epoch_id_from_prev_block(&cold_head_hash)?;
    let shard_layout = runtime.get_shard_layout(epoch_id)?;

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

// This method will copy data from hot storage to cold storage in a loop.
// It will try to copy blocks as fast as possible up until cold head = final head.
// Once the cold head reaches the final head it will sleep for one second before
// trying to copy data at the next height.
// TODO clean up the interface, currently we need to pass hot store, cold store and
// cold_db which is redundant.
fn cold_store_loop(
    keep_going: Arc<AtomicBool>,
    hot_store: Store,
    cold_store: Store,
    cold_db: Arc<ColdDB>,
    genesis_height: BlockHeight,
    runtime: Arc<NightshadeRuntime>,
) {
    tracing::info!(target: "cold_store", "starting cold store loop");

    loop {
        if !keep_going.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::debug!(target: "cold_store", "stopping the cold store loop");
            break;
        }
        let result = cold_store_copy(&hot_store, &cold_store, &cold_db, genesis_height, &runtime);

        metrics::COLD_STORE_COPY_RESULT
            .with_label_values(&[cold_store_copy_result_to_string(&result)])
            .inc();

        let sleep_duration = std::time::Duration::from_secs(1);
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
    runtime: Arc<NightshadeRuntime>,
) -> anyhow::Result<Option<ColdStoreLoopHandle>> {
    let hot_store = storage.get_hot_store();
    let cold_store = match storage.get_cold_store() {
        Some(cold_store) => cold_store,
        None => {
            tracing::debug!(target:"cold_store", "Not spawning cold store loop because cold store is not configured");
            return Ok(None);
        }
    };
    let cold_db = match storage.cold_db() {
        Some(cold_db) => cold_db.clone(),
        None => {
            tracing::debug!(target:"cold_store", "Not spawning cold store loop because cold store is not configured");
            return Ok(None);
        }
    };

    let genesis_height = config.genesis.config.genesis_height;
    let keep_going = Arc::new(AtomicBool::new(true));
    let keep_going_clone = keep_going.clone();

    tracing::info!(target:"cold_store", "Spawning cold store loop");
    let join_handle =
        std::thread::Builder::new().name("cold_store_loop".to_string()).spawn(move || {
            cold_store_loop(
                keep_going_clone,
                hot_store,
                cold_store,
                cold_db,
                genesis_height,
                runtime,
            )
        })?;

    Ok(Some(ColdStoreLoopHandle { join_handle, keep_going }))
}
