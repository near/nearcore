use std::sync::{atomic::AtomicBool, Arc};

use near_chain::types::Tip;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{hash::CryptoHash, types::BlockHeight};
use near_store::{
    cold_storage::{update_cold_db, update_cold_head},
    db::ColdDB,
    DBCol, NodeStorage, Store, FINAL_HEAD_KEY, HEAD_KEY,
};

use crate::{NearConfig, NightshadeRuntime};

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

/// The status of the cold store loop indicates if the loop should sleep before
/// the next copy attempt or if it should actively continue copying.
enum ColdStoreLoopStatus {
    Sleep,
    Continue,
}

// Checks if cold store head is behind the final head and if so copies data
// at height equal to the cold store head height + 1.
fn cold_store_copy(
    hot_store: &Store,
    cold_store: &Store,
    cold_db: &Arc<ColdDB>,
    genesis_height: BlockHeight,
    runtime: &Arc<NightshadeRuntime>,
) -> anyhow::Result<ColdStoreLoopStatus> {
    let cold_head = cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
    let cold_head_height = match cold_head {
        Some(cold_head) => cold_head.height,
        None => genesis_height,
    };

    // If FINAL_HEAD is not set for hot storage though, we default it to 0.
    let hot_final_head = hot_store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;
    let hot_final_head_height = match hot_final_head {
        Some(hot_final_head) => hot_final_head.height,
        None => genesis_height,
    };

    tracing::debug!(target: "cold_store", "cold store loop, cold_head {}, hot_final_head {}", cold_head_height, hot_final_head_height);

    if cold_head_height > hot_final_head_height {
        tracing::error!(target: "cold_store", "Error, cold head is ahead of final head, this should never take place!");
    }

    if cold_head_height >= hot_final_head_height {
        return Ok(ColdStoreLoopStatus::Sleep);
    }

    let next_height = cold_head_height + 1;

    // Here it should be sufficient to just read from hot storage.
    // Because BlockHeight is never garbage collectable and is not even copied to cold.
    let cold_head_hash =
        hot_store.get_ser::<CryptoHash>(DBCol::BlockHeight, &cold_head_height.to_le_bytes())?;
    let cold_head_hash = match cold_head_hash {
        Some(cold_head_hash) => cold_head_hash,
        None => return Err(anyhow::anyhow!("Failed to read the cold head hash")),
    };

    // The previous block is the cold head so we can use it to get epoch id.
    let epoch_id = &runtime.get_epoch_id_from_prev_block(&cold_head_hash)?;
    let shard_layout = runtime.get_shard_layout(epoch_id)?;

    update_cold_db(cold_db, hot_store, &shard_layout, &next_height)?;

    update_cold_head(cold_db, hot_store, &next_height)?;

    if next_height >= hot_final_head_height {
        Ok(ColdStoreLoopStatus::Sleep)
    } else {
        Ok(ColdStoreLoopStatus::Continue)
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

    // TODO - is there a better way to schedule a function to run periodically?
    // NOTE - sleeping here, if spawned in rayon, would likely hog the thread forever
    // which is why this method is spawned in rust native thread. The hope is that
    // the OS will correctly handle this thread sleeping and return the CPU time to
    // other threads.
    loop {
        if !keep_going.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::debug!(target: "cold_store", "stopping the cold store loop");
            break;
        }
        let status = cold_store_copy(&hot_store, &cold_store, &cold_db, genesis_height, &runtime);

        match status {
            Err(err) => {
                tracing::error!(target:"cold_store", "cold_store_copy failed with error : {err}");
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            Ok(ColdStoreLoopStatus::Sleep) => {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            Ok(ColdStoreLoopStatus::Continue) => {
                continue;
            }
        }
    }
}

/// Spawns the cold store loop in a background thread and returns ColdStoreLoopHandle.
/// If cold store is not configured it does nothing and returns None.
pub fn spawn_cold_store_loop(
    config: &NearConfig,
    storage: &NodeStorage,
    runtime: Arc<NightshadeRuntime>,
) -> anyhow::Result<Option<ColdStoreLoopHandle>> {
    tracing::info!("tracing info spawn cold store loop");
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
