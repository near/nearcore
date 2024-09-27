use super::downloader::StateSyncDownloader;
use super::task_tracker::TaskTracker;
use super::ChainFinalizationRequest;
use crate::metrics;
use crate::sync::state::util::query_epoch_id_and_height_for_block;
use futures::{StreamExt, TryStreamExt};
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_chain::types::RuntimeAdapter;
use near_chain::BlockHeader;
use near_client_primitives::types::ShardSyncStatus;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunk;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StatePartKey;
use near_primitives::types::{EpochId, ShardId};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::flat::{FlatStorageReadyStatus, FlatStorageStatus};
use near_store::{DBCol, ShardUId, Store};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(super) struct StateSyncShardHandle {
    pub status: Arc<Mutex<ShardSyncStatus>>,
    pub result: oneshot::Receiver<Result<(), near_chain::Error>>,
    pub cancel: CancellationToken,
}

impl StateSyncShardHandle {
    pub fn status(&self) -> ShardSyncStatus {
        *self.status.lock().unwrap()
    }
}

impl Drop for StateSyncShardHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// The maximum parallelism to use per shard. This is mostly for fairness, because
/// the actual rate limiting is done by the TaskTrackers, but this is useful for
/// balancing the shards a little.
const MAX_PARALLELISM_PER_SHARD_FOR_FAIRNESS: usize = 6;

macro_rules! return_if_cancelled {
    ($cancel:expr) => {
        if $cancel.is_cancelled() {
            return Err(near_chain::Error::Other("Cancelled".to_owned()));
        }
    };
}
pub(super) async fn run_state_sync_for_shard(
    store: Store,
    shard_id: ShardId,
    sync_hash: CryptoHash,
    downloader: Arc<StateSyncDownloader>,
    runtime: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    computation_task_tracker: TaskTracker,
    status: Arc<Mutex<ShardSyncStatus>>,
    chain_finalization_queue: UnboundedSender<ChainFinalizationRequest>,
    cancel: CancellationToken,
    future_spawner: Arc<dyn FutureSpawner>,
) -> Result<(), near_chain::Error> {
    tracing::info!("Running state sync for shard {}", shard_id);
    *status.lock().unwrap() = ShardSyncStatus::StateDownloadHeader;
    let header = downloader.ensure_shard_header(shard_id, sync_hash, cancel.clone()).await?;
    let state_root = header.chunk_prev_state_root();
    let num_parts = header.num_state_parts();
    let block_header =
        store.get_ser::<BlockHeader>(DBCol::BlockHeader, sync_hash.as_bytes())?.ok_or_else(
            || near_chain::Error::DBNotFoundErr(format!("No block header {}", sync_hash)),
        )?;
    let epoch_id = *block_header.epoch_id();
    let shard_uid = epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
    metrics::STATE_SYNC_PARTS_TOTAL
        .with_label_values(&[&shard_id.to_string()])
        .set(num_parts as i64);

    return_if_cancelled!(cancel);
    *status.lock().unwrap() = ShardSyncStatus::StateDownloadParts;
    tokio_stream::iter(0..num_parts)
        .map(|part_id| {
            let future = downloader.ensure_shard_part_downloaded(
                shard_id,
                sync_hash,
                part_id,
                header.clone(),
                cancel.clone(),
            );
            respawn_for_parallelism(&*future_spawner, "state sync download part", future)
        })
        .buffer_unordered(MAX_PARALLELISM_PER_SHARD_FOR_FAIRNESS)
        .try_collect::<Vec<_>>()
        .await?;

    return_if_cancelled!(cancel);
    *status.lock().unwrap() = ShardSyncStatus::StateApplyInProgress;
    runtime.get_tries().unload_mem_trie(&shard_uid);
    let mut store_update = store.store_update();
    runtime
        .get_flat_storage_manager()
        .remove_flat_storage_for_shard(shard_uid, &mut store_update.flat_store_update())?;
    store_update.commit()?;

    return_if_cancelled!(cancel);
    tokio_stream::iter(0..num_parts)
        .map(|part_id| {
            let store = store.clone();
            let runtime = runtime.clone();
            let computation_task_tracker = computation_task_tracker.clone();
            let cancel = cancel.clone();
            let future = apply_state_part(
                store,
                runtime,
                computation_task_tracker,
                cancel,
                sync_hash,
                shard_id,
                part_id,
                num_parts,
                state_root,
                epoch_id,
            );
            respawn_for_parallelism(&*future_spawner, "state sync apply part", future)
        })
        .buffer_unordered(MAX_PARALLELISM_PER_SHARD_FOR_FAIRNESS)
        .try_collect::<Vec<_>>()
        .await?;

    return_if_cancelled!(cancel);
    // Create flat storage.
    {
        let (epoch_id, _) = query_epoch_id_and_height_for_block(&store, sync_hash)?;
        let shard_uid = epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
        let chunk = header.cloned_chunk();
        let block_hash = chunk.prev_block();

        // We synced shard state on top of _previous_ block for chunk in shard state header and applied state parts to
        // flat storage. Now we can set flat head to hash of this block and create flat storage.
        // If block_hash is equal to default - this means that we're all the way back at genesis.
        // So we don't have to add the storage state for shard in such case.
        // TODO(8438) - add additional test scenarios for this case.
        if *block_hash != CryptoHash::default() {
            create_flat_storage_for_shard(&store, &*runtime, shard_uid, &chunk)?;
        }
    }
    return_if_cancelled!(cancel);
    // Load memtrie.
    {
        let handle = computation_task_tracker.get_handle(&format!("shard {}", shard_id)).await;
        handle.set_status("Loading memtrie");
        runtime.get_tries().load_mem_trie_on_catchup(&shard_uid, &state_root)?;
    }

    return_if_cancelled!(cancel);

    // Finalize; this needs to be done by the Chain.
    *status.lock().unwrap() = ShardSyncStatus::StateApplyFinalizing;
    let (response_sender, response_receiver) = oneshot::channel();
    chain_finalization_queue
        .send(ChainFinalizationRequest { shard_id, sync_hash, response_sender })
        .map_err(|_| near_chain::Error::Other("Chain finalization queue closed".to_owned()))?;
    response_receiver.await.map_err(|_| {
        near_chain::Error::Other("Chain finalization response dropped".to_owned())
    })??;

    *status.lock().unwrap() = ShardSyncStatus::StateSyncDone;

    Ok(())
}

fn create_flat_storage_for_shard(
    store: &Store,
    runtime: &dyn RuntimeAdapter,
    shard_uid: ShardUId,
    chunk: &ShardChunk,
) -> Result<(), near_chain::Error> {
    let flat_storage_manager = runtime.get_flat_storage_manager();
    // Flat storage must not exist at this point because leftover keys corrupt its state.
    assert!(flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_none());

    let flat_head_hash = *chunk.prev_block();
    let flat_head_header =
        store.get_ser::<BlockHeader>(DBCol::BlockHeader, flat_head_hash.as_bytes())?.ok_or_else(
            || near_chain::Error::DBNotFoundErr(format!("No block header {}", flat_head_hash)),
        )?;
    let flat_head_prev_hash = *flat_head_header.prev_hash();
    let flat_head_height = flat_head_header.height();

    tracing::debug!(target: "store", ?shard_uid, ?flat_head_hash, flat_head_height, "set_state_finalize - initialized flat storage");

    let mut store_update = store.flat_store().store_update();
    store_update.set_flat_storage_status(
        shard_uid,
        FlatStorageStatus::Ready(FlatStorageReadyStatus {
            flat_head: near_store::flat::BlockInfo {
                hash: flat_head_hash,
                prev_hash: flat_head_prev_hash,
                height: flat_head_height,
            },
        }),
    );
    store_update.commit()?;
    flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
    Ok(())
}

async fn apply_state_part(
    store: Store,
    runtime: Arc<dyn RuntimeAdapter>,
    computation_task_tracker: TaskTracker,
    cancel: CancellationToken,
    sync_hash: CryptoHash,
    shard_id: ShardId,
    part_id: u64,
    num_parts: u64,
    state_root: CryptoHash,
    epoch_id: EpochId,
) -> anyhow::Result<(), near_chain::Error> {
    return_if_cancelled!(cancel);
    let handle =
        computation_task_tracker.get_handle(&format!("shard {} part {}", shard_id, part_id)).await;
    return_if_cancelled!(cancel);
    handle.set_status("Loading part data from store");
    let data = store
        .get(
            DBCol::StateParts,
            &borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id)).unwrap(),
        )?
        .ok_or_else(|| {
            near_chain::Error::DBNotFoundErr(format!(
                "No state part {} for shard {}",
                part_id, shard_id
            ))
        })?
        .to_vec();
    handle.set_status("Applying part data to runtime");
    runtime.apply_state_part(
        shard_id,
        &state_root,
        PartId { idx: part_id, total: num_parts },
        &data,
        &epoch_id,
    )?;
    Ok(())
}

/// Given a future, respawn it as an equivalent future but which does not block the
/// driver of the future. For example, if the given future directly performs
/// computation, normally the whoever drives the future (such as a buffered_unordered)
/// would be blocked by the computation, thereby not allowing computation of other
/// futures driven by the same driver to proceed. This function respawns the future
/// onto the FutureSpawner, so the driver of the returned future would not be blocked.
fn respawn_for_parallelism<T: Send + 'static>(
    future_spawner: &dyn FutureSpawner,
    name: &'static str,
    f: impl std::future::Future<Output = T> + Send + 'static,
) -> impl std::future::Future<Output = T> + Send + 'static {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    future_spawner.spawn(name, async move {
        sender.send(f.await).ok();
    });
    async move { receiver.await.unwrap() }
}
