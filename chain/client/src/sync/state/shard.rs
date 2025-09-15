use super::downloader::StateSyncDownloader;
use super::task_tracker::TaskTracker;
use crate::metrics;
use crate::sync::state::chain_requests::ChainFinalizationRequest;
use futures::{StreamExt, TryStreamExt};
use itertools::any;
use near_async::futures::{FutureSpawner, respawn_for_parallelism};
use near_async::messaging::AsyncSender;
use near_chain::BlockHeader;
use near_chain::types::RuntimeAdapter;
use near_client_primitives::types::ShardSyncStatus;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_o11y::span_wrapped_msg::{SpanWrapped, SpanWrappedMessageExt};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunk;
use near_primitives::state_part::{PartId, StatePart};
use near_primitives::state_sync::StatePartKey;
use near_primitives::types::{EpochId, ShardId};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolVersion};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::flat::{FlatStorageReadyStatus, FlatStorageStatus};
use near_store::{DBCol, ShardUId, Store};
use parking_lot::Mutex;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(super) struct StateSyncShardHandle {
    pub status: Arc<Mutex<ShardSyncStatus>>,
    pub result: oneshot::Receiver<Result<(), near_chain::Error>>,
    pub cancel: CancellationToken,
}

impl StateSyncShardHandle {
    pub fn status(&self) -> ShardSyncStatus {
        *self.status.lock()
    }
}

impl Drop for StateSyncShardHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

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
    chain_finalization_sender: AsyncSender<
        SpanWrapped<ChainFinalizationRequest>,
        Result<(), near_chain::Error>,
    >,
    cancel: CancellationToken,
    future_spawner: Arc<dyn FutureSpawner>,
    concurrency_limit: u8,
) -> Result<(), near_chain::Error> {
    tracing::info!("Running state sync for shard {}", shard_id);
    *status.lock() = ShardSyncStatus::StateDownloadHeader;
    let header = downloader.ensure_shard_header(shard_id, sync_hash, cancel.clone()).await?;
    let state_root = header.chunk_prev_state_root();
    let num_parts = header.num_state_parts();
    let block_header =
        store.get_ser::<BlockHeader>(DBCol::BlockHeader, sync_hash.as_bytes())?.ok_or_else(
            || near_chain::Error::DBNotFoundErr(format!("No block header {}", sync_hash)),
        )?;
    let epoch_id = *block_header.epoch_id();
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    let shard_uid = shard_id_to_uid(epoch_manager.as_ref(), shard_id, &epoch_id)?;
    metrics::STATE_SYNC_PARTS_TOTAL
        .with_label_values(&[&shard_id.to_string()])
        .set(num_parts as i64);

    return_if_cancelled!(cancel);
    *status.lock() = ShardSyncStatus::StateDownloadParts;
    let mut parts_to_download: Vec<u64> = (0..num_parts).collect();
    {
        // Peer selection is designed such that different nodes downloading the same part will tend
        // to send the requests to the same host. It allows the host to benefit from caching the part.
        //
        // At the start of an epoch, a number of nodes begin state sync at the same time. If we
        // don't randomize the order in which the parts are requested, the nodes will request the
        // parts in roughly the same order, producing spikes of traffic to the same hosts.
        let mut rng = thread_rng();
        parts_to_download.shuffle(&mut rng);
    }
    let mut attempt_count = 0;
    while !parts_to_download.is_empty() {
        return_if_cancelled!(cancel);
        let results = tokio_stream::iter(parts_to_download.clone())
            .map(|part_id| {
                let future = downloader.ensure_shard_part_downloaded_single_attempt(
                    shard_id,
                    sync_hash,
                    state_root,
                    num_parts,
                    part_id,
                    attempt_count,
                    cancel.clone(),
                    protocol_version,
                );
                respawn_for_parallelism(&*future_spawner, "state sync download part", future)
            })
            .buffered(concurrency_limit.into())
            .collect::<Vec<_>>()
            .await;
        attempt_count += 1;
        // Update the list of parts_to_download retaining only the ones that failed
        parts_to_download = results
            .iter()
            .enumerate()
            .filter_map(|(task_index, res)| {
                res.as_ref().err().map(|_| parts_to_download[task_index])
            })
            .collect();
    }

    return_if_cancelled!(cancel);
    *status.lock() = ShardSyncStatus::StateApplyInProgress;
    runtime.get_tries().unload_memtrie(&shard_uid);

    // Clear flat storage, but only if we haven't started applying parts yet.
    // (Otherwise we will delete the parts we already applied)
    let apply_parts_started = any(0..num_parts, |part_id| {
        let key = StatePartKey(sync_hash, shard_id, part_id);
        let key_bytes = borsh::to_vec(&key).unwrap();
        store.exists(DBCol::StatePartsApplied, &key_bytes).unwrap_or(false)
    });
    if apply_parts_started {
        tracing::debug!(target: "sync", ?shard_id, ?sync_hash, "Not clearing flat storage before applying state parts because some parts were already applied");
    } else {
        tracing::debug!(target: "sync", ?shard_id, ?sync_hash, "Clearing flat storage before applying state parts");
        let mut store_update = store.store_update();
        runtime
            .get_flat_storage_manager()
            .remove_flat_storage_for_shard(shard_uid, &mut store_update.flat_store_update())?;
        store_update.commit()?;
    }

    return_if_cancelled!(cancel);
    let _results = tokio_stream::iter(0..num_parts)
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
                protocol_version,
            );
            respawn_for_parallelism(&*future_spawner, "state sync apply part", future)
        })
        .buffer_unordered(concurrency_limit.into())
        .try_collect::<Vec<_>>()
        .await?;

    return_if_cancelled!(cancel);
    // Create flat storage, but only if we haven't done it already.
    // (Otherwise we will try to create flat storage second time and fail)
    let flat_storage_manager = runtime.get_flat_storage_manager();
    if flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_none() {
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
        let head_protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let shard_uids_pending_resharding = epoch_manager
            .get_shard_uids_pending_resharding(head_protocol_version, PROTOCOL_VERSION)?;
        handle.set_status("Loading memtrie");
        runtime.get_tries().load_memtrie_on_catchup(
            &shard_uid,
            &state_root,
            &shard_uids_pending_resharding,
        )?;
    }

    return_if_cancelled!(cancel);

    // Finalize; this needs to be done by the Chain.
    *status.lock() = ShardSyncStatus::StateApplyFinalizing;
    chain_finalization_sender
        .send_async(ChainFinalizationRequest { shard_id, sync_hash }.span_wrap())
        .await
        .map_err(|_| {
            near_chain::Error::Other("Chain finalization request could not be handled".to_owned())
        })??;

    *status.lock() = ShardSyncStatus::StateSyncDone;

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

#[derive(Debug, PartialEq, Eq)]
pub enum StatePartApplyResult {
    Applied,
    AlreadyApplied,
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
    protocol_version: ProtocolVersion,
) -> Result<StatePartApplyResult, near_chain::Error> {
    let key = StatePartKey(sync_hash, shard_id, part_id);
    let key_bytes = borsh::to_vec(&key).unwrap();
    let already_applied = store.exists(DBCol::StatePartsApplied, &key_bytes)?;
    if already_applied {
        tracing::debug!(target: "sync", ?key, "State part already applied, skipping");
        return Ok(StatePartApplyResult::AlreadyApplied);
    }
    return_if_cancelled!(cancel);
    let handle =
        computation_task_tracker.get_handle(&format!("shard {} part {}", shard_id, part_id)).await;
    return_if_cancelled!(cancel);
    handle.set_status("Loading part data from store");
    let bytes = store
        .get(DBCol::StateParts, &key_bytes)?
        .ok_or_else(|| {
            near_chain::Error::DBNotFoundErr(format!(
                "No state part {} for shard {}",
                part_id, shard_id
            ))
        })?
        .to_vec();
    let state_part = StatePart::from_bytes(bytes, protocol_version)?;
    handle.set_status("Applying part data to runtime");
    runtime.apply_state_part(
        shard_id,
        &state_root,
        PartId { idx: part_id, total: num_parts },
        &state_part,
        &epoch_id,
    )?;
    tracing::debug!(target: "sync", ?key, "Applied state part");

    // Mark part as applied.
    let mut store_update = store.store_update();
    store_update.set_ser(DBCol::StatePartsApplied, &key_bytes, &true)?;
    store_update.commit()?;

    Ok(StatePartApplyResult::Applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_chain_configs::Genesis;
    use near_epoch_manager::EpochManager;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::state::PartialState;
    use near_primitives::state_sync::StatePartKey;
    use near_primitives::types::EpochId;
    use near_store::genesis::initialize_genesis_state;
    use near_store::test_utils::create_test_store;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    fn create_dummy_state_part() -> StatePart {
        let dummy_trie_values =
            vec![Arc::from("test_value_1".as_bytes()), Arc::from("test_value_2".as_bytes())];
        let partial_state = PartialState::TrieValues(dummy_trie_values);
        StatePart::from_partial_state(partial_state, PROTOCOL_VERSION, 1)
    }

    fn create_test_runtime_and_store() -> (Arc<dyn RuntimeAdapter>, Store, TempDir) {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let store = create_test_store();
        let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

        initialize_genesis_state(store.clone(), &genesis, Some(tmp_dir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let runtime = near_chain::runtime::NightshadeRuntime::test(
            tmp_dir.path(),
            store.clone(),
            &genesis.config,
            epoch_manager,
        );

        (runtime, store, tmp_dir)
    }

    #[tokio::test]
    async fn test_apply_state() {
        let (runtime, store, _tmp_dir) = create_test_runtime_and_store();
        let task_tracker = TaskTracker::new(10);
        let cancel = CancellationToken::new();

        // Some arbitrary values for use in the test
        let sync_hash = CryptoHash::default();
        let shard_id = ShardLayout::single_shard().get_shard_id(0).unwrap();
        let part_id = 0;
        let num_parts = 1;
        let state_root = CryptoHash::default();
        let epoch_id = EpochId::default();

        // Create and store a state part
        let state_part = create_dummy_state_part();
        let key = StatePartKey(sync_hash, shard_id, part_id);
        let key_bytes = borsh::to_vec(&key).unwrap();
        let part_bytes = state_part.to_bytes(PROTOCOL_VERSION);

        let mut store_update = store.store_update();
        store_update.set(DBCol::StateParts, &key_bytes, &part_bytes);
        store_update.commit().unwrap();

        // Apply the state part for the first time
        let result = apply_state_part(
            store.clone(),
            runtime.clone(),
            task_tracker.clone(),
            cancel.clone(),
            sync_hash,
            shard_id,
            part_id,
            num_parts,
            state_root,
            epoch_id,
            PROTOCOL_VERSION,
        )
        .await
        .unwrap();

        // Should be applied
        assert_eq!(result, StatePartApplyResult::Applied);

        // Part should be marked as applied in store
        assert!(store.exists(DBCol::StatePartsApplied, &key_bytes).unwrap());

        // Try to apply the state part again
        let result = apply_state_part(
            store.clone(),
            runtime.clone(),
            task_tracker.clone(),
            cancel.clone(),
            sync_hash,
            shard_id,
            part_id,
            num_parts,
            state_root,
            epoch_id,
            PROTOCOL_VERSION,
        )
        .await
        .unwrap();

        // Should be skipped
        assert_eq!(result, StatePartApplyResult::AlreadyApplied);
    }
}
