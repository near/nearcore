/// Implementation for all resharding logic.
/// ReshardingRequest and ReshardingResponse are exchanged across the client_actor and SyncJobsActor.
/// build_state_for_split_shards_preprocessing and build_state_for_split_shards_postprocessing are handled
/// by the client_actor while the heavy resharding build_state_for_split_shards is done by SyncJobsActor
/// so as to not affect client.
use crate::metrics::{
    ReshardingStatus, RESHARDING_BATCH_APPLY_TIME, RESHARDING_BATCH_COMMIT_TIME,
    RESHARDING_BATCH_COUNT, RESHARDING_BATCH_PREPARE_TIME, RESHARDING_BATCH_SIZE,
    RESHARDING_STATUS,
};
use crate::Chain;
use itertools::Itertools;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::error::Error;
use near_primitives::errors::StorageError::StorageInconsistentState;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::state::FlatStateValue;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ShardId, StateRoot};
use near_store::flat::{
    store_helper, BlockInfo, FlatStorageError, FlatStorageManager, FlatStorageReadyStatus,
    FlatStorageStatus,
};
use near_store::resharding::{get_delayed_receipts, get_promise_yield_timeouts};
use near_store::trie::SnapshotError;
use near_store::{ShardTries, ShardUId, StorageError, Store, Trie, TrieDBStorage, TrieStorage};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

/// ReshardingRequest has all the information needed to start a resharding job. This message is sent
/// from ClientActor to SyncJobsActor. We do not want to stall the ClientActor with a long running
/// resharding job. The SyncJobsActor is helpful for handling such long running jobs.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct ReshardingRequest {
    pub tries: Arc<ShardTries>,
    // The block hash of the first block of the epoch.
    pub sync_hash: CryptoHash,
    // The prev hash of the sync_hash. We want the state at that block hash.
    pub prev_hash: CryptoHash,
    // The prev prev hash of the sync_hash. The state snapshot should be saved at that block hash.
    pub prev_prev_hash: CryptoHash,
    // Parent shardUId to be split into child shards.
    pub shard_uid: ShardUId,
    // The state root of the parent ShardUId. This is different from block sync_hash
    pub state_root: StateRoot,
    // The shard layout in the next epoch.
    pub next_epoch_shard_layout: ShardLayout,
    // Time we've spent polling for the state snapshot to be ready. We autofail after a certain time.
    pub curr_poll_time: Duration,
    // Configuration for resharding. Can be used to throttle resharding if needed.
    pub config: MutableConfigValue<ReshardingConfig>,
    // A handle that allows the main process to interrupt resharding if needed.
    // This typically happens when the main process is interrupted.
    pub handle: ReshardingHandle,
}

// Skip `runtime_adapter`, because it's a complex object that has complex logic
// and many fields.
impl Debug for ReshardingRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReshardingRequest")
            .field("tries", &"<not shown>")
            .field("sync_hash", &self.sync_hash)
            .field("prev_hash", &self.prev_hash)
            .field("prev_prev_hash", &self.prev_prev_hash)
            .field("shard_uid", &self.shard_uid)
            .field("state_root", &self.state_root)
            .field("next_epoch_shard_layout_version", &self.next_epoch_shard_layout.version())
            .field("curr_poll_time", &self.curr_poll_time)
            .finish()
    }
}

// ReshardingResponse is the response sent from SyncJobsActor to ClientActor once resharding is completed.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ReshardingResponse {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
    pub new_state_roots: Result<HashMap<ShardUId, StateRoot>, Error>,
}

fn get_checked_account_id_to_shard_uid_fn(
    shard_uid: ShardUId,
    new_shards: Vec<ShardUId>,
    next_epoch_shard_layout: ShardLayout,
) -> impl Fn(&AccountId) -> ShardUId {
    let split_shard_ids: HashSet<_> = new_shards.into_iter().collect();
    move |account_id: &AccountId| {
        let new_shard_uid = account_id_to_shard_uid(account_id, &next_epoch_shard_layout);
        // check that all accounts in the shard are mapped the shards that this shard will split
        // to according to shard layout
        assert!(
            split_shard_ids.contains(&new_shard_uid),
            "Inconsistent shard_layout specs. Account {:?} in shard {:?} and in shard {:?}, but the former is not parent shard for the latter",
            account_id,
            shard_uid,
            new_shard_uid,
        );
        new_shard_uid
    }
}

// Format of the trie key, value pair that is used in tries.add_values_to_children_states() function
type TrieEntry = (Vec<u8>, Option<Vec<u8>>);

struct TrieUpdateBatch {
    entries: Vec<TrieEntry>,
    size: u64,
}

// Function to return batches of trie key, value pairs from flat storage iter. We return None at the end of iter.
// The batch size is roughly batch_memory_limit.
fn get_trie_update_batch(
    config: &ReshardingConfig,
    iter: &mut impl Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>), FlatStorageError>>,
) -> Result<Option<TrieUpdateBatch>, FlatStorageError> {
    let mut size: u64 = 0;
    let mut entries = Vec::new();
    while let Some(item) = iter.next() {
        let (key, value) = item?;
        size += key.len() as u64 + value.as_ref().map_or(0, |v| v.len() as u64);
        entries.push((key, value));
        if size > config.batch_size.as_u64() {
            break;
        }
    }
    if entries.is_empty() {
        Ok(None)
    } else {
        Ok(Some(TrieUpdateBatch { entries, size }))
    }
}

fn apply_delayed_receipts<'a>(
    config: &ReshardingConfig,
    tries: &ShardTries,
    orig_shard_uid: ShardUId,
    orig_state_root: StateRoot,
    state_roots: HashMap<ShardUId, StateRoot>,
    account_id_to_shard_uid: &(dyn Fn(&AccountId) -> ShardUId + 'a),
) -> Result<HashMap<ShardUId, StateRoot>, Error> {
    let mut total_count = 0;
    let orig_trie_update = tries.new_trie_update_view(orig_shard_uid, orig_state_root);

    let mut start_index = None;
    let mut new_state_roots = state_roots;
    while let Some((next_index, receipts)) =
        get_delayed_receipts(&orig_trie_update, start_index, config.batch_size)?
    {
        total_count += receipts.len() as u64;
        let (store_update, updated_state_roots) = tries.apply_delayed_receipts_to_children_states(
            &new_state_roots,
            &receipts,
            account_id_to_shard_uid,
        )?;
        new_state_roots = updated_state_roots;
        start_index = Some(next_index);
        store_update.commit()?;
    }

    tracing::debug!(target: "resharding", ?orig_shard_uid, ?total_count, "Applied delayed receipts");
    Ok(new_state_roots)
}

fn apply_promise_yield_timeouts<'a>(
    config: &ReshardingConfig,
    tries: &ShardTries,
    orig_shard_uid: ShardUId,
    orig_state_root: StateRoot,
    state_roots: HashMap<ShardUId, StateRoot>,
    account_id_to_shard_uid: &(dyn Fn(&AccountId) -> ShardUId + 'a),
) -> Result<HashMap<ShardUId, StateRoot>, Error> {
    let mut total_count = 0;
    let orig_trie_update = tries.new_trie_update_view(orig_shard_uid, orig_state_root);

    let mut start_index = None;
    let mut new_state_roots = state_roots;
    while let Some((next_index, timeouts)) =
        get_promise_yield_timeouts(&orig_trie_update, start_index, config.batch_size)?
    {
        total_count += timeouts.len() as u64;
        let (store_update, updated_state_roots) = tries
            .apply_promise_yield_timeouts_to_children_states(
                &new_state_roots,
                &timeouts,
                account_id_to_shard_uid,
            )?;
        new_state_roots = updated_state_roots;
        start_index = Some(next_index);
        store_update.commit()?;
    }

    tracing::debug!(target: "resharding", ?orig_shard_uid, ?total_count, "Applied PromiseYield timeouts");
    Ok(new_state_roots)
}

// function to set up flat storage status to Ready after a resharding event
// TODO(resharding) : Consolidate this with setting up flat storage during state sync logic
fn set_flat_storage_state(
    store: Store,
    flat_storage_manager: &FlatStorageManager,
    shard_uid: ShardUId,
    block_info: BlockInfo,
) -> Result<(), Error> {
    let mut store_update = store.store_update();
    store_helper::set_flat_storage_status(
        &mut store_update,
        shard_uid,
        FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: block_info }),
    );
    store_update.commit()?;
    flat_storage_manager.create_flat_storage_for_shard(shard_uid)?;
    Ok(())
}

/// Helper function to read the value from flat storage.
/// It either returns the inlined value or reads ref value from the storage.
fn read_flat_state_value(
    trie_storage: &TrieDBStorage,
    flat_state_value: FlatStateValue,
) -> Vec<u8> {
    match flat_state_value {
        FlatStateValue::Ref(val) => trie_storage.retrieve_raw_bytes(&val.hash).unwrap().to_vec(),
        FlatStateValue::Inlined(val) => val,
    }
}

impl Chain {
    pub fn build_state_for_resharding_preprocessing(
        &self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        resharding_scheduler: &near_async::messaging::Sender<ReshardingRequest>,
    ) -> Result<(), Error> {
        tracing::debug!(target: "resharding", ?shard_id, ?sync_hash, "preprocessing started");
        let block_header = self.get_block_header(sync_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
        let next_epoch_shard_layout =
            self.epoch_manager.get_shard_layout(block_header.next_epoch_id())?;
        assert_ne!(shard_layout, next_epoch_shard_layout);

        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let prev_hash = block_header.prev_hash();
        let prev_block_header = self.get_block_header(prev_hash)?;
        let prev_prev_hash = prev_block_header.prev_hash();
        let state_root = *self.get_chunk_extra(&prev_hash, &shard_uid)?.state_root();

        resharding_scheduler.send(ReshardingRequest {
            tries: Arc::new(self.runtime_adapter.get_tries()),
            sync_hash: *sync_hash,
            prev_hash: *prev_hash,
            prev_prev_hash: *prev_prev_hash,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
            curr_poll_time: Duration::ZERO,
            config: self.resharding_config.clone(),
            handle: self.resharding_handle.clone(),
        });

        RESHARDING_STATUS
            .with_label_values(&[&shard_uid.to_string()])
            .set(ReshardingStatus::Scheduled.into());
        Ok(())
    }

    /// Function to check whether the snapshot is ready for resharding or not. We return true if the snapshot is not
    /// ready and we need to retry/reschedule the resharding job.
    pub fn retry_build_state_for_split_shards(resharding_request: &ReshardingRequest) -> bool {
        let ReshardingRequest { tries, prev_prev_hash, curr_poll_time, config, .. } =
            resharding_request;
        let config = config.get();

        // Do not retry if we have spent more than max_poll_time
        // The error would be caught in build_state_for_split_shards and propagated to client actor
        if curr_poll_time > &config.max_poll_time {
            tracing::warn!(target: "resharding", ?curr_poll_time, ?config.max_poll_time, "exceeded max poll time while waiting for snapshot");
            return false;
        }

        let state_snapshot = tries.get_state_snapshot(prev_prev_hash);
        if let Err(err) = state_snapshot {
            tracing::debug!(target: "resharding", ?err, "state snapshot is not ready");
            return match err {
                SnapshotError::SnapshotNotFound(_) => true,
                SnapshotError::LockWouldBlock => true,
                SnapshotError::IncorrectSnapshotRequested(_, _) => false,
                SnapshotError::Other(_) => false,
            };
        }

        // The snapshot is Ok, no need to retry.
        return false;
    }

    pub fn build_state_for_split_shards(
        resharding_request: ReshardingRequest,
    ) -> ReshardingResponse {
        let shard_uid = resharding_request.shard_uid;
        let shard_id = shard_uid.shard_id();
        let sync_hash = resharding_request.sync_hash;
        let new_state_roots = Self::build_state_for_split_shards_impl(resharding_request);
        match &new_state_roots {
            Ok(_) => {}
            Err(err) => {
                tracing::error!(target: "resharding", ?shard_uid, ?err, "Resharding failed, manual recovery is necessary!");
                RESHARDING_STATUS
                    .with_label_values(&[&shard_uid.to_string()])
                    .set(ReshardingStatus::Failed.into());
            }
        }
        ReshardingResponse { shard_id, sync_hash, new_state_roots }
    }

    fn build_state_for_split_shards_impl(
        resharding_request: ReshardingRequest,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let ReshardingRequest {
            tries,
            prev_hash,
            prev_prev_hash,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
            config,
            handle,
            ..
        } = resharding_request;
        tracing::debug!(target: "resharding", config=?config.get(), ?shard_uid, "build_state_for_split_shards_impl starting");

        let shard_id = shard_uid.shard_id();
        let new_shards = next_epoch_shard_layout
            .get_children_shards_uids(shard_id)
            .ok_or(Error::InvalidShardId(shard_id))?;
        let mut state_roots: HashMap<_, _> =
            new_shards.iter().map(|shard_uid| (*shard_uid, Trie::EMPTY_ROOT)).collect();

        RESHARDING_STATUS
            .with_label_values(&[&shard_uid.to_string()])
            .set(ReshardingStatus::BuildingState.into());

        // Build the required iterator from flat storage and delta changes. Note that we are
        // working with iterators as we don't want to have all the state in memory at once.
        //
        // Iterator is built by chaining the following:
        // 1. Flat storage iterator from the snapshot state as of `prev_prev_hash`.
        // 2. Delta changes iterator from the snapshot state as of `prev_hash`.
        //
        // The snapshot when created has the flat head as of `prev_prev_hash`, i.e. the hash as
        // of the second last block of the previous epoch. Hence we need to append the detla
        // changes on top of it.
        let (snapshot_store, flat_storage_manager) = tries
            .get_state_snapshot(&prev_prev_hash)
            .map_err(|err| StorageInconsistentState(err.to_string()))?;
        let flat_storage_chunk_view = flat_storage_manager.chunk_view(shard_uid, prev_prev_hash);
        let flat_storage_chunk_view = flat_storage_chunk_view.ok_or_else(|| {
            StorageInconsistentState("Chunk view missing for snapshot flat storage".to_string())
        })?;
        // Get the flat storage iter and wrap the value in Optional::Some to
        // match the delta iterator so that they can be chained.
        let flat_storage_iter = flat_storage_chunk_view.iter_flat_state_entries(None, None);
        let flat_storage_iter = flat_storage_iter.map_ok(|(key, value)| (key, Some(value)));

        // Get the delta iter and wrap the items in Result to match the flat
        // storage iter so that they can be chained.
        let delta = store_helper::get_delta_changes(&snapshot_store, shard_uid, prev_hash)
            .map_err(|err| StorageInconsistentState(err.to_string()))?;
        let delta = delta.ok_or_else(|| {
            StorageInconsistentState("Delta missing for snapshot flat storage".to_string())
        })?;
        let delta_iter = delta.0.into_iter();
        let delta_iter = delta_iter.map(|item| Ok(item));

        // chain the flat storage and flat storage delta iterators
        let iter = flat_storage_iter.chain(delta_iter);

        // map the iterator to read the values
        let trie_storage = TrieDBStorage::new(tries.get_store(), shard_uid);
        let iter = iter.map_ok(move |(key, value)| {
            (key, value.map(|value| read_flat_state_value(&trie_storage, value)))
        });

        // function to map account id to shard uid in range of child shards
        let checked_account_id_to_shard_uid =
            get_checked_account_id_to_shard_uid_fn(shard_uid, new_shards, next_epoch_shard_layout);

        let shard_uid_string = shard_uid.to_string();
        let metrics_labels = [shard_uid_string.as_str()];

        // Once we build the iterator, we break it into batches using the get_trie_update_batch function.
        let mut iter = iter;
        loop {
            if !handle.get() {
                // The keep_going is set to false, interrupt processing.
                tracing::info!(target: "resharding", ?shard_uid, "build_state_for_split_shards_impl interrupted");
                return Err(Error::Other("Resharding interrupted.".to_string()));
            }
            // Prepare the batch.
            let batch = {
                let histogram = RESHARDING_BATCH_PREPARE_TIME.with_label_values(&metrics_labels);
                let _timer = histogram.start_timer();
                let batch = get_trie_update_batch(&config.get(), &mut iter);
                let batch = batch.map_err(Into::<StorageError>::into)?;
                let Some(batch) = batch else { break };
                batch
            };

            // Apply the batch - add values to the children shards.
            let TrieUpdateBatch { entries, size } = batch;
            let store_update = {
                let histogram = RESHARDING_BATCH_APPLY_TIME.with_label_values(&metrics_labels);
                let _timer = histogram.start_timer();
                // TODO(#9435): This is highly inefficient as for each key in the batch, we are parsing the account_id
                // A better way would be to use the boundary account to construct the from and to key range for flat storage iterator
                let (store_update, new_state_roots) = tries.add_values_to_children_states(
                    &state_roots,
                    entries,
                    &checked_account_id_to_shard_uid,
                )?;
                state_roots = new_state_roots;
                store_update
            };

            // Commit the store update.
            {
                let histogram = RESHARDING_BATCH_COMMIT_TIME.with_label_values(&metrics_labels);
                let _timer = histogram.start_timer();
                store_update.commit()?;
            }

            RESHARDING_BATCH_COUNT.with_label_values(&metrics_labels).inc();
            RESHARDING_BATCH_SIZE.with_label_values(&metrics_labels).add(size as i64);

            // sleep between batches in order to throttle resharding and leave
            // some resource for the regular node operation
            std::thread::sleep(config.get().batch_delay.unsigned_abs());
        }

        state_roots = apply_delayed_receipts(
            &config.get(),
            &tries,
            shard_uid,
            state_root,
            state_roots,
            &checked_account_id_to_shard_uid,
        )?;

        state_roots = apply_promise_yield_timeouts(
            &config.get(),
            &tries,
            shard_uid,
            state_root,
            state_roots,
            &checked_account_id_to_shard_uid,
        )?;

        tracing::debug!(target: "resharding", ?shard_uid, "build_state_for_split_shards_impl finished");
        Ok(state_roots)
    }

    pub fn build_state_for_split_shards_postprocessing(
        &mut self,
        shard_uid: ShardUId,
        sync_hash: &CryptoHash,
        state_roots: HashMap<ShardUId, StateRoot>,
    ) -> Result<(), Error> {
        let block_header = self.get_block_header(sync_hash)?;
        let prev_hash = block_header.prev_hash();

        let child_shard_uids = state_roots.keys().cloned().collect_vec();
        self.initialize_flat_storage(&prev_hash, &child_shard_uids)?;
        // TODO(resharding) #10844 Load in-memory trie if needed.

        let mut chain_store_update = self.mut_chain_store().store_update();
        for (shard_uid, state_root) in state_roots {
            // here we store the state roots in chunk_extra in the database for later use
            let chunk_extra = ChunkExtra::new_with_only_state_root(&state_root);
            chain_store_update.save_chunk_extra(&prev_hash, &shard_uid, chunk_extra);
            debug!(target:"resharding", "Finish building resharding for shard {:?} {:?} {:?} ", shard_uid, prev_hash, state_root);
        }
        chain_store_update.commit()?;

        RESHARDING_STATUS
            .with_label_values(&[&shard_uid.to_string()])
            .set(ReshardingStatus::Finished.into());

        Ok(())
    }

    // Here we iterate over all the child shards and initialize flat storage for them by calling set_flat_storage_state
    // Note that this function is called on the current_block which is the first block the next epoch.
    // We set the flat_head as the prev_block as after resharding, the state written to flat storage corresponds to the
    // state as of prev_block, and that's the convention that we follow.
    fn initialize_flat_storage(
        &self,
        prev_hash: &CryptoHash,
        child_shard_uids: &[ShardUId],
    ) -> Result<(), Error> {
        let prev_block_header = self.get_block_header(prev_hash)?;
        let prev_block_info = BlockInfo {
            hash: *prev_block_header.hash(),
            prev_hash: *prev_block_header.prev_hash(),
            height: prev_block_header.height(),
        };

        // create flat storage for child shards
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        for shard_uid in child_shard_uids {
            let store = self.runtime_adapter.store().clone();
            set_flat_storage_state(store, &flat_storage_manager, *shard_uid, prev_block_info)?;
        }
        Ok(())
    }
}
