/// Implementation for all resharding V2 logic.
/// ReshardingRequest and ReshardingResponse are exchanged across the client_actor and SyncJobsActor.
/// build_state_for_split_shards_preprocessing and build_state_for_split_shards_postprocessing are handled
/// by the client_actor while the heavy resharding build_state_for_split_shards is done by SyncJobsActor
/// so as to not affect client.
use crate::Chain;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::error::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ShardId, StateRoot};
use near_store::flat::FlatStorageError;
use near_store::resharding_v2::{get_delayed_receipts, get_promise_yield_timeouts};
use near_store::{ShardTries, ShardUId, StorageError, Trie};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

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
    // Indicated whether or not resharding has been triggered manually on demand.
    pub on_demand: bool,
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
        Ok(Some(TrieUpdateBatch { entries }))
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

impl Chain {
    pub fn build_state_for_split_shards_v2(
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
            }
        }
        ReshardingResponse { shard_id, sync_hash, new_state_roots }
    }

    fn apply_resharding_batches(
        shard_uid: &ShardUId,
        config: &MutableConfigValue<ReshardingConfig>,
        tries: &Arc<ShardTries>,
        checked_account_id_to_shard_uid: &impl Fn(&AccountId) -> ShardUId,
        handle: ReshardingHandle,
        state_roots: &mut HashMap<ShardUId, CryptoHash>,
        mut iter: &mut impl Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>), FlatStorageError>>,
    ) -> Result<(), Error> {
        let mut batch_count = 0;

        loop {
            if !handle.get() {
                // The keep_going is set to false, interrupt processing.
                tracing::info!(target: "resharding", ?shard_uid, "build_state_for_split_shards_impl interrupted");
                return Err(Error::Other("Resharding interrupted.".to_string()));
            }
            // Prepare the batch.
            let (batch, prepare_time) = {
                let timer = Instant::now();
                let batch = get_trie_update_batch(&config.get(), &mut iter);
                let batch = batch.map_err(Into::<StorageError>::into)?;
                let Some(batch) = batch else { break };
                (batch, timer.elapsed())
            };

            // Apply the batch - add values to the children shards.
            let TrieUpdateBatch { entries } = batch;
            let (store_update, apply_time) = {
                let timer = Instant::now();
                // TODO(#9435): This is highly inefficient as for each key in the batch, we are parsing the account_id
                // A better way would be to use the boundary account to construct the from and to key range for flat storage iterator
                let (store_update, new_state_roots) = tries.add_values_to_children_states(
                    &state_roots,
                    entries,
                    &checked_account_id_to_shard_uid,
                )?;
                *state_roots = new_state_roots;
                (store_update, timer.elapsed())
            };

            // Commit the store update.
            let commit_time = {
                let timer = Instant::now();
                store_update.commit()?;
                timer.elapsed()
            };

            batch_count += 1;

            debug!(target: "resharding", ?shard_uid, ?batch_count, ?prepare_time, ?apply_time, ?commit_time, "batch processed");

            // sleep between batches in order to throttle resharding and leave
            // some resource for the regular node operation
            std::thread::sleep(config.get().batch_delay.unsigned_abs());
        }
        Ok(())
    }

    fn build_state_for_split_shards_impl(
        resharding_request: ReshardingRequest,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let ReshardingRequest {
            tries,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
            config,
            handle,
            on_demand,
            ..
        } = resharding_request;
        tracing::debug!(target: "resharding", config=?config.get(), ?shard_uid, "build_state_for_split_shards_impl starting");

        let shard_id = shard_uid.shard_id();
        let new_shards = next_epoch_shard_layout
            .get_children_shards_uids(shard_id)
            .ok_or(Error::InvalidShardId(shard_id))?;
        let mut state_roots: HashMap<_, _> =
            new_shards.iter().map(|shard_uid| (*shard_uid, Trie::EMPTY_ROOT)).collect();

        // function to map account id to shard uid in range of child shards
        let checked_account_id_to_shard_uid =
            get_checked_account_id_to_shard_uid_fn(shard_uid, new_shards, next_epoch_shard_layout);

        if !on_demand {
            panic!("Resharding V2 can be triggered only on-demand")
        }

        // When resharding is triggered on demand no special iterator chaining is required
        // because we fallback to use the tries stored on disk.
        let trie: Trie = tries.get_trie_for_shard(shard_uid, state_root);

        // However some gymnastics are required to make the iterator signature match the one required by
        // `apply_resharding_batches`.
        let mut iter = trie.disk_iter()?.map(|item| {
            item.map(|(lhs, rhs)| (lhs, Some(rhs)))
                .map_err(|err| FlatStorageError::StorageInternalError(err.to_string()))
        });

        Self::apply_resharding_batches(
            &shard_uid,
            &config,
            &tries,
            &checked_account_id_to_shard_uid,
            handle,
            &mut state_roots,
            &mut iter,
        )?;

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

    /// Preprocessing stage for on demand resharding. This method should be called only by the resharding tool.
    pub fn custom_build_state_for_resharding_v2_preprocessing(
        &self,
        // The resharding will execute on the post state of the target_hash block.
        target_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<ReshardingRequest, Error> {
        let sync_hash = target_hash;
        tracing::debug!(target: "resharding-v2", ?shard_id, ?sync_hash, "preprocessing started");
        let block_header = self.get_block_header(sync_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
        let next_epoch_id = block_header.next_epoch_id();
        let next_epoch_shard_layout = self.epoch_manager.get_shard_layout(next_epoch_id)?;
        assert_ne!(shard_layout, next_epoch_shard_layout);

        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let state_root = *self.get_chunk_extra(&target_hash, &shard_uid)?.state_root();

        let resharding_request = ReshardingRequest {
            tries: Arc::new(self.runtime_adapter.get_tries()),
            sync_hash: *sync_hash,
            // `prev_hash` and `prev_prev_hash` aren't used while doing on demand resharding without flat-storage.
            prev_hash: *target_hash,
            prev_prev_hash: *target_hash,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
            curr_poll_time: Duration::ZERO,
            config: self.resharding_config.clone(),
            handle: self.resharding_handle.clone(),
            on_demand: true,
        };

        Ok(resharding_request)
    }

    /// Postprocessing stage for on demand resharding. This method should be called only by the resharding tool.
    pub fn custom_build_state_for_split_shards_v2_postprocessing(
        &mut self,
        sync_hash: &CryptoHash,
        state_roots: HashMap<ShardUId, StateRoot>,
    ) -> Result<(), Error> {
        let block_header = self.get_block_header(sync_hash)?;
        let prev_hash = block_header.prev_hash();

        for (shard_uid, state_root) in state_roots {
            debug!(target:"resharding-v2", "Finish building resharding for shard {:?} {:?} {:?} ", shard_uid, prev_hash, state_root);
            // Chunk extra should match the ones committed into the chain.
            let new_chunk_extra = ChunkExtra::new_with_only_state_root(&state_root);
            if let Ok(chunk_extra) = self.get_chunk_extra(sync_hash, &shard_uid) {
                if chunk_extra.state_root() != new_chunk_extra.state_root() {
                    error!(target:"resharding-v2", ?chunk_extra, ?new_chunk_extra, "Chunk extra state_root mismatch!");
                }
            } else {
                warn!(target:"resharding-v2", ?sync_hash, "Chunk extra not found in DB");
            }
        }

        Ok(())
    }
}
