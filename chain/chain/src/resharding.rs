use crate::metrics::{
    ReshardingStatus, RESHARDING_BATCH_COUNT, RESHARDING_BATCH_SIZE, RESHARDING_STATUS,
};
/// Implementation for all resharding logic.
/// StateSplitRequest and StateSplitResponse are exchanged across the client_actor and SyncJobsActor.
/// build_state_for_split_shards_preprocessing and build_state_for_split_shards_postprocessing are handled
/// by the client_actor while the heavy resharding build_state_for_split_shards is done by SyncJobsActor
/// so as to not affect client.
use crate::Chain;
use itertools::Itertools;
use near_chain_primitives::error::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::state::FlatStateValue;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::get_num_state_parts;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ShardId, StateRoot};
use near_store::flat::{
    store_helper, BlockInfo, FlatStorageManager, FlatStorageReadyStatus, FlatStorageStatus,
};
use near_store::split_state::get_delayed_receipts;
use near_store::{ShardTries, ShardUId, Store, Trie, TrieDBStorage, TrieStorage};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tracing::debug;

// This is the approx batch size of the trie key, value pair entries that are written to the child shard trie.
const RESHARDING_BATCH_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(300 * bytesize::MIB);

/// StateSplitRequest has all the information needed to start a resharding job. This message is sent
/// from ClientActor to SyncJobsActor. We do not want to stall the ClientActor with a long running
/// resharding job. The SyncJobsActor is helpful for handling such long running jobs.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct StateSplitRequest {
    pub tries: Arc<ShardTries>,
    pub sync_hash: CryptoHash,
    pub shard_uid: ShardUId,
    pub state_root: StateRoot,
    pub next_epoch_shard_layout: ShardLayout,
}

// Skip `runtime_adapter`, because it's a complex object that has complex logic
// and many fields.
impl Debug for StateSplitRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateSplitRequest")
            .field("tries", &"<not shown>")
            .field("sync_hash", &self.sync_hash)
            .field("shard_uid", &self.shard_uid)
            .field("state_root", &self.state_root)
            .field("next_epoch_shard_layout", &self.next_epoch_shard_layout)
            .finish()
    }
}

// StateSplitResponse is the response sent from SyncJobsActor to ClientActor once resharding is completed.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct StateSplitResponse {
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

// Return iterate over flat storage to get key, value. Used later in the get_trie_update_batch function.
// TODO(#9436): This isn't completely correct. We need to get the flat storage iterator based off a
// particular block, specifically, the last block of the previous epoch.
fn get_flat_storage_iter<'a>(
    store: &'a Store,
    shard_uid: ShardUId,
) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a {
    let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);
    store_helper::iter_flat_state_entries(shard_uid, &store, None, None).map(
        move |entry| -> (Vec<u8>, Vec<u8>) {
            let (key, value) = entry.unwrap();
            let value = match value {
                FlatStateValue::Ref(ref_value) => {
                    trie_storage.retrieve_raw_bytes(&ref_value.hash).unwrap().to_vec()
                }
                FlatStateValue::Inlined(inline_value) => inline_value,
            };
            (key, value)
        },
    )
}

// Format of the trie key, value pair that is used in tries.add_values_to_split_states() function
type TrieEntry = (Vec<u8>, Option<Vec<u8>>);

struct TrieUpdateBatch {
    entries: Vec<TrieEntry>,
    size: u64,
}

// Function to return batches of trie key, value pairs from flat storage iter. We return None at the end of iter.
// The batch size is roughly RESHARDING_BATCH_MEMORY_LIMIT (300 MB)
fn get_trie_update_batch(
    iter: &mut impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
) -> Option<TrieUpdateBatch> {
    let mut size: u64 = 0;
    let mut entries = Vec::new();
    while let Some((key, value)) = iter.next() {
        size += key.len() as u64 + value.len() as u64;
        entries.push((key, Some(value)));
        if size > RESHARDING_BATCH_MEMORY_LIMIT.as_u64() {
            break;
        }
    }
    if entries.is_empty() {
        None
    } else {
        Some(TrieUpdateBatch { entries, size })
    }
}

fn apply_delayed_receipts<'a>(
    tries: &ShardTries,
    orig_shard_uid: ShardUId,
    orig_state_root: StateRoot,
    state_roots: HashMap<ShardUId, StateRoot>,
    account_id_to_shard_uid: &(dyn Fn(&AccountId) -> ShardUId + 'a),
) -> Result<HashMap<ShardUId, StateRoot>, Error> {
    let orig_trie_update = tries.new_trie_update_view(orig_shard_uid, orig_state_root);

    let mut start_index = None;
    let mut new_state_roots = state_roots;
    while let Some((next_index, receipts)) =
        get_delayed_receipts(&orig_trie_update, start_index, RESHARDING_BATCH_MEMORY_LIMIT)?
    {
        let (store_update, updated_state_roots) = tries.apply_delayed_receipts_to_split_states(
            &new_state_roots,
            &receipts,
            account_id_to_shard_uid,
        )?;
        new_state_roots = updated_state_roots;
        start_index = Some(next_index);
        store_update.commit()?;
    }

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

impl Chain {
    pub fn build_state_for_split_shards_preprocessing(
        &self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        state_split_scheduler: &dyn Fn(StateSplitRequest),
    ) -> Result<(), Error> {
        let block_header = self.get_block_header(sync_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
        let next_epoch_shard_layout =
            self.epoch_manager.get_shard_layout(block_header.next_epoch_id())?;
        assert_ne!(shard_layout, next_epoch_shard_layout);

        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let prev_hash = block_header.prev_hash();
        let state_root = *self.get_chunk_extra(&prev_hash, &shard_uid)?.state_root();

        state_split_scheduler(StateSplitRequest {
            tries: Arc::new(self.runtime_adapter.get_tries()),
            sync_hash: *sync_hash,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
        });

        RESHARDING_STATUS
            .with_label_values(&[&shard_uid.to_string()])
            .set(ReshardingStatus::Scheduled.into());

        Ok(())
    }

    pub fn build_state_for_split_shards(
        state_split_request: StateSplitRequest,
    ) -> StateSplitResponse {
        let shard_id = state_split_request.shard_uid.shard_id();
        let sync_hash = state_split_request.sync_hash;
        let new_state_roots = Self::build_state_for_split_shards_impl(state_split_request);
        StateSplitResponse { shard_id, sync_hash, new_state_roots }
    }

    // TODO(#9446) remove function when shifting to flat storage iteration for resharding
    fn build_state_for_split_shards_impl(
        state_split_request: StateSplitRequest,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let StateSplitRequest { tries, shard_uid, state_root, next_epoch_shard_layout, .. } =
            state_split_request;
        let trie = tries.get_view_trie_for_shard(shard_uid, state_root);
        let shard_id = shard_uid.shard_id();
        let new_shards = next_epoch_shard_layout
            .get_split_shard_uids(shard_id)
            .ok_or(Error::InvalidShardId(shard_id))?;
        let mut state_roots: HashMap<_, _> =
            new_shards.iter().map(|shard_uid| (*shard_uid, Trie::EMPTY_ROOT)).collect();
        let checked_account_id_to_shard_uid =
            get_checked_account_id_to_shard_uid_fn(shard_uid, new_shards, next_epoch_shard_layout);

        let state_root_node = trie.retrieve_root_node()?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        debug!(target: "resharding", "splitting state for shard {} to {} parts to build new states", shard_id, num_parts);
        for part_id in 0..num_parts {
            let trie_items = trie.get_trie_items_for_part(PartId::new(part_id, num_parts))?;
            let (store_update, new_state_roots) = tries.add_values_to_split_states(
                &state_roots,
                trie_items.into_iter().map(|(key, value)| (key, Some(value))).collect(),
                &checked_account_id_to_shard_uid,
            )?;
            state_roots = new_state_roots;
            store_update.commit()?;
        }
        state_roots = apply_delayed_receipts(
            &tries,
            shard_uid,
            state_root,
            state_roots,
            &checked_account_id_to_shard_uid,
        )?;

        Ok(state_roots)
    }

    // TODO(#9446) After implementing iterator at specific head, shift to build_state_for_split_shards_impl_v2
    #[allow(dead_code)]
    fn build_state_for_split_shards_impl_v2(
        state_split_request: StateSplitRequest,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let StateSplitRequest { tries, shard_uid, state_root, next_epoch_shard_layout, .. } =
            state_split_request;
        let store = tries.get_store();

        let shard_id = shard_uid.shard_id();

        RESHARDING_STATUS
            .with_label_values(&[&shard_uid.to_string()])
            .set(ReshardingStatus::BuildingState.into());

        let new_shards = next_epoch_shard_layout
            .get_split_shard_uids(shard_id)
            .ok_or(Error::InvalidShardId(shard_id))?;
        let mut state_roots: HashMap<_, _> =
            new_shards.iter().map(|shard_uid| (*shard_uid, Trie::EMPTY_ROOT)).collect();

        // function to map account id to shard uid in range of child shards
        let checked_account_id_to_shard_uid =
            get_checked_account_id_to_shard_uid_fn(shard_uid, new_shards, next_epoch_shard_layout);

        let mut iter = get_flat_storage_iter(&store, shard_uid);
        while let Some(batch) = get_trie_update_batch(&mut iter) {
            let TrieUpdateBatch { entries, size } = batch;
            // TODO(#9435): This is highly inefficient as for each key in the batch, we are parsing the account_id
            // A better way would be to use the boundary account to construct the from and to key range for flat storage iterator
            let (store_update, new_state_roots) = tries.add_values_to_split_states(
                &state_roots,
                entries,
                &checked_account_id_to_shard_uid,
            )?;
            state_roots = new_state_roots;
            store_update.commit()?;
            RESHARDING_BATCH_COUNT.with_label_values(&[shard_uid.to_string().as_str()]).inc();
            RESHARDING_BATCH_SIZE
                .with_label_values(&[shard_uid.to_string().as_str()])
                .add(size as i64)
        }

        state_roots = apply_delayed_receipts(
            &tries,
            shard_uid,
            state_root,
            state_roots,
            &checked_account_id_to_shard_uid,
        )?;

        Ok(state_roots)
    }

    pub fn build_state_for_split_shards_postprocessing(
        &mut self,
        sync_hash: &CryptoHash,
        state_roots: HashMap<ShardUId, StateRoot>,
    ) -> Result<(), Error> {
        let block_header = self.get_block_header(sync_hash)?;
        let prev_hash = block_header.prev_hash();

        let child_shard_uids = state_roots.keys().cloned().collect_vec();
        self.initialize_flat_storage(&prev_hash, &child_shard_uids)?;

        let mut chain_store_update = self.mut_store().store_update();
        for (shard_uid, state_root) in state_roots {
            // here we store the state roots in chunk_extra in the database for later use
            let chunk_extra = ChunkExtra::new_with_only_state_root(&state_root);
            chain_store_update.save_chunk_extra(&prev_hash, &shard_uid, chunk_extra);
            debug!(target:"resharding", "Finish building split state for shard {:?} {:?} {:?} ", shard_uid, prev_hash, state_root);
        }
        chain_store_update.commit()?;

        for shard_uid in child_shard_uids {
            RESHARDING_STATUS
                .with_label_values(&[&shard_uid.to_string()])
                .set(ReshardingStatus::Finished.into());
        }

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
