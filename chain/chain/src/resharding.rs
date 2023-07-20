/// Implementation for all resharding logic.
/// StateSplitRequest and StateSplitResponse are exchanged across the client_actor and SyncJobsActor.
/// build_state_for_split_shards_preprocessing and build_state_for_split_shards_postprocessing are handled
/// by the client_actor while the heavy resharding build_state_for_split_shards is done by SyncJobsActor
/// so as to not affect client.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use near_chain_primitives::error::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::state::FlatStateValue;
use near_primitives::syncing::STATE_PART_MEMORY_LIMIT;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ShardId, StateRoot};
use near_store::flat::store_helper;
use near_store::split_state::get_delayed_receipts;
use near_store::{ShardTries, ShardUId, Trie};

use tracing::debug;

use crate::types::RuntimeAdapter;
use crate::Chain;

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct StateSplitRequest {
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub sync_hash: CryptoHash,
    pub shard_uid: ShardUId,
    pub state_root: StateRoot,
    pub next_epoch_shard_layout: ShardLayout,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct StateSplitResponse {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
    pub new_state_roots: Result<HashMap<ShardUId, StateRoot>, Error>,
}

fn get_trie_update_batch(
    iter: &mut impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
) -> Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let mut size: u64 = 0;
    let mut entries = Vec::new();
    while let Some((key, value)) = iter.next() {
        size += value.len() as u64;
        entries.push((key, Some(value)));
        if size > STATE_PART_MEMORY_LIMIT.as_u64() {
            break;
        }
    }
    (!entries.is_empty()).then_some(entries)
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
        get_delayed_receipts(&orig_trie_update, start_index, STATE_PART_MEMORY_LIMIT)?
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

impl Chain {
    pub fn build_state_for_split_shards_preprocessing(
        &self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        state_split_scheduler: &dyn Fn(StateSplitRequest),
    ) -> Result<(), Error> {
        let (epoch_id, next_epoch_id) = {
            let block_header = self.get_block_header(sync_hash)?;
            (block_header.epoch_id().clone(), block_header.next_epoch_id().clone())
        };
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let next_epoch_shard_layout = self.epoch_manager.get_shard_layout(&next_epoch_id)?;
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let prev_hash = *self.get_block_header(sync_hash)?.prev_hash();
        let state_root = *self.get_chunk_extra(&prev_hash, &shard_uid)?.state_root();
        assert_ne!(shard_layout, next_epoch_shard_layout);

        state_split_scheduler(StateSplitRequest {
            runtime_adapter: self.runtime_adapter.clone(),
            sync_hash: *sync_hash,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
        });

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

    fn build_state_for_split_shards_impl(
        state_split_request: StateSplitRequest,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let StateSplitRequest {
            runtime_adapter,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
            ..
        } = state_split_request;

        let tries = runtime_adapter.get_tries();
        let trie = tries.get_view_trie_for_shard(shard_uid, state_root);

        let mut iter =
            store_helper::iter_flat_state_entries(shard_uid, runtime_adapter.store(), None, None)
                .map(|entry| -> (Vec<u8>, Vec<u8>) {
                    let (key, value) = entry.unwrap();
                    let value = match value {
                        FlatStateValue::Ref(ref_value) => {
                            trie.storage.retrieve_raw_bytes(&ref_value.hash).unwrap().to_vec()
                        }
                        FlatStateValue::Inlined(inline_value) => inline_value,
                    };
                    (key, value)
                });

        let shard_id = shard_uid.shard_id();
        let new_shards = next_epoch_shard_layout
            .get_split_shard_uids(shard_id)
            .ok_or(Error::InvalidShardId(shard_id))?;
        let mut state_roots: HashMap<_, _> =
            new_shards.iter().map(|shard_uid| (*shard_uid, Trie::EMPTY_ROOT)).collect();

        // function to map account id to shard uid in range of child shards
        let split_shard_ids: HashSet<_> = new_shards.into_iter().collect();
        let checked_account_id_to_shard_uid = |account_id: &AccountId| {
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
        };

        while let Some(batch) = get_trie_update_batch(&mut iter) {
            // TODO(shreyan): This is highly inefficient as for each key in the batch, we are parsing the account_id
            // A better way would be to use the boundary account to construct the from and to key range for flat storage iterator
            let (store_update, new_state_roots) = tries.add_values_to_split_states(
                &state_roots,
                batch,
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

    pub fn build_state_for_split_shards_postprocessing(
        &mut self,
        sync_hash: &CryptoHash,
        state_roots: Result<HashMap<ShardUId, StateRoot>, Error>,
    ) -> Result<(), Error> {
        let prev_hash = *self.get_block_header(sync_hash)?.prev_hash();
        let mut chain_store_update = self.mut_store().store_update();
        for (shard_uid, state_root) in state_roots? {
            // here we store the state roots in chunk_extra in the database for later use
            let chunk_extra = ChunkExtra::new_with_only_state_root(&state_root);
            chain_store_update.save_chunk_extra(&prev_hash, &shard_uid, chunk_extra);
            debug!(target:"chain", "Finish building split state for shard {:?} {:?} {:?} ", shard_uid, prev_hash, state_root);
        }
        chain_store_update.commit()
    }
}
