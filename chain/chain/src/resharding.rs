/// Implementation for all resharding logic.
/// StateSplitRequest and StateSplitResponse are exchanged across the client_actor and SyncJobsActor.
/// build_state_for_split_shards_preprocessing and build_state_for_split_shards_postprocessing are handled
/// by the client_actor while the heavy resharding build_state_for_split_shards is done by SyncJobsActor
/// so as to not affect client.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use near_chain_primitives::error::Error;
use near_client_primitives::types::StateSplitApplyingStatus;
use near_o11y::log_assert;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, STATE_PART_MEMORY_LIMIT};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ShardId, StateRoot};
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
    pub shard_id: ShardId,
    pub shard_uid: ShardUId,
    pub state_root: StateRoot,
    pub next_epoch_shard_layout: ShardLayout,
    pub state_split_status: Arc<StateSplitApplyingStatus>,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct StateSplitResponse {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
    pub new_state_roots: Result<HashMap<ShardUId, StateRoot>, Error>,
}

fn apply_delayed_receipts<'a>(
    tries: &ShardTries,
    orig_shard_uid: ShardUId,
    orig_state_root: StateRoot,
    state_roots: HashMap<ShardUId, StateRoot>,
    account_id_to_shard_id: &(dyn Fn(&AccountId) -> ShardUId + 'a),
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
            account_id_to_shard_id,
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
        state_split_status: Arc<StateSplitApplyingStatus>,
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
            shard_id,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
            state_split_status,
        });

        Ok(())
    }

    pub fn build_state_for_split_shards(
        state_split_request: StateSplitRequest,
    ) -> Result<HashMap<ShardUId, StateRoot>, Error> {
        let StateSplitRequest {
            runtime_adapter,
            shard_uid,
            state_root,
            next_epoch_shard_layout,
            state_split_status,
            ..
        } = state_split_request;
        // TODO(resharding) use flat storage to split the trie here
        let tries = runtime_adapter.get_tries();
        let trie = tries.get_view_trie_for_shard(shard_uid, state_root);
        let shard_id = shard_uid.shard_id();
        let new_shards = next_epoch_shard_layout
            .get_split_shard_uids(shard_id)
            .ok_or(Error::InvalidShardId(shard_id))?;
        let mut state_roots: HashMap<_, _> =
            new_shards.iter().map(|shard_uid| (*shard_uid, Trie::EMPTY_ROOT)).collect();
        let split_shard_ids: HashSet<_> = new_shards.into_iter().collect();
        let checked_account_id_to_shard_id = |account_id: &AccountId| {
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

        let state_root_node = trie.retrieve_root_node()?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        if state_split_status.total_parts.set(num_parts).is_err() {
            log_assert!(false, "splitting state was done twice for shard {}", shard_id);
        }
        debug!(target: "runtime", "splitting state for shard {} to {} parts to build new states", shard_id, num_parts);
        for part_id in 0..num_parts {
            let trie_items = trie.get_trie_items_for_part(PartId::new(part_id, num_parts))?;
            let (store_update, new_state_roots) = tries.add_values_to_split_states(
                &state_roots,
                trie_items.into_iter().map(|(key, value)| (key, Some(value))).collect(),
                &checked_account_id_to_shard_id,
            )?;
            state_roots = new_state_roots;
            store_update.commit()?;
            state_split_status.done_parts.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        }
        state_roots = apply_delayed_receipts(
            &tries,
            shard_uid,
            state_root,
            state_roots,
            &checked_account_id_to_shard_id,
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
