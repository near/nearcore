/// Implementation for all resharding logic.
/// StateSplitRequest and StateSplitResponse are exchanged across the client_actor and SyncJobsActor.
/// build_state_for_split_shards_preprocessing and build_state_for_split_shards_postprocessing are handled
/// by the client_actor while the heavy resharding build_state_for_split_shards is done by SyncJobsActor
/// so as to not affect client.
use itertools::Itertools;
use near_chain_primitives::error::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, STATE_PART_MEMORY_LIMIT};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ShardId, StateRoot};
use near_store::flat::{
    store_helper, BlockInfo, FlatStorageManager, FlatStorageReadyStatus, FlatStorageStatus,
};
use near_store::split_state::get_delayed_receipts;
use near_store::{ShardTries, ShardUId, Store, Trie};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
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

// Skip `runtime_adapter`, because it's a complex object that has complex logic
// and many fields.
impl Debug for StateSplitRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateSplitRequest")
            .field("runtime_adapter", &"<not shown>")
            .field("sync_hash", &self.sync_hash)
            .field("shard_uid", &self.shard_uid)
            .field("state_root", &self.state_root)
            .field("next_epoch_shard_layout", &self.next_epoch_shard_layout)
            .finish()
    }
}

#[derive(actix::Message, Debug)]
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
        state_roots: HashMap<ShardUId, StateRoot>,
    ) -> Result<(), Error> {
        let block_header = self.get_block_header(sync_hash)?;
        let prev_hash = block_header.prev_hash();

        let child_shard_uids = state_roots.keys().collect_vec();
        self.initialize_flat_storage(&prev_hash, &child_shard_uids)?;

        let mut chain_store_update = self.mut_store().store_update();
        for (shard_uid, state_root) in state_roots {
            // here we store the state roots in chunk_extra in the database for later use
            let chunk_extra = ChunkExtra::new_with_only_state_root(&state_root);
            chain_store_update.save_chunk_extra(&prev_hash, &shard_uid, chunk_extra);
            debug!(target:"chain", "Finish building split state for shard {:?} {:?} {:?} ", shard_uid, prev_hash, state_root);
        }
        chain_store_update.commit()?;

        Ok(())
    }

    // Here we iterate over all the child shards and initialize flat storage for them by calling set_flat_storage_state
    // Note that this function is called on the current_block which is the first block the next epoch.
    // We set the flat_head as the prev_block as after resharding, the state written to flat storage corresponds to the
    // state as of prev_block, and that's the convention that we follow.
    fn initialize_flat_storage(
        &self,
        prev_hash: &CryptoHash,
        child_shard_uids: &[&ShardUId],
    ) -> Result<(), Error> {
        let prev_block_header = self.get_block_header(prev_hash)?;
        let prev_block_info = BlockInfo {
            hash: *prev_block_header.hash(),
            prev_hash: *prev_block_header.prev_hash(),
            height: prev_block_header.height(),
        };

        // create flat storage for child shards
        if let Some(flat_storage_manager) = self.runtime_adapter.get_flat_storage_manager() {
            for shard_uid in child_shard_uids {
                let store = self.runtime_adapter.store().clone();
                set_flat_storage_state(store, &flat_storage_manager, **shard_uid, prev_block_info)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::account::Account;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::{DelayedReceiptIndices, Receipt};
    use near_primitives::state_part::PartId;
    use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_raw_key;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::{
        AccountId, NumShards, StateChangeCause, StateChangesForSplitStates, StateRoot,
    };
    use near_store::test_utils::{
        create_tries, gen_receipts, gen_unique_accounts, get_all_delayed_receipts,
    };
    use near_store::{
        get, get_delayed_receipt_indices, set, set_account, ShardTries, ShardUId, Trie,
    };
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::collections::HashMap;

    #[test]
    fn test_split_and_update_states() {
        // build states
        let mut rng = rand::thread_rng();
        for _ in 0..20 {
            test_split_and_update_state_impl(&mut rng);
        }
    }

    fn test_split_and_update_state_impl(rng: &mut impl Rng) {
        let tries = create_tries();
        // add accounts and receipts to state
        let mut account_ids = gen_unique_accounts(rng, 1, 100);
        let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), Trie::EMPTY_ROOT);
        for account_id in account_ids.iter() {
            set_account(
                &mut trie_update,
                account_id.clone(),
                &Account::new(0, 0, CryptoHash::default(), 0),
            );
        }
        let receipts = gen_receipts(rng, 100);
        // add accounts and receipts to the original shard
        let mut state_root = {
            for (index, receipt) in receipts.iter().enumerate() {
                set(&mut trie_update, TrieKey::DelayedReceipt { index: index as u64 }, receipt);
            }
            set(
                &mut trie_update,
                TrieKey::DelayedReceiptIndices,
                &DelayedReceiptIndices {
                    first_index: 0,
                    next_available_index: receipts.len() as u64,
                },
            );
            trie_update.commit(StateChangeCause::Resharding);
            let (_, trie_changes, _) = trie_update.finalize().unwrap();
            let mut store_update = tries.store_update();
            let state_root =
                tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
            store_update.commit().unwrap();
            state_root
        };

        let num_shards = 4;
        let account_id_to_shard_id = &|account_id: &AccountId| ShardUId {
            shard_id: (hash(account_id.as_ref().as_bytes()).0[0] as NumShards % num_shards) as u32,
            version: 1,
        };

        // add accounts and receipts to the split shards
        let mut split_state_roots = {
            let trie_items = tries
                .get_view_trie_for_shard(ShardUId::single_shard(), state_root)
                .get_trie_items_for_part(PartId::new(0, 1))
                .unwrap();
            let split_state_roots: HashMap<_, _> = (0..num_shards)
                .map(|shard_id| {
                    (ShardUId { version: 1, shard_id: shard_id as u32 }, Trie::EMPTY_ROOT)
                })
                .collect();
            let (store_update, split_state_roots) = tries
                .add_values_to_split_states(
                    &split_state_roots,
                    trie_items.into_iter().map(|(key, value)| (key, Some(value))).collect(),
                    account_id_to_shard_id,
                )
                .unwrap();
            store_update.commit().unwrap();
            let (store_update, split_state_roots) = tries
                .apply_delayed_receipts_to_split_states(
                    &split_state_roots,
                    &get_all_delayed_receipts(&tries, &ShardUId::single_shard(), &state_root),
                    account_id_to_shard_id,
                )
                .unwrap();
            store_update.commit().unwrap();
            split_state_roots
        };
        compare_state_and_split_states(
            &tries,
            &state_root,
            &split_state_roots,
            account_id_to_shard_id,
        );

        // update the original shard
        for _ in 0..10 {
            // add accounts
            let new_accounts = gen_unique_accounts(rng, 1, 10);
            let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), state_root);
            for account_id in new_accounts.iter() {
                set_account(
                    &mut trie_update,
                    account_id.clone(),
                    &Account::new(0, 0, CryptoHash::default(), 0),
                );
            }
            // remove accounts
            account_ids.shuffle(rng);
            let remove_count = rng.gen_range(0..10).min(account_ids.len());
            for account_id in account_ids[0..remove_count].iter() {
                trie_update.remove(TrieKey::Account { account_id: account_id.clone() });
            }
            account_ids = account_ids[remove_count..].to_vec();
            account_ids.extend(new_accounts);

            // remove delayed receipts
            let mut delayed_receipt_indices = get_delayed_receipt_indices(&trie_update).unwrap();
            println!(
                "delayed receipt indices {} {}",
                delayed_receipt_indices.first_index, delayed_receipt_indices.next_available_index
            );
            let next_first_index = rng.gen_range(
                delayed_receipt_indices.first_index
                    ..delayed_receipt_indices.next_available_index + 1,
            );
            let mut removed_receipts = vec![];
            for index in delayed_receipt_indices.first_index..next_first_index {
                let trie_key = TrieKey::DelayedReceipt { index };
                removed_receipts.push(get::<Receipt>(&trie_update, &trie_key).unwrap().unwrap());
                trie_update.remove(trie_key);
            }
            delayed_receipt_indices.first_index = next_first_index;
            // add delayed receipts
            let new_receipts = gen_receipts(rng, 10);
            for receipt in new_receipts {
                set(
                    &mut trie_update,
                    TrieKey::DelayedReceipt { index: delayed_receipt_indices.next_available_index },
                    &receipt,
                );
                delayed_receipt_indices.next_available_index += 1;
            }
            println!(
                "after: delayed receipt indices {} {}",
                delayed_receipt_indices.first_index, delayed_receipt_indices.next_available_index
            );
            set(&mut trie_update, TrieKey::DelayedReceiptIndices, &delayed_receipt_indices);
            trie_update.commit(StateChangeCause::Resharding);
            let (_, trie_changes, state_changes) = trie_update.finalize().unwrap();
            let mut store_update = tries.store_update();
            let new_state_root =
                tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
            store_update.commit().unwrap();
            state_root = new_state_root;

            // update split states
            let mut trie_updates = tries
                .apply_state_changes_to_split_states(
                    &split_state_roots,
                    StateChangesForSplitStates::from_raw_state_changes(
                        &state_changes,
                        removed_receipts,
                    ),
                    account_id_to_shard_id,
                )
                .unwrap();
            split_state_roots = trie_updates
                .drain()
                .map(|(shard_uid, trie_update)| {
                    let mut state_update = tries.store_update();
                    let (_, trie_changes, _) = trie_update.finalize().unwrap();
                    let state_root = tries.apply_all(&trie_changes, shard_uid, &mut state_update);
                    state_update.commit().unwrap();
                    (shard_uid, state_root)
                })
                .collect();

            compare_state_and_split_states(
                &tries,
                &state_root,
                &split_state_roots,
                account_id_to_shard_id,
            );
        }
    }

    fn compare_state_and_split_states(
        tries: &ShardTries,
        state_root: &StateRoot,
        state_roots: &HashMap<ShardUId, StateRoot>,
        account_id_to_shard_id: &dyn Fn(&AccountId) -> ShardUId,
    ) {
        // Get trie items before resharding and split them by account shard
        let trie_items_before_resharding =
            get_trie_items_except_delayed_receipts(tries, &ShardUId::single_shard(), state_root);

        let trie_items_after_resharding: HashMap<_, _> = state_roots
            .iter()
            .map(|(&shard_uid, state_root)| {
                (shard_uid, get_trie_items_except_delayed_receipts(tries, &shard_uid, state_root))
            })
            .collect();

        let mut expected_trie_items_by_shard: HashMap<_, _> =
            state_roots.iter().map(|(&shard_uid, _)| (shard_uid, vec![])).collect();
        for item in trie_items_before_resharding {
            let account_id = parse_account_id_from_raw_key(&item.0).unwrap().unwrap();
            let shard_uid: ShardUId = account_id_to_shard_id(&account_id);
            expected_trie_items_by_shard.get_mut(&shard_uid).unwrap().push(item);
        }
        assert_eq!(expected_trie_items_by_shard, trie_items_after_resharding);

        // check that the new tries combined to the orig trie for delayed receipts
        let receipts_from_split_states: HashMap<_, _> = state_roots
            .iter()
            .map(|(&shard_uid, state_root)| {
                let receipts = get_all_delayed_receipts(tries, &shard_uid, state_root);
                (shard_uid, receipts)
            })
            .collect();

        let mut expected_receipts_by_shard: HashMap<_, Vec<_>> =
            state_roots.iter().map(|(&shard_uid, _)| (shard_uid, vec![])).collect();
        for receipt in get_all_delayed_receipts(tries, &ShardUId::single_shard(), state_root) {
            let shard_uid = account_id_to_shard_id(&receipt.receiver_id);
            expected_receipts_by_shard.get_mut(&shard_uid).unwrap().push(receipt.clone());
        }
        assert_eq!(expected_receipts_by_shard, receipts_from_split_states);
    }

    fn get_trie_items_except_delayed_receipts(
        tries: &ShardTries,
        shard_uid: &ShardUId,
        state_root: &StateRoot,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        tries
            .get_trie_for_shard(*shard_uid, *state_root)
            .iter()
            .unwrap()
            .map(Result::unwrap)
            .filter(|(key, _)| parse_account_id_from_raw_key(key).unwrap().is_some())
            .collect()
    }
}
