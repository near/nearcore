use super::sharding::shard_was_split;
use crate::utils::sharding::{
    client_tracking_shard, get_memtrie_for_shard, get_tracked_shards,
    get_tracked_shards_from_prev_block,
};
use borsh::BorshDeserialize;
use itertools::Itertools;
use near_chain::ChainStoreAccess;
use near_chain::types::Tip;
use near_client::Client;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state::FlatStateValue;
use near_primitives::types::{AccountId, EpochId, NumShards};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::get_shard_uid_mapping;
use near_store::db::refcount::decode_value_with_rc;
use near_store::flat::FlatStorageStatus;
use near_store::{DBCol, ShardUId};
use std::collections::{HashMap, HashSet};

// For each epoch, keep a map from AccountId to a map with keys equal to
// the set of shards that account tracks in that epoch, and bool values indicating
// whether the equality of flat storage and memtries has been checked for that shard
type EpochTrieCheck = HashMap<AccountId, HashMap<ShardUId, bool>>;

/// Keeps track of the needed trie comparisons for each epoch. After we successfully call
/// assert_state_sanity() for an account ID, we mark those shards as checked for that epoch,
/// and then at the end of the test we check whether all expected shards for each account
/// were checked at least once in that epoch. We do this because assert_state_sanity() isn't
/// always able to perform the check if child shard flat storages are still being created, but
/// we want to make sure that it's always eventually checked by the end of the epoch
pub struct TrieSanityCheck {
    accounts: Vec<AccountId>,
    load_memtries_for_tracked_shards: bool,
    checks: HashMap<EpochId, EpochTrieCheck>,
}

impl TrieSanityCheck {
    pub fn new(clients: &[&Client], load_memtries_for_tracked_shards: bool) -> Self {
        let accounts = clients
            .iter()
            .filter_map(|c| {
                let signer = c.validator_signer.get();
                signer.map(|s| s.validator_id().clone())
            })
            .collect();
        Self { accounts, load_memtries_for_tracked_shards, checks: HashMap::new() }
    }

    // If it's not already stored, initialize it with the expected ShardUIds for each account
    fn get_epoch_check(
        &mut self,
        client: &Client,
        tip: &Tip,
        new_num_shards: NumShards,
    ) -> &mut EpochTrieCheck {
        let protocol_version =
            client.epoch_manager.get_epoch_protocol_version(&tip.epoch_id).unwrap();
        let shards_pending_resharding = client
            .epoch_manager
            .get_shard_uids_pending_resharding(protocol_version, PROTOCOL_VERSION)
            .unwrap();
        let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
        let is_resharded = shard_layout.num_shards() == new_num_shards;

        if self.checks.contains_key(&tip.epoch_id) {
            return self.checks.get_mut(&tip.epoch_id).unwrap();
        }

        let mut check = HashMap::new();
        for account_id in &self.accounts {
            let check_shard_uids = self.get_epoch_check_for_account(
                client,
                tip,
                is_resharded,
                &shards_pending_resharding,
                &shard_layout,
                account_id,
            );
            check.insert(account_id.clone(), check_shard_uids);
        }

        self.checks.insert(tip.epoch_id, check);
        self.checks.get_mut(&tip.epoch_id).unwrap()
    }

    // Returns the expected shard uids for the given account.
    fn get_epoch_check_for_account(
        &self,
        client: &Client,
        tip: &Tip,
        is_resharded: bool,
        shards_pending_resharding: &HashSet<ShardUId>,
        shard_layout: &ShardLayout,
        account_id: &AccountId,
    ) -> HashMap<ShardUId, bool> {
        let mut check_shard_uids = HashMap::new();
        for shard_uid in shard_layout.shard_uids() {
            if !should_assert_state_sanity(
                self.load_memtries_for_tracked_shards,
                is_resharded,
                shards_pending_resharding,
                shard_layout,
                &shard_uid,
            ) {
                continue;
            }

            let cares = client.shard_tracker.cares_about_shard(
                Some(account_id),
                &tip.prev_block_hash,
                shard_uid.shard_id(),
                false,
            );
            if !cares {
                continue;
            }
            check_shard_uids.insert(shard_uid, false);
        }
        check_shard_uids
    }

    // Check trie sanity and keep track of which shards were successfully fully checked
    pub fn assert_state_sanity(&mut self, clients: &[&Client], new_num_shards: NumShards) {
        for client in clients {
            let signer = client.validator_signer.get();
            let Some(account_id) = signer.as_ref().map(|s| s.validator_id()) else {
                // For now this is never relevant, since all of them have account IDs, but
                // if this changes in the future, here we'll just skip those.
                continue;
            };
            let head = client.chain.head().unwrap();
            if head.epoch_id == EpochId::default() {
                continue;
            }
            let final_head = client.chain.final_head().unwrap();
            // At the end of an epoch, we unload memtries for shards we'll no longer track. Also,
            // the key/value equality comparison in assert_state_equal() is only guaranteed for
            // final blocks. So these two together mean that we should only check this when the head
            // and final head are in the same epoch.
            if head.epoch_id != final_head.epoch_id {
                continue;
            }
            let checked_shards = assert_state_sanity(
                client,
                &final_head,
                self.load_memtries_for_tracked_shards,
                new_num_shards,
            );
            let check = self.get_epoch_check(client, &head, new_num_shards);
            let check = check.get_mut(account_id).unwrap();
            for shard_uid in checked_shards {
                check.insert(shard_uid, true);
            }
        }
    }

    /// Look through all the epochs before the current one (because the current one will be early into the epoch,
    /// and we won't have checked it yet) and make sure that for all accounts, all expected shards were checked at least once
    pub fn check_epochs(&self, client: &Client) {
        let tip = client.chain.head().unwrap();
        let mut block_info = client.epoch_manager.get_block_info(&tip.last_block_hash).unwrap();

        loop {
            let epoch_id = client
                .epoch_manager
                .get_prev_epoch_id_from_prev_block(block_info.prev_hash())
                .unwrap();
            if epoch_id == EpochId::default() {
                break;
            }
            let check = self.checks.get(&epoch_id).unwrap_or_else(|| {
                panic!("No trie comparison checks made for epoch {}", &epoch_id.0)
            });
            for (account_id, checked_shards) in check {
                for (shard_uid, checked) in checked_shards {
                    assert!(
                        checked,
                        "No trie comparison checks made for account {} epoch {} shard {}",
                        account_id, &epoch_id.0, shard_uid
                    );
                }
            }

            block_info =
                client.epoch_manager.get_block_info(block_info.epoch_first_block()).unwrap();
            block_info = client.epoch_manager.get_block_info(block_info.prev_hash()).unwrap();
        }
    }
}

/// Asserts that for each child shard, MemTrie, FlatState and DiskTrie all
/// contain the same key-value pairs. If `load_memtries_for_tracked_shards` is
/// false, we only enforce memtries for shards pending resharding in the old
/// layout and the shards that were split in the new shard layout.
///
/// Returns the ShardUIds that this client tracks and has sane memtries and flat
/// storage for
///
/// The new num shards argument is a clumsy way to check if the head is before
/// or after resharding.
fn assert_state_sanity(
    client: &Client,
    final_head: &Tip,
    load_memtries_for_tracked_shards: bool,
    new_num_shards: NumShards,
) -> Vec<ShardUId> {
    let shard_layout = client.epoch_manager.get_shard_layout(&final_head.epoch_id).unwrap();
    let is_resharded = shard_layout.num_shards() == new_num_shards;
    let mut checked_shards = Vec::new();

    let protocol_version =
        client.epoch_manager.get_epoch_protocol_version(&final_head.epoch_id).unwrap();
    let shards_pending_resharding = client
        .epoch_manager
        .get_shard_uids_pending_resharding(protocol_version, PROTOCOL_VERSION)
        .unwrap();

    for shard_uid in shard_layout.shard_uids() {
        if !should_assert_state_sanity(
            load_memtries_for_tracked_shards,
            is_resharded,
            &shards_pending_resharding,
            &shard_layout,
            &shard_uid,
        ) {
            continue;
        }

        if !client_tracking_shard(client, shard_uid.shard_id(), &final_head.prev_block_hash) {
            continue;
        }

        let memtrie = get_memtrie_for_shard(client, &shard_uid, &final_head.prev_block_hash);
        let memtrie_state =
            memtrie.lock_for_iter().iter().unwrap().collect::<Result<HashSet<_>, _>>().unwrap();

        let state_root = *client
            .chain
            .get_chunk_extra(&final_head.prev_block_hash, &shard_uid)
            .unwrap()
            .state_root();

        // To get a view on disk tries we can leverage the fact that get_view_trie_for_shard() never
        // uses memtries.
        let trie = client
            .runtime_adapter
            .get_view_trie_for_shard(shard_uid.shard_id(), &final_head.prev_block_hash, state_root)
            .unwrap();
        assert!(!trie.has_memtries());
        let trie_state =
            trie.lock_for_iter().iter().unwrap().collect::<Result<HashSet<_>, _>>().unwrap();
        assert_state_equal(&memtrie_state, &trie_state, shard_uid, "memtrie and trie");

        let flat_storage_manager = client.chain.runtime_adapter.get_flat_storage_manager();
        // FlatStorageChunkView::iter_range() used below to retrieve all key-value pairs in Flat
        // Storage only looks at the data committed into the DB. For this reasons comparing Flat
        // Storage and Memtries makes sense only if we can retrieve a view at the same height from
        // both.
        if let FlatStorageStatus::Ready(status) =
            flat_storage_manager.get_flat_storage_status(shard_uid)
        {
            if status.flat_head.hash != final_head.prev_block_hash {
                tracing::warn!(target: "test", "skipping flat storage - memtrie state check");
                continue;
            } else {
                tracing::debug!(target: "test", "checking flat storage - memtrie state");
            }
        } else {
            continue;
        };
        let Some(flat_store_chunk_view) =
            flat_storage_manager.chunk_view(shard_uid, final_head.last_block_hash)
        else {
            continue;
        };
        let flat_store_state = flat_store_chunk_view
            .iter_range(None, None)
            .map_ok(|(key, value)| {
                let value = match value {
                    FlatStateValue::Ref(value) => client
                        .chain
                        .chain_store()
                        .store()
                        .trie_store()
                        .get(shard_uid, &value.hash)
                        .unwrap()
                        .to_vec(),
                    FlatStateValue::Inlined(data) => data,
                };
                (key, value)
            })
            .collect::<Result<HashSet<_>, _>>()
            .unwrap();

        assert_state_equal(&memtrie_state, &flat_store_state, shard_uid, "memtrie and flat store");
        checked_shards.push(shard_uid);
    }
    checked_shards
}

fn assert_state_equal(
    values1: &HashSet<(Vec<u8>, Vec<u8>)>,
    values2: &HashSet<(Vec<u8>, Vec<u8>)>,
    shard_uid: ShardUId,
    cmp_msg: &str,
) {
    let diff = values1.symmetric_difference(values2);
    let mut has_diff = false;
    for (key, value) in diff {
        has_diff = true;
        tracing::error!(target: "test", ?shard_uid, key=?key, ?value, "Difference in state between {}!", cmp_msg);
    }
    assert!(!has_diff, "{} state mismatch!", cmp_msg);
}

fn should_assert_state_sanity(
    load_memtries_for_tracked_shards: bool,
    is_resharded: bool,
    shards_pending_resharding: &HashSet<ShardUId>,
    shard_layout: &ShardLayout,
    shard_uid: &ShardUId,
) -> bool {
    // Always assert if the tracked shards are loaded into memory.
    if load_memtries_for_tracked_shards {
        return true;
    }

    // In the old layout do not enforce except for shards pending resharding.
    if !is_resharded && !shards_pending_resharding.contains(&shard_uid) {
        return false;
    }

    // In the new layout do not enforce for shards that were not split.
    if is_resharded && !shard_was_split(shard_layout, shard_uid.shard_id()) {
        return false;
    }

    true
}

/// Asserts that all parent shard State is accessible via parent and children shards.
pub fn check_state_shard_uid_mapping_after_resharding(
    client: &Client,
    resharding_block_hash: &CryptoHash,
    parent_shard_uid: ShardUId,
) {
    let tip = client.chain.head().unwrap();
    let epoch_id = tip.epoch_id;
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let children_shard_uids =
        shard_layout.get_children_shards_uids(parent_shard_uid.shard_id()).unwrap();
    assert_eq!(children_shard_uids.len(), 2);

    // Currently tracked shards.
    let tracked_shards = get_tracked_shards_from_prev_block(client, &tip.prev_block_hash);
    // ShardUId mappings (different than map to itself) that we have stored in DB.
    let mut shard_uid_mapping = HashMap::new();
    // Currently tracked children shards that are mapped to an ancestor.
    let mut tracked_mapped_children = vec![];
    let store = client.chain.chain_store.store();
    for child_shard_uid in &children_shard_uids {
        let mapped_shard_uid = get_shard_uid_mapping(&store, *child_shard_uid);
        if &mapped_shard_uid == child_shard_uid {
            continue;
        }
        shard_uid_mapping.insert(child_shard_uid, mapped_shard_uid);
        if tracked_shards.contains(child_shard_uid) {
            tracked_mapped_children.push(*child_shard_uid);
        }
    }
    // Currently we set the mapping for both children, or the mapping has been deleted.
    assert!(shard_uid_mapping.is_empty() || shard_uid_mapping.len() == 2);

    // Whether we found any value in DB for which we could test the mapping.
    let mut has_any_parent_shard_uid_prefix = false;
    let trie_store = store.trie_store();
    for kv in store.iter_raw_bytes(DBCol::State) {
        let (key, value) = kv.unwrap();
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        // Just after resharding, no State data must be keyed using children ShardUIds.
        assert!(!shard_uid_mapping.contains_key(&shard_uid));
        if shard_uid != parent_shard_uid {
            continue;
        }
        has_any_parent_shard_uid_prefix = true;
        let node_hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
        let (value, rc) = decode_value_with_rc(&value);
        // It is possible we have delayed receipts leftovers on disk,
        // that would result it `MissingTrieValue` if we attempt to read them through the Trie interface.
        assert!(rc > 0);
        let parent_value = trie_store.get(parent_shard_uid, &node_hash);
        // Sanity check: parent shard data must still be accessible using Trie interface and parent ShardUId.
        assert_eq!(&parent_value.unwrap()[..], value.unwrap());

        // All parent shard data is available via both children shards.
        for child_shard_uid in &tracked_mapped_children {
            let child_value = trie_store.get(*child_shard_uid, &node_hash);
            assert_eq!(&child_value.unwrap()[..], value.unwrap());
        }
    }
    // If we do not have the parent State, the ShardUId mapping has to be empty as well.
    if !has_any_parent_shard_uid_prefix {
        assert!(shard_uid_mapping.is_empty());
    }

    let shards_tracked_before_resharding = get_tracked_shards(client, resharding_block_hash);
    let tracked_parent_before_resharding =
        shards_tracked_before_resharding.contains(&parent_shard_uid);
    let shards_tracked_after_resharding =
        get_tracked_shards_from_prev_block(client, resharding_block_hash);

    // Sanity checks if the node tracks all shards (e.g. it is RPC node).
    if client.config.tracked_shards_config.tracks_all_shards() {
        assert_eq!(tracked_mapped_children.len(), 2);
        assert_eq!(shards_tracked_after_resharding.len(), shard_layout.num_shards() as usize,);
    }
    // If neither child shard was tracked after resharding, we do not have the mapping set.
    if children_shard_uids
        .iter()
        .all(|child_shard_uid| !shards_tracked_after_resharding.contains(child_shard_uid))
    {
        // Possible corner case is if parent was tracked before resharding, but no child was tracked after resharding.
        // TODO(resharding) Consider not resharding in such case. When fixed, the assert below should become unconditional.
        if !tracked_parent_before_resharding {
            assert!(shard_uid_mapping.is_empty());
        }
    }
}
