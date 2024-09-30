use super::arena::single_thread::STArena;
use super::mem_tries::MemTries;
use super::node::MemTrieNodeId;
use crate::adapter::StoreAdapter;
use crate::flat::FlatStorageStatus;
use crate::trie::mem::arena::Arena;
use crate::trie::mem::construction::TrieConstructor;
use crate::trie::mem::parallel_loader::load_memtrie_in_parallel;
use crate::{DBCol, NibbleSlice, Store};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{get_block_shard_uid, ShardUId};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, StateRoot};
use std::collections::BTreeSet;
use std::time::Instant;
use tracing::{debug, info};

/// Loads a trie from the FlatState column. The returned `MemTries` contains
/// exactly one trie root.
///
/// `parallelize` can be used to speed up reading from db. However, it should
/// only be used when no other work is being done, such as during initial
/// startup. It also incurs a higher peak memory usage.
fn load_trie_from_flat_state(
    store: &Store,
    shard_uid: ShardUId,
    state_root: StateRoot,
    block_height: BlockHeight,
    parallelize: bool,
) -> Result<MemTries, StorageError> {
    if state_root == StateRoot::default() {
        return Ok(MemTries::new(shard_uid));
    }

    let load_start = Instant::now();
    let (arena, root_id) = if parallelize {
        const NUM_PARALLEL_SUBTREES_DESIRED: usize = 256;
        load_memtrie_in_parallel(
            store.trie_store(),
            shard_uid,
            state_root,
            NUM_PARALLEL_SUBTREES_DESIRED,
            shard_uid.to_string(),
        )?
    } else {
        load_memtrie_single_thread(store, shard_uid)?
    };

    info!(target: "memtrie", shard_uid=%shard_uid, "Done loading trie from flat state, took {:?}", load_start.elapsed());
    let root = root_id.as_ptr(arena.memory());
    assert_eq!(
        root.view().node_hash(),
        state_root,
        "In-memory trie for shard {} has incorrect state root",
        shard_uid
    );
    Ok(MemTries::new_from_arena_and_root(shard_uid, block_height, arena, root_id))
}

fn load_memtrie_single_thread(
    store: &Store,
    shard_uid: ShardUId,
) -> Result<(STArena, MemTrieNodeId), StorageError> {
    info!(target: "memtrie", shard_uid=%shard_uid, "Loading trie from flat state...");
    let mut arena = STArena::new(shard_uid.to_string());
    let mut recon = TrieConstructor::new(&mut arena);
    let mut num_keys_loaded = 0;
    for item in store.flat_store().iter(shard_uid) {
        let (key, value) = item?;
        recon.add_leaf(NibbleSlice::new(&key), value);
        num_keys_loaded += 1;
        if num_keys_loaded % 1000000 == 0 {
            debug!(
                target: "memtrie",
                %shard_uid,
                "Loaded {} keys, current key: {}",
                num_keys_loaded,
                hex::encode(&key)
            );
        }
    }
    let root_id = recon.finalize().expect("state root cannot be empty");
    Ok((arena, root_id))
}

fn get_state_root(
    store: &Store,
    block_hash: CryptoHash,
    shard_uid: ShardUId,
) -> Result<StateRoot, StorageError> {
    let chunk_extra = store
        .get_ser::<ChunkExtra>(DBCol::ChunkExtra, &get_block_shard_uid(&block_hash, &shard_uid))
        .map_err(|err| {
            StorageError::StorageInconsistentState(format!(
                "Cannot fetch ChunkExtra for block {} in shard {}: {:?}",
                block_hash, shard_uid, err
            ))
        })?
        .ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "No ChunkExtra for block {} in shard {}",
                block_hash, shard_uid
            ))
        })?;
    Ok(*chunk_extra.state_root())
}

/// Constructs in-memory tries for the given shard, so that they represent the
/// same information as the flat storage, including the final state and the
/// deltas. The returned tries would contain a root for each block that the
/// flat storage currently has, i.e. one for the final block, and one for each
/// block that flat storage has a delta for, possibly in more than one fork.
/// `state_root` parameter is required if `ChunkExtra` is not available, e.g. on catchup.
pub fn load_trie_from_flat_state_and_delta(
    store: &Store,
    shard_uid: ShardUId,
    state_root: Option<StateRoot>,
    parallelize: bool,
) -> Result<MemTries, StorageError> {
    debug!(target: "memtrie", %shard_uid, "Loading base trie from flat state...");
    let flat_store = store.flat_store();
    let flat_head = match flat_store.get_flat_storage_status(shard_uid)? {
        FlatStorageStatus::Ready(status) => status.flat_head,
        other => {
            return Err(StorageError::MemTrieLoadingError(format!(
                            "Cannot load memtries when flat storage is not ready for shard {}, actual status: {:?}",
                            shard_uid, other
                        )));
        }
    };

    let state_root = match state_root {
        Some(state_root) => state_root,
        None => get_state_root(store, flat_head.hash, shard_uid)?,
    };

    let mut mem_tries =
        load_trie_from_flat_state(&store, shard_uid, state_root, flat_head.height, parallelize)
            .unwrap();

    debug!(target: "memtrie", %shard_uid, "Loading flat state deltas...");
    // We load the deltas in order of height, so that we always have the previous state root
    // already loaded.
    let mut sorted_deltas: BTreeSet<(BlockHeight, CryptoHash, CryptoHash)> = Default::default();
    for delta in flat_store.get_all_deltas_metadata(shard_uid).unwrap() {
        sorted_deltas.insert((delta.block.height, delta.block.hash, delta.block.prev_hash));
    }

    debug!(target: "memtrie", %shard_uid, "{} deltas to apply", sorted_deltas.len());
    for (height, hash, prev_hash) in sorted_deltas.into_iter() {
        let delta = flat_store.get_delta(shard_uid, hash).unwrap();
        if let Some(changes) = delta {
            let old_state_root = get_state_root(store, prev_hash, shard_uid)?;
            let new_state_root = get_state_root(store, hash, shard_uid)?;

            let mut trie_update = mem_tries.update(old_state_root, false)?;
            for (key, value) in changes.0 {
                match value {
                    Some(value) => {
                        trie_update.insert_memtrie_only(&key, value);
                    }
                    None => trie_update.delete(&key),
                };
            }

            let mem_trie_changes = trie_update.to_mem_trie_changes_only();
            let new_root_after_apply = mem_tries.apply_memtrie_changes(height, &mem_trie_changes);
            assert_eq!(new_root_after_apply, new_state_root);
        }
        debug!(target: "memtrie", %shard_uid, "Applied memtrie changes for height {}", height);
    }

    debug!(target: "memtrie", %shard_uid, "Done loading memtries for shard");
    Ok(mem_tries)
}

#[cfg(test)]
mod tests {
    use super::load_trie_from_flat_state_and_delta;
    use crate::adapter::{StoreAdapter, StoreUpdateAdapter};
    use crate::flat::test_utils::MockChain;
    use crate::flat::{BlockInfo, FlatStorageReadyStatus, FlatStorageStatus};
    use crate::test_utils::{
        create_test_store, simplify_changes, test_populate_flat_storage, test_populate_trie,
        TestTriesBuilder,
    };
    use crate::trie::mem::loading::load_trie_from_flat_state;
    use crate::trie::mem::lookup::memtrie_lookup;
    use crate::trie::mem::nibbles_utils::{all_two_nibble_nibbles, multi_hex_to_nibbles};
    use crate::{DBCol, KeyLookupMode, NibbleSlice, ShardTries, Store, Trie, TrieUpdate};
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::{get_block_shard_uid, ShardUId};
    use near_primitives::state::FlatStateValue;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::{StateChangeCause, StateRoot};
    use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn check_maybe_parallelize(keys: Vec<Vec<u8>>, parallelize: bool) {
        let shard_tries = TestTriesBuilder::new().with_flat_storage(true).build();
        let shard_uid = ShardUId::single_shard();
        let changes = keys.iter().map(|key| (key.to_vec(), Some(key.to_vec()))).collect::<Vec<_>>();
        let changes = simplify_changes(&changes);
        test_populate_flat_storage(
            &shard_tries,
            shard_uid,
            &CryptoHash::default(),
            &CryptoHash::default(),
            &changes,
        );
        let state_root = test_populate_trie(&shard_tries, &Trie::EMPTY_ROOT, shard_uid, changes);

        eprintln!("Trie and flat storage populated");
        let in_memory_trie = load_trie_from_flat_state(
            &shard_tries.store().store(),
            shard_uid,
            state_root,
            123,
            parallelize,
        )
        .unwrap();
        eprintln!("In memory trie loaded");

        if keys.is_empty() {
            assert_eq!(in_memory_trie.num_roots(), 0);
            return;
        }

        let trie_update = TrieUpdate::new(shard_tries.get_trie_with_block_hash_for_shard(
            shard_uid,
            state_root,
            &CryptoHash::default(),
            false,
        ));
        let _mode_guard = trie_update
            .with_trie_cache_mode(Some(near_primitives::types::TrieCacheMode::CachingChunk));
        let trie = trie_update.trie();
        let root = in_memory_trie.get_root(&state_root).unwrap();

        // Check access to each key to make sure the in-memory trie is consistent with
        // real trie. Check non-existent keys too.
        for key in keys.iter().chain([b"not in trie".to_vec()].iter()) {
            let mut nodes_accessed = Vec::new();
            let actual_value_ref = memtrie_lookup(root, key, Some(&mut nodes_accessed))
                .map(|v| v.to_optimized_value_ref());
            let expected_value_ref =
                trie.get_optimized_ref(key, KeyLookupMode::FlatStorage).unwrap();
            assert_eq!(actual_value_ref, expected_value_ref, "{:?}", NibbleSlice::new(key));

            // Do another access with the trie to see how many nodes we're supposed to
            // have accessed.
            let temp_trie = shard_tries.get_trie_for_shard(shard_uid, state_root);
            temp_trie.get_optimized_ref(key, KeyLookupMode::Trie).unwrap();
            assert_eq!(
                temp_trie.get_trie_nodes_count().db_reads,
                nodes_accessed.len() as u64,
                "Number of accessed nodes does not equal number of trie nodes along the way"
            );

            // Check that the accessed nodes are consistent with those from disk.
            for (node_hash, serialized_node) in nodes_accessed {
                let expected_serialized_node =
                    trie.internal_retrieve_trie_node(&node_hash, false, true).unwrap();
                assert_eq!(expected_serialized_node, serialized_node);
            }
        }
    }

    fn check_random(max_key_len: usize, max_keys_count: usize, test_count: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..test_count {
            let key_cnt = rng.gen_range(1..=max_keys_count);
            let mut keys = Vec::new();
            for _ in 0..key_cnt {
                let mut key = Vec::new();
                let key_len = rng.gen_range(0..=max_key_len);
                for _ in 0..key_len {
                    let byte: u8 = rng.gen();
                    key.push(byte);
                }
                keys.push(key);
            }
            check(keys);
        }
    }

    fn check(keys: Vec<Vec<u8>>) {
        check_maybe_parallelize(keys.clone(), false);
        check_maybe_parallelize(keys, true);
    }

    #[test]
    fn test_memtrie_empty() {
        check(vec![]);
    }

    #[test]
    fn test_memtrie_root_is_leaf() {
        check(multi_hex_to_nibbles("_"));
        check(multi_hex_to_nibbles("00"));
        check(multi_hex_to_nibbles("01"));
        check(multi_hex_to_nibbles("ff"));
        check(multi_hex_to_nibbles("0123456789abcdef"));
    }

    #[test]
    fn test_memtrie_root_is_extension() {
        check(multi_hex_to_nibbles("1234 13 14"));
        check(multi_hex_to_nibbles("12345678 1234abcd"));
    }

    #[test]
    fn test_memtrie_root_is_branch() {
        check(multi_hex_to_nibbles("11 22"));
        check(multi_hex_to_nibbles("12345678 22345678 32345678"));
        check(multi_hex_to_nibbles("11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff"));
    }

    #[test]
    fn test_memtrie_root_is_branch_with_value() {
        check(multi_hex_to_nibbles("_ 11"));
    }

    #[test]
    fn test_memtrie_prefix_patterns() {
        check(multi_hex_to_nibbles("10 21 2210 2221 222210 222221 22222210 22222221"));
        check(multi_hex_to_nibbles("11111112 11111120 111112 111120 1112 1120 12 20"));
        check(multi_hex_to_nibbles("11 1111 111111 11111111 1111111111 111111111111"));
        check(multi_hex_to_nibbles("_ 11 1111 111111 11111111 1111111111 111111111111"));
    }

    #[test]
    fn test_full_16ary_trees() {
        check(all_two_nibble_nibbles())
    }

    #[test]
    fn test_memtrie_rand_small() {
        check_random(3, 20, 10);
    }

    #[test]
    fn test_memtrie_rand_many_keys() {
        check_random(5, 1000, 10);
    }

    #[test]
    fn test_memtrie_rand_long_keys() {
        check_random(20, 100, 10);
    }

    #[test]
    fn test_memtrie_rand_long_long_keys() {
        check_random(1000, 1000, 1);
    }

    #[test]
    fn test_memtrie_rand_large_data() {
        check_random(32, 100000, 1);
    }

    #[test]
    fn test_memtrie_load_with_delta() {
        let test_key = TrieKey::ContractData {
            account_id: "test_account".parse().unwrap(),
            key: b"test_key".to_vec(),
        };
        let test_val0 = b"test_val0".to_vec();
        let test_val1 = b"test_val1".to_vec();
        let test_val2 = b"test_val2".to_vec();
        let test_val3 = b"test_val3".to_vec();
        let test_val4 = b"test_val4".to_vec();

        // A chain with two forks.
        // 0 |-> 1 -> 3
        //   --> 2 -> 4
        let chain = MockChain::chain_with_two_forks(5);
        let store = create_test_store();
        let shard_tries = TestTriesBuilder::new().with_store(store.clone()).build();
        let shard_uid = ShardUId { version: 1, shard_id: 1 };

        // Populate the initial flat storage state at block 0.
        let mut store_update = shard_tries.store().flat_store().store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        store_update.set(shard_uid, test_key.to_vec(), Some(FlatStateValue::inlined(&test_val0)));
        store_update.commit().unwrap();

        // Populate the initial trie at block 0 too.
        let state_root_0 = test_populate_trie(
            &shard_tries,
            &Trie::EMPTY_ROOT,
            shard_uid,
            vec![(test_key.to_vec(), Some(test_val0.clone()))],
        );
        write_chunk_extra(&store, chain.get_block(0).hash, shard_uid, state_root_0);

        // Apply four changes to the trie, and for each, a flat storage delta.
        let state_root_1 = apply_trie_changes(
            &shard_tries,
            shard_uid,
            state_root_0,
            chain.get_block(1),
            vec![(test_key.clone(), test_val1.clone())],
        );
        write_chunk_extra(&store, chain.get_block(1).hash, shard_uid, state_root_1);

        let state_root_2 = apply_trie_changes(
            &shard_tries,
            shard_uid,
            state_root_0,
            chain.get_block(2),
            vec![(test_key.clone(), test_val2.clone())],
        );
        write_chunk_extra(&store, chain.get_block(2).hash, shard_uid, state_root_2);

        let state_root_3 = apply_trie_changes(
            &shard_tries,
            shard_uid,
            state_root_1,
            chain.get_block(3),
            vec![(test_key.clone(), test_val3.clone())],
        );
        write_chunk_extra(&store, chain.get_block(3).hash, shard_uid, state_root_3);

        let state_root_4 = apply_trie_changes(
            &shard_tries,
            shard_uid,
            state_root_2,
            chain.get_block(4),
            vec![(test_key.clone(), test_val4.clone())],
        );
        write_chunk_extra(&store, chain.get_block(4).hash, shard_uid, state_root_4);

        // Load into memory. It should load the base flat state (block 0), plus all
        // four deltas. We'll check against the state roots at each block; they should
        // all exist in the loaded memtrie.
        let mem_tries = load_trie_from_flat_state_and_delta(&store, shard_uid, None, true).unwrap();

        assert_eq!(
            memtrie_lookup(mem_tries.get_root(&state_root_0).unwrap(), &test_key.to_vec(), None)
                .map(|v| v.to_flat_value()),
            Some(FlatStateValue::inlined(&test_val0))
        );
        assert_eq!(
            memtrie_lookup(mem_tries.get_root(&state_root_1).unwrap(), &test_key.to_vec(), None)
                .map(|v| v.to_flat_value()),
            Some(FlatStateValue::inlined(&test_val1))
        );
        assert_eq!(
            memtrie_lookup(mem_tries.get_root(&state_root_2).unwrap(), &test_key.to_vec(), None)
                .map(|v| v.to_flat_value()),
            Some(FlatStateValue::inlined(&test_val2))
        );
        assert_eq!(
            memtrie_lookup(mem_tries.get_root(&state_root_3).unwrap(), &test_key.to_vec(), None)
                .map(|v| v.to_flat_value()),
            Some(FlatStateValue::inlined(&test_val3))
        );
        assert_eq!(
            memtrie_lookup(mem_tries.get_root(&state_root_4).unwrap(), &test_key.to_vec(), None)
                .map(|v| v.to_flat_value()),
            Some(FlatStateValue::inlined(&test_val4))
        );
    }

    /// Makes the given changes to both the trie and flat storage.
    fn apply_trie_changes(
        tries: &ShardTries,
        shard_uid: ShardUId,
        old_state_root: StateRoot,
        block: BlockInfo,
        changes: Vec<(TrieKey, Vec<u8>)>,
    ) -> StateRoot {
        let mut trie_update = tries.new_trie_update(shard_uid, old_state_root);
        for (key, value) in changes {
            trie_update.set(key, value);
        }
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (_, trie_changes, state_changes) = trie_update.finalize().unwrap();
        let mut store_update = tries.store_update();
        tries.apply_insertions(&trie_changes, shard_uid, &mut store_update);
        store_update.store_update().merge(
            tries
                .get_flat_storage_manager()
                .save_flat_state_changes(
                    block.hash,
                    block.prev_hash,
                    block.height,
                    shard_uid,
                    &state_changes,
                )
                .unwrap()
                .into(),
        );
        store_update.commit().unwrap();

        trie_changes.new_root
    }

    /// Writes the state root into chunk extra for the given block and shard,
    /// because the loading code needs it to look up the state roots.
    fn write_chunk_extra(
        store: &Store,
        block_hash: CryptoHash,
        shard_uid: ShardUId,
        state_root: StateRoot,
    ) {
        let congestion_info = ProtocolFeature::CongestionControl
            .enabled(PROTOCOL_VERSION)
            .then(CongestionInfo::default);

        let chunk_extra = ChunkExtra::new(
            PROTOCOL_VERSION,
            &state_root,
            CryptoHash::default(),
            Vec::new(),
            0,
            0,
            0,
            congestion_info,
        );
        let mut store_update = store.store_update();
        store_update
            .set_ser(DBCol::ChunkExtra, &get_block_shard_uid(&block_hash, &shard_uid), &chunk_extra)
            .unwrap();
        store_update.commit().unwrap();
    }
}
