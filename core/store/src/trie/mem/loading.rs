use super::node::MemTrieNodeId;
use super::MemTries;
use crate::flat::store_helper::decode_flat_state_db_key;
use crate::trie::mem::construction::TrieConstructor;
use crate::{DBCol, Store};
use borsh::BorshSerialize;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::types::BlockHeight;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::time::Instant;
use tracing::{debug, info};

/// Loads a trie from the FlatState column. The returned `MemTries` contains
/// exactly one trie root.
pub fn load_trie_from_flat_state(
    store: &Store,
    shard_uid: ShardUId,
    state_root: CryptoHash,
    block_height: BlockHeight,
    maximum_arena_size: usize,
) -> anyhow::Result<MemTries> {
    let mut tries = MemTries::new(maximum_arena_size, shard_uid);

    tries.construct_root(block_height, |arena| -> anyhow::Result<MemTrieNodeId> {
        info!(target: "memtrie", shard_uid=%shard_uid, "Loading trie from flat state...");
        let load_start = Instant::now();
        let mut recon = TrieConstructor::new(arena);
        let mut num_keys_loaded = 0;
        for item in store
            .iter_prefix_ser::<FlatStateValue>(DBCol::FlatState, &shard_uid.try_to_vec().unwrap())
        {
            let (key, value) = item?;
            let (_, key) = decode_flat_state_db_key(&key)?;
            recon.add_leaf(&key, value);
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
        let root_id = recon.finalize();

        debug!(
            target: "memtrie",
            %shard_uid,
            "Loaded {} keys; computing hash and memory usage...",
            num_keys_loaded
        );
        let mut subtrees = Vec::new();
        root_id.as_ptr_mut(arena.memory_mut()).take_small_subtrees(1024 * 1024, &mut subtrees);
        subtrees.into_par_iter().for_each(|mut subtree| {
            subtree.compute_hash_recursively();
        });
        root_id.as_ptr_mut(arena.memory_mut()).compute_hash_recursively();
        info!(target: "memtrie", shard_uid=%shard_uid, "Done loading trie from flat state, took {:?}", load_start.elapsed());

        let root = root_id.as_ptr(arena.memory());
        assert_eq!(
            root.view().node_hash(), state_root,
            "In-memory trie for shard {} has incorrect state root", shard_uid);
        Ok(root.id())
    })?;
    Ok(tries)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{
        create_tries, simplify_changes, test_populate_flat_storage, test_populate_trie,
    };
    use crate::trie::mem::loading::load_trie_from_flat_state;
    use crate::trie::mem::lookup::memtrie_lookup;
    use crate::{KeyLookupMode, NibbleSlice, Trie, TrieUpdate};
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_vm_runner::logic::TrieNodesCount;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashSet;

    fn check(keys: Vec<Vec<u8>>) {
        let shard_tries = create_tries();
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
            &shard_tries.get_store(),
            shard_uid,
            state_root,
            123,
            64 * 1024 * 1024 * 1024,
        )
        .unwrap();
        eprintln!("In memory trie loaded");

        let trie_update = TrieUpdate::new(shard_tries.get_trie_for_shard(shard_uid, state_root));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let root = in_memory_trie.get_root(&state_root).unwrap();
        let mut cache = HashSet::new();
        let mut nodes_count = TrieNodesCount { db_reads: 0, mem_reads: 0 };
        for key in keys.iter() {
            let actual_value_ref = memtrie_lookup(root, key, Some(&mut cache), &mut nodes_count)
                .map(|v| v.to_value_ref());
            let expected_value_ref = trie.get_ref(key, KeyLookupMode::Trie).unwrap();
            assert_eq!(actual_value_ref, expected_value_ref, "{:?}", NibbleSlice::new(key));
            assert_eq!(&nodes_count, &trie.get_trie_nodes_count());
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

    #[test]
    fn test_memtrie_basic() {
        check(vec![vec![0, 1], vec![1, 0]]);
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
}
