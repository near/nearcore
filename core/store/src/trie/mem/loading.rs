use super::node::MemTrieNodeId;
use super::MemTries;
use crate::flat::store_helper::decode_flat_state_db_key;
use crate::trie::mem::construction::TrieConstructor;
use crate::{DBCol, Store};
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

    tries.construct_root(block_height, |arena| -> anyhow::Result<Option<MemTrieNodeId>> {
        info!(target: "memtrie", shard_uid=%shard_uid, "Loading trie from flat state...");
        let load_start = Instant::now();
        let mut recon = TrieConstructor::new(arena);
        let mut num_keys_loaded = 0;
        for item in store
            .iter_prefix_ser::<FlatStateValue>(DBCol::FlatState, &borsh::to_vec(&shard_uid).unwrap())
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
        let root_id = match recon.finalize() {
            Some(root_id) => root_id,
            None => {
                info!(target: "memtrie", shard_uid=%shard_uid, "No keys loaded, trie is empty");
                return Ok(None);
            }
        };

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
        Ok(Some(root.id()))
    })?;
    Ok(tries)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{
        simplify_changes, test_populate_flat_storage, test_populate_trie, TestTriesBuilder,
    };
    use crate::trie::mem::loading::load_trie_from_flat_state;
    use crate::trie::mem::lookup::memtrie_lookup;
    use crate::trie::OptimizedValueRef;
    use crate::{KeyLookupMode, NibbleSlice, Trie, TrieUpdate};
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_vm_runner::logic::TrieNodesCount;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashSet;

    fn check(keys: Vec<Vec<u8>>) {
        let shard_tries = TestTriesBuilder::new().build();
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

        if keys.is_empty() {
            assert_eq!(in_memory_trie.num_roots(), 0);
            return;
        }

        let trie_update = TrieUpdate::new(shard_tries.get_trie_for_shard(shard_uid, state_root));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let root = in_memory_trie.get_root(&state_root).unwrap();
        let mut cache = HashSet::new();
        let mut nodes_count = TrieNodesCount { db_reads: 0, mem_reads: 0 };
        for key in keys.iter() {
            let actual_value_ref = memtrie_lookup(root, key, Some(&mut cache), &mut nodes_count)
                .map(OptimizedValueRef::from_flat_value);
            let expected_value_ref = trie.get_optimized_ref(key, KeyLookupMode::Trie).unwrap();
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

    fn nibbles(hex: &str) -> Vec<u8> {
        if hex == "_" {
            return vec![];
        }
        assert!(hex.len() % 2 == 0);
        hex::decode(hex).unwrap()
    }

    fn all_nibbles(hexes: &str) -> Vec<Vec<u8>> {
        hexes.split_whitespace().map(|x| nibbles(x)).collect()
    }

    #[test]
    fn test_memtrie_empty() {
        check(vec![]);
    }

    #[test]
    fn test_memtrie_root_is_leaf() {
        check(all_nibbles("_"));
        check(all_nibbles("00"));
        check(all_nibbles("01"));
        check(all_nibbles("ff"));
        check(all_nibbles("0123456789abcdef"));
    }

    #[test]
    fn test_memtrie_root_is_extension() {
        check(all_nibbles("1234 13 14"));
        check(all_nibbles("12345678 1234abcd"));
    }

    #[test]
    fn test_memtrie_root_is_branch() {
        check(all_nibbles("11 22"));
        check(all_nibbles("12345678 22345678 32345678"));
        check(all_nibbles("11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff"));
    }

    #[test]
    fn test_memtrie_root_is_branch_with_value() {
        check(all_nibbles("_ 11"));
    }

    #[test]
    fn test_memtrie_prefix_patterns() {
        check(all_nibbles("10 21 2210 2221 222210 222221 22222210 22222221"));
        check(all_nibbles("11111112 11111120 111112 111120 1112 1120 12 20"));
        check(all_nibbles("11 1111 111111 11111111 1111111111 111111111111"));
        check(all_nibbles("_ 11 1111 111111 11111111 1111111111 111111111111"));
    }

    #[test]
    fn test_full_16ary_trees() {
        check(all_nibbles(
            "
            00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
            10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f
            20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f
            30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f
            40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f
            50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f
            60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f
            70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f
            80 81 82 83 84 85 86 87 88 89 8a 8b 8c 8d 8e 8f
            90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f
            a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 aa ab ac ad ae af
            b0 b1 b2 b3 b4 b5 b6 b7 b8 b9 ba bb bc bd be bf
            c0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf
            d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 da db dc dd de df
            e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 ea eb ec ed ee ef
            f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff
        ",
        ))
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
