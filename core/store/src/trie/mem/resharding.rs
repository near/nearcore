#[cfg(test)]
use crate::trie::ops::interface::GenericNodeOrIndex;
use crate::trie::ops::resharding::{
    boundary_account_to_intervals, intervals_to_nibbles, GenericTrieUpdateRetain, RetainMode,
};
use crate::trie::trie_storage_update::TrieStorageUpdate;
use crate::{Trie, TrieChanges};

use super::arena::ArenaMemory;
use super::mem_trie_update::MemTrieUpdate;
use near_primitives::errors::StorageError;
use near_primitives::types::{AccountId, StateRoot};
use std::ops::Range;

impl<'a, M: ArenaMemory> MemTrieUpdate<'a, M> {
    /// Splits the trie, separating entries by the boundary account.
    /// Leaves the left or right part of the trie, depending on the retain mode.
    ///
    /// Returns the changes to be applied to in-memory trie and the proof of
    /// the split operation. Doesn't modifies trie itself, it's a caller's
    /// responsibility to apply the changes.
    pub fn retain_split_shard(
        self,
        boundary_account: &AccountId,
        retain_mode: RetainMode,
    ) -> TrieChanges {
        let intervals = boundary_account_to_intervals(boundary_account, retain_mode);
        self.retain_multi_range(&intervals)
    }

    /// Retains keys belonging to any of the ranges given in `intervals` from
    /// the trie.
    ///
    /// Returns changes to be applied to in-memory trie and proof of the
    /// retain operation.
    fn retain_multi_range(mut self, intervals: &[Range<Vec<u8>>]) -> TrieChanges {
        debug_assert!(intervals.iter().all(|range| range.start < range.end));
        let intervals_nibbles = intervals_to_nibbles(intervals);

        // TODO(resharding): consider handling the case when no changes are made.
        self.retain_multi_range_recursive(0, vec![], &intervals_nibbles).unwrap();
        self.to_trie_changes()
    }
}

impl Trie {
    pub fn retain_split_shard(
        &self,
        boundary_account: &AccountId,
        retain_mode: RetainMode,
    ) -> Result<StateRoot, StorageError> {
        let intervals = boundary_account_to_intervals(boundary_account, retain_mode);
        self.retain_multi_range(&intervals)
    }

    fn retain_multi_range(&self, intervals: &[Range<Vec<u8>>]) -> Result<StateRoot, StorageError> {
        debug_assert!(intervals.iter().all(|range| range.start < range.end));
        let intervals_nibbles = intervals_to_nibbles(intervals);

        let mut trie_update = TrieStorageUpdate::new(&self);
        let root_node = self.move_node_to_mutable(&mut trie_update, &self.root)?;

        trie_update.retain_multi_range_recursive(0, vec![], &intervals_nibbles).unwrap();

        #[cfg(test)]
        {
            self.memory_usage_verify(&trie_update, GenericNodeOrIndex::Updated(root_node.0));
        }
        let result = trie_update.flatten_nodes(&self.root, root_node.0)?;
        Ok(result.new_root)
    }
}

// TODO(resharding): consider adding tests for `retain_split_shard` API.
#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use std::ops::Range;
    use std::sync::Arc;

    use itertools::Itertools;
    use near_primitives::{shard_layout::ShardUId, types::StateRoot};

    use crate::test_utils::TestTriesBuilder;
    use crate::trie::mem::iter::MemTrieIterator;
    use crate::trie::mem::mem_trie_update::TrackingMode;
    use crate::trie::mem::mem_tries::MemTries;
    use crate::trie::mem::nibbles_utils::{
        all_two_nibble_nibbles, hex_to_nibbles, multi_hex_to_nibbles,
    };
    use crate::trie::trie_recording::TrieRecorder;
    use crate::trie::trie_storage::TrieMemoryPartialStorage;
    use crate::trie::Trie;

    // Logic for a single test.
    // Creates trie from initial entries, applies retain multi range to it and
    // compares the result with naive approach.
    fn run(initial_entries: Vec<(Vec<u8>, Vec<u8>)>, retain_multi_ranges: Vec<Range<Vec<u8>>>) {
        // Generate naive result and state root.
        let mut retain_result_naive = initial_entries
            .iter()
            .filter(|&(key, _)| retain_multi_ranges.iter().any(|range| range.contains(key)))
            .cloned()
            .collect_vec();
        retain_result_naive.sort();

        let shard_tries = TestTriesBuilder::new().build();
        let changes = retain_result_naive
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect_vec();
        let expected_naive_state_root = crate::test_utils::test_populate_trie(
            &shard_tries,
            &Trie::EMPTY_ROOT,
            ShardUId::single_shard(),
            changes,
        );

        let shard_tries = TestTriesBuilder::new().build();
        let initial_changes = initial_entries
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect_vec();
        let initial_state_root = crate::test_utils::test_populate_trie(
            &shard_tries,
            &Trie::EMPTY_ROOT,
            ShardUId::single_shard(),
            initial_changes,
        );
        let trie = shard_tries.get_trie_for_shard(ShardUId::single_shard(), initial_state_root);
        let expected_disk_state_root = trie.retain_multi_range(&retain_multi_ranges).unwrap();

        let mut memtries = MemTries::new(ShardUId::single_shard());
        let mut update = memtries.update(Trie::EMPTY_ROOT, TrackingMode::None).unwrap();
        for (key, value) in initial_entries {
            update.insert(&key, value).unwrap();
        }
        let memtrie_changes = update.to_mem_trie_changes_only();
        let state_root = memtries.apply_memtrie_changes(0, &memtrie_changes);

        let mut trie_recorder = TrieRecorder::new();
        let mode = TrackingMode::RefcountsAndAccesses(&mut trie_recorder);
        let update = memtries.update(state_root, mode).unwrap();
        let mut trie_changes = update.retain_multi_range(&retain_multi_ranges);
        let memtrie_changes = trie_changes.mem_trie_changes.take().unwrap();
        let mem_state_root = memtries.apply_memtrie_changes(1, &memtrie_changes);
        let proof = trie_recorder.recorded_storage();

        let partial_trie = Trie::from_recorded_storage(proof, state_root, false);
        let expected_proof_based_state_root =
            partial_trie.retain_multi_range(&retain_multi_ranges).unwrap();

        let entries = if mem_state_root != StateRoot::default() {
            let state_root_ptr = memtries.get_root(&mem_state_root).unwrap();
            let trie =
                Trie::new(Arc::new(TrieMemoryPartialStorage::default()), mem_state_root, None);
            MemTrieIterator::new(Some(state_root_ptr), &trie).map(|e| e.unwrap()).collect_vec()
        } else {
            vec![]
        };

        // Check entries first to provide more context in case of failure.
        assert_eq!(entries, retain_result_naive);

        // Check state root, because it must be unique.
        assert_eq!(mem_state_root, expected_naive_state_root);

        // Check state root with disk-trie state root.
        assert_eq!(mem_state_root, expected_disk_state_root);

        // Check state root resulting by retain based on partial storage.
        assert_eq!(mem_state_root, expected_proof_based_state_root);
    }

    #[test]
    /// Applies single range retain to the trie and checks the result.
    fn test_retain_single_range() {
        let initial_entries = vec![
            (b"alice".to_vec(), vec![1]),
            (b"bob".to_vec(), vec![2]),
            (b"charlie".to_vec(), vec![3]),
            (b"david".to_vec(), vec![4]),
        ];
        let retain_range = b"amy".to_vec()..b"david".to_vec();
        run(initial_entries, vec![retain_range]);
    }

    #[test]
    /// Applies two ranges retain to the trie and checks the result.
    fn test_retain_two_ranges() {
        let initial_entries = vec![
            (b"alice".to_vec(), vec![1]),
            (b"bob".to_vec(), vec![2]),
            (b"charlie".to_vec(), vec![3]),
            (b"david".to_vec(), vec![4]),
            (b"edward".to_vec(), vec![5]),
            (b"frank".to_vec(), vec![6]),
        ];
        let retain_ranges =
            vec![b"bill".to_vec()..b"bowl".to_vec(), b"daaa".to_vec()..b"france".to_vec()];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when no keys are retained.
    fn test_empty_result() {
        let initial_entries = vec![
            (b"alice".to_vec(), vec![1]),
            (b"miles".to_vec(), vec![2]),
            (b"willy".to_vec(), vec![3]),
        ];
        let retain_ranges = vec![b"ellie".to_vec()..b"key".to_vec()];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when all keys are retained.
    fn test_full_result() {
        let initial_entries = vec![
            (b"f23".to_vec(), vec![1]),
            (b"f32".to_vec(), vec![2]),
            (b"f44".to_vec(), vec![3]),
        ];
        let retain_ranges = vec![b"f11".to_vec()..b"f45".to_vec()];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks empty trie.
    fn test_empty_trie() {
        let initial_entries = vec![];
        let retain_ranges = vec![b"bar".to_vec()..b"foo".to_vec()];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when all keys are prefixes of some string.
    fn test_prefixes() {
        let initial_entries = vec![
            (b"a".to_vec(), vec![1]),
            (b"aa".to_vec(), vec![2]),
            (b"aaa".to_vec(), vec![3]),
            (b"aaaa".to_vec(), vec![1]),
            (b"aaaaa".to_vec(), vec![2]),
            (b"aaaaaa".to_vec(), vec![3]),
        ];
        let retain_ranges = vec![b"aa".to_vec()..b"aaaaa".to_vec()];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when branch and extension nodes are explored but completely
    /// removed.
    fn test_descend_and_remove() {
        let keys = multi_hex_to_nibbles("00 0000 0011");
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![hex_to_nibbles("0001")..hex_to_nibbles("0010")];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when branch is converted to leaf.
    fn test_branch_to_leaf() {
        let keys = multi_hex_to_nibbles("ba bc ca");
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![hex_to_nibbles("bc")..hex_to_nibbles("be")];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when branch with value is converted to leaf.
    fn test_branch_with_value_to_leaf() {
        let keys = multi_hex_to_nibbles("d4 d4a3 d4b9 d5 e6");
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![hex_to_nibbles("d4")..hex_to_nibbles("d4a0")];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when branch without value is converted to extension.
    fn test_branch_to_extension() {
        let keys = multi_hex_to_nibbles("21 2200 2201");
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![hex_to_nibbles("2200")..hex_to_nibbles("2202")];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when result is a single key, and all nodes on the way are
    /// squashed, in particular, extension nodes are joined into one.
    fn test_extend_extensions() {
        let keys = multi_hex_to_nibbles("dd d0 d1 dddd00 dddd01 dddddd");
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![hex_to_nibbles("dddddd")..hex_to_nibbles("ddddde")];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case when branch is visited but not restructured.
    fn test_branch_not_restructured() {
        let keys = multi_hex_to_nibbles("60 61 62 70");
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![hex_to_nibbles("61")..hex_to_nibbles("71")];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks case with branching on every step but when only prefixes of some
    /// key are retained.
    fn test_branch_prefixes() {
        let keys = multi_hex_to_nibbles(
            "
            00
            10
            01
            0000
            0010
            0001
            000000
            000010
            000001
            00000000
            00000010
            00000001
            0000000000
            0000000010
            0000000001
            000000000000
            000000000010
            000000000011
            ",
        );
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![hex_to_nibbles("0000")..hex_to_nibbles("00000000")];
        run(initial_entries, retain_ranges);
    }

    #[test]
    /// Checks multiple ranges retain on full 16-ary tree.
    fn test_full_16ary() {
        let keys = all_two_nibble_nibbles();
        let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
        let retain_ranges = vec![
            hex_to_nibbles("0f")..hex_to_nibbles("10"),
            hex_to_nibbles("20")..hex_to_nibbles("2fff"),
            hex_to_nibbles("55")..hex_to_nibbles("56"),
            hex_to_nibbles("a5aa")..hex_to_nibbles("c3"),
            hex_to_nibbles("c3")..hex_to_nibbles("c5"),
            hex_to_nibbles("c8")..hex_to_nibbles("ca"),
            hex_to_nibbles("cb")..hex_to_nibbles("cc"),
        ];
        run(initial_entries, retain_ranges);
    }

    fn random_key(max_key_len: usize, rng: &mut StdRng) -> Vec<u8> {
        let key_len = rng.gen_range(0..=max_key_len);
        let mut key = Vec::new();
        for _ in 0..key_len {
            let byte: u8 = rng.gen();
            key.push(byte);
        }
        key
    }

    fn check_random(max_key_len: usize, max_keys_count: usize, test_count: usize) {
        let mut rng = StdRng::seed_from_u64(442);
        for _ in 0..test_count {
            let key_cnt = rng.gen_range(1..=max_keys_count);
            let mut keys = Vec::new();
            for _ in 0..key_cnt {
                keys.push(random_key(max_key_len, &mut rng));
            }
            keys.sort();
            keys.dedup();
            keys.shuffle(&mut rng);

            let mut boundary_left = random_key(max_key_len, &mut rng);
            let mut boundary_right = random_key(max_key_len, &mut rng);
            if boundary_left == boundary_right {
                continue;
            }
            if boundary_left > boundary_right {
                std::mem::swap(&mut boundary_left, &mut boundary_right);
            }
            let initial_entries = keys.into_iter().map(|key| (key, vec![1])).collect_vec();
            let retain_ranges = vec![boundary_left..boundary_right];
            run(initial_entries, retain_ranges);
        }
    }

    #[test]
    fn test_rand_small() {
        check_random(3, 20, 10);
    }

    #[test]
    fn test_rand_many_keys() {
        check_random(5, 1000, 10);
    }

    #[test]
    fn test_rand_long_keys() {
        check_random(20, 100, 10);
    }

    #[test]
    fn test_rand_long_long_keys() {
        check_random(1000, 1000, 1);
    }

    #[test]
    fn test_rand_large_data() {
        check_random(32, 100000, 1);
    }
}
