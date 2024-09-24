use crate::{NibbleSlice, TrieChanges};

use super::arena::ArenaMemory;
use super::updating::{MemTrieUpdate, OldOrUpdatedNodeId, TrieAccesses, UpdatedMemTrieNode};
use itertools::Itertools;
use near_primitives::types::AccountId;
use std::ops::Range;

/// Whether to retain left or right part of trie after shard split.
pub enum RetainMode {
    Left,
    Right,
}

/// Decision on the subtree exploration.
#[derive(Debug)]
enum RetainDecision {
    /// Retain the whole subtree.
    RetainAll,
    /// The whole subtree is not retained.
    DiscardAll,
    /// Descend into all child subtrees.
    Descend,
}

impl<'a, M: ArenaMemory> MemTrieUpdate<'a, M> {
    /// Splits the trie, separating entries by the boundary account.
    /// Leaves the left or right part of the trie, depending on the retain mode.
    ///
    /// Returns the changes to be applied to in-memory trie and the proof of
    /// the split operation. Doesn't modifies trie itself, it's a caller's
    /// responsibility to apply the changes.
    pub fn retain_split_shard(
        self,
        _boundary_account: AccountId,
        _retain_mode: RetainMode,
    ) -> (TrieChanges, TrieAccesses) {
        // TODO(#12074): generate intervals in nibbles.

        self.retain_multi_range(&[])
    }

    /// Retains keys belonging to any of the ranges given in `intervals` from
    /// the trie.
    ///
    /// Returns changes to be applied to in-memory trie and proof of the
    /// retain operation.
    fn retain_multi_range(mut self, intervals: &[Range<Vec<u8>>]) -> (TrieChanges, TrieAccesses) {
        debug_assert!(intervals.iter().all(|range| range.start < range.end));
        let intervals_nibbles = intervals
            .iter()
            .map(|range| {
                NibbleSlice::new(&range.start).iter().collect_vec()
                    ..NibbleSlice::new(&range.end).iter().collect_vec()
            })
            .collect_vec();

        // TODO(#12074): consider handling the case when no changes are made.
        // TODO(#12074): restore proof as well.
        self.retain_multi_range_recursive(0, vec![], &intervals_nibbles);
        self.to_trie_changes()
    }

    /// Recursive implementation of the algorithm of retaining keys belonging to
    /// any of the ranges given in `intervals` from the trie. All changes are
    /// applied in `updated_nodes`.
    ///
    /// `node_id` is the root of subtree being explored.
    /// `key_nibbles` is the key corresponding to `root`.
    /// `intervals_nibbles` is the list of ranges to be retained.
    fn retain_multi_range_recursive(
        &mut self,
        node_id: usize,
        key_nibbles: Vec<u8>,
        intervals_nibbles: &[Range<Vec<u8>>],
    ) {
        let decision = retain_decision(&key_nibbles, intervals_nibbles);
        match decision {
            RetainDecision::RetainAll => return,
            RetainDecision::DiscardAll => {
                let _ = self.take_node(node_id);
                self.place_node(node_id, UpdatedMemTrieNode::Empty);
                return;
            }
            RetainDecision::Descend => {
                // We need to descend into all children. The logic follows below.
            }
        }

        let node = self.take_node(node_id);
        match node {
            UpdatedMemTrieNode::Empty => {
                // Nowhere to descend.
                self.place_node(node_id, UpdatedMemTrieNode::Empty);
                return;
            }
            UpdatedMemTrieNode::Leaf { extension, value } => {
                let full_key_nibbles =
                    [key_nibbles, NibbleSlice::from_encoded(&extension).0.iter().collect_vec()]
                        .concat();
                if !intervals_nibbles.iter().any(|interval| interval.contains(&full_key_nibbles)) {
                    self.place_node(node_id, UpdatedMemTrieNode::Empty);
                } else {
                    self.place_node(node_id, UpdatedMemTrieNode::Leaf { extension, value });
                }
                return;
            }
            UpdatedMemTrieNode::Branch { mut children, mut value } => {
                if !intervals_nibbles.iter().any(|interval| interval.contains(&key_nibbles)) {
                    value = None;
                }

                for (i, child) in children.iter_mut().enumerate() {
                    let Some(old_child_id) = child.take() else {
                        continue;
                    };

                    let new_child_id = self.ensure_updated(old_child_id);
                    let child_key_nibbles = [key_nibbles.clone(), vec![i as u8]].concat();
                    self.retain_multi_range_recursive(
                        new_child_id,
                        child_key_nibbles,
                        intervals_nibbles,
                    );
                    if self.updated_nodes[new_child_id] == Some(UpdatedMemTrieNode::Empty) {
                        *child = None;
                    } else {
                        *child = Some(OldOrUpdatedNodeId::Updated(new_child_id));
                    }
                }

                self.place_node(node_id, UpdatedMemTrieNode::Branch { children, value });
            }
            UpdatedMemTrieNode::Extension { extension, child } => {
                let new_child_id = self.ensure_updated(child);
                let extension_nibbles =
                    NibbleSlice::from_encoded(&extension).0.iter().collect_vec();
                let child_key = [key_nibbles, extension_nibbles].concat();
                self.retain_multi_range_recursive(new_child_id, child_key, intervals_nibbles);

                let node = UpdatedMemTrieNode::Extension {
                    extension,
                    child: OldOrUpdatedNodeId::Updated(new_child_id),
                };
                self.place_node(node_id, node);
            }
        }

        // We may need to change node type to keep the trie structure unique.
        self.squash_node(node_id);
    }
}

/// Based on the key and the intervals, makes decision on the subtree exploration.
fn retain_decision(key: &[u8], intervals: &[Range<Vec<u8>>]) -> RetainDecision {
    let mut should_descend = false;
    for interval in intervals {
        // If key can be extended to be equal to start or end of the interval,
        // its subtree may have keys inside the interval. At the same time,
        // it can be extended with bytes which would fall outside the interval.
        //
        // For example, if key is "a" and interval is "ab".."cd", subtree may
        // contain both "aa" which must be excluded and "ac" which must be
        // retained.
        if interval.start.starts_with(key) || interval.end.starts_with(key) {
            should_descend = true;
            continue;
        }

        // If key is not a prefix of boundaries and falls inside the interval,
        // one can show that all the keys in the subtree are also inside the
        // interval.
        if interval.start.as_slice() <= key && key < interval.end.as_slice() {
            return RetainDecision::RetainAll;
        }

        // Otherwise, all the keys in the subtree are outside the interval.
    }

    if should_descend {
        RetainDecision::Descend
    } else {
        RetainDecision::DiscardAll
    }
}

// TODO(#12074): tests for
// - `retain_split_shard` API
// - checking not accessing not-inlined values
// - proof correctness
#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use std::ops::Range;
    use std::sync::Arc;

    use itertools::Itertools;
    use near_primitives::{shard_layout::ShardUId, types::StateRoot};

    use crate::{
        test_utils::TestTriesBuilder,
        trie::{
            mem::{
                iter::MemTrieIterator,
                mem_tries::MemTries,
                nibbles_utils::{all_two_nibble_nibbles, hex_to_nibbles, multi_hex_to_nibbles},
            },
            trie_storage::TrieMemoryPartialStorage,
        },
        Trie,
    };

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
        let expected_state_root = crate::test_utils::test_populate_trie(
            &shard_tries,
            &Trie::EMPTY_ROOT,
            ShardUId::single_shard(),
            changes,
        );

        let mut memtries = MemTries::new(ShardUId::single_shard());
        let mut update = memtries.update(Trie::EMPTY_ROOT, false).unwrap();
        for (key, value) in initial_entries {
            update.insert(&key, value);
        }
        let memtrie_changes = update.to_mem_trie_changes_only();
        let state_root = memtries.apply_memtrie_changes(0, &memtrie_changes);

        let update = memtries.update(state_root, true).unwrap();
        let (mut trie_changes, _) = update.retain_multi_range(&retain_multi_ranges);
        let memtrie_changes = trie_changes.mem_trie_changes.take().unwrap();
        let new_state_root = memtries.apply_memtrie_changes(1, &memtrie_changes);

        let entries = if new_state_root != StateRoot::default() {
            let state_root_ptr = memtries.get_root(&new_state_root).unwrap();
            let trie =
                Trie::new(Arc::new(TrieMemoryPartialStorage::default()), new_state_root, None);
            MemTrieIterator::new(Some(state_root_ptr), &trie).map(|e| e.unwrap()).collect_vec()
        } else {
            vec![]
        };

        // Check entries first to provide more context in case of failure.
        assert_eq!(entries, retain_result_naive);

        // Check state root, because it must be unique.
        assert_eq!(new_state_root, expected_state_root);
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
