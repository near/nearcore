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
    NoRetain,
    /// Descend into all child subtrees.
    Descend,
}

impl<'a, M: ArenaMemory> MemTrieUpdate<'a, M> {
    /// Splits the trie, separating entries by the boundary account.
    /// Leaves the left or right part of the trie, depending on the retain mode.
    ///
    /// Returns the changes to be applied to in-memory trie and the proof of
    /// the split operation.
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
        // let root = self.get_root().unwrap();
        // TODO(#12074): consider handling the case when no changes are made.
        // TODO(#12074): restore proof as well.
        self.retain_multi_range_recursive(0, vec![], &intervals_nibbles);
        self.to_trie_changes()
    }

    /// Recursive implementation of the algorithm of retaining keys belonging to
    /// any of the ranges given in `intervals` from the trie.
    ///
    /// `node_id` is the root of subtree being explored.
    /// `key_nibbles` is the key corresponding to `root`.
    /// `intervals_nibbles` is the list of ranges to be retained.
    ///
    /// Returns id of the node after retain applied.
    fn retain_multi_range_recursive(
        &mut self,
        node_id: usize,
        key_nibbles: Vec<u8>,
        intervals_nibbles: &[Range<Vec<u8>>],
    ) {
        let decision = retain_decision(&key_nibbles, intervals_nibbles);
        match decision {
            RetainDecision::RetainAll => return,
            RetainDecision::NoRetain => {
                let _ = self.take_node(node_id);
                self.place_node(node_id, UpdatedMemTrieNode::Empty);
                return;
            }
            RetainDecision::Descend => {}
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
            }
            UpdatedMemTrieNode::Branch { mut children, mut value } => {
                if !intervals_nibbles.iter().any(|interval| interval.contains(&key_nibbles)) {
                    value = None;
                }

                for i in 0..16 {
                    let child = &mut children[i];
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

                // TODO(#12074): squash the branch if needed. Consider reusing
                // `squash_nodes`.

                self.place_node(node_id, UpdatedMemTrieNode::Branch { children, value });
            }
            UpdatedMemTrieNode::Extension { extension, child } => {
                let new_child_id = self.ensure_updated(child);
                let extension_nibbles =
                    NibbleSlice::from_encoded(&extension).0.iter().collect_vec();
                let child_key = [key_nibbles, extension_nibbles].concat();
                self.retain_multi_range_recursive(new_child_id, child_key, intervals_nibbles);

                if self.updated_nodes[new_child_id] == Some(UpdatedMemTrieNode::Empty) {
                    self.place_node(node_id, UpdatedMemTrieNode::Empty);
                } else {
                    self.place_node(
                        node_id,
                        UpdatedMemTrieNode::Extension {
                            extension,
                            child: OldOrUpdatedNodeId::Updated(new_child_id),
                        },
                    );
                }
            }
        }
    }
}

/// Based on the key and the intervals, makes decision on the subtree exploration.
fn retain_decision(key: &[u8], intervals: &[Range<Vec<u8>>]) -> RetainDecision {
    let mut should_descend = false;
    for interval in intervals {
        // If key can be extended to be equal to start or end of the interval,
        // its subtree may have keys inside the interval. At the same time,
        // it can be extended with bytes which would fall outside the interval.
        // For example, if key is "a" and interval is "ab".."cd", subtree may
        // contain "aa" which must be excluded.
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
        RetainDecision::NoRetain
    }
}

// TODO(#12074): tests for
// - multiple retain ranges
// - removing keys one-by-one gives the same result as corresponding range retain
// - `retain_split_shard` API
// - all results of squashing branch
// - checking not accessing not-inlined nodes
// - proof correctness
// - (maybe) retain result is empty or complete tree
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use near_primitives::{shard_layout::ShardUId, types::StateRoot};

    use crate::{
        trie::{
            mem::{iter::MemTrieIterator, mem_tries::MemTries},
            trie_storage::TrieMemoryPartialStorage,
        },
        Trie,
    };

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
        let retain_result = vec![(b"bob".to_vec(), vec![2]), (b"charlie".to_vec(), vec![3])];

        let mut memtries = MemTries::new(ShardUId::single_shard());
        let empty_state_root = StateRoot::default();
        let mut update = memtries.update(empty_state_root, false).unwrap();
        for (key, value) in initial_entries {
            update.insert(&key, value);
        }
        let memtrie_changes = update.to_mem_trie_changes_only();
        let state_root = memtries.apply_memtrie_changes(0, &memtrie_changes);

        let update = memtries.update(state_root, true).unwrap();
        let (mut trie_changes, _) = update.retain_multi_range(&[retain_range]);
        let memtrie_changes = trie_changes.mem_trie_changes.take().unwrap();
        let new_state_root = memtries.apply_memtrie_changes(1, &memtrie_changes);

        let state_root_ptr = memtries.get_root(&new_state_root).unwrap();
        let trie = Trie::new(Arc::new(TrieMemoryPartialStorage::default()), new_state_root, None);
        let entries =
            MemTrieIterator::new(Some(state_root_ptr), &trie).map(|e| e.unwrap()).collect_vec();

        assert_eq!(entries, retain_result);
    }
}
