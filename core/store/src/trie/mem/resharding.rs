use crate::trie::{MemTrieChanges, TrieRefcountDeltaMap};
use crate::{NibbleSlice, TrieChanges};

use super::flexible_data::children::ChildrenView;
use super::flexible_data::value::ValueView;
use super::node::{MemTrieNodeId, MemTrieNodeView};
use super::updating::{
    MemTrieUpdate, OldOrUpdatedNodeId, UpdatedMemTrieNode, UpdatedMemTrieNodeId,
};
use super::{arena::ArenaMemory, node::MemTrieNodePtr};
use itertools::Itertools;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

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

/// Tracks changes to the trie caused by the retain.
struct UpdatesTracker {
    /// Accessed node hashes and their serializations. Used for proof
    /// generation.
    #[allow(unused)]
    node_accesses: HashMap<CryptoHash, Arc<[u8]>>,
    /// All new nodes to be created.
    updated_nodes: Vec<UpdatedMemTrieNode>,
    /// On-disk changes applied to reference counts of nodes. In fact, these
    /// are only increments.
    refcount_changes: TrieRefcountDeltaMap,
}

impl UpdatesTracker {
    pub fn new() -> Self {
        Self {
            node_accesses: HashMap::new(),
            updated_nodes: Vec::new(),
            refcount_changes: TrieRefcountDeltaMap::new(),
        }
    }

    pub fn add_node(&mut self, node: UpdatedMemTrieNode) -> UpdatedMemTrieNodeId {
        let id = self.updated_nodes.len();
        debug_assert!(node != UpdatedMemTrieNode::Empty);
        self.updated_nodes.push(node);
        // TODO(#12074): apply remaining changes to `updates_tracker`.

        id
    }
}

impl<'a, M: ArenaMemory> MemTrieUpdate<'a, M> {
    /// Splits the trie, separating entries by the boundary account.
    /// Leaves the left or right part of the trie, depending on the retain mode.
    ///
    /// Returns the changes to be applied to in-memory trie and the proof of
    /// the split operation.
    pub fn retain_split_shard(
        &'a mut self,
        _boundary_account: AccountId,
        _retain_mode: RetainMode,
    ) -> (TrieChanges, PartialState) {
        // TODO(#12074): generate intervals in nibbles.

        self.retain_multi_range(&[])
    }

    /// Retains keys belonging to any of the ranges given in `intervals` from
    /// the trie.
    ///
    /// Returns changes to be applied to in-memory trie and proof of the
    /// retain operation.
    fn retain_multi_range(
        &'a mut self,
        intervals: &[Range<Vec<u8>>],
    ) -> (TrieChanges, PartialState) {
        let intervals_nibbles = intervals
            .iter()
            .map(|range| {
                NibbleSlice::new(&range.start).iter().collect_vec()
                    ..NibbleSlice::new(&range.end).iter().collect_vec()
            })
            .collect_vec();
        let mut updates_tracker = UpdatesTracker::new();
        let root = self.get_root().unwrap();
        // TODO(#12074): consider handling the case when no changes are made.
        let _ =
            retain_multi_range_recursive(root, vec![], &intervals_nibbles, &mut updates_tracker);

        let UpdatesTracker { updated_nodes, refcount_changes, .. } = updates_tracker;
        // TODO(#12074): the next method requires more generic node structure.
        // Consider simplifying the interface.
        let ordered_nodes = (0..updated_nodes.len()).collect_vec();
        let updated_nodes = updated_nodes.into_iter().map(Some).collect_vec();
        let hashes_and_serialized_nodes =
            self.compute_hashes_and_serialized_nodes(&ordered_nodes, &updated_nodes);
        let node_ids_with_hashes = hashes_and_serialized_nodes
            .iter()
            .map(|(node_id, hash, _)| (*node_id, *hash))
            .collect();
        let memtrie_changes = MemTrieChanges { node_ids_with_hashes, updated_nodes };

        let (trie_insertions, _) = TrieRefcountDeltaMap::into_changes(refcount_changes);
        let trie_changes = TrieChanges {
            // TODO(#12074): all the default fields are not used, consider
            // using simpler struct.
            old_root: CryptoHash::default(),
            new_root: CryptoHash::default(),
            insertions: trie_insertions,
            deletions: Vec::default(),
            mem_trie_changes: Some(memtrie_changes),
        };

        // TODO(#12074): restore proof as well.
        (trie_changes, PartialState::default())
    }
}

/// Recursive implementation of the algorithm of retaining keys belonging to
/// any of the ranges given in `intervals` from the trie.
///
/// `root` is the root of subtree being explored.
/// `key_nibbles` is the key corresponding to `root`.
/// `intervals_nibbles` is the list of ranges to be retained.
/// `updates_tracker` track changes to the trie caused by the retain.
///
/// Returns id of the node after retain applied.
fn retain_multi_range_recursive<'a, M: ArenaMemory>(
    root: MemTrieNodePtr<'a, M>,
    key_nibbles: Vec<u8>,
    intervals_nibbles: &[Range<Vec<u8>>],
    updates_tracker: &mut UpdatesTracker,
) -> Option<OldOrUpdatedNodeId> {
    let decision = retain_decision(&key_nibbles, intervals_nibbles);
    match decision {
        RetainDecision::RetainAll => return Some(OldOrUpdatedNodeId::Old(root.id())),
        RetainDecision::NoRetain => return None,
        RetainDecision::Descend => {}
    }

    let node_view = root.view();

    let mut retain_in_branch = |children: &ChildrenView<'a, M>, value: Option<&ValueView>| {
        retain_multi_range_in_branch(
            root.id(),
            children,
            value,
            key_nibbles.clone(),
            intervals_nibbles,
            updates_tracker,
        )
    };

    match node_view {
        MemTrieNodeView::Leaf { extension, .. } => {
            let extension = NibbleSlice::from_encoded(extension).0;
            let full_key_nibbles = [key_nibbles, extension.iter().collect_vec()].concat();
            if !intervals_nibbles.iter().any(|interval| interval.contains(&full_key_nibbles)) {
                None
            } else {
                Some(OldOrUpdatedNodeId::Old(root.id()))
            }
        }
        MemTrieNodeView::Branch { children, .. } => retain_in_branch(&children, None),
        MemTrieNodeView::BranchWithValue { children, value, .. } => {
            retain_in_branch(&children, Some(&value))
        }
        MemTrieNodeView::Extension { extension, child, .. } => {
            let extension_nibbles = NibbleSlice::from_encoded(extension).0.iter().collect_vec();
            let child_key = [key_nibbles, extension_nibbles].concat();
            let new_child =
                retain_multi_range_recursive(child, child_key, intervals_nibbles, updates_tracker);

            match new_child {
                None => None,
                Some(OldOrUpdatedNodeId::Old(id)) => Some(OldOrUpdatedNodeId::Old(id)),
                Some(OldOrUpdatedNodeId::Updated(id)) => {
                    let new_node = UpdatedMemTrieNode::Extension {
                        extension: extension.to_vec().into_boxed_slice(),
                        child: OldOrUpdatedNodeId::Updated(id),
                    };
                    Some(OldOrUpdatedNodeId::Updated(updates_tracker.add_node(new_node)))
                }
            }
        }
    }
}

/// Helper function for `retain_multi_range_recursive` when subtree is rooted
/// at a branch.
fn retain_multi_range_in_branch<'a, M: ArenaMemory>(
    root_id: MemTrieNodeId,
    children: &ChildrenView<'a, M>,
    mut value: Option<&ValueView>,
    key_nibbles: Vec<u8>,
    intervals_nibbles: &[Range<Vec<u8>>],
    updates_tracker: &mut UpdatesTracker,
) -> Option<OldOrUpdatedNodeId> {
    let mut new_children = [None; 16];
    let mut changed = false;

    if !intervals_nibbles.iter().any(|interval| interval.contains(&key_nibbles)) {
        value = None;
        changed = true;
    }

    for i in 0..16 {
        let Some(child) = children.get(i) else {
            continue;
        };

        let child_key_nibbles = [key_nibbles.clone(), vec![i as u8]].concat();
        let new_child = retain_multi_range_recursive(
            child,
            child_key_nibbles,
            intervals_nibbles,
            updates_tracker,
        );
        match new_child {
            Some(OldOrUpdatedNodeId::Old(id)) => {
                new_children[i] = Some(OldOrUpdatedNodeId::Old(id));
            }
            Some(OldOrUpdatedNodeId::Updated(id)) => {
                changed = true;
                new_children[i] = Some(OldOrUpdatedNodeId::Updated(id));
            }
            None => {
                changed = true;
                new_children[i] = None;
            }
        }
    }

    if changed {
        let new_node = UpdatedMemTrieNode::Branch {
            children: Box::new(new_children),
            value: value.map(|v| v.to_flat_value()),
        };
        // TODO(#12074): squash the branch if needed.
        // TODO(#12074): return None if needed.
        Some(OldOrUpdatedNodeId::Updated(updates_tracker.add_node(new_node)))
    } else {
        Some(OldOrUpdatedNodeId::Old(root_id))
    }
}

/// Based on the key and the intervals, makes decision on the subtree exploration.
fn retain_decision(key: &[u8], intervals: &[Range<Vec<u8>>]) -> RetainDecision {
    let mut should_descend = false;
    for interval in intervals {
        if key < interval.start.as_slice() {
            if interval.start.starts_with(key) {
                should_descend = true;
            } else {
                // No retain for this interval.
            }
        } else {
            if key >= interval.end.as_slice() {
                // No retain for this interval.
            } else if interval.end.starts_with(key) {
                should_descend = true;
            } else {
                return RetainDecision::RetainAll;
            }
        }
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

        let mut update = memtries.update(state_root, false).unwrap();
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
