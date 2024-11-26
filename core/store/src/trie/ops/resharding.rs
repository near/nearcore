use std::ops::Range;

use itertools::Itertools;
use near_primitives::errors::StorageError;
use near_primitives::trie_key::col::COLUMNS_WITH_ACCOUNT_ID_IN_KEY;
use near_primitives::types::AccountId;

use crate::NibbleSlice;

use super::interface::{
    GenericNodeOrIndex, GenericTrieUpdate, GenericUpdatedTrieNode, GenericUpdatedTrieNodeWithSize,
    HasValueLength, UpdatedNodeId,
};
use super::squash::GenericTrieUpdateSquash;

#[derive(Debug)]
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

/// By the boundary account and the retain mode, generates the list of ranges
/// to be retained in trie.
pub(crate) fn boundary_account_to_intervals(
    boundary_account: &AccountId,
    retain_mode: RetainMode,
) -> Vec<Range<Vec<u8>>> {
    let mut intervals = vec![];
    // TODO(#12074): generate correct intervals in nibbles.
    for (col, _) in COLUMNS_WITH_ACCOUNT_ID_IN_KEY {
        match retain_mode {
            RetainMode::Left => {
                intervals.push(vec![col]..[&[col], boundary_account.as_bytes()].concat())
            }
            RetainMode::Right => {
                intervals.push([&[col], boundary_account.as_bytes()].concat()..vec![col + 1])
            }
        }
    }
    intervals
}

/// Converts the list of ranges in bytes to the list of ranges in nibbles.
pub(crate) fn intervals_to_nibbles(intervals: &[Range<Vec<u8>>]) -> Vec<Range<Vec<u8>>> {
    intervals
        .iter()
        .map(|range| {
            NibbleSlice::new(&range.start).iter().collect_vec()
                ..NibbleSlice::new(&range.end).iter().collect_vec()
        })
        .collect_vec()
}

pub(crate) trait GenericTrieUpdateRetain<'a, N, V>:
    GenericTrieUpdateSquash<'a, N, V>
where
    N: std::fmt::Debug,
    V: std::fmt::Debug + HasValueLength,
{
    /// Recursive implementation of the algorithm of retaining keys belonging to
    /// any of the ranges given in `intervals` from the trie. All changes are
    /// applied in `updated_nodes`.
    ///
    /// `node_id` is the root of subtree being explored.
    /// `key_nibbles` is the key corresponding to `root`.
    /// `intervals_nibbles` is the list of ranges to be retained.
    fn retain_multi_range_recursive(
        &mut self,
        node_id: UpdatedNodeId,
        key_nibbles: Vec<u8>,
        intervals_nibbles: &[Range<Vec<u8>>],
    ) -> Result<(), StorageError> {
        let decision = retain_decision(&key_nibbles, intervals_nibbles);
        match decision {
            RetainDecision::RetainAll => return Ok(()),
            RetainDecision::DiscardAll => {
                let _ = self.take_node(node_id);
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty());
                return Ok(());
            }
            RetainDecision::Descend => {
                // We need to descend into all children. The logic follows below.
            }
        }

        let GenericUpdatedTrieNodeWithSize { node, memory_usage } = self.take_node(node_id);
        match node {
            GenericUpdatedTrieNode::Empty => {
                // Nowhere to descend.
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty());
                return Ok(());
            }
            GenericUpdatedTrieNode::Leaf { extension, value } => {
                let full_key_nibbles =
                    [key_nibbles, NibbleSlice::from_encoded(&extension).0.iter().collect_vec()]
                        .concat();
                if !intervals_nibbles.iter().any(|interval| interval.contains(&full_key_nibbles)) {
                    self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize::empty());
                } else {
                    self.place_node_at(
                        node_id,
                        GenericUpdatedTrieNodeWithSize {
                            node: GenericUpdatedTrieNode::Leaf { extension, value },
                            memory_usage,
                        },
                    );
                }
                return Ok(());
            }
            GenericUpdatedTrieNode::Branch { mut children, mut value } => {
                if !intervals_nibbles.iter().any(|interval| interval.contains(&key_nibbles)) {
                    value = None;
                }

                let mut memory_usage = 0;
                for (i, child) in children.iter_mut().enumerate() {
                    let Some(old_child_id) = child.take() else {
                        continue;
                    };

                    let new_child_id = self.ensure_updated(old_child_id)?;
                    let child_key_nibbles = [key_nibbles.clone(), vec![i as u8]].concat();
                    self.retain_multi_range_recursive(
                        new_child_id,
                        child_key_nibbles,
                        intervals_nibbles,
                    )?;

                    let GenericUpdatedTrieNodeWithSize { node, memory_usage: child_memory_usage } =
                        self.get_node_ref(new_child_id);
                    if matches!(node, GenericUpdatedTrieNode::Empty) {
                        *child = None;
                    } else {
                        *child = Some(GenericNodeOrIndex::Updated(new_child_id));
                        memory_usage += child_memory_usage;
                    }
                }

                let node = GenericUpdatedTrieNode::Branch { children, value };
                memory_usage += node.memory_usage_direct();
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize { node, memory_usage });
            }
            GenericUpdatedTrieNode::Extension { extension, child } => {
                let new_child_id = self.ensure_updated(child)?;
                let extension_nibbles =
                    NibbleSlice::from_encoded(&extension).0.iter().collect_vec();
                let child_key = [key_nibbles, extension_nibbles].concat();
                self.retain_multi_range_recursive(new_child_id, child_key, intervals_nibbles)?;

                let node = GenericUpdatedTrieNode::Extension {
                    extension,
                    child: GenericNodeOrIndex::Updated(new_child_id),
                };
                let child_memory_usage = self.get_node_ref(new_child_id).memory_usage;
                let memory_usage = node.memory_usage_direct() + child_memory_usage;
                self.place_node_at(node_id, GenericUpdatedTrieNodeWithSize { node, memory_usage });
            }
        }

        // We may need to change node type to keep the trie structure unique.
        self.squash_node(node_id)
    }
}

impl<'a, N, V, T> GenericTrieUpdateRetain<'a, N, V> for T
where
    N: std::fmt::Debug,
    V: std::fmt::Debug + HasValueLength,
    T: GenericTrieUpdate<'a, N, V>,
{
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
