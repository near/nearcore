use std::fmt::Debug;
use std::ops::Range;

use itertools::Itertools;
use near_primitives::errors::StorageError;
use near_primitives::trie_key::col;
use near_primitives::types::AccountId;

use crate::NibbleSlice;
use crate::trie::AccessOptions;

use super::interface::{
    GenericNodeOrIndex, GenericUpdatedTrieNode, GenericUpdatedTrieNodeWithSize, HasValueLength,
    UpdatedNodeId,
};
use super::squash::GenericTrieUpdateSquash;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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
fn boundary_account_to_intervals(
    boundary_account: &AccountId,
    retain_mode: RetainMode,
) -> Vec<Range<Vec<u8>>> {
    let mut intervals = vec![];
    for (prefix, name) in col::ALL_COLUMNS_WITH_NAMES {
        // Here we are handling all the keys that have the account_id prefix.
        // These are considered to be the `green` keys in resharding design.
        if col::COLUMNS_WITH_ACCOUNT_ID_IN_KEY.contains(&(prefix, name)) {
            intervals.push(get_interval_for_split_keys(boundary_account, &retain_mode, prefix));
            continue;
        }
        // Here we are handling all the keys that DO NOT have the account_id prefix.
        // These are the `red` and `yellow` keys in resharding design.
        // They are handled by either copying the keys to both shards or only to the lower index shard.
        match prefix {
            col::DELAYED_RECEIPT_OR_INDICES
            | col::PROMISE_YIELD_INDICES
            | col::PROMISE_YIELD_TIMEOUT
            | col::BANDWIDTH_SCHEDULER_STATE
            | col::GLOBAL_CONTRACT_CODE => {
                // This section contains the keys that we need to copy to both shards.
                intervals.push(get_interval_for_copy_to_both_children(prefix))
            }
            col::BUFFERED_RECEIPT_INDICES
            | col::BUFFERED_RECEIPT
            | col::BUFFERED_RECEIPT_GROUPS_QUEUE_DATA
            | col::BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM => {
                // This section contains the keys that we only copy to the lower index shard.
                if let Some(interval) = get_interval_for_copy_to_one_child(&retain_mode, prefix) {
                    intervals.push(interval);
                }
            }
            // Poor man's exhaustive check for handling all column types.
            _ => panic!("Unhandled trie key type: {}", name),
        }
    }
    intervals
}

fn append_key(trie_key: u8, boundary_account: &AccountId) -> Vec<u8> {
    let mut key = vec![trie_key];
    key.extend_from_slice(boundary_account.as_bytes());
    key
}

// This function generates the range of keys that need to be retained in the trie
// for trie keys that contain the account_id prefix.
// This includes trie keys like account, contract code, access key, received data, etc.
//
// Suppose the boundary account is "alice.near" and the trie key is ACCESS_KEY with u8 value 2.
// Left child interval: ["2", "2alice.near")
// Right child interval: ["2alice.near", "3")
fn get_interval_for_split_keys(
    boundary_account: &AccountId,
    retain_mode: &RetainMode,
    prefix: u8,
) -> Range<Vec<u8>> {
    match retain_mode {
        RetainMode::Left => vec![prefix]..append_key(prefix, boundary_account),
        RetainMode::Right => append_key(prefix, boundary_account)..vec![prefix + 1],
    }
}

// This function generates the range of keys that need to be retained in the both children.
// This includes trie keys related to delayed receipts, promise yield and bandwidth scheduler.
//
// Suppose the trie key has u8 value K
// Left child interval: [K, K+1)
// Right child interval: [K, K+1)
fn get_interval_for_copy_to_both_children(prefix: u8) -> Range<Vec<u8>> {
    vec![prefix]..vec![prefix + 1]
}

// This function generates the range of keys that only need to be retained by the
// lower index child.
// This includes trie keys related to buffered receipts.
//
// Suppose the trie key has u8 value K
// Left child interval: [K, K+1)
// Right child interval: None
fn get_interval_for_copy_to_one_child(
    retain_mode: &RetainMode,
    prefix: u8,
) -> Option<Range<Vec<u8>>> {
    match retain_mode {
        RetainMode::Left => Some(vec![prefix]..vec![prefix + 1]),
        RetainMode::Right => None,
    }
}

/// Converts the list of ranges in bytes to the list of ranges in nibbles.
fn intervals_to_nibbles(intervals: &[Range<Vec<u8>>]) -> Vec<Range<Vec<u8>>> {
    intervals
        .iter()
        .map(|range| {
            NibbleSlice::new(&range.start).iter().collect_vec()
                ..NibbleSlice::new(&range.end).iter().collect_vec()
        })
        .collect_vec()
}

trait GenericTrieUpdateRetainInner<'a, N, V>: GenericTrieUpdateSquash<'a, N, V>
where
    N: Debug,
    V: Debug + HasValueLength,
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
        opts: AccessOptions,
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

                    let new_child_id = self.ensure_updated(old_child_id, opts)?;
                    let child_key_nibbles = [key_nibbles.clone(), vec![i as u8]].concat();
                    self.retain_multi_range_recursive(
                        new_child_id,
                        child_key_nibbles,
                        intervals_nibbles,
                        opts,
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
                let new_child_id = self.ensure_updated(child, opts)?;
                let extension_nibbles =
                    NibbleSlice::from_encoded(&extension).0.iter().collect_vec();
                let child_key = [key_nibbles, extension_nibbles].concat();
                self.retain_multi_range_recursive(
                    new_child_id,
                    child_key,
                    intervals_nibbles,
                    opts,
                )?;

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
        self.squash_node(node_id, opts)
    }
}

// Default impl for all types that implement `GenericTrieUpdateSquash`.
impl<'a, N, V, T> GenericTrieUpdateRetainInner<'a, N, V> for T
where
    N: Debug,
    V: Debug + HasValueLength,
    T: GenericTrieUpdateSquash<'a, N, V>,
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

    if should_descend { RetainDecision::Descend } else { RetainDecision::DiscardAll }
}

pub(crate) trait GenericTrieUpdateRetain<'a, N, V>:
    GenericTrieUpdateSquash<'a, N, V>
where
    N: Debug,
    V: Debug + HasValueLength,
{
    /// Splits the trie, separating entries by the boundary account.
    /// Leaves the left or right part of the trie, depending on the retain mode.
    fn retain_split_shard(
        &mut self,
        boundary_account: &AccountId,
        retain_mode: RetainMode,
        opts: AccessOptions,
    );
}

impl<'a, N, V, T> GenericTrieUpdateRetain<'a, N, V> for T
where
    N: Debug,
    V: Debug + HasValueLength,
    T: GenericTrieUpdateRetainInner<'a, N, V>,
{
    /// Splits the trie, separating entries by the boundary account.
    /// Leaves the left or right part of the trie, depending on the retain mode.
    fn retain_split_shard(
        &mut self,
        boundary_account: &AccountId,
        retain_mode: RetainMode,
        opts: AccessOptions,
    ) {
        let intervals = boundary_account_to_intervals(boundary_account, retain_mode);
        let intervals_nibbles = intervals_to_nibbles(&intervals);
        self.retain_multi_range_recursive(0, vec![], &intervals_nibbles, opts).unwrap();
    }
}

// Expose function that takes custom ranges for testing.
#[cfg(test)]
#[allow(private_bounds)]
pub fn retain_split_shard_custom_ranges<'a, N, V>(
    update: &mut impl GenericTrieUpdateRetainInner<'a, N, V>,
    retain_multi_ranges: &Vec<Range<Vec<u8>>>,
) where
    N: Debug,
    V: Debug + HasValueLength,
{
    let intervals_nibbles = intervals_to_nibbles(retain_multi_ranges);
    update
        .retain_multi_range_recursive(0, vec![], &intervals_nibbles, AccessOptions::DEFAULT)
        .unwrap();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;
    use near_primitives::trie_key::col;
    use near_primitives::types::AccountId;

    use super::{RetainMode, append_key, boundary_account_to_intervals};

    #[test]
    fn test_boundary_account_to_intervals() {
        let column_name_map =
            col::ALL_COLUMNS_WITH_NAMES.iter().cloned().collect::<HashMap<_, _>>();
        let alice_account = AccountId::try_from("alice.near".to_string()).unwrap();

        let left_intervals = boundary_account_to_intervals(&alice_account, RetainMode::Left);
        let expected_left_intervals = vec![
            vec![col::ACCOUNT]..append_key(col::ACCOUNT, &alice_account),
            vec![col::CONTRACT_CODE]..append_key(col::CONTRACT_CODE, &alice_account),
            vec![col::ACCESS_KEY]..append_key(col::ACCESS_KEY, &alice_account),
            vec![col::RECEIVED_DATA]..append_key(col::RECEIVED_DATA, &alice_account),
            vec![col::POSTPONED_RECEIPT_ID]..append_key(col::POSTPONED_RECEIPT_ID, &alice_account),
            vec![col::PENDING_DATA_COUNT]..append_key(col::PENDING_DATA_COUNT, &alice_account),
            vec![col::POSTPONED_RECEIPT]..append_key(col::POSTPONED_RECEIPT, &alice_account),
            vec![col::DELAYED_RECEIPT_OR_INDICES]..vec![col::DELAYED_RECEIPT_OR_INDICES + 1],
            vec![col::CONTRACT_DATA]..append_key(col::CONTRACT_DATA, &alice_account),
            vec![col::PROMISE_YIELD_INDICES]..vec![col::PROMISE_YIELD_INDICES + 1],
            vec![col::PROMISE_YIELD_TIMEOUT]..vec![col::PROMISE_YIELD_TIMEOUT + 1],
            vec![col::PROMISE_YIELD_RECEIPT]
                ..append_key(col::PROMISE_YIELD_RECEIPT, &alice_account),
            vec![col::BUFFERED_RECEIPT_INDICES]..vec![col::BUFFERED_RECEIPT_INDICES + 1],
            vec![col::BUFFERED_RECEIPT]..vec![col::BUFFERED_RECEIPT + 1],
            vec![col::BANDWIDTH_SCHEDULER_STATE]..vec![col::BANDWIDTH_SCHEDULER_STATE + 1],
            vec![col::BUFFERED_RECEIPT_GROUPS_QUEUE_DATA]
                ..vec![col::BUFFERED_RECEIPT_GROUPS_QUEUE_DATA + 1],
            vec![col::BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM]
                ..vec![col::BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM + 1],
            vec![col::GLOBAL_CONTRACT_CODE]..vec![col::GLOBAL_CONTRACT_CODE + 1],
        ];
        assert!(left_intervals.iter().all(|range| range.start < range.end));
        for (actual, expected) in left_intervals.iter().zip_eq(expected_left_intervals.iter()) {
            let column_name = column_name_map[&expected.start[0]];
            assert_eq!(actual, expected, "Mismatch in key: {:?}", column_name);
        }

        let right_intervals = boundary_account_to_intervals(&alice_account, RetainMode::Right);
        let expected_right_intervals = vec![
            append_key(col::ACCOUNT, &alice_account)..vec![col::ACCOUNT + 1],
            append_key(col::CONTRACT_CODE, &alice_account)..vec![col::CONTRACT_CODE + 1],
            append_key(col::ACCESS_KEY, &alice_account)..vec![col::ACCESS_KEY + 1],
            append_key(col::RECEIVED_DATA, &alice_account)..vec![col::RECEIVED_DATA + 1],
            append_key(col::POSTPONED_RECEIPT_ID, &alice_account)
                ..vec![col::POSTPONED_RECEIPT_ID + 1],
            append_key(col::PENDING_DATA_COUNT, &alice_account)..vec![col::PENDING_DATA_COUNT + 1],
            append_key(col::POSTPONED_RECEIPT, &alice_account)..vec![col::POSTPONED_RECEIPT + 1],
            vec![col::DELAYED_RECEIPT_OR_INDICES]..vec![col::DELAYED_RECEIPT_OR_INDICES + 1],
            append_key(col::CONTRACT_DATA, &alice_account)..vec![col::CONTRACT_DATA + 1],
            vec![col::PROMISE_YIELD_INDICES]..vec![col::PROMISE_YIELD_INDICES + 1],
            vec![col::PROMISE_YIELD_TIMEOUT]..vec![col::PROMISE_YIELD_TIMEOUT + 1],
            append_key(col::PROMISE_YIELD_RECEIPT, &alice_account)
                ..vec![col::PROMISE_YIELD_RECEIPT + 1],
            vec![col::BANDWIDTH_SCHEDULER_STATE]..vec![col::BANDWIDTH_SCHEDULER_STATE + 1],
            vec![col::GLOBAL_CONTRACT_CODE]..vec![col::GLOBAL_CONTRACT_CODE + 1],
        ];
        assert!(right_intervals.iter().all(|range| range.start < range.end));
        for (actual, expected) in right_intervals.iter().zip_eq(expected_right_intervals.iter()) {
            let column_name = column_name_map[&expected.start[0]];
            assert_eq!(actual, expected, "Mismatch in key: {:?}", column_name);
        }
    }
}
