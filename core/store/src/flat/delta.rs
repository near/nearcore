use std::collections::HashMap;

use borsh::{BorshSerialize, BorshDeserialize};
use near_primitives::hash::CryptoHash;
use near_primitives::state::ValueRef;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};

use crate::StoreUpdate;

use super::store_helper;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct KeyForFlatStateDelta {
    pub shard_id: ShardId,
    pub block_hash: CryptoHash,
}

/// Delta of the state for some shard and block, stores mapping from keys to value refs or None, if key was removed in
/// this block.
#[derive(BorshSerialize, BorshDeserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct FlatStateDelta(HashMap<Vec<u8>, Option<ValueRef>>);

impl<const N: usize> From<[(Vec<u8>, Option<ValueRef>); N]> for FlatStateDelta {
    fn from(arr: [(Vec<u8>, Option<ValueRef>); N]) -> Self {
        Self(HashMap::from(arr))
    }
}

impl FlatStateDelta {
    /// Assumed number of bytes used to store an entry in the cache.
    ///
    /// Based on 36 bytes for `ValueRef` + guessed overhead of 24 bytes for `Vec` and `HashMap`.
    pub(crate) const PER_ENTRY_OVERHEAD: u64 = 60;

    /// Returns `Some(Option<ValueRef>)` from delta for the given key. If key is not present, returns None.
    pub fn get(&self, key: &[u8]) -> Option<Option<ValueRef>> {
        self.0.get(key).cloned()
    }

    /// Inserts a key-value pair to delta.
    pub fn insert(&mut self, key: Vec<u8>, value: Option<ValueRef>) -> Option<Option<ValueRef>> {
        self.0.insert(key, value)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    fn total_size(&self) -> u64 {
        self.0.keys().map(|key| key.len() as u64 + Self::PER_ENTRY_OVERHEAD).sum()
    }

    /// Merge two deltas. Values from `other` should override values from `self`.
    pub fn merge(&mut self, other: &Self) {
        self.0.extend(other.0.iter().map(|(k, v)| (k.clone(), v.clone())))
    }

    /// Creates delta using raw state changes for some block.
    pub fn from_state_changes(changes: &[RawStateChangesWithTrieKey]) -> Self {
        let mut delta = HashMap::new();
        for change in changes.iter() {
            let key = change.trie_key.to_vec();
            if near_primitives::state_record::is_delayed_receipt_key(&key) {
                continue;
            }

            // `RawStateChangesWithTrieKey` stores all sequential changes for a key within a chunk, so it is sufficient
            // to take only the last change.
            let last_change = &change
                .changes
                .last()
                .expect("Committed entry should have at least one change")
                .data;
            match last_change {
                Some(value) => {
                    delta.insert(key, Some(near_primitives::state::ValueRef::new(value)))
                }
                None => delta.insert(key, None),
            };
        }
        Self(delta)
    }

    /// Applies delta to the flat state.
    pub fn apply_to_flat_state(self, store_update: &mut StoreUpdate) {
        for (key, value) in self.0.into_iter() {
            store_helper::set_ref(store_update, key, value).expect("Borsh cannot fail");
        }
    }
}