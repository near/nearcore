use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::hash::hash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::{FlatStateValue, ValueRef};
use near_primitives::types::RawStateChangesWithTrieKey;
use std::collections::HashMap;
use std::sync::Arc;

use super::{store_helper, BlockInfo};
use crate::{CryptoHash, StoreUpdate};

#[derive(Debug)]
pub struct FlatStateDelta {
    pub metadata: FlatStateDeltaMetadata,
    pub changes: FlatStateChanges,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
pub struct FlatStateDeltaMetadata {
    pub block: BlockInfo,
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct KeyForFlatStateDelta {
    pub shard_uid: ShardUId,
    pub block_hash: CryptoHash,
}

impl KeyForFlatStateDelta {
    pub fn to_bytes(&self) -> [u8; 40] {
        let mut res = [0; 40];
        res[..8].copy_from_slice(&self.shard_uid.to_bytes());
        res[8..].copy_from_slice(self.block_hash.as_bytes());
        res
    }
}
/// Delta of the state for some shard and block, stores mapping from keys to values
/// or None, if key was removed in this block.
#[derive(BorshSerialize, BorshDeserialize, Clone, Default, PartialEq, Eq)]
pub struct FlatStateChanges(pub(crate) HashMap<Vec<u8>, Option<FlatStateValue>>);

impl<T> From<T> for FlatStateChanges
where
    T: IntoIterator<Item = (Vec<u8>, Option<FlatStateValue>)>,
{
    fn from(iter: T) -> Self {
        Self(HashMap::from_iter(iter))
    }
}

impl std::fmt::Debug for FlatStateChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatStateChanges")
            .field("changes", &near_fmt::Slice(&Vec::from_iter(self.0.iter())))
            .finish()
    }
}

impl FlatStateChanges {
    /// Returns `Some(Option<FlatStateValue>)` from delta for the given key. If key is not present, returns None.
    pub fn get(&self, key: &[u8]) -> Option<Option<FlatStateValue>> {
        self.0.get(key).cloned()
    }

    /// Inserts a key-value pair to delta.
    pub fn insert(
        &mut self,
        key: Vec<u8>,
        value: Option<FlatStateValue>,
    ) -> Option<Option<FlatStateValue>> {
        self.0.insert(key, value)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Merge two deltas. Values from `other` should override values from `self`.
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0.into_iter())
    }

    /// Creates delta using raw state changes for some block.
    pub fn from_state_changes(changes: &[RawStateChangesWithTrieKey]) -> Self {
        let mut delta = HashMap::new();
        for change in changes.iter() {
            let key = change.trie_key.to_vec();
            // `RawStateChangesWithTrieKey` stores all sequential changes for a key within a chunk, so it is sufficient
            // to take only the last change.
            let last_change = &change
                .changes
                .last()
                .expect("Committed entry should have at least one change")
                .data;
            let flat_state_value = last_change.as_ref().map(|value| FlatStateValue::on_disk(value));
            delta.insert(key, flat_state_value);
        }
        Self(delta)
    }

    /// Applies delta to the flat state.
    pub fn apply_to_flat_state(self, store_update: &mut StoreUpdate, shard_uid: ShardUId) {
        for (key, value) in self.0.into_iter() {
            store_helper::set_flat_state_value(store_update, shard_uid, key, value);
        }
    }
}

/// `FlatStateChanges` which uses hash of raw `TrieKey`s instead of keys themselves.
/// Used to reduce memory used by deltas and serves read queries.
#[derive(Debug)]
pub struct CachedFlatStateChanges(HashMap<CryptoHash, Option<ValueRef>>);

#[derive(Debug)]
pub struct CachedFlatStateDelta {
    pub metadata: FlatStateDeltaMetadata,
    pub changes: Arc<CachedFlatStateChanges>,
}

impl From<FlatStateChanges> for CachedFlatStateChanges {
    fn from(delta: FlatStateChanges) -> Self {
        Self(
            delta
                .0
                .into_iter()
                .map(|(key, value)| (hash(&key), value.map(|v| v.to_value_ref())))
                .collect(),
        )
    }
}

impl CachedFlatStateChanges {
    /// Size of cache entry in bytes.
    const ENTRY_SIZE: usize =
        std::mem::size_of::<CryptoHash>() + std::mem::size_of::<Option<ValueRef>>();

    /// Returns `Some(Option<ValueRef>)` from delta for the given key. If key is not present, returns None.
    pub(crate) fn get(&self, key: &[u8]) -> Option<Option<ValueRef>> {
        self.0.get(&hash(key)).cloned()
    }

    /// Returns number of all entries.
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Total size in bytes consumed by delta. May be changed if we implement inlining of `ValueRef`s.
    pub(crate) fn total_size(&self) -> u64 {
        (self.0.capacity() as u64) * (Self::ENTRY_SIZE as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::FlatStateChanges;
    use near_primitives::state::FlatStateValue;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::{RawStateChange, RawStateChangesWithTrieKey, StateChangeCause};

    /// Check correctness of creating `FlatStateChanges` from state changes.
    #[test]
    fn flat_state_changes_creation() {
        let alice_trie_key = TrieKey::ContractCode { account_id: "alice".parse().unwrap() };
        let bob_trie_key = TrieKey::ContractCode { account_id: "bob".parse().unwrap() };
        let carol_trie_key = TrieKey::ContractCode { account_id: "carol".parse().unwrap() };
        let delayed_trie_key = TrieKey::DelayedReceiptIndices;
        let delayed_receipt_trie_key = TrieKey::DelayedReceipt { index: 1 };

        let state_changes = vec![
            RawStateChangesWithTrieKey {
                trie_key: delayed_trie_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: Some(vec![1]),
                }],
            },
            RawStateChangesWithTrieKey {
                trie_key: delayed_receipt_trie_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: Some(vec![2]),
                }],
            },
            RawStateChangesWithTrieKey {
                trie_key: alice_trie_key.clone(),
                changes: vec![
                    RawStateChange {
                        cause: StateChangeCause::InitialState,
                        data: Some(vec![1, 2]),
                    },
                    RawStateChange {
                        cause: StateChangeCause::ReceiptProcessing {
                            receipt_hash: Default::default(),
                        },
                        data: Some(vec![3, 4]),
                    },
                ],
            },
            RawStateChangesWithTrieKey {
                trie_key: bob_trie_key.clone(),
                changes: vec![
                    RawStateChange {
                        cause: StateChangeCause::InitialState,
                        data: Some(vec![5, 6]),
                    },
                    RawStateChange {
                        cause: StateChangeCause::ReceiptProcessing {
                            receipt_hash: Default::default(),
                        },
                        data: None,
                    },
                ],
            },
        ];

        let flat_state_changes = FlatStateChanges::from_state_changes(&state_changes);
        assert_eq!(
            flat_state_changes.get(&alice_trie_key.to_vec()),
            Some(Some(FlatStateValue::inlined(&[3, 4])))
        );
        assert_eq!(flat_state_changes.get(&bob_trie_key.to_vec()), Some(None));
        assert_eq!(flat_state_changes.get(&carol_trie_key.to_vec()), None);
        assert_eq!(
            flat_state_changes.get(&delayed_trie_key.to_vec()),
            Some(Some(FlatStateValue::inlined(&[1])))
        );
        assert_eq!(
            flat_state_changes.get(&delayed_receipt_trie_key.to_vec()),
            Some(Some(FlatStateValue::inlined(&[2])))
        );
    }

    /// Check that merge of `FlatStateChanges`s overrides the old changes for the same keys and doesn't conflict with
    /// different keys.
    #[test]
    fn flat_state_changes_merge() {
        let mut changes = FlatStateChanges::from([
            (vec![1], Some(FlatStateValue::value_ref(&[4]))),
            (vec![2], Some(FlatStateValue::value_ref(&[5]))),
            (vec![3], None),
            (vec![4], Some(FlatStateValue::value_ref(&[6]))),
        ]);
        let changes_new = FlatStateChanges::from([
            (vec![2], Some(FlatStateValue::value_ref(&[7]))),
            (vec![3], Some(FlatStateValue::value_ref(&[8]))),
            (vec![4], None),
            (vec![5], Some(FlatStateValue::value_ref(&[9]))),
        ]);
        changes.merge(changes_new);

        assert_eq!(changes.get(&[1]), Some(Some(FlatStateValue::value_ref(&[4]))));
        assert_eq!(changes.get(&[2]), Some(Some(FlatStateValue::value_ref(&[7]))));
        assert_eq!(changes.get(&[3]), Some(Some(FlatStateValue::value_ref(&[8]))));
        assert_eq!(changes.get(&[4]), Some(None));
        assert_eq!(changes.get(&[5]), Some(Some(FlatStateValue::value_ref(&[9]))));
    }
}
