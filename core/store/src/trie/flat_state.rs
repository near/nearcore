use crate::{DBCol, StoreUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::state::ValueRef;
use near_primitives::types::RawStateChangesWithTrieKey;
use std::collections::HashMap;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct FlatStateDelta(pub HashMap<Vec<u8>, Option<ValueRef>>);

impl FlatStateDelta {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

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

    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0)
    }

    pub fn apply_to_flat_state(self, store_update: &mut StoreUpdate) {
        for (key, value) in self.0.into_iter() {
            match value {
                Some(value) => {
                    store_update
                        .set_ser(DBCol::FlatState, &key, &value)
                        .expect("Borsh cannot fail");
                }
                None => {
                    store_update.delete(DBCol::FlatState, &key);
                }
            }
        }
    }
}
