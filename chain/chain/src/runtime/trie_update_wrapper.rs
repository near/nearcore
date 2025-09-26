use std::cell::RefCell;
use std::collections::HashSet;

use near_primitives::trie_key::{SmallKeyVec, TrieKey};
use near_store::trie::AccessOptions;
use near_store::{StorageError, TrieAccess, TrieUpdate};

/// Wraps TrieUpdate and estimates recorded storage proof size for all reads as if the reads were
/// from a real trie, not TrieUpdate. Used in early transaction preparation where we read account
/// data from TrieUpdate before applying trie changes, but still want to make sure that recorded
/// storage proof size stays within limits.
pub struct TrieUpdateWitnessSizeWrapper {
    pub trie_update: TrieUpdate,
    recorded: RefCell<Recorded>,
}

struct Recorded {
    read_keys: HashSet<SmallKeyVec>,
    additional_charged: usize,
}

impl TrieUpdateWitnessSizeWrapper {
    pub fn new(trie_update: TrieUpdate) -> TrieUpdateWitnessSizeWrapper {
        TrieUpdateWitnessSizeWrapper {
            trie_update,
            recorded: RefCell::new(Recorded { read_keys: HashSet::new(), additional_charged: 0 }),
        }
    }

    pub fn recorded_storage_size(&self) -> usize {
        self.trie_update.trie.recorded_storage_size() + self.recorded.borrow().additional_charged
    }
}

impl TrieAccess for TrieUpdateWitnessSizeWrapper {
    fn get(&self, key: &TrieKey, opts: AccessOptions) -> Result<Option<Vec<u8>>, StorageError> {
        let mut key_bytes = SmallKeyVec::new_const();
        key.append_into(&mut key_bytes);
        let key_bytes_len: usize = key_bytes.len();

        let trie_read = self.trie_update.trie.get(&key_bytes, opts)?;
        let update_read = self.trie_update.get_from_updates(&key, |_| Ok(None))?;

        let r = match (trie_read, update_read) {
            (None, None) => None,
            (Some(t), None) => {
                // Reading from the trie, not from the StateUpdate
                // todo - what if trie update deleted this key?
                self.recorded.borrow_mut().read_keys.insert(key_bytes);
                Some(t)
            }
            (None, Some(u)) => {
                // First time reading a key that is only in the TrieUpdate
                // Charge for the estimated cost of adding this value to the trie (key.len() + value.len() + one node size)
                let value_len = u.len();
                let mut recorded = self.recorded.borrow_mut();
                let insertion_trie_cost_base = 64;
                if recorded.read_keys.insert(key_bytes) {
                    recorded.additional_charged = recorded
                        .additional_charged
                        .saturating_add(key_bytes_len)
                        .saturating_add(value_len)
                        .saturating_add(insertion_trie_cost_base);
                }

                Some(u)
            }
            (Some(t), Some(u)) => {
                let mut recorded = self.recorded.borrow_mut();
                if recorded.read_keys.insert(key_bytes) {
                    // First time reading a value that is both in Trie and in the Update
                    // Key and trie path are covered, charge extra if the new value is longer.
                    // todo - charge less if the new value is shorter?
                    recorded.additional_charged =
                        recorded.additional_charged.saturating_add(u.len()).saturating_sub(t.len());
                }

                Some(u)
            }
        };

        Ok(r)
    }

    fn contains_key(&self, _key: &TrieKey, _opts: AccessOptions) -> Result<bool, StorageError> {
        unimplemented!()
    }
}
