use std::cell::RefCell;
use std::collections::HashSet;

use near_primitives::trie_key::{SmallKeyVec, TrieKey};
use near_store::trie::AccessOptions;
use near_store::{StorageError, TrieAccess, TrieUpdate};

/// Wraps TrieUpdate and estimates recorded storage proof size for all reads as if the reads were
/// done on a real Trie, not TrieUpdate.
/// Used in early transaction preparation which reads account data from TrieUpdate and needs to
/// ensure that the size of recorded storage proof is reasonable.
/// TrieUpdate keeps a Key-Value map of updated entries which doesn't say much about the size of
/// trie nodes recorded during the reads. The wrapper reads from the Trie to record nodes and
/// estimates additional recorded size for any updated entries.
pub struct TrieUpdateWitnessSizeWrapper {
    pub trie_update: TrieUpdate,
    recorded: RefCell<Recorded>,
}

struct Recorded {
    /// All keys read from this wrapper. Used to charge only once for each key.
    read_keys: HashSet<SmallKeyVec>,
    /// Additional (on top of Trie's recorder) estimated storage proof size caused by newly added
    /// entries that exist only in TrieUpdate, not in the Trie.
    additional_storage_proof_size: i64,
}

impl TrieUpdateWitnessSizeWrapper {
    pub fn new(trie_update: TrieUpdate) -> TrieUpdateWitnessSizeWrapper {
        TrieUpdateWitnessSizeWrapper {
            trie_update,
            recorded: RefCell::new(Recorded {
                read_keys: HashSet::new(),
                additional_storage_proof_size: 0,
            }),
        }
    }

    pub fn recorded_storage_size(&self) -> usize {
        let trie_recorded_i64: i64 = self
            .trie_update
            .trie
            .recorded_storage_size()
            .try_into()
            .expect("Can't convert usize to i64");
        let result = trie_recorded_i64
            .saturating_add(self.recorded.borrow().additional_storage_proof_size)
            .max(0);
        let result_usize: usize = result.try_into().expect("Can't convert i64 to usize");
        result_usize
    }
}

impl TrieAccess for TrieUpdateWitnessSizeWrapper {
    // Intercept reads to the TrieUpdate and do storage proof size accounting
    fn get(&self, key: &TrieKey, opts: AccessOptions) -> Result<Option<Vec<u8>>, StorageError> {
        let mut key_bytes = SmallKeyVec::new_const();
        key.append_into(&mut key_bytes);
        let key_bytes_len: i64 =
            key_bytes.len().try_into().expect("Trie key len doesn't fit in i64");

        // First read from the trie - this will record all nodes on the path in the existing trie,
        // adding them to the trie's storage proof size counter.
        let trie_read: Option<Vec<u8>> = self.trie_update.trie.get(&key_bytes, opts)?;

        // Then read from the TrieUpdate (without falling back to the trie)
        let mut key_updated_in_trie_update = true;
        let update_read: Option<Vec<u8>> = self.trie_update.get_from_updates(&key, |_| {
            // If the fallback function is called, then the key isn't present in TrieUpdate, it wasn't modified.
            key_updated_in_trie_update = false;
            Ok(None)
        })?;

        let mut recorded = self.recorded.borrow_mut();
        let is_first_read = recorded.read_keys.insert(key_bytes);

        let read_value = match (trie_read, update_read) {
            (None, None) => None, // Key doesn't exist. Read attempt counted by Trie's recorder.
            (Some(trie_val), None) => {
                if key_updated_in_trie_update {
                    // Entry deleted in TrieUpdate. Read attempt counted by Trie's recorder.
                    None
                } else {
                    // Key exists in Trie, wasn't modified by TrieUpdate. Normal trie read.
                    Some(trie_val)
                }
            }
            (None, Some(update_val)) => {
                // First time reading a key that is only in the TrieUpdate, doesn't exist in the Trie.
                // Charge for the estimated cost of adding this value to the trie (key.len() + value.len() + ~one node size).
                // Existing nodes along the path to the key are counted by Trie's recorder.
                let value_len: i64 =
                    update_val.len().try_into().expect("Trie value len doesn't fit in i64");
                let insertion_trie_cost_base = 64;
                if is_first_read {
                    recorded.additional_storage_proof_size = recorded
                        .additional_storage_proof_size
                        .saturating_add(key_bytes_len)
                        .saturating_add(value_len)
                        .saturating_add(insertion_trie_cost_base);
                }

                Some(update_val)
            }
            (Some(trie_val), Some(update_val)) => {
                // Reading a value that exists in the Trie, but was updated in the TrieUpdate.
                // Key and trie path are covered, count the difference in recorded value length.
                if is_first_read {
                    let trie_val_len: i64 =
                        trie_val.len().try_into().expect("Trie value len doesn't fit in i64");
                    let update_val_len: i64 =
                        update_val.len().try_into().expect("Trie value len doesn't fit in i64");

                    recorded.additional_storage_proof_size = recorded
                        .additional_storage_proof_size
                        .saturating_add(trie_val_len)
                        .saturating_sub(update_val_len);
                }

                Some(update_val)
            }
        };

        Ok(read_value)
    }

    fn contains_key(&self, _key: &TrieKey, _opts: AccessOptions) -> Result<bool, StorageError> {
        // not used in prepare_transactions
        unimplemented!()
    }
}
