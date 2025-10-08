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

/// Estimation how much larger the Trie becomes after adding one entry, excluding key and value lengths.
/// Used to estimate storage proof size for newly added entries that are present only in TrieUpdate.
const TRIE_INSERT_BASE_SIZE_ESTIMATION: i64 = 90;

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
    /// Intercept reads to the TrieUpdate and do storage proof size accounting
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
                // Charge for the estimated cost of adding this value to the trie (key.len() + value.len() + one/two nodes).
                // Existing nodes along the path to the key are counted by Trie's recorder.
                let value_len: i64 =
                    update_val.len().try_into().expect("Trie value len doesn't fit in i64");
                if is_first_read {
                    recorded.additional_storage_proof_size = recorded
                        .additional_storage_proof_size
                        .saturating_add(key_bytes_len)
                        .saturating_add(value_len)
                        .saturating_add(TRIE_INSERT_BASE_SIZE_ESTIMATION);
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use near_crypto::{ED25519PublicKey, PublicKey};
    use near_primitives::account::AccessKey;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::AccountId;
    use near_store::test_utils::{TestTriesBuilder, test_populate_trie};
    use near_store::trie::AccessOptions;
    use near_store::{Trie, TrieAccess, TrieUpdate};

    use crate::runtime::trie_update_wrapper::TRIE_INSERT_BASE_SIZE_ESTIMATION;

    use super::TrieUpdateWitnessSizeWrapper;

    fn make_wrapper_for_test(
        initial_trie: Vec<(TrieKey, Vec<u8>)>,
        trie_update_writes: Vec<(TrieKey, Option<Vec<u8>>)>,
    ) -> TrieUpdateWitnessSizeWrapper {
        let shard_layout = ShardLayout::single_shard();
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
        let trie_changes =
            initial_trie.into_iter().map(|(key, val)| (key.to_vec(), Some(val))).collect();
        let state_root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes);
        let trie: Trie =
            tries.get_trie_for_shard(shard_uid, state_root).recording_reads_new_recorder();
        let mut trie_update = TrieUpdate::new(trie);

        for (k, v) in trie_update_writes {
            match v {
                Some(val) => trie_update.set(k, val),
                None => trie_update.remove(k),
            }
        }

        TrieUpdateWitnessSizeWrapper::new(trie_update)
    }

    fn int_key(i: u64) -> TrieKey {
        TrieKey::DelayedReceipt { index: i }
    }

    fn make_wrapper_for_test_ints(
        initial_trie: Vec<(u64, u64)>,
        trie_update_writes: Vec<(u64, Option<u64>)>,
    ) -> TrieUpdateWitnessSizeWrapper {
        make_wrapper_for_test(
            initial_trie.into_iter().map(|(k, v)| (int_key(k), v.to_be_bytes().to_vec())).collect(),
            trie_update_writes
                .into_iter()
                .map(|(k, v)| (int_key(k), v.map(|i| i.to_be_bytes().to_vec())))
                .collect(),
        )
    }

    fn read_int(t: &impl TrieAccess, i: u64) -> Option<u64> {
        t.get(&int_key(i), AccessOptions::DEFAULT)
            .unwrap()
            .map(|val_bytes| u64::from_be_bytes(val_bytes.try_into().unwrap()))
    }

    /// Trie and TrieUpdate are both empty, no data in the wrapper.
    #[test]
    fn test_empty_trie_empty_wrapper() {
        let w = make_wrapper_for_test(vec![], vec![]);
        assert_eq!(w.recorded_storage_size(), 0);
        assert_eq!(read_int(&w, 0), None);
        assert_eq!(w.recorded_storage_size(), 0);
    }

    /// Trie has data, TrieUpdate is empty.
    #[test]
    fn test_full_trie_empty_wrapper() {
        let w = make_wrapper_for_test_ints(vec![(0, 1), (2, 3), (4, 5)], vec![]);
        assert_eq!(w.recorded_storage_size(), 0);

        // Reading a value is properly accounted for by the trie recorder
        assert_eq!(read_int(&w, 0), Some(1));
        assert_eq!(w.recorded_storage_size(), 219);

        // Reading the value twice doesn't increase recorded size
        assert_eq!(read_int(&w, 0), Some(1));
        assert_eq!(w.recorded_storage_size(), 219);

        // Reading another value increases recorded size
        assert_eq!(read_int(&w, 4), Some(5));
        assert_eq!(w.recorded_storage_size(), 284);

        // Reading a nonexistent key returns None
        assert_eq!(read_int(&w, 1), None);
        assert_eq!(w.recorded_storage_size(), 284);
    }

    /// When the TrieUpdate is empty (e.g. in normal `prepare_transactions`) reading from the
    /// wrapper should produce exactly the same results as reading from the Trie.
    #[test]
    fn test_wrapper_is_noop_on_empty_trie_update() {
        let num_entries = 100;
        let wrapper =
            make_wrapper_for_test_ints((0..num_entries).map(|i| (i, 2 * i)).collect(), vec![]);
        for i in 0..num_entries {
            assert_eq!(read_int(&wrapper, i), Some(2 * i));
        }
        let storage_size_wrapper = wrapper.recorded_storage_size();

        let trie = wrapper.trie_update.trie.recording_reads_new_recorder();
        for i in 0..num_entries {
            assert_eq!(read_int(&trie, i), Some(2 * i));
        }
        let storage_size_trie = trie.recorded_storage_size();

        assert_eq!(storage_size_wrapper, storage_size_trie);
    }

    /// Trie is empty, TrieUpdate contains data.
    #[test]
    fn test_empty_trie_full_wrapper() {
        let w = make_wrapper_for_test_ints(vec![], vec![(6, Some(7)), (8, Some(9)), (9, None)]);
        assert_eq!(w.recorded_storage_size(), 0);

        // Wrapper estimates this much recorded storage for each newly added entry. (base + key.len() + value.len())
        let added_entry_estimation = TRIE_INSERT_BASE_SIZE_ESTIMATION as usize + 8 + 1 + 8; // TrieKey::DelayedReceipt: 1 + 8, u64 value: 8

        // Reading a value is properly accounted for by the trie recorder
        assert_eq!(read_int(&w, 6), Some(7));
        assert_eq!(w.recorded_storage_size(), added_entry_estimation);

        // Reading the value twice doesn't increase recorded size
        assert_eq!(read_int(&w, 6), Some(7));
        assert_eq!(w.recorded_storage_size(), added_entry_estimation);

        // Reading another value increases recorded size
        assert_eq!(read_int(&w, 8), Some(9));
        assert_eq!(w.recorded_storage_size(), 2 * added_entry_estimation);

        // Reading a nonexistent key works, doesn't increase recorded size
        assert_eq!(read_int(&w, 1000), None);
        assert_eq!(w.recorded_storage_size(), 2 * added_entry_estimation);

        // Reading a deleted key works, doesn't increase recorded size
        assert_eq!(read_int(&w, 9), None);
        assert_eq!(w.recorded_storage_size(), 2 * added_entry_estimation);
    }

    /// Both Trie and TrieUpdate contain data
    #[test]
    fn test_full_trie_full_wrapper() {
        let w = make_wrapper_for_test_ints(
            vec![(0, 1), (2, 300), (4, 500)],
            vec![(2, Some(3)), (4, None), (6, Some(7)), (8, None)],
        );
        assert_eq!(w.recorded_storage_size(), 0);

        // Read 0 - exists, only in Trie
        assert_eq!(read_int(&w, 0), Some(1));
        assert_eq!(w.recorded_storage_size(), 219);

        // Read 2 - exists in the Trie, but is overwritten by TrieUpdate
        assert_eq!(read_int(&w, 2), Some(3));
        assert_eq!(w.recorded_storage_size(), 284);

        // Read 4 - exists in the Trie, but is deleted in the TrieUpdate
        // Still records some storage proof, that's expected.
        assert_eq!(read_int(&w, 4), None);
        assert_eq!(w.recorded_storage_size(), 349);

        // read 6 - exists only in the TrieUpdate
        assert_eq!(read_int(&w, 6), Some(7));
        assert_eq!(w.recorded_storage_size(), 456);

        // Read 8 - doesn't exist in the Trie, deleted in TrieUpdate
        assert_eq!(read_int(&w, 8), None);
        assert_eq!(w.recorded_storage_size(), 456);

        // Read 10 - doesn't exist in either
        assert_eq!(read_int(&w, 10), None);
        assert_eq!(w.recorded_storage_size(), 456);
    }

    /// Test the standard flow from prepare_transactions:
    /// * read account data
    /// * set new account data
    /// * maybe read account data with the same key again
    ///
    /// Storage proof size is only calculated for the first read, subsequent reads to the same key
    /// are ignored.
    #[test]
    fn test_modified_trie_update() {
        let mut w = make_wrapper_for_test_ints(vec![(0, 1)], vec![]);
        assert_eq!(w.recorded_storage_size(), 0);

        // Read an entry from the wrapper
        assert_eq!(read_int(&w, 0), Some(1));
        assert_eq!(w.recorded_storage_size(), 67);

        // Set a new value for the key, inserting a much larger value
        w.trie_update.set(int_key(0), vec![0; 1024]);

        // Read the value, should return the newly inserted value
        assert_eq!(w.get(&int_key(0), AccessOptions::DEFAULT), Ok(Some(vec![0; 1024])));

        // Storage proof doesn't increase for a key that was read before the update.
        // Only the first read matters.
        assert_eq!(w.recorded_storage_size(), 67);
    }

    /// Compare recorded storage size when reading from Trie and TrieUpdate with 100 entries.
    /// See if the TrieUpdate estimation is reasonable.
    #[test]
    fn test_wrapper_estimation_is_reasonable() {
        let num_entries = 100;

        let trie_read_wrapper =
            make_wrapper_for_test_ints((0..num_entries).map(|i| (i, 2 * i)).collect(), vec![]);

        let update_read_wrapper = make_wrapper_for_test_ints(
            vec![],
            (0..num_entries).map(|i| (i, Some(2 * i))).collect(),
        );

        for i in 0..num_entries {
            assert_eq!(read_int(&trie_read_wrapper, i), Some(2 * i));
            assert_eq!(read_int(&update_read_wrapper, i), Some(2 * i));
        }

        // The estimation should be reasonably close to the actual recorded size.
        let trie_recorded_size = trie_read_wrapper.recorded_storage_size();
        assert_eq!(trie_recorded_size, 10059);

        let wrapper_recorded_size = update_read_wrapper.recorded_storage_size();
        assert_eq!(wrapper_recorded_size, 10700);

        // The difference should be less than 10%
        assert!(trie_recorded_size.abs_diff(wrapper_recorded_size) < trie_recorded_size / 10);
    }

    /// Test the worst-case scenario that `TrieUpdateWitnessSizeWrapper` protects from.
    /// In this scenario the attacker manipulates the state to create a long trie path with many
    /// large branch nodes. Then they add a new entry at the end of this path and submit a
    /// transaction where `prepare_transactions` needs to read the entry to validate the
    /// transaction. The problem is that early prepare transactions reads the new entry from
    /// TrieUpdate and not from the trie, which in the naive implementation wouldn't generate any
    /// storage proof. This way the attacker bypass the storage proof size limit in
    /// `prepare_transactions`. They could submit many such transactions to make the chunk's storage
    /// proof uncomfortably large.
    /// TrieUpdateWitnessSizeWrapper protects from this by reading from the Trie and estimating the
    /// correct recorded storage proof size.
    #[test]
    fn test_wrapper_worst_case_scenario() {
        // Define the "target_key". Reading the target_key will generate a lot of storage proof.
        let target_account: AccountId = "a".repeat(64).parse().unwrap();
        let target_public_key = PublicKey::ED25519(ED25519PublicKey([0; 32]));
        let target_key = TrieKey::AccessKey {
            account_id: target_account.clone(),
            public_key: target_public_key.clone(),
        };

        // Generate other keys which are similar to the target_key, but differ in one nibble.
        // This will generate a lot of branch nodes with many children on the path to target_key.
        let mut all_keys: Vec<TrieKey> = vec![target_key.clone()];

        fn set_nibble(bytes: &mut [u8], nibble_index: usize, value: u8) {
            assert!(value < 16);
            assert!(nibble_index < 2 * bytes.len());

            if nibble_index % 2 == 0 {
                bytes[nibble_index / 2] = (bytes[nibble_index / 2] & 0xF0) | value;
            } else {
                bytes[nibble_index / 2] = (bytes[nibble_index / 2] & 0x0F) | (value >> 4);
            }
        }

        // Generate keys that differ in account_id
        let account_bytes = target_account.as_bytes().to_vec();
        for nibble_index in 0..(2 * account_bytes.len()) {
            let mut new_account_bytes = account_bytes.clone();
            for val in 0..16 {
                set_nibble(&mut new_account_bytes, nibble_index, val);
                let Some(Some(new_account_id)) = std::str::from_utf8(&new_account_bytes)
                    .ok()
                    .map(|s| AccountId::from_str(s).ok())
                else {
                    continue;
                };
                all_keys.push(TrieKey::AccessKey {
                    account_id: new_account_id,
                    public_key: target_public_key.clone(),
                });
            }
        }

        // Generate keys that differ in public_key
        for nibble_index in 0..(2 * 32) {
            let mut new_public_key_bytes = vec![0u8; 32];
            for val in 0..16 {
                set_nibble(&mut new_public_key_bytes, nibble_index, val);
                let new_key = PublicKey::ED25519(ED25519PublicKey(
                    new_public_key_bytes.clone().try_into().unwrap(),
                ));
                all_keys.push(TrieKey::AccessKey {
                    account_id: target_account.clone(),
                    public_key: new_key,
                });
            }
        }

        let example_access_key = borsh::to_vec(&AccessKey::full_access()).unwrap();

        // First create a Trie that contains all keys, including target_key.
        // TrieUpdate is empty.
        let full_trie_entries =
            all_keys.iter().map(|key| (key.clone(), example_access_key.clone())).collect();
        let full_trie_wrapper = make_wrapper_for_test(full_trie_entries, vec![]);
        let full_trie = full_trie_wrapper.trie_update.trie;
        assert_eq!(full_trie.recorded_storage_size(), 0);

        // Reading target_key from the Trie generates 52kB of storage proof
        assert_eq!(
            full_trie.get(&target_key.to_vec(), AccessOptions::DEFAULT),
            Ok(Some(example_access_key.clone()))
        );
        assert_eq!(full_trie.recorded_storage_size(), 52638);

        // Then create a Trie that contains all keys except the target_key.
        // TrieUpdate contains the target_key.
        let trie_without_target_entries = all_keys
            .iter()
            .filter(|k| **k != target_key)
            .map(|key| (key.clone(), example_access_key.clone()))
            .collect();
        let worst_case_wrapper = make_wrapper_for_test(
            trie_without_target_entries,
            vec![(target_key.clone(), Some(example_access_key.clone()))],
        );
        assert_eq!(worst_case_wrapper.recorded_storage_size(), 0);

        // Reading target_key from the wrapper correctly estimates ~52kB of generated storage proof.
        assert_eq!(
            worst_case_wrapper.get(&target_key, AccessOptions::DEFAULT),
            Ok(Some(example_access_key))
        );
        assert_eq!(worst_case_wrapper.recorded_storage_size(), 52745);
    }
}
