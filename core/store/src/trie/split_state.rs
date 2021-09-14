use crate::trie::iterator::TrieItem;
use crate::{get_delayed_receipt_indices, set, ShardTries, StoreUpdate, Trie, TrieUpdate};
use borsh::BorshDeserialize;
use bytesize::ByteSize;
use near_primitives::account::id::AccountId;
use near_primitives::errors::StorageError;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{StateChangeCause, StateRoot};
use std::collections::HashMap;

impl Trie {
    /// Computes the set of trie items (nodes with keys and values) for a state part.
    ///
    /// # Panics
    /// storage must be a TrieCachingStorage
    /// part_id must be in [0..num_parts)
    ///
    /// # Errors
    /// StorageError if the storage is corrupted
    pub fn get_trie_items_for_part(
        &self,
        part_id: u64,
        num_parts: u64,
        state_root: &StateRoot,
    ) -> Result<Vec<TrieItem>, StorageError> {
        assert!(part_id < num_parts);
        assert!(self.storage.as_caching_storage().is_some());

        let path_begin = self.find_path_for_part_boundary(state_root, part_id, num_parts)?;
        let path_end = self.find_path_for_part_boundary(state_root, part_id + 1, num_parts)?;
        self.iter(state_root)?.get_trie_items(&path_begin, &path_end)
    }
}

impl ShardTries {
    /// Apply `changes` to build states for new shards
    /// `state_roots` contains state roots for the new shards
    /// The caller must guarantee that `state_roots` contains all shard_ids
    /// that `key_to_shard_id` that may return
    /// Ignore changes on DelayedReceipts or DelayedReceiptsIndices
    /// Update `store_update` and return new state_roots
    /// used for building states for new shards in resharding
    pub fn apply_changes_to_new_states<'a>(
        &self,
        state_roots: &HashMap<ShardUId, StateRoot>,
        changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        key_to_shard_id: &(dyn Fn(&[u8]) -> Result<Option<ShardUId>, StorageError> + 'a),
    ) -> Result<(StoreUpdate, HashMap<ShardUId, StateRoot>), StorageError> {
        let mut changes_by_shard: HashMap<_, Vec<_>> = HashMap::new();
        for (raw_key, value) in changes.into_iter() {
            if let Some(new_shard_uid) = key_to_shard_id(&raw_key)? {
                changes_by_shard.entry(new_shard_uid).or_default().push((raw_key, value));
            }
        }
        let mut new_state_roots = state_roots.clone();
        let mut store_update = StoreUpdate::new_with_tries(self.clone());
        for (shard_uid, changes) in changes_by_shard {
            let trie = self.get_trie_for_shard(shard_uid.clone());
            // Here we assume that state_roots contains shard_uid, the caller of this method will guarantee that
            let trie_changes = trie.update(&state_roots[&shard_uid], changes.into_iter())?;
            let (update, state_root) = self.apply_all(&trie_changes, shard_uid.clone())?;
            new_state_roots.insert(shard_uid, state_root);
            store_update.merge(update);
        }
        Ok((store_update, new_state_roots))
    }

    pub fn apply_delayed_receipts_to_split_states<'a>(
        &self,
        state_roots: &HashMap<ShardUId, StateRoot>,
        receipts: &[Receipt],
        account_id_to_shard_id: &(dyn Fn(&AccountId) -> ShardUId + 'a),
    ) -> Result<(StoreUpdate, HashMap<ShardUId, StateRoot>), StorageError> {
        let mut trie_updates = HashMap::new();
        let mut delayed_receipts_indices_by_shard = HashMap::new();
        for (shard_uid, state_root) in state_roots {
            let trie_update = self.new_trie_update(shard_uid.clone(), state_root.clone());
            delayed_receipts_indices_by_shard
                .insert(shard_uid, get_delayed_receipt_indices(&trie_update)?);
            trie_updates.insert(shard_uid.clone(), trie_update);
        }

        for receipt in receipts {
            let new_shard_uid: ShardUId = account_id_to_shard_id(&receipt.receiver_id);
            if !trie_updates.contains_key(&new_shard_uid) {
                let err = format!(
                    "Account {} is in new shard {:?} but state_roots only contains {:?}",
                    receipt.receiver_id,
                    new_shard_uid,
                    trie_updates.keys(),
                );
                return Err(StorageError::StorageInconsistentState(err));
            }
            // we already checked that new_shard_uid is in trie_updates and delayed_receipts_indices
            // so we can safely unwrap here
            let mut delayed_receipts_indices =
                delayed_receipts_indices_by_shard.get_mut(&new_shard_uid).unwrap();
            set(
                trie_updates.get_mut(&new_shard_uid).unwrap(),
                TrieKey::DelayedReceipt { index: delayed_receipts_indices.next_available_index },
                receipt,
            );
            delayed_receipts_indices.next_available_index =
                delayed_receipts_indices.next_available_index.checked_add(1).ok_or_else(|| {
                    StorageError::StorageInconsistentState(
                        "Next available index for delayed receipt exceeded the integer limit"
                            .to_string(),
                    )
                })?;
        }

        // commit the trie_updates and update state_roots
        let mut merged_store_update = StoreUpdate::new_with_tries(self.clone());
        let mut new_state_roots = HashMap::new();
        for (shard_uid, mut trie_update) in trie_updates {
            set(
                &mut trie_update,
                TrieKey::DelayedReceiptIndices,
                delayed_receipts_indices_by_shard.get(&shard_uid).unwrap(),
            );
            trie_update.commit(StateChangeCause::Resharding);
            let (trie_changes, _) = trie_update.finalize()?;
            let (store_update, state_root) = self.apply_all(&trie_changes, shard_uid)?;
            new_state_roots.insert(shard_uid, state_root);
            merged_store_update.merge(store_update);
        }

        Ok((merged_store_update, new_state_roots))
    }
}

/// Retrieve delayed receipts starting with `start_index` until `memory_limit` is hit
/// return None if there is no delayed receipts with index >= start_index
pub fn get_delayed_receipts(
    state_update: &TrieUpdate,
    start_index: Option<u64>,
    memory_limit: ByteSize,
) -> Result<Option<(u64, Vec<Receipt>)>, StorageError> {
    let mut delayed_receipt_indices = get_delayed_receipt_indices(state_update)?;
    if let Some(start_index) = start_index {
        if start_index >= delayed_receipt_indices.next_available_index {
            return Ok(None);
        }
        delayed_receipt_indices.first_index = start_index.max(delayed_receipt_indices.first_index);
    }
    let mut used_memory = 0;
    let mut receipts = vec![];

    while used_memory < memory_limit.as_u64()
        && delayed_receipt_indices.first_index < delayed_receipt_indices.next_available_index
    {
        let key = TrieKey::DelayedReceipt { index: delayed_receipt_indices.first_index };
        let data = state_update.get(&key)?.ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Delayed receipt #{} should be in the state",
                delayed_receipt_indices.first_index
            ))
        })?;
        used_memory += data.len() as u64;
        delayed_receipt_indices.first_index += 1;

        let receipt = Receipt::try_from_slice(&data).map_err(|_| {
            StorageError::StorageInconsistentState("Failed to deserialize".to_string())
        })?;
        receipts.push(receipt);
    }
    Ok(Some((delayed_receipt_indices.first_index, receipts)))
}

#[cfg(test)]
mod tests {
    use crate::split_state::get_delayed_receipts;
    use crate::test_utils::{create_tries, gen_changes, gen_receipts, test_populate_trie};
    use crate::{get, get_delayed_receipt_indices, set, ShardTries, ShardUId, Trie, TrieUpdate};
    use near_primitives::account::id::AccountId;
    use near_primitives::borsh::BorshSerialize;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::{DelayedReceiptIndices, Receipt};
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::{NumShards, StateChangeCause, StateRoot};
    use rand::Rng;
    use std::collections::HashMap;

    fn get_all_delayed_receipts(state_update: &TrieUpdate) -> Vec<Receipt> {
        let mut delayed_receipt_indices = get_delayed_receipt_indices(state_update).unwrap();

        let mut receipts = vec![];
        while delayed_receipt_indices.first_index < delayed_receipt_indices.next_available_index {
            let key = TrieKey::DelayedReceipt { index: delayed_receipt_indices.first_index };
            let receipt = get(state_update, &key).unwrap().unwrap();
            delayed_receipt_indices.first_index += 1;
            receipts.push(receipt);
        }
        receipts
    }

    #[test]
    fn test_apply_changes_to_new_states() {
        let mut rng = rand::thread_rng();

        for _ in 0..20 {
            let mut tries = create_tries();
            // add 4 new shards for version 1
            let num_shards = 4;
            let shards: Vec<_> = (0..num_shards)
                .map(|shard_id| ShardUId { shard_id: shard_id as u32, version: 1 })
                .collect();
            tries.add_new_shards(&shards);
            let mut state_root = Trie::empty_root();
            let mut state_roots: HashMap<_, _> = (0..num_shards)
                .map(|x| (ShardUId { version: 1, shard_id: x as u32 }, CryptoHash::default()))
                .collect();
            for _ in 0..10 {
                let trie = tries.get_trie_for_shard(ShardUId::default());
                let changes = gen_changes(&mut rng, 100);
                state_root =
                    test_populate_trie(&tries, &state_root, ShardUId::default(), changes.clone());

                let (store_update, new_state_roots) = tries
                    .apply_changes_to_new_states(&state_roots, changes, &|raw_key| {
                        Ok(Some(ShardUId {
                            version: 1,
                            shard_id: (hash(raw_key).0[0] as NumShards % num_shards) as u32,
                        }))
                    })
                    .unwrap();
                store_update.commit().unwrap();
                state_roots = new_state_roots;

                // check that the 4 tries combined to the orig trie
                let trie_items: HashMap<_, _> =
                    trie.iter(&state_root).unwrap().map(Result::unwrap).collect();
                let mut combined_trie_items: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
                state_roots.iter().for_each(|(shard_uid, state_root)| {
                    let trie = tries.get_view_trie_for_shard(shard_uid.clone());
                    combined_trie_items.extend(trie.iter(state_root).unwrap().map(Result::unwrap));
                });
                assert_eq!(trie_items, combined_trie_items);
            }
        }
    }

    #[test]
    fn test_get_delayed_receipts() {
        let mut rng = rand::thread_rng();
        for _ in 0..20 {
            let memory_limit = bytesize::ByteSize::b(rng.gen_range(200, 1000));
            let all_receipts = gen_receipts(&mut rng, 200);

            // push receipt to trie
            let tries = create_tries();
            let mut trie_update = tries.new_trie_update(ShardUId::default(), StateRoot::default());
            let mut delayed_receipt_indices = DelayedReceiptIndices::default();

            for (i, receipt) in all_receipts.iter().enumerate() {
                set(&mut trie_update, TrieKey::DelayedReceipt { index: i as u64 }, receipt);
            }
            delayed_receipt_indices.next_available_index = all_receipts.len() as u64;
            set(&mut trie_update, TrieKey::DelayedReceiptIndices, &delayed_receipt_indices);
            trie_update.commit(StateChangeCause::Resharding);
            let (trie_changes, _) = trie_update.finalize().unwrap();
            let (store_update, state_root) =
                tries.apply_all(&trie_changes, ShardUId::default()).unwrap();
            store_update.commit().unwrap();

            let trie_update = tries.new_trie_update(ShardUId::default(), state_root);
            assert_eq!(all_receipts, get_all_delayed_receipts(&trie_update));
            let mut start_index = 0;

            while let Some((next_index, receipts)) =
                get_delayed_receipts(&trie_update, Some(start_index), memory_limit).unwrap()
            {
                assert_eq!(receipts, all_receipts[start_index as usize..next_index as usize]);
                start_index = next_index;

                let total_memory_use = receipts.iter().fold(0 as u64, |sum, receipt| {
                    sum + receipt.try_to_vec().unwrap().len() as u64
                });
                let memory_use_without_last_receipt =
                    receipts[..receipts.len() - 1].iter().fold(0 as u64, |sum, receipt| {
                        sum + receipt.try_to_vec().unwrap().len() as u64
                    });

                assert!(
                    total_memory_use >= memory_limit.as_u64()
                        || next_index == all_receipts.len() as u64
                );
                assert!(memory_use_without_last_receipt < memory_limit.as_u64());
            }
        }
    }

    fn test_apply_delayed_receipts<'a>(
        tries: &ShardTries,
        new_receipts: &[Receipt],
        existing_receipts: &[Receipt],
        state_roots: HashMap<ShardUId, StateRoot>,
        account_id_to_shard_id: &(dyn Fn(&AccountId) -> ShardUId + 'a),
    ) -> HashMap<ShardUId, StateRoot> {
        let (state_update, new_state_roots) = tries
            .apply_delayed_receipts_to_split_states(
                &state_roots,
                new_receipts,
                account_id_to_shard_id,
            )
            .unwrap();
        state_update.commit().unwrap();

        let receipts_by_shard: HashMap<_, _> = new_state_roots
            .iter()
            .map(|(shard_uid, state_root)| {
                let trie_update = tries.new_trie_update(*shard_uid, *state_root);
                let receipts = get_all_delayed_receipts(&trie_update);
                (shard_uid, receipts)
            })
            .collect();

        let mut all_receipts = existing_receipts.to_vec();
        all_receipts.extend_from_slice(new_receipts);

        let mut expected_receipts_by_shard: HashMap<_, _> =
            state_roots.iter().map(|(shard_uid, _)| (shard_uid, vec![])).collect();
        for receipt in all_receipts {
            let shard_uid = account_id_to_shard_id(&receipt.receiver_id);
            expected_receipts_by_shard.get_mut(&shard_uid).unwrap().push(receipt);
        }
        assert_eq!(expected_receipts_by_shard, receipts_by_shard);

        new_state_roots
    }

    #[test]
    fn test_apply_delayed_receipts_to_new_states() {
        let mut rng = rand::thread_rng();

        let mut tries = create_tries();
        let num_shards = 4;
        let shards: Vec<_> = (0..num_shards)
            .map(|shard_id| ShardUId { shard_id: shard_id as u32, version: 1 })
            .collect();
        tries.add_new_shards(&shards);

        for _ in 0..10 {
            let mut state_roots: HashMap<_, _> = (0..num_shards)
                .map(|x| (ShardUId { version: 1, shard_id: x as u32 }, CryptoHash::default()))
                .collect();
            let mut existing_receipts = vec![];
            for _ in 0..10 {
                let receipts = gen_receipts(&mut rng, 100);

                state_roots = test_apply_delayed_receipts(
                    &tries,
                    &receipts,
                    &existing_receipts,
                    state_roots,
                    &|account_id| ShardUId {
                        shard_id: (hash(account_id.as_ref().as_bytes()).0[0] as NumShards
                            % num_shards) as u32,
                        version: 1,
                    },
                );
                existing_receipts.extend_from_slice(&receipts);
            }
        }
    }
}
