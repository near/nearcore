use self::forks_manager::ForksManager;
use crate::db::DBCol::ColFlatState;
use crate::runtime_state::iterator::StateIterator;
use crate::runtime_state::state::{ReadOnlyState, ValueRef};
use crate::{PartialStorage, StorageChanges, Store, StoreUpdate, Trie, TrieChanges};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::challenge::PartialState;
use near_primitives::errors::StorageError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{BlockHeight, ShardId, StateChanges, StateRoot};
use std::sync::Arc;

pub(crate) mod forks_manager;

enum FlatKVStateError {
    /// StorageInternalError: database is corrupted
    Storage(StorageError),
    /// Too many writes / forks: caller should fall back to trie
    FlatKVInefficient,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct KeyTuple(ShardId, Vec<u8>, BlockHeight, CryptoHash);

// Relying on the fact that it's a prefix in borsh representation
#[derive(BorshSerialize, BorshDeserialize)]
struct PrefixKeyTuple(ShardId, Vec<u8>);

/// Database stores (key, block_height, block_hash) -> (ValueRef)
///
/// block_hash is the hash of the block that contains a write transaction - StateRoot in that block is before the update.
///
/// For now we use a naive implementation:
/// - getting a value iterates over all writes to the key
/// - writes are checked using ForksManager in a naive way
///
/// TODO possible efficient implementation:
/// - Apply deletions in chain code to discard / reapply blocks when switching between forks
/// - Discard changes from old blocks
/// - ForksManager won't be needed
pub struct FlatKVState {
    pub(crate) store: Arc<Store>,
    pub(crate) shard_id: ShardId,
    pub(crate) block_hash: CryptoHash,
    pub(crate) parent_hash: CryptoHash,
    pub(crate) block_height: BlockHeight,
    pub(crate) forks_manager: ForksManager,
}

pub struct FlatDBChanges {
    pub shard_id: ShardId,
    pub block_hash: CryptoHash,
    pub block_height: BlockHeight,
}

impl FlatKVState {
    pub fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
        Ok(self.get_ref_internal(key).unwrap())
    }
}

impl FlatKVState {
    fn get_ref_internal(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
        let mut best_value: Option<(BlockHeight, Option<ValueRef>)> = None;
        let key_prefix =
            PrefixKeyTuple(self.shard_id, key.to_vec()).try_to_vec().expect("borsh cannot fail");

        let (parent_hash, parent_height) = if self.block_height == 0 {
            (Default::default(), 0)
        } else {
            (self.parent_hash, self.block_height - 1)
        };

        for item in self.store.iter_prefix_ser::<Option<ValueRef>>(ColFlatState, &key_prefix) {
            let (key_tuple, value) = item.expect("storage internal error"); //.map_err(|_| StorageError::StorageInternalError)?;
            let KeyTuple(shard_id, key2, block_height, block_hash) =
                KeyTuple::try_from_slice(&key_tuple).expect("storage internal error"); //.map_err(|_| StorageError::StorageInternalError)?;
            debug_assert_eq!(key, &key2[..]);
            if block_height > parent_height
                || !self.forks_manager.is_same_chain(
                    block_height,
                    block_hash,
                    parent_height,
                    parent_hash,
                )
            {
                continue;
            }
            if best_value
                .as_ref()
                .map_or(true, |(best_block_height, _)| *best_block_height < block_height)
            {
                best_value = Some((block_height, value));
            }
        }
        Ok(best_value.and_then(|(_, value_ref)| value_ref))
    }
}

impl FlatDBChanges {
    pub fn insertions_into(
        &self,
        kv_changes: &StateChanges,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        for (key, values_list) in kv_changes.iter() {
            let value = &values_list.last().unwrap().1;
            let key_tuple =
                KeyTuple(self.shard_id, key.clone(), self.block_height, self.block_hash);
            let db_key = key_tuple.try_to_vec().unwrap();
            let db_value = value.as_ref().map(|value| ValueRef((value.len() as u32, hash(&value))));
            store_update.set_ser(ColFlatState, &db_key, &db_value).expect("borsh cannot fail");
        }
        Ok(())
    }

    /// For state sync.
    pub fn from_full_trie(
        &self,
        trie: Arc<Trie>,
        trie_changes: &TrieChanges,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        assert_eq!(trie_changes.old_root, Default::default());
        let storage =
            trie.storage.as_caching_storage().expect("always caching storage").store.clone();
        let nodes =
            trie_changes.insertions.iter().map(|(_k, v, _rc)| v.clone()).collect::<Vec<_>>();
        let new_trie = Trie::from_recorded_storage(PartialStorage { nodes: PartialState(nodes) });

        let key_prefix = self.shard_id.try_to_vec().expect("borsh cannot fail");

        //        for item in storage.iter_prefix_ser::<Option<ValueRef>>(ColFlatState, &key_prefix) {
        //            let (key_tuple, value) = item.expect("storage internal error"); //.map_err(|_| StorageError::StorageInternalError)?;
        //            let db_key = key_tuple.try_to_vec().expect("borsh cannot fail");
        //            store_update
        //                .set_ser::<Option<ValueRef>>(ColFlatState, &db_key, &None)
        //                .expect("borsh cannot fail");
        //        }

        for item in new_trie.iter(&trie_changes.get_new_root()).expect("trie was validated") {
            let (key, value) = item.expect("trie was validated");
            let key_tuple =
                KeyTuple(self.shard_id, key.clone(), self.block_height, self.block_hash);
            let db_key = key_tuple.try_to_vec().unwrap();
            let db_value = Some(ValueRef((value.len() as u32, hash(&value))));
            store_update.set_ser(ColFlatState, &db_key, &db_value).expect("borsh cannot fail");
        }

        Ok(())
    }
}
