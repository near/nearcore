use crate::runtime_state::iterator::StateIterator;
use crate::runtime_state::state::{ReadOnlyState, ValueRef};
use crate::{StorageChanges, StoreUpdate, Trie};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use std::sync::Arc;

pub struct FlatKVState {}

pub struct FlatDBChanges {}

impl ReadOnlyState for FlatKVState {
    fn state_root(&self) -> CryptoHash {
        unimplemented!()
    }

    fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
        unimplemented!()
    }

    fn deref(&self, value_hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        unimplemented!()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn StateIterator + 'a>, StorageError> {
        unimplemented!()
    }
}

impl FlatKVState {
    pub fn update(
        &self,
        changes: Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>>,
    ) -> Result<FlatDBChanges, StorageError> {
        unimplemented!()
    }
}

impl StorageChanges for FlatDBChanges {
    fn get_new_root(&self) -> CryptoHash {
        unimplemented!()
    }

    fn insertions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn deletions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn key_value_changes_into(
        &self,
        trie: Arc<Trie>,
        block_hash: CryptoHash,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}
