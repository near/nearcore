use crate::db::DBCol::ColKeyValueChanges;
use crate::flat_db_state::FlatDBChanges;
use crate::{StorageError, StoreUpdate, Trie, TrieChanges};
use borsh::BorshSerialize;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{StateChangeCause, StateChanges, StateRoot};
use std::sync::Arc;

/// An object of this trait stores an update to the state.
/// It can be written to storage using insertions_into/deletions_into/key_value_changes_into
pub trait StorageChanges {
    /// Returns the state root after changes
    fn get_new_root(&self) -> StateRoot;

    /// Add insertions to StoreUpdate transaction
    fn insertions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError>;

    /// Add deletions to StoreUpdate transaction
    fn deletions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError>;

    /// Add kv changes to StoreUpdate transaction
    fn key_value_changes_into(
        &self,
        trie: Arc<Trie>,
        block_hash: CryptoHash,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError>;

    /// Return a StoreUpdate transaction with insertions and deletions (but not kv changes)
    fn into2(&self, trie: Arc<Trie>) -> Result<(StoreUpdate, StateRoot), StorageError> {
        let mut store_update = StoreUpdate::new_with_trie(trie.clone());
        self.insertions_into(trie.clone(), &mut store_update)?;
        self.deletions_into(trie, &mut store_update)?;
        Ok((store_update, self.get_new_root()))
    }
}

impl StorageChanges for TrieChanges {
    fn get_new_root(&self) -> StateRoot {
        self.new_root
    }

    fn insertions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        TrieChanges::insertions_into(self, trie, store_update)
    }

    fn deletions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        TrieChanges::deletions_into(self, trie, store_update)
    }

    fn key_value_changes_into(
        &self,
        trie: Arc<Trie>,
        block_hash: CryptoHash,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }
}

pub struct WrappedTrieChanges {
    trie: Arc<Trie>,
    trie_changes: CombinedDBChanges,
    block_hash: CryptoHash,
}

impl WrappedTrieChanges {
    pub fn new(trie: Arc<Trie>, trie_changes: CombinedDBChanges, block_hash: CryptoHash) -> Self {
        WrappedTrieChanges { trie, trie_changes, block_hash }
    }

    pub fn empty(trie: Arc<Trie>, block_hash: CryptoHash, root: StateRoot) -> Self {
        let trie_changes = CombinedDBChanges::empty(root, block_hash);
        WrappedTrieChanges { trie, trie_changes, block_hash }
    }

    pub fn insertions_into(&self, store_update: &mut StoreUpdate) -> Result<(), StorageError> {
        self.trie_changes.insertions_into(self.trie.clone(), store_update)
    }

    pub fn deletions_into(&self, store_update: &mut StoreUpdate) -> Result<(), StorageError> {
        self.trie_changes.deletions_into(self.trie.clone(), store_update)
    }

    pub fn key_value_changes_into(
        &self,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        self.trie_changes.key_value_changes_into(self.trie.clone(), self.block_hash, store_update)
    }
}

pub struct CombinedDBChanges {
    pub(crate) trie_changes: TrieChanges,
    pub(crate) flat_db_changes: Option<FlatDBChanges>,
    pub(crate) kv_changes: StateChanges,
}

impl StorageChanges for CombinedDBChanges {
    fn get_new_root(&self) -> CryptoHash {
        self.trie_changes.get_new_root()
    }

    fn insertions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        self.trie_changes.insertions_into(trie.clone(), store_update)?;
        if let Some(ref flat_db_changes) = self.flat_db_changes {
            flat_db_changes.insertions_into(&self.kv_changes, store_update)?;
        }
        Ok(())
    }

    fn deletions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        self.trie_changes.deletions_into(trie.clone(), store_update)?;
        Ok(())
    }

    fn key_value_changes_into(
        &self,
        trie: Arc<Trie>,
        block_hash: CryptoHash,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        store_update.trie = Some(trie);
        for (key, changes) in &self.kv_changes {
            assert!(
                !changes.iter().any(|(change_cause, _)| {
                    if let StateChangeCause::NotWritableToDisk = change_cause {
                        true
                    } else {
                        false
                    }
                }),
                "NotWritableToDisk changes must never be finalized."
            );
            let mut storage_key = Vec::with_capacity(block_hash.as_ref().len() + key.len());
            storage_key.extend_from_slice(block_hash.as_ref());
            storage_key.extend_from_slice(key);
            let value = changes.try_to_vec().expect("borsh cannot fail");
            store_update.set(ColKeyValueChanges, storage_key.as_ref(), &value);
        }
        Ok(())
    }
}

impl CombinedDBChanges {
    pub fn empty(state_root: CryptoHash, block_hash: CryptoHash) -> Self {
        Self {
            trie_changes: TrieChanges::empty(state_root),
            flat_db_changes: None,
            kv_changes: Default::default(),
        }
    }
}
