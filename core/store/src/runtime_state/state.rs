use crate::flat_db_state::{FlatDBChanges, FlatKVState};
use crate::runtime_state::iterator::StateIterator;
use crate::runtime_state::state_changes::{CombinedDBChanges, StorageChanges};
use crate::runtime_state::state_trie::TrieState;
use crate::TrieChanges;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::StateChanges;
use std::any::Any;

/// value length and hash
pub struct ValueRef(pub (u32, CryptoHash));

/// State for reads:
/// - Flat kv state
/// - Trie from partial storage
/// - Trie backed by db storage
pub trait ReadOnlyState: Any + Send + Sync + 'static {
    fn state_root(&self) -> CryptoHash;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        match self.get_ref(key)? {
            None => Ok(None),
            Some(ValueRef((_, value_hash))) => self.deref(&value_hash).map(Some),
        }
    }

    fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError>;

    fn deref(&self, value_hash: &CryptoHash) -> Result<Vec<u8>, StorageError>;

    fn iter<'a>(&'a self) -> Result<Box<dyn StateIterator + 'a>, StorageError>;

    fn get_trie_state(&self) -> Option<&TrieState> {
        None
    }
}

pub struct PersistentState {
    pub(crate) trie: TrieState,
    pub(crate) flat_state: FlatKVState,
}

impl ReadOnlyState for PersistentState {
    fn state_root(&self) -> CryptoHash {
        self.trie.root
    }

    fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
        self.trie.get_ref(key)
    }

    fn deref(&self, value_hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        self.trie.deref(&value_hash)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn StateIterator + 'a>, StorageError> {
        self.trie.iter()
    }

    fn get_trie_state(&self) -> Option<&TrieState> {
        Some(&self.trie)
    }
}
