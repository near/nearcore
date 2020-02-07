use crate::flat_db_state::forks_manager::ForksManager;
use crate::flat_db_state::{FlatDBChanges, FlatKVState};
use crate::runtime_state::iterator::StateIterator;
use crate::runtime_state::state_changes::{CombinedDBChanges, StorageChanges};
use crate::runtime_state::state_trie::TrieState;
use crate::TrieChanges;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, ShardId, StateChanges, StateRoot};
use std::any::Any;

/// value length and hash
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub struct ValueRef(pub (u32, CryptoHash));

/// State for reads:
/// - Flat kv state
/// - Trie from partial storage
/// - Trie backed by db storage
pub trait ReadOnlyState: Any + Send + Sync + 'static {
    fn state_root(&self) -> StateRoot;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        match self.get_ref(key)? {
            None => Ok(None),
            Some(ValueRef((_, value_hash))) => self.deref(&value_hash).map(Some),
        }
    }

    fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError>;

    fn deref(&self, value_hash: &CryptoHash) -> Result<Vec<u8>, StorageError>;

    fn iter<'a>(&'a self) -> Result<Box<dyn StateIterator + 'a>, StorageError>;

    fn get_trie_state(&self) -> &TrieState;

    fn as_combined_state(&self) -> Option<&CombinedDBState> {
        None
    }
}

pub struct CombinedDBState {
    pub(crate) trie: TrieState,
    pub(crate) flat_state: FlatKVState,
}

impl CombinedDBState {
    pub fn new(
        trie: TrieState,
        shard_id: ShardId,
        block_height: BlockHeight,
        block_hash: CryptoHash,
        parent_hash: CryptoHash,
    ) -> CombinedDBState {
        let store =
            trie.trie.storage.as_caching_storage().expect("always CachingStorage").store.clone();
        let flat_state = FlatKVState {
            shard_id,
            store: store.clone(),
            block_hash,
            parent_hash,
            block_height,
            forks_manager: ForksManager { store },
        };
        CombinedDBState { trie, flat_state }
    }
}

impl ReadOnlyState for CombinedDBState {
    fn state_root(&self) -> StateRoot {
        self.trie.state_root()
    }

    fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
        let value1 = self.flat_state.get_ref(key);
        let value2 = self.trie.get_ref(key);
        assert_eq!(value1, value2);
        value1
    }

    fn deref(&self, value_hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        self.trie.deref(&value_hash)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn StateIterator + 'a>, StorageError> {
        self.trie.iter()
    }

    fn get_trie_state(&self) -> &TrieState {
        &self.trie
    }

    fn as_combined_state(&self) -> Option<&CombinedDBState> {
        Some(self)
    }
}
