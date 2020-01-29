use crate::runtime_state::iterator::StateIterator;
use crate::runtime_state::state::{ReadOnlyState, ValueRef};
use crate::{Trie, TrieChanges, TrieIterator};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::StateRoot;
use std::sync::Arc;

pub struct TrieState {
    pub trie: Arc<Trie>,
    pub root: StateRoot,
}

impl ReadOnlyState for TrieState {
    fn state_root(&self) -> StateRoot {
        self.root
    }

    fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, StorageError> {
        self.trie.get_ref(&self.root, key).map(|option| option.map(ValueRef))
    }

    fn deref(&self, value_hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        self.trie.retrieve_raw_bytes(&value_hash)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn StateIterator + 'a>, StorageError> {
        self.trie.iter(&self.root).map(|x| Box::new(x) as Box<dyn StateIterator>)
    }

    fn get_trie_state(&self) -> &TrieState {
        self
    }
}

impl TrieState {
    pub fn new(trie: Arc<Trie>, root: CryptoHash) -> TrieState {
        TrieState { trie, root }
    }

    pub fn update<'a>(
        &self,
        changes: Box<Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> + 'a>,
    ) -> Result<TrieChanges, StorageError> {
        self.trie.update(&self.root, changes)
    }
}

impl<'a> StateIterator for TrieIterator<'a> {
    fn seek(&mut self, key: &[u8]) -> Result<(), StorageError> {
        TrieIterator::seek(self, key)
    }
}
