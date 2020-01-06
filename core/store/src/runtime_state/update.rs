use crate::flat_db_state::FlatKVState;
use crate::runtime_state::iterator::StateIterator;
use crate::runtime_state::state::{PersistentState, ReadOnlyState, ValueRef};
use crate::runtime_state::state_changes::StorageChanges;
use crate::runtime_state::state_trie::TrieState;
use crate::{CombinedDBChanges, TrieChanges};
use near_primitives::hash::CryptoHash;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::Peekable;
use std::sync::Arc;

use near_primitives::types::{StateChangeCause, StateChanges};

use crate::StorageError;

use crate::Trie;
//use super::{Trie, TrieIterator};

/// key that was updated -> the update.
pub type ProspectiveUpdates = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

/// Provides a way to access Storage and record changes with future commit.
pub struct StateUpdate {
    pub state: Arc<dyn ReadOnlyState>,
    committed: StateChanges,
    prospective: ProspectiveUpdates,
}

pub enum TrieUpdateValuePtr<'a> {
    HashAndSize(&'a dyn ReadOnlyState, u32, CryptoHash),
    MemoryRef(&'a Vec<u8>),
}

impl<'a> TrieUpdateValuePtr<'a> {
    pub fn len(&self) -> u32 {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => value.len() as u32,
            TrieUpdateValuePtr::HashAndSize(_, length, _) => *length,
        }
    }

    pub fn deref_value(&self) -> Result<Vec<u8>, StorageError> {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => Ok((*value).clone()),
            TrieUpdateValuePtr::HashAndSize(trie, _, hash) => trie.deref(hash),
        }
    }
}

/// For each prefix, the value is a <key, value> map that records the changes in the
/// trie update.
pub type PrefixKeyValueChanges = HashMap<Vec<u8>, HashMap<Vec<u8>, Option<Vec<u8>>>>;

impl StateUpdate {
    pub fn from_state(state: Arc<dyn ReadOnlyState>) -> Self {
        StateUpdate { state, committed: BTreeMap::default(), prospective: BTreeMap::default() }
    }
    pub fn from_trie(trie: Arc<Trie>, root: CryptoHash) -> Self {
        let trie = TrieState { trie, root };
        let flat_state = FlatKVState {};
        let state = PersistentState { trie, flat_state };
        StateUpdate::from_state(Arc::new(state))
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(value) = self.prospective.get(key) {
            return Ok(value.as_ref().map(<Vec<u8>>::clone));
        } else if let Some(changes) = self.committed.get(key) {
            let (_, last_change) = changes.last().expect("changes are never empty");
            return Ok(last_change.as_ref().map(<Vec<u8>>::clone));
        }

        self.state.get(key)
    }

    pub fn get_ref(&self, key: &[u8]) -> Result<Option<TrieUpdateValuePtr>, StorageError> {
        if let Some(value) = self.prospective.get(key) {
            Ok(value.as_ref().map(TrieUpdateValuePtr::MemoryRef))
        } else if let Some(changes) = self.committed.get(key) {
            let (_, last_change) = changes.last().expect("changes are never empty");
            Ok(last_change.as_ref().map(TrieUpdateValuePtr::MemoryRef))
        } else {
            self.state.get_ref(key).map(|option| {
                option.map(|ValueRef((length, hash))| {
                    TrieUpdateValuePtr::HashAndSize(self.state.as_ref(), length, hash)
                })
            })
        }
    }

    /// Get values in trie update for a set of keys.
    /// Returns: a hash map of prefix -> <key, value> changes in the trie update.
    /// This function will commit changes. Need to be used with caution
    pub fn get_prefix_changes(
        &mut self,
        prefixes: &HashSet<Vec<u8>>,
    ) -> Result<PrefixKeyValueChanges, StorageError> {
        assert!(self.prospective.is_empty(), "Uncommitted changes exist");
        let mut res = HashMap::new();
        for prefix in prefixes {
            let mut prefix_key_value_change = HashMap::new();
            for (key, changes) in self.committed.range(prefix.to_vec()..) {
                if !key.starts_with(prefix) {
                    break;
                }
                if let Some((_, last_change)) = changes.last() {
                    prefix_key_value_change.insert(key.to_vec(), last_change.clone());
                }
            }
            if !prefix_key_value_change.is_empty() {
                res.insert(prefix.to_vec(), prefix_key_value_change);
            }
        }
        Ok(res)
    }

    pub fn committed_updates_per_cause(&self) -> &StateChanges {
        &self.committed
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.prospective.insert(key, Some(value));
    }
    pub fn remove(&mut self, key: &[u8]) {
        self.prospective.insert(key.to_vec(), None);
    }

    pub fn remove_starts_with(&mut self, prefix: &[u8]) -> Result<(), StorageError> {
        let mut keys = vec![];
        for key in self.iter(prefix)? {
            keys.push(key);
        }
        for key in keys {
            let key = key?;
            self.remove(&key);
        }
        Ok(())
    }

    pub fn for_keys_with_prefix<F: FnMut(&[u8])>(
        &self,
        prefix: &[u8],
        mut f: F,
    ) -> Result<(), StorageError> {
        let iter = self.iter(prefix)?;
        for key in iter {
            let key = key?;
            f(&key);
        }
        Ok(())
    }

    pub fn commit(&mut self, event: StateChangeCause) {
        for (key, val) in std::mem::replace(&mut self.prospective, BTreeMap::new()).into_iter() {
            self.committed.entry(key).or_default().push((event.clone(), val));
        }
    }

    pub fn rollback(&mut self) {
        self.prospective.clear();
    }

    pub fn finalize(self) -> Result<CombinedDBChanges, StorageError> {
        assert!(self.prospective.is_empty(), "Finalize cannot be called with uncommitted changes.");
        let StateUpdate { state, committed, .. } = self;
        let iter = Box::new(committed.iter().map(|(k, changes)| {
            let (_, last_change) =
                &changes.last().as_ref().expect("Committed entry should have at least one change");
            (k.clone(), last_change.clone())
        }));
        let trie_changes = state.get_trie_state().expect("always have a trie").update(iter)?;
        Ok(CombinedDBChanges { trie_changes, kv_changes: committed })
    }

    /// Returns Error if the underlying storage fails
    pub fn iter(&self, prefix: &[u8]) -> Result<TrieUpdateIterator, StorageError> {
        TrieUpdateIterator::new(self, prefix, b"", None)
    }

    pub fn range(
        &self,
        prefix: &[u8],
        start: &[u8],
        end: &[u8],
    ) -> Result<TrieUpdateIterator, StorageError> {
        TrieUpdateIterator::new(self, prefix, start, Some(end))
    }

    pub fn get_root(&self) -> CryptoHash {
        self.state.state_root()
    }
}

struct MergeIter<'a> {
    left: Peekable<Box<dyn Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)> + 'a>>,
    right: Peekable<Box<dyn Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)> + 'a>>,
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = (&'a Vec<u8>, &'a Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        let res = match (self.left.peek(), self.right.peek()) {
            (Some(&(ref left_key, _)), Some(&(ref right_key, _))) => left_key.cmp(&right_key),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => return None,
        };

        // Check which elements comes first and only advance the corresponding iterator.
        // If two keys are equal, take the value from `right`.
        match res {
            std::cmp::Ordering::Less => self.left.next(),
            std::cmp::Ordering::Greater => self.right.next(),
            std::cmp::Ordering::Equal => {
                self.left.next();
                self.right.next()
            }
        }
    }
}

pub struct TrieUpdateIterator<'a> {
    prefix: Vec<u8>,
    end_offset: Option<Vec<u8>>,
    trie_iter: Peekable<Box<dyn StateIterator + 'a>>,
    overlay_iter: Peekable<MergeIter<'a>>,
}

impl<'a> TrieUpdateIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    pub fn new(
        state_update: &'a StateUpdate,
        prefix: &[u8],
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, StorageError> {
        let mut trie_iter = state_update.state.iter()?;
        let mut start_offset = prefix.to_vec();
        start_offset.extend_from_slice(start);
        let end_offset = match end {
            Some(end) => {
                let mut p = prefix.to_vec();
                p.extend_from_slice(end);
                Some(p)
            }
            None => None,
        };
        trie_iter.seek(&start_offset)?;
        let committed_iter =
            state_update.committed.range(start_offset.clone()..).map(|(k, changes)| {
                (
                    k,
                    &changes
                        .last()
                        .as_ref()
                        .expect("Committed entry should have at least one change.")
                        .1,
                )
            });
        let prospective_iter = state_update.prospective.range(start_offset..);
        let overlay_iter = MergeIter {
            left: (Box::new(committed_iter) as Box<dyn Iterator<Item = _>>).peekable(),
            right: (Box::new(prospective_iter) as Box<dyn Iterator<Item = _>>).peekable(),
        }
        .peekable();
        Ok(TrieUpdateIterator {
            prefix: prefix.to_vec(),
            end_offset,
            trie_iter: trie_iter.peekable(),
            overlay_iter,
        })
    }
}

impl<'a> Iterator for TrieUpdateIterator<'a> {
    type Item = Result<Vec<u8>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let stop_cond = |key: &Vec<u8>, prefix: &Vec<u8>, end_offset: &Option<Vec<u8>>| {
            !key.starts_with(prefix)
                || match end_offset {
                    Some(end) => key >= end,
                    None => false,
                }
        };
        enum Ordering {
            Trie,
            Overlay,
            Both,
        }
        // Usually one iteration, unless need to skip None values in prospective / committed.
        loop {
            let res = {
                match (self.trie_iter.peek(), self.overlay_iter.peek()) {
                    (Some(&Ok((ref left_key, _))), Some(&(ref right_key, _))) => {
                        match (
                            stop_cond(left_key, &self.prefix, &self.end_offset),
                            stop_cond(*right_key, &self.prefix, &self.end_offset),
                        ) {
                            (false, false) => {
                                if left_key < *right_key {
                                    Ordering::Trie
                                } else if &left_key == right_key {
                                    Ordering::Both
                                } else {
                                    Ordering::Overlay
                                }
                            }
                            (false, true) => Ordering::Trie,
                            (true, false) => Ordering::Overlay,
                            (true, true) => {
                                return None;
                            }
                        }
                    }
                    (Some(&Ok((ref left_key, _))), None) => {
                        if stop_cond(left_key, &self.prefix, &self.end_offset) {
                            return None;
                        }
                        Ordering::Trie
                    }
                    (None, Some(&(ref right_key, _))) => {
                        if stop_cond(right_key, &self.prefix, &self.end_offset) {
                            return None;
                        }
                        Ordering::Overlay
                    }
                    (None, None) => return None,
                    (Some(&Err(ref e)), _) => return Some(Err(e.clone())),
                }
            };

            // Check which elements comes first and only advance the corresponding iterator.
            // If two keys are equal, take the value from `right`.
            return match res {
                Ordering::Trie => match self.trie_iter.next() {
                    Some(Ok((key, _value))) => Some(Ok(key)),
                    _ => None,
                },
                Ordering::Overlay => match self.overlay_iter.next() {
                    Some((key, Some(_))) => Some(Ok(key.clone())),
                    Some((_, None)) => continue,
                    None => None,
                },
                Ordering::Both => {
                    self.trie_iter.next();
                    match self.overlay_iter.next() {
                        Some((key, Some(_))) => Some(Ok(key.clone())),
                        Some((_, None)) => continue,
                        None => None,
                    }
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::create_trie;

    use super::*;
    use crate::runtime_state::state_changes::StorageChanges;
    use crate::runtime_state::state_trie::TrieState;

    #[test]
    fn trie() {
        let trie = create_trie();
        let mut trie_update = StateUpdate::from_trie(trie.clone(), CryptoHash::default());
        trie_update.set(b"dog".to_vec(), b"puppy".to_vec());
        trie_update.set(b"dog2".to_vec(), b"puppy".to_vec());
        trie_update.set(b"xxx".to_vec(), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) = trie_update.finalize().unwrap().into2(trie.clone()).unwrap();
        store_update.commit().ok();
        let trie_update2 = StateUpdate::from_trie(trie.clone(), new_root);
        assert_eq!(trie_update2.get(b"dog"), Ok(Some(b"puppy".to_vec())));
        let mut values = vec![];
        trie_update2.for_keys_with_prefix(b"dog", |key| values.push(key.to_vec())).unwrap();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);
    }

    #[test]
    fn trie_remove() {
        let trie = create_trie();

        // Delete non-existing element.
        let mut trie_update = StateUpdate::from_trie(trie.clone(), CryptoHash::default());
        trie_update.remove(b"dog");
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) = trie_update.finalize().unwrap().into2(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());

        // Add and right away delete element.
        let mut trie_update = StateUpdate::from_trie(trie.clone(), CryptoHash::default());
        trie_update.set(b"dog".to_vec(), b"puppy".to_vec());
        trie_update.remove(b"dog");
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) = trie_update.finalize().unwrap().into2(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());

        // Add, apply changes and then delete element.
        let mut trie_update = StateUpdate::from_trie(trie.clone(), CryptoHash::default());
        trie_update.set(b"dog".to_vec(), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) = trie_update.finalize().unwrap().into2(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_ne!(new_root, CryptoHash::default());
        let mut trie_update = StateUpdate::from_trie(trie.clone(), new_root);
        trie_update.remove(b"dog");
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) = trie_update.finalize().unwrap().into2(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());
    }

    #[test]
    fn trie_iter() {
        let trie = create_trie();
        let mut trie_update = StateUpdate::from_trie(trie.clone(), CryptoHash::default());
        trie_update.set(b"dog".to_vec(), b"puppy".to_vec());
        trie_update.set(b"aaa".to_vec(), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) = trie_update.finalize().unwrap().into2(trie.clone()).unwrap();
        store_update.commit().ok();

        let mut trie_update = StateUpdate::from_trie(trie.clone(), new_root);
        trie_update.set(b"dog2".to_vec(), b"puppy".to_vec());
        trie_update.set(b"xxx".to_vec(), b"puppy".to_vec());

        let values: Result<Vec<Vec<u8>>, _> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.unwrap(), vec![b"dog".to_vec(), b"dog2".to_vec()]);

        trie_update.rollback();

        let values: Result<Vec<Vec<u8>>, _> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.unwrap(), vec![b"dog".to_vec()]);

        let mut trie_update = StateUpdate::from_trie(trie.clone(), new_root);
        trie_update.remove(b"dog");

        let values: Result<Vec<Vec<u8>>, _> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.unwrap().len(), 0);

        let mut trie_update = StateUpdate::from_trie(trie.clone(), new_root);
        trie_update.set(b"dog2".to_vec(), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        trie_update.remove(b"dog2");

        let values: Result<Vec<Vec<u8>>, _> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.unwrap(), vec![b"dog".to_vec()]);

        let mut trie_update = StateUpdate::from_trie(trie.clone(), new_root);
        trie_update.set(b"dog2".to_vec(), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        trie_update.set(b"dog3".to_vec(), b"puppy".to_vec());

        let values: Result<Vec<Vec<u8>>, _> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.unwrap(), vec![b"dog".to_vec(), b"dog2".to_vec(), b"dog3".to_vec()]);

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.range(b"do", b"g", b"g21").unwrap().collect();
        assert_eq!(values.unwrap(), vec![b"dog".to_vec(), b"dog2".to_vec()]);

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.range(b"do", b"", b"xyz").unwrap().collect();
        assert_eq!(values.unwrap(), vec![b"dog".to_vec(), b"dog2".to_vec(), b"dog3".to_vec()]);
    }
}
