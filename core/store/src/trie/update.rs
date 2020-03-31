use std::collections::BTreeMap;
use std::iter::Peekable;
use std::sync::Arc;

use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    RawStateChange, RawStateChanges, RawStateChangesWithTrieKey, StateChangeCause,
};

use crate::trie::TrieChanges;
use crate::StorageError;

use super::{Trie, TrieIterator};
use near_primitives::trie_key::TrieKey;

/// Key-value update. Contains a TrieKey and a value.
pub struct TrieKeyValueUpdate {
    pub trie_key: TrieKey,
    pub value: Option<Vec<u8>>,
}

/// key that was updated -> the update.
pub type TrieUpdates = BTreeMap<Vec<u8>, TrieKeyValueUpdate>;

/// Provides a way to access Storage and record changes with future commit.
pub struct TrieUpdate {
    pub trie: Arc<Trie>,
    root: CryptoHash,
    committed: RawStateChanges,
    prospective: TrieUpdates,
}

pub enum TrieUpdateValuePtr<'a> {
    HashAndSize(&'a Trie, u32, CryptoHash),
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
            TrieUpdateValuePtr::HashAndSize(trie, _, hash) => trie.retrieve_raw_bytes(hash),
        }
    }
}

impl TrieUpdate {
    pub fn new(trie: Arc<Trie>, root: CryptoHash) -> Self {
        TrieUpdate { trie, root, committed: Default::default(), prospective: Default::default() }
    }

    pub fn get(&self, key: &TrieKey) -> Result<Option<Vec<u8>>, StorageError> {
        let key = key.to_vec();
        if let Some(key_value) = self.prospective.get(&key) {
            return Ok(key_value.value.as_ref().map(<Vec<u8>>::clone));
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.as_ref().map(<Vec<u8>>::clone));
            }
        }

        self.trie.get(&self.root, &key)
    }

    pub fn get_ref(&self, key: &TrieKey) -> Result<Option<TrieUpdateValuePtr>, StorageError> {
        let key = key.to_vec();
        if let Some(key_value) = self.prospective.get(&key) {
            return Ok(key_value.value.as_ref().map(TrieUpdateValuePtr::MemoryRef));
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.as_ref().map(TrieUpdateValuePtr::MemoryRef));
            }
        }
        self.trie.get_ref(&self.root, &key).map(|option| {
            option.map(|(length, hash)| TrieUpdateValuePtr::HashAndSize(&self.trie, length, hash))
        })
    }

    pub fn set(&mut self, trie_key: TrieKey, value: Vec<u8>) {
        // NOTE: Converting `TrieKey` to a `Vec<u8>` is useful here for 2 reasons:
        // - Using `Vec<u8>` for sorting `BTreeMap` in the same order as a `Trie` and
        //   avoid recomputing `Vec<u8>` every time. It helps for merging iterators.
        // - Using `TrieKey` later for `RawStateChangesWithTrieKey` for State changes RPCs.
        self.prospective
            .insert(trie_key.to_vec(), TrieKeyValueUpdate { trie_key, value: Some(value) });
    }
    pub fn remove(&mut self, trie_key: TrieKey) {
        self.prospective.insert(trie_key.to_vec(), TrieKeyValueUpdate { trie_key, value: None });
    }

    pub fn commit(&mut self, event: StateChangeCause) {
        let prospective = std::mem::take(&mut self.prospective);
        for (raw_key, TrieKeyValueUpdate { trie_key, value }) in prospective.into_iter() {
            self.committed
                .entry(raw_key)
                .or_insert_with(|| RawStateChangesWithTrieKey { trie_key, changes: Vec::new() })
                .changes
                .push(RawStateChange { cause: event.clone(), data: value });
        }
    }

    pub fn rollback(&mut self) {
        self.prospective.clear();
    }

    pub fn finalize(self) -> Result<(TrieChanges, Vec<RawStateChangesWithTrieKey>), StorageError> {
        assert!(self.prospective.is_empty(), "Finalize cannot be called with uncommitted changes.");
        let TrieUpdate { trie, root, committed, .. } = self;
        let mut state_changes = Vec::with_capacity(committed.len());
        let trie_changes = trie.update(
            &root,
            committed.into_iter().map(|(k, changes_with_trie_key)| {
                let data = changes_with_trie_key
                    .changes
                    .last()
                    .expect("Committed entry should have at least one change")
                    .data
                    .clone();
                state_changes.push(changes_with_trie_key);
                (k, data)
            }),
        )?;
        Ok((trie_changes, state_changes))
    }

    /// Returns Error if the underlying storage fails
    pub fn iter(&self, key_prefix: &[u8]) -> Result<TrieUpdateIterator, StorageError> {
        TrieUpdateIterator::new(self, key_prefix, b"", None)
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
        self.root
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
    trie_iter: Peekable<TrieIterator<'a>>,
    overlay_iter: Peekable<MergeIter<'a>>,
}

impl<'a> TrieUpdateIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    pub fn new(
        state_update: &'a TrieUpdate,
        prefix: &[u8],
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, StorageError> {
        let mut trie_iter = state_update.trie.iter(&state_update.root)?;
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
        let committed_iter = state_update.committed.range(start_offset.clone()..).map(
            |(raw_key, changes_with_trie_key)| {
                (
                    raw_key,
                    &changes_with_trie_key
                        .changes
                        .last()
                        .as_ref()
                        .expect("Committed entry should have at least one change.")
                        .data,
                )
            },
        );
        let prospective_iter = state_update
            .prospective
            .range(start_offset..)
            .map(|(raw_key, key_value)| (raw_key, &key_value.value));
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

    fn test_key(key: Vec<u8>) -> TrieKey {
        TrieKey::ContractData { account_id: "alice".to_string(), key }
    }

    #[test]
    fn trie() {
        let trie = create_trie();
        let root = CryptoHash::default();
        let mut trie_update = TrieUpdate::new(trie.clone(), root);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"xxx".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) =
            trie_update.finalize().unwrap().0.into(trie.clone()).unwrap();
        store_update.commit().ok();
        let trie_update2 = TrieUpdate::new(trie.clone(), new_root);
        assert_eq!(trie_update2.get(&test_key(b"dog".to_vec())), Ok(Some(b"puppy".to_vec())));
        let values = trie_update2
            .iter(&test_key(b"dog".to_vec()).to_vec())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            values,
            vec![test_key(b"dog".to_vec()).to_vec(), test_key(b"dog2".to_vec()).to_vec()]
        );
    }

    #[test]
    fn trie_remove() {
        let trie = create_trie();

        // Delete non-existing element.
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) =
            trie_update.finalize().unwrap().0.into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());

        // Add and right away delete element.
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) =
            trie_update.finalize().unwrap().0.into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());

        // Add, apply changes and then delete element.
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) =
            trie_update.finalize().unwrap().0.into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_ne!(new_root, CryptoHash::default());
        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) =
            trie_update.finalize().unwrap().0.into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());
    }

    #[test]
    fn trie_iter() {
        let trie = create_trie();
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"aaa".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let (store_update, new_root) =
            trie_update.finalize().unwrap().0.into(trie.clone()).unwrap();
        store_update.commit().ok();

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"xxx".to_vec()), b"puppy".to_vec());

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(
            values.unwrap(),
            vec![test_key(b"dog".to_vec()).to_vec(), test_key(b"dog2".to_vec()).to_vec()]
        );

        trie_update.rollback();

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(values.unwrap(), vec![test_key(b"dog".to_vec()).to_vec()]);

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.remove(test_key(b"dog".to_vec()));

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(values.unwrap().len(), 0);

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        trie_update.remove(test_key(b"dog2".to_vec()));

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(values.unwrap(), vec![test_key(b"dog".to_vec()).to_vec()]);

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        trie_update.set(test_key(b"dog3".to_vec()), b"puppy".to_vec());

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(
            values.unwrap(),
            vec![
                test_key(b"dog".to_vec()).to_vec(),
                test_key(b"dog2".to_vec()).to_vec(),
                test_key(b"dog3".to_vec()).to_vec()
            ]
        );

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.range(&test_key(b"do".to_vec()).to_vec(), b"g", b"g21").unwrap().collect();
        assert_eq!(
            values.unwrap(),
            vec![test_key(b"dog".to_vec()).to_vec(), test_key(b"dog2".to_vec()).to_vec(),]
        );

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.range(&test_key(b"do".to_vec()).to_vec(), b"", b"xyz").unwrap().collect();

        assert_eq!(
            values.unwrap(),
            vec![
                test_key(b"dog".to_vec()).to_vec(),
                test_key(b"dog2".to_vec()).to_vec(),
                test_key(b"dog3".to_vec()).to_vec()
            ]
        );
    }
}
