use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::Peekable;
use std::sync::Arc;

use kvdb::DBValue;
use log::debug;

use near_primitives::hash::CryptoHash;

use crate::trie::TrieChanges;

use super::{Trie, TrieIterator};
use crate::StorageError;

/// Provides a way to access Storage and record changes with future commit.
pub struct TrieUpdate {
    pub trie: Arc<Trie>,
    root: CryptoHash,
    committed: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    prospective: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

/// For each prefix, the value is a <key, value> map that records the changes in the
/// trie update.
pub type PrefixKeyValueChanges = HashMap<Vec<u8>, HashMap<Vec<u8>, Option<Vec<u8>>>>;

impl TrieUpdate {
    pub fn new(trie: Arc<Trie>, root: CryptoHash) -> Self {
        TrieUpdate { trie, root, committed: BTreeMap::default(), prospective: BTreeMap::default() }
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<DBValue>, StorageError> {
        if let Some(value) = self.prospective.get(key) {
            Ok(value.as_ref().map(|val| DBValue::from_slice(val)))
        } else if let Some(value) = self.committed.get(key) {
            Ok(value.as_ref().map(|val| DBValue::from_slice(val)))
        } else {
            self.trie.get(&self.root, key).map(|x| x.map(DBValue::from_vec))
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
            for (key, value) in self.committed.range(prefix.to_vec()..) {
                if !key.starts_with(prefix) {
                    break;
                }
                prefix_key_value_change.insert(key.to_vec(), value.clone());
            }
            if !prefix_key_value_change.is_empty() {
                res.insert(prefix.to_vec(), prefix_key_value_change);
            }
        }
        Ok(res)
    }

    pub fn set(&mut self, key: Vec<u8>, value: DBValue) {
        self.prospective.insert(key, Some(value.into_vec()));
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
            self.remove(&key);
        }
        Ok(())
    }

    pub fn for_keys_with_prefix<F: FnMut(&[u8])>(&self, prefix: &[u8], mut f: F) {
        match self.iter(prefix) {
            Ok(iter) => {
                for key in iter {
                    f(&key);
                }
            }
            Err(e) => {
                debug!(target: "trie", "Error while iterating by prefix: {}", e);
            }
        }
    }

    pub fn commit(&mut self) {
        if self.committed.is_empty() {
            std::mem::swap(&mut self.prospective, &mut self.committed);
        } else {
            for (key, val) in std::mem::replace(&mut self.prospective, BTreeMap::new()).into_iter()
            {
                *self.committed.entry(key).or_default() = val;
            }
        }
    }

    pub fn rollback(&mut self) {
        self.prospective.clear();
    }

    pub fn finalize(mut self) -> Result<TrieChanges, StorageError> {
        if !self.prospective.is_empty() {
            self.commit();
        }
        let TrieUpdate { trie, root, committed, .. } = self;
        trie.update(&root, committed.into_iter())
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
        self.root
    }
}

struct MergeIter<'a, I: Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)>> {
    left: Peekable<I>,
    right: Peekable<I>,
}

impl<'a, I: Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)>> Iterator for MergeIter<'a, I> {
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

type MergeBTreeRange<'a> =
    MergeIter<'a, std::collections::btree_map::Range<'a, Vec<u8>, Option<Vec<u8>>>>;

pub struct TrieUpdateIterator<'a> {
    prefix: Vec<u8>,
    end_offset: Option<Vec<u8>>,
    trie_iter: Peekable<TrieIterator<'a>>,
    overlay_iter: Peekable<MergeBTreeRange<'a>>,
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
        let committed_iter = state_update.committed.range(start_offset.clone()..);
        let prospective_iter = state_update.prospective.range(start_offset..);
        let overlay_iter =
            MergeIter { left: committed_iter.peekable(), right: prospective_iter.peekable() }
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
    type Item = Vec<u8>;

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
                    (Some(&Err(_)), _) => return None,
                }
            };

            // Check which elements comes first and only advance the corresponding iterator.
            // If two keys are equal, take the value from `right`.
            return match res {
                Ordering::Trie => match self.trie_iter.next() {
                    Some(Ok(value)) => Some(value.0),
                    _ => None,
                },
                Ordering::Overlay => match self.overlay_iter.next() {
                    Some((key, Some(_))) => Some(key.clone()),
                    Some((_, None)) => continue,
                    None => None,
                },
                Ordering::Both => {
                    self.trie_iter.next();
                    match self.overlay_iter.next() {
                        Some((key, Some(_))) => Some(key.clone()),
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

    #[test]
    fn trie() {
        let trie = create_trie();
        let root = CryptoHash::default();
        let mut trie_update = TrieUpdate::new(trie.clone(), root);
        trie_update.set(b"dog".to_vec(), DBValue::from_slice(b"puppy"));
        trie_update.set(b"dog2".to_vec(), DBValue::from_slice(b"puppy"));
        trie_update.set(b"xxx".to_vec(), DBValue::from_slice(b"puppy"));
        let (store_update, new_root) = trie_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().ok();
        let trie_update2 = TrieUpdate::new(trie.clone(), new_root);
        assert_eq!(trie_update2.get(b"dog"), Ok(Some(DBValue::from_slice(b"puppy"))));
        let mut values = vec![];
        trie_update2.for_keys_with_prefix(b"dog", |key| values.push(key.to_vec()));
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);
    }

    #[test]
    fn trie_remove() {
        let trie = create_trie();

        // Delete non-existing element.
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.remove(b"dog");
        let (store_update, new_root) = trie_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());

        // Add and right away delete element.
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.set(b"dog".to_vec(), DBValue::from_slice(b"puppy"));
        trie_update.remove(b"dog");
        let (store_update, new_root) = trie_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());

        // Add, apply changes and then delete element.
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.set(b"dog".to_vec(), DBValue::from_slice(b"puppy"));
        let (store_update, new_root) = trie_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_ne!(new_root, CryptoHash::default());
        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.remove(b"dog");
        let (store_update, new_root) = trie_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().ok();
        assert_eq!(new_root, CryptoHash::default());
    }

    #[test]
    fn trie_iter() {
        let trie = create_trie();
        let mut trie_update = TrieUpdate::new(trie.clone(), CryptoHash::default());
        trie_update.set(b"dog".to_vec(), DBValue::from_slice(b"puppy"));
        trie_update.set(b"aaa".to_vec(), DBValue::from_slice(b"puppy"));
        let (store_update, new_root) = trie_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().ok();

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.set(b"dog2".to_vec(), DBValue::from_slice(b"puppy"));
        trie_update.set(b"xxx".to_vec(), DBValue::from_slice(b"puppy"));

        let values: Vec<Vec<u8>> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);

        trie_update.rollback();

        let values: Vec<Vec<u8>> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec()]);

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.remove(b"dog");

        let values: Vec<Vec<u8>> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.len(), 0);

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.set(b"dog2".to_vec(), DBValue::from_slice(b"puppy"));
        trie_update.commit();
        trie_update.remove(b"dog2");

        let values: Vec<Vec<u8>> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec()]);

        let mut trie_update = TrieUpdate::new(trie.clone(), new_root);
        trie_update.set(b"dog2".to_vec(), DBValue::from_slice(b"puppy"));
        trie_update.commit();
        trie_update.set(b"dog3".to_vec(), DBValue::from_slice(b"puppy"));

        let values: Vec<Vec<u8>> = trie_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec(), b"dog3".to_vec()]);

        let values: Vec<Vec<u8>> = trie_update.range(b"do", b"g", b"g21").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);

        let values: Vec<Vec<u8>> = trie_update.range(b"do", b"", b"xyz").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec(), b"dog3".to_vec()]);
    }
}
