use kvdb::DBValue;
use primitives::types::MerkleHash;
use std::collections::BTreeMap;
use std::iter::Peekable;
use std::sync::Arc;

use crate::state_db::trie;
use crate::state_db::trie::DBChanges;
use crate::state_db::StateDb;

/// Provides a way to access Storage and record changes with future commit.
pub struct StateDbUpdate {
    state_db: Arc<StateDb>,
    root: MerkleHash,
    committed: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    prospective: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl StateDbUpdate {
    pub fn new(state_db: Arc<StateDb>, root: MerkleHash) -> Self {
        StateDbUpdate {
            state_db,
            root,
            committed: BTreeMap::default(),
            prospective: BTreeMap::default(),
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<DBValue> {
        if let Some(value) = self.prospective.get(key) {
            Some(DBValue::from_slice(value.as_ref()?))
        } else if let Some(value) = self.committed.get(key) {
            Some(DBValue::from_slice(value.as_ref()?))
        } else {
            self.state_db.trie.get(&self.root, key).map(|x| DBValue::from_slice(&x))
        }
    }
    pub fn set(&mut self, key: &[u8], value: &DBValue) {
        self.prospective.insert(key.to_vec(), Some(value.to_vec()));
    }
    pub fn remove(&mut self, key: &[u8]) {
        self.prospective.insert(key.to_vec(), None);
    }
    pub fn for_keys_with_prefix<F: FnMut(&[u8])>(&self, prefix: &[u8], mut f: F) {
        // TODO: join with iterating over committed / perspective overlay here.
        let mut iter = move || -> Result<(), String> {
            let mut iter = self.state_db.trie.iter(&self.root)?;
            iter.seek(prefix)?;
            for x in iter {
                let (key, _) = x?;
                if !key.starts_with(prefix) {
                    break;
                }
                f(&key);
            }
            Ok(())
        };
        if let Err(e) = iter() {
            debug!(target: "trie", "Error while iterating by prefix: {}", e);
        }
    }
    pub fn commit(&mut self) {
        if self.committed.is_empty() {
            ::std::mem::swap(&mut self.prospective, &mut self.committed);
        } else {
            for (key, val) in self.prospective.iter() {
                *self.committed.entry(key.clone()).or_default() = val.clone();
            }
            self.prospective.clear();
        }
    }
    pub fn rollback(&mut self) {
        self.prospective.clear();
    }
    pub fn finalize(mut self) -> (MerkleHash, DBChanges) {
        if !self.prospective.is_empty() {
            self.commit();
        }
        let (db_changes, root) = self.state_db.trie.update(
            &self.root,
            self.committed.iter().map(|(key, value)| (key.clone(), value.clone())),
        );
        (root, db_changes)
    }
    pub fn iter(&self, prefix: &[u8]) -> Result<StateDbUpdateIterator, String> {
        StateDbUpdateIterator::new(self, prefix, b"", None)
    }

    pub fn range(
        &self,
        prefix: &[u8],
        start: &[u8],
        end: &[u8],
    ) -> Result<StateDbUpdateIterator, String> {
        StateDbUpdateIterator::new(self, prefix, start, Some(end))
    }

    pub fn get_root(&self) -> MerkleHash {
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
            (Some(&(ref left_key, _)), Some(&(ref right_key, _))) => {
                if left_key < right_key {
                    std::cmp::Ordering::Less
                } else {
                    if left_key == right_key {
                        std::cmp::Ordering::Equal
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
            }
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

pub struct StateDbUpdateIterator<'a> {
    prefix: Vec<u8>,
    end_offset: Option<Vec<u8>>,
    trie_iter: Peekable<trie::TrieIterator<'a>>,
    overlay_iter: Peekable<MergeBTreeRange<'a>>,
}

impl<'a> StateDbUpdateIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    pub fn new(
        state_update: &'a StateDbUpdate,
        prefix: &[u8],
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, String> {
        let mut trie_iter = state_update.state_db.trie.iter(&state_update.root)?;
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
        Ok(StateDbUpdateIterator {
            prefix: prefix.to_vec(),
            end_offset,
            trie_iter: trie_iter.peekable(),
            overlay_iter,
        })
    }
    //
    //    #[inline]
    //    fn stop_cond(&self, key: &Vec<u8>) -> bool {
    //        !key.starts_with(&self.prefix) || match &self.end_offset {
    //            Some(end) => key > end,
    //            None => true
    //        }
    //    }
}

impl<'a> Iterator for StateDbUpdateIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let stop_cond = |key: &Vec<u8>, prefix: &Vec<u8>, end_offset: &Option<Vec<u8>>| {
            !key.starts_with(prefix)
                || match end_offset {
                    Some(end) => key > end,
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
                                } else {
                                    if &left_key == right_key {
                                        Ordering::Both
                                    } else {
                                        Ordering::Overlay
                                    }
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
    use crate::test_utils::create_state_db;

    use super::*;

    #[test]
    fn state_db() {
        let state_db = Arc::new(create_state_db());
        let root = MerkleHash::default();
        let mut state_db_update = StateDbUpdate::new(state_db.clone(), root);
        state_db_update.set(b"dog", &DBValue::from_slice(b"puppy"));
        state_db_update.set(b"dog2", &DBValue::from_slice(b"puppy"));
        state_db_update.set(b"xxx", &DBValue::from_slice(b"puppy"));
        let (new_root, transaction) = state_db_update.finalize();
        state_db.commit(transaction).ok();
        let state_db_update2 = StateDbUpdate::new(state_db.clone(), new_root);
        assert_eq!(state_db_update2.get(b"dog").unwrap(), DBValue::from_slice(b"puppy"));
        let mut values = vec![];
        state_db_update2.for_keys_with_prefix(b"dog", |key| values.push(key.to_vec()));
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);
    }

    #[test]
    fn state_db_iter() {
        let state_db = Arc::new(create_state_db());
        let mut state_db_update = StateDbUpdate::new(state_db.clone(), MerkleHash::default());
        state_db_update.set(b"dog", &DBValue::from_slice(b"puppy"));
        state_db_update.set(b"aaa", &DBValue::from_slice(b"puppy"));
        let (new_root, transaction) = state_db_update.finalize();
        state_db.commit(transaction).ok();

        let mut state_db_update = StateDbUpdate::new(state_db.clone(), new_root);
        state_db_update.set(b"dog2", &DBValue::from_slice(b"puppy"));
        state_db_update.set(b"xxx", &DBValue::from_slice(b"puppy"));

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);

        state_db_update.rollback();

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec()]);

        let mut state_db_update = StateDbUpdate::new(state_db.clone(), new_root);
        state_db_update.remove(b"dog");

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.len(), 0);

        let mut state_db_update = StateDbUpdate::new(state_db.clone(), new_root);
        state_db_update.set(b"dog2", &DBValue::from_slice(b"puppy"));
        state_db_update.commit();
        state_db_update.remove(b"dog2");

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec()]);

        let mut state_db_update = StateDbUpdate::new(state_db.clone(), new_root);
        state_db_update.set(b"dog2", &DBValue::from_slice(b"puppy"));
        state_db_update.commit();
        state_db_update.set(b"dog3", &DBValue::from_slice(b"puppy"));

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec(), b"dog3".to_vec()]);

        let values: Vec<Vec<u8>> = state_db_update.range(b"do", b"g", b"g2").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);

        let values: Vec<Vec<u8>> = state_db_update.range(b"do", b"", b"xyz").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec(), b"dog3".to_vec()]);
    }
}
