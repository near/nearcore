extern crate byteorder;
extern crate elastic_array;
#[cfg(test)]
extern crate hex_literal;
extern crate kvdb;
extern crate kvdb_memorydb;
extern crate kvdb_rocksdb;
#[macro_use]
extern crate log;
extern crate primitives;
extern crate serde;

use std::collections::{BTreeMap, HashMap};
use std::iter::Peekable;
use std::sync::Arc;

pub use kvdb::{DBValue, DBTransaction, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};
use parking_lot::RwLock;

use primitives::traits::{Decode, Encode};
use primitives::types::MerkleHash;
pub use crate::trie::DBChanges;

mod nibble_slice;

pub mod test_utils;
pub mod trie;

pub const COL_STATE: Option<u32> = Some(0);
pub const COL_EXTRA: Option<u32> = Some(1);
pub const COL_BLOCKS: Option<u32> = Some(2);
pub const COL_HEADERS: Option<u32> = Some(3);
pub const COL_BLOCK_INDEX: Option<u32> = Some(4);
pub const TOTAL_COLUMNS: Option<u32> = Some(5);

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
    pub fn delete(&mut self, key: &[u8]) {
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
    pub fn finalize(mut self) -> (DBChanges, MerkleHash) {
        if !self.prospective.is_empty() {
            self.commit();
        }
        self.state_db.trie.update(
            &self.root,
          self.committed
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())))
    }
    pub fn iter<'a>(&'a self, prefix: &[u8]) -> Result<StateDbUpdateIterator<'a>, String> {
        StateDbUpdateIterator::new(self, prefix)
    }
}

struct MergeIter<'a, I: Iterator<Item=(&'a Vec<u8>, &'a Option<Vec<u8>>)>> {
    left: Peekable<I>,
    right: Peekable<I>
}

impl<'a, I: Iterator<Item=(&'a Vec<u8>, &'a Option<Vec<u8>>)>> Iterator for MergeIter<'a, I> {
    type Item = (&'a Vec<u8>, &'a Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        let res = match (self.left.peek(), self.right.peek()) {
            (Some(&(ref left_key, _)), Some(&(ref right_key, _))) => if left_key < right_key {
                std::cmp::Ordering::Less
            } else {
                if left_key == right_key { std::cmp::Ordering::Equal } else { std::cmp::Ordering::Greater }
            },
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

type MergeBTreeRange<'a> = MergeIter<'a, std::collections::btree_map::Range<'a, Vec<u8>, Option<Vec<u8>>>>;

pub struct StateDbUpdateIterator<'a> {
    prefix: Vec<u8>,
    trie_iter: Peekable<trie::TrieIterator<'a>>,
    overlay_iter: Peekable<MergeBTreeRange<'a>>,
}

impl<'a> StateDbUpdateIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    pub fn new(state_update: &'a StateDbUpdate, prefix: &[u8]) -> Result<Self, String> {
        let mut trie_iter = state_update.state_db.trie.iter(&state_update.root)?;
        trie_iter.seek(prefix)?;
        let committed_iter = state_update.committed.range(prefix.to_vec()..);
        let prospective_iter = state_update.prospective.range(prefix.to_vec()..);
        let overlay_iter = MergeIter { left: committed_iter.peekable(), right: prospective_iter.peekable() }.peekable();
        Ok(StateDbUpdateIterator {
            prefix: prefix.to_vec(),
            trie_iter: trie_iter.peekable(),
            overlay_iter
        })
    }
}

impl<'a> Iterator for StateDbUpdateIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
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
                        match (left_key.starts_with(&self.prefix), right_key.starts_with(&self.prefix)) {
                            (true, true) => if left_key < right_key {
                                Ordering::Trie
                            } else {
                                if &left_key == right_key { Ordering::Both } else { Ordering::Overlay }
                            },
                            (true, false) => Ordering::Trie,
                            (false, true) => Ordering::Overlay,
                            (false, false) => { return None; }
                        }
                    },
                    (Some(&Ok((ref left_key, _))), None) => {
                        if !left_key.starts_with(&self.prefix) {
                            return None
                        }
                        Ordering::Trie
                    },
                    (None, Some(&(ref right_key, _))) => {
                        if !right_key.starts_with(&self.prefix) {
                            return None
                        }
                        Ordering::Overlay
                    },
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
                Ordering::Overlay => {
                    match self.overlay_iter.next() {
                        Some((key, Some(_))) => Some(key.clone()),
                        Some((_, None)) => continue,
                        None => None
                    }
                },
                Ordering::Both => {
                    self.trie_iter.next();
                    match self.overlay_iter.next() {
                        Some((key, Some(_))) => Some(key.clone()),
                        Some((_, None)) => continue,
                        None => None
                    }
                }
            };
        }
    }
}

pub type Storage = KeyValueDB;
pub type DiskStorageConfig = DatabaseConfig;
pub type DiskStorage = Database;

pub struct StateDb {
    trie: trie::Trie,
    storage: Arc<KeyValueDB>,
}

impl StateDb {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        StateDb { trie: trie::Trie::new(storage.clone(), COL_STATE), storage }
    }

    pub fn commit(&self, transaction: DBChanges) -> std::io::Result<()> {
        trie::apply_changes(&self.storage, COL_STATE, transaction)
    }
}

pub fn open_database(storage_path: &str) -> Database {
    let storage_config = DiskStorageConfig::with_columns(TOTAL_COLUMNS);
    DiskStorage::open(&storage_config, storage_path).expect("Database wasn't open")
}

pub fn write_with_cache<T: Clone + Encode>(
    storage: &Arc<Storage>,
    col: Option<u32>,
    cache: &RwLock<HashMap<Vec<u8>, T>>,
    key: &[u8],
    value: &T,
) {
    let data = Encode::encode(value).expect("Error serializing data");
    cache.write().insert(key.to_vec(), value.clone());

    let mut db_transaction = storage.transaction();
    db_transaction.put(col, key, &data);
    storage.write(db_transaction).expect("Database write failed");
}

pub fn extend_with_cache<T: Clone + Encode>(
    storage: &Arc<Storage>,
    col: Option<u32>,
    cache: &RwLock<HashMap<Vec<u8>, T>>,
    values: HashMap<Vec<u8>, T>,
) {
    let mut db_transaction = storage.transaction();
    for (key, value) in values {
        let data = Encode::encode(&value).expect("Error serializing data");
        cache.write().insert(key.clone(), value.clone());
        db_transaction.put(col, &key, &data);
    }
    storage.write(db_transaction).expect("Database write failed");
}

pub fn read_with_cache<T: Clone + Decode>(
    storage: &Arc<Storage>,
    col: Option<u32>,
    cache: &RwLock<HashMap<Vec<u8>, T>>,
    key: &[u8],
) -> Option<T> {
    {
        let read = cache.read();
        if let Some(v) = read.get(key) {
            return Some(v.clone());
        }
    }

    match storage.get(col, key) {
        Ok(Some(value)) => Decode::decode(value.as_ref()).ok().map(|value: T| {
            let mut write = cache.write();
            write.insert(key.to_vec(), value.clone());
            value
        }),
        _ => None,
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
        let (transaction, new_root) = state_db_update.finalize();
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
        let (transaction, new_root) = state_db_update.finalize();
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
        state_db_update.delete(b"dog");

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values.len(), 0);

        let mut state_db_update = StateDbUpdate::new(state_db.clone(), new_root);
        state_db_update.set(b"dog2", &DBValue::from_slice(b"puppy"));
        state_db_update.commit();
        state_db_update.delete(b"dog2");

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec()]);

        let mut state_db_update = StateDbUpdate::new(state_db.clone(), new_root);
        state_db_update.set(b"dog2", &DBValue::from_slice(b"puppy"));
        state_db_update.commit();
        state_db_update.set(b"dog3", &DBValue::from_slice(b"puppy"));

        let values: Vec<Vec<u8>> = state_db_update.iter(b"dog").unwrap().collect();
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec(), b"dog3".to_vec()]);
    }
}
