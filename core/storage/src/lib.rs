extern crate elastic_array;
#[cfg(test)]
extern crate hex_literal;
extern crate kvdb;
extern crate kvdb_memorydb;
extern crate kvdb_rocksdb;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate primitives;
extern crate serde;

use std::collections::HashMap;
use std::sync::Arc;

pub use kvdb::{DBValue, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};

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
    committed: HashMap<Vec<u8>, Option<Vec<u8>>>,
    prospective: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl StateDbUpdate {
    pub fn new(state_db: Arc<StateDb>, root: MerkleHash) -> Self {
        StateDbUpdate {
            state_db,
            root,
            committed: HashMap::default(),
            prospective: HashMap::default(),
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
            for (key, val) in self.prospective.drain() {
                *self.committed.entry(key).or_default() = val;
            }
        }
    }
    pub fn rollback(&mut self) {
        self.prospective.clear();
    }
    pub fn finalize(mut self) -> (DBChanges, MerkleHash) {
        if !self.prospective.is_empty() {
            self.commit();
        }
        self.state_db.trie.update(&self.root, self.committed.drain())
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
}
