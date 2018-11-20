extern crate bincode;
extern crate parity_rocksdb;
extern crate parking_lot;
extern crate primitives;
extern crate serde;
extern crate substrate_primitives;
extern crate substrate_state_machine;
extern crate substrate_trie;

#[cfg(test)]
extern crate hex_literal;
#[cfg(test)]
extern crate memory_db;
#[cfg(test)]
extern crate trie_db;

use parity_rocksdb::{Writable, DB};
use parking_lot::RwLock;
use primitives::types::{DBValue, MerkleHash};
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Concrete implementation of StateDbUpdate.
/// Provides a way to access Storage and record changes with future commit.
pub struct StateDbUpdate<'a> {
    state_db: &'a mut StateDb,
    root: &'a MerkleHash,
    committed: HashMap<Vec<u8>, Option<DBValue>>,
    prospective: HashMap<Vec<u8>, Option<DBValue>>,
}

impl<'a> StateDbUpdate<'a> {
    pub fn new(state_db: &'a mut StateDb, root: &'a MerkleHash) -> Self {
        StateDbUpdate {
            state_db,
            root,
            committed: HashMap::default(),
            prospective: HashMap::default(),
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<DBValue> {
        match self.prospective.get(key) {
            Some(value) => value.clone(),
            None => match self.committed.get(key) {
                Some(value) => value.clone(),
                None => self.state_db.get(self.root, key),
            },
        }
    }
    pub fn set(&mut self, key: &[u8], value: DBValue) {
        self.prospective.insert(key.to_vec(), Some(value));
    }
    pub fn delete(&mut self, key: &[u8]) {
        self.prospective.insert(key.to_vec(), None);
    }
    pub fn commit(&mut self) {
        for (key, value) in self.prospective.iter() {
            self.committed.insert(key.to_vec(), value.clone());
        }
        self.prospective = HashMap::new();
    }
    pub fn rollback(&mut self) {
        self.prospective = HashMap::new();
    }
    pub fn finalize(&self) -> MerkleHash {
        for (key, value) in self.committed.iter() {
            match value {
                Some(value) => self.state_db.set(key, value),
                None => self.state_db.delete(key),
            }
        }
        MerkleHash::default()
    }
}

pub trait Storage: Send + Sync {
    fn set(&self, key: &[u8], value: &[u8]);
    fn get(&self, key: &[u8]) -> Option<DBValue>;
}

#[derive(Default)]
pub struct MemoryStorage {
    db: RwLock<HashMap<Vec<u8>, DBValue>>,
}

impl Storage for MemoryStorage {
    fn set(&self, key: &[u8], value: &[u8]) {
        self.db.write().insert(key.to_vec(), value.to_vec());
    }
    fn get(&self, key: &[u8]) -> Option<DBValue> {
        match self.db.read().get(key) {
            Some(value) => Some(value.to_vec()),
            None => None,
        }
    }
}

pub struct DiskStorage {
    db: DB,
}

impl DiskStorage {
    pub fn new(path: &str) -> Self {
        let db = DB::open_default(&path).unwrap();
        DiskStorage { db }
    }
}

impl Storage for DiskStorage {
    fn set(&self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).ok();
    }
    fn get(&self, key: &[u8]) -> Option<DBValue> {
        match self.db.get(key) {
            Ok(Some(value)) => Some(value.to_vec()),
            Ok(None) => None,
            Err(_e) => None,
        }
    }
}

#[allow(dead_code)]
pub struct StateDb {
    storage: Arc<Storage>,
}

impl StateDb {
    pub fn new(storage: &Arc<Storage>) -> Self {
        StateDb {
            storage: storage.clone(),
        }
    }
    pub fn get_state_view(&self) -> MerkleHash {
        MerkleHash::default()
    }
    pub fn set(&self, _key: &[u8], _value: &[u8]) {}
    pub fn get(&self, _root: &MerkleHash, _key: &[u8]) -> Option<DBValue> {
        None
    }
    pub fn delete(&self, _key: &[u8]) {}
}
