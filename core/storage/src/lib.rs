extern crate bincode;
extern crate parity_rocksdb;
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

use bincode::{deserialize, serialize};
use parity_rocksdb::{Writable, DB};
use primitives::hash::CryptoHash;
use primitives::types::{DBValue, MerkleHash};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

#[cfg(test)]
mod tests;

/// Concrete implementation of StateDbView.
/// Provides a way to interact with Storage and record changes with future commit.
#[derive(Default)]
pub struct StateDbView {
    // QUESTION: How to pass here StateDb so no need to pass it as argument everywhere?
    committed: HashMap<Vec<u8>, Option<DBValue>>,
    prospective: HashMap<Vec<u8>, Option<DBValue>>,
}

impl StateDbView {
    pub fn merkle_root(&self) -> MerkleHash {
        0
    }
    pub fn get(&self, _state_db: &StateDb, _key: &[u8]) -> Option<DBValue> {
        match self.prospective.get(_key) {
            Some(value) => value.clone(),
            None => match self.committed.get(_key) {
                Some(value) => value.clone(),
                None => _state_db.get(_key),
            },
        }
    }
    pub fn set(&mut self, _state_db: &StateDb, _key: &[u8], _value: DBValue) {
        self.prospective.insert(_key.to_vec(), Some(_value));
    }
    pub fn delete(&mut self, _state_db: &StateDb, _key: &[u8]) {
        self.prospective.insert(_key.to_vec(), None);
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
    pub fn finish(&self, _state_db: &mut StateDb) -> Self {
        StateDbView::default()
    }
}

pub struct Storage {
    db: DB,
}

impl Storage {
    pub fn new(path: &str) -> Self {
        let db = DB::open_default(&path).unwrap();
        Storage { db }
    }

    pub fn put<T: Serialize>(&self, obj: T) -> CryptoHash {
        let header_data = serialize(&obj).unwrap();
        let header_key = primitives::hash::hash(&header_data);
        self.db.put(header_key.as_ref(), &header_data).ok();
        header_key
    }

    pub fn get<T: DeserializeOwned>(&self, key: CryptoHash) -> Option<T> {
        match self.db.get(key.as_ref()) {
            Ok(Some(value)) => deserialize(&value).unwrap(),
            Ok(None) => None,
            Err(_e) => None,
        }
    }
}

#[allow(dead_code)]
pub struct StateDb {
    storage: Storage,
}

impl StateDb {
    pub fn new(storage: Storage) -> Self {
        StateDb { storage }
    }
    pub fn get_state_view(&self) -> StateDbView {
        StateDbView::default()
    }
    pub fn get(&self, _key: &[u8]) -> Option<DBValue> {
        None
    }
}
