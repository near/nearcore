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
use serde::{de::DeserializeOwned, Serialize};

#[cfg(test)]
mod tests;

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
