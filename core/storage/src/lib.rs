extern crate parity_rocksdb;
extern crate bincode;
extern crate primitives;
extern crate serde;

use parity_rocksdb::{DB, Writable};
use bincode::{serialize, deserialize};
use serde::{Serialize, de::DeserializeOwned};


pub struct Storage {
    db: DB,
}

impl Storage {
    pub fn new(path: String) -> Self {
        let db = DB::open_default(&path).unwrap();
        Storage {
            db
        }
    }

    pub fn put<T: Serialize>(&self, obj: T) -> primitives::hash::CryptoHash {
        let header_data = serialize(&obj).unwrap();
        let header_key = primitives::hash::hash(&header_data);
        self.db.put(&header_key.0, &header_data).ok();
        header_key
    }

    pub fn get<T: DeserializeOwned>(&self, key: primitives::hash::CryptoHash) -> Option<T> {
        match self.db.get(&key.0) {
            Ok(Some(value)) => deserialize(&value).unwrap(),
            Ok(None) => None,
            Err(_e) => None,
        }
    }
}
