
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

    pub fn put<T: Serialize>(&self, obj: T) -> primitives::hash::HashValue {
        let header_data = serialize(&obj).unwrap();
        let header_key : Vec<u8> = primitives::hash::hash(&header_data).into();
        self.db.put(&header_key, &header_data).ok();
        primitives::hash::HashValue::new(&header_key)
    }

    pub fn get<T: DeserializeOwned>(&self, key: primitives::hash::HashValue) -> Option<T> {
        let header_key : Vec<u8> = key.into();
        match self.db.get(&header_key) {
            Ok(Some(value)) => deserialize(&value).unwrap(),
            Ok(None) => None,
            Err(_e) => None,
        }
    }
}
