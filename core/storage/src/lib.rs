
extern crate parity_rocksdb;
extern crate bincode;
extern crate primitives;
extern crate serde;

use parity_rocksdb::{DB, Writable};
use bincode::{serialize, deserialize};


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

    pub fn put<T: serde::ser::Serialize>(&self, obj: T) {
        let header_data = serialize(&obj).unwrap();
        let header_key : Vec<u8>  = primitives::hash::hash(&header_data).into();
        self.db.put(&header_key, &header_data).ok();
    }

    pub fn get<T>(&self, key: primitives::hash::HashValue) -> Option<T>
       where for<'a> T: serde::Deserialize<'a> {
        let header_key : Vec<u8> = key.into();
        match self.db.get(&header_key) {
            Ok(Some(value)) => deserialize(&value).unwrap(),
            Ok(None) => None,
            Err(_e) => None,
        }
    }
}
