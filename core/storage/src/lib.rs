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

use std::collections::HashMap;
use std::sync::Arc;

pub use kvdb::{DBTransaction, DBValue, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};
use parking_lot::RwLock;

use primitives::traits::{Decode, Encode};

pub mod trie;
pub mod test_utils;
pub mod storages;

pub use trie::{DBChanges, Trie};
pub use trie::update::{TrieUpdate, TrieUpdateIterator};

pub const COL_STATE: Option<u32> = Some(0);
pub const COL_EXTRA: Option<u32> = Some(1);
pub const COL_BLOCKS: Option<u32> = Some(2);
pub const COL_HEADERS: Option<u32> = Some(3);
pub const COL_BLOCK_INDEX: Option<u32> = Some(4);
pub const TOTAL_COLUMNS: Option<u32> = Some(5);

pub type Storage = KeyValueDB;

pub fn open_database(storage_path: &str) -> Database {
    let storage_config = DatabaseConfig::with_columns(TOTAL_COLUMNS);
    Database::open(&storage_config, storage_path).expect("Database wasn't open")
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
