use std::sync::Arc;
use std::{fmt, io};

use cached::{Cached, SizedCache};
pub use kvdb::DBValue;
use kvdb::{DBOp, DBTransaction, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};
use serde::de::DeserializeOwned;
use serde::Serialize;

use near_primitives::serialize::{to_base, Decode, Encode};

pub use crate::trie::{
    update::TrieUpdate, update::TrieUpdateIterator, Trie, TrieChanges, TrieIterator,
    WrappedTrieChanges,
};

pub mod test_utils;
mod trie;

pub const COL_BLOCK_MISC: Option<u32> = Some(0);
pub const COL_BLOCK: Option<u32> = Some(1);
pub const COL_BLOCK_HEADER: Option<u32> = Some(2);
pub const COL_BLOCK_INDEX: Option<u32> = Some(3);
pub const COL_STATE: Option<u32> = Some(4);
pub const COL_STATE_REF: Option<u32> = Some(5);
pub const COL_TRANSACTION_RESULT: Option<u32> = Some(6);
pub const COL_RECEIPTS: Option<u32> = Some(7);
pub const COL_PEERS: Option<u32> = Some(8);
pub const COL_VALIDATORS: Option<u32> = Some(9);
const NUM_COLS: u32 = 10;

pub struct Store {
    storage: Arc<dyn KeyValueDB>,
}

impl Store {
    pub fn new(storage: Arc<dyn KeyValueDB>) -> Store {
        Store { storage }
    }

    pub fn get(&self, column: Option<u32>, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        self.storage.get(column, key).map(|a| a.map(|b| b.to_vec()))
    }

    pub fn get_ser<T: Decode + DeserializeOwned>(
        &self,
        column: Option<u32>,
        key: &[u8],
    ) -> Result<Option<T>, io::Error> {
        match self.storage.get(column, key) {
            Ok(Some(bytes)) => match Decode::decode(bytes.as_ref()) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(e),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn exists(&self, column: Option<u32>, key: &[u8]) -> Result<bool, io::Error> {
        self.storage.get(column, key).map(|value| value.is_some())
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate::new(self.storage.clone())
    }

    pub fn iter<'a>(
        &'a self,
        column: Option<u32>,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        self.storage.iter(column)
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    storage: Arc<dyn KeyValueDB>,
    transaction: DBTransaction,
    /// Optionally has reference to the trie to clear cache on the commit.
    trie: Option<Arc<Trie>>,
}

impl StoreUpdate {
    pub fn new(storage: Arc<dyn KeyValueDB>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, trie: None }
    }

    pub fn new_with_trie(storage: Arc<dyn KeyValueDB>, trie: Arc<Trie>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, trie: Some(trie) }
    }

    pub fn set(&mut self, column: Option<u32>, key: &[u8], value: &[u8]) {
        self.transaction.put(column, key, value)
    }

    pub fn set_ser<T: Encode>(
        &mut self,
        column: Option<u32>,
        key: &[u8],
        value: &T,
    ) -> Result<(), io::Error> {
        let data = Encode::encode(value)?;
        self.set(column, key, &data);
        Ok(())
    }

    pub fn delete(&mut self, column: Option<u32>, key: &[u8]) {
        self.transaction.delete(column, key);
    }

    /// Merge another store update into this one.
    pub fn merge(&mut self, other: StoreUpdate) {
        if self.trie.is_none() {
            if let Some(trie) = other.trie {
                self.trie = Some(trie);
            }
        }
        self.merge_transaction(other.transaction);
    }

    /// Merge DB Transaction.
    pub fn merge_transaction(&mut self, transaction: DBTransaction) {
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => self.transaction.put(col, &key, &value),
                DBOp::Delete { col, key } => self.transaction.delete(col, &key),
            }
        }
    }

    pub fn commit(self) -> Result<(), io::Error> {
        if let Some(trie) = self.trie {
            trie.update_cache(&self.transaction)?;
        }
        self.storage.write(self.transaction)
    }
}

impl fmt::Debug for StoreUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Store Update {{\n")?;
        for op in self.transaction.ops.iter() {
            match op {
                DBOp::Insert { col, key, value: _ } => {
                    write!(f, "  + {:?} {}\n", col, to_base(key))?
                }
                DBOp::Delete { col, key } => write!(f, "  - {:?} {}\n", col, to_base(key))?,
            }
        }
        write!(f, "}}\n")
    }
}

pub fn read_with_cache<'a, T: Decode + DeserializeOwned + 'a>(
    storage: &Store,
    col: Option<u32>,
    cache: &'a mut SizedCache<Vec<u8>, T>,
    key: &[u8],
) -> io::Result<Option<&'a T>> {
    let key_vec = key.to_vec();
    if cache.cache_get(&key_vec).is_some() {
        return Ok(Some(cache.cache_get(&key_vec).unwrap()));
    }
    if let Some(result) = storage.get_ser(col, key)? {
        cache.cache_set(key.to_vec(), result);
        return Ok(cache.cache_get(&key_vec));
    }
    Ok(None)
}

pub fn create_store(path: &str) -> Arc<Store> {
    let db_config = DatabaseConfig::with_columns(Some(NUM_COLS));
    let db = Arc::new(Database::open(&db_config, path).expect("Failed to open the database"));
    Arc::new(Store::new(db))
}

/// Reads an object from Trie.
pub fn get<T: DeserializeOwned>(state_update: &TrieUpdate, key: &[u8]) -> Option<T> {
    state_update.get(key).and_then(|data| Decode::decode(&data).ok())
}

/// Writes an object into Trie.
pub fn set<T: Serialize>(state_update: &mut TrieUpdate, key: Vec<u8>, value: &T) {
    value.encode().ok().map(|data| state_update.set(key, DBValue::from_vec(data))).or_else(|| None);
}
