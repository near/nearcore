extern crate byteorder;
extern crate elastic_array;
#[cfg(test)]
extern crate hex_literal;
#[macro_use]
extern crate log;
extern crate primitives;
#[cfg(test)]
extern crate rand;

use std::sync::Arc;
use std::sync::RwLock;

pub use kvdb::{DBTransaction, DBValue, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};

use serde::{de::DeserializeOwned, Serialize};

pub use crate::storages::beacon::BeaconChainStorage;
pub use crate::storages::shard::ShardChainStorage;
use crate::storages::NUM_COLS;
pub use crate::storages::{BlockChainStorage, GenericStorage};
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator};
pub use crate::trie::{DBChanges, Trie};
use primitives::serialize::{Decode, Encode};

pub mod storages;
pub mod test_utils;
pub mod trie;

pub fn get<T: DeserializeOwned>(state_update: &TrieUpdate, key: &[u8]) -> Option<T> {
    state_update.get(key).and_then(|data| Decode::decode(&data).ok())
}

pub fn set<T: Serialize>(state_update: &mut TrieUpdate, key: Vec<u8>, value: &T) {
    value.encode().ok().map(|data| state_update.set(key, DBValue::from_vec(data))).or_else(|| {
        debug!("set value failed");
        None
    });
}

/// Initializes beacon and shard chain storages from the given path.
pub fn create_storage(
    storage_path: &str,
    num_shards: u32,
) -> (Arc<RwLock<BeaconChainStorage>>, Vec<Arc<RwLock<ShardChainStorage>>>) {
    let db_config = DatabaseConfig::with_columns(Some(NUM_COLS));
    let db =
        Arc::new(Database::open(&db_config, storage_path).expect("Failed to open the database"));
    let beacon = Arc::new(RwLock::new(BeaconChainStorage::new(db.clone())));
    let mut shards = vec![];
    for id in 0..num_shards {
        shards.push(Arc::new(RwLock::new(ShardChainStorage::new(db.clone(), id))));
    }
    (beacon, shards)
}
