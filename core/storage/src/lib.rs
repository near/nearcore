extern crate byteorder;
extern crate elastic_array;
#[cfg(test)]
extern crate hex_literal;
#[macro_use]
extern crate log;
extern crate primitives;

pub use kvdb::{DBTransaction, DBValue, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};


pub mod storages;
pub mod test_utils;
pub mod trie;

use crate::storages::NUM_COLS;
use std::sync::Arc;
pub use trie::update::{TrieUpdate, TrieUpdateIterator};
pub use trie::{DBChanges, Trie};
pub use storages::{BlockChainStorage, GenericStorage};
pub use storages::beacon::BeaconChainStorage;
pub use storages::shard::ShardChainStorage;
use std::sync::RwLock;

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
