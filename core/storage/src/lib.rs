extern crate byteorder;
extern crate elastic_array;
#[cfg(test)]
extern crate hex_literal;
#[macro_use]
extern crate log;
extern crate primitives;

use std::sync::Arc;
use std::sync::RwLock;

pub use kvdb::{DBTransaction, DBValue, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};

pub use crate::storages::{BlockChainStorage, GenericStorage};
pub use crate::storages::beacon::BeaconChainStorage;
use crate::storages::NUM_COLS;
pub use crate::storages::shard::ShardChainStorage;
pub use crate::trie::{DBChanges, Trie};
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator};

pub mod storages;
pub mod test_utils;
pub mod trie;

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
