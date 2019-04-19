use std::sync::Arc;

use crate::storages::beacon::BeaconChainStorage;
use crate::storages::shard::ShardChainStorage;
use crate::storages::NUM_COLS;
use crate::Trie;
use std::sync::RwLock;

/// Creates one beacon storage and one shard storage using in-memory database.
pub fn create_beacon_shard_storages() -> (Arc<RwLock<BeaconChainStorage>>, Arc<RwLock<ShardChainStorage>>) {
    let db = Arc::new(kvdb_memorydb::create(NUM_COLS));
    let beacon = BeaconChainStorage::new(db.clone());
    let shard = ShardChainStorage::new(db.clone(), 0);
    (Arc::new(RwLock::new(beacon)), Arc::new(RwLock::new(shard)))
}

/// Creates a Trie using a single shard storage that uses in-memory database.
pub fn create_trie() -> Arc<Trie> {
    let shard_storage = create_beacon_shard_storages().1;
    Arc::new(Trie::new(shard_storage))
}