use std::collections::HashMap;

use near_primitives::types::ShardId;

use crate::Store;

use super::storage::FlatStorage;

pub struct FlatStorageManager {
    store: Store,
    instances: HashMap<ShardId, FlatStorage>
}

impl FlatStorageManager {
    pub fn create(&mut self, shard_id: ShardId) {
        let flat_strage = FlatStorage::from_store(self.store.clone(), shard_id);
        self.instances.insert(shard_id, flat_strage);
    }

    pub fn get(&self, _shard_id: ShardId) -> FlatStorage {
        todo!()
    }
}
