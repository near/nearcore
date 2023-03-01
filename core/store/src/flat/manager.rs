use std::collections::HashMap;

use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{ShardId, BlockHeight};

use crate::Store;

use super::creation::{FlatStorageCreator, FlatStorageCreationStatus};
use super::storage::FlatStorage;
use super::store_helper;

enum FlatStorageSlot {
    Creator(FlatStorageCreator),
    Instance(FlatStorage),
    Disabled
}

pub struct FlatStorageManager {
    store: Store,
    instances: HashMap<ShardId, FlatStorage>
}

impl FlatStorageManager {
    pub fn add_for_shards(&self, shards: &[ShardUId], height: BlockHeight) {
        for shard_uid in shards {
            let status = store_helper::get_flat_storage_creation_status(&self.store, shard_uid.shard_id());
            let _slot = match status {
                FlatStorageCreationStatus::Ready => FlatStorageSlot::Instance(todo!()),
                FlatStorageCreationStatus::DontCreate => FlatStorageSlot::Disabled,
                _ => FlatStorageSlot::Creator(FlatStorageCreator::new(shard_uid.clone(), height))
            };
        }
    }

    pub fn update_status() {
    }

    pub fn instance(&self, shard_id: ShardId) -> FlatStorage {
        //let flat_strage = FlatStorage::from_store(self.store.clone(), shard_id);
        //self.instances.insert(shard_id, flat_strage);
        todo!()
    }
}
