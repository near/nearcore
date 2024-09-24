use crate::{
    flat::POISONED_LOCK_ERR,
    io::{Error, Result},
};
use near_primitives::shard_layout::ShardUId;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Stores a mapping from ShardUId to ShardUId.
///
/// Protected with mutex for concurrent access.
/// That is for resharding V3 purposes, where we use the mapping strategy for State column.
#[derive(Clone)]
pub(crate) struct ShardUIdMapping(Arc<Mutex<ShardUIdMappingInner>>);

impl ShardUIdMapping {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(ShardUIdMappingInner::new())))
    }

    pub fn map(&self, shard_uid: &ShardUId) -> Option<ShardUId> {
        self.lock().map(shard_uid)
    }

    pub fn update(&self, shard_uid: &ShardUId, db_mapped_shard_uid: Option<ShardUId>) -> ShardUId {
        self.lock().update(shard_uid, db_mapped_shard_uid)
    }

    fn lock(&self) -> std::sync::MutexGuard<ShardUIdMappingInner> {
        self.0.lock().expect(POISONED_LOCK_ERR)
    }
}

pub fn retrieve_shard_uid_from_db_key(key: &[u8]) -> Result<ShardUId> {
    // TODO(reshardingV3) Consider changing the Error type to `StorageError`?
    // Would need changing error types for `Store` methods as well.
    ShardUId::try_from(&key[..8])
        .map_err(|e| Error::other(format!("Could not retrieve ShardUId from db key: {}", e)))
}

pub fn replace_shard_uid_key_prefix(key: &[u8], shard_uid: ShardUId) -> Vec<u8> {
    let mut mapped_key = [0u8; 40];
    mapped_key[..8].copy_from_slice(&shard_uid.to_bytes());
    mapped_key[8..].copy_from_slice(&key[8..]);
    mapped_key.to_vec()
}

struct ShardUIdMappingInner {
    mapping: HashMap<ShardUId, ShardUId>,
}

impl ShardUIdMappingInner {
    pub fn new() -> Self {
        Self { mapping: HashMap::new() }
    }

    pub fn map(&self, shard_uid: &ShardUId) -> Option<ShardUId> {
        self.mapping.get(shard_uid).copied()
    }

    pub fn update(
        &mut self,
        shard_uid: &ShardUId,
        db_mapped_shard_uid: Option<ShardUId>,
    ) -> ShardUId {
        // No mapping means we map shard_uid to itself
        let mapped_shard_uid = db_mapped_shard_uid.unwrap_or(*shard_uid);
        self.mapping.insert(*shard_uid, mapped_shard_uid);
        mapped_shard_uid
    }
}
