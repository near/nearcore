use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};
use crate::db::{
    CLOUD_BLOCK_HEAD_KEY, CLOUD_MIN_HEAD_KEY, CLOUD_PREV_EPOCH_END_KEY, CLOUD_SHARD_HEAD_PREFIX,
    cloud_shard_head_key, cloud_shard_head_key_shard_id,
};
use crate::{DBCol, Store, StoreUpdate};
use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, ShardId};

/// What the node's own store holds for cloud archival.
#[derive(Clone)]
pub struct CloudArchivalStoreAdapter {
    store: Store,
}

impl StoreAdapter for CloudArchivalStoreAdapter {
    fn store_ref(&self) -> &Store {
        &self.store
    }
}

impl CloudArchivalStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn store_update(&self) -> CloudArchivalStoreUpdateAdapter<'static> {
        CloudArchivalStoreUpdateAdapter {
            store_update: StoreUpdateHolder::Owned(self.store.store_update()),
        }
    }

    /// Height up to which every archivized component has reached the bucket.
    pub fn min_head(&self) -> Option<BlockHeight> {
        self.store.get_ser(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY)
    }

    /// Height up to which block data has reached the bucket.
    pub fn block_head(&self) -> Option<BlockHeight> {
        self.store.get_ser(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY)
    }

    /// Height up to which one shard's data has reached the bucket.
    pub fn shard_head(&self, shard_id: ShardId) -> Option<BlockHeight> {
        self.store.get_ser(DBCol::BlockMisc, &cloud_shard_head_key(shard_id))
    }

    /// Last block of the latest fully archivized epoch.
    pub fn prev_epoch_end(&self) -> Option<CryptoHash> {
        self.store.get_ser(DBCol::BlockMisc, CLOUD_PREV_EPOCH_END_KEY)
    }

    /// Every shard the node has recorded a head for, in shard order.
    pub fn all_shard_heads(&self) -> Vec<(ShardId, BlockHeight)> {
        let mut heads = Vec::new();
        for (key, value) in self.store.iter_prefix(DBCol::BlockMisc, CLOUD_SHARD_HEAD_PREFIX) {
            let Some(shard_id) = cloud_shard_head_key_shard_id(&key) else {
                continue;
            };
            let Ok(height) = BlockHeight::try_from_slice(&value) else {
                continue;
            };
            heads.push((shard_id, height));
        }
        heads.sort_by_key(|(shard_id, _)| *shard_id);
        heads
    }
}

pub struct CloudArchivalStoreUpdateAdapter<'a> {
    store_update: StoreUpdateHolder<'a>,
}

impl Into<StoreUpdate> for CloudArchivalStoreUpdateAdapter<'static> {
    fn into(self) -> StoreUpdate {
        self.store_update.into()
    }
}

impl CloudArchivalStoreUpdateAdapter<'static> {
    pub fn commit(self) {
        let store_update: StoreUpdate = self.into();
        store_update.commit();
    }
}

impl<'a> StoreUpdateAdapter for CloudArchivalStoreUpdateAdapter<'a> {
    fn store_update(&mut self) -> &mut StoreUpdate {
        &mut self.store_update
    }
}

impl<'a> CloudArchivalStoreUpdateAdapter<'a> {
    pub fn new(store_update: &'a mut StoreUpdate) -> Self {
        Self { store_update: StoreUpdateHolder::Reference(store_update) }
    }

    pub fn set_min_head(&mut self, height: BlockHeight) {
        self.store_update.set_ser(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY, &height);
    }

    pub fn set_block_head(&mut self, height: BlockHeight) {
        self.store_update.set_ser(DBCol::BlockMisc, CLOUD_BLOCK_HEAD_KEY, &height);
    }

    pub fn set_shard_head(&mut self, shard_id: ShardId, height: BlockHeight) {
        self.store_update.set_ser(DBCol::BlockMisc, &cloud_shard_head_key(shard_id), &height);
    }

    pub fn set_prev_epoch_end(&mut self, block_hash: CryptoHash) {
        self.store_update.set_ser(DBCol::BlockMisc, CLOUD_PREV_EPOCH_END_KEY, &block_hash);
    }
}
