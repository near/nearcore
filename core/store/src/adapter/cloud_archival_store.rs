use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};
use crate::db::{
    CLOUD_READER_HEAD_KEY, CLOUD_WRITER_BLOCK_HEAD_KEY, CLOUD_WRITER_MIN_HEAD_KEY,
    CLOUD_WRITER_PREV_EPOCH_END_KEY, CLOUD_WRITER_SHARD_HEAD_PREFIX, cloud_writer_shard_head_key,
    cloud_writer_shard_head_key_shard_id,
};
use crate::{DBCol, Store, StoreUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

/// How far a reader has taken the archive, and the block it continues from.
#[derive(Clone, Copy, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct CloudReaderHead {
    /// Height every component is written through. Can name a height carrying no block.
    pub height: BlockHeight,
    /// Nearest present block at or below `height`.
    pub last_present_block_hash: CryptoHash,
}

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
    pub fn writer_min_head(&self) -> Option<BlockHeight> {
        self.store.get_ser(DBCol::BlockMisc, CLOUD_WRITER_MIN_HEAD_KEY)
    }

    /// Height up to which block data has reached the bucket.
    pub fn writer_block_head(&self) -> Option<BlockHeight> {
        self.store.get_ser(DBCol::BlockMisc, CLOUD_WRITER_BLOCK_HEAD_KEY)
    }

    /// Height up to which one shard's data has reached the bucket.
    pub fn writer_shard_head(&self, shard_id: ShardId) -> Option<BlockHeight> {
        self.store.get_ser(DBCol::BlockMisc, &cloud_writer_shard_head_key(shard_id))
    }

    /// Last block of the latest fully archivized epoch.
    pub fn writer_prev_epoch_end(&self) -> Option<CryptoHash> {
        self.store.get_ser(DBCol::BlockMisc, CLOUD_WRITER_PREV_EPOCH_END_KEY)
    }

    /// How far a reader has written every component, absent unless this store is a
    /// reader's.
    pub fn reader_head(&self) -> Option<CloudReaderHead> {
        self.store.get_ser(DBCol::BlockMisc, CLOUD_READER_HEAD_KEY)
    }

    /// Every shard the node has recorded a head for, in shard order.
    pub fn all_writer_shard_heads(&self) -> Vec<(ShardId, BlockHeight)> {
        let mut heads = Vec::new();
        for (key, value) in self.store.iter_prefix(DBCol::BlockMisc, CLOUD_WRITER_SHARD_HEAD_PREFIX)
        {
            let Some(shard_id) = cloud_writer_shard_head_key_shard_id(&key) else {
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

    pub fn set_writer_min_head(&mut self, height: BlockHeight) {
        self.store_update.set_ser(DBCol::BlockMisc, CLOUD_WRITER_MIN_HEAD_KEY, &height);
    }

    pub fn set_writer_block_head(&mut self, height: BlockHeight) {
        self.store_update.set_ser(DBCol::BlockMisc, CLOUD_WRITER_BLOCK_HEAD_KEY, &height);
    }

    pub fn set_writer_shard_head(&mut self, shard_id: ShardId, height: BlockHeight) {
        self.store_update.set_ser(
            DBCol::BlockMisc,
            &cloud_writer_shard_head_key(shard_id),
            &height,
        );
    }

    pub fn set_writer_prev_epoch_end(&mut self, block_hash: CryptoHash) {
        self.store_update.set_ser(DBCol::BlockMisc, CLOUD_WRITER_PREV_EPOCH_END_KEY, &block_hash);
    }

    pub fn set_reader_head(&mut self, head: &CloudReaderHead) {
        self.store_update.set_ser(DBCol::BlockMisc, CLOUD_READER_HEAD_KEY, head);
    }
}
