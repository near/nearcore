use super::types::ReshardingRequest;
use crate::flat_storage_resharder::{FlatStorageResharder, FlatStorageReshardingTaskStatus};
use crate::ChainStore;
use near_async::messaging::{self, Handler};
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_store::{ShardUId, Store};

/// Dedicated actor for resharding V3.
pub struct ReshardingActor {
    chain_store: ChainStore,
}

impl messaging::Actor for ReshardingActor {}

impl Handler<ReshardingRequest> for ReshardingActor {
    fn handle(&mut self, msg: ReshardingRequest) {
        match msg {
            ReshardingRequest::FlatStorageSplitShard { resharder } => {
                self.handle_flat_storage_split_shard(resharder);
            }
            ReshardingRequest::FlatStorageShardCatchup {
                resharder,
                shard_uid,
                flat_head_block_hash,
            } => {
                self.handle_flat_storage_shard_catchup(resharder, shard_uid, flat_head_block_hash);
            }
            ReshardingRequest::MemtrieReload { shard_uid } => self.handle_memtrie_reload(shard_uid),
        }
    }
}

impl ReshardingActor {
    pub fn new(store: Store, genesis_height: BlockHeight) -> Self {
        Self { chain_store: ChainStore::new(store, genesis_height, false) }
    }

    fn handle_memtrie_reload(&self, _shard_uid: ShardUId) {
        // TODO(resharding)
    }

    fn handle_flat_storage_split_shard(&self, resharder: FlatStorageResharder) {
        if matches!(resharder.split_shard_task(), FlatStorageReshardingTaskStatus::Failed) {
            panic!("impossible to recover from a flat storage resharding failure");
        }
    }

    fn handle_flat_storage_shard_catchup(
        &self,
        resharder: FlatStorageResharder,
        shard_uid: ShardUId,
        flat_head_block_hash: CryptoHash,
    ) {
        if matches!(
            resharder.shard_catchup_task(shard_uid, flat_head_block_hash, &self.chain_store),
            FlatStorageReshardingTaskStatus::Failed
        ) {
            panic!("impossible to recover from a flat storage resharding failure");
        }
    }
}
