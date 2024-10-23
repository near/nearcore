use super::types::ReshardingRequest;
use near_async::messaging::{self, Handler};

/// Dedicated actor for resharding V3.
pub struct ReshardingActor {}

impl messaging::Actor for ReshardingActor {}

impl Handler<ReshardingRequest> for ReshardingActor {
    fn handle(&mut self, msg: ReshardingRequest) {
        match msg {
            ReshardingRequest::FlatStorageSplitShard { resharder } => {
                resharder.split_shard_task();
            }
            ReshardingRequest::FlatStorageShardCatchup {
                resharder,
                shard,
                flat_head_block_hash,
            } => {
                resharder.shard_catchup_task(shard, flat_head_block_hash);
            }
            ReshardingRequest::MemtrieReload { shard } => self.handle_memtrie_reload(shard),
        }
    }
}

impl ReshardingActor {
    pub fn new() -> Self {
        Self {}
    }

    fn handle_memtrie_reload(&self, _shard: near_store::ShardUId) {
        // TODO(resharding)
    }
}
