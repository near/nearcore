use super::types::FlatStorageSplitShardRequest;
use crate::flat_storage_resharder::split_shard_task;
use near_async::messaging::{self, Handler};

/// Dedicated actor for resharding V3.
pub struct ReshardingActor {}

impl messaging::Actor for ReshardingActor {}

impl Handler<FlatStorageSplitShardRequest> for ReshardingActor {
    fn handle(&mut self, msg: FlatStorageSplitShardRequest) {
        self.handle_flat_storage_split_shard_request(msg);
    }
}

impl ReshardingActor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn handle_flat_storage_split_shard_request(&mut self, msg: FlatStorageSplitShardRequest) {
        split_shard_task(msg.resharder);
    }
}
