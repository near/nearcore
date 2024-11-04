use super::types::{
    FlatStorageShardCatchupRequest, FlatStorageSplitShardRequest, MemtrieReloadRequest,
};
use crate::flat_storage_resharder::{FlatStorageResharder, FlatStorageReshardingTaskStatus};
use crate::ChainStore;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::{self, Handler, HandlerWithContext};
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_store::{ShardUId, Store};
use time::Duration;

/// Dedicated actor for resharding V3.
pub struct ReshardingActor {
    chain_store: ChainStore,
}

impl messaging::Actor for ReshardingActor {}

impl Handler<FlatStorageSplitShardRequest> for ReshardingActor {
    fn handle(&mut self, msg: FlatStorageSplitShardRequest) {
        match msg.resharder.split_shard_task() {
            FlatStorageReshardingTaskStatus::Successful { .. } => {
                // All good.
            }
            FlatStorageReshardingTaskStatus::Failed => {
                panic!("impossible to recover from a flat storage split shard failure!")
            }
            FlatStorageReshardingTaskStatus::Cancelled => {
                // The task has been cancelled. Nothing else to do.
            }
        }
    }
}

impl HandlerWithContext<FlatStorageShardCatchupRequest> for ReshardingActor {
    fn handle(
        &mut self,
        msg: FlatStorageShardCatchupRequest,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        // Shard catchup task is delayed and could get postponed several times. This must be
        // done to cover the scenario in which catchup is triggered so fast that the initial
        // state of the new flat storage is beyond the chain final tip.
        ctx.run_later(
            "ReshardingActor FlatStorageShardCatchup",
            Duration::milliseconds(100),
            move |act, _| {
                act.handle_flat_storage_shard_catchup(
                    msg.resharder,
                    msg.shard_uid,
                    msg.flat_head_block_hash,
                );
            },
        );
    }
}

impl Handler<MemtrieReloadRequest> for ReshardingActor {
    fn handle(&mut self, _msg: MemtrieReloadRequest) {
        // TODO
    }
}

impl ReshardingActor {
    pub fn new(store: Store, genesis_height: BlockHeight) -> Self {
        Self { chain_store: ChainStore::new(store, genesis_height, false) }
    }

    fn handle_flat_storage_shard_catchup(
        &self,
        resharder: FlatStorageResharder,
        shard_uid: ShardUId,
        flat_head_block_hash: CryptoHash,
    ) {
        match resharder.shard_catchup_task(shard_uid, flat_head_block_hash, &self.chain_store) {
            FlatStorageReshardingTaskStatus::Successful { .. } => {
                // All good.
            }
            FlatStorageReshardingTaskStatus::Failed => {
                panic!("impossible to recover from a flat storage shard catchup failure!")
            }
            FlatStorageReshardingTaskStatus::Cancelled => {
                // The task has been cancelled. Nothing else to do.
            }
        }
    }
}
