use super::types::{
    FlatStorageShardCatchupRequest, FlatStorageSplitShardRequest, MemtrieReloadRequest,
};
use crate::flat_storage_resharder::{
    FlatStorageResharder, FlatStorageReshardingSchedulableTaskResult,
    FlatStorageReshardingTaskResult,
};
use crate::ChainStore;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::{self, Handler, HandlerWithContext};
use near_primitives::types::BlockHeight;
use near_store::Store;
use time::Duration;

/// Dedicated actor for resharding V3.
pub struct ReshardingActor {
    chain_store: ChainStore,
}

impl messaging::Actor for ReshardingActor {}

impl HandlerWithContext<FlatStorageSplitShardRequest> for ReshardingActor {
    fn handle(
        &mut self,
        msg: FlatStorageSplitShardRequest,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        self.handle_flat_storage_split_shard(msg.resharder, ctx);
    }
}

impl Handler<FlatStorageShardCatchupRequest> for ReshardingActor {
    fn handle(&mut self, msg: FlatStorageShardCatchupRequest) {
        match msg.resharder.shard_catchup_task(
            msg.shard_uid,
            msg.flat_head_block_hash,
            &self.chain_store,
        ) {
            FlatStorageReshardingTaskResult::Successful { .. } => {
                // All good.
            }
            FlatStorageReshardingTaskResult::Failed => {
                panic!("impossible to recover from a flat storage shard catchup failure!")
            }
        }
    }
}

impl Handler<MemtrieReloadRequest> for ReshardingActor {
    fn handle(&mut self, _msg: MemtrieReloadRequest) {
        // TODO(resharding)
    }
}

impl ReshardingActor {
    pub fn new(store: Store, genesis_height: BlockHeight) -> Self {
        Self { chain_store: ChainStore::new(store, genesis_height, false) }
    }

    fn handle_flat_storage_split_shard(
        &self,
        resharder: FlatStorageResharder,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        // In order to run to completion, the split task must wait until the resharding block
        // becomes final. If the resharding block is not yet final, the task will exit early with
        // `Postponed` status and it must be rescheduled.
        match resharder.split_shard_task(&self.chain_store) {
            FlatStorageReshardingSchedulableTaskResult::Successful { .. } => {
                // All good.
            }
            FlatStorageReshardingSchedulableTaskResult::Failed => {
                panic!("impossible to recover from a flat storage split shard failure!")
            }
            FlatStorageReshardingSchedulableTaskResult::Cancelled => {
                // The task has been cancelled. Nothing else to do.
            }
            FlatStorageReshardingSchedulableTaskResult::Postponed => {
                // The task must be retried later.
                ctx.run_later(
                    "ReshardingActor FlatStorageSplitShard",
                    Duration::milliseconds(1000),
                    move |act, ctx| {
                        act.handle_flat_storage_split_shard(resharder, ctx);
                    },
                );
            }
        }
    }
}
