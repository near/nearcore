use super::types::{
    FlatStorageShardCatchupRequest, FlatStorageSplitShardRequest, MemtrieReloadRequest,
};
use crate::flat_storage_resharder::{FlatStorageResharder, FlatStorageReshardingTaskResult};
use crate::{ChainGenesis, ChainStore};
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::{self, Handler, HandlerWithContext};
use near_primitives::shard_layout::ShardUId;
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

impl HandlerWithContext<FlatStorageShardCatchupRequest> for ReshardingActor {
    fn handle(
        &mut self,
        msg: FlatStorageShardCatchupRequest,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        self.handle_flat_storage_catchup(msg.resharder, msg.shard_uid, ctx);
    }
}

impl Handler<MemtrieReloadRequest> for ReshardingActor {
    fn handle(&mut self, _msg: MemtrieReloadRequest) {
        // TODO(resharding): implement memtrie reloading. At this stage the flat storage for
        // `msg.shard_uid` should be ready. Construct a new memtrie in the background and replace
        // with hybrid memtrie with it. Afterwards, the hybrid memtrie can be deleted.
    }
}

impl ReshardingActor {
    pub fn new(store: Store, genesis: &ChainGenesis) -> Self {
        Self { chain_store: ChainStore::new(store, false, genesis.transaction_validity_period) }
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
            FlatStorageReshardingTaskResult::Successful { .. } => {
                // All good.
            }
            FlatStorageReshardingTaskResult::Failed => {
                panic!("impossible to recover from a flat storage split shard failure!")
            }
            FlatStorageReshardingTaskResult::Cancelled => {
                // The task has been cancelled. Nothing else to do.
            }
            FlatStorageReshardingTaskResult::Postponed => {
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

    fn handle_flat_storage_catchup(
        &self,
        resharder: FlatStorageResharder,
        shard_uid: ShardUId,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        match resharder.shard_catchup_task(shard_uid, &self.chain_store) {
            FlatStorageReshardingTaskResult::Successful { .. } => {
                // All good.
            }
            FlatStorageReshardingTaskResult::Failed => {
                panic!("impossible to recover from a flat storage shard catchup failure!")
            }
            FlatStorageReshardingTaskResult::Cancelled => {
                // The task has been cancelled. Nothing else to do.
            }
            FlatStorageReshardingTaskResult::Postponed => {
                // The task must be retried later.
                ctx.run_later(
                    "ReshardingActor FlatStorageCatchup",
                    Duration::milliseconds(1000),
                    move |act, ctx| {
                        act.handle_flat_storage_catchup(resharder, shard_uid, ctx);
                    },
                );
            }
        }
    }
}
