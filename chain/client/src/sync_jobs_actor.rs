use std::sync::Arc;

use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{self, CanSend, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::chain::{
    BlockCatchUpRequest, BlockCatchUpResponse, do_apply_chunks_and_process_results,
};
use near_chain::update_shard::ShardUpdateResult;
use near_chain_primitives::error::Error;
use near_o11y::span_wrapped_msg::{SpanWrapped, SpanWrappedMessageExt};
use near_performance_metrics_macros::perf;
use near_primitives::optimistic_block::{BlockToApply, CachedShardUpdateKey};
use near_primitives::types::ShardId;

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForSyncJobs {
    block_catch_up_response: Sender<SpanWrapped<BlockCatchUpResponse>>,
}

pub struct SyncJobsActor {
    client_sender: ClientSenderForSyncJobs,
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
}

impl messaging::Actor for SyncJobsActor {}

impl Handler<BlockCatchUpRequest> for SyncJobsActor {
    #[perf]
    fn handle(&mut self, msg: BlockCatchUpRequest) {
        self.handle_block_catch_up_request(msg);
    }
}

impl SyncJobsActor {
    pub fn new(
        client_sender: ClientSenderForSyncJobs,
        apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    ) -> Self {
        Self { client_sender, apply_chunks_spawner }
    }

    pub fn handle_block_catch_up_request(&mut self, msg: BlockCatchUpRequest) {
        tracing::debug!(target: "sync", ?msg);
        let sync_hash = msg.sync_hash;
        let block_hash = msg.block_hash;
        let client_sender = self.client_sender.clone();
        let process_results =
            move |res: Vec<(ShardId, CachedShardUpdateKey, Result<ShardUpdateResult, Error>)>| {
                let results =
                    res.into_iter().map(|(shard_id, _, result)| (shard_id, result)).collect();
                client_sender
                    .send(BlockCatchUpResponse { sync_hash, block_hash, results }.span_wrap());
            };
        do_apply_chunks_and_process_results(
            self.apply_chunks_spawner.clone(),
            BlockToApply::Normal(msg.block_hash),
            msg.block_height,
            msg.work,
            process_results,
        );
    }
}
