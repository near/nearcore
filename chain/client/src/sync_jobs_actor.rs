use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{self, CanSend, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::Error;
use near_chain::PendingShardJobs;
use near_chain::chain::{BlockCatchUpRequest, BlockCatchUpResponse};
use near_chain::update_shard::ShardUpdateResult;
use near_o11y::span_wrapped_msg::{SpanWrapped, SpanWrappedMessageExt};
use near_primitives::optimistic_block::CachedShardUpdateKey;
use near_primitives::types::ShardId;
use std::sync::Arc;

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
        let client_sender = self.client_sender.clone();
        let sync_hash = msg.sync_hash;
        let block_hash = msg.block_hash;
        let parent_span = tracing::Span::current();

        // Schedule the shard jobs on apply_chunks_spawner, using PendingShardJobs.
        // On completion, send the results back to the client.
        let on_done = move |results: Vec<(
            (ShardId, CachedShardUpdateKey),
            Result<ShardUpdateResult, Error>,
        )>| {
            let results =
                results.into_iter().map(|((shard_id, _), result)| (shard_id, result)).collect();
            client_sender.send(BlockCatchUpResponse { sync_hash, block_hash, results }.span_wrap());
        };
        let jobs = msg
            .work
            .into_iter()
            .map(|(shard_id, cached_shard_update_key, task)| {
                let parent_span = parent_span.clone();
                let boxed: Box<dyn FnOnce() -> _ + Send> = Box::new(move || task(&parent_span));
                ((shard_id, cached_shard_update_key), boxed)
            })
            .collect();
        PendingShardJobs::run("apply_chunks", self.apply_chunks_spawner.clone(), jobs, on_done);
    }
}
