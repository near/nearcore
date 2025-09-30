use near_async::messaging::{self, CanSend, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::ApplyChunksIterationMode;
use near_chain::chain::{BlockCatchUpRequest, BlockCatchUpResponse, do_apply_chunks};
use near_o11y::span_wrapped_msg::{SpanWrapped, SpanWrappedMessageExt};
use near_performance_metrics_macros::perf;
use near_primitives::optimistic_block::BlockToApply;

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForSyncJobs {
    block_catch_up_response: Sender<SpanWrapped<BlockCatchUpResponse>>,
}

pub struct SyncJobsActor {
    client_sender: ClientSenderForSyncJobs,
    apply_chunks_iteration_mode: ApplyChunksIterationMode,
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
        apply_chunks_iteration_mode: ApplyChunksIterationMode,
    ) -> Self {
        Self { client_sender, apply_chunks_iteration_mode }
    }

    pub fn handle_block_catch_up_request(&mut self, msg: BlockCatchUpRequest) {
        tracing::debug!(target: "sync", ?msg);
        let results = do_apply_chunks(
            self.apply_chunks_iteration_mode,
            BlockToApply::Normal(msg.block_hash),
            msg.block_height,
            msg.work,
        )
        .into_iter()
        .map(|res| (res.0, res.2))
        .collect();

        self.client_sender.send(
            BlockCatchUpResponse { sync_hash: msg.sync_hash, block_hash: msg.block_hash, results }
                .span_wrap(),
        );
    }
}
