use crate::ClientActor;
use actix::AsyncContext;

use near_chain::chain::{do_apply_chunks, BlockCatchUpRequest, BlockCatchUpResponse};
use near_chain::resharding::StateSplitRequest;
use near_chain::Chain;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_performance_metrics_macros::perf;
use std::time::Duration;

const RESHARDING_RETRY_TIME: Duration = Duration::from_secs(30);

pub(crate) struct SyncJobsActor {
    pub(crate) client_addr: actix::Addr<ClientActor>,
}

pub(crate) fn create_sync_job_scheduler<M>(address: actix::Addr<SyncJobsActor>) -> Box<dyn Fn(M)>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    SyncJobsActor: actix::Handler<WithSpanContext<M>>,
{
    Box::new(move |msg: M| {
        if let Err(err) = address.try_send(msg.with_span_context()) {
            match err {
                actix::dev::SendError::Full(request) => {
                    address.do_send(request);
                }
                actix::dev::SendError::Closed(_) => {
                    tracing::error!("Can't send message to SyncJobsActor, mailbox is closed");
                }
            }
        }
    })
}

impl SyncJobsActor {
    pub(crate) const MAILBOX_CAPACITY: usize = 100;
}

impl actix::Actor for SyncJobsActor {
    type Context = actix::Context<Self>;
}

impl actix::Handler<WithSpanContext<BlockCatchUpRequest>> for SyncJobsActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<BlockCatchUpRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        tracing::debug!(target: "client", ?msg);
        let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);

        self.client_addr.do_send(
            BlockCatchUpResponse { sync_hash: msg.sync_hash, block_hash: msg.block_hash, results }
                .with_span_context(),
        );
    }
}

impl actix::Handler<WithSpanContext<StateSplitRequest>> for SyncJobsActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<StateSplitRequest>,
        context: &mut Self::Context,
    ) -> Self::Result {
        let (_span, mut state_split_request) = handler_debug_span!(target: "client", msg);
        if Chain::retry_build_state_for_split_shards(&state_split_request) {
            // Actix implementation let's us send message to ourselves with a delay.
            // In case snapshots are not ready yet, we will retry resharding later.
            tracing::debug!(target: "client", ?state_split_request, "Snapshot missing, retrying resharding later");
            state_split_request.curr_poll_time += RESHARDING_RETRY_TIME;
            context.notify_later(state_split_request.with_span_context(), RESHARDING_RETRY_TIME);
        } else {
            tracing::debug!(target: "client", ?state_split_request, "Starting resharding");
            let response = Chain::build_state_for_split_shards(state_split_request);
            self.client_addr.do_send(response.with_span_context());
        }
    }
}
