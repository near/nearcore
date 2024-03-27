use crate::sync_jobs_actions::SyncJobsActions;
use near_async::futures::ActixFutureSpawner;
use near_chain::chain::{ApplyStatePartsRequest, BlockCatchUpRequest};
use near_chain::resharding::ReshardingRequest;
use near_o11y::{handler_debug_span, WithSpanContext};
use near_performance_metrics_macros::perf;

pub(crate) struct SyncJobsActor {
    pub(crate) actions: SyncJobsActions,
}

impl SyncJobsActor {
    pub(crate) const MAILBOX_CAPACITY: usize = 100;
}

impl actix::Actor for SyncJobsActor {
    type Context = actix::Context<Self>;
}

impl actix::Handler<WithSpanContext<ApplyStatePartsRequest>> for SyncJobsActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ApplyStatePartsRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        self.actions.handle_apply_state_parts_request(msg);
    }
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
        self.actions.handle_block_catch_up_request(msg);
    }
}

impl actix::Handler<WithSpanContext<ReshardingRequest>> for SyncJobsActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ReshardingRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, resharding_request) = handler_debug_span!(target: "resharding", msg);
        self.actions.handle_resharding_request(resharding_request, &ActixFutureSpawner);
    }
}
