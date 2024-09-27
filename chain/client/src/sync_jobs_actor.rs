use actix::Actor;
use near_async::actix_wrapper::ActixWrapper;
use near_async::messaging::{self, CanSend, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::chain::{do_apply_chunks, BlockCatchUpRequest, BlockCatchUpResponse};
use near_performance_metrics_macros::perf;

// Set the mailbox capacity for the SyncJobsActor from default 16 to 100.
const MAILBOX_CAPACITY: usize = 100;

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForSyncJobs {
    block_catch_up_response: Sender<BlockCatchUpResponse>,
}

pub struct SyncJobsActor {
    client_sender: ClientSenderForSyncJobs,
}

impl messaging::Actor for SyncJobsActor {}

impl Handler<BlockCatchUpRequest> for SyncJobsActor {
    #[perf]
    fn handle(&mut self, msg: BlockCatchUpRequest) {
        self.handle_block_catch_up_request(msg);
    }
}

impl SyncJobsActor {
    pub fn new(client_sender: ClientSenderForSyncJobs) -> Self {
        Self { client_sender }
    }

    pub fn spawn_actix_actor(self) -> actix::Addr<ActixWrapper<Self>> {
        let actix_wrapper = ActixWrapper::new(self);
        let arbiter = actix::Arbiter::new().handle();
        let addr = ActixWrapper::<Self>::start_in_arbiter(&arbiter, |ctx| {
            ctx.set_mailbox_capacity(MAILBOX_CAPACITY);
            actix_wrapper
        });
        addr
    }

    pub fn handle_block_catch_up_request(&mut self, msg: BlockCatchUpRequest) {
        tracing::debug!(target: "sync", ?msg);
        let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);

        self.client_sender.send(BlockCatchUpResponse {
            sync_hash: msg.sync_hash,
            block_hash: msg.block_hash,
            results,
        });
    }
}
