use near_o11y::WithSpanContext;
use near_store::StoreUpdate;

/// Runs IO tasks that would block a normal arbiter for too long.
///
/// This actor should be running in a SyncArbiter, which means there can be
/// multiple instances of the actor in parallel, each with its own OS thread.
/// Compared to running in a normal arbiter, this means blocking IO requests
/// don't stall other jobs.
pub struct BlockingIoActor;

#[derive(actix::Message)]
#[rtype(result = "std::io::Result<()>")]
pub(crate) enum BlockingIoActorMessage {
    /// Take a prepared DB transaction and persist it.
    CommitStoreUpdate(StoreUpdate),
}

impl actix::Actor for BlockingIoActor {
    type Context = actix::SyncContext<Self>;
}

impl actix::Handler<WithSpanContext<BlockingIoActorMessage>> for BlockingIoActor {
    type Result = std::io::Result<()>;

    fn handle(
        &mut self,
        msg: WithSpanContext<BlockingIoActorMessage>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        // For now there is only one message type.
        let BlockingIoActorMessage::CommitStoreUpdate(update) = msg.msg;
        let _span = tracing::debug_span!(target: "client", "commit_store_update");
        update.commit()
    }
}
