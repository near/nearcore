use crate::client_actions::SyncJobsSenderForClientMessage;
use crate::sync_jobs_actor::SyncJobsActor;
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::futures::TestLoopDelayedActionRunner;

pub fn forward_messages_from_client_to_sync_jobs_actor(
    mut ctx: TestLoopDelayedActionRunner<SyncJobsActor>,
) -> LoopEventHandler<SyncJobsActor, SyncJobsSenderForClientMessage> {
    LoopEventHandler::new_simple(move |msg, sync_jobs_actor: &mut SyncJobsActor| match msg {
        SyncJobsSenderForClientMessage::_apply_state_parts(msg) => {
            sync_jobs_actor.handle(msg);
        }
        SyncJobsSenderForClientMessage::_block_catch_up(msg) => {
            sync_jobs_actor.handle(msg);
        }
        SyncJobsSenderForClientMessage::_resharding(msg) => {
            sync_jobs_actor.handle_resharding_request(msg, &mut ctx);
        }
        SyncJobsSenderForClientMessage::_load_memtrie(msg) => {
            sync_jobs_actor.handle(msg);
        }
    })
}
