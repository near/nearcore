use crate::client_actions::SyncJobsSenderForClientMessage;
use crate::sync_jobs_actor::{SyncJobsActor, SyncJobsSenderForSyncJobsMessage};
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::futures::TestLoopFutureSpawner;

pub fn forward_messages_from_client_to_sync_jobs_actor(
    future_spawner: TestLoopFutureSpawner,
) -> LoopEventHandler<SyncJobsActor, SyncJobsSenderForClientMessage> {
    LoopEventHandler::new_simple(move |msg, sync_jobs_actor: &mut SyncJobsActor| match msg {
        SyncJobsSenderForClientMessage::_apply_state_parts(msg) => {
            sync_jobs_actor.handle(msg);
        }
        SyncJobsSenderForClientMessage::_block_catch_up(msg) => {
            sync_jobs_actor.handle(msg);
        }
        SyncJobsSenderForClientMessage::_resharding(msg) => {
            sync_jobs_actor.handle_resharding_request(msg, &future_spawner);
        }
        SyncJobsSenderForClientMessage::_load_memtrie(msg) => {
            sync_jobs_actor.handle(msg);
        }
    })
}

pub fn forward_messages_from_sync_jobs_to_sync_jobs_actor(
    future_spawner: TestLoopFutureSpawner,
) -> LoopEventHandler<SyncJobsActor, SyncJobsSenderForSyncJobsMessage> {
    LoopEventHandler::new_simple(move |msg, sync_jobs_actor: &mut SyncJobsActor| match msg {
        SyncJobsSenderForSyncJobsMessage::_resharding_request(msg) => {
            sync_jobs_actor.handle_resharding_request(msg, &future_spawner);
        }
    })
}
