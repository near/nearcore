use crate::client_actions::SyncJobsSenderForClientMessage;
use crate::sync_jobs_actions::{SyncJobsActions, SyncJobsSenderForSyncJobsMessage};
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::futures::TestLoopFutureSpawner;

pub fn forward_sync_jobs_messages_from_client_to_sync_jobs_actions(
    future_spawner: TestLoopFutureSpawner,
) -> LoopEventHandler<SyncJobsActions, SyncJobsSenderForClientMessage> {
    LoopEventHandler::new_simple(move |msg, sync_jobs_actions: &mut SyncJobsActions| match msg {
        SyncJobsSenderForClientMessage::_apply_state_parts(msg) => {
            sync_jobs_actions.handle_apply_state_parts_request(msg);
        }
        SyncJobsSenderForClientMessage::_block_catch_up(msg) => {
            sync_jobs_actions.handle_block_catch_up_request(msg);
        }
        SyncJobsSenderForClientMessage::_resharding(msg) => {
            sync_jobs_actions.handle_resharding_request(msg, &future_spawner);
        }
    })
}

pub fn forward_sync_jobs_messages_from_sync_jobs_to_sync_jobs_actions(
    future_spawner: TestLoopFutureSpawner,
) -> LoopEventHandler<SyncJobsActions, SyncJobsSenderForSyncJobsMessage> {
    LoopEventHandler::new_simple(move |msg, sync_jobs_actions: &mut SyncJobsActions| match msg {
        SyncJobsSenderForSyncJobsMessage::_resharding_request(msg) => {
            sync_jobs_actions.handle_resharding_request(msg, &future_spawner);
        }
    })
}
