use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::futures::TestLoopFutureSpawner;

use crate::client_actions::SyncJobsSenderForClientMessage;
use crate::stateless_validation::state_witness_distribution_actions::StateWitnessDistributionActions;
use crate::sync_jobs_actions::SyncJobsActions;
use crate::DistributeChunkStateWitnessRequest;

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
        SyncJobsSenderForClientMessage::_load_memtrie(msg) => {
            sync_jobs_actions.handle_load_memtrie_request(msg);
        }
    })
}

pub fn forward_messages_from_client_to_state_witness_distribution_actor(
) -> LoopEventHandler<StateWitnessDistributionActions, DistributeChunkStateWitnessRequest> {
    LoopEventHandler::new_simple(
        |msg, state_witness_distribution_actions: &mut StateWitnessDistributionActions| {
            state_witness_distribution_actions
                .handle_distribute_chunk_state_witness_request(msg)
                .unwrap();
        },
    )
}
