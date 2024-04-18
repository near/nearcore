use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::futures::TestLoopFutureSpawner;
use near_chunks::client::ShardsManagerResponse;
use near_network::client::ClientSenderForNetworkMessage;
use near_network::state_witness::StateWitnessSenderForNetworkMessage;

use crate::client_actions::SyncJobsSenderForClientMessage;
use crate::client_actions::{ClientActionHandler, ClientActions, ClientSenderForClientMessage};
use crate::stateless_validation::state_witness_actions::StateWitnessActions;
use crate::stateless_validation::state_witness_actor::StateWitnessSenderForClientMessage;
use crate::sync_jobs_actions::ClientSenderForSyncJobsMessage;
use crate::sync_jobs_actions::SyncJobsActions;

pub fn forward_client_messages_from_network_to_client_actions(
) -> LoopEventHandler<ClientActions, ClientSenderForNetworkMessage> {
    LoopEventHandler::new(|msg, client_actions: &mut ClientActions| {
        match msg {
            ClientSenderForNetworkMessage::_state_response(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block_approval(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_transaction(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block_headers(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_challenge(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_network_info(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_chunk_state_witness(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_chunk_endorsement(msg) => {
                (msg.callback)(Ok(client_actions.handle(msg.message)));
            }
            _ => {
                return Err(msg);
            }
        }
        Ok(())
    })
}

pub fn forward_client_messages_from_client_to_client_actions(
) -> LoopEventHandler<ClientActions, ClientSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, client_actions: &mut ClientActions| match msg {
        ClientSenderForClientMessage::_apply_chunks_done(msg) => client_actions.handle(msg),
    })
}

pub fn forward_client_messages_from_sync_jobs_to_client_actions(
) -> LoopEventHandler<ClientActions, ClientSenderForSyncJobsMessage> {
    LoopEventHandler::new_simple(|msg, client_actions: &mut ClientActions| match msg {
        ClientSenderForSyncJobsMessage::_apply_state_parts_response(msg) => {
            client_actions.handle(msg)
        }
        ClientSenderForSyncJobsMessage::_block_catch_up_response(msg) => client_actions.handle(msg),
        ClientSenderForSyncJobsMessage::_resharding_response(msg) => client_actions.handle(msg),
        ClientSenderForSyncJobsMessage::_load_memtrie_response(msg) => client_actions.handle(msg),
    })
}

pub fn forward_client_messages_from_shards_manager(
) -> LoopEventHandler<ClientActions, ShardsManagerResponse> {
    LoopEventHandler::new_simple(|msg, client_actions: &mut ClientActions| {
        client_actions.handle(msg);
    })
}

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

pub fn forward_messages_from_network_to_state_witness_actor(
) -> LoopEventHandler<StateWitnessActions, StateWitnessSenderForNetworkMessage> {
    LoopEventHandler::new_simple(|msg, state_witness_actions: &mut StateWitnessActions| match msg {
        StateWitnessSenderForNetworkMessage::_chunk_state_witness_ack(msg) => {
            state_witness_actions.handle_chunk_state_witness_ack(msg.0);
        }
        StateWitnessSenderForNetworkMessage::_partial_encoded_state_witness(msg) => {
            state_witness_actions.handle_partial_encoded_state_witness(msg.0).unwrap();
        }
        StateWitnessSenderForNetworkMessage::_partial_encoded_state_witness_forward(msg) => {
            state_witness_actions.handle_partial_encoded_state_witness_forward(msg.0).unwrap();
        }
    })
}

pub fn forward_messages_from_client_to_state_witness_actor(
) -> LoopEventHandler<StateWitnessActions, StateWitnessSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, state_witness_actions: &mut StateWitnessActions| match msg {
        StateWitnessSenderForClientMessage::_distribute_chunk_state_witness(msg) => {
            state_witness_actions.handle_distribute_state_witness_request(msg).unwrap();
        }
    })
}
