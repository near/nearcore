use crate::client_actions::{ClientActionHandler, ClientActions, ClientSenderForClientMessage};
use crate::sync_jobs_actions::ClientSenderForSyncJobsMessage;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_chunks::client::ShardsManagerResponse;
use near_network::client::ClientSenderForNetworkMessage;

pub fn forward_client_messages_from_network_to_client_actions(
) -> LoopEventHandler<ClientActions, ClientSenderForNetworkMessage> {
    LoopEventHandler::new(|msg, client_actions: &mut ClientActions, _| {
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
    })
}

pub fn forward_client_messages_from_shards_manager(
) -> LoopEventHandler<ClientActions, ShardsManagerResponse> {
    LoopEventHandler::new_simple(|msg, client_actions: &mut ClientActions| {
        client_actions.handle(msg);
    })
}
