use crate::client_actor::{ClientActorInner, ClientSenderForClientMessage};
use crate::sync_jobs_actor::ClientSenderForSyncJobsMessage;
use crate::SyncMessage;
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_chunks::client::ShardsManagerResponse;
use near_network::client::ClientSenderForNetworkMessage;

pub fn forward_client_messages_from_network_to_client_actor(
) -> LoopEventHandler<ClientActorInner, ClientSenderForNetworkMessage> {
    LoopEventHandler::new(|msg, client_actor: &mut ClientActorInner| {
        match msg {
            ClientSenderForNetworkMessage::_state_response(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block_approval(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_transaction(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block_headers(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_challenge(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_network_info(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_chunk_state_witness(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_chunk_endorsement(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            _ => {
                return Err(msg);
            }
        }
        Ok(())
    })
}

pub fn forward_client_messages_from_client_to_client_actor(
) -> LoopEventHandler<ClientActorInner, ClientSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| match msg {
        ClientSenderForClientMessage::_apply_chunks_done(msg) => client_actor.handle(msg),
    })
}

pub fn forward_client_messages_from_sync_jobs_to_client_actor(
) -> LoopEventHandler<ClientActorInner, ClientSenderForSyncJobsMessage> {
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| match msg {
        ClientSenderForSyncJobsMessage::_apply_state_parts_response(msg) => {
            client_actor.handle(msg)
        }
        ClientSenderForSyncJobsMessage::_block_catch_up_response(msg) => client_actor.handle(msg),
        ClientSenderForSyncJobsMessage::_resharding_response(msg) => client_actor.handle(msg),
        ClientSenderForSyncJobsMessage::_load_memtrie_response(msg) => client_actor.handle(msg),
    })
}

pub fn forward_client_messages_from_shards_manager(
) -> LoopEventHandler<ClientActorInner, ShardsManagerResponse> {
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| {
        client_actor.handle(msg);
    })
}

pub fn forward_client_messages_from_sync_adapter() -> LoopEventHandler<ClientActorInner, SyncMessage>
{
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| {
        client_actor.handle(msg);
    })
}
