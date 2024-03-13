use crate::client_actions::{ClientActionHandler, ClientActions, ClientSenderForClientMessage};
use crate::sync_jobs_actions::ClientSenderForSyncJobsMessage;
use crate::SyncMessage;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_chunks::client::ShardsManagerResponse;
use near_network::client::ClientSenderForNetworkMessage;

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
            ClientSenderForNetworkMessage::_chunk_state_witness_ack(msg) => {
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

pub fn forward_client_messages_from_sync_adapter() -> LoopEventHandler<ClientActions, SyncMessage> {
    LoopEventHandler::new_simple(|msg, client_actions: &mut ClientActions| {
        client_actions.handle(msg);
    })
}

pub fn print_basic_client_info_before_each_event<Data, Event>(
    idx: Option<usize>,
) -> LoopEventHandler<Data, Event>
where
    Data: AsRef<ClientActions>,
{
    let idx_prefix = idx.map(|idx| format!("[Client #{}] ", idx)).unwrap_or_default();

    LoopEventHandler::new(move |msg, data: &mut Data| {
        let client = &data.as_ref().client;
        tracing::info!("{}sync_status: {:?}", idx_prefix, client.sync_status);
        let head = client.chain.head().unwrap();
        tracing::info!("{}Chain HEAD: {:?}", idx_prefix, head);

        if let Some(signer) = client.validator_signer.as_ref() {
            let account_id = signer.validator_id();

            let mut tracked_shards = Vec::new();
            let mut next_tracked_shards = Vec::new();
            let epoch_manager = client.epoch_manager.as_ref();
            for shard_id in &epoch_manager.shard_ids(&head.epoch_id).unwrap() {
                let tracks_shard = client
                    .epoch_manager
                    .cares_about_shard_from_prev_block(&head.prev_block_hash, account_id, *shard_id)
                    .unwrap();
                if tracks_shard {
                    tracked_shards.push(*shard_id);
                }
                let next_tracks_shard = client
                    .epoch_manager
                    .cares_about_shard_next_epoch_from_prev_block(
                        &head.prev_block_hash,
                        account_id,
                        *shard_id,
                    )
                    .unwrap();
                if next_tracks_shard {
                    next_tracked_shards.push(*shard_id);
                }
            }
            tracing::info!(
                "{}Validator assigned shards: this epoch = {:?}; next epoch = {:?}",
                idx_prefix,
                tracked_shards,
                next_tracked_shards
            );

            tracing::info!("{}Tx pool: {}", idx_prefix, client.sharded_tx_pool.debug_status());
        }
        Err(msg)
    })
}
