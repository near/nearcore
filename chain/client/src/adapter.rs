use crate::client_actor::ClientActor;
use crate::{RpcHandlerActor, ViewClientActor};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::messaging::IntoSender;
use near_network::client::ClientSenderForNetwork;

pub fn client_sender_for_network(
    client_addr: actix::Addr<ClientActor>,
    view_client_addr: actix::Addr<ViewClientActor>,
    rpc_handler: actix::Addr<RpcHandlerActor>,
) -> ClientSenderForNetwork {
    let client_addr = client_addr.with_auto_span_context();
    let view_client_addr = view_client_addr.with_auto_span_context();
    let rpc_handler = rpc_handler.with_auto_span_context();
    ClientSenderForNetwork {
        block: client_addr.clone().into_sender(),
        block_headers: client_addr.clone().into_sender(),
        block_approval: client_addr.clone().into_sender(),
        block_headers_request: view_client_addr.clone().into_sender(),
        block_request: view_client_addr.clone().into_sender(),
        network_info: client_addr.clone().into_sender(),
        state_request_header: view_client_addr.clone().into_sender(),
        state_request_part: view_client_addr.clone().into_sender(),
        state_response: client_addr.clone().into_sender(),
        tx_status_request: view_client_addr.clone().into_sender(),
        tx_status_response: view_client_addr.clone().into_sender(),
        transaction: rpc_handler.clone().into_sender(),
        announce_account: view_client_addr.into_sender(),
        chunk_endorsement: rpc_handler.into_sender(),
        epoch_sync_request: client_addr.clone().into_sender(),
        epoch_sync_response: client_addr.clone().into_sender(),
        optimistic_block_receiver: client_addr.into_sender(),
    }
}
