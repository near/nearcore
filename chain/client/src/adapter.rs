use crate::chunk_endorsement_handler::ChunkEndorsementHandler;
use crate::client_actor::ClientActorInner;
use crate::{RpcHandler, ViewClientActorInner};
use near_async::messaging::{IntoAsyncSender, IntoSender};
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::tokio::TokioRuntimeHandle;
use near_network::client::ClientSenderForNetwork;

pub fn client_sender_for_network(
    client_addr: TokioRuntimeHandle<ClientActorInner>,
    view_client_addr: MultithreadRuntimeHandle<ViewClientActorInner>,
    rpc_handler: MultithreadRuntimeHandle<RpcHandler>,
    chunk_endorsement_handler: MultithreadRuntimeHandle<ChunkEndorsementHandler>,
) -> ClientSenderForNetwork {
    ClientSenderForNetwork {
        block: client_addr.clone().into_async_sender(),
        block_headers: client_addr.clone().into_async_sender(),
        block_approval: client_addr.clone().into_async_sender(),
        block_headers_request: view_client_addr.clone().into_async_sender(),
        block_request: view_client_addr.clone().into_async_sender(),
        network_info: client_addr.clone().into_async_sender(),
        state_response: client_addr.clone().into_async_sender(),
        tx_status_request: view_client_addr.clone().into_async_sender(),
        tx_status_response: view_client_addr.clone().into_async_sender(),
        transaction: rpc_handler.into_async_sender(),
        announce_account: view_client_addr.into_async_sender(),
        chunk_endorsement: chunk_endorsement_handler.into_async_sender(),
        epoch_sync_request: client_addr.clone().into_sender(),
        epoch_sync_response: client_addr.clone().into_sender(),
        optimistic_block_receiver: client_addr.into_sender(),
    }
}
