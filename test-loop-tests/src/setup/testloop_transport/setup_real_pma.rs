//! Real-PMA-side helpers for `setup_client`.
//!
//! - `build_client_sender_for_network` constructs the 16-field
//!   `ClientSenderForNetwork`. Mirrors
//!   `chain/client/src/adapter.rs::client_sender_for_network`
//!   field-by-field — one mis-wire here = silent message drop in
//!   the real-PMA path.
//! - `pma_adapter_from` constructs the 4-field `PeerManagerAdapter`
//!   from a PMA's `TestLoopSender<A>`. Used by both mock and real
//!   branches; the typed late-bound at `setup.rs` binds the result
//!   via explicit `.bind(...)`.

use near_async::messaging::{Actor, HandlerWithContext, IntoAsyncSender, IntoSender};
use near_async::test_loop::sender::TestLoopSender;
use near_client::client_actor::ClientActor;
use near_client::{ChunkEndorsementHandlerActor, RpcHandlerActor, ViewClientActor};
use near_network::client::ClientSenderForNetwork;
use near_network::types::{
    PeerManagerAdapter, PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo,
    StateSyncEvent,
};

/// The 4 per-actor senders that cover all 16 fields of
/// `ClientSenderForNetwork`. `state_request_adapter` is a separate
/// network-state field and isn't included here.
#[allow(dead_code)]
pub(crate) struct ActorSenders {
    pub client_sender: TestLoopSender<ClientActor>,
    pub view_client_sender: TestLoopSender<ViewClientActor>,
    pub rpc_handler_sender: TestLoopSender<RpcHandlerActor>,
    pub chunk_endorsement_handler_sender: TestLoopSender<ChunkEndorsementHandlerActor>,
}

/// Build the 4-field `PeerManagerAdapter` from a PMA's
/// `TestLoopSender<A>`. Used by both mock and real branches;
/// the typed `network_adapter: LateBoundSender<PeerManagerAdapter>`
/// at `setup.rs:78` binds the result via explicit `.bind(...)`.
/// Single audit point for "are all 4 PeerManagerAdapter fields
/// wired?".
#[allow(dead_code)]
pub(crate) fn pma_adapter_from<A>(sender: TestLoopSender<A>) -> PeerManagerAdapter
where
    A: Actor
        + HandlerWithContext<PeerManagerMessageRequest>
        + HandlerWithContext<SetChainInfo>
        + HandlerWithContext<StateSyncEvent>
        + HandlerWithContext<PeerManagerMessageRequest, PeerManagerMessageResponse>
        + 'static,
{
    PeerManagerAdapter {
        async_request_sender: sender.clone().into_async_sender(),
        request_sender: sender.clone().into_sender(),
        set_chain_info_sender: sender.clone().into_sender(),
        state_sync_event_sender: sender.into_sender(),
    }
}

/// Build the 16-field `ClientSenderForNetwork` by mapping each field
/// to its destination testloop actor. Mirrors
/// `chain/client/src/adapter.rs:14`.
#[allow(dead_code)]
pub(crate) fn build_client_sender_for_network(s: ActorSenders) -> ClientSenderForNetwork {
    ClientSenderForNetwork {
        // ── ClientActor: block flow + epoch sync + optimistic block + state response
        block: s.client_sender.clone().into_async_sender(),
        block_headers: s.client_sender.clone().into_async_sender(),
        block_approval: s.client_sender.clone().into_async_sender(),
        network_info: s.client_sender.clone().into_async_sender(),
        state_response: s.client_sender.clone().into_async_sender(),
        epoch_sync_request: s.client_sender.clone().into_sender(),
        epoch_sync_response: s.client_sender.clone().into_sender(),
        optimistic_block_receiver: s.client_sender.into_sender(),
        // ── ViewClientActor: read paths + announce-account
        block_request: s.view_client_sender.clone().into_async_sender(),
        block_headers_request: s.view_client_sender.clone().into_async_sender(),
        tx_status_request: s.view_client_sender.clone().into_async_sender(),
        tx_status_response: s.view_client_sender.clone().into_async_sender(),
        announce_account: s.view_client_sender.clone().into_async_sender(),
        current_epoch_height_request: s.view_client_sender.into_async_sender(),
        // ── RpcHandlerActor: incoming transactions
        transaction: s.rpc_handler_sender.into_async_sender(),
        // ── ChunkEndorsementHandlerActor: chunk endorsements
        chunk_endorsement: s.chunk_endorsement_handler_sender.into_async_sender(),
    }
}
