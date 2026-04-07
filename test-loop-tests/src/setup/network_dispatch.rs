//! Dispatch function that converts incoming `PeerMessage`s into actor sends
//! on the receiving testloop node. This mirrors what `PeerActor::receive_message`
//! and `NetworkState::receive_routed_message` do in production, but using
//! synchronous testloop senders instead of async network channels.

use std::sync::Arc;

use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{CanSend, CanSendAsync};
use near_client::{BlockApproval, BlockResponse};
use near_network::client::{
    BlockHeadersRequest, BlockHeadersResponse, BlockRequest, ChunkEndorsementMessage,
    EpochSyncRequestMessage, EpochSyncResponseMessage, OptimisticBlockMessage, ProcessTxRequest,
    SpiceChunkEndorsementMessage,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::spice_data_distribution::{
    SpiceChunkContractAccessesMessage, SpiceContractCodeRequestMessage,
    SpiceContractCodeResponseMessage, SpiceIncomingPartialData,
};
use near_network::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
};
use near_network::types::{
    NetworkTransport, PeerMessage, T1MessageBody, T2MessageBody, TieredMessageBody,
};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::network::PeerId;

use near_network::types::PeerInfo;

use near_network::TestLoopNetworkBlockInfo;

use super::peer_manager_actor::{
    ClientSenderForTestLoopNetwork, OneClientSenders, TestLoopNetworkSharedState,
    ViewClientSenderForTestLoopNetwork,
};

/// Dispatches an incoming `PeerMessage` to the appropriate actor sender on the
/// receiving node. This is the testloop equivalent of the production receive path
/// (`PeerActor::receive_message` for non-routed messages and
/// `NetworkState::receive_routed_message` for routed messages).
///
/// Most dispatches are fire-and-forget. For request/response patterns like
/// `BlockRequest` and `BlockHeadersRequest`, this function spawns an async task
/// to await the view_client response and route it back to the requesting node.
pub(crate) fn dispatch_peer_message(
    from_peer: PeerId,
    msg: PeerMessage,
    senders: &OneClientSenders,
    future_spawner: &dyn FutureSpawner,
    requester_client_sender: &ClientSenderForTestLoopNetwork,
    target_view_client_sender: &ViewClientSenderForTestLoopNetwork,
) {
    match msg {
        // --- Non-routed messages ---
        PeerMessage::Block(block) => {
            // Track block headers per peer so push_network_info can report
            // highest_height_peers. This is needed by sync tests.
            senders.peer_manager_sender.send(TestLoopNetworkBlockInfo {
                peer: PeerInfo { id: from_peer.clone(), addr: None, account_id: None },
                block_header: block.header().clone(),
            });
            let future = senders.client_sender.send_async(
                BlockResponse { block, peer_id: from_peer, was_requested: false }.span_wrap(),
            );
            drop(future);
        }
        PeerMessage::BlockHeaders(headers) => {
            let future = senders
                .client_sender
                .send_async(BlockHeadersResponse(headers, from_peer).span_wrap());
            drop(future);
        }
        PeerMessage::BlockRequest(hash) => {
            // Send request to the target's view_client, spawn a task to await
            // the response and route it back to the requesting node's client.
            let response_future = target_view_client_sender.send_async(BlockRequest(hash));
            let responder = requester_client_sender.clone();
            let peer_id = from_peer;
            future_spawner.spawn("dispatch: route BlockResponse back to requester", async move {
                let Ok(Some(block)) = response_future.await else {
                    // The peer may have GC'd this block. Mimic production
                    // behavior: the requester simply doesn't get a response.
                    return;
                };
                let future = responder
                    .send_async(BlockResponse { block, peer_id, was_requested: true }.span_wrap());
                drop(future);
            });
        }
        PeerMessage::BlockHeadersRequest(hashes) => {
            // Send request to the target's view_client, spawn a task to await
            // the response and route it back to the requesting node's client.
            let response_future = target_view_client_sender.send_async(BlockHeadersRequest(hashes));
            let responder = requester_client_sender.clone();
            let peer_id = from_peer;
            future_spawner.spawn(
                "dispatch: route BlockHeadersResponse back to requester",
                async move {
                    let Ok(Some(headers)) = response_future.await else {
                        return;
                    };
                    let future =
                        responder.send_async(BlockHeadersResponse(headers, peer_id).span_wrap());
                    drop(future);
                },
            );
        }
        PeerMessage::Transaction(transaction) => {
            let future = senders.rpc_handler_sender.send_async(ProcessTxRequest {
                transaction,
                is_forwarded: false,
                check_only: false,
            });
            drop(future);
        }
        PeerMessage::EpochSyncRequest => {
            senders.client_sender.send(EpochSyncRequestMessage { from_peer });
        }
        PeerMessage::EpochSyncResponse(proof) => {
            senders.client_sender.send(EpochSyncResponseMessage { from_peer, proof });
        }
        PeerMessage::OptimisticBlock(ob) => {
            senders
                .client_sender
                .send(OptimisticBlockMessage { from_peer, optimistic_block: ob }.span_wrap());
        }

        // Protocol-level messages that don't need dispatch in testloop.
        PeerMessage::SyncSnapshotHosts(_)
        | PeerMessage::Challenge(_)
        | PeerMessage::Tier1Handshake(_)
        | PeerMessage::Tier2Handshake(_)
        | PeerMessage::Tier3Handshake(_)
        | PeerMessage::HandshakeFailure(_, _)
        | PeerMessage::LastEdge(_)
        | PeerMessage::SyncRoutingTable(_)
        | PeerMessage::RequestUpdateNonce(_)
        | PeerMessage::SyncAccountsData(_)
        | PeerMessage::PeersRequest(_)
        | PeerMessage::PeersResponse(_)
        | PeerMessage::Disconnect(_)
        | PeerMessage::StateRequestHeader(_, _)
        | PeerMessage::StateRequestPart(_, _, _)
        | PeerMessage::VersionedStateResponse(_) => {
            tracing::warn!(
                target: "test_loop",
                "unhandled non-routed PeerMessage variant in testloop dispatch"
            );
        }

        // --- Routed messages ---
        PeerMessage::Routed(routed_msg) => {
            let msg_hash = routed_msg.hash();
            let body = routed_msg.body_owned();
            dispatch_routed_message(from_peer, msg_hash, body, senders);
        }
    }
}

/// Dispatches a routed message body (T1 or T2) to the appropriate actor sender.
fn dispatch_routed_message(
    from_peer: PeerId,
    msg_hash: near_primitives::hash::CryptoHash,
    body: TieredMessageBody,
    senders: &OneClientSenders,
) {
    match body {
        TieredMessageBody::T1(body) => dispatch_t1(*body, from_peer, senders),
        TieredMessageBody::T2(body) => dispatch_t2(*body, from_peer, msg_hash, senders),
    }
}

/// Dispatches a T1 routed message body.
fn dispatch_t1(body: T1MessageBody, from_peer: PeerId, senders: &OneClientSenders) {
    match body {
        T1MessageBody::BlockApproval(approval) => {
            let future =
                senders.client_sender.send_async(BlockApproval(approval, from_peer).span_wrap());
            drop(future);
        }
        T1MessageBody::VersionedPartialEncodedChunk(chunk) => {
            senders
                .shards_manager_sender
                .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(*chunk));
        }
        T1MessageBody::PartialEncodedChunkForward(forward) => {
            senders
                .shards_manager_sender
                .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(forward));
        }
        T1MessageBody::PartialEncodedStateWitness(witness) => {
            senders.partial_witness_sender.send(PartialEncodedStateWitnessMessage(witness));
        }
        T1MessageBody::PartialEncodedStateWitnessForward(witness) => {
            senders.partial_witness_sender.send(PartialEncodedStateWitnessForwardMessage(witness));
        }
        T1MessageBody::VersionedChunkEndorsement(endorsement) => {
            let future = senders
                .chunk_endorsement_handler_sender
                .send_async(ChunkEndorsementMessage(endorsement));
            drop(future);
        }
        T1MessageBody::ChunkContractAccesses(accesses) => {
            senders.partial_witness_sender.send(ChunkContractAccessesMessage(accesses));
        }
        T1MessageBody::ContractCodeRequest(request) => {
            senders.partial_witness_sender.send(ContractCodeRequestMessage(request));
        }
        T1MessageBody::ContractCodeResponse(response) => {
            senders.partial_witness_sender.send(ContractCodeResponseMessage(response));
        }
        // Spice variants
        T1MessageBody::SpicePartialData(data) => {
            senders.spice_data_distributor_actor.send(SpiceIncomingPartialData { data });
        }
        T1MessageBody::SpiceChunkEndorsement(endorsement) => {
            senders.spice_core_writer_sender.send(SpiceChunkEndorsementMessage(endorsement));
        }
        T1MessageBody::SpicePartialDataRequest(request) => {
            senders.spice_data_distributor_actor.send(request);
        }
        T1MessageBody::SpiceChunkContractAccesses(accesses) => {
            senders.spice_data_distributor_actor.send(SpiceChunkContractAccessesMessage(accesses));
        }
        T1MessageBody::SpiceContractCodeRequest(request) => {
            senders.spice_data_distributor_actor.send(SpiceContractCodeRequestMessage(request));
        }
        T1MessageBody::SpiceContractCodeResponse(response) => {
            senders.spice_data_distributor_actor.send(SpiceContractCodeResponseMessage(response));
        }
    }
}

/// Dispatches a T2 routed message body.
fn dispatch_t2(
    body: T2MessageBody,
    _from_peer: PeerId,
    msg_hash: near_primitives::hash::CryptoHash,
    senders: &OneClientSenders,
) {
    match body {
        T2MessageBody::ForwardTx(transaction) => {
            let future = senders.rpc_handler_sender.send_async(ProcessTxRequest {
                transaction,
                is_forwarded: true,
                check_only: false,
            });
            drop(future);
        }
        T2MessageBody::PartialEncodedChunkRequest(request) => {
            senders.shards_manager_sender.send(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                    partial_encoded_chunk_request: request,
                    route_back: msg_hash,
                },
            );
        }
        T2MessageBody::PartialEncodedChunkResponse(response) => {
            senders.shards_manager_sender.send(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                    partial_encoded_chunk_response: response,
                    received_time: near_async::time::Instant::now(),
                },
            );
        }
        T2MessageBody::ChunkStateWitnessAck(ack) => {
            senders.partial_witness_sender.send(ChunkStateWitnessAckMessage(ack));
        }
        T2MessageBody::PartialEncodedContractDeploys(deploys) => {
            senders.partial_witness_sender.send(PartialEncodedContractDeploysMessage(deploys));
        }
        T2MessageBody::TxStatusRequest(..) => {
            // TxStatusRequest requires response routing which is complex.
            // Skip for now — can be added in iteration 11 if needed.
            tracing::warn!(
                target: "test_loop",
                "TxStatusRequest dispatch not yet implemented in testloop"
            );
        }
        T2MessageBody::TxStatusResponse(..) => {
            // ClientSenderForTestLoopNetwork doesn't include a TxStatusResponse sender.
            // Skip for now — can be added in iteration 11 if needed.
            tracing::warn!(
                target: "test_loop",
                "TxStatusResponse dispatch not yet implemented in testloop"
            );
        }
        T2MessageBody::StateHeaderRequest(_) | T2MessageBody::StatePartRequest(_) => {
            // These become Tier3Requests in production, routed through peer_manager.
            // Not needed for typical testloop scenarios.
            tracing::warn!(
                target: "test_loop",
                "state sync request dispatch not yet implemented in testloop"
            );
        }
        T2MessageBody::StateRequestAck(_) => {
            // State sync ack — not commonly needed in testloop.
            tracing::warn!(
                target: "test_loop",
                "StateRequestAck dispatch not yet implemented in testloop"
            );
        }
        T2MessageBody::Ping(_) | T2MessageBody::Pong(_) => {
            // Test-only networking diagnostics, not relevant for testloop.
        }
    }
}

/// TestLoop transport that implements `NetworkTransport` by dispatching
/// `PeerMessage`s directly to target node actors via `dispatch_peer_message`.
///
/// Uses `TestLoopNetworkSharedState` for peer lookup and network partition
/// support: `senders_for_peer()` returns drop-event senders when a link is
/// blocked via `disallowed_peer_links`.
pub(crate) struct TestLoopTransport {
    my_peer_id: PeerId,
    shared_state: TestLoopNetworkSharedState,
    future_spawner: Arc<dyn FutureSpawner>,
}

impl TestLoopTransport {
    pub fn new(
        my_peer_id: PeerId,
        shared_state: TestLoopNetworkSharedState,
        future_spawner: Arc<dyn FutureSpawner>,
    ) -> Self {
        Self { my_peer_id, shared_state, future_spawner }
    }
}

impl NetworkTransport for TestLoopTransport {
    fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        let target_senders = self.shared_state.senders_for_peer(&self.my_peer_id, &peer_id);
        let requester_senders = self.shared_state.senders_for_peer(&peer_id, &self.my_peer_id);
        let msg = Arc::try_unwrap(msg).unwrap_or_else(|arc| (*arc).clone());

        // For routed messages that expect a response, record the route_back on
        // the receiving node so it can route the response back to the sender.
        // In production, intermediate routers do this; in testloop with direct
        // delivery there are no intermediaries.
        if let PeerMessage::Routed(ref routed_msg) = msg {
            if routed_msg.expect_response() {
                if let Some(target_state) = self.shared_state.network_state_for_peer(&peer_id) {
                    target_state.tier2_route_back.lock().insert(
                        &near_async::time::Clock::real(),
                        routed_msg.hash(),
                        self.my_peer_id.clone(),
                    );
                }
            }
        }

        dispatch_peer_message(
            self.my_peer_id.clone(),
            msg,
            &target_senders,
            &*self.future_spawner,
            &requester_senders.client_sender,
            &target_senders.view_client_sender,
        );
        true
    }

    fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        for peer_id in self.shared_state.all_peer_ids() {
            if peer_id == self.my_peer_id {
                continue;
            }
            let target_senders = self.shared_state.senders_for_peer(&self.my_peer_id, &peer_id);
            let requester_senders = self.shared_state.senders_for_peer(&peer_id, &self.my_peer_id);
            let msg = (*msg).clone();
            dispatch_peer_message(
                self.my_peer_id.clone(),
                msg,
                &target_senders,
                &*self.future_spawner,
                &requester_senders.client_sender,
                &target_senders.view_client_sender,
            );
        }
    }
}
