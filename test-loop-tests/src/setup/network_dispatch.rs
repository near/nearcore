#![allow(dead_code, unused_must_use, unused_variables)]

use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{CanSend, CanSendAsync};
use near_async::time::Clock;
use near_network::client::{
    BlockApproval, BlockHeadersRequest, BlockHeadersResponse, BlockRequest, BlockResponse,
    ChunkEndorsementMessage, EpochSyncRequestMessage, EpochSyncResponseMessage,
    OptimisticBlockMessage, ProcessTxRequest, SpiceChunkEndorsementMessage, StateResponse,
    StateResponseReceived, TxStatusResponse,
};
use near_network::peer_manager_exports::connection::transport::NetworkTransport;
use near_network::peer_manager_exports::network_state::NetworkState;
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
use near_network::types::PeerMessage;
use near_network::{
    PeerIdOrHash, RawRoutedMessage, T1MessageBody, T2MessageBody, TieredMessageBody,
};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use super::peer_manager_actor::TestLoopNetworkSharedState;

/// In-memory transport for testloop that dispatches messages to target nodes
/// via their NetworkState, bypassing TCP entirely.
pub struct TestLoopTransport {
    my_peer_id: PeerId,
    shared_state: TestLoopNetworkSharedState,
    /// Map from PeerId to the target node's NetworkState.
    node_network_states: Arc<Mutex<HashMap<PeerId, Arc<NetworkState>>>>,
    clock: Clock,
    future_spawner: Arc<dyn FutureSpawner>,
}

impl TestLoopTransport {
    pub fn new(
        my_peer_id: PeerId,
        shared_state: TestLoopNetworkSharedState,
        node_network_states: Arc<Mutex<HashMap<PeerId, Arc<NetworkState>>>>,
        clock: Clock,
        future_spawner: Arc<dyn FutureSpawner>,
    ) -> Self {
        Self { my_peer_id, shared_state, node_network_states, clock, future_spawner }
    }

    fn dispatch_to_target(&self, target_peer_id: &PeerId, msg: Arc<PeerMessage>) -> bool {
        // Check partition simulation.
        if !self.shared_state.is_link_allowed(&self.my_peer_id, target_peer_id) {
            tracing::debug!(
                target: "network",
                from = %self.my_peer_id,
                to = %target_peer_id,
                "testloop transport: link disallowed, dropping message"
            );
            return false;
        }

        // Apply message filters (transport-level interception).
        let msg = match self.shared_state.apply_message_filters(
            &self.my_peer_id,
            target_peer_id,
            msg.as_ref().clone(),
        ) {
            Some(msg) => msg,
            None => return false, // dropped by filter
        };

        let target_state = {
            let states = self.node_network_states.lock();
            match states.get(target_peer_id) {
                Some(state) => state.clone(),
                None => {
                    tracing::warn!(
                        target: "network",
                        to = %target_peer_id,
                        "testloop transport: target node not found"
                    );
                    return false;
                }
            }
        };

        let my_peer_id = self.my_peer_id.clone();
        let clock = self.clock.clone();

        match msg {
            PeerMessage::Routed(msg) => {
                let msg_hash = msg.hash();
                let msg_author = msg.author().clone();
                let expects_response = msg.expect_response();
                let body = msg.body_owned();

                // Record route-back on the target so responses find their way back.
                if expects_response {
                    target_state.tier2_route_back.lock().insert(
                        &clock,
                        msg_hash,
                        my_peer_id.clone(),
                    );
                }

                // Dedup check.
                {
                    let mut recent = target_state.recent_routed_messages.lock();
                    if recent.put(msg_hash, ()).is_some() {
                        // Already seen this message.
                        return true;
                    }
                }

                if expects_response {
                    // Messages expecting a response must be dispatched async
                    // so we can await the response and send it back.
                    let node_states = self.node_network_states.clone();
                    let shared_state = self.shared_state.clone();
                    self.future_spawner.spawn("testloop_routed_dispatch", async move {
                        let response = target_state
                            .receive_routed_message(
                                &clock,
                                msg_author.clone(),
                                my_peer_id,
                                msg_hash,
                                body,
                            )
                            .await;

                        if let Some(response_body) = response {
                            let response_msg = target_state.sign_message(
                                &clock,
                                RawRoutedMessage {
                                    target: PeerIdOrHash::Hash(msg_hash),
                                    body: response_body,
                                },
                            );
                            let author_state = {
                                let states = node_states.lock();
                                states.get(&msg_author).cloned()
                            };
                            if let Some(author_state) = author_state {
                                if shared_state
                                    .is_link_allowed(&target_state.config.node_id(), &msg_author)
                                {
                                    let response_hash = response_msg.hash();
                                    let response_body = response_msg.body_owned();
                                    author_state
                                        .receive_routed_message(
                                            &clock,
                                            target_state.config.node_id(),
                                            target_state.config.node_id(),
                                            response_hash,
                                            response_body,
                                        )
                                        .await;
                                }
                            }
                        }
                    });
                } else {
                    // Non-response messages: dispatch directly to target's actor
                    // senders (single-hop). This matches the old mock's behavior
                    // where messages were delivered in the same event, avoiding
                    // the extra event cycle from future_spawner.spawn().
                    dispatch_routed_message_directly(&target_state, my_peer_id, msg_hash, body);
                }
                true
            }

            PeerMessage::Block(block) => {
                // Record sender's block info so highest_height_peers is populated for sync.
                let height = block.header().height();
                let hash = *block.hash();
                target_state.testloop_peer_block_info.lock().insert(
                    my_peer_id.clone(),
                    (
                        near_network::types::PeerInfo {
                            id: my_peer_id.clone(),
                            addr: None,
                            account_id: None,
                        },
                        near_network::types::BlockInfo { height, hash },
                    ),
                );
                target_state.client.send_async(
                    BlockResponse { block, peer_id: my_peer_id, was_requested: false }.span_wrap(),
                );
                true
            }

            PeerMessage::Transaction(tx) => {
                target_state.client.send_async(ProcessTxRequest {
                    transaction: tx,
                    is_forwarded: false,
                    check_only: false,
                });
                true
            }

            PeerMessage::BlockRequest(hash) => {
                let target = target_state;
                let my_id = my_peer_id;
                let node_states = self.node_network_states.clone();
                self.future_spawner.spawn("testloop_block_request", async move {
                    if let Ok(Some(block)) = target.client.send_async(BlockRequest(hash)).await {
                        // Send block back to requester.
                        let requester_state = {
                            let states = node_states.lock();
                            states.get(&my_id).cloned()
                        };
                        if let Some(requester) = requester_state {
                            requester.client.send_async(
                                BlockResponse {
                                    block,
                                    peer_id: target.config.node_id(),
                                    was_requested: true,
                                }
                                .span_wrap(),
                            );
                        }
                    }
                });
                true
            }

            PeerMessage::BlockHeadersRequest(hashes) => {
                let target = target_state;
                let my_id = my_peer_id;
                let shared_state = self.shared_state.clone();
                let node_states = self.node_network_states.clone();
                self.future_spawner.spawn("testloop_block_headers_request", async move {
                    if let Ok(Some(headers)) =
                        target.client.send_async(BlockHeadersRequest(hashes)).await
                    {
                        // Send response as PeerMessage::BlockHeaders through the
                        // filter chain (apply_message_filters) so transport filters
                        // like throttle_header_sync can intercept it.
                        let response = PeerMessage::BlockHeaders(headers);
                        let target_node_id = target.config.node_id();
                        let response = match shared_state.apply_message_filters(
                            &target_node_id,
                            &my_id,
                            response,
                        ) {
                            Some(msg) => msg,
                            None => return, // dropped by filter
                        };
                        // Dispatch the (possibly filtered) response to the requester.
                        if let PeerMessage::BlockHeaders(headers) = response {
                            let requester_state = {
                                let states = node_states.lock();
                                states.get(&my_id).cloned()
                            };
                            if let Some(requester) = requester_state {
                                requester.client.send_async(
                                    BlockHeadersResponse(headers, target_node_id).span_wrap(),
                                );
                            }
                        }
                    }
                });
                true
            }

            PeerMessage::BlockHeaders(headers) => {
                target_state
                    .client
                    .send_async(BlockHeadersResponse(headers, my_peer_id).span_wrap());
                true
            }

            PeerMessage::VersionedStateResponse(info) => {
                target_state.client.send_async(
                    StateResponseReceived {
                        peer_id: my_peer_id,
                        state_response: StateResponse::State(info.into()),
                    }
                    .span_wrap(),
                );
                true
            }

            PeerMessage::EpochSyncRequest => {
                target_state.client.send(EpochSyncRequestMessage { from_peer: my_peer_id });
                true
            }

            PeerMessage::EpochSyncResponse(proof) => {
                target_state.client.send(EpochSyncResponseMessage { from_peer: my_peer_id, proof });
                true
            }

            PeerMessage::OptimisticBlock(ob) => {
                target_state.client.send(
                    OptimisticBlockMessage { from_peer: my_peer_id, optimistic_block: ob }
                        .span_wrap(),
                );
                true
            }

            other => {
                tracing::warn!(
                    target: "network",
                    msg = ?format!("{:?}", std::mem::discriminant(&other)),
                    "testloop transport: unhandled non-routed message type, dropping"
                );
                false
            }
        }
    }
}

impl NetworkTransport for TestLoopTransport {
    fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        self.dispatch_to_target(&peer_id, msg)
    }

    fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        let all_peers: Vec<PeerId> = {
            let states = self.node_network_states.lock();
            states.keys().filter(|id| **id != self.my_peer_id).cloned().collect()
        };
        for peer_id in all_peers {
            self.dispatch_to_target(&peer_id, msg.clone());
        }
    }
}

/// Dispatch a routed message body directly to the target node's actor senders.
/// This is a single-hop delivery (matching the old mock's behavior) instead of
/// going through future_spawner.spawn() + receive_routed_message (two hops).
///
/// For non-response messages only — response messages need the async path to
/// await the response and route it back.
///
/// `send_async()` enqueues the message immediately when called (before `.await`),
/// so we can call it and drop the returned future — the message is already queued.
/// `send()` is synchronous fire-and-forget.
fn dispatch_routed_message_directly(
    target_state: &Arc<NetworkState>,
    from_peer: PeerId,
    msg_hash: CryptoHash,
    body: TieredMessageBody,
) {
    match body {
        TieredMessageBody::T1(body) => match *body {
            T1MessageBody::BlockApproval(approval) => {
                target_state.client.send_async(BlockApproval(approval, from_peer).span_wrap());
            }
            T1MessageBody::VersionedPartialEncodedChunk(chunk) => {
                target_state
                    .shards_manager_adapter
                    .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(*chunk));
            }
            T1MessageBody::PartialEncodedChunkForward(msg) => {
                target_state
                    .shards_manager_adapter
                    .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(msg));
            }
            T1MessageBody::PartialEncodedStateWitness(witness) => {
                target_state
                    .partial_witness_adapter
                    .send(PartialEncodedStateWitnessMessage(witness));
            }
            T1MessageBody::PartialEncodedStateWitnessForward(witness) => {
                target_state
                    .partial_witness_adapter
                    .send(PartialEncodedStateWitnessForwardMessage(witness));
            }
            T1MessageBody::VersionedChunkEndorsement(endorsement) => {
                target_state.client.send_async(ChunkEndorsementMessage(endorsement));
            }
            T1MessageBody::ChunkContractAccesses(accesses) => {
                target_state.partial_witness_adapter.send(ChunkContractAccessesMessage(accesses));
            }
            T1MessageBody::ContractCodeRequest(request) => {
                target_state.partial_witness_adapter.send(ContractCodeRequestMessage(request));
            }
            T1MessageBody::ContractCodeResponse(response) => {
                target_state.partial_witness_adapter.send(ContractCodeResponseMessage(response));
            }
            T1MessageBody::SpicePartialData(spice_partial_data) => {
                target_state
                    .spice_data_distributor_adapter
                    .send(SpiceIncomingPartialData { data: spice_partial_data });
            }
            T1MessageBody::SpiceChunkEndorsement(endorsement) => {
                target_state
                    .spice_core_writer_adapter
                    .send(SpiceChunkEndorsementMessage(endorsement));
            }
            T1MessageBody::SpicePartialDataRequest(request) => {
                target_state.spice_data_distributor_adapter.send(request);
            }
            T1MessageBody::SpiceChunkContractAccesses(accesses) => {
                target_state
                    .spice_data_distributor_adapter
                    .send(SpiceChunkContractAccessesMessage(accesses));
            }
            T1MessageBody::SpiceContractCodeRequest(request) => {
                target_state
                    .spice_data_distributor_adapter
                    .send(SpiceContractCodeRequestMessage(request));
            }
            T1MessageBody::SpiceContractCodeResponse(response) => {
                target_state
                    .spice_data_distributor_adapter
                    .send(SpiceContractCodeResponseMessage(response));
            }
        },
        TieredMessageBody::T2(body) => match *body {
            T2MessageBody::TxStatusResponse(tx_result) => {
                target_state.client.send_async(TxStatusResponse(tx_result.into()));
            }
            T2MessageBody::ForwardTx(transaction) => {
                target_state.client.send_async(ProcessTxRequest {
                    transaction,
                    is_forwarded: true,
                    check_only: false,
                });
            }
            T2MessageBody::PartialEncodedChunkRequest(request) => {
                target_state.shards_manager_adapter.send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                        partial_encoded_chunk_request: request,
                        route_back: msg_hash,
                    },
                );
            }
            T2MessageBody::PartialEncodedChunkResponse(response) => {
                target_state.shards_manager_adapter.send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                        partial_encoded_chunk_response: response,
                        received_time: near_async::time::Instant::now().into(),
                    },
                );
            }
            T2MessageBody::ChunkStateWitnessAck(ack) => {
                target_state.partial_witness_adapter.send(ChunkStateWitnessAckMessage(ack));
            }
            T2MessageBody::PartialEncodedContractDeploys(deploys) => {
                target_state
                    .partial_witness_adapter
                    .send(PartialEncodedContractDeploysMessage(deploys));
            }
            T2MessageBody::StateRequestAck(ack) => {
                target_state.client.send_async(
                    StateResponseReceived {
                        peer_id: from_peer,
                        state_response: StateResponse::Ack(ack),
                    }
                    .span_wrap(),
                );
            }
            body => {
                tracing::warn!(
                    target: "network",
                    ?body,
                    "testloop direct dispatch: unhandled routed message type, dropping"
                );
            }
        },
    }
}
