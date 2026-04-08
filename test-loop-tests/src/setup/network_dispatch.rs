#![allow(dead_code, unused_must_use, unused_variables)]

use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{CanSend, CanSendAsync};
use near_async::time::Clock;
use near_network::client::{
    BlockHeadersRequest, BlockHeadersResponse, BlockRequest, BlockResponse,
    EpochSyncRequestMessage, EpochSyncResponseMessage, OptimisticBlockMessage, ProcessTxRequest,
    StateResponse, StateResponseReceived,
};
use near_network::peer_manager_exports::connection::transport::NetworkTransport;
use near_network::peer_manager_exports::network_state::NetworkState;
use near_network::types::PeerMessage;
use near_network::{PeerIdOrHash, RawRoutedMessage};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
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

        match msg.as_ref().clone() {
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

                // Dispatch asynchronously via future_spawner.
                let node_states = self.node_network_states.clone();
                let shared_state = self.shared_state.clone();
                let spawner_my_peer_id = self.my_peer_id.clone();
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

                    // If there's a response, send it back to the author.
                    if let Some(response_body) = response {
                        let response_msg = target_state.sign_message(
                            &clock,
                            RawRoutedMessage {
                                target: PeerIdOrHash::Hash(msg_hash),
                                body: response_body,
                            },
                        );
                        // Find the author's state to deliver the response.
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
                true
            }

            PeerMessage::Block(block) => {
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
                let node_states = self.node_network_states.clone();
                self.future_spawner.spawn("testloop_block_headers_request", async move {
                    if let Ok(Some(headers)) =
                        target.client.send_async(BlockHeadersRequest(hashes)).await
                    {
                        let requester_state = {
                            let states = node_states.lock();
                            states.get(&my_id).cloned()
                        };
                        if let Some(requester) = requester_state {
                            requester.client.send_async(
                                BlockHeadersResponse(headers, target.config.node_id()).span_wrap(),
                            );
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
