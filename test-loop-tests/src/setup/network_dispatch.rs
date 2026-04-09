#![allow(dead_code, unused_must_use, unused_variables)]

use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::CanSendAsync;
use near_async::time::Clock;
use near_network::client::{
    BlockHeadersRequest, BlockHeadersResponse, BlockRequest, BlockResponse,
};
use near_network::peer_manager_exports::connection::transport::NetworkTransport;
use near_network::peer_manager_exports::network_state::{NetworkState, RoutedAction};
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

        match &msg {
            PeerMessage::Routed(_) => {
                if let PeerMessage::Routed(routed_msg) = msg {
                    self.dispatch_routed_with_hops(target_peer_id.clone(), target_state, routed_msg)
                } else {
                    unreachable!()
                }
            }
            PeerMessage::BlockRequest(_) | PeerMessage::BlockHeadersRequest(_) => {
                // Request-response messages need async handling.
                match msg {
                    PeerMessage::BlockRequest(hash) => {
                        let target = target_state;
                        let my_id = my_peer_id;
                        let node_states = self.node_network_states.clone();
                        self.future_spawner.spawn("testloop_block_request", async move {
                            if let Ok(Some(block)) =
                                target.client.send_async(BlockRequest(hash)).await
                            {
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
                                let response = PeerMessage::BlockHeaders(headers);
                                let target_node_id = target.config.node_id();
                                let response = match shared_state.apply_message_filters(
                                    &target_node_id,
                                    &my_id,
                                    response,
                                ) {
                                    Some(msg) => msg,
                                    None => return,
                                };
                                if let PeerMessage::BlockHeaders(headers) = response {
                                    let requester_state = {
                                        let states = node_states.lock();
                                        states.get(&my_id).cloned()
                                    };
                                    if let Some(requester) = requester_state {
                                        requester.client.send_async(
                                            BlockHeadersResponse(headers, target_node_id)
                                                .span_wrap(),
                                        );
                                    }
                                }
                            }
                        });
                        true
                    }
                    _ => unreachable!(),
                }
            }
            _ => {
                // Fire-and-forget messages: use shared dispatch on NetworkState.
                if target_state.dispatch_incoming_message(my_peer_id, msg) {
                    true
                } else {
                    tracing::warn!(
                        target: "network",
                        "testloop transport: unhandled non-routed message type, dropping"
                    );
                    false
                }
            }
        }
    }
}

impl TestLoopTransport {
    /// Dispatch a routed message through the network, following multi-hop BFS
    /// routing. Each intermediate node processes the message via
    /// `process_incoming_routed`, which either consumes it (if the node is the
    /// target) or returns the next hop to forward to.
    ///
    /// Messages that expect a response (e.g. TxStatusRequest) are dispatched
    /// async at the final destination so the response can be awaited and routed
    /// back to the author.
    fn dispatch_routed_with_hops(
        &self,
        first_hop: PeerId,
        first_hop_state: Arc<NetworkState>,
        routed_msg: Box<near_network::RoutedMessage>,
    ) -> bool {
        let expects_response = routed_msg.expect_response();

        // For response-expecting messages, deliver directly to the target
        // (first hop) using the async path, same as before.
        if expects_response {
            let msg_hash = routed_msg.hash();
            let msg_author = routed_msg.author().clone();
            let body = routed_msg.body_owned();
            let my_peer_id = self.my_peer_id.clone();
            let clock = self.clock.clone();
            let target_state = first_hop_state;
            let node_states = self.node_network_states.clone();
            let shared_state = self.shared_state.clone();

            // Record route-back on the target.
            target_state.tier2_route_back.lock().insert(&clock, msg_hash, my_peer_id.clone());

            // Dedup check.
            {
                let mut recent = target_state.recent_routed_messages.lock();
                if recent.put(msg_hash, ()).is_some() {
                    return true;
                }
            }

            self.future_spawner.spawn("testloop_routed_dispatch", async move {
                let response = target_state
                    .receive_routed_message(&clock, msg_author.clone(), my_peer_id, msg_hash, body)
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
                        if shared_state.is_link_allowed(&target_state.config.node_id(), &msg_author)
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
            return true;
        }

        // Non-response messages: follow the hop loop.
        let mut current_peer = first_hop;
        let mut current_state = first_hop_state;
        let mut from = self.my_peer_id.clone();
        let mut msg = routed_msg;

        loop {
            match current_state.process_incoming_routed(&self.clock, from.clone(), msg) {
                RoutedAction::Consumed => return true,
                RoutedAction::Forward { next_hop, msg: fwd_msg } => {
                    // Check partition for this hop.
                    if !self.shared_state.is_link_allowed(&current_peer, &next_hop) {
                        return false;
                    }
                    // Apply filters for this hop.
                    let routed_peer_msg = PeerMessage::Routed(fwd_msg);
                    let filtered = self.shared_state.apply_message_filters(
                        &current_peer,
                        &next_hop,
                        routed_peer_msg,
                    );
                    match filtered {
                        Some(PeerMessage::Routed(filtered_msg)) => {
                            let next_state = {
                                let states = self.node_network_states.lock();
                                states.get(&next_hop).cloned()
                            };
                            let Some(next_state) = next_state else {
                                return false;
                            };
                            from = current_peer;
                            current_peer = next_hop;
                            current_state = next_state;
                            msg = filtered_msg;
                        }
                        _ => return false, // dropped by filter
                    }
                }
                RoutedAction::Dropped => return false,
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
