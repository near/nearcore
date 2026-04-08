//! TestLoop transport that delivers `PeerMessage`s directly to target node
//! actors. Non-routed fire-and-forget messages and routed messages are
//! dispatched via the production `MessageDispatcher`, eliminating duplication
//! between testloop and production dispatch paths.
//!
//! Request/response messages (`BlockRequest`, `BlockHeadersRequest`) are handled
//! inline because the response must be routed back to the requesting node's
//! client sender, which requires caller-side context that `MessageDispatcher`
//! does not have.

use std::sync::Arc;

use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::CanSendAsync;
use near_client::BlockResponse;
use near_network::client::{BlockHeadersRequest, BlockHeadersResponse, BlockRequest};
use near_network::types::{NetworkTransport, PeerMessage};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::network::PeerId;

use super::peer_manager_actor::TestLoopNetworkSharedState;

/// TestLoop transport that implements `NetworkTransport` by dispatching
/// `PeerMessage`s to target node actors.
///
/// Most messages are dispatched via the target node's `MessageDispatcher`,
/// which uses the same code path as production. Request/response messages
/// (`BlockRequest`, `BlockHeadersRequest`) are handled inline because the
/// response must be routed back to the requester's client sender.
///
/// Uses `TestLoopNetworkSharedState` for peer lookup and network partition
/// support: disallowed links cause messages to be silently dropped.
///
/// Transport-level message filters registered on `TestLoopNetworkSharedState`
/// are applied before delivery. Each filter receives (from, to, &msg) and
/// can modify or drop messages.
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

    /// Dispatches a single message to the target peer. Returns `false` if the
    /// message was dropped (disallowed link, filtered, or missing network state).
    fn dispatch_to_peer(&self, peer_id: &PeerId, msg: PeerMessage) -> bool {
        // Check if the link is allowed (network partition simulation).
        if !self.shared_state.is_link_allowed(&self.my_peer_id, peer_id) {
            return false;
        }

        // Apply transport-level message filters from shared state.
        let msg = match self.shared_state.apply_message_filters(&self.my_peer_id, peer_id, msg) {
            Some(msg) => msg,
            None => return false,
        };

        let target_state = match self.shared_state.network_state_for_peer(peer_id) {
            Some(state) => state,
            None => return false,
        };

        // For routed messages that expect a response, record the route_back on
        // the receiving node so it can route the response back to the sender.
        // In production, intermediate routers do this; in testloop with direct
        // delivery there are no intermediaries.
        if let PeerMessage::Routed(ref routed_msg) = msg {
            if routed_msg.expect_response() {
                target_state.dispatcher.tier2_route_back.lock().insert(
                    &near_async::time::Clock::real(),
                    routed_msg.hash(),
                    self.my_peer_id.clone(),
                );
            }
        }

        // BlockRequest/BlockHeadersRequest need special handling: the response
        // must be routed back to the REQUESTER's client sender (via its
        // MessageDispatcher). Also check reverse link (target -> requester)
        // for the response path.
        match msg {
            PeerMessage::BlockRequest(hash) => {
                let response_future = target_state.dispatcher.client.send_async(BlockRequest(hash));
                // Get requester's dispatcher for routing the response back.
                let requester_state =
                    self.shared_state.network_state_for_peer(&self.my_peer_id).unwrap();
                let reverse_allowed = self.shared_state.is_link_allowed(peer_id, &self.my_peer_id);
                let peer_id = self.my_peer_id.clone();
                self.future_spawner.spawn(
                    "dispatch: route BlockResponse back to requester",
                    async move {
                        if !reverse_allowed {
                            return;
                        }
                        let Ok(Some(block)) = response_future.await else {
                            return;
                        };
                        let future = requester_state.dispatcher.client.send_async(
                            BlockResponse { block, peer_id, was_requested: true }.span_wrap(),
                        );
                        drop(future);
                    },
                );
            }
            PeerMessage::BlockHeadersRequest(hashes) => {
                let response_future =
                    target_state.dispatcher.client.send_async(BlockHeadersRequest(hashes));
                let requester_state =
                    self.shared_state.network_state_for_peer(&self.my_peer_id).unwrap();
                let reverse_allowed = self.shared_state.is_link_allowed(peer_id, &self.my_peer_id);
                let peer_id = self.my_peer_id.clone();
                self.future_spawner.spawn(
                    "dispatch: route BlockHeadersResponse back to requester",
                    async move {
                        if !reverse_allowed {
                            return;
                        }
                        let Ok(Some(headers)) = response_future.await else {
                            return;
                        };
                        let future = requester_state
                            .dispatcher
                            .client
                            .send_async(BlockHeadersResponse(headers, peer_id).span_wrap());
                        drop(future);
                    },
                );
            }
            // All other messages: delegate to the production MessageDispatcher.
            msg => {
                let dispatcher = target_state.dispatcher.clone();
                let from_peer = self.my_peer_id.clone();
                self.future_spawner.spawn(
                    "dispatch: MessageDispatcher.dispatch_peer_message",
                    async move {
                        dispatcher
                            .dispatch_peer_message(&near_async::time::Clock::real(), from_peer, msg)
                            .await;
                    },
                );
            }
        }

        true
    }
}

impl NetworkTransport for TestLoopTransport {
    fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        let msg = Arc::try_unwrap(msg).unwrap_or_else(|arc| (*arc).clone());
        self.dispatch_to_peer(&peer_id, msg)
    }

    fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        for peer_id in self.shared_state.all_peer_ids() {
            if peer_id == self.my_peer_id {
                continue;
            }
            let msg = (*msg).clone();
            self.dispatch_to_peer(&peer_id, msg);
        }
    }
}
