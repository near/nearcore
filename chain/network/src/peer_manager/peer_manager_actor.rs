use crate::client;
use crate::config;
use crate::debug::{DebugStatus, GetDebugStatus};
use crate::network_protocol::{
    AccountOrPeerIdOrHash, Disconnect, Edge, PeerIdOrHash, PeerMessage, Ping, Pong,
    RawRoutedMessage, RoutedMessageBody,
};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::{NetworkState, WhitelistNode};
use crate::peer_manager::peer_store;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::stats::metrics;
use crate::store;
use crate::tcp;
use crate::types::{
    ConnectedPeerInfo, GetNetworkInfo, HighestHeightPeerInfo, KnownProducer, NetworkInfo,
    NetworkRequests, NetworkResponses, PeerInfo, PeerManagerMessageRequest,
    PeerManagerMessageResponse, PeerType, SetChainInfo,
};
use actix::fut::future::wrap_future;
use actix::{Actor as _, AsyncContext as _};
use anyhow::Context as _;
use near_async::messaging::Sender;
use near_o11y::{handler_debug_span, handler_trace_span, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::block::GenesisId;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time;
use near_primitives::views::{
    ConnectionInfoView, EdgeView, KnownPeerStateView, NetworkGraphView, PeerStoreView,
    RecentOutboundConnectionsView,
};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use rand::Rng;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument as _;

/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The initial waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_INITIAL_DURATION: time::Duration = time::Duration::milliseconds(10);
/// How often should we check wheter local edges match the connection pool.
const FIX_LOCAL_EDGES_INTERVAL: time::Duration = time::Duration::seconds(60);

/// Number of times to attempt reconnection when trying to re-establish a connection.
const MAX_RECONNECT_ATTEMPTS: usize = 6;

/// How often to report bandwidth stats.
const REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL: time::Duration =
    time::Duration::milliseconds(60_000);

/// If we received more than `REPORT_BANDWIDTH_THRESHOLD_BYTES` of data from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_BYTES: usize = 10_000_000;
/// If we received more than REPORT_BANDWIDTH_THRESHOLD_COUNT` of messages from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_COUNT: usize = 10_000;

/// If a peer is more than these blocks behind (comparing to our current head) - don't route any messages through it.
/// We are updating the list of unreliable peers every MONITOR_PEER_MAX_DURATION (60 seconds) - so the current
/// horizon value is roughly matching this threshold (if the node is 60 blocks behind, it will take it a while to recover).
/// If we set this horizon too low (for example 2 blocks) - we're risking excluding a lot of peers in case of a short
/// network issue.
const UNRELIABLE_PEER_HORIZON: u64 = 60;

/// Due to implementation limits of `Graph` in `near-network`, we support up to 128 client.
pub const MAX_TIER2_PEERS: usize = 128;

/// When picking a peer to connect to, we'll pick from the 'safer peers'
/// (a.k.a. ones that we've been connected to in the past) with these odds.
/// Otherwise, we'd pick any peer that we've heard about.
const PREFER_PREVIOUSLY_CONNECTED_PEER: f64 = 0.6;

/// How often to update the connections in storage.
pub(crate) const UPDATE_CONNECTION_STORE_INTERVAL: time::Duration = time::Duration::minutes(1);

/// TEST-ONLY
/// A generic set of events (observable in tests) that the Network may generate.
/// Ideally the tests should observe only public API properties, but until
/// we are at that stage, feel free to add any events that you need to observe.
/// In particular prefer emitting a new event to polling for a state change.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    PeerManagerStarted,
    ServerStarted,
    RoutedMessageDropped,
    AccountsAdded(Vec<AnnounceAccount>),
    EdgesAdded(Vec<Edge>),
    Ping(Ping),
    Pong(Pong),
    // Reported once a message has been processed.
    // In contrast to typical RPC protocols, many P2P messages do not trigger
    // sending a response at the end of processing.
    // However, for precise instrumentation in tests it is useful to know when
    // processing has been finished. We simulate the "RPC response" by reporting
    // an event MessageProcessed.
    //
    // Given that processing is asynchronous and unstructured as of now,
    // it is hard to pinpoint all the places when the processing of a message is
    // actually complete. Currently this event is reported only for some message types,
    // feel free to add support for more.
    MessageProcessed(tcp::Tier, PeerMessage),
    // Reported when a reconnect loop is spawned.
    ReconnectLoopSpawned(PeerInfo),
    // Reported when a handshake has been started.
    HandshakeStarted(crate::peer::peer_actor::HandshakeStartedEvent),
    // Reported when a handshake has been successfully completed.
    HandshakeCompleted(crate::peer::peer_actor::HandshakeCompletedEvent),
    // Reported when the TCP connection has been closed.
    ConnectionClosed(crate::peer::peer_actor::ConnectionClosedEvent),
}

impl actix::Actor for PeerManagerActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Periodically starts peer monitoring.
        tracing::debug!(target: "network",
               max_period=?self.state.config.monitor_peers_max_period,
               "monitor_peers_trigger");
        self.monitor_peers_trigger(
            ctx,
            MONITOR_PEERS_INITIAL_DURATION,
            (MONITOR_PEERS_INITIAL_DURATION, ),
        );
    }

    /// Try to gracefully disconnect from connected peers.
    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
        tracing::warn!("PeerManager: stopping");
        self.state.tier2.broadcast_message(Arc::new(PeerMessage::Disconnect(Disconnect {
            remove_from_connection_store: false,
        })));
        actix::Running::Stop
    }
}

    /// Return whether the message is sent or not.
    fn send_message_to_account_or_peer_or_hash(
        &mut self,
        target: &AccountOrPeerIdOrHash,
        msg: RoutedMessageBody,
    ) -> bool {
        let target = match target {
            AccountOrPeerIdOrHash::AccountId(account_id) => { return self.send_message_to_account(account_id, msg); }
            AccountOrPeerIdOrHash::PeerId(it) => PeerIdOrHash::PeerId(it.clone()),
            AccountOrPeerIdOrHash::Hash(it) => PeerIdOrHash::Hash(*it),
        };
        self.send_message_to_peer(tcp::Tier::T2, self.sign_message(RawRoutedMessage { target, body: msg }))
    }

    #[perf]
    fn handle_msg_network_requests(
        &self,
    ) -> NetworkResponses {
        metrics::REQUEST_COUNT_BY_TYPE_TOTAL.with_label_values(&[msg.as_ref()]).inc();
        match msg {
            NetworkRequests::Block { block } => {
                self.tier2.broadcast_message(&PeerMessage::Block(block)).await
            }
            NetworkRequests::Approval { approval_message } => {
                self.send_message_to_account(
                    &approval_message.target,
                    RoutedMessageBody::BlockApproval(approval_message.approval),
                ).await
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                self.tier2.send_message(peer_id, Arc::new(PeerMessage::BlockRequest(hash))).await
            }
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if self.tier2.send_message(peer_id, &PeerMessage::BlockHeadersRequest(hashes)).await
            }
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, target } => {
                self.send_message_to_account_or_peer_or_hash(
                    &target,
                    RoutedMessageBody::StateRequestHeader(shard_id, sync_hash),
                )
            }
            NetworkRequests::StateRequestPart { shard_id, sync_hash, part_id, target } => {
                self.send_message_to_account_or_peer_or_hash(
                    &target,
                    RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id),
                )
            }
            NetworkRequests::StateResponse { route_back, response } => {
                let body = RoutedMessageBody::VersionedStateResponse(response);
                self.send_message_to_peer(
                    tcp::Tier::T2,
                    self.sign_message(
                        RawRoutedMessage { target: PeerIdOrHash::Hash(route_back), body },
                    ),
                )
            }
            NetworkRequests::PartialEncodedChunkRequest { target, request, create_time } => {
                metrics::PARTIAL_ENCODED_CHUNK_REQUEST_DELAY.observe((ctx::time::now() - create_time.0).as_seconds_f64());
                let mut success = false;
                let body = RoutedMessageBody::PartialEncodedChunkRequest(request.clone());
                // Make two attempts to send the message. First following the preference of `prefer_peer`,
                // and if it fails, against the preference.
                for prefer_peer in &[target.prefer_peer, !target.prefer_peer] {
                    if !prefer_peer {
                        if let Some(account_id) = target.account_id.as_ref() {
                            if self.send_message_to_account(account_id, body) {
                                success = true;
                                break;
                            }
                        }
                    } else {
                        let mut matching_peers = vec![];
                        for (peer_id, peer) in &self.tier2.load().ready {
                            let last_block = peer.last_block.load();
                            if (peer.archival || !target.only_archival)
                                && last_block.is_some()
                                && last_block.as_ref().unwrap().height >= target.min_height
                                && peer.tracked_shards.contains(&target.shard_id)
                            {
                                matching_peers.push(peer_id.clone());
                            }
                        }

                        if let Some(matching_peer) = matching_peers.iter().choose(&mut thread_rng())
                        {
                            if self.send_message_to_peer(
                                tcp::Tier::T2,
                                self.sign_message(RawRoutedMessage {
                                    target: PeerIdOrHash::PeerId(matching_peer.clone()),
                                    body: body.clone(),
                                }),
                            ) {
                                success = true;
                                break;
                            }
                        } else {
                            tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find any matching peer for chunk");
                        }
                    }
                }

                if success {
                    NetworkResponses::NoResponse
                } else {
                    tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find a route for chunk");
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                self.send_message_to_peer(
                    tcp::Tier::T2,
                    self.state.sign_message(
                        &self.clock,
                        RawRoutedMessage {
                            target: PeerIdOrHash::Hash(route_back),
                            body: RoutedMessageBody::PartialEncodedChunkResponse(response),
                        },
                    ),
                )
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                self.send_message_to_account(&account_id, RoutedMessageBody::VersionedPartialEncodedChunk(partial_encoded_chunk.into()))
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                self.send_message_to_account(&account_id, RoutedMessageBody::PartialEncodedChunkForward(forward)).await
            }
            NetworkRequests::ForwardTx(account_id, tx) => {
                self.send_message_to_account(&account_id, RoutedMessageBody::ForwardTx(tx)).await
            }
            NetworkRequests::TxStatus(account_id, signer_account_id, tx_hash) => {
                self.send_message_to_account(&account_id, RoutedMessageBody::TxStatusRequest(signer_account_id, tx_hash).await
            }
            NetworkRequests::Challenge(challenge) => self.tier2.broadcast_message(&PeerMessage::Challenge(challenge)).await
        }
    }

    fn handle_peer_manager_message(
        &mut self,
        msg: PeerManagerMessageRequest,
        ctx: &mut actix::Context<Self>,
    ) -> PeerManagerMessageResponse {
        match msg {
            PeerManagerMessageRequest::NetworkRequests(msg) => {
                PeerManagerMessageResponse::NetworkResponses(
                    self.handle_msg_network_requests(msg, ctx),
                )
            }
        }
    }
}

impl actix::Handler<WithSpanContext<SetChainInfo>> for PeerManagerActor {
    type Result = ();
    fn handle(&mut self, msg: WithSpanContext<SetChainInfo>, ctx: &mut Self::Context) {
        let (_span, SetChainInfo(info)) = handler_trace_span!(target: "network", msg);
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&["SetChainInfo"]).start_timer();
        // We call self.state.set_chain_info()
        // synchronously, therefore, assuming actix in-order delivery,
        // there will be no race condition between subsequent SetChainInfo
        // calls.
        if !self.state.set_chain_info(info) {
            // We early exit in case the set of TIER1 account keys hasn't changed.
            return;
        }

        let state = self.state.clone();
        let clock = self.clock.clone();
        ctx.spawn(wrap_future(
            async move {
                // This node might have become a TIER1 node due to the change of the key set.
                // If so we should recompute and readvertise the list of proxies.
                // This is mostly important in case a node is its own proxy. In all other cases
                // (when proxies are different nodes) the update of the key set happens asynchronously
                // and this node won't be able to connect to proxies until it happens (and only the
                // connected proxies are included in the advertisement). We run tier1_advertise_proxies
                // periodically in the background anyway to cover those cases.
                state.tier1_advertise_proxies(&clock).await;
            }
            .in_current_span(),
        ));
    }
}
