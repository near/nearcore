use crate::peer::codec::Codec;
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::peer_store::{PeerStore, TrustLevel};
use crate::private_actix::{
    PeerRequestResult, PeersRequest, RegisterPeer, RegisterPeerResponse, SendMessage, StopMsg,
    Unregister, ValidateEdgeList,
};
use crate::routing::edge_validator_actor::EdgeValidatorHelper;
use crate::routing::routing_table_actor::{
    Prune, RoutingTableActor, RoutingTableMessages, RoutingTableMessagesResponse,
};
use crate::routing::routing_table_view::{RoutingTableView, DELETE_PEERS_AFTER_TIME};
use crate::stats::metrics;
use crate::stats::metrics::NetworkMetrics;
use crate::types::{
    FullPeerInfo, NetworkClientMessages, NetworkInfo, NetworkRequests, NetworkResponses,
    PeerManagerMessageRequest, PeerManagerMessageResponse, PeerMessage, PeerRequest, PeerResponse,
    PeersResponse, RoutingTableUpdate,
};
use actix::{
    Actor, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner, Handler,
    Recipient, Running, StreamHandler, WrapFuture,
};
use futures::task::Poll;
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use futures::FutureExt;
use futures::{future, Stream, StreamExt};
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, Ban, BlockedPorts, Edge, InboundTcpConnect, KnownPeerState,
    KnownPeerStatus, KnownProducer, NetworkConfig, NetworkViewClientMessages,
    NetworkViewClientResponses, OutboundTcpConnect, PeerIdOrHash, PeerInfo, PeerManagerRequest,
    PeerType, Ping, Pong, QueryPeerStats, RawRoutedMessage, ReasonForBan, RoutedMessage,
    RoutedMessageBody, RoutedMessageFrom, StateResponseInfo,
};
use near_network_primitives::types::{EdgeState, PartialEdgeInfo};
use near_performance_metrics::framed_write::FramedWrite;
use near_performance_metrics_macros::perf;
use near_primitives::checked_feature;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time::Clock;
use near_primitives::types::{AccountId, ProtocolVersion};
use near_primitives::utils::from_timestamp;
use near_rate_limiter::{
    ActixMessageResponse, ActixMessageWrapper, ThrottleController, ThrottleFramedRead,
    ThrottleToken,
};
use near_store::Store;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, trace, warn};

/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: Duration = Duration::from_millis(60_000);
/// How much time to wait (in milliseconds) after we send update nonce request before disconnecting.
/// This number should be large to handle pair of nodes with high latency.
const WAIT_ON_TRY_UPDATE_NONCE: Duration = Duration::from_millis(6_000);
/// If we see an edge between us and other peer, but this peer is not a current connection, wait this
/// timeout and in case it didn't become a connected peer, broadcast edge removal update.
const WAIT_PEER_BEFORE_REMOVE: Duration = Duration::from_millis(6_000);
/// Maximum number an edge can increase between oldest known edge and new proposed edge.
const EDGE_NONCE_BUMP_ALLOWED: u64 = 1_000;
/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The maximum waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_MAX_DURATION: Duration = Duration::from_millis(60_000);
/// The initial waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_INITIAL_DURATION: Duration = Duration::from_millis(10);
/// Limit number of pending Peer actors to avoid OOM.
const LIMIT_PENDING_PEERS: usize = 60;
/// How ofter should we broadcast edges.
const BROADCAST_VALIDATED_EDGES_INTERVAL: Duration = Duration::from_millis(50);
/// Maximum amount of time spend processing edges.
const BROAD_CAST_EDGES_MAX_WORK_ALLOWED: Duration = Duration::from_millis(50);
/// Delay syncinc for 1 second to avoid race condition
const WAIT_FOR_SYNC_DELAY: Duration = Duration::from_millis(1_000);
/// How often should we update the routing table
const UPDATE_ROUTING_TABLE_INTERVAL: Duration = Duration::from_millis(1_000);
/// How often to report bandwidth stats.
const REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL: Duration = Duration::from_millis(60_000);

/// Max number of messages we received from peer, and they are in progress, before we start throttling.
/// Disabled for now (TODO PUT UNDER FEATURE FLAG)
const MAX_MESSAGES_COUNT: usize = usize::MAX;
/// Max total size of all messages that are in progress, before we start throttling.
/// Disabled for now (TODO PUT UNDER FEATURE FLAG)
const MAX_MESSAGES_TOTAL_SIZE: usize = usize::MAX;
/// If we received more than `REPORT_BANDWIDTH_THRESHOLD_BYTES` of data from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_BYTES: usize = 10_000_000;
/// If we received more than REPORT_BANDWIDTH_THRESHOLD_COUNT` of messages from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_COUNT: usize = 10_000;

/// Contains information relevant to a connected peer.
struct ConnectedPeer {
    addr: Addr<PeerActor>,
    full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    sent_bytes_per_sec: u64,
    /// Last time requested peers.
    last_time_peer_requested: Instant,
    /// Last time we received a message from this peer.
    last_time_received_message: Instant,
    /// Time where the connection was established.
    connection_established_time: Instant,
    /// Who started connection. Inbound (other) or Outbound (us).
    peer_type: PeerType,
    /// A helper data structure for limiting reading, reporting stats.
    throttle_controller: ThrottleController,
}

#[derive(Default)]
struct AdvHelper {
    #[cfg(feature = "test_features")]
    adv_disable_edge_propagation: bool,
    #[cfg(feature = "test_features")]
    adv_disable_edge_signature_verification: bool,
    #[cfg(feature = "test_features")]
    adv_disable_edge_pruning: bool,
}

impl AdvHelper {
    #[cfg(not(feature = "test_features"))]
    fn can_broadcast_edges(&self) -> bool {
        true
    }

    #[cfg(feature = "test_features")]
    fn can_broadcast_edges(&self) -> bool {
        !self.adv_disable_edge_propagation
    }

    #[cfg(not(feature = "test_features"))]
    fn adv_disable_edge_pruning(&self) -> bool {
        false
    }

    #[cfg(feature = "test_features")]
    fn adv_disable_edge_pruning(&self) -> bool {
        self.adv_disable_edge_pruning
    }
}

// TODO Incoming needs someone to own TcpListener, temporary workaround until there is a better way
pub struct IncomingCrutch {
    listener: tokio_stream::wrappers::TcpListenerStream,
}

impl Stream for IncomingCrutch {
    type Item = std::io::Result<TcpStream>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Stream::poll_next(std::pin::Pin::new(&mut self.listener), cx)
    }
}

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    /// Networking configuration.
    config: NetworkConfig,
    /// Peer information for this node.
    my_peer_id: PeerId,
    /// Address of the client actor.
    client_addr: Recipient<NetworkClientMessages>,
    /// Address of the view client actor.
    view_client_addr: Recipient<NetworkViewClientMessages>,
    /// Peer store that provides read/write access to peers.
    peer_store: PeerStore,
    /// Set of outbound connections that were not consolidated yet.
    outgoing_peers: HashSet<PeerId>,
    /// Connected peers (inbound and outbound) with their full peer information.
    connected_peers: HashMap<PeerId, ConnectedPeer>,
    /// View of the Routing table. It keeps:
    /// - routing information - how to route messages
    /// - edges adjacent to my_peer_id
    /// - account id
    /// Full routing table (that currently includes information about all edges in the graph) is now inside Routing Table.
    routing_table_view: RoutingTableView,
    /// Fields used for communicating with EdgeValidatorActor
    routing_table_exchange_helper: EdgeValidatorHelper,
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,
    /// Connected peers we have sent new edge update, but we haven't received response so far.
    local_peer_pending_update_nonce_request: HashMap<PeerId, u64>,
    /// Dynamic Prometheus metrics
    network_metrics: NetworkMetrics,
    /// RoutingTableActor, responsible for computing routing table, routing table exchange, etc.
    routing_table_addr: Addr<RoutingTableActor>,
    /// Shared counter across all PeerActors, which counts number of `RoutedMessageBody::ForwardTx`
    /// messages sincce last block.
    txns_since_last_block: Arc<AtomicUsize>,
    /// Number of incoming connections, that were not established yet; used for rate limiting.
    pending_incoming_connections_counter: Arc<AtomicUsize>,
    /// Number of connected peers, used for rate limiting.
    peer_counter: Arc<AtomicUsize>,
    /// Used for testing, for disabling features.
    adv_helper: AdvHelper,
}

impl Actor for PeerManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start server if address provided.
        if let Some(server_addr) = self.config.addr {
            // TODO: for now crashes if server didn't start.

            TcpListener::bind(server_addr).into_actor(self).then(
                move |listener, act, ctx| {
                    let listener = listener.unwrap_or_else(|_| panic!("Failed to PeerManagerActor at {}", server_addr));
                    let incoming = IncomingCrutch {
                        listener: tokio_stream::wrappers::TcpListenerStream::new(listener),
                    };
                    info!(target: "stats", peer_id = ?act.my_peer_id, addr = ?server_addr, "Server listening at");
                    let pending_incoming_connections_counter =
                        act.pending_incoming_connections_counter.clone();
                    let peer_counter = act.peer_counter.clone();
                    let max_num_peers: usize = act.config.max_num_peers as usize;

                    ctx.add_message_stream(incoming.filter_map(move |conn| {
                        if let Ok(conn) = conn {
                            if pending_incoming_connections_counter.load(Ordering::SeqCst)
                                + peer_counter.load(Ordering::SeqCst)
                                < max_num_peers + LIMIT_PENDING_PEERS
                            {
                                pending_incoming_connections_counter.fetch_add(1, Ordering::SeqCst);
                                return future::ready(Some(
                                    PeerManagerMessageRequest::InboundTcpConnect(
                                        InboundTcpConnect::new(conn),
                                    ),
                                ));
                            }
                        }

                        future::ready(None)
                    }));
                    actix::fut::ready(())
                },
            ).spawn(ctx);
        }

        // Periodically push network information to client.
        self.push_network_info_trigger(ctx, self.config.push_info_period);

        // Periodically starts peer monitoring.
        let max_interval =
            Duration::min(MONITOR_PEERS_MAX_DURATION, self.config.bootstrap_peers_period);
        debug!(target: "network", ?max_interval, "monitor_peers_trigger");
        self.monitor_peers_trigger(
            ctx,
            MONITOR_PEERS_INITIAL_DURATION,
            (MONITOR_PEERS_INITIAL_DURATION, max_interval),
        );

        // Periodically starts active peer stats querying.
        self.monitor_peer_stats_trigger(ctx, self.config.peer_stats_period);

        // Periodically reads valid edges from `EdgesVerifierActor` and broadcast.
        self.broadcast_validated_edges_trigger(ctx, BROADCAST_VALIDATED_EDGES_INTERVAL);

        // Periodically updates routing table and prune edges that are no longer reachable.
        self.update_routing_table_trigger(ctx, UPDATE_ROUTING_TABLE_INTERVAL);

        // Periodically prints bandwidth stats for each peer.
        self.report_bandwidth_stats_trigger(ctx, REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL);
    }

    /// Try to gracefully disconnect from connected peers.
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        let msg = SendMessage { message: PeerMessage::Disconnect };

        for (_, active_peer) in self.connected_peers.iter() {
            active_peer.addr.do_send(msg.clone());
        }

        actix::spawn(self.routing_table_addr.send(StopMsg {}));

        Running::Stop
    }
}

impl PeerManagerActor {
    pub fn new(
        store: Store,
        config: NetworkConfig,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
        routing_table_addr: Addr<RoutingTableActor>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let peer_store = PeerStore::new(store.clone(), &config.boot_nodes)?;
        debug!(target: "network", len = peer_store.len(), boot_nodes = config.boot_nodes.len(), "Found known peers");
        debug!(target: "network", blacklist = ?config.blacklist, "Blacklist");

        let my_peer_id: PeerId = PeerId::new(config.public_key.clone());
        let routing_table = RoutingTableView::new(store);

        let txns_since_last_block = Arc::new(AtomicUsize::new(0));

        Ok(PeerManagerActor {
            my_peer_id,
            config,
            client_addr,
            view_client_addr,
            peer_store,
            connected_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            routing_table_view: routing_table,
            routing_table_exchange_helper: Default::default(),
            started_connect_attempts: false,
            local_peer_pending_update_nonce_request: HashMap::new(),
            network_metrics: NetworkMetrics::new(),
            routing_table_addr,
            txns_since_last_block,
            pending_incoming_connections_counter: Arc::new(AtomicUsize::new(0)),
            peer_counter: Arc::new(AtomicUsize::new(0)),
            adv_helper: AdvHelper::default(),
        })
    }

    fn update_routing_table_and_prune_edges(
        &mut self,
        ctx: &mut Context<Self>,
        prune: Prune,
        prune_edges_not_reachable_for: Duration,
    ) {
        self.routing_table_addr
            .send(RoutingTableMessages::RoutingTableUpdate { prune, prune_edges_not_reachable_for })
            .into_actor(self)
            .map(|response, act, ctx| match response {
                Ok(RoutingTableMessagesResponse::RoutingTableUpdateResponse {
                    local_edges_to_remove,
                    peer_forwarding,
                    peers_to_ban,
                }) => {
                    act.routing_table_view.remove_local_edges(local_edges_to_remove.iter());
                    act.routing_table_view.peer_forwarding = peer_forwarding;
                    for peer in peers_to_ban {
                        act.ban_peer(ctx, &peer, ReasonForBan::InvalidEdge);
                    }
                }
                _ => error!(target: "network", "expected RoutingTableUpdateResponse"),
            })
            .spawn(ctx);
    }

    fn add_verified_edges_to_routing_table(
        &mut self,
        ctx: &mut Context<Self>,
        edges: Vec<Edge>,
        broadcast_edges: bool,
    ) {
        if edges.is_empty() {
            return;
        }
        // RoutingTable keeps list of local edges; let's apply changes immediately.
        for edge in edges.iter() {
            if let Some(other_peer) = edge.other(&self.my_peer_id) {
                if !self.routing_table_view.is_local_edge_newer(other_peer, edge.nonce()) {
                    continue;
                }
                self.routing_table_view.local_edges_info.insert(other_peer.clone(), edge.clone());
            }
        }

        self.routing_table_addr
            .send(RoutingTableMessages::AddVerifiedEdges { edges })
            .into_actor(self)
            .map(move |response, act, _ctx| match response {
                Ok(RoutingTableMessagesResponse::AddVerifiedEdgesResponse(filtered_edges)) => {
                    // Broadcast new edges to all other peers.
                    if broadcast_edges && act.adv_helper.can_broadcast_edges() {
                        let sync_routing_table = RoutingTableUpdate::from_edges(filtered_edges);
                        Self::broadcast_message(
                            &act.connected_peers,
                            SendMessage {
                                message: PeerMessage::SyncRoutingTable(sync_routing_table),
                            },
                        )
                    }
                }
                _ => error!(target: "network", "expected AddIbfSetResponse"),
            })
            .spawn(ctx);
    }

    fn broadcast_accounts(&mut self, accounts: Vec<AnnounceAccount>) {
        if accounts.is_empty() {
            return;
        }
        debug!(target: "network", account_id = ?self.config.account_id, ?accounts, "Received new accounts");
        for account in accounts.iter() {
            self.routing_table_view.add_account(account.clone());
        }

        Self::broadcast_message(
            &self.connected_peers,
            SendMessage {
                message: PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_accounts(accounts)),
            },
        )
    }

    /// `update_routing_table_trigger` schedule updating routing table to `RoutingTableActor`
    /// Usually we do edge pruning one an hour. However it may be disabled in following cases:
    /// - there are edges, that were supposed to be added, but are still in EdgeValidatorActor,
    ///   waiting to have their signatures checked.
    /// - edge pruning may be disabled for unit testing.
    fn update_routing_table_trigger(&mut self, ctx: &mut Context<Self>, interval: Duration) {
        let can_prune_edges = !self.adv_helper.adv_disable_edge_pruning();

        self.update_routing_table_and_prune_edges(
            ctx,
            if can_prune_edges { Prune::OncePerHour } else { Prune::Disable },
            DELETE_PEERS_AFTER_TIME,
        );

        near_performance_metrics::actix::run_later(ctx, interval, move |act, ctx| {
            act.update_routing_table_trigger(ctx, interval);
        });
    }

    /// Periodically prints bandwidth stats for each peer.
    fn report_bandwidth_stats_trigger(&mut self, ctx: &mut Context<Self>, every: Duration) {
        let mut total_bandwidth_used_by_all_peers: usize = 0;
        let mut total_msg_received_count: usize = 0;
        let mut max_max_record_num_messages_in_progress: usize = 0;
        for (peer_id, active_peer) in self.connected_peers.iter_mut() {
            let bandwidth_used = active_peer.throttle_controller.consume_bandwidth_used();
            let msg_received_count = active_peer.throttle_controller.consume_msg_seen();
            let max_record = active_peer.throttle_controller.consume_max_messages_in_progress();

            if bandwidth_used > REPORT_BANDWIDTH_THRESHOLD_BYTES
                || total_msg_received_count > REPORT_BANDWIDTH_THRESHOLD_COUNT
            {
                warn!(
                    ?peer_id,
                    bandwidth_used, msg_received_count, "Peer bandwidth exceeded threshold",
                );
            }
            total_bandwidth_used_by_all_peers += bandwidth_used;
            total_msg_received_count += msg_received_count;
            max_max_record_num_messages_in_progress =
                max(max_max_record_num_messages_in_progress, max_record);
        }

        info!(
            total_bandwidth_used_by_all_peers,
            total_msg_received_count, max_max_record_num_messages_in_progress, "Bandwidth stats"
        );

        near_performance_metrics::actix::run_later(ctx, every, move |act, ctx| {
            act.report_bandwidth_stats_trigger(ctx, every);
        });
    }

    /// Receives list of edges that were verified, in a trigger every 20ms, and adds them to
    /// the routing table.
    fn broadcast_validated_edges_trigger(
        &mut self,
        ctx: &mut Context<PeerManagerActor>,
        interval: Duration,
    ) {
        let start = Clock::instant();
        let mut new_edges = Vec::new();
        while let Some(edge) = self.routing_table_exchange_helper.edges_to_add_receiver.pop() {
            new_edges.push(edge);
            if start.elapsed() >= BROAD_CAST_EDGES_MAX_WORK_ALLOWED {
                break;
            }
        }

        if !new_edges.is_empty() {
            // Check whenever there is an edge indicating whenever there is a peer we should be
            // connected to but we aren't. And try to resolve the inconsistency.
            //
            // Also check whenever there is an edge indicating that we should be disconnected
            // from a peer, but we are connected. And try to resolve the inconsistency.
            for edge in new_edges.iter() {
                if let Some(other_peer) = edge.other(&self.my_peer_id) {
                    if !self.routing_table_view.is_local_edge_newer(other_peer, edge.nonce()) {
                        continue;
                    }
                    // Check whether we belong to this edge.
                    if self.connected_peers.contains_key(other_peer) {
                        // This is an active connection.
                        if edge.edge_type() == EdgeState::Removed {
                            self.maybe_remove_connected_peer(ctx, edge, other_peer);
                        }
                    } else if edge.edge_type() == EdgeState::Active {
                        // We are not connected to this peer, but routing table contains
                        // information that we do. We should wait and remove that peer
                        // from routing table
                        self.wait_peer_or_remove(ctx, edge.clone());
                    }
                }
            }
            // Add new edge update to the routing table and broadcast it to peers.
            self.add_verified_edges_to_routing_table(ctx, new_edges, true);
        };

        near_performance_metrics::actix::run_later(ctx, interval, move |act, ctx| {
            act.broadcast_validated_edges_trigger(ctx, interval);
        });
    }

    fn is_blacklisted(
        blacklist: &HashMap<std::net::IpAddr, BlockedPorts>,
        addr: &SocketAddr,
    ) -> bool {
        blacklist.get(&addr.ip()).map_or(false, |blocked_ports| match blocked_ports {
            BlockedPorts::All => true,
            BlockedPorts::Some(ports) => ports.contains(&addr.port()),
        })
    }

    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    fn initialize_routing_table_exchange(
        &mut self,
        peer_id: PeerId,
        peer_type: PeerType,
        addr: Addr<PeerActor>,
        ctx: &mut Context<Self>,
        throttle_controller: ThrottleController,
    ) {
        let throttle_controller_clone = throttle_controller.clone();
        near_performance_metrics::actix::run_later(ctx, WAIT_FOR_SYNC_DELAY, move |act, ctx| {
            if peer_type == PeerType::Inbound {
                act.routing_table_addr
                    .send(ActixMessageWrapper::new_without_size(
                        RoutingTableMessages::AddPeerIfMissing(peer_id, None),
                        Some(throttle_controller),
                    ))
                    .into_actor(act)
                    .map(move |response, act, _ctx| match response.map(|x| x.into_inner()) {
                        Ok(RoutingTableMessagesResponse::AddPeerResponse { seed }) => act
                            .start_routing_table_syncv2(
                                addr,
                                seed,
                                Some(throttle_controller_clone),
                            ),
                        _ => error!(target: "network", "expected AddIbfSetResponse"),
                    })
                    .spawn(ctx);
            }
        });
    }

    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    fn start_routing_table_syncv2(
        &self,
        addr: Addr<PeerActor>,
        seed: u64,
        throttle_controller: Option<ThrottleController>,
    ) {
        actix::spawn(
            self.routing_table_addr
                .send(ActixMessageWrapper::new_without_size(
                    RoutingTableMessages::StartRoutingTableSync { seed },
                    throttle_controller,
                ))
                .then(move |response| match response.map(|r| r.into_inner()) {
                    Ok(RoutingTableMessagesResponse::StartRoutingTableSyncResponse(response)) => {
                        let _ = addr.do_send(SendMessage {
                            message: crate::types::PeerMessage::RoutingTableSyncV2(response),
                        });
                        future::ready(())
                    }
                    _ => {
                        error!(target: "network", "expected StartRoutingTableSyncResponse");
                        future::ready(())
                    }
                }),
        );
    }

    /// Register a direct connection to a new peer. This will be called after successfully
    /// establishing a connection with another peer. It become part of the connected peers.
    ///
    /// To build new edge between this pair of nodes both signatures are required.
    /// Signature from this node is passed in `edge_info`
    /// Signature from the other node is passed in `full_peer_info.edge_info`.
    #[allow(clippy::too_many_arguments)]
    fn register_peer(
        &mut self,
        full_peer_info: FullPeerInfo,
        partial_edge_info: PartialEdgeInfo,
        peer_type: PeerType,
        addr: Addr<PeerActor>,
        peer_protocol_version: ProtocolVersion,
        throttle_controller: ThrottleController,
        ctx: &mut Context<Self>,
    ) {
        #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
        let peer_id = full_peer_info.peer_info.id.clone();
        debug!(target: "network", ?full_peer_info, "Consolidated connection");

        if self.outgoing_peers.contains(&full_peer_info.peer_info.id) {
            self.outgoing_peers.remove(&full_peer_info.peer_info.id);
        }
        if let Err(err) = self.peer_store.peer_connected(&full_peer_info.peer_info) {
            error!(target: "network", ?err, "Failed to save peer data");
            return;
        };

        let target_peer_id = full_peer_info.peer_info.id.clone();

        let new_edge = Edge::new(
            self.my_peer_id.clone(),
            target_peer_id.clone(),
            partial_edge_info.nonce,
            partial_edge_info.signature,
            full_peer_info.partial_edge_info.signature.clone(),
        );

        self.connected_peers.insert(
            target_peer_id.clone(),
            ConnectedPeer {
                addr: addr.clone(),
                full_peer_info,
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                last_time_peer_requested: Clock::instant(),
                last_time_received_message: Clock::instant(),
                connection_established_time: Clock::instant(),
                peer_type,
                throttle_controller: throttle_controller.clone(),
            },
        );

        self.add_verified_edges_to_routing_table(ctx, vec![new_edge.clone()], false);

        checked_feature!(
            "protocol_feature_routing_exchange_algorithm",
            RoutingExchangeAlgorithm,
            peer_protocol_version,
            {
                self.initialize_routing_table_exchange(
                    peer_id,
                    peer_type,
                    addr.clone(),
                    ctx,
                    throttle_controller,
                );
                self.send_sync(peer_type, addr, ctx, target_peer_id, new_edge, Vec::new());
            },
            {
                near_performance_metrics::actix::run_later(
                    ctx,
                    WAIT_FOR_SYNC_DELAY,
                    move |act, ctx| {
                        act.routing_table_addr
                            .send(ActixMessageWrapper::new_without_size(
                                RoutingTableMessages::RequestRoutingTable,
                                Some(throttle_controller),
                            ))
                            .into_actor(act)
                            .map(move |response, act, ctx| match response.map(|r| r.into_inner()) {
                                Ok(RoutingTableMessagesResponse::RequestRoutingTableResponse {
                                    edges_info: routing_table,
                                }) => {
                                    act.send_sync(
                                        peer_type,
                                        addr,
                                        ctx,
                                        target_peer_id.clone(),
                                        new_edge,
                                        routing_table,
                                    );
                                }
                                _ => error!(target: "network", "expected AddIbfSetResponse"),
                            })
                            .spawn(ctx);
                    },
                );
            }
        );
    }

    fn send_sync(
        &mut self,
        peer_type: PeerType,
        addr: Addr<PeerActor>,
        ctx: &mut Context<PeerManagerActor>,
        target_peer_id: PeerId,
        new_edge: Edge,
        known_edges: Vec<Edge>,
    ) {
        near_performance_metrics::actix::run_later(ctx, WAIT_FOR_SYNC_DELAY, move |act, _ctx| {
            // Start syncing network point of view. Wait until both parties are connected before start
            // sending messages.
            let known_accounts = act.routing_table_view.get_announce_accounts();
            let _ = addr.do_send(SendMessage {
                message: PeerMessage::SyncRoutingTable(RoutingTableUpdate::new(
                    known_edges,
                    known_accounts,
                )),
            });

            // Ask for peers list on connection.
            let _ = addr.do_send(SendMessage { message: PeerMessage::PeersRequest });
            if let Some(active_peer) = act.connected_peers.get_mut(&target_peer_id) {
                active_peer.last_time_peer_requested = Clock::instant();
            }

            if peer_type == PeerType::Outbound {
                // Only broadcast new message from the outbound endpoint.
                // Wait a time out before broadcasting this new edge to let the other party finish handshake.
                Self::broadcast_message(
                    &act.connected_peers,
                    SendMessage {
                        message: PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_edges(
                            vec![new_edge],
                        )),
                    },
                );
            }
        });
    }

    /// Remove peer from active set.
    /// Check it match peer_type to avoid removing a peer that both started connection to each other.
    /// If peer_type is None, remove anyway disregarding who started the connection.
    fn remove_active_peer(
        &mut self,
        ctx: &mut Context<Self>,
        peer_id: &PeerId,
        peer_type: Option<PeerType>,
    ) {
        if let Some(peer_type) = peer_type {
            if let Some(peer) = self.connected_peers.get(peer_id) {
                if peer.peer_type != peer_type {
                    // Don't remove the peer
                    return;
                }
            }
        }

        // If the last edge we have with this peer represent a connection addition, create the edge
        // update that represents the connection removal.
        self.connected_peers.remove(peer_id);

        #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
        actix::spawn(
            self.routing_table_addr.send(RoutingTableMessages::RemovePeer(peer_id.clone())),
        );

        if let Some(edge) = self.routing_table_view.get_local_edge(peer_id) {
            if edge.edge_type() == EdgeState::Active {
                let edge_update =
                    edge.remove_edge(self.my_peer_id.clone(), &self.config.secret_key);
                self.add_verified_edges_to_routing_table(ctx, vec![edge_update.clone()], false);
                Self::broadcast_message(
                    &self.connected_peers,
                    SendMessage {
                        message: PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_edges(
                            vec![edge_update],
                        )),
                    },
                );
            }
        }
    }

    /// Remove a peer from the connected peer set. If the peer doesn't belong to the connected peer set
    /// data from ongoing connection established is removed.
    fn unregister_peer(
        &mut self,
        ctx: &mut Context<Self>,
        peer_id: PeerId,
        peer_type: PeerType,
        remove_from_peer_store: bool,
    ) {
        debug!(target: "network", ?peer_id, ?peer_type, "Unregister peer");
        // If this is an unconsolidated peer because failed / connected inbound, just delete it.
        if peer_type == PeerType::Outbound && self.outgoing_peers.contains(&peer_id) {
            self.outgoing_peers.remove(&peer_id);
            return;
        }

        if remove_from_peer_store {
            self.remove_active_peer(ctx, &peer_id, Some(peer_type));
            if let Err(err) = self.peer_store.peer_disconnected(&peer_id) {
                error!(target: "network", ?err, "Failed to save peer data");
            };
        }
    }

    /// Add peer to ban list.
    /// This function should only be called after Peer instance is stopped.
    /// Note: Use `try_ban_peer` if there might be a Peer instance still active.
    fn ban_peer(&mut self, ctx: &mut Context<Self>, peer_id: &PeerId, ban_reason: ReasonForBan) {
        warn!(target: "network", ?peer_id, ?ban_reason, "Banning peer");
        self.remove_active_peer(ctx, peer_id, None);
        if let Err(err) = self.peer_store.peer_ban(peer_id, ban_reason) {
            error!(target: "network", ?err, "Failed to save peer data");
        };
    }

    /// Ban peer. Stop peer instance if it is still active,
    /// and then mark peer as banned in the peer store.
    pub(crate) fn try_ban_peer(
        &mut self,
        ctx: &mut Context<Self>,
        peer_id: &PeerId,
        ban_reason: ReasonForBan,
    ) {
        if let Some(peer) = self.connected_peers.get(peer_id) {
            let _ = peer.addr.do_send(PeerManagerRequest::BanPeer(ban_reason));
        } else {
            warn!(target: "network", ?ban_reason, ?peer_id, "Try to ban a disconnected peer for");
            // Call `ban_peer` in peer manager to trigger action that persists information
            // of ban in disk.
            self.ban_peer(ctx, peer_id, ban_reason);
        }
    }

    /// Connects peer with given TcpStream and optional information if it's outbound.
    /// This might fail if the other peers drop listener at its endpoint while establishing connection.
    fn try_connect_peer(
        &mut self,
        recipient: Addr<Self>,
        stream: TcpStream,
        peer_type: PeerType,
        peer_info: Option<PeerInfo>,
        partial_edge_info: Option<PartialEdgeInfo>,
    ) {
        let my_peer_id = self.my_peer_id.clone();
        let account_id = self.config.account_id.clone();
        let server_addr = self.config.addr;
        let handshake_timeout = self.config.handshake_timeout;
        let client_addr = self.client_addr.clone();
        let view_client_addr = self.view_client_addr.clone();

        let server_addr = match server_addr {
            Some(server_addr) => server_addr,
            None => match stream.local_addr() {
                Ok(server_addr) => server_addr,
                _ => {
                    warn!(target: "network", ?peer_info, "Failed establishing connection with");
                    return;
                }
            },
        };

        let remote_addr = match stream.peer_addr() {
            Ok(remote_addr) => remote_addr,
            _ => {
                warn!(target: "network", ?peer_info, "Failed establishing connection with");
                return;
            }
        };

        let network_metrics = self.network_metrics.clone();
        let txns_since_last_block = Arc::clone(&self.txns_since_last_block);

        // Start every peer actor on separate thread.
        let arbiter = Arbiter::new();
        let peer_counter = self.peer_counter.clone();
        peer_counter.fetch_add(1, Ordering::SeqCst);

        PeerActor::start_in_arbiter(&arbiter.handle(), move |ctx| {
            let (read, write) = tokio::io::split(stream);

            // TODO: check if peer is banned or known based on IP address and port.
            let rate_limiter = ThrottleController::new(MAX_MESSAGES_COUNT, MAX_MESSAGES_TOTAL_SIZE);
            PeerActor::add_stream(
                ThrottleFramedRead::new(read, Codec::default(), rate_limiter.clone())
                    .take_while(|x| match x {
                        Ok(_) => future::ready(true),
                        Err(e) => {
                            warn!(target: "network", ?e, "Peer stream error");
                            future::ready(false)
                        }
                    })
                    .map(Result::unwrap),
                ctx,
            );

            PeerActor::new(
                PeerInfo { id: my_peer_id, addr: Some(server_addr), account_id },
                remote_addr,
                peer_info,
                peer_type,
                FramedWrite::new(write, Codec::default(), Codec::default(), ctx),
                handshake_timeout,
                recipient,
                client_addr,
                view_client_addr,
                partial_edge_info,
                network_metrics,
                txns_since_last_block,
                peer_counter,
                rate_limiter,
            )
        });
    }

    /// Check if it is needed to create a new outbound connection.
    /// If the number of active connections is less than `ideal_connections_lo` or
    /// (the number of outgoing connections is less than `minimum_outbound_peers`
    ///     and the total connections is less than `max_num_peers`)
    fn is_outbound_bootstrap_needed(&self) -> bool {
        let total_connections = self.connected_peers.len() + self.outgoing_peers.len();
        let potential_outgoing_connections = (self.connected_peers.values())
            .filter(|connected_peer| connected_peer.peer_type == PeerType::Outbound)
            .count()
            + self.outgoing_peers.len();

        (total_connections < self.config.ideal_connections_lo as usize
            || (total_connections < self.config.max_num_peers as usize
                && potential_outgoing_connections < self.config.minimum_outbound_peers as usize))
            && !self.config.outbound_disabled
    }

    fn is_inbound_allowed(&self) -> bool {
        self.connected_peers.len() + self.outgoing_peers.len() < self.config.max_num_peers as usize
    }

    /// Returns single random peer with close to the highest height
    fn highest_height_peers(&self) -> Vec<FullPeerInfo> {
        // This finds max height among peers, and returns one peer close to such height.
        let max_height = match (self.connected_peers.values())
            .map(|connected_peer| connected_peer.full_peer_info.chain_info.height)
            .max()
        {
            Some(height) => height,
            None => return vec![],
        };
        // Find all peers whose height is within `highest_peer_horizon` from max height peer(s).
        self.connected_peers
            .values()
            .filter(|active_peer| {
                active_peer
                    .full_peer_info
                    .chain_info
                    .height
                    .saturating_add(self.config.highest_peer_horizon)
                    >= max_height
            })
            .map(|active_peer| active_peer.full_peer_info.clone())
            .collect::<Vec<_>>()
    }

    /// Returns bytes sent/received across all peers.
    fn get_total_bytes_per_sec(&self) -> (u64, u64) {
        let sent_bps = self.connected_peers.values().map(|x| x.sent_bytes_per_sec).sum();
        let received_bps = self.connected_peers.values().map(|x| x.received_bytes_per_sec).sum();
        (sent_bps, received_bps)
    }

    /// Get a random peer we are not connected to from the known list.
    fn sample_random_peer(&self, ignore_fn: impl Fn(&KnownPeerState) -> bool) -> Option<PeerInfo> {
        self.peer_store.unconnected_peer(ignore_fn)
    }

    /// Query current peers for more peers.
    fn query_active_peers_for_more_peers(&mut self) {
        let mut requests = futures::stream::FuturesUnordered::new();
        let msg = SendMessage { message: PeerMessage::PeersRequest };
        for (_, active_peer) in self.connected_peers.iter_mut() {
            if active_peer.last_time_peer_requested.elapsed() > REQUEST_PEERS_INTERVAL {
                active_peer.last_time_peer_requested = Clock::instant();
                requests.push(active_peer.addr.send(msg.clone()));
            }
        }
        actix::spawn(async move {
            while let Some(response) = requests.next().await {
                if let Err(e) = response {
                    debug!(target: "network", ?e, "Failed sending broadcast message(query_active_peers)");
                }
            }
        });
    }

    #[cfg(all(feature = "test_features", feature = "protocol_feature_routing_exchange_algorithm"))]
    fn adv_remove_edges_from_routing_table(
        &mut self,
        edges: Vec<near_network_primitives::types::SimpleEdge>,
    ) {
        // Create fake edges with no signature for unit test purposes
        let edges: Vec<Edge> = edges
            .iter()
            .map(|se| {
                Edge::new(
                    se.key().0.clone(),
                    se.key().1.clone(),
                    se.nonce(),
                    near_crypto::Signature::default(),
                    near_crypto::Signature::default(),
                )
            })
            .collect();
        self.routing_table_view
            .remove_local_edges(edges.iter().filter_map(|e| e.other(&self.my_peer_id)));
        self.routing_table_addr.do_send(RoutingTableMessages::AdvRemoveEdges(edges));
    }

    fn wait_peer_or_remove(&mut self, ctx: &mut Context<Self>, edge: Edge) {
        // This edge says this is an connected peer, which is currently not in the set of connected peers.
        // Wait for some time to let the connection begin or broadcast edge removal instead.

        near_performance_metrics::actix::run_later(
            ctx,
            WAIT_PEER_BEFORE_REMOVE,
            move |act, _ctx| {
                let other = edge.other(&act.my_peer_id).unwrap();
                if !act.connected_peers.contains_key(other) {
                    // Peer is still not active after waiting a timeout.
                    let new_edge = edge.remove_edge(act.my_peer_id.clone(), &act.config.secret_key);
                    Self::broadcast_message(
                        &act.connected_peers,
                        SendMessage {
                            message: PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_edges(
                                vec![new_edge],
                            )),
                        },
                    );
                }
            },
        );
    }

    // If we receive an edge indicating that we should no longer be connected to a peer.
    // We will broadcast that edge to that peer, and if that peer doesn't reply within specific time,
    // that peer will be removed. However, the connected peer may gives us a new edge indicating
    // that we should in fact be connected to it.
    fn maybe_remove_connected_peer(
        &mut self,
        ctx: &mut Context<Self>,
        edge: &Edge,
        other: &PeerId,
    ) {
        let nonce = edge.next();

        if let Some(last_nonce) = self.local_peer_pending_update_nonce_request.get(other) {
            if *last_nonce >= nonce {
                // We already tried to update an edge with equal or higher nonce.
                return;
            }
        }

        Self::send_message(
            &self.connected_peers,
            other.clone(),
            PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
                &self.my_peer_id,
                other,
                nonce,
                &self.config.secret_key,
            )),
        );

        self.local_peer_pending_update_nonce_request.insert(other.clone(), nonce);

        let other = other.clone();
        near_performance_metrics::actix::run_later(
            ctx,
            WAIT_ON_TRY_UPDATE_NONCE,
            move |act, _ctx| {
                if let Some(cur_nonce) = act.local_peer_pending_update_nonce_request.get(&other) {
                    if *cur_nonce == nonce {
                        if let Some(peer) = act.connected_peers.get(&other) {
                            // Send disconnect signal to this peer if we haven't edge update.
                            peer.addr.do_send(PeerManagerRequest::UnregisterPeer);
                        }
                        act.local_peer_pending_update_nonce_request.remove(&other);
                    }
                }
            },
        );
    }

    /// Periodically query peer actors for latest weight and traffic info.
    fn monitor_peer_stats_trigger(&mut self, ctx: &mut Context<Self>, interval: Duration) {
        for (peer_id, active_peer) in self.connected_peers.iter() {
            let peer_id1 = peer_id.clone();
            active_peer
                .addr
                .send(QueryPeerStats {})
                .into_actor(self)
                .map(|result, _, _| result.map_err(|err|
                    error!(target: "network", ?err, "Failed sending message(monitor_peer_stats)")))
                .map(move |res, act, _| {
                    let _ignore = res.map(|res| {
                        if res.is_abusive {
                            trace!(target: "network", ?peer_id1, sent = res.message_counts.0, recv = res.message_counts.1, "Banning peer for abuse");
                            // TODO(MarX, #1586): Ban peer if we found them abusive. Fix issue with heavy
                            //  network traffic that flags honest peers.
                            // Send ban signal to peer instance. It should send ban signal back and stop the instance.
                            // if let Some(active_peer) = act.connected_peers.get(&peer_id1) {
                            //     active_peer.addr.do_send(PeerManagerRequest::BanPeer(ReasonForBan::Abusive));
                            // }
                        } else if let Some(active_peer) = act.connected_peers.get_mut(&peer_id1) {
                            active_peer.full_peer_info.chain_info = res.chain_info;
                            active_peer.sent_bytes_per_sec = res.sent_bytes_per_sec;
                            active_peer.received_bytes_per_sec = res.received_bytes_per_sec;
                        }
                    });
                })
                .spawn(ctx);
        }

        near_performance_metrics::actix::run_later(ctx, interval, move |act, ctx| {
            act.monitor_peer_stats_trigger(ctx, interval);
        });
    }

    /// Select one peer and send signal to stop connection to it gracefully.
    /// Selection process:
    ///     Create a safe set of peers, and among the remaining peers select one at random.
    ///     If the number of outbound connections is less or equal than minimum_outbound_connections,
    ///         add all outbound connections to the safe set.
    ///     While the length of the safe set is less than safe_set_size:
    ///         Among all the peers we have received a message within the last peer_recent_time_window,
    ///             find the one we connected earlier and add it to the safe set.
    ///         else break
    fn try_stop_active_connection(&self) {
        debug!(target: "network",
            connected_peers_len = self.connected_peers.len(),
            ideal_connections_hi = self.config.ideal_connections_hi,
            "Trying to stop an active connection.",
        );

        // Build safe set
        let mut safe_set = HashSet::new();

        if (self.connected_peers.values())
            .filter(|active_peer| active_peer.peer_type == PeerType::Outbound)
            .count()
            + self.outgoing_peers.len()
            <= self.config.minimum_outbound_peers as usize
        {
            for (peer, active) in self.connected_peers.iter() {
                if active.peer_type == PeerType::Outbound {
                    safe_set.insert(peer);
                }
            }
        }

        if self.config.archive
            && (self.connected_peers.values())
                .filter(|connected_peer| connected_peer.full_peer_info.chain_info.archival)
                .count()
                <= self.config.archival_peer_connections_lower_bound as usize
        {
            for (peer, active) in self.connected_peers.iter() {
                if active.full_peer_info.chain_info.archival {
                    safe_set.insert(peer);
                }
            }
        }

        // Find all recent connections
        let mut recent_connections = (self.connected_peers.iter())
            .filter_map(|(peer_id, active)| {
                if active.last_time_received_message.elapsed() < self.config.peer_recent_time_window
                {
                    Some((peer_id.clone(), active.connection_established_time))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // Sort by established time
        recent_connections.sort_by(|(_, established_time_a), (_, established_time_b)| {
            established_time_a.cmp(established_time_b)
        });

        // Take remaining peers
        for (peer_id, _) in recent_connections
            .iter()
            .take((self.config.safe_set_size as usize).saturating_sub(safe_set.len()))
        {
            safe_set.insert(peer_id);
        }

        // Build valid candidate list to choose the peer to be removed. All peers outside the safe set.
        let candidates = self.connected_peers.keys().filter_map(|peer_id| {
            if safe_set.contains(peer_id) {
                None
            } else {
                Some(peer_id)
            }
        });

        if let Some(peer_id) = candidates.choose(&mut rand::thread_rng()) {
            if let Some(active_peer) = self.connected_peers.get(peer_id) {
                debug!(target: "network", ?peer_id, "Stop active connection");
                active_peer.addr.do_send(PeerManagerRequest::UnregisterPeer);
            }
        }
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - unban peers that have been banned for awhile,
    ///  - remove expired peers,
    ///
    /// # Arguments:
    /// - `interval` - Time between consequent runs.
    /// - `default_interval` - we will set `interval` to this value once, after first successful connection
    /// - `max_interval` - maximum value of interval
    /// NOTE: in the current implementation `interval` increases by 1% every time, and it will
    ///       reach value of `max_internal` eventually.
    fn monitor_peers_trigger(
        &mut self,
        ctx: &mut Context<Self>,
        mut interval: Duration,
        (default_interval, max_interval): (Duration, Duration),
    ) {
        let mut to_unban = vec![];
        for (peer_id, peer_state) in self.peer_store.iter() {
            if let KnownPeerStatus::Banned(_, last_banned) = peer_state.status {
                let interval =
                    (Clock::utc() - from_timestamp(last_banned)).to_std().unwrap_or_default();
                if interval > self.config.ban_window {
                    info!(target: "network", unbanned = ?peer_id, after = ?interval, "Monitor peers:");
                    to_unban.push(peer_id.clone());
                }
            }
        }

        for peer_id in to_unban {
            if let Err(err) = self.peer_store.peer_unban(&peer_id) {
                error!(target: "network", ?err, "Failed to unban a peer");
                // TODO: Do we really want to return?
                // Doesn't this stop the trigger?
                return;
            }
        }

        if self.is_outbound_bootstrap_needed() {
            if let Some(peer_info) = self.sample_random_peer(|peer_state| {
                // Ignore connecting to ourself
                self.my_peer_id == peer_state.peer_info.id
                    || self.config.addr == peer_state.peer_info.addr
                    // Or to peers we are currently trying to connect to
                    || self.outgoing_peers.contains(&peer_state.peer_info.id)
            }) {
                // Start monitor_peers_attempts from start after we discover the first healthy peer
                if !self.started_connect_attempts {
                    self.started_connect_attempts = true;
                    interval = default_interval;
                }

                self.outgoing_peers.insert(peer_info.id.clone());
                ctx.notify(PeerManagerMessageRequest::OutboundTcpConnect(OutboundTcpConnect {
                    peer_info,
                }));
            } else {
                self.query_active_peers_for_more_peers();
            }
        }

        // If there are too many active connections try to remove some connections
        if self.connected_peers.len() > self.config.ideal_connections_hi as usize {
            self.try_stop_active_connection();
        }

        if let Err(err) = self.peer_store.remove_expired(&self.config) {
            error!(target: "network", ?err, "Failed to remove expired peers");
            // TODO: Do we really want to return?
            return;
        };

        let new_interval = min(
            max_interval,
            Duration::from_nanos((interval.as_nanos() as f64 * EXPONENTIAL_BACKOFF_RATIO) as u64),
        );

        near_performance_metrics::actix::run_later(ctx, interval, move |act, ctx| {
            act.monitor_peers_trigger(ctx, new_interval, (default_interval, max_interval));
        });
    }

    /// Sends list of edges, from peer `peer_id` to check their signatures to `EdgeValidatorActor`.
    /// Bans peer `peer_id` if an invalid edge is found.
    /// `PeerManagerActor` periodically runs `broadcast_validated_edges_trigger`, which gets edges
    /// from `EdgeValidatorActor` concurrent queue and sends edges to be added to `RoutingTableActor`.
    fn validate_edges_and_add_to_routing_table(
        &mut self,
        peer_id: PeerId,
        edges: Vec<Edge>,
        throttle_controller: Option<ThrottleController>,
    ) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_addr.do_send(ActixMessageWrapper::new_without_size(
            RoutingTableMessages::ValidateEdgeList(ValidateEdgeList {
                source_peer_id: peer_id,
                edges,
                edges_info_shared: self.routing_table_exchange_helper.edges_info_shared.clone(),
                sender: self.routing_table_exchange_helper.edges_to_add_sender.clone(),
                #[cfg(feature = "test_features")]
                adv_disable_edge_signature_verification: self
                    .adv_helper
                    .adv_disable_edge_signature_verification,
            }),
            throttle_controller,
        ));
    }

    /// Broadcast message to all active peers.
    fn broadcast_message(connected_peers: &HashMap<PeerId, ConnectedPeer>, msg: SendMessage) {
        // TODO(MarX, #1363): Implement smart broadcasting. (MST)

        // Change message to reference counted to allow sharing with all actors
        // without cloning.
        let msg = Arc::new(msg);
        let mut requests: futures::stream::FuturesUnordered<_> =
            connected_peers.values().map(|peer| peer.addr.send(Arc::clone(&msg))).collect();

        actix::spawn(async move {
            while let Some(response) = requests.next().await {
                if let Err(e) = response {
                    debug!(target: "network", ?e, "Failed sending broadcast message(broadcast_message):");
                }
            }
        });
    }

    fn announce_account(&mut self, announce_account: AnnounceAccount) {
        debug!(target: "network", account_id = ?self.config.account_id, ?announce_account, "Account announce");
        if !self.routing_table_view.contains_account(&announce_account) {
            self.routing_table_view.add_account(announce_account.clone());
            Self::broadcast_message(
                &self.connected_peers,
                SendMessage {
                    message: PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_accounts(
                        vec![announce_account],
                    )),
                },
            );
        }
    }

    /// Send message to peer that belong to our active set
    /// Return whether the message is sent or not.
    fn send_message(
        connected_peers: &HashMap<PeerId, ConnectedPeer>,
        peer_id: PeerId,
        message: PeerMessage,
    ) -> bool {
        if let Some(connected_peer) = connected_peers.get(&peer_id) {
            let msg_kind = message.msg_variant().to_string();
            trace!(target: "network", ?msg_kind, "Send message");
            actix::spawn(connected_peer.addr.send(SendMessage { message }));
            true
        } else {
            debug!(target: "network",
                   to = ?peer_id,
                   num_connected_peers = connected_peers.len(),
                   ?message,
                   "Failed sending message"
            );
            false
        }
    }

    /// Return whether the message is sent or not.
    fn send_message_to_account_or_peer_or_hash(
        &mut self,
        target: &AccountOrPeerIdOrHash,
        msg: RoutedMessageBody,
    ) -> bool {
        match target {
            AccountOrPeerIdOrHash::AccountId(account_id) => {
                self.send_message_to_account(account_id, msg)
            }
            peer_or_hash @ AccountOrPeerIdOrHash::PeerId(_)
            | peer_or_hash @ AccountOrPeerIdOrHash::Hash(_) => self
                .send_message_to_peer(RawRoutedMessage { target: peer_or_hash.clone(), body: msg }),
        }
    }

    /// Route signed message to target peer.
    /// Return whether the message is sent or not.
    fn send_signed_message_to_peer(&mut self, msg: RoutedMessage) -> bool {
        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = &msg.target {
            if target == &self.my_peer_id {
                debug!(target: "network", account_id = ?self.config.account_id, my_peer_id = ?self.my_peer_id, ?msg, "Drop signed message to myself");
                return false;
            }
        }

        match self.routing_table_view.find_route(&msg.target) {
            Ok(peer_id) => {
                // Remember if we expect a response for this message.
                if msg.author == self.my_peer_id && msg.expect_response() {
                    trace!(target: "network", ?msg, "initiate route back");
                    self.routing_table_view.add_route_back(msg.hash(), self.my_peer_id.clone());
                }

                Self::send_message(&self.connected_peers, peer_id, PeerMessage::Routed(msg))
            }
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                self.network_metrics.inc(
                    NetworkMetrics::peer_message_dropped(strum::AsStaticRef::as_static(&msg.body))
                        .as_str(),
                );

                debug!(target: "network",
                      account_id = ?self.config.account_id,
                      to = ?msg.target,
                      reason = ?find_route_error,
                      known_peers = ?self.routing_table_view.peer_forwarding.len(),
                      msg = ?msg.body,
                    "Drop signed message"
                );
                false
            }
        }
    }

    /// Route message to target peer.
    /// Return whether the message is sent or not.
    fn send_message_to_peer(&mut self, msg: RawRoutedMessage) -> bool {
        let msg = self.sign_routed_message(msg, self.my_peer_id.clone());
        self.send_signed_message_to_peer(msg)
    }

    /// Send message to specific account.
    /// Return whether the message is sent or not.
    fn send_message_to_account(&mut self, account_id: &AccountId, msg: RoutedMessageBody) -> bool {
        let target = match self.routing_table_view.account_owner(account_id) {
            Ok(peer_id) => peer_id,
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                metrics::DROP_MESSAGE_UNKNOWN_ACCOUNT.inc();
                debug!(target: "network",
                       account_id = ?self.config.account_id,
                       to = ?account_id,
                       reason = ?find_route_error,
                       ?msg,"Drop message",
                );
                trace!(target: "network", known_peers = ?self.routing_table_view.get_accounts_keys());
                return false;
            }
        };

        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body: msg };
        self.send_message_to_peer(msg)
    }

    fn sign_routed_message(&self, msg: RawRoutedMessage, my_peer_id: PeerId) -> RoutedMessage {
        msg.sign(my_peer_id, &self.config.secret_key, self.config.routed_message_ttl)
    }

    // Determine if the given target is referring to us.
    fn message_for_me(
        routing_table_view: &mut RoutingTableView,
        my_peer_id: &PeerId,
        target: &PeerIdOrHash,
    ) -> bool {
        match target {
            PeerIdOrHash::PeerId(peer_id) => peer_id == my_peer_id,
            PeerIdOrHash::Hash(hash) => routing_table_view.compare_route_back(*hash, my_peer_id),
        }
    }

    fn propose_edge(&self, peer1: &PeerId, with_nonce: Option<u64>) -> PartialEdgeInfo {
        // When we create a new edge we increase the latest nonce by 2 in case we miss a removal
        // proposal from our partner.
        let nonce = with_nonce.unwrap_or_else(|| {
            self.routing_table_view.get_local_edge(peer1).map_or(1, |edge| edge.next())
        });

        PartialEdgeInfo::new(&self.my_peer_id, peer1, nonce, &self.config.secret_key)
    }

    // Ping pong useful functions.

    // for unit tests
    fn send_ping(&mut self, nonce: usize, target: PeerId) {
        let body =
            RoutedMessageBody::Ping(Ping { nonce: nonce as u64, source: self.my_peer_id.clone() });
        self.routing_table_view.sending_ping(nonce, target.clone());
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(msg);
    }

    fn send_pong(&mut self, nonce: usize, target: CryptoHash) {
        let body =
            RoutedMessageBody::Pong(Pong { nonce: nonce as u64, source: self.my_peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(msg);
    }

    fn handle_ping(&mut self, ping: Ping, hash: CryptoHash) {
        self.send_pong(ping.nonce as usize, hash);
        self.routing_table_view.add_ping(ping);
    }

    /// Handle pong messages. Add pong temporary to the routing table, mostly used for testing.
    fn handle_pong(&mut self, pong: Pong) {
        self.routing_table_view.add_pong(pong);
    }

    pub(crate) fn get_network_info(&mut self) -> NetworkInfo {
        let (sent_bytes_per_sec, received_bytes_per_sec) = self.get_total_bytes_per_sec();
        NetworkInfo {
            connected_peers: self
                .connected_peers
                .values()
                .map(|a| a.full_peer_info.clone())
                .collect::<Vec<_>>(),
            num_connected_peers: self.connected_peers.len(),
            peer_max_count: self.config.max_num_peers,
            highest_height_peers: self.highest_height_peers(),
            sent_bytes_per_sec,
            received_bytes_per_sec,
            known_producers: self
                .routing_table_view
                .get_announce_accounts()
                .iter()
                .map(|announce_account| KnownProducer {
                    account_id: announce_account.account_id.clone(),
                    peer_id: announce_account.peer_id.clone(),
                    // TODO: fill in the address.
                    addr: None,
                })
                .collect(),
            peer_counter: self.peer_counter.load(Ordering::SeqCst),
        }
    }

    fn push_network_info_trigger(&mut self, ctx: &mut Context<Self>, interval: Duration) {
        let network_info = self.get_network_info();

        let _ = self.client_addr.do_send(NetworkClientMessages::NetworkInfo(network_info));

        near_performance_metrics::actix::run_later(ctx, interval, move |act, ctx| {
            act.push_network_info_trigger(ctx, interval);
        });
    }

    #[perf]
    fn handle_msg_network_requests(
        &mut self,
        msg: NetworkRequests,
        ctx: &mut Context<Self>,
        throttle_controller: Option<ThrottleController>,
    ) -> NetworkResponses {
        #[cfg(feature = "delay_detector")]
        let _d =
            delay_detector::DelayDetector::new(format!("network request {}", msg.as_ref()).into());
        match msg {
            NetworkRequests::Block { block } => {
                Self::broadcast_message(
                    &self.connected_peers,
                    SendMessage { message: PeerMessage::Block(block) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::Approval { approval_message } => {
                self.send_message_to_account(
                    &approval_message.target,
                    RoutedMessageBody::BlockApproval(approval_message.approval),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                if Self::send_message(
                    &self.connected_peers,
                    peer_id,
                    PeerMessage::BlockRequest(hash),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if Self::send_message(
                    &self.connected_peers,
                    peer_id,
                    PeerMessage::BlockHeadersRequest(hashes),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, target } => {
                if self.send_message_to_account_or_peer_or_hash(
                    &target,
                    RoutedMessageBody::StateRequestHeader(shard_id, sync_hash),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateRequestPart { shard_id, sync_hash, part_id, target } => {
                if self.send_message_to_account_or_peer_or_hash(
                    &target,
                    RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateResponse { route_back, response } => {
                let body = match response {
                    StateResponseInfo::V1(response) => RoutedMessageBody::StateResponse(response),
                    response @ StateResponseInfo::V2(_) => {
                        RoutedMessageBody::VersionedStateResponse(response)
                    }
                };
                if self.send_message_to_peer(RawRoutedMessage {
                    target: AccountOrPeerIdOrHash::Hash(route_back),
                    body,
                }) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::EpochSyncRequest { peer_id, epoch_id } => {
                if Self::send_message(
                    &self.connected_peers,
                    peer_id,
                    PeerMessage::EpochSyncRequest(epoch_id),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::EpochSyncFinalizationRequest { peer_id, epoch_id } => {
                if Self::send_message(
                    &self.connected_peers,
                    peer_id,
                    PeerMessage::EpochSyncFinalizationRequest(epoch_id),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                self.try_ban_peer(ctx, &peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                self.announce_account(announce_account);
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedChunkRequest { target, request } => {
                let mut success = false;

                // Make two attempts to send the message. First following the preference of `prefer_peer`,
                // and if it fails, against the preference.
                for prefer_peer in &[target.prefer_peer, !target.prefer_peer] {
                    if !prefer_peer {
                        if let Some(account_id) = target.account_id.as_ref() {
                            if self.send_message_to_account(
                                account_id,
                                RoutedMessageBody::PartialEncodedChunkRequest(request.clone()),
                            ) {
                                success = true;
                                break;
                            }
                        }
                    } else {
                        let mut matching_peers = vec![];
                        for (peer_id, active_peer) in self.connected_peers.iter() {
                            if (active_peer.full_peer_info.chain_info.archival
                                || !target.only_archival)
                                && active_peer.full_peer_info.chain_info.height >= target.min_height
                                && active_peer
                                    .full_peer_info
                                    .chain_info
                                    .tracked_shards
                                    .contains(&target.shard_id)
                            {
                                matching_peers.push(peer_id.clone());
                            }
                        }

                        if let Some(matching_peer) = matching_peers.iter().choose(&mut thread_rng())
                        {
                            if self.send_message_to_peer(RawRoutedMessage {
                                target: AccountOrPeerIdOrHash::PeerId(matching_peer.clone()),
                                body: RoutedMessageBody::PartialEncodedChunkRequest(
                                    request.clone(),
                                ),
                            }) {
                                success = true;
                                break;
                            }
                        }
                    }
                }

                if success {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                if self.send_message_to_peer(RawRoutedMessage {
                    target: AccountOrPeerIdOrHash::Hash(route_back),
                    body: RoutedMessageBody::PartialEncodedChunkResponse(response),
                }) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                if self.send_message_to_account(&account_id, partial_encoded_chunk.into()) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                if self.send_message_to_account(
                    &account_id,
                    RoutedMessageBody::PartialEncodedChunkForward(forward),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ForwardTx(account_id, tx) => {
                if self.send_message_to_account(&account_id, RoutedMessageBody::ForwardTx(tx)) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::TxStatus(account_id, signer_account_id, tx_hash) => {
                if self.send_message_to_account(
                    &account_id,
                    RoutedMessageBody::TxStatusRequest(signer_account_id, tx_hash),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::Query { query_id, account_id, block_reference, request } => {
                if self.send_message_to_account(
                    &account_id,
                    RoutedMessageBody::QueryRequest { query_id, block_reference, request },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ReceiptOutComeRequest(account_id, receipt_id) => {
                if self.send_message_to_account(
                    &account_id,
                    RoutedMessageBody::ReceiptOutcomeRequest(receipt_id),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            // For unit tests
            NetworkRequests::FetchRoutingTable => {
                NetworkResponses::RoutingTableInfo(self.routing_table_view.info())
            }
            NetworkRequests::SyncRoutingTable { peer_id, routing_table_update } => {
                // Process edges and add new edges to the routing table. Also broadcast new edges.
                let edges = routing_table_update.edges;
                let accounts = routing_table_update.accounts;

                // Filter known accounts before validating them.
                let accounts = accounts
                    .into_iter()
                    .filter_map(|announce_account| {
                        if let Some(current_announce_account) =
                            self.routing_table_view.get_announce(&announce_account.account_id)
                        {
                            if announce_account.epoch_id == current_announce_account.epoch_id {
                                None
                            } else {
                                Some((announce_account, Some(current_announce_account.epoch_id)))
                            }
                        } else {
                            Some((announce_account, None))
                        }
                    })
                    .collect();

                // Ask client to validate accounts before accepting them.
                let peer_id_clone = peer_id.clone();
                self.view_client_addr
                    .send(NetworkViewClientMessages::AnnounceAccount(accounts))
                    .into_actor(self)
                    .then(move |response, act, ctx| {
                        match response {
                            Ok(NetworkViewClientResponses::Ban { ban_reason }) => {
                                act.try_ban_peer(ctx, &peer_id_clone, ban_reason);
                            }
                            Ok(NetworkViewClientResponses::AnnounceAccount(accounts)) => {
                                act.broadcast_accounts(accounts);
                            }
                            _ => {
                                debug!(target: "network", "Received invalid account confirmation from client.");
                            }
                        }
                        actix::fut::ready(())
                    }).spawn(ctx);

                self.validate_edges_and_add_to_routing_table(peer_id, edges, throttle_controller);

                NetworkResponses::NoResponse
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            NetworkRequests::IbfMessage { peer_id, ibf_msg } => match ibf_msg {
                crate::network_protocol::RoutingSyncV2::Version2(ibf_msg) => {
                    if let Some(addr) = self.connected_peers.get(&peer_id).map(|p| p.addr.clone()) {
                        self.process_ibf_msg(&peer_id, ibf_msg, addr, throttle_controller)
                    }
                    NetworkResponses::NoResponse
                }
            },
            NetworkRequests::Challenge(challenge) => {
                // TODO(illia): smarter routing?
                Self::broadcast_message(
                    &self.connected_peers,
                    SendMessage { message: PeerMessage::Challenge(challenge) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::RequestUpdateNonce(peer_id, edge_info) => {
                if Edge::partial_verify(&self.my_peer_id, &peer_id, &edge_info) {
                    if let Some(cur_edge) = self.routing_table_view.get_local_edge(&peer_id) {
                        if cur_edge.edge_type() == EdgeState::Active
                            && cur_edge.nonce() >= edge_info.nonce
                        {
                            return NetworkResponses::EdgeUpdate(Box::new(cur_edge.clone()));
                        }
                    }

                    let new_edge = Edge::build_with_secret_key(
                        self.my_peer_id.clone(),
                        peer_id,
                        edge_info.nonce,
                        &self.config.secret_key,
                        edge_info.signature,
                    );

                    self.add_verified_edges_to_routing_table(ctx, vec![new_edge.clone()], false);
                    NetworkResponses::EdgeUpdate(Box::new(new_edge))
                } else {
                    NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            NetworkRequests::ResponseUpdateNonce(edge) => {
                if let Some(other_peer) = edge.other(&self.my_peer_id) {
                    if edge.verify() {
                        // This happens in case, we get an edge, in `RoutingTableActor`,
                        // which says that we shouldn't be connected to local peer, but we are.
                        // This is a part of logic used to ask peer, if he really want to be disconnected.
                        if self.routing_table_view.is_local_edge_newer(other_peer, edge.nonce()) {
                            if let Some(nonce) =
                                self.local_peer_pending_update_nonce_request.get(other_peer)
                            {
                                if edge.nonce() >= *nonce {
                                    // This means that, `other_peer` responded that we should keep
                                    // the connection that that peer. Therefore, we are
                                    // cleaning up this data structure.
                                    self.local_peer_pending_update_nonce_request.remove(other_peer);
                                }
                            }
                        }
                        self.add_verified_edges_to_routing_table(ctx, vec![edge.clone()], false);
                        NetworkResponses::NoResponse
                    } else {
                        NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                    }
                } else {
                    NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            // For unit tests
            NetworkRequests::PingTo(nonce, target) => {
                self.send_ping(nonce, target);
                NetworkResponses::NoResponse
            }
            // For unit tests
            NetworkRequests::FetchPingPongInfo => {
                let (pings, pongs) = self.routing_table_view.fetch_ping_pong();
                NetworkResponses::PingPongInfo {
                    pings: pings.map(|(k, v)| (*k, v.clone())).collect(),
                    pongs: pongs.map(|(k, v)| (*k, v.clone())).collect(),
                }
            }
        }
    }

    #[cfg(all(feature = "test_features", feature = "protocol_feature_routing_exchange_algorithm"))]
    #[perf]
    fn handle_msg_start_routing_table_sync(
        &mut self,
        msg: crate::private_actix::StartRoutingTableSync,
        ctx: &mut Context<Self>,
        throttle_controller: Option<ThrottleController>,
    ) {
        if let Some(active_peer) = self.connected_peers.get(&msg.peer_id) {
            let addr = active_peer.addr.clone();
            self.initialize_routing_table_exchange(
                msg.peer_id,
                PeerType::Inbound,
                addr,
                ctx,
                throttle_controller.unwrap(),
            );
        }
    }

    #[cfg(feature = "test_features")]
    #[perf]
    fn handle_msg_set_adv_options(&mut self, msg: crate::test_utils::SetAdvOptions) {
        if let Some(disable_edge_propagation) = msg.disable_edge_propagation {
            self.adv_helper.adv_disable_edge_propagation = disable_edge_propagation;
        }
        if let Some(disable_edge_signature_verification) = msg.disable_edge_signature_verification {
            self.adv_helper.adv_disable_edge_signature_verification =
                disable_edge_signature_verification;
        }
        if let Some(disable_edge_pruning) = msg.disable_edge_pruning {
            self.adv_helper.adv_disable_edge_pruning = disable_edge_pruning;
        }
        if let Some(set_max_peers) = msg.set_max_peers {
            self.config.max_num_peers = set_max_peers as u32;
        }
    }

    #[cfg(all(feature = "test_features", feature = "protocol_feature_routing_exchange_algorithm"))]
    #[perf]
    fn handle_msg_set_routing_table(
        &mut self,
        msg: crate::test_utils::SetRoutingTable,
        ctx: &mut Context<Self>,
    ) {
        if let Some(add_edges) = msg.add_edges {
            debug!(target: "network", len = add_edges.len(), "test_features add_edges");
            self.add_verified_edges_to_routing_table(ctx, add_edges, false);
        }
        if let Some(remove_edges) = msg.remove_edges {
            debug!(target: "network", len = remove_edges.len(), "test_features remove_edges");
            self.adv_remove_edges_from_routing_table(remove_edges);
        }
        if let Some(true) = msg.prune_edges {
            debug!(target: "network", "test_features prune_edges");
            self.update_routing_table_and_prune_edges(ctx, Prune::Now, Duration::from_secs(2));
        }
    }

    #[perf]
    fn handle_msg_inbound_tcp_connect(&mut self, msg: InboundTcpConnect, ctx: &mut Context<Self>) {
        {
            #[cfg(feature = "delay_detector")]
            let _d = delay_detector::DelayDetector::new("inbound tcp connect".into());
        }

        if self.is_inbound_allowed() {
            self.try_connect_peer(ctx.address(), msg.stream, PeerType::Inbound, None, None);
        } else {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network", "Inbound connection dropped (network at max capacity).");
        }
        self.pending_incoming_connections_counter.fetch_sub(1, Ordering::SeqCst);
    }

    #[cfg(feature = "test_features")]
    #[perf]
    fn handle_msg_get_peer_id(
        &mut self,
        _msg: crate::private_actix::GetPeerId,
    ) -> crate::private_actix::GetPeerIdResult {
        crate::private_actix::GetPeerIdResult { peer_id: self.my_peer_id.clone() }
    }

    #[perf]
    fn handle_msg_outbound_tcp_connect(
        &mut self,
        msg: OutboundTcpConnect,
        ctx: &mut Context<Self>,
    ) {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("outbound tcp connect".into());
        debug!(target: "network", to = ?msg.peer_info, "Trying to connect");
        if let Some(addr) = msg.peer_info.addr {
            // The `connect` may take several minutes. This happens when the
            // `SYN` packet for establishing a TCP connection gets silently
            // dropped, in which case the default TCP timeout is applied. That's
            // too long for us, so we shorten it to one second.
            //
            // Why exactly a second? It was hard-coded in a library we used
            // before, so we keep it to preserve behavior. Removing the timeout
            // completely was observed to break stuff for real on the testnet.
            tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
                .into_actor(self)
                .then(move |res, act, ctx| match res {
                    Ok(res) => match res {
                        Ok(stream) => {
                            debug!(target: "network", peer_info = ?msg.peer_info, "Connecting");
                            let edge_info = act.propose_edge(&msg.peer_info.id, None);

                            act.try_connect_peer(
                                ctx.address(),
                                stream,
                                PeerType::Outbound,
                                Some(msg.peer_info),
                                Some(edge_info),
                            );
                            actix::fut::ready(())
                        }
                        Err(err) => {
                            info!(target: "network", ?addr, ?err, "Error connecting to");
                            act.outgoing_peers.remove(&msg.peer_info.id);
                            actix::fut::ready(())
                        }
                    },
                    Err(err) => {
                        info!(target: "network", ?addr, ?err, "Error connecting to");
                        act.outgoing_peers.remove(&msg.peer_info.id);
                        actix::fut::ready(())
                    }
                })
                .wait(ctx);
        } else {
            warn!(target: "network", peer_info = ?msg.peer_info, "Trying to connect to peer with no public address");
        }
    }

    #[perf]
    fn handle_msg_register_peer(
        &mut self,
        msg: RegisterPeer,
        ctx: &mut Context<Self>,
    ) -> RegisterPeerResponse {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("consolidate".into());

        // Check if this is a blacklisted peer.
        if (msg.peer_info.addr.as_ref())
            .map_or(true, |addr| Self::is_blacklisted(&self.config.blacklist, addr))
        {
            debug!(target: "network", peer_info = ?msg.peer_info, "Dropping connection from blacklisted peer or unknown address");
            return RegisterPeerResponse::Reject;
        }

        if self.peer_store.is_banned(&msg.peer_info.id) {
            debug!(target: "network", id = ?msg.peer_info.id, "Dropping connection from banned peer");
            return RegisterPeerResponse::Reject;
        }

        // We already connected to this peer.
        if self.connected_peers.contains_key(&msg.peer_info.id) {
            debug!(target: "network", peer_info = ?self.my_peer_id, id = ?msg.peer_info.id, "Dropping handshake (Active Peer).");
            return RegisterPeerResponse::Reject;
        }

        // This is incoming connection but we have this peer already in outgoing.
        // This only happens when both of us connect at the same time, break tie using higher peer id.
        if msg.peer_type == PeerType::Inbound && self.outgoing_peers.contains(&msg.peer_info.id) {
            // We pick connection that has lower id.
            if msg.peer_info.id > self.my_peer_id {
                debug!(target: "network", my_peer_id = ?self.my_peer_id, id = ?msg.peer_info.id, "Dropping handshake (Tied).");
                return RegisterPeerResponse::Reject;
            }
        }

        if msg.peer_type == PeerType::Inbound && !self.is_inbound_allowed() {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network",
                active_peers = self.connected_peers.len(), outgoing_peers = self.outgoing_peers.len(),
                max_num_peers = self.config.max_num_peers,
                "Inbound connection dropped (network at max capacity)."
            );
            return RegisterPeerResponse::Reject;
        }

        if msg.other_edge_info.nonce == 0 {
            debug!(target: "network", nonce = msg.other_edge_info.nonce, "Invalid nonce. It must be greater than 0.");
            return RegisterPeerResponse::Reject;
        }

        let last_edge = self.routing_table_view.get_local_edge(&msg.peer_info.id);
        let last_nonce = last_edge.as_ref().map_or(0, |edge| edge.nonce());

        // Check that the received nonce is greater than the current nonce of this connection.
        if last_nonce >= msg.other_edge_info.nonce {
            debug!(target: "network", nonce = msg.other_edge_info.nonce, last_nonce, my_peer_id = ?self.my_peer_id, ?msg.peer_info.id, "Too low nonce");
            // If the check fails don't allow this connection.
            return RegisterPeerResponse::InvalidNonce(last_edge.cloned().map(Box::new).unwrap());
        }

        if msg.other_edge_info.nonce >= Edge::next_nonce(last_nonce) + EDGE_NONCE_BUMP_ALLOWED {
            debug!(target: "network", nonce = msg.other_edge_info.nonce, last_nonce, ?EDGE_NONCE_BUMP_ALLOWED, ?self.my_peer_id, ?msg.peer_info.id, "Too large nonce");
            return RegisterPeerResponse::Reject;
        }

        let require_response = msg.this_edge_info.is_none();

        let edge_info = msg.this_edge_info.clone().unwrap_or_else(|| {
            self.propose_edge(&msg.peer_info.id, Some(msg.other_edge_info.nonce))
        });

        let edge_info_response = if require_response { Some(edge_info.clone()) } else { None };

        // TODO: double check that address is connectable and add account id.
        self.register_peer(
            FullPeerInfo {
                peer_info: msg.peer_info,
                chain_info: msg.chain_info,
                partial_edge_info: msg.other_edge_info,
            },
            edge_info,
            msg.peer_type,
            msg.actor,
            msg.peer_protocol_version,
            msg.throttle_controller,
            ctx,
        );

        RegisterPeerResponse::Accept(edge_info_response)
    }

    #[perf]
    fn handle_msg_unregister(&mut self, msg: Unregister, ctx: &mut Context<Self>) {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("unregister".into());
        self.unregister_peer(ctx, msg.peer_id, msg.peer_type, msg.remove_from_peer_store);
    }

    #[perf]
    fn handle_msg_ban(&mut self, msg: Ban, ctx: &mut Context<Self>) {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("ban".into());
        self.ban_peer(ctx, &msg.peer_id, msg.ban_reason);
    }

    #[perf]
    fn handle_msg_peers_request(&mut self, _msg: PeersRequest) -> PeerRequestResult {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("peers request".into());
        PeerRequestResult {
            peers: self.peer_store.healthy_peers(self.config.max_send_peers as usize),
        }
    }

    fn handle_msg_peers_response(&mut self, msg: PeersResponse) {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("peers response".into());
        if let Err(err) = self.peer_store.add_indirect_peers(
            msg.peers.into_iter().filter(|peer_info| peer_info.id != self.my_peer_id).collect(),
        ) {
            error!(target: "network", ?err, "Fail to update peer store");
        };
    }

    fn handle_peer_manager_message(
        &mut self,
        msg: PeerManagerMessageRequest,
        ctx: &mut Context<Self>,
        throttle_controller: Option<ThrottleController>,
    ) -> PeerManagerMessageResponse {
        match msg {
            PeerManagerMessageRequest::RoutedMessageFrom(msg) => {
                PeerManagerMessageResponse::RoutedMessageFrom(self.handle_msg_routed_from(msg))
            }
            PeerManagerMessageRequest::NetworkRequests(msg) => {
                PeerManagerMessageResponse::NetworkResponses(self.handle_msg_network_requests(
                    msg,
                    ctx,
                    throttle_controller,
                ))
            }
            PeerManagerMessageRequest::RegisterPeer(msg) => {
                PeerManagerMessageResponse::RegisterPeerResponse(
                    self.handle_msg_register_peer(msg, ctx),
                )
            }
            PeerManagerMessageRequest::PeersRequest(msg) => {
                PeerManagerMessageResponse::PeerRequestResult(self.handle_msg_peers_request(msg))
            }
            PeerManagerMessageRequest::PeersResponse(msg) => {
                self.handle_msg_peers_response(msg);
                PeerManagerMessageResponse::PeersResponseResult(())
            }
            PeerManagerMessageRequest::PeerRequest(msg) => {
                PeerManagerMessageResponse::PeerResponse(self.handle_msg_peer_request(msg))
            }
            #[cfg(feature = "test_features")]
            PeerManagerMessageRequest::GetPeerId(msg) => {
                PeerManagerMessageResponse::GetPeerIdResult(self.handle_msg_get_peer_id(msg))
            }
            PeerManagerMessageRequest::OutboundTcpConnect(msg) => {
                self.handle_msg_outbound_tcp_connect(msg, ctx);
                PeerManagerMessageResponse::OutboundTcpConnect(())
            }
            PeerManagerMessageRequest::InboundTcpConnect(msg) => {
                self.handle_msg_inbound_tcp_connect(msg, ctx);
                PeerManagerMessageResponse::InboundTcpConnect(())
            }
            PeerManagerMessageRequest::Unregister(msg) => {
                self.handle_msg_unregister(msg, ctx);
                PeerManagerMessageResponse::Unregister(())
            }
            PeerManagerMessageRequest::Ban(msg) => {
                self.handle_msg_ban(msg, ctx);
                PeerManagerMessageResponse::Ban(())
            }
            #[cfg(feature = "test_features")]
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            PeerManagerMessageRequest::StartRoutingTableSync(msg) => {
                self.handle_msg_start_routing_table_sync(msg, ctx, throttle_controller);
                PeerManagerMessageResponse::StartRoutingTableSync(())
            }
            #[cfg(feature = "test_features")]
            PeerManagerMessageRequest::SetAdvOptions(msg) => {
                self.handle_msg_set_adv_options(msg);
                PeerManagerMessageResponse::SetAdvOptions(())
            }
            #[cfg(feature = "test_features")]
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            PeerManagerMessageRequest::SetRoutingTable(msg) => {
                self.handle_msg_set_routing_table(msg, ctx);
                PeerManagerMessageResponse::SetRoutingTable(())
            }
        }
    }

    /// "Return" true if this message is for this peer and should be sent to the client.
    /// Otherwise try to route this message to the final receiver and return false.
    fn handle_msg_routed_from(&mut self, msg: RoutedMessageFrom) -> bool {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new(
            format!("routed message from {}", strum::AsStaticRef::as_static(&msg.msg.body)).into(),
        );
        let RoutedMessageFrom { mut msg, from } = msg;

        if msg.expect_response() {
            trace!(target: "network", route_back = ?PeerMessage::Routed(msg.clone()), "Received peer message that requires");
            self.routing_table_view.add_route_back(msg.hash(), from.clone());
        }

        if Self::message_for_me(&mut self.routing_table_view, &self.my_peer_id, &msg.target) {
            // Handle Ping and Pong message if they are for us without sending to client.
            // i.e. Return false in case of Ping and Pong
            match &msg.body {
                RoutedMessageBody::Ping(ping) => self.handle_ping(ping.clone(), msg.hash()),
                RoutedMessageBody::Pong(pong) => self.handle_pong(pong.clone()),
                _ => return true,
            }

            false
        } else {
            if msg.decrease_ttl() {
                self.send_signed_message_to_peer(msg);
            } else {
                warn!(target: "network", ?msg, ?from, "Message dropped because TTL reached 0.");
            }
            false
        }
    }

    fn handle_msg_peer_request(&mut self, msg: PeerRequest) -> PeerResponse {
        #[cfg(feature = "delay_detector")]
        let _d =
            delay_detector::DelayDetector::new(format!("peer request {}", msg.as_ref()).into());
        match msg {
            PeerRequest::UpdateEdge((peer, nonce)) => {
                PeerResponse::UpdatedEdge(self.propose_edge(&peer, Some(nonce)))
            }
            PeerRequest::RouteBack(body, target) => {
                trace!(target: "network", ?target, "Sending message to route back");
                self.send_message_to_peer(RawRoutedMessage {
                    target: AccountOrPeerIdOrHash::Hash(target),
                    body: *body,
                });
                PeerResponse::NoResponse
            }
            PeerRequest::UpdatePeerInfo(peer_info) => {
                if let Err(err) = self.peer_store.add_trusted_peer(peer_info, TrustLevel::Direct) {
                    error!(target: "network", ?err, "Fail to update peer store");
                }
                PeerResponse::NoResponse
            }
            PeerRequest::ReceivedMessage(peer_id, last_time_received_message) => {
                if let Some(active_peer) = self.connected_peers.get_mut(&peer_id) {
                    active_peer.last_time_received_message = last_time_received_message;
                }
                PeerResponse::NoResponse
            }
        }
    }

    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    fn process_ibf_msg(
        &mut self,
        peer_id: &PeerId,
        mut ibf_msg: crate::network_protocol::RoutingVersion2,
        addr: Addr<PeerActor>,
        throttle_controller: Option<ThrottleController>,
    ) {
        let mut edges: Vec<Edge> = Vec::new();
        std::mem::swap(&mut edges, &mut ibf_msg.edges);
        self.validate_edges_and_add_to_routing_table(
            peer_id.clone(),
            edges,
            throttle_controller.clone(),
        );
        actix::spawn(
            self.routing_table_addr
                .send(ActixMessageWrapper::new_without_size(
                    RoutingTableMessages::ProcessIbfMessage { peer_id: peer_id.clone(), ibf_msg },
                    throttle_controller,
                ))
                .then(move |response| {
                    match response.map(|r| r.into_inner()) {
                        Ok(RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                            ibf_msg: response_ibf_msg,
                        }) => {
                            if let Some(response_ibf_msg) = response_ibf_msg {
                                let _ = addr.do_send(SendMessage {
                                    message: PeerMessage::RoutingTableSyncV2(
                                        crate::network_protocol::RoutingSyncV2::Version2(
                                            response_ibf_msg,
                                        ),
                                    ),
                                });
                            }
                        }
                        _ => error!(target: "network", "expected ProcessIbfMessageResponse"),
                    }
                    future::ready(())
                }),
        );
    }
}

impl Handler<ActixMessageWrapper<PeerManagerMessageRequest>> for PeerManagerActor {
    type Result = ActixMessageResponse<PeerManagerMessageResponse>;

    fn handle(
        &mut self,
        msg: ActixMessageWrapper<PeerManagerMessageRequest>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        // Unpack throttle controller
        let (msg, throttle_token) = msg.take();

        let throttle_controller = throttle_token.throttle_controller().cloned();
        let result = self.handle_peer_manager_message(msg, ctx, throttle_controller);

        // TODO(#5155) Add support for DeepSizeOf to result
        ActixMessageResponse::new(
            result,
            ThrottleToken::new_without_size(throttle_token.throttle_controller().cloned()),
        )
    }
}

impl Handler<PeerManagerMessageRequest> for PeerManagerActor {
    type Result = PeerManagerMessageResponse;
    fn handle(&mut self, msg: PeerManagerMessageRequest, ctx: &mut Self::Context) -> Self::Result {
        self.handle_peer_manager_message(msg, ctx, None)
    }
}
