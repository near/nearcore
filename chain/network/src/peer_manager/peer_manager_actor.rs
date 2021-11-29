use crate::common::message_wrapper::{ActixMessageResponse, ActixMessageWrapper};
use crate::peer::codec::Codec;
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::peer_store::{PeerStore, TrustLevel};
#[cfg(all(
    feature = "test_features",
    feature = "protocol_feature_routing_exchange_algorithm"
))]
use crate::routing::edge::SimpleEdge;
use crate::routing::edge::{Edge, EdgeInfo, EdgeType};
use crate::routing::edge_verifier_actor::EdgeVerifierHelper;
use crate::routing::routing::{
    PeerRequestResult, RoutingTableView, DELETE_PEERS_AFTER_TIME, MAX_NUM_PEERS,
};
use crate::routing::routing_table_actor::Prune;
use crate::stats::metrics;
use crate::stats::metrics::NetworkMetrics;
#[cfg(feature = "test_features")]
use crate::types::SetAdvOptions;
use crate::types::{
    Consolidate, ConsolidateResponse, GetPeerId, GetPeerIdResult, NetworkInfo,
    PeerManagerMessageRequest, PeerManagerMessageResponse, PeerMessage, PeerRequest, PeerResponse,
    PeersRequest, PeersResponse, SendMessage, StopMsg, SyncData, Unregister, ValidateEdgeList,
};
use crate::types::{FullPeerInfo, NetworkClientMessages, NetworkRequests, NetworkResponses};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::types::{RoutingSyncV2, RoutingVersion2};
use crate::{PeerInfo, RoutingTableActor, RoutingTableMessages, RoutingTableMessagesResponse};
use actix::{
    Actor, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner, Handler,
    Recipient, Running, StreamHandler, WrapFuture,
};
use futures::task::Poll;
use futures::{future, Stream, StreamExt};
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, Ban, BlockedPorts, InboundTcpConnect, KnownPeerState, KnownPeerStatus,
    KnownProducer, NetworkConfig, NetworkViewClientMessages, NetworkViewClientResponses,
    OutboundTcpConnect, PeerIdOrHash, PeerManagerRequest, PeerType, Ping, Pong, QueryPeerStats,
    RawRoutedMessage, ReasonForBan, RoutedMessage, RoutedMessageBody, RoutedMessageFrom,
    StateResponseInfo,
};
use near_performance_metrics::framed_write::FramedWrite;
use near_performance_metrics_macros::perf;
use near_primitives::checked_feature;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time::Clock;
use near_primitives::types::{AccountId, ProtocolVersion};
use near_primitives::utils::from_timestamp;
use near_rate_limiter::{ThrottleController, ThrottleToken, ThrottledFrameRead};
use near_store::Store;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::thread_rng;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::PollSemaphore;
use tracing::{debug, error, info, trace, warn};

/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: Duration = Duration::from_millis(60_000);
/// How much time to wait (in milliseconds) after we send update nonce request before disconnecting.
/// This number should be large to handle pair of nodes with high latency.
const WAIT_ON_TRY_UPDATE_NONCE: Duration = Duration::from_millis(6_000);
/// If we see an edge between us and other peer, but this peer is not a current connection, wait this
/// timeout and in case it didn't become an active peer, broadcast edge removal update.
const WAIT_PEER_BEFORE_REMOVE: Duration = Duration::from_millis(6_000);
/// Maximum number an edge can increase between oldest known edge and new proposed edge.
const EDGE_NONCE_BUMP_ALLOWED: u64 = 1_000;
/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The maximum waiting time between consecutive attempts to establish connection
/// with another peer is 60 seconds. This is the minimum exponent after such threshold
/// is less than the exponential backoff.
///
/// 10 * EXPONENTIAL_BACKOFF_RATIO**EXPONENTIAL_BACKOFF_LIMIT > 60000
///
/// EXPONENTIAL_BACKOFF_LIMIT = math.log(60000 / 10, EXPONENTIAL_BACKOFF_RATIO)
const EXPONENTIAL_BACKOFF_LIMIT: u64 = 91;
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

/// Max number of messages we received from peer, and they are in progress, before we start throttling.
/// Disabled for now (TODO PUT UNDER FEATURE FLAG)
const MAX_MESSAGES_COUNT: usize = usize::MAX;
/// Max total size of all messages that are in progress, before we start throttling.
/// Disabled for now (TODO PUT UNDER FEATURE FLAG)
const MAX_MESSAGES_TOTAL_SIZE: usize = usize::MAX;

macro_rules! unwrap_or_error(($obj: expr, $error: expr) => (match $obj {
    Ok(result) => result,
    Err(err) => {
        error!(target: "network", "{}: {}", $error, err);
        return;
    }
}));

/// Contains information relevant to an active peer.
struct ActivePeer {
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
    /// Active peers (inbound and outbound) with their full peer information.
    active_peers: HashMap<PeerId, ActivePeer>,
    /// View of the Routing table. It keeps:
    /// - routing information - how to route messages
    /// - edges adjacent to my_peer_id
    /// - account id
    /// Full routing table (that currently includes information about all edges in the graph) is now inside Routing Table.
    routing_table_view: RoutingTableView,
    /// Fields used for communicating with EdgeVerifier
    routing_table_exchange_helper: EdgeVerifierHelper,
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,
    /// Monitor peers attempts, used for fast checking in the beginning with exponential backoff.
    monitor_peers_attempts: u64,
    /// Active peers we have sent new edge update, but we haven't received response so far.
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
    /// Number of active peers, used for rate limiting.
    peer_counter: Arc<AtomicUsize>,
    /// Used for testing, for disabling features.
    adv_helper: AdvHelper,
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

impl PeerManagerActor {
    pub fn new(
        store: Arc<Store>,
        config: NetworkConfig,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
        routing_table_addr: Addr<RoutingTableActor>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if config.max_num_peers as usize > MAX_NUM_PEERS {
            panic!("Exceeded max peer limit: {}", MAX_NUM_PEERS);
        }

        let peer_store = PeerStore::new(store.clone(), &config.boot_nodes)?;
        debug!(target: "network", "Found known peers: {} (boot nodes={})", peer_store.len(), config.boot_nodes.len());
        debug!(target: "network", "Blacklist: {:?}", config.blacklist);

        let my_peer_id: PeerId = PeerId::new(config.public_key.clone());
        let routing_table = RoutingTableView::new(my_peer_id.clone(), store);

        let txns_since_last_block = Arc::new(AtomicUsize::new(0));

        Ok(PeerManagerActor {
            my_peer_id,
            config,
            client_addr,
            view_client_addr,
            peer_store,
            active_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            routing_table_view: routing_table,
            routing_table_exchange_helper: Default::default(),
            monitor_peers_attempts: 0,
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
                    edges_to_remove,
                    peer_forwarding,
                    peers_to_ban,
                }) => {
                    act.routing_table_view.remove_edges(&edges_to_remove);
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
            if !edge.contains_peer(&self.my_peer_id) {
                continue;
            }
            let key = edge.key();
            if !self.routing_table_view.is_local_edge_newer(key, edge.nonce()) {
                continue;
            }
            self.routing_table_view.local_edges_info.insert(edge.key().clone(), edge.clone());
        }

        self.routing_table_addr
            .send(RoutingTableMessages::AddVerifiedEdges { edges })
            .into_actor(self)
            .map(move |response, act, ctx| match response {
                Ok(RoutingTableMessagesResponse::AddVerifiedEdgesResponse(filtered_edges)) => {
                    // Broadcast new edges to all other peers.
                    if broadcast_edges && act.adv_helper.can_broadcast_edges() {
                        let new_data =
                            SyncData { edges: filtered_edges, accounts: Default::default() };
                        act.broadcast_message(
                            ctx,
                            SendMessage { message: PeerMessage::RoutingTableSync(new_data) },
                        )
                    }
                }
                _ => error!(target: "network", "expected AddIbfSetResponse"),
            })
            .spawn(ctx);
    }

    fn broadcast_accounts(
        &mut self,
        ctx: &mut Context<PeerManagerActor>,
        accounts: Vec<AnnounceAccount>,
    ) {
        if !accounts.is_empty() {
            debug!(target: "network", "{:?} Received new accounts: {:?}", self.config.account_id, accounts);
        }
        for account in accounts.iter() {
            self.routing_table_view.add_account(account.clone());
        }

        let new_data = SyncData { edges: Default::default(), accounts };

        if !new_data.is_empty() {
            self.broadcast_message(
                ctx,
                SendMessage { message: PeerMessage::RoutingTableSync(new_data) },
            )
        };
    }

    /// `update_routing_table_trigger` schedule updating routing table to `RoutingTableActor`
    /// Usually we do edge pruning one an hour. However it may be disabled in following cases:
    /// - there are edges, that were supposed to be added, but are still in `EdgeVerifierActor,
    ///   waiting to have their signatures checked.
    /// - edge pruning may be disabled for unit testing.
    fn update_routing_table_trigger(&mut self, ctx: &mut Context<Self>, interval: Duration) {
        let can_prune_edges = !self.adv_helper.adv_disable_edge_pruning();

        self.update_routing_table_and_prune_edges(
            ctx,
            if can_prune_edges { Prune::PruneOncePerHour } else { Prune::Disable },
            DELETE_PEERS_AFTER_TIME,
        );

        near_performance_metrics::actix::run_later(ctx, interval, move |act, ctx| {
            act.update_routing_table_trigger(ctx, interval);
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
                // check if edge is local; contains our peer
                if !edge.contains_peer(&self.my_peer_id) {
                    continue;
                }
                let key = edge.key();
                if !self.routing_table_view.is_local_edge_newer(key, edge.nonce()) {
                    continue;
                }
                // Check whenever peer needs to be removed when edge is removed.
                if let Some(other) = edge.other(&self.my_peer_id) {
                    // We belong to this edge.
                    if self.active_peers.contains_key(other) {
                        // This is an active connection.
                        match edge.edge_type() {
                            EdgeType::Removed => {
                                self.maybe_remove_connected_peer(ctx, edge.clone(), other);
                            }
                            _ => {}
                        }
                    } else {
                        match edge.edge_type() {
                            EdgeType::Added => {
                                // We are not connected to this peer, but routing table contains
                                // information that we do. We should wait and remove that peer
                                // from routing table
                                self.wait_peer_or_remove(ctx, edge.clone());
                            }
                            _ => {}
                        }
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

    fn num_active_peers(&self) -> usize {
        self.active_peers.len()
    }

    fn is_blacklisted(&self, addr: &SocketAddr) -> bool {
        if let Some(blocked_ports) = self.config.blacklist.get(&addr.ip()) {
            match blocked_ports {
                BlockedPorts::All => true,
                BlockedPorts::Some(ports) => ports.contains(&addr.port()),
            }
        } else {
            false
        }
    }

    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    fn initialize_routing_table_exchange(
        &mut self,
        peer_id: PeerId,
        peer_type: PeerType,
        addr: Addr<PeerActor>,
        ctx: &mut Context<Self>,
    ) {
        near_performance_metrics::actix::run_later(ctx, WAIT_FOR_SYNC_DELAY, move |act, ctx| {
            if peer_type == PeerType::Inbound {
                act.routing_table_addr
                    .send(RoutingTableMessages::AddPeerIfMissing(peer_id, None))
                    .into_actor(act)
                    .map(move |response, act, ctx| match response {
                        Ok(RoutingTableMessagesResponse::AddPeerResponse { seed }) => {
                            act.start_routing_table_syncv2(ctx, addr, seed)
                        }
                        _ => error!(target: "network", "expected AddIbfSetResponse"),
                    })
                    .spawn(ctx);
            }
        });
    }

    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    fn start_routing_table_syncv2(
        &self,
        ctx: &mut Context<Self>,
        addr: Addr<PeerActor>,
        seed: u64,
    ) {
        self.routing_table_addr
            .send(RoutingTableMessages::StartRoutingTableSync { seed })
            .into_actor(self)
            .map(move |response, _act, _ctx| match response {
                Ok(RoutingTableMessagesResponse::StartRoutingTableSyncResponse(response)) => {
                    let _ = addr.do_send(SendMessage { message: response });
                }
                _ => error!(target: "network", "expected StartRoutingTableSyncResponse"),
            })
            .spawn(ctx);
    }

    /// Register a direct connection to a new peer. This will be called after successfully
    /// establishing a connection with another peer. It become part of the active peers.
    ///
    /// To build new edge between this pair of nodes both signatures are required.
    /// Signature from this node is passed in `edge_info`
    /// Signature from the other node is passed in `full_peer_info.edge_info`.
    fn register_peer(
        &mut self,
        full_peer_info: FullPeerInfo,
        edge_info: EdgeInfo,
        peer_type: PeerType,
        addr: Addr<PeerActor>,
        peer_protocol_version: ProtocolVersion,
        ctx: &mut Context<Self>,
    ) {
        #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
        let peer_id = full_peer_info.peer_info.id.clone();
        debug!(target: "network", "Consolidated connection with {:?}", full_peer_info);

        if self.outgoing_peers.contains(&full_peer_info.peer_info.id) {
            self.outgoing_peers.remove(&full_peer_info.peer_info.id);
        }
        unwrap_or_error!(
            self.peer_store.peer_connected(&full_peer_info.peer_info),
            "Failed to save peer data"
        );

        let target_peer_id = full_peer_info.peer_info.id.clone();

        let new_edge = Edge::new(
            self.my_peer_id.clone(),
            target_peer_id.clone(),
            edge_info.nonce,
            edge_info.signature,
            full_peer_info.edge_info.signature.clone(),
        );

        self.active_peers.insert(
            target_peer_id.clone(),
            ActivePeer {
                addr: addr.clone(),
                full_peer_info,
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                last_time_peer_requested: Clock::instant(),
                last_time_received_message: Clock::instant(),
                connection_established_time: Clock::instant(),
                peer_type,
            },
        );

        self.add_verified_edges_to_routing_table(ctx, vec![new_edge.clone()], false);

        checked_feature!(
            "protocol_feature_routing_exchange_algorithm",
            RoutingExchangeAlgorithm,
            peer_protocol_version,
            {
                self.initialize_routing_table_exchange(peer_id, peer_type, addr.clone(), ctx);
                self.send_sync(peer_type, addr, ctx, target_peer_id.clone(), new_edge, Vec::new());
                return;
            }
        );
        near_performance_metrics::actix::run_later(ctx, WAIT_FOR_SYNC_DELAY, move |act, ctx| {
            act.routing_table_addr
                .send(RoutingTableMessages::RequestRoutingTable)
                .into_actor(act)
                .map(move |response, act, ctx| match response {
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
        });
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
        let known_accounts = self.routing_table_view.get_announce_accounts();

        // Start syncing network point of view. Wait until both parties are connected before start
        // sending messages.

        near_performance_metrics::actix::run_later(ctx, WAIT_FOR_SYNC_DELAY, move |act, ctx| {
            let _ = addr.do_send(SendMessage {
                message: PeerMessage::RoutingTableSync(SyncData {
                    edges: known_edges,
                    accounts: known_accounts,
                }),
            });

            // Ask for peers list on connection.
            let _ = addr.do_send(SendMessage { message: PeerMessage::PeersRequest });
            if let Some(active_peer) = act.active_peers.get_mut(&target_peer_id) {
                active_peer.last_time_peer_requested = Clock::instant();
            }

            if peer_type == PeerType::Outbound {
                // Only broadcast new message from the outbound endpoint.
                // Wait a time out before broadcasting this new edge to let the other party finish handshake.
                act.broadcast_message(
                    ctx,
                    SendMessage {
                        message: PeerMessage::RoutingTableSync(SyncData::edge(new_edge)),
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
            if let Some(peer) = self.active_peers.get(peer_id) {
                if peer.peer_type != peer_type {
                    // Don't remove the peer
                    return;
                }
            }
        }

        // If the last edge we have with this peer represent a connection addition, create the edge
        // update that represents the connection removal.
        self.active_peers.remove(peer_id);

        #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
        self.routing_table_addr
            .send(RoutingTableMessages::RemovePeer(peer_id.clone()))
            .into_actor(self)
            .map(|_, _, _| ())
            .spawn(ctx);

        if let Some(edge) =
            self.routing_table_view.get_edge(self.my_peer_id.clone(), peer_id.clone())
        {
            if edge.edge_type() == EdgeType::Added {
                let edge_update =
                    edge.remove_edge(self.my_peer_id.clone(), &self.config.secret_key);
                self.add_verified_edges_to_routing_table(ctx, vec![edge_update.clone()], false);
                self.broadcast_message(
                    ctx,
                    SendMessage {
                        message: PeerMessage::RoutingTableSync(SyncData::edge(edge_update)),
                    },
                );
            }
        }
    }

    /// Remove a peer from the active peer set. If the peer doesn't belong to the active peer set
    /// data from ongoing connection established is removed.
    fn unregister_peer(
        &mut self,
        ctx: &mut Context<Self>,
        peer_id: PeerId,
        peer_type: PeerType,
        remove_from_peer_store: bool,
    ) {
        debug!(target: "network", "Unregister peer: {:?} {:?}", peer_id, peer_type);
        // If this is an unconsolidated peer because failed / connected inbound, just delete it.
        if peer_type == PeerType::Outbound && self.outgoing_peers.contains(&peer_id) {
            self.outgoing_peers.remove(&peer_id);
            return;
        }

        if remove_from_peer_store {
            self.remove_active_peer(ctx, &peer_id, Some(peer_type));
            unwrap_or_error!(
                self.peer_store.peer_disconnected(&peer_id),
                "Failed to save peer data"
            );
        }
    }

    /// Add peer to ban list.
    /// This function should only be called after Peer instance is stopped.
    /// Note: Use `try_ban_peer` if there might be a Peer instance still active.
    fn ban_peer(&mut self, ctx: &mut Context<Self>, peer_id: &PeerId, ban_reason: ReasonForBan) {
        warn!(target: "network", "Banning peer {:?} for {:?}", peer_id, ban_reason);
        self.remove_active_peer(ctx, peer_id, None);
        unwrap_or_error!(self.peer_store.peer_ban(peer_id, ban_reason), "Failed to save peer data");
    }

    /// Ban peer. Stop peer instance if it is still active,
    /// and then mark peer as banned in the peer store.
    pub(crate) fn try_ban_peer(
        &mut self,
        ctx: &mut Context<Self>,
        peer_id: &PeerId,
        ban_reason: ReasonForBan,
    ) {
        if let Some(peer) = self.active_peers.get(peer_id) {
            let _ = peer.addr.do_send(PeerManagerRequest::BanPeer(ban_reason));
        } else {
            warn!(target: "network", "Try to ban a disconnected peer for {:?}: {:?}", ban_reason, peer_id);
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
        edge_info: Option<EdgeInfo>,
    ) {
        let peer_id = self.my_peer_id.clone();
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
                    warn!(target: "network", "Failed establishing connection with {:?}", peer_info);
                    return;
                }
            },
        };

        let remote_addr = match stream.peer_addr() {
            Ok(remote_addr) => remote_addr,
            _ => {
                warn!(target: "network", "Failed establishing connection with {:?}", peer_info);
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
            let semaphore = PollSemaphore::new(Arc::new(Semaphore::new(0)));
            let rate_limiter = ThrottleController::new(
                semaphore.clone(),
                MAX_MESSAGES_COUNT,
                MAX_MESSAGES_TOTAL_SIZE,
            );
            PeerActor::add_stream(
                ThrottledFrameRead::new(read, Codec::default(), rate_limiter.clone(), semaphore)
                    .take_while(|x| match x {
                        Ok(_) => future::ready(true),
                        Err(e) => {
                            warn!(target: "network", "Peer stream error: {:?}", e);
                            future::ready(false)
                        }
                    })
                    .map(Result::unwrap),
                ctx,
            );

            PeerActor::new(
                PeerInfo { id: peer_id, addr: Some(server_addr), account_id },
                remote_addr,
                peer_info,
                peer_type,
                FramedWrite::new(write, Codec::default(), Codec::default(), ctx),
                handshake_timeout,
                recipient,
                client_addr,
                view_client_addr,
                edge_info,
                network_metrics,
                txns_since_last_block,
                peer_counter,
                rate_limiter,
            )
        });
    }

    fn num_active_outgoing_peers(&self) -> usize {
        self.active_peers
            .values()
            .filter(|active_peer| active_peer.peer_type == PeerType::Outbound)
            .count()
    }

    fn num_archival_peers(&self) -> usize {
        self.active_peers
            .values()
            .filter(|active_peer| active_peer.full_peer_info.chain_info.archival)
            .count()
    }

    /// Check if it is needed to create a new outbound connection.
    /// If the number of active connections is less than `ideal_connections_lo` or
    /// (the number of outgoing connections is less than `minimum_outbound_peers`
    ///     and the total connections is less than `max_num_peers`)
    fn is_outbound_bootstrap_needed(&self) -> bool {
        let total_connections = self.active_peers.len() + self.outgoing_peers.len();
        let potential_outgoing_connections =
            self.num_active_outgoing_peers() + self.outgoing_peers.len();

        (total_connections < self.config.ideal_connections_lo as usize
            || (total_connections < self.config.max_num_peers as usize
                && potential_outgoing_connections < self.config.minimum_outbound_peers as usize))
            && !self.config.outbound_disabled
    }

    fn is_inbound_allowed(&self) -> bool {
        self.active_peers.len() + self.outgoing_peers.len() < self.config.max_num_peers as usize
    }

    /// Returns single random peer with close to the highest height
    fn highest_height_peers(&self) -> Vec<FullPeerInfo> {
        // This finds max height among peers, and returns one peer close to such height.
        let max_height = match self
            .active_peers
            .values()
            .map(|active_peers| active_peers.full_peer_info.chain_info.height)
            .max()
        {
            Some(height) => height,
            None => return vec![],
        };
        // Find all peers whose height is within `highest_peer_horizon` from max height peer(s).
        self.active_peers
            .values()
            .filter_map(|active_peer| {
                if active_peer.full_peer_info.chain_info.height + self.config.highest_peer_horizon
                    >= max_height
                {
                    Some(active_peer.full_peer_info.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    /// Returns bytes sent/received across all peers.
    fn get_total_bytes_per_sec(&self) -> (u64, u64) {
        let sent_bps = self.active_peers.values().map(|x| x.sent_bytes_per_sec).sum();
        let received_bps = self.active_peers.values().map(|x| x.received_bytes_per_sec).sum();
        (sent_bps, received_bps)
    }

    /// Get a random peer we are not connected to from the known list.
    fn sample_random_peer(&self, ignore_fn: impl Fn(&KnownPeerState) -> bool) -> Option<PeerInfo> {
        let unconnected_peers = self.peer_store.unconnected_peers(ignore_fn);
        unconnected_peers.choose(&mut rand::thread_rng()).cloned()
    }

    /// Query current peers for more peers.
    fn query_active_peers_for_more_peers(&mut self, ctx: &mut Context<Self>) {
        let mut requests = futures::stream::FuturesUnordered::new();
        let msg = SendMessage { message: PeerMessage::PeersRequest };
        for (_, active_peer) in self.active_peers.iter_mut() {
            if active_peer.last_time_peer_requested.elapsed() > REQUEST_PEERS_INTERVAL {
                active_peer.last_time_peer_requested = Clock::instant();
                requests.push(active_peer.addr.send(msg.clone()));
            }
        }
        ctx.spawn(async move {
            while let Some(response) = requests.next().await {
                if let Err(e) = response {
                    debug!(target: "network", "Failed sending broadcast message(query_active_peers): {}", e);
                }
            }
        }.into_actor(self));
    }

    #[cfg(all(feature = "test_features", feature = "protocol_feature_routing_exchange_algorithm"))]
    fn adv_remove_edges_from_routing_table(
        &mut self,
        ctx: &mut Context<Self>,
        edges: Vec<SimpleEdge>,
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
        self.routing_table_view.remove_edges(&edges);
        self.routing_table_addr
            .send(RoutingTableMessages::AdvRemoveEdges(edges))
            .into_actor(self)
            .map(|_, _, _| ())
            .spawn(ctx);
    }

    fn wait_peer_or_remove(&mut self, ctx: &mut Context<Self>, edge: Edge) {
        // This edge says this is an active peer, which is currently not in the set of active peers.
        // Wait for some time to let the connection begin or broadcast edge removal instead.

        near_performance_metrics::actix::run_later(
            ctx,
            WAIT_PEER_BEFORE_REMOVE,
            move |act, ctx| {
                let other = edge.other(&act.my_peer_id).unwrap();
                if !act.active_peers.contains_key(other) {
                    // Peer is still not active after waiting a timeout.
                    let new_edge = edge.remove_edge(act.my_peer_id.clone(), &act.config.secret_key);
                    act.broadcast_message(
                        ctx,
                        SendMessage {
                            message: PeerMessage::RoutingTableSync(SyncData::edge(new_edge)),
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
    fn maybe_remove_connected_peer(&mut self, ctx: &mut Context<Self>, edge: Edge, other: &PeerId) {
        let nonce = edge.next();

        if let Some(last_nonce) = self.local_peer_pending_update_nonce_request.get(other) {
            if *last_nonce >= nonce {
                // We already tried to update an edge with equal or higher nonce.
                return;
            }
        }

        self.send_message(
            ctx,
            other.clone(),
            PeerMessage::RequestUpdateNonce(EdgeInfo::new(
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
                        if let Some(peer) = act.active_peers.get(&other) {
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
        for (peer_id, active_peer) in self.active_peers.iter() {
            let peer_id1 = peer_id.clone();
            active_peer
                .addr
                .send(QueryPeerStats {})
                .into_actor(self)
                .map(|result, _, _| result.map_err(|err| error!(target: "network", "Failed sending message(monitor_peer_stats): {}", err)))
                .map(move |res, act, _| {
                    let _ignore = res.map(|res| {
                        if res.is_abusive {
                            trace!(target: "network", "Banning peer {} for abuse ({} sent, {} recv)", peer_id1, res.message_counts.0, res.message_counts.1);
                            // TODO(MarX, #1586): Ban peer if we found them abusive. Fix issue with heavy
                            //  network traffic that flags honest peers.
                            // Send ban signal to peer instance. It should send ban signal back and stop the instance.
                            // if let Some(active_peer) = act.active_peers.get(&peer_id1) {
                            //     active_peer.addr.do_send(PeerManagerRequest::BanPeer(ReasonForBan::Abusive));
                            // }
                        } else if let Some(active_peer) = act.active_peers.get_mut(&peer_id1) {
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
        debug!(target: "network", "Trying to stop an active connection. Number of active connections: {}", self.active_peers.len());

        // Build safe set
        let mut safe_set = HashSet::new();

        if self.num_active_outgoing_peers() + self.outgoing_peers.len()
            <= self.config.minimum_outbound_peers as usize
        {
            for (peer, active) in self.active_peers.iter() {
                if active.peer_type == PeerType::Outbound {
                    safe_set.insert(peer.clone());
                }
            }
        }

        if self.config.archive
            && self.num_archival_peers()
                <= self.config.archival_peer_connections_lower_bound as usize
        {
            for (peer, active) in self.active_peers.iter() {
                if active.full_peer_info.chain_info.archival {
                    safe_set.insert(peer.clone());
                }
            }
        }

        // Find all recent connections
        let mut recent_connections = self
            .active_peers
            .iter()
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
            .into_iter()
            .take((self.config.safe_set_size as usize).saturating_sub(safe_set.len()))
        {
            safe_set.insert(peer_id.clone());
        }

        // Build valid candidate list to choose the peer to be removed. All peers outside the safe set.
        let candidates = self
            .active_peers
            .keys()
            .filter_map(
                |peer_id| {
                    if safe_set.contains(peer_id) {
                        None
                    } else {
                        Some(peer_id.clone())
                    }
                },
            )
            .collect::<Vec<_>>();

        if let Some(peer_id) = candidates.choose(&mut rand::thread_rng()) {
            if let Some(active_peer) = self.active_peers.get(peer_id) {
                debug!(target: "network", "Stop active connection: {:?}", peer_id);
                active_peer.addr.do_send(PeerManagerRequest::UnregisterPeer);
            }
        }
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - unban peers that have been banned for awhile,
    ///  - remove expired peers,
    fn monitor_peers_trigger(&mut self, ctx: &mut Context<Self>, max_interval: Duration) {
        let mut to_unban = vec![];
        for (peer_id, peer_state) in self.peer_store.iter() {
            if let KnownPeerStatus::Banned(_, last_banned) = peer_state.status {
                let interval = unwrap_or_error!(
                    (Clock::utc() - from_timestamp(last_banned)).to_std(),
                    "Failed to convert time"
                );
                if interval > self.config.ban_window {
                    info!(target: "network", "Monitor peers: unbanned {} after {:?}.", peer_id, interval);
                    to_unban.push(peer_id.clone());
                }
            }
        }

        for peer_id in to_unban {
            unwrap_or_error!(self.peer_store.peer_unban(&peer_id), "Failed to unban a peer");
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
                    self.monitor_peers_attempts = 0;
                }

                self.outgoing_peers.insert(peer_info.id.clone());
                ctx.notify(PeerManagerMessageRequest::OutboundTcpConnect(OutboundTcpConnect {
                    peer_info,
                }));
            } else {
                self.query_active_peers_for_more_peers(ctx);
            }
        }

        // If there are too many active connections try to remove some connections
        if self.active_peers.len() > self.config.ideal_connections_hi as usize {
            self.try_stop_active_connection();
        }

        unwrap_or_error!(
            self.peer_store.remove_expired(&self.config),
            "Failed to remove expired peers"
        );

        // Reschedule the bootstrap peer task, starting of as quick as possible with exponential backoff.
        let wait = if self.monitor_peers_attempts >= EXPONENTIAL_BACKOFF_LIMIT {
            // This is expected to be 60 seconds
            max_interval.as_millis() as u64
        } else {
            (10f64 * EXPONENTIAL_BACKOFF_RATIO.powf(self.monitor_peers_attempts as f64)) as u64
        };

        self.monitor_peers_attempts =
            cmp::min(EXPONENTIAL_BACKOFF_LIMIT, self.monitor_peers_attempts + 1);

        near_performance_metrics::actix::run_later(
            ctx,
            Duration::from_millis(wait),
            move |act, ctx| {
                act.monitor_peers_trigger(ctx, max_interval);
            },
        );
    }

    /// Sends list of edges, from peer `peer_id` to check their signatures to `EdgeVerifierActor`.
    /// Bans peer `peer_id` if an invalid edge is found.
    /// `PeerManagerActor` periodically runs `broadcast_validated_edges_trigger`, which gets edges
    /// from `EdgeVerifierActor` concurrent queue and sends edges to be added to `RoutingTableActor`.
    fn validate_edges_and_add_to_routing_table(
        &mut self,
        _ctx: &mut Context<Self>,
        peer_id: PeerId,
        edges: Vec<Edge>,
    ) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_addr.do_send(ValidateEdgeList {
            edges,
            edges_info_shared: self.routing_table_exchange_helper.edges_info_shared.clone(),
            sender: self.routing_table_exchange_helper.edges_to_add_sender.clone(),
            #[cfg(feature = "test_features")]
            adv_disable_edge_signature_verification: self
                .adv_helper
                .adv_disable_edge_signature_verification,
            peer_id,
        });
    }

    /// Broadcast message to all active peers.
    fn broadcast_message(&self, ctx: &mut Context<Self>, msg: SendMessage) {
        // TODO(MarX, #1363): Implement smart broadcasting. (MST)

        // Change message to reference counted to allow sharing with all actors
        // without cloning.
        let msg = Arc::new(msg);
        let mut requests: futures::stream::FuturesUnordered<_> =
            self.active_peers.values().map(|peer| peer.addr.send(Arc::clone(&msg))).collect();

        ctx.spawn(async move {
            while let Some(response) = requests.next().await {
                if let Err(e) = response {
                    debug!(target: "network", "Failed sending broadcast message(broadcast_message): {}", e);
                }
            }
        }.into_actor(self));
    }

    fn announce_account(&mut self, ctx: &mut Context<Self>, announce_account: AnnounceAccount) {
        debug!(target: "network", "{:?} Account announce: {:?}", self.config.account_id, announce_account);
        if !self.routing_table_view.contains_account(&announce_account) {
            self.routing_table_view.add_account(announce_account.clone());
            self.broadcast_message(
                ctx,
                SendMessage {
                    message: PeerMessage::RoutingTableSync(SyncData::account(announce_account)),
                },
            );
        }
    }

    /// Send message to peer that belong to our active set
    /// Return whether the message is sent or not.
    fn send_message(
        &mut self,
        ctx: &mut Context<Self>,
        peer_id: PeerId,
        message: PeerMessage,
    ) -> bool {
        if let Some(active_peer) = self.active_peers.get(&peer_id) {
            let msg_kind = message.msg_variant().to_string();
            trace!(target: "network", "Send message: {}", msg_kind);
            active_peer
                .addr
                .send(SendMessage { message })
                .into_actor(self)
                .map(move |res, act, _|
                    res.map_err(|e| {
                        // Peer could have disconnect between check and sending the message.
                        if act.active_peers.contains_key(&peer_id) {
                            error!(target: "network", "Failed sending message(send_message, {}): {}", msg_kind, e)
                        }
                    })
                )
                .map(|_, _, _| ())
                .spawn(ctx);
            true
        } else {
            debug!(target: "network",
                   "Sending message to: {} (which is not an active peer) Num active Peers: {}\n{}",
                   peer_id,
                   self.active_peers.len(),
                   message
            );
            false
        }
    }

    /// Return whether the message is sent or not.
    fn send_message_to_account_or_peer_or_hash(
        &mut self,
        ctx: &mut Context<Self>,
        target: &AccountOrPeerIdOrHash,
        msg: RoutedMessageBody,
    ) -> bool {
        match target {
            AccountOrPeerIdOrHash::AccountId(account_id) => {
                self.send_message_to_account(ctx, account_id, msg)
            }
            peer_or_hash @ AccountOrPeerIdOrHash::PeerId(_)
            | peer_or_hash @ AccountOrPeerIdOrHash::Hash(_) => self.send_message_to_peer(
                ctx,
                RawRoutedMessage { target: peer_or_hash.clone(), body: msg },
            ),
        }
    }

    /// Route signed message to target peer.
    /// Return whether the message is sent or not.
    fn send_signed_message_to_peer(&mut self, ctx: &mut Context<Self>, msg: RoutedMessage) -> bool {
        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = &msg.target {
            if target == &self.my_peer_id {
                debug!(target: "network", "{:?} Drop signed message to myself ({:?}). Message: {:?}.", self.config.account_id, self.my_peer_id, msg);
                return false;
            }
        }

        match self.routing_table_view.find_route(&msg.target) {
            Ok(peer_id) => {
                // Remember if we expect a response for this message.
                if msg.author == self.my_peer_id && msg.expect_response() {
                    trace!(target: "network", "initiate route back {:?}", msg);
                    self.routing_table_view.add_route_back(msg.hash(), self.my_peer_id.clone());
                }

                self.send_message(ctx, peer_id, PeerMessage::Routed(msg))
            }
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                self.network_metrics.inc(
                    NetworkMetrics::peer_message_dropped(strum::AsStaticRef::as_static(&msg.body))
                        .as_str(),
                );

                debug!(target: "network", "{:?} Drop signed message to {:?} Reason {:?}. Num known peers: {} Message {:?}",
                      self.config.account_id,
                      msg.target,
                      find_route_error,
                      self.routing_table_view.peer_forwarding.len(),
                      msg.body,
                );
                false
            }
        }
    }

    /// Route message to target peer.
    /// Return whether the message is sent or not.
    fn send_message_to_peer(&mut self, ctx: &mut Context<Self>, msg: RawRoutedMessage) -> bool {
        let msg = self.sign_routed_message(msg);
        self.send_signed_message_to_peer(ctx, msg)
    }

    /// Send message to specific account.
    /// Return whether the message is sent or not.
    fn send_message_to_account(
        &mut self,
        ctx: &mut Context<Self>,
        account_id: &AccountId,
        msg: RoutedMessageBody,
    ) -> bool {
        let target = match self.routing_table_view.account_owner(account_id) {
            Ok(peer_id) => peer_id,
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                metrics::DROP_MESSAGE_UNKNOWN_ACCOUNT.inc();
                debug!(target: "network", "{:?} Drop message to {} Reason {:?}. Message {:?}",
                       self.config.account_id,
                       account_id,
                       find_route_error,
                       msg,
                );
                trace!(target: "network", "Known peers: {:?}", self.routing_table_view.get_accounts_keys());
                return false;
            }
        };

        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body: msg };
        self.send_message_to_peer(ctx, msg)
    }

    fn sign_routed_message(&self, msg: RawRoutedMessage) -> RoutedMessage {
        msg.sign(self.my_peer_id.clone(), &self.config.secret_key, self.config.routed_message_ttl)
    }

    // Determine if the given target is referring to us.
    fn message_for_me(&mut self, target: &PeerIdOrHash) -> bool {
        match target {
            PeerIdOrHash::PeerId(peer_id) => peer_id == &self.my_peer_id,
            PeerIdOrHash::Hash(hash) => {
                self.routing_table_view.compare_route_back(*hash, &self.my_peer_id)
            }
        }
    }

    fn propose_edge(&self, peer1: PeerId, with_nonce: Option<u64>) -> EdgeInfo {
        let key = Edge::make_key(self.my_peer_id.clone(), peer1.clone());

        // When we create a new edge we increase the latest nonce by 2 in case we miss a removal
        // proposal from our partner.
        let nonce = with_nonce.unwrap_or_else(|| {
            self.routing_table_view
                .get_edge(self.my_peer_id.clone(), peer1)
                .map_or(1, |edge| edge.next())
        });

        EdgeInfo::new(&key.0, &key.1, nonce, &self.config.secret_key)
    }

    // Ping pong useful functions.

    // for unit tests
    fn send_ping(&mut self, ctx: &mut Context<Self>, nonce: usize, target: PeerId) {
        let body =
            RoutedMessageBody::Ping(Ping { nonce: nonce as u64, source: self.my_peer_id.clone() });
        self.routing_table_view.sending_ping(nonce, target.clone());
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(ctx, msg);
    }

    fn send_pong(&mut self, ctx: &mut Context<Self>, nonce: usize, target: CryptoHash) {
        let body =
            RoutedMessageBody::Pong(Pong { nonce: nonce as u64, source: self.my_peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(ctx, msg);
    }

    fn handle_ping(&mut self, ctx: &mut Context<Self>, ping: Ping, hash: CryptoHash) {
        self.send_pong(ctx, ping.nonce as usize, hash);
        self.routing_table_view.add_ping(ping);
    }

    /// Handle pong messages. Add pong temporary to the routing table, mostly used for testing.
    fn handle_pong(&mut self, _ctx: &mut Context<Self>, pong: Pong) {
        #[allow(unused_variables)]
        let latency = self.routing_table_view.add_pong(pong);
    }

    pub(crate) fn get_network_info(&mut self) -> NetworkInfo {
        let (sent_bytes_per_sec, received_bytes_per_sec) = self.get_total_bytes_per_sec();
        NetworkInfo {
            active_peers: self
                .active_peers
                .values()
                .map(|a| a.full_peer_info.clone())
                .collect::<Vec<_>>(),
            num_active_peers: self.num_active_peers(),
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

impl Actor for PeerManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start server if address provided.
        if let Some(server_addr) = self.config.addr {
            // TODO: for now crashes if server didn't start.

            ctx.spawn(TcpListener::bind(server_addr).into_actor(self).then(
                move |listener, act, ctx| {
                    let listener = listener.unwrap();
                    let incoming = IncomingCrutch {
                        listener: tokio_stream::wrappers::TcpListenerStream::new(listener),
                    };
                    info!(target: "stats", "Server listening at {}@{}", act.my_peer_id, server_addr);
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
            ));
        }

        // Periodically push network information to client.
        self.push_network_info_trigger(ctx, self.config.push_info_period);

        // Periodically starts peer monitoring.
        self.monitor_peers_trigger(ctx, self.config.bootstrap_peers_period);

        // Periodically starts active peer stats querying.
        self.monitor_peer_stats_trigger(ctx, self.config.peer_stats_period);

        // Periodically reads valid edges from `EdgesVerifierActor` and broadcast.
        self.broadcast_validated_edges_trigger(ctx, BROADCAST_VALIDATED_EDGES_INTERVAL);

        // Periodically updates routing table and prune edges that are no longer reachable.
        self.update_routing_table_trigger(ctx, UPDATE_ROUTING_TABLE_INTERVAL);
    }

    /// Try to gracefully disconnect from active peers.
    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        let msg = SendMessage { message: PeerMessage::Disconnect };

        for (_, active_peer) in self.active_peers.iter() {
            active_peer.addr.do_send(msg.clone());
        }

        self.routing_table_addr
            .send(StopMsg {})
            .into_actor(self)
            .then(move |_, _, _| actix::fut::ready(()))
            .spawn(ctx);

        Running::Stop
    }
}

impl PeerManagerActor {
    #[perf]
    fn handle_msg_network_requests(
        &mut self,
        msg: NetworkRequests,
        ctx: &mut Context<Self>,
    ) -> NetworkResponses {
        #[cfg(feature = "delay_detector")]
        let _d =
            delay_detector::DelayDetector::new(format!("network request {}", msg.as_ref()).into());
        match msg {
            NetworkRequests::Block { block } => {
                self.broadcast_message(ctx, SendMessage { message: PeerMessage::Block(block) });
                NetworkResponses::NoResponse
            }
            NetworkRequests::Approval { approval_message } => {
                self.send_message_to_account(
                    ctx,
                    &approval_message.target,
                    RoutedMessageBody::BlockApproval(approval_message.approval),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                if self.send_message(ctx, peer_id, PeerMessage::BlockRequest(hash)) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if self.send_message(ctx, peer_id, PeerMessage::BlockHeadersRequest(hashes)) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, target } => {
                if self.send_message_to_account_or_peer_or_hash(
                    ctx,
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
                    ctx,
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
                if self.send_message_to_peer(
                    ctx,
                    RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(route_back), body },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::EpochSyncRequest { peer_id, epoch_id } => {
                if self.send_message(ctx, peer_id, PeerMessage::EpochSyncRequest(epoch_id)) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::EpochSyncFinalizationRequest { peer_id, epoch_id } => {
                if self.send_message(
                    ctx,
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
                self.announce_account(ctx, announce_account);
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
                                ctx,
                                account_id,
                                RoutedMessageBody::PartialEncodedChunkRequest(request.clone()),
                            ) {
                                success = true;
                                break;
                            }
                        }
                    } else {
                        let mut matching_peers = vec![];
                        for (peer_id, active_peer) in self.active_peers.iter() {
                            if (active_peer.full_peer_info.chain_info.archival
                                || !target.only_archival)
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
                            if self.send_message_to_peer(
                                ctx,
                                RawRoutedMessage {
                                    target: AccountOrPeerIdOrHash::PeerId(matching_peer.clone()),
                                    body: RoutedMessageBody::PartialEncodedChunkRequest(
                                        request.clone(),
                                    ),
                                },
                            ) {
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
                if self.send_message_to_peer(
                    ctx,
                    RawRoutedMessage {
                        target: AccountOrPeerIdOrHash::Hash(route_back),
                        body: RoutedMessageBody::PartialEncodedChunkResponse(response),
                    },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                if self.send_message_to_account(ctx, &account_id, partial_encoded_chunk.into()) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                if self.send_message_to_account(
                    ctx,
                    &account_id,
                    RoutedMessageBody::PartialEncodedChunkForward(forward),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ForwardTx(account_id, tx) => {
                if self.send_message_to_account(ctx, &account_id, RoutedMessageBody::ForwardTx(tx))
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::TxStatus(account_id, signer_account_id, tx_hash) => {
                if self.send_message_to_account(
                    ctx,
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
                    ctx,
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
                    ctx,
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
            NetworkRequests::Sync { peer_id, sync_data } => {
                // Process edges and add new edges to the routing table. Also broadcast new edges.
                let SyncData { edges, accounts } = sync_data;

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
                                act.broadcast_accounts(ctx, accounts);
                            }
                            _ => {
                                debug!(target: "network", "Received invalid account confirmation from client.");
                            }
                        }
                        actix::fut::ready(())
                    }).spawn(ctx);

                self.validate_edges_and_add_to_routing_table(ctx, peer_id, edges);

                NetworkResponses::NoResponse
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            NetworkRequests::IbfMessage { peer_id, ibf_msg } => match ibf_msg {
                RoutingSyncV2::Version2(ibf_msg) => {
                    if let Some(addr) = self.active_peers.get(&peer_id).map(|p| p.addr.clone()) {
                        self.process_ibf_msg(ctx, &peer_id, ibf_msg, addr)
                    }
                    NetworkResponses::NoResponse
                }
            },
            NetworkRequests::Challenge(challenge) => {
                // TODO(illia): smarter routing?
                self.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::Challenge(challenge) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::RequestUpdateNonce(peer_id, edge_info) => {
                if Edge::partial_verify(self.my_peer_id.clone(), peer_id.clone(), &edge_info) {
                    if let Some(cur_edge) =
                        self.routing_table_view.get_edge(self.my_peer_id.clone(), peer_id.clone())
                    {
                        if cur_edge.edge_type() == EdgeType::Added
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
                if edge.contains_peer(&self.my_peer_id) && edge.verify() {
                    let key = edge.key();
                    if self.routing_table_view.is_local_edge_newer(key, edge.nonce()) {
                        let other = edge.other(&self.my_peer_id).unwrap();
                        if let Some(nonce) = self.local_peer_pending_update_nonce_request.get(other)
                        {
                            if edge.nonce() >= *nonce {
                                self.local_peer_pending_update_nonce_request.remove(other);
                            }
                        }
                    }
                    self.add_verified_edges_to_routing_table(ctx, vec![edge.clone()], false);
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            // For unit tests
            NetworkRequests::PingTo(nonce, target) => {
                self.send_ping(ctx, nonce, target);
                NetworkResponses::NoResponse
            }
            // For unit tests
            NetworkRequests::FetchPingPongInfo => {
                let (pings, pongs) = self.routing_table_view.fetch_ping_pong();
                NetworkResponses::PingPongInfo { pings, pongs }
            }
        }
    }

    #[cfg(all(feature = "test_features", feature = "protocol_feature_routing_exchange_algorithm"))]
    #[perf]
    fn handle_msg_start_routing_table_sync(
        &mut self,
        msg: crate::types::StartRoutingTableSync,
        ctx: &mut Context<Self>,
    ) {
        if let Some(active_peer) = self.active_peers.get(&msg.peer_id) {
            let addr = active_peer.addr.clone();
            self.initialize_routing_table_exchange(msg.peer_id, PeerType::Inbound, addr, ctx);
        }
    }

    #[cfg(feature = "test_features")]
    #[perf]
    fn handle_msg_set_adv_options(&mut self, msg: SetAdvOptions, _ctx: &mut Context<Self>) {
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
        msg: crate::types::SetRoutingTable,
        ctx: &mut Context<Self>,
    ) {
        if let Some(add_edges) = msg.add_edges {
            debug!(target: "network", "test_features add_edges {}", add_edges.len());
            self.add_verified_edges_to_routing_table(ctx, add_edges, false);
        }
        if let Some(remove_edges) = msg.remove_edges {
            debug!(target: "network", "test_features remove_edges {}", remove_edges.len());
            self.adv_remove_edges_from_routing_table(ctx, remove_edges);
        }
        if let Some(true) = msg.prune_edges {
            debug!(target: "network", "test_features prune_edges");
            self.update_routing_table_and_prune_edges(ctx, Prune::PruneNow, Duration::from_secs(2));
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

    #[perf]
    fn handle_msg_get_peer_id(
        &mut self,
        msg: GetPeerId,
        _ctx: &mut Context<Self>,
    ) -> GetPeerIdResult {
        GetPeerIdResult { peer_id: self.my_peer_id.clone() }
    }

    #[perf]
    fn handle_msg_outbound_tcp_connect(
        &mut self,
        msg: OutboundTcpConnect,
        ctx: &mut Context<Self>,
    ) {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("outbound tcp connect".into());
        debug!(target: "network", "Trying to connect to {}", msg.peer_info);
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
                            debug!(target: "network", "Connecting to {}", msg.peer_info);
                            let edge_info = act.propose_edge(msg.peer_info.id.clone(), None);

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
                            info!(target: "network", "Error connecting to {}: {}", addr, err);
                            act.outgoing_peers.remove(&msg.peer_info.id);
                            actix::fut::ready(())
                        }
                    },
                    Err(err) => {
                        info!(target: "network", "Error connecting to {}: {}", addr, err);
                        act.outgoing_peers.remove(&msg.peer_info.id);
                        actix::fut::ready(())
                    }
                })
                .wait(ctx);
        } else {
            warn!(target: "network", "Trying to connect to peer with no public address: {:?}", msg.peer_info);
        }
    }

    #[perf]
    fn handle_msg_consolidate(
        &mut self,
        msg: Consolidate,
        ctx: &mut Context<Self>,
    ) -> ConsolidateResponse {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("consolidate".into());

        // Check if this is a blacklisted peer.
        if msg.peer_info.addr.as_ref().map_or(true, |addr| self.is_blacklisted(addr)) {
            debug!(target: "network", "Dropping connection from blacklisted peer or unknown address: {:?}", msg.peer_info);
            return ConsolidateResponse::Reject;
        }

        if self.peer_store.is_banned(&msg.peer_info.id) {
            debug!(target: "network", "Dropping connection from banned peer: {:?}", msg.peer_info.id);
            return ConsolidateResponse::Reject;
        }

        // We already connected to this peer.
        if self.active_peers.contains_key(&msg.peer_info.id) {
            debug!(target: "network", "Dropping handshake (Active Peer). {:?} {:?}", self.my_peer_id, msg.peer_info.id);
            return ConsolidateResponse::Reject;
        }

        // This is incoming connection but we have this peer already in outgoing.
        // This only happens when both of us connect at the same time, break tie using higher peer id.
        if msg.peer_type == PeerType::Inbound && self.outgoing_peers.contains(&msg.peer_info.id) {
            // We pick connection that has lower id.
            if msg.peer_info.id > self.my_peer_id {
                debug!(target: "network", "Dropping handshake (Tied). {:?} {:?}", self.my_peer_id, msg.peer_info.id);
                return ConsolidateResponse::Reject;
            }
        }

        if msg.peer_type == PeerType::Inbound && !self.is_inbound_allowed() {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network", "Inbound connection dropped (network at max capacity).");
            return ConsolidateResponse::Reject;
        }

        if msg.other_edge_info.nonce == 0 {
            debug!(target: "network", "Invalid nonce. It must be greater than 0. nonce={}", msg.other_edge_info.nonce);
            return ConsolidateResponse::Reject;
        }

        let last_edge =
            self.routing_table_view.get_edge(self.my_peer_id.clone(), msg.peer_info.id.clone());
        let last_nonce = last_edge.as_ref().map_or(0, |edge| edge.nonce());

        // Check that the received nonce is greater than the current nonce of this connection.
        if last_nonce >= msg.other_edge_info.nonce {
            debug!(target: "network", "Too low nonce. ({} <= {}) {:?} {:?}", msg.other_edge_info.nonce, last_nonce, self.my_peer_id, msg.peer_info.id);
            // If the check fails don't allow this connection.
            return ConsolidateResponse::InvalidNonce(last_edge.cloned().map(Box::new).unwrap());
        }

        if msg.other_edge_info.nonce >= Edge::next_nonce(last_nonce) + EDGE_NONCE_BUMP_ALLOWED {
            debug!(target: "network", "Too large nonce. ({} >= {} + {}) {:?} {:?}", msg.other_edge_info.nonce, last_nonce, EDGE_NONCE_BUMP_ALLOWED, self.my_peer_id, msg.peer_info.id);
            return ConsolidateResponse::Reject;
        }

        let require_response = msg.this_edge_info.is_none();

        let edge_info = msg.this_edge_info.clone().unwrap_or_else(|| {
            self.propose_edge(msg.peer_info.id.clone(), Some(msg.other_edge_info.nonce))
        });

        let edge_info_response = if require_response { Some(edge_info.clone()) } else { None };

        // TODO: double check that address is connectable and add account id.
        self.register_peer(
            FullPeerInfo {
                peer_info: msg.peer_info,
                chain_info: msg.chain_info,
                edge_info: msg.other_edge_info,
            },
            edge_info,
            msg.peer_type,
            msg.actor,
            msg.peer_protocol_version,
            ctx,
        );

        ConsolidateResponse::Accept(edge_info_response)
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
    fn handle_msg_peers_request(
        &mut self,
        msg: PeersRequest,
        _ctx: &mut Context<Self>,
    ) -> PeerRequestResult {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("peers request".into());
        PeerRequestResult { peers: self.peer_store.healthy_peers(self.config.max_send_peers) }
    }

    fn handle_msg_peers_response(&mut self, msg: PeersResponse, _ctx: &mut Context<Self>) {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new("peers response".into());
        unwrap_or_error!(
            self.peer_store.add_indirect_peers(
                msg.peers.into_iter().filter(|peer_info| peer_info.id != self.my_peer_id).collect()
            ),
            "Fail to update peer store"
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

        let result = self.handle(msg, ctx);

        // TODO(#5155) Add support for DeepSizeOf to result
        ActixMessageResponse::new(result, ThrottleToken::new(throttle_token.into_inner(), 0))
    }
}

impl Handler<PeerManagerMessageRequest> for PeerManagerActor {
    type Result = PeerManagerMessageResponse;

    fn handle(&mut self, msg: PeerManagerMessageRequest, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            PeerManagerMessageRequest::RoutedMessageFrom(msg) => {
                PeerManagerMessageResponse::RoutedMessageFrom(self.handle_msg_routed_from(msg, ctx))
            }
            PeerManagerMessageRequest::NetworkRequests(msg) => {
                PeerManagerMessageResponse::NetworkResponses(
                    self.handle_msg_network_requests(msg, ctx),
                )
            }
            PeerManagerMessageRequest::Consolidate(msg) => {
                PeerManagerMessageResponse::ConsolidateResponse(
                    self.handle_msg_consolidate(msg, ctx),
                )
            }
            PeerManagerMessageRequest::PeersRequest(msg) => {
                PeerManagerMessageResponse::PeerRequestResult(
                    self.handle_msg_peers_request(msg, ctx),
                )
            }
            PeerManagerMessageRequest::PeersResponse(msg) => {
                self.handle_msg_peers_response(msg, ctx);
                PeerManagerMessageResponse::PeersResponseResult(())
            }
            PeerManagerMessageRequest::PeerRequest(msg) => {
                PeerManagerMessageResponse::PeerResponse(self.handle_msg_peer_request(msg, ctx))
            }
            PeerManagerMessageRequest::GetPeerId(msg) => {
                PeerManagerMessageResponse::GetPeerIdResult(self.handle_msg_get_peer_id(msg, ctx))
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
                PeerManagerMessageResponse::StartRoutingTableSync(
                    self.handle_msg_start_routing_table_sync(msg, ctx),
                )
            }
            #[cfg(feature = "test_features")]
            PeerManagerMessageRequest::SetAdvOptions(msg) => {
                PeerManagerMessageResponse::SetAdvOptions(self.handle_msg_set_adv_options(msg, ctx))
            }
            #[cfg(feature = "test_features")]
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            PeerManagerMessageRequest::SetRoutingTable(msg) => {
                PeerManagerMessageResponse::SetRoutingTable(
                    self.handle_msg_set_routing_table(msg, ctx),
                )
            }
        }
    }
}

/// "Return" true if this message is for this peer and should be sent to the client.
/// Otherwise try to route this message to the final receiver and return false.
impl PeerManagerActor {
    fn handle_msg_routed_from(&mut self, msg: RoutedMessageFrom, ctx: &mut Context<Self>) -> bool {
        #[cfg(feature = "delay_detector")]
        let _d = delay_detector::DelayDetector::new(
            format!("routed message from {}", strum::AsStaticRef::as_static(&msg.msg.body)).into(),
        );
        let RoutedMessageFrom { mut msg, from } = msg;

        if msg.expect_response() {
            trace!(target: "network", "Received peer message that requires route back: {}", PeerMessage::Routed(msg.clone()));
            self.routing_table_view.add_route_back(msg.hash(), from.clone());
        }

        if self.message_for_me(&msg.target) {
            // Handle Ping and Pong message if they are for us without sending to client.
            // i.e. Return false in case of Ping and Pong
            match &msg.body {
                RoutedMessageBody::Ping(ping) => self.handle_ping(ctx, ping.clone(), msg.hash()),
                RoutedMessageBody::Pong(pong) => self.handle_pong(ctx, pong.clone()),
                _ => return true,
            }

            false
        } else {
            if msg.decrease_ttl() {
                self.send_signed_message_to_peer(ctx, msg);
            } else {
                warn!(target: "network", "Message dropped because TTL reached 0. Message: {:?} From: {:?}", msg, from);
            }
            false
        }
    }
}

impl PeerManagerActor {
    fn handle_msg_peer_request(
        &mut self,
        msg: PeerRequest,
        ctx: &mut Context<Self>,
    ) -> PeerResponse {
        #[cfg(feature = "delay_detector")]
        let _d =
            delay_detector::DelayDetector::new(format!("peer request {}", msg.as_ref()).into());
        match msg {
            PeerRequest::UpdateEdge((peer, nonce)) => {
                PeerResponse::UpdatedEdge(self.propose_edge(peer, Some(nonce)))
            }
            PeerRequest::RouteBack(body, target) => {
                trace!(target: "network", "Sending message to route back: {:?}", target);
                self.send_message_to_peer(
                    ctx,
                    RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body: *body },
                );
                PeerResponse::NoResponse
            }
            PeerRequest::UpdatePeerInfo(peer_info) => {
                if let Err(err) = self.peer_store.add_trusted_peer(peer_info, TrustLevel::Direct) {
                    error!(target: "network", "Fail to update peer store: {}", err);
                }
                PeerResponse::NoResponse
            }
            PeerRequest::ReceivedMessage(peer_id, last_time_received_message) => {
                if let Some(active_peer) = self.active_peers.get_mut(&peer_id) {
                    active_peer.last_time_received_message = last_time_received_message;
                }
                PeerResponse::NoResponse
            }
        }
    }
}

#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
impl PeerManagerActor {
    fn process_ibf_msg(
        &mut self,
        ctx: &mut Context<PeerManagerActor>,
        peer_id: &PeerId,
        mut ibf_msg: RoutingVersion2,
        addr: Addr<PeerActor>,
    ) {
        let mut edges: Vec<Edge> = Vec::new();
        std::mem::swap(&mut edges, &mut ibf_msg.edges);
        self.validate_edges_and_add_to_routing_table(ctx, peer_id.clone(), edges);
        self.routing_table_addr
            .send(RoutingTableMessages::ProcessIbfMessage { peer_id: peer_id.clone(), ibf_msg })
            .into_actor(self)
            .map(move |response, _act: &mut PeerManagerActor, _ctx| match response {
                Ok(RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                    ibf_msg: response_ibf_msg,
                }) => {
                    if let Some(response_ibf_msg) = response_ibf_msg {
                        let _ = addr.do_send(SendMessage {
                            message: PeerMessage::RoutingTableSyncV2(RoutingSyncV2::Version2(
                                response_ibf_msg,
                            )),
                        });
                    }
                }
                _ => error!(target: "network", "expected ProcessIbfMessageResponse"),
            })
            .spawn(ctx);
    }
}
