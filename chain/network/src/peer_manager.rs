use rand::seq::{IteratorRandom, SliceRandom};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Duration, Instant};

use actix::{
    Actor, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner, Handler,
    Recipient, Running, StreamHandler, SyncArbiter, SyncContext, WrapFuture,
};
use chrono::Utc;
use futures::task::Poll;
use futures::{future, Stream, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, trace, warn};

use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use near_primitives::utils::from_timestamp;
use near_store::Store;

use crate::codec::Codec;
use crate::metrics;
use crate::peer::Peer;
use crate::peer_store::{PeerStore, TrustLevel};
#[cfg(feature = "metric_recorder")]
use crate::recorder::{MetricRecorder, PeerMessageMetadata};
use crate::routing::{
    Edge, EdgeInfo, EdgeType, EdgeVerifierHelper, ProcessEdgeResult, RoutingTable, MAX_NUM_PEERS,
};
use crate::types::{
    AccountOrPeerIdOrHash, Ban, BlockedPorts, Consolidate, ConsolidateResponse, FullPeerInfo,
    InboundTcpConnect, KnownPeerStatus, KnownProducer, NetworkInfo, NetworkViewClientMessages,
    NetworkViewClientResponses, OutboundTcpConnect, PeerIdOrHash, PeerList, PeerManagerRequest,
    PeerMessage, PeerRequest, PeerResponse, PeerType, PeersRequest, PeersResponse, Ping, Pong,
    QueryPeerStats, RawRoutedMessage, ReasonForBan, RoutedMessage, RoutedMessageBody,
    RoutedMessageFrom, SendMessage, StateResponseInfo, SyncData, Unregister,
};
use crate::types::{
    EdgeList, KnownPeerState, NetworkClientMessages, NetworkConfig, NetworkRequests,
    NetworkResponses, PeerInfo,
};
#[cfg(feature = "delay_detector")]
use delay_detector::DelayDetector;
use metrics::NetworkMetrics;
use near_performance_metrics::framed_write::FramedWrite;
use near_performance_metrics_macros::perf;
use rand::thread_rng;
use std::cmp::max;

/// How often to request peers from active peers.
const REQUEST_PEERS_SECS: u64 = 60;
/// How much time to wait (in milliseconds) after we send update nonce request before disconnecting.
/// This number should be large to handle pair of nodes with high latency.
const WAIT_ON_TRY_UPDATE_NONCE: u64 = 6_000;
/// If we see an edge between us and other peer, but this peer is not a current connection, wait this
/// timeout and in case it didn't become an active peer, broadcast edge removal update.
const WAIT_PEER_BEFORE_REMOVE: u64 = 6_000;
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
/// Time to wait before sending ping to all reachable peers.
#[cfg(feature = "metric_recorder")]
const WAIT_BEFORE_PING: u64 = 20_000;
/// Limit number of pending Peer actors to avoid OOM.
const LIMIT_PENDING_PEERS: usize = 60;
/// How ofter should we broadcast edges.
const BROADCAST_EDGES_INTERVAL: Duration = Duration::from_millis(50);
/// Maximum amount of time spend processing edges.
const BROAD_CAST_EDGES_MAX_WORK_ALLOVED: Duration = Duration::from_millis(50);

macro_rules! unwrap_or_error(($obj: expr, $error: expr) => (match $obj {
    Ok(result) => result,
    Err(err) => {
        error!(target: "network", "{}: {}", $error, err);
        return;
    }
}));

/// Contains information relevant to an active peer.
struct ActivePeer {
    addr: Addr<Peer>,
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

struct EdgeVerifier {}

impl Actor for EdgeVerifier {
    type Context = SyncContext<Self>;
}

impl Handler<EdgeList> for EdgeVerifier {
    type Result = bool;

    #[perf]
    fn handle(&mut self, msg: EdgeList, _ctx: &mut Self::Context) -> Self::Result {
        for edge in msg.edges {
            let key = (edge.peer0.clone(), edge.peer1.clone());
            if msg.edges_info_shared.lock().unwrap().get(&key).cloned().unwrap_or(0u64)
                >= edge.nonce
            {
                continue;
            }
            if !edge.verify() {
                return false;
            }
            {
                let mut guard = msg.edges_info_shared.lock().unwrap();
                let entry = guard.entry(key);

                let cur_nonce = entry.or_insert(edge.nonce);
                *cur_nonce = max(*cur_nonce, edge.nonce);
            }
            msg.sender.push(edge);
        }
        true
    }
}

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    /// Networking configuration.
    config: NetworkConfig,
    /// Peer information for this node.
    peer_id: PeerId,
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
    /// Routing table to keep track of account id
    routing_table: RoutingTable,
    /// Fields used for communicating with EdgeVerifier
    routing_table_exchange_helper: EdgeVerifierHelper,
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,
    /// Monitor peers attempts, used for fast checking in the beginning with exponential backoff.
    monitor_peers_attempts: u64,
    /// Active peers we have sent new edge update, but we haven't received response so far.
    pending_update_nonce_request: HashMap<PeerId, u64>,
    /// Dynamic Prometheus metrics
    network_metrics: NetworkMetrics,
    /// Store all collected metrics from a node.
    #[cfg(feature = "metric_recorder")]
    metric_recorder: MetricRecorder,
    edge_verifier_pool: Addr<EdgeVerifier>,
    txns_since_last_block: Arc<AtomicUsize>,
    pending_incoming_connections_counter: Arc<AtomicUsize>,
    peer_counter: Arc<AtomicUsize>,
    scheduled_routing_table_update: bool,
    edge_verifier_requests_in_progress: u64,
}

impl PeerManagerActor {
    pub fn new(
        store: Arc<Store>,
        config: NetworkConfig,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if config.max_num_peers as usize > MAX_NUM_PEERS {
            panic!("Exceeded max peer limit: {}", MAX_NUM_PEERS);
        }

        let peer_store = PeerStore::new(store.clone(), &config.boot_nodes)?;
        debug!(target: "network", "Found known peers: {} (boot nodes={})", peer_store.len(), config.boot_nodes.len());
        debug!(target: "network", "Blacklist: {:?}", config.blacklist);

        let edge_verifier_pool = SyncArbiter::start(4, || EdgeVerifier {});

        let me: PeerId = config.public_key.clone().into();
        let routing_table = RoutingTable::new(me.clone(), store);

        #[cfg(feature = "metric_recorder")]
        let metric_recorder = MetricRecorder::default().set_me(me.clone());

        let txns_since_last_block = Arc::new(AtomicUsize::new(0));

        Ok(PeerManagerActor {
            peer_id: me,
            config,
            client_addr,
            view_client_addr,
            peer_store,
            active_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            routing_table,
            routing_table_exchange_helper: Default::default(),
            monitor_peers_attempts: 0,
            started_connect_attempts: false,
            pending_update_nonce_request: HashMap::new(),
            network_metrics: NetworkMetrics::new(),
            edge_verifier_pool,
            #[cfg(feature = "metric_recorder")]
            metric_recorder,
            txns_since_last_block,
            pending_incoming_connections_counter: Arc::new(AtomicUsize::new(0)),
            peer_counter: Arc::new(AtomicUsize::new(0)),
            scheduled_routing_table_update: false,
            edge_verifier_requests_in_progress: 0,
        })
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
            self.routing_table.add_account(account.clone());
        }

        let new_data = SyncData { edges: Default::default(), accounts };

        if !new_data.is_empty() {
            self.broadcast_message(
                ctx,
                SendMessage { message: PeerMessage::RoutingTableSync(new_data) },
            )
        };
    }

    fn broadcast_edges(&mut self, ctx: &mut Context<PeerManagerActor>) {
        let me = self.peer_id.clone();

        let start = Instant::now();
        let mut new_edges = Vec::new();
        while let Some(edge) = self.routing_table_exchange_helper.edges_to_add_receiver.pop() {
            if let Some(cur_edge) =
                self.routing_table.get_edge(edge.peer0.clone(), edge.peer1.clone())
            {
                if cur_edge.nonce >= edge.nonce {
                    // We have newer update. Drop this.
                    continue;
                }
            }
            // Add new edge update to the routing table.
            self.process_edges(ctx, vec![edge.clone()]);
            if let Some(other) = edge.other(&me) {
                // We belong to this edge.
                if self.active_peers.contains_key(&other) {
                    // This is an active connection.
                    match edge.edge_type() {
                        EdgeType::Added => {}
                        EdgeType::Removed => {
                            // Try to update the nonce, and in case it fails removes the peer.
                            self.try_update_nonce(ctx, edge.clone(), other);
                            continue;
                        }
                    }
                } else {
                    match edge.edge_type() {
                        EdgeType::Added => {
                            self.wait_peer_or_remove(ctx, edge.clone());
                            continue;
                        }
                        EdgeType::Removed => {}
                    }
                }
            }
            new_edges.push(edge);
            if start.elapsed() >= BROAD_CAST_EDGES_MAX_WORK_ALLOVED {
                break;
            }
        }

        let new_data = SyncData { edges: new_edges, accounts: Default::default() };

        if !new_data.is_empty() {
            self.broadcast_message(
                ctx,
                SendMessage { message: PeerMessage::RoutingTableSync(new_data) },
            )
        };

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            BROADCAST_EDGES_INTERVAL,
            move |act, ctx| {
                act.broadcast_edges(ctx);
            },
        );
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
        addr: Addr<Peer>,
        ctx: &mut Context<Self>,
    ) {
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
            self.peer_id.clone(),   // source
            target_peer_id.clone(), // target
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
                last_time_peer_requested: Instant::now(),
                last_time_received_message: Instant::now(),
                connection_established_time: Instant::now(),
                peer_type,
            },
        );

        self.process_edges(ctx, vec![new_edge.clone()]);

        // TODO(MarX, #1363): Implement sync service. Right now all edges and known validators
        //  are sent during handshake.
        let known_edges = self.routing_table.get_edges();
        let known_accounts = self.routing_table.get_announce_accounts();
        let wait_for_sync = 1;

        // Start syncing network point of view. Wait until both parties are connected before start
        // sending messages.

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            Duration::from_secs(wait_for_sync),
            move |act, ctx| {
                let _ = addr.do_send(SendMessage {
                    message: PeerMessage::RoutingTableSync(SyncData {
                        edges: known_edges,
                        accounts: known_accounts,
                    }),
                });

                // Ask for peers list on connection.
                let _ = addr.do_send(SendMessage { message: PeerMessage::PeersRequest });
                if let Some(active_peer) = act.active_peers.get_mut(&target_peer_id) {
                    active_peer.last_time_peer_requested = Instant::now();
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
            },
        );
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
            if let Some(peer) = self.active_peers.get(&peer_id) {
                if peer.peer_type != peer_type {
                    // Don't remove the peer
                    return;
                }
            }
        }

        // If the last edge we have with this peer represent a connection addition, create the edge
        // update that represents the connection removal.
        self.active_peers.remove(&peer_id);

        if let Some(edge) = self.routing_table.get_edge(self.peer_id.clone(), peer_id.clone()) {
            if edge.edge_type() == EdgeType::Added {
                let edge_update = edge.remove_edge(self.peer_id.clone(), &self.config.secret_key);
                self.process_edges(ctx, vec![edge_update.clone()]);
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
        if let Some(peer) = self.active_peers.get(&peer_id) {
            let _ = peer.addr.do_send(PeerManagerRequest::BanPeer(ban_reason));
        } else {
            warn!(target: "network", "Try to ban a disconnected peer for {:?}: {:?}", ban_reason, peer_id);
            // Call `ban_peer` in peer manager to trigger action that persists information
            // of ban in disk.
            self.ban_peer(ctx, &peer_id, ban_reason);
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
        let peer_id = self.peer_id.clone();
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

        Peer::start_in_arbiter(&arbiter.handle(), move |ctx| {
            let (read, write) = tokio::io::split(stream);

            // TODO: check if peer is banned or known based on IP address and port.
            Peer::add_stream(
                FramedRead::new(read, Codec::new())
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

            Peer::new(
                PeerInfo { id: peer_id, addr: Some(server_addr), account_id },
                remote_addr,
                peer_info,
                peer_type,
                FramedWrite::new(write, Codec::new(), Codec::new(), ctx),
                handshake_timeout,
                recipient,
                client_addr,
                view_client_addr,
                edge_info,
                network_metrics,
                txns_since_last_block,
                peer_counter,
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
            if active_peer.last_time_peer_requested.elapsed().as_secs() > REQUEST_PEERS_SECS {
                active_peer.last_time_peer_requested = Instant::now();
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

    /// Add an edge update to the routing table and return if it is a new edge update.
    fn process_edges(&mut self, ctx: &mut Context<Self>, edges: Vec<Edge>) -> bool {
        let ProcessEdgeResult { new_edge } = self.routing_table.process_edges(edges);

        if !self.scheduled_routing_table_update {
            self.scheduled_routing_table_update = true;
            near_performance_metrics::actix::run_later(
                ctx,
                file!(),
                line!(),
                Duration::from_millis(1000),
                |act, _ctx| {
                    act.scheduled_routing_table_update = false;
                    // We only want to save prune edges if there are no pending requests to EdgeVerifier
                    act.routing_table.update(act.edge_verifier_requests_in_progress == 0);
                    #[cfg(feature = "metric_recorder")]
                    act.metric_recorder.set_graph(act.routing_table.get_raw_graph())
                },
            );
        }

        new_edge
    }

    fn wait_peer_or_remove(&mut self, ctx: &mut Context<Self>, edge: Edge) {
        // This edge says this is an active peer, which is currently not in the set of active peers.
        // Wait for some time to let the connection begin or broadcast edge removal instead.

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            Duration::from_millis(WAIT_PEER_BEFORE_REMOVE),
            move |act, ctx| {
                let other = edge.other(&act.peer_id).unwrap();
                if !act.active_peers.contains_key(&other) {
                    // Peer is still not active after waiting a timeout.
                    let new_edge = edge.remove_edge(act.peer_id.clone(), &act.config.secret_key);
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

    fn try_update_nonce(&mut self, ctx: &mut Context<Self>, edge: Edge, other: PeerId) {
        let nonce = edge.next();

        if let Some(last_nonce) = self.pending_update_nonce_request.get(&other) {
            if *last_nonce >= nonce {
                // We already tried to update an edge with equal or higher nonce.
                return;
            }
        }

        self.send_message(
            ctx,
            other.clone(),
            PeerMessage::RequestUpdateNonce(EdgeInfo::new(
                self.peer_id.clone(),
                other.clone(),
                nonce,
                &self.config.secret_key,
            )),
        );

        self.pending_update_nonce_request.insert(other.clone(), nonce);

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            Duration::from_millis(WAIT_ON_TRY_UPDATE_NONCE),
            move |act, _ctx| {
                if let Some(cur_nonce) = act.pending_update_nonce_request.get(&other) {
                    if *cur_nonce == nonce {
                        if let Some(peer) = act.active_peers.get(&other) {
                            // Send disconnect signal to this peer if we haven't edge update.
                            peer.addr.do_send(PeerManagerRequest::UnregisterPeer);
                        }
                        act.pending_update_nonce_request.remove(&other);
                    }
                }
            },
        );
    }

    #[cfg(feature = "metric_recorder")]
    fn ping_all_peers(&mut self, ctx: &mut Context<Self>) {
        for peer_id in self.routing_table.reachable_peers().cloned().collect::<Vec<_>>() {
            let nonce = self.routing_table.get_ping(peer_id.clone());
            self.send_ping(ctx, nonce, peer_id);
        }

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            Duration::from_millis(WAIT_BEFORE_PING),
            move |act, ctx| {
                act.ping_all_peers(ctx);
            },
        );
    }

    /// Periodically query peer actors for latest weight and traffic info.
    fn monitor_peer_stats(&mut self, ctx: &mut Context<Self>) {
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

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            self.config.peer_stats_period,
            move |act, ctx| {
                act.monitor_peer_stats(ctx);
            },
        );
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
                    if safe_set.contains(&peer_id) {
                        None
                    } else {
                        Some(peer_id.clone())
                    }
                },
            )
            .collect::<Vec<_>>();

        if let Some(peer_id) = candidates.choose(&mut rand::thread_rng()) {
            if let Some(active_peer) = self.active_peers.get(&peer_id) {
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
    fn monitor_peers(&mut self, ctx: &mut Context<Self>) {
        let mut to_unban = vec![];
        for (peer_id, peer_state) in self.peer_store.iter() {
            if let KnownPeerStatus::Banned(_, last_banned) = peer_state.status {
                let interval = unwrap_or_error!(
                    (Utc::now() - from_timestamp(last_banned)).to_std(),
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
                self.peer_id == peer_state.peer_info.id
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
                ctx.notify(OutboundTcpConnect { peer_info });
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
            self.config.bootstrap_peers_period.as_millis() as u64
        } else {
            (10f64 * EXPONENTIAL_BACKOFF_RATIO.powf(self.monitor_peers_attempts as f64)) as u64
        };

        self.monitor_peers_attempts =
            cmp::min(EXPONENTIAL_BACKOFF_LIMIT, self.monitor_peers_attempts + 1);

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            Duration::from_millis(wait),
            move |act, ctx| {
                act.monitor_peers(ctx);
            },
        );
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
        if !self.routing_table.contains_account(&announce_account) {
            self.routing_table.add_account(announce_account.clone());
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
                self.send_message_to_account(ctx, &account_id, msg)
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
            if target == &self.peer_id {
                debug!(target: "network", "{:?} Drop signed message to myself ({:?}). Message: {:?}.", self.config.account_id, self.peer_id, msg);
                return false;
            }
        }

        match self.routing_table.find_route(&msg.target) {
            Ok(peer_id) => {
                // Remember if we expect a response for this message.
                if msg.author == self.peer_id && msg.expect_response() {
                    trace!(target: "network", "initiate route back {:?}", msg);
                    self.routing_table.add_route_back(msg.hash(), self.peer_id.clone());
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
                      self.routing_table.peer_forwarding.len(),
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
        let target = match self.routing_table.account_owner(&account_id) {
            Ok(peer_id) => peer_id,
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                near_metrics::inc_counter(&metrics::DROP_MESSAGE_UNKNOWN_ACCOUNT);
                debug!(target: "network", "{:?} Drop message to {} Reason {:?}. Message {:?}",
                       self.config.account_id,
                       account_id,
                       find_route_error,
                       msg,
                );
                trace!(target: "network", "Known peers: {:?}", self.routing_table.get_accounts_keys());
                return false;
            }
        };

        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body: msg };
        self.send_message_to_peer(ctx, msg)
    }

    fn sign_routed_message(&self, msg: RawRoutedMessage) -> RoutedMessage {
        msg.sign(self.peer_id.clone(), &self.config.secret_key, self.config.routed_message_ttl)
    }

    // Determine if the given target is referring to us.
    fn message_for_me(&mut self, target: &PeerIdOrHash) -> bool {
        match target {
            PeerIdOrHash::PeerId(peer_id) => peer_id == &self.peer_id,
            PeerIdOrHash::Hash(hash) => {
                self.routing_table.compare_route_back(hash.clone(), &self.peer_id)
            }
        }
    }

    fn propose_edge(&self, peer1: PeerId, with_nonce: Option<u64>) -> EdgeInfo {
        let key = Edge::key(self.peer_id.clone(), peer1.clone());

        // When we create a new edge we increase the latest nonce by 2 in case we miss a removal
        // proposal from our partner.
        let nonce = with_nonce.unwrap_or_else(|| {
            self.routing_table.get_edge(self.peer_id.clone(), peer1).map_or(1, |edge| edge.next())
        });

        EdgeInfo::new(key.0, key.1, nonce, &self.config.secret_key)
    }

    // Ping pong useful functions.

    fn send_ping(&mut self, ctx: &mut Context<Self>, nonce: usize, target: PeerId) {
        let body =
            RoutedMessageBody::Ping(Ping { nonce: nonce as u64, source: self.peer_id.clone() });
        self.routing_table.sending_ping(nonce, target.clone());
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(ctx, msg);
    }

    fn send_pong(&mut self, ctx: &mut Context<Self>, nonce: usize, target: CryptoHash) {
        let body =
            RoutedMessageBody::Pong(Pong { nonce: nonce as u64, source: self.peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(ctx, msg);
    }

    fn handle_ping(&mut self, ctx: &mut Context<Self>, ping: Ping, hash: CryptoHash) {
        self.send_pong(ctx, ping.nonce as usize, hash);
        self.routing_table.add_ping(ping);
    }

    /// Handle pong messages. Add pong temporary to the routing table, mostly used for testing.
    /// If `metric_recorder` feature flag is enabled, save how much time passed since we sent ping.
    fn handle_pong(&mut self, _ctx: &mut Context<Self>, pong: Pong) {
        #[cfg(feature = "metric_recorder")]
        let source = pong.source.clone();
        #[allow(unused_variables)]
        let latency = self.routing_table.add_pong(pong);
        #[cfg(feature = "metric_recorder")]
        latency.and_then::<(), _>(|latency| {
            self.metric_recorder.add_latency(source, latency);
            None
        });
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
                .routing_table
                .get_announce_accounts()
                .iter()
                .map(|announce_account| KnownProducer {
                    account_id: announce_account.account_id.clone(),
                    peer_id: announce_account.peer_id.clone(),
                    // TODO: fill in the address.
                    addr: None,
                })
                .collect(),
            #[cfg(feature = "metric_recorder")]
            metric_recorder: self.metric_recorder.clone(),
            peer_counter: self.peer_counter.load(Ordering::SeqCst),
        }
    }

    fn push_network_info(&mut self, ctx: &mut Context<Self>) {
        let network_info = self.get_network_info();

        let _ = self.client_addr.do_send(NetworkClientMessages::NetworkInfo(network_info));

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            self.config.push_info_period,
            move |act, ctx| {
                act.push_network_info(ctx);
            },
        );
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

            let pending_incoming_connections_counter =
                self.pending_incoming_connections_counter.clone();
            let peer_counter = self.peer_counter.clone();
            let max_num_peers: usize = self.config.max_num_peers as usize;

            ctx.spawn(TcpListener::bind(server_addr).into_actor(self).then(
                move |listener, act, ctx| {
                    let listener = listener.unwrap();
                    let incoming = IncomingCrutch {
                        listener: tokio_stream::wrappers::TcpListenerStream::new(listener),
                    };
                    info!(target: "stats", "Server listening at {}@{}", act.peer_id, server_addr);

                    ctx.add_message_stream(incoming.filter_map(move |conn| {
                        if let Ok(conn) = conn {
                            if pending_incoming_connections_counter.load(Ordering::SeqCst)
                                + peer_counter.load(Ordering::SeqCst)
                                < max_num_peers + LIMIT_PENDING_PEERS
                            {
                                pending_incoming_connections_counter.fetch_add(1, Ordering::SeqCst);
                                return future::ready(Some(InboundTcpConnect::new(conn)));
                            }
                        }

                        future::ready(None)
                    }));
                    actix::fut::ready(())
                },
            ));
        }

        // Periodically push network information to client
        self.push_network_info(ctx);

        // Start peer monitoring.
        self.monitor_peers(ctx);

        // Start active peer stats querying.
        self.monitor_peer_stats(ctx);

        // Periodically ping all peers to determine latencies between pair of peers.
        #[cfg(feature = "metric_recorder")]
        self.ping_all_peers(ctx);

        self.broadcast_edges(ctx);
    }

    /// Try to gracefully disconnect from active peers.
    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        let msg = SendMessage { message: PeerMessage::Disconnect };

        for (_, active_peer) in self.active_peers.iter() {
            active_peer.addr.do_send(msg.clone());
        }

        Running::Stop
    }
}

impl Handler<NetworkRequests> for PeerManagerActor {
    type Result = NetworkResponses;

    #[perf]
    fn handle(&mut self, msg: NetworkRequests, ctx: &mut Context<Self>) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new(format!("network request {}", msg.as_ref()).into());
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
                                &account_id,
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
            NetworkRequests::FetchRoutingTable => {
                NetworkResponses::RoutingTableInfo(self.routing_table.info())
            }
            NetworkRequests::Sync { peer_id, sync_data } => {
                // Process edges and add new edges to the routing table. Also broadcast new edges.
                let SyncData { edges, accounts } = sync_data;

                // Filter known accounts before validating them.
                let accounts = accounts
                    .into_iter()
                    .filter_map(|announce_account| {
                        if let Some(current_announce_account) =
                            self.routing_table.get_announce(&announce_account.account_id)
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

                // Broadcast edges once EdgeVerifier finishes
                self.edge_verifier_requests_in_progress += 1;
                self.edge_verifier_pool
                    .send(EdgeList {
                        edges,
                        edges_info_shared: self
                            .routing_table_exchange_helper
                            .edges_info_shared
                            .clone(),
                        sender: self.routing_table_exchange_helper.edges_to_add_sender.clone(),
                    })
                    .into_actor(self)
                    .then(move |response, act, ctx| {
                        act.edge_verifier_requests_in_progress -= 1;
                        match response {
                            Ok(false) => act.try_ban_peer(ctx, &peer_id, ReasonForBan::InvalidEdge),
                            Ok(true) => {}
                            Err(err) => warn!(target: "network", "error validating edges: {}", err),
                        }
                        actix::fut::ready(())
                    })
                    .spawn(ctx);

                NetworkResponses::NoResponse
            }
            NetworkRequests::Challenge(challenge) => {
                // TODO(illia): smarter routing?
                self.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::Challenge(challenge) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::RequestUpdateNonce(peer_id, edge_info) => {
                if Edge::partial_verify(self.peer_id.clone(), peer_id.clone(), &edge_info) {
                    if let Some(cur_edge) =
                        self.routing_table.get_edge(self.peer_id.clone(), peer_id.clone())
                    {
                        if cur_edge.edge_type() == EdgeType::Added
                            && cur_edge.nonce >= edge_info.nonce
                        {
                            return NetworkResponses::EdgeUpdate(Box::new(cur_edge));
                        }
                    }

                    let new_edge = Edge::build_with_secret_key(
                        self.peer_id.clone(),
                        peer_id,
                        edge_info.nonce,
                        &self.config.secret_key,
                        edge_info.signature,
                    );

                    self.process_edges(ctx, vec![new_edge.clone()]);
                    NetworkResponses::EdgeUpdate(Box::new(new_edge))
                } else {
                    NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            NetworkRequests::ResponseUpdateNonce(edge) => {
                if edge.contains_peer(&self.peer_id) && edge.verify() {
                    if self.process_edges(ctx, vec![edge.clone()]) {
                        let other = edge.other(&self.peer_id).unwrap();
                        if let Some(nonce) = self.pending_update_nonce_request.get(&other) {
                            if edge.nonce >= *nonce {
                                self.pending_update_nonce_request.remove(&other);
                            }
                        }
                    }
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            NetworkRequests::PingTo(nonce, target) => {
                self.send_ping(ctx, nonce, target);
                NetworkResponses::NoResponse
            }
            NetworkRequests::FetchPingPongInfo => {
                let (pings, pongs) = self.routing_table.fetch_ping_pong();
                NetworkResponses::PingPongInfo { pings, pongs }
            }
        }
    }
}

impl Handler<InboundTcpConnect> for PeerManagerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: InboundTcpConnect, ctx: &mut Self::Context) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("inbound tcp connect".into());
        if self.is_inbound_allowed() {
            self.try_connect_peer(ctx.address(), msg.stream, PeerType::Inbound, None, None);
        } else {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network", "Inbound connection dropped (network at max capacity).");
        }
        self.pending_incoming_connections_counter.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Handler<OutboundTcpConnect> for PeerManagerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: OutboundTcpConnect, ctx: &mut Self::Context) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("outbound tcp connect".into());
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
}

impl Handler<Consolidate> for PeerManagerActor {
    type Result = ConsolidateResponse;

    #[perf]
    fn handle(&mut self, msg: Consolidate, ctx: &mut Self::Context) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("consolidate".into());
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
            debug!(target: "network", "Dropping handshake (Active Peer). {:?} {:?}", self.peer_id, msg.peer_info.id);
            return ConsolidateResponse::Reject;
        }

        // This is incoming connection but we have this peer already in outgoing.
        // This only happens when both of us connect at the same time, break tie using higher peer id.
        if msg.peer_type == PeerType::Inbound && self.outgoing_peers.contains(&msg.peer_info.id) {
            // We pick connection that has lower id.
            if msg.peer_info.id > self.peer_id {
                debug!(target: "network", "Dropping handshake (Tied). {:?} {:?}", self.peer_id, msg.peer_info.id);
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

        let last_edge = self.routing_table.get_edge(self.peer_id.clone(), msg.peer_info.id.clone());
        let last_nonce = last_edge.as_ref().map_or(0, |edge| edge.nonce);

        // Check that the received nonce is greater than the current nonce of this connection.
        if last_nonce >= msg.other_edge_info.nonce {
            debug!(target: "network", "Too low nonce. ({} <= {}) {:?} {:?}", msg.other_edge_info.nonce, last_nonce, self.peer_id, msg.peer_info.id);
            // If the check fails don't allow this connection.
            return ConsolidateResponse::InvalidNonce(last_edge.map(Box::new).unwrap());
        }

        if msg.other_edge_info.nonce >= Edge::next_nonce(last_nonce) + EDGE_NONCE_BUMP_ALLOWED {
            debug!(target: "network", "Too large nonce. ({} >= {} + {}) {:?} {:?}", msg.other_edge_info.nonce, last_nonce, EDGE_NONCE_BUMP_ALLOWED, self.peer_id, msg.peer_info.id);
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
            ctx,
        );

        return ConsolidateResponse::Accept(edge_info_response);
    }
}

impl Handler<Unregister> for PeerManagerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: Unregister, ctx: &mut Self::Context) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("unregister".into());
        self.unregister_peer(ctx, msg.peer_id, msg.peer_type, msg.remove_from_peer_store);
    }
}

impl Handler<Ban> for PeerManagerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: Ban, ctx: &mut Self::Context) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("ban".into());
        self.ban_peer(ctx, &msg.peer_id, msg.ban_reason);
    }
}

impl Handler<PeersRequest> for PeerManagerActor {
    type Result = PeerList;

    #[perf]
    fn handle(&mut self, msg: PeersRequest, _ctx: &mut Self::Context) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("peers request".into());
        PeerList { peers: self.peer_store.healthy_peers(self.config.max_send_peers) }
    }
}

impl Handler<PeersResponse> for PeerManagerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: PeersResponse, _ctx: &mut Self::Context) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("peers response".into());
        unwrap_or_error!(
            self.peer_store.add_indirect_peers(
                msg.peers.into_iter().filter(|peer_info| peer_info.id != self.peer_id).collect()
            ),
            "Fail to update peer store"
        );
    }
}

/// "Return" true if this message is for this peer and should be sent to the client.
/// Otherwise try to route this message to the final receiver and return false.
impl Handler<RoutedMessageFrom> for PeerManagerActor {
    type Result = bool;

    #[perf]
    fn handle(&mut self, msg: RoutedMessageFrom, ctx: &mut Self::Context) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new(
            format!("routed message from {}", strum::AsStaticRef::as_static(&msg.msg.body)).into(),
        );
        let RoutedMessageFrom { mut msg, from } = msg;

        if msg.expect_response() {
            trace!(target: "network", "Received peer message that requires route back: {}", PeerMessage::Routed(msg.clone()));
            self.routing_table.add_route_back(msg.hash(), from.clone());
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

impl Handler<RawRoutedMessage> for PeerManagerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: RawRoutedMessage, ctx: &mut Self::Context) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new(
            format!("raw routed message {}", strum::AsStaticRef::as_static(&msg.body)).into(),
        );
        if let AccountOrPeerIdOrHash::AccountId(target) = msg.target {
            self.send_message_to_account(ctx, &target, msg.body);
        } else {
            self.send_message_to_peer(ctx, msg);
        }
    }
}

impl Handler<PeerRequest> for PeerManagerActor {
    type Result = PeerResponse;

    #[perf]
    fn handle(&mut self, msg: PeerRequest, ctx: &mut Self::Context) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new(format!("peer request {}", msg.as_ref()).into());
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

#[cfg(feature = "metric_recorder")]
impl Handler<PeerMessageMetadata> for PeerManagerActor {
    type Result = ();
    #[perf]
    fn handle(&mut self, msg: PeerMessageMetadata, _ctx: &mut Self::Context) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("peer message metadata".into());
        self.metric_recorder.handle_peer_message(msg);
    }
}
