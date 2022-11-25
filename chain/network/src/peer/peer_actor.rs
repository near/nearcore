use crate::accounts_data;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::concurrency::demux;
use crate::network_protocol::{
    Edge, EdgeState, Encoding, ParsePeerMessageError, PartialEdgeInfo, PeerChainInfoV2, PeerInfo,
    RawRoutedMessage, RoutedMessageBody, RoutingTableUpdate, SyncAccountsData,
};
use crate::peer::stream;
use crate::peer::tracker::Tracker;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::{NetworkState, PRUNE_EDGES_AFTER};
use crate::peer_manager::peer_manager_actor::Event;
use crate::private_actix::{RegisterPeerError, SendMessage};
use crate::routing::edge::verify_nonce;
use crate::stats::metrics;
use crate::tcp;
use crate::time;
use crate::types::{
    BlockInfo, Handshake, HandshakeFailureReason, PeerIdOrHash, PeerMessage, PeerType, ReasonForBan,
};
use actix::fut::future::wrap_future;
use actix::{Actor as _, ActorContext as _, ActorFutureExt as _, AsyncContext as _};
use lru::LruCache;
use near_crypto::Signature;
use near_o11y::{handler_debug_span, log_assert, pretty, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::EpochId;
use near_primitives::utils::DisplayOption;
use near_primitives::version::{
    ProtocolVersion, PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument as _;

/// Maximum number of messages per minute from single peer.
// TODO(#5453): current limit is way to high due to us sending lots of messages during sync.
const MAX_PEER_MSG_PER_MIN: usize = usize::MAX;
/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: time::Duration = time::Duration::seconds(60);

/// Maximum number of transaction messages we will accept between block messages.
/// The purpose of this constant is to ensure we do not spend too much time deserializing and
/// dispatching transactions when we should be focusing on consensus-related messages.
const MAX_TRANSACTIONS_PER_BLOCK_MESSAGE: usize = 1000;
/// Limit cache size of 1000 messages
const ROUTED_MESSAGE_CACHE_SIZE: usize = 1000;
/// Duplicated messages will be dropped if routed through the same peer multiple times.
const DROP_DUPLICATED_MESSAGES_PERIOD: time::Duration = time::Duration::milliseconds(50);
/// How often to send the latest block to peers.
const SYNC_LATEST_BLOCK_INTERVAL: time::Duration = time::Duration::seconds(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionClosedEvent {
    pub(crate) stream_id: tcp::StreamId,
    pub(crate) reason: ClosingReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeStartedEvent {
    pub(crate) stream_id: tcp::StreamId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeCompletedEvent {
    pub(crate) stream_id: tcp::StreamId,
    pub(crate) edge: Edge,
}

#[derive(thiserror::Error, Clone, PartialEq, Eq, Debug)]
pub(crate) enum ClosingReason {
    #[error("too many inbound connections in connecting state")]
    TooManyInbound,
    #[error("outbound not allowed: {0}")]
    OutboundNotAllowed(connection::PoolError),

    #[error("peer banned: {0:?}")]
    Ban(ReasonForBan),
    #[error("handshake failed")]
    HandshakeFailed,
    #[error("rejected by PeerManager: {0:?}")]
    RejectedByPeerManager(RegisterPeerError),
    #[error("stream error")]
    StreamError,
    #[error("PeerManager requested to close the connection")]
    PeerManager,
    #[error("Received DisconnectMessage from peer")]
    DisconnectMessage,
    #[error("PeerActor stopped NOT via PeerActor::stop()")]
    Unknown,
}

pub(crate) struct PeerActor {
    clock: time::Clock,

    /// Shared state of the network module.
    network_state: Arc<NetworkState>,
    /// This node's id and address (either listening or socket address).
    my_node_info: PeerInfo,

    /// TEST-ONLY
    stream_id: crate::tcp::StreamId,
    /// Peer address from connection.
    peer_addr: SocketAddr,
    /// Peer type.
    peer_type: PeerType,

    /// Framed wrapper to send messages through the TCP connection.
    framed: stream::FramedStream<PeerActor>,

    /// Tracker for requests and responses.
    tracker: Arc<Mutex<Tracker>>,
    /// Network bandwidth stats.
    stats: Arc<connection::Stats>,
    /// Cache of recently routed messages, this allows us to drop duplicates
    routed_message_cache: LruCache<(PeerId, PeerIdOrHash, Signature), time::Instant>,
    /// Whether we detected support for protocol buffers during handshake.
    protocol_buffers_supported: bool,
    /// Whether the PeerActor should skip protobuf support detection and use
    /// a given encoding right away.
    force_encoding: Option<Encoding>,

    /// Peer status.
    peer_status: PeerStatus,
    closing_reason: Option<ClosingReason>,
    /// Peer id and info. Present when Ready,
    /// or (for outbound only) when Connecting.
    // TODO: move it to ConnectingStatus::Outbound.
    // When ready, use connection.peer_info instead.
    peer_info: DisplayOption<PeerInfo>,
}

impl Debug for PeerActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.my_node_info)
    }
}

#[derive(Clone, Debug)]
struct HandshakeSpec {
    /// ID of the peer on the other side of the connection.
    peer_id: PeerId,
    protocol_version: ProtocolVersion,
    partial_edge_info: PartialEdgeInfo,
}

impl PeerActor {
    pub(crate) fn spawn(
        clock: time::Clock,
        stream: tcp::Stream,
        force_encoding: Option<Encoding>,
        network_state: Arc<NetworkState>,
    ) -> anyhow::Result<actix::Addr<Self>> {
        let stream_id = stream.id();
        match Self::spawn_inner(clock, stream, force_encoding, network_state.clone()) {
            Ok(it) => Ok(it),
            Err(reason) => {
                network_state.config.event_sink.push(Event::ConnectionClosed(
                    ConnectionClosedEvent { stream_id, reason: reason.clone() },
                ));
                Err(reason.into())
            }
        }
    }

    fn spawn_inner(
        clock: time::Clock,
        stream: tcp::Stream,
        force_encoding: Option<Encoding>,
        network_state: Arc<NetworkState>,
    ) -> Result<actix::Addr<Self>, ClosingReason> {
        let connecting_status = match &stream.type_ {
            tcp::StreamType::Inbound => ConnectingStatus::Inbound(
                network_state
                    .inbound_handshake_permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ClosingReason::TooManyInbound)?,
            ),
            tcp::StreamType::Outbound { peer_id } => ConnectingStatus::Outbound {
                _permit: network_state
                    .tier2
                    .start_outbound(peer_id.clone())
                    .map_err(ClosingReason::OutboundNotAllowed)?,
                handshake_spec: HandshakeSpec {
                    partial_edge_info: network_state.propose_edge(&clock, peer_id, None),
                    protocol_version: PROTOCOL_VERSION,
                    peer_id: peer_id.clone(),
                },
            },
        };

        let my_node_info = PeerInfo {
            id: network_state.config.node_id(),
            addr: network_state.config.node_addr.clone(),
            account_id: network_state.config.validator.as_ref().map(|v| v.account_id()),
        };
        // Start PeerActor on separate thread.
        Ok(Self::start_in_arbiter(&actix::Arbiter::new().handle(), move |ctx| {
            let stats = Arc::new(connection::Stats::default());
            let stream_id = stream.id();
            let peer_addr = stream.peer_addr;
            let stream_type = stream.type_.clone();
            let framed = stream::FramedStream::spawn(ctx, stream, stats.clone());
            Self {
                closing_reason: None,
                clock,
                my_node_info,
                stream_id,
                peer_addr,
                peer_type: match &stream_type {
                    tcp::StreamType::Inbound => PeerType::Inbound,
                    tcp::StreamType::Outbound { .. } => PeerType::Outbound,
                },
                peer_status: PeerStatus::Connecting(connecting_status),
                framed,
                tracker: Default::default(),
                stats,
                routed_message_cache: LruCache::new(ROUTED_MESSAGE_CACHE_SIZE),
                protocol_buffers_supported: false,
                force_encoding,
                peer_info: match &stream_type {
                    tcp::StreamType::Inbound => None,
                    tcp::StreamType::Outbound { peer_id } => Some(PeerInfo {
                        id: peer_id.clone(),
                        addr: Some(peer_addr),
                        account_id: None,
                    }),
                }
                .into(),
                network_state,
            }
        }))
    }

    // Determines the encoding to use for communication with the peer.
    // It can be None while Handshake with the peer has not been finished yet.
    // In case it is None, both encodings are attempted for parsing, and each message
    // is sent twice.
    fn encoding(&self) -> Option<Encoding> {
        if self.force_encoding.is_some() {
            return self.force_encoding;
        }
        if self.protocol_buffers_supported {
            return Some(Encoding::Proto);
        }
        match self.peer_status {
            PeerStatus::Connecting { .. } => None,
            PeerStatus::Ready { .. } => Some(Encoding::Borsh),
        }
    }

    fn parse_message(&mut self, msg: &[u8]) -> Result<PeerMessage, ParsePeerMessageError> {
        if let Some(e) = self.encoding() {
            return PeerMessage::deserialize(e, msg);
        }
        if let Ok(msg) = PeerMessage::deserialize(Encoding::Proto, msg) {
            self.protocol_buffers_supported = true;
            return Ok(msg);
        }
        return PeerMessage::deserialize(Encoding::Borsh, msg);
    }

    fn send_message_or_log(&self, msg: &PeerMessage) {
        self.send_message(msg);
    }

    fn send_message(&self, msg: &PeerMessage) {
        if let (PeerStatus::Ready(conn), PeerMessage::PeersRequest) = (&self.peer_status, msg) {
            conn.last_time_peer_requested.store(Some(self.clock.now()));
        }
        if let Some(enc) = self.encoding() {
            return self.send_message_with_encoding(msg, enc);
        }
        self.send_message_with_encoding(msg, Encoding::Proto);
        self.send_message_with_encoding(msg, Encoding::Borsh);
    }

    fn send_message_with_encoding(&self, msg: &PeerMessage, enc: Encoding) {
        let msg_type: &str = msg.msg_variant();
        let _span = tracing::trace_span!(
            target: "network",
            "send_message_with_encoding",
            msg_type)
        .entered();
        // Skip sending block and headers if we received it or header from this peer.
        // Record block requests in tracker.
        match msg {
            // Temporarily disable this check because now the node needs to send block to its
            // peers to update its height at the peer. In the future we will introduce a new
            // peer message type for that and then we can enable this check again.
            //PeerMessage::Block(b) if self.tracker.lock().has_received(b.hash()) => return,
            PeerMessage::BlockRequest(h) => self.tracker.lock().push_request(*h),
            _ => (),
        };

        let bytes = msg.serialize(enc);
        self.tracker.lock().increment_sent(&self.clock, bytes.len() as u64);
        let bytes_len = bytes.len();
        tracing::trace!(target: "network", msg_len = bytes_len);
        self.framed.send(stream::Frame(bytes));
        metrics::PEER_DATA_SENT_BYTES.inc_by(bytes_len as u64);
        metrics::PEER_MESSAGE_SENT_BY_TYPE_TOTAL.with_label_values(&[msg_type]).inc();
        metrics::PEER_MESSAGE_SENT_BY_TYPE_BYTES
            .with_label_values(&[msg_type])
            .inc_by(bytes_len as u64);
    }

    fn send_handshake(&self, spec: HandshakeSpec) {
        let (height, tracked_shards) =
            if let Some(chain_info) = self.network_state.chain_info.load().as_ref() {
                (chain_info.block.header().height(), chain_info.tracked_shards.clone())
            } else {
                (0, vec![])
            };
        let handshake = Handshake {
            protocol_version: spec.protocol_version,
            oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
            sender_peer_id: self.network_state.config.node_id(),
            target_peer_id: spec.peer_id,
            sender_listen_port: self.network_state.config.node_addr.map(|a| a.port()),
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: self.network_state.genesis_id.clone(),
                // TODO: remove `height` from PeerChainInfo
                height,
                tracked_shards,
                archival: self.network_state.config.archive,
            },
            partial_edge_info: spec.partial_edge_info,
        };
        let msg = PeerMessage::Tier2Handshake(handshake);
        self.send_message_or_log(&msg);
    }

    fn stop(&mut self, ctx: &mut actix::Context<PeerActor>, reason: ClosingReason) {
        // Only the first call to stop sets the closing_reason.
        if self.closing_reason.is_none() {
            self.closing_reason = Some(reason);
        }
        ctx.stop();
    }

    /// `PeerId` of the current node.
    fn my_node_id(&self) -> &PeerId {
        &self.my_node_info.id
    }

    /// `PeerId` of the other node.
    fn other_peer_id(&self) -> Option<&PeerId> {
        self.peer_info.as_ref().as_ref().map(|peer_info| &peer_info.id)
    }

    /// Update stats when receiving msg
    fn update_stats_on_receiving_message(&mut self, msg_len: usize) {
        metrics::PEER_DATA_RECEIVED_BYTES.inc_by(msg_len as u64);
        metrics::PEER_MESSAGE_RECEIVED_TOTAL.inc();
        tracing::trace!(target: "network", msg_len);
        self.tracker.lock().increment_received(&self.clock, msg_len as u64);
    }

    fn process_handshake(
        &mut self,
        ctx: &mut <PeerActor as actix::Actor>::Context,
        handshake: Handshake,
    ) {
        tracing::debug!(target: "network", "{:?}: Received handshake {:?}", self.my_node_info.id, handshake);
        let cs = match &self.peer_status {
            PeerStatus::Connecting(it) => it,
            _ => panic!("process_handshake called in non-connecting state"),
        };
        match cs {
            ConnectingStatus::Outbound { handshake_spec: spec, .. } => {
                if handshake.protocol_version != spec.protocol_version {
                    tracing::warn!(target: "network", "Protocol version mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                if handshake.sender_chain_info.genesis_id != self.network_state.genesis_id {
                    tracing::warn!(target: "network", "Genesis mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                if handshake.sender_peer_id != spec.peer_id {
                    tracing::warn!(target: "network", "PeerId mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                if handshake.partial_edge_info.nonce != spec.partial_edge_info.nonce {
                    tracing::warn!(target: "network", "Nonce mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
            }
            ConnectingStatus::Inbound { .. } => {
                if PEER_MIN_ALLOWED_PROTOCOL_VERSION > handshake.protocol_version
                    || handshake.protocol_version > PROTOCOL_VERSION
                {
                    tracing::debug!(
                        target: "network",
                        version = handshake.protocol_version,
                        "Received connection from node with unsupported PROTOCOL_VERSION.");
                    self.send_message_or_log(&PeerMessage::HandshakeFailure(
                        self.my_node_info.clone(),
                        HandshakeFailureReason::ProtocolVersionMismatch {
                            version: PROTOCOL_VERSION,
                            oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
                        },
                    ));
                    return;
                }
                let genesis_id = self.network_state.genesis_id.clone();
                if handshake.sender_chain_info.genesis_id != genesis_id {
                    tracing::debug!(target: "network", "Received connection from node with different genesis.");
                    self.send_message_or_log(&PeerMessage::HandshakeFailure(
                        self.my_node_info.clone(),
                        HandshakeFailureReason::GenesisMismatch(genesis_id),
                    ));
                    return;
                }
                if handshake.target_peer_id != self.my_node_info.id {
                    tracing::debug!(target: "network", "Received handshake from {:?} to {:?} but I am {:?}", handshake.sender_peer_id, handshake.target_peer_id, self.my_node_info.id);
                    self.send_message_or_log(&PeerMessage::HandshakeFailure(
                        self.my_node_info.clone(),
                        HandshakeFailureReason::InvalidTarget,
                    ));
                    return;
                }

                // Verify if nonce is sane.
                if let Err(err) = verify_nonce(&self.clock, handshake.partial_edge_info.nonce) {
                    tracing::debug!(target: "network", nonce=?handshake.partial_edge_info.nonce, my_node_id = ?self.my_node_id(), peer_id=?handshake.sender_peer_id, "bad nonce, disconnecting: {err}");
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                // Check that the received nonce is greater than the current nonce of this connection.
                // If not (and this is an inbound connection) propose a new nonce.
                if let Some(last_edge) =
                    self.network_state.graph.load().local_edges.get(&handshake.sender_peer_id)
                {
                    if last_edge.nonce() >= handshake.partial_edge_info.nonce {
                        tracing::debug!(target: "network", "{:?}: Received too low nonce from peer {:?} sending evidence.", self.my_node_id(), self.peer_addr);
                        self.send_message_or_log(&PeerMessage::LastEdge(last_edge.clone()));
                        return;
                    }
                }
            }
        }

        // Verify that the received partial edge is valid.
        // WARNING: signature is verified against the 2nd argument.
        if !Edge::partial_verify(
            &self.my_node_id(),
            &handshake.sender_peer_id,
            &handshake.partial_edge_info,
        ) {
            tracing::warn!(target: "network", "partial edge with invalid signature, disconnecting");
            self.stop(ctx, ClosingReason::Ban(ReasonForBan::InvalidSignature));
            return;
        }

        // Merge partial edges.
        let nonce = handshake.partial_edge_info.nonce;
        let partial_edge_info = match cs {
            ConnectingStatus::Outbound { handshake_spec, .. } => {
                handshake_spec.partial_edge_info.clone()
            }
            ConnectingStatus::Inbound { .. } => {
                self.network_state.propose_edge(&self.clock, &handshake.sender_peer_id, Some(nonce))
            }
        };
        let edge = Edge::new(
            self.my_node_id().clone(),
            handshake.sender_peer_id.clone(),
            nonce,
            partial_edge_info.signature.clone(),
            handshake.partial_edge_info.signature.clone(),
        );
        debug_assert!(edge.verify());

        // TODO(gprusak): not enabling a port for listening is also a valid setup.
        // In that case peer_info.addr should be None (same as now), however
        // we still should do the check against the PeerStore::blacklist.
        // Currently PeerManager is rejecting connections with peer_info.addr == None
        // preemptively.
        let peer_info = PeerInfo {
            id: handshake.sender_peer_id.clone(),
            addr: handshake
                .sender_listen_port
                .map(|port| SocketAddr::new(self.peer_addr.ip(), port)),
            account_id: None,
        };

        let now = self.clock.now();
        let conn = Arc::new(connection::Connection {
            addr: ctx.address(),
            peer_info: peer_info.clone(),
            edge: AtomicCell::new(edge),
            genesis_id: handshake.sender_chain_info.genesis_id.clone(),
            tracked_shards: handshake.sender_chain_info.tracked_shards.clone(),
            archival: handshake.sender_chain_info.archival,
            last_block: Default::default(),
            peer_type: self.peer_type,
            stats: self.stats.clone(),
            _peer_connections_metric: metrics::PEER_CONNECTIONS.new_point(&metrics::Connection {
                type_: self.peer_type,
                encoding: self.encoding(),
            }),
            last_time_peer_requested: AtomicCell::new(None),
            last_time_received_message: AtomicCell::new(now),
            connection_established_time: now,
            send_accounts_data_demux: demux::Demux::new(
                self.network_state.config.accounts_data_broadcast_rate_limit,
            ),
        });

        let tracker = self.tracker.clone();
        let clock = self.clock.clone();
        let mut interval =
            time::Interval::new(clock.now(), self.network_state.config.peer_stats_period);
        ctx.spawn({
            let conn = conn.clone();
            wrap_future(async move {
                loop {
                    interval.tick(&clock).await;
                    let sent = tracker.lock().sent_bytes.minute_stats(&clock);
                    let received = tracker.lock().received_bytes.minute_stats(&clock);
                    conn.stats
                        .received_bytes_per_sec
                        .store(received.bytes_per_min / 60, Ordering::Relaxed);
                    conn.stats.sent_bytes_per_sec.store(sent.bytes_per_min / 60, Ordering::Relaxed);
                    // Whether the peer is considered abusive due to sending too many messages.
                    // I am allowing this for now because I assume `MAX_PEER_MSG_PER_MIN` will
                    // some day be less than `u64::MAX`.
                    let is_abusive = received.count_per_min > MAX_PEER_MSG_PER_MIN
                        || sent.count_per_min > MAX_PEER_MSG_PER_MIN;
                    if is_abusive {
                        tracing::trace!(
                        target: "network",
                        peer_id = ?conn.peer_info.id,
                        sent = sent.count_per_min,
                        recv = received.count_per_min,
                        "Banning peer for abuse");
                        // TODO(MarX, #1586): Ban peer if we found them abusive. Fix issue with heavy
                        //  network traffic that flags honest peers.
                        // Send ban signal to peer instance. It should send ban signal back and stop the instance.
                        // if let Some(connected_peer) = act.connected_peers.get(&peer_id1) {
                        //     connected_peer.addr.do_send(PeerManagerRequest::BanPeer(ReasonForBan::Abusive));
                        // }
                    }
                }
            })
        });

        // This time is used to figure out when the first run of the callbacks it run.
        // It is important that it is set here (rather than calling clock.now() within the future) - as it makes testing a lot easier (and more deterministic).

        let start_time = self.clock.now();

        // Here we stop processing any PeerActor events until PeerManager
        // decides whether to accept the connection or not: ctx.wait makes
        // the actor event loop poll on the future until it completes before
        // processing any other events.
        ctx.wait(wrap_future({
            let network_state = self.network_state.clone();
            let clock = self.clock.clone();
            let conn = conn.clone();
            async move { network_state.register(&clock,conn).await }
        })
            .map(move |res, act: &mut PeerActor, ctx| {
                match res {
                    Ok(()) => {
                        act.peer_info = Some(peer_info).into();
                        act.peer_status = PeerStatus::Ready(conn.clone());
                        // Respond to handshake if it's inbound and connection was consolidated.
                        if act.peer_type == PeerType::Inbound {
                            act.send_handshake(HandshakeSpec{
                                peer_id: handshake.sender_peer_id.clone(),
                                protocol_version: handshake.protocol_version,
                                partial_edge_info: partial_edge_info,
                            });
                        } else {
                            // Outbound peer triggers the inital full accounts data sync.
                            // TODO(gprusak): implement triggering the periodic full sync.
                            act.send_message_or_log(&PeerMessage::SyncAccountsData(SyncAccountsData{
                                accounts_data: act.network_state.accounts_data.load().data.values().cloned().collect(),
                                incremental: false,
                                requesting_full_sync: true,
                            }));
                            // Only broadcast the new edge from the outbound endpoint.
                            act.network_state.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                                RoutingTableUpdate::from_edges(vec![conn.edge.load()]),
                            )));
                        }

                        // Exchange peers periodically.
                        ctx.spawn(wrap_future({
                            let clock = act.clock.clone();
                            let conn = conn.clone();
                            async move {
                                let mut interval = time::Interval::new(clock.now(),REQUEST_PEERS_INTERVAL);
                                loop {
                                    interval.tick(&clock).await;
                                    conn.send_message(Arc::new(PeerMessage::PeersRequest));
                                }
                            }
                        }));
                        // Send latest block periodically
                        ctx.spawn(wrap_future({
                            let clock = act.clock.clone();
                            let conn = conn.clone();
                            let state = act.network_state.clone();
                            async move {
                                let mut interval =
                                    time::Interval::new(clock.now(), SYNC_LATEST_BLOCK_INTERVAL);
                                loop {
                                    // the first tick is immediate, so the tick should go sync_latest_block
                                    interval.tick(&clock).await;
                                    if let Some(chain_info) = state.chain_info.load().as_ref() {
                                        conn.send_message(Arc::new(PeerMessage::Block(
                                            chain_info.block.clone(),
                                        )));
                                    }
                                }
                            }
                        }));

                        // Refresh connection nonces but only if we're outbound. For inbound connection, the other party should 
                        // take care of nonce refresh.
                        if act.peer_type == PeerType::Outbound {
                            ctx.spawn(wrap_future({
                                let conn = conn.clone();
                                let network_state = act.network_state.clone();
                                let clock = act.clock.clone();
                                async move {
                                    // How often should we refresh a nonce from a peer.
                                    // It should be smaller than PRUNE_EDGES_AFTER.
                                    let mut interval = time::Interval::new(start_time + PRUNE_EDGES_AFTER / 3, PRUNE_EDGES_AFTER / 3);
                                    loop {
                                        interval.tick(&clock).await;
                                        conn.send_message(Arc::new(
                                            PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
                                                &network_state.config.node_id(),
                                                &conn.peer_info.id,
                                                Edge::create_fresh_nonce(&clock),
                                                &network_state.config.node_key,
                                            )
                                        )));

                                    }
                                }
                            }));
                        }
                        // Sync the RoutingTable.
                        act.sync_routing_table();
                        act.network_state.config.event_sink.push(Event::HandshakeCompleted(HandshakeCompletedEvent{
                            stream_id: act.stream_id,
                            edge: conn.edge.load(),
                        }));
                    },
                    Err(err) => {
                        tracing::info!(target: "network", "{:?}: Connection with {:?} rejected by PeerManager: {:?}", act.my_node_id(),conn.peer_info.id,err);
                        act.stop(ctx,ClosingReason::RejectedByPeerManager(err));
                    }
                }
            })
        );
    }

    // Send full RoutingTable.
    fn sync_routing_table(&self) {
        let mut known_edges: Vec<Edge> =
            self.network_state.graph.load().edges.values().cloned().collect();
        if self.network_state.config.skip_tombstones.is_some() {
            known_edges.retain(|edge| edge.removal_info().is_none());
            metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
        }
        let known_accounts = self.network_state.graph.routing_table.get_announce_accounts();
        self.send_message_or_log(&PeerMessage::SyncRoutingTable(RoutingTableUpdate::new(
            known_edges,
            known_accounts,
        )));
    }

    fn handle_msg_connecting(&mut self, ctx: &mut actix::Context<Self>, msg: PeerMessage) {
        match (&mut self.peer_status, msg) {
            (
                PeerStatus::Connecting(ConnectingStatus::Outbound { handshake_spec, .. }),
                PeerMessage::HandshakeFailure(peer_info, reason),
            ) => {
                match reason {
                    HandshakeFailureReason::GenesisMismatch(genesis) => {
                        tracing::warn!(target: "network", "Attempting to connect to a node ({}) with a different genesis block. Our genesis: {:?}, their genesis: {:?}", peer_info, self.network_state.genesis_id, genesis);
                        self.stop(ctx, ClosingReason::HandshakeFailed);
                    }
                    HandshakeFailureReason::ProtocolVersionMismatch {
                        version,
                        oldest_supported_version,
                    } => {
                        // Retry the handshake with the common protocol version.
                        let common_version = std::cmp::min(version, PROTOCOL_VERSION);
                        if common_version < oldest_supported_version
                            || common_version < PEER_MIN_ALLOWED_PROTOCOL_VERSION
                        {
                            tracing::warn!(target: "network", "Unable to connect to a node ({}) due to a network protocol version mismatch. Our version: {:?}, their: {:?}", peer_info, (PROTOCOL_VERSION, PEER_MIN_ALLOWED_PROTOCOL_VERSION), (version, oldest_supported_version));
                            self.stop(ctx, ClosingReason::HandshakeFailed);
                            return;
                        }
                        handshake_spec.protocol_version = common_version;
                        let spec = handshake_spec.clone();
                        ctx.wait(actix::fut::ready(()).then(move |_, act: &mut Self, _| {
                            act.send_handshake(spec);
                            actix::fut::ready(())
                        }));
                    }
                    HandshakeFailureReason::InvalidTarget => {
                        tracing::debug!(target: "network", "Peer found was not what expected. Updating peer info with {:?}", peer_info);
                        if let Err(err) =
                            self.network_state.peer_store.add_direct_peer(&self.clock, peer_info)
                        {
                            tracing::error!(target: "network", ?err, "Fail to update peer store");
                        }
                        self.stop(ctx, ClosingReason::HandshakeFailed);
                    }
                }
            }
            // TODO(gprusak): LastEdge should rather be a variant of HandshakeFailure.
            // Clean this up (you don't have to modify the proto, just the translation layer).
            (
                PeerStatus::Connecting(ConnectingStatus::Outbound { handshake_spec, .. }),
                PeerMessage::LastEdge(edge),
            ) => {
                // Check that the edge provided:
                let ok =
                    // - is for the relevant pair of peers
                    edge.key()==&Edge::make_key(self.my_node_info.id.clone(),handshake_spec.peer_id.clone()) &&
                    // - is not younger than what we proposed originally. This protects us from
                    //   a situation in which the peer presents us with a very outdated edge e,
                    //   and then we sign a new edge with nonce e.nonce+1 which is also outdated.
                    //   It may still happen that an edge with an old nonce gets signed, but only
                    //   if both nodes not know about the newer edge. We don't defend against that.
                    //   Also a malicious peer might send the LastEdge with the edge we just
                    //   signed (pretending that it is old) but we cannot detect that, because the
                    //   signatures are currently deterministic.
                    edge.nonce() >= handshake_spec.partial_edge_info.nonce &&
                    // - is a correctly signed edge
                    edge.verify();
                // Disconnect if neighbor sent an invalid edge.
                if !ok {
                    tracing::info!(target: "network", "{:?}: Peer {:?} sent invalid edge. Disconnect.", self.my_node_id(), self.peer_addr);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                // Recreate the edge with a newer nonce.
                handshake_spec.partial_edge_info = self.network_state.propose_edge(
                    &self.clock,
                    &handshake_spec.peer_id,
                    Some(std::cmp::max(edge.next(), Edge::create_fresh_nonce(&self.clock))),
                );
                let spec = handshake_spec.clone();
                ctx.wait(actix::fut::ready(()).then(move |_, act: &mut Self, _| {
                    act.send_handshake(spec);
                    actix::fut::ready(())
                }));
            }
            (PeerStatus::Connecting { .. }, PeerMessage::Tier2Handshake(msg)) => {
                self.process_handshake(ctx, msg)
            }
            (_, msg) => {
                tracing::warn!(target:"network","unexpected message during handshake: {}",msg)
            }
        }
    }

    async fn receive_routed_message(
        clock: &time::Clock,
        network_state: &NetworkState,
        peer_id: PeerId,
        msg_hash: CryptoHash,
        body: RoutedMessageBody,
    ) -> Result<Option<RoutedMessageBody>, ReasonForBan> {
        let _span = tracing::trace_span!(target: "network", "receive_routed_message").entered();
        Ok(match body {
            RoutedMessageBody::TxStatusRequest(account_id, tx_hash) => network_state
                .client
                .tx_status_request(account_id, tx_hash)
                .await
                .map(|v| RoutedMessageBody::TxStatusResponse(*v)),
            RoutedMessageBody::TxStatusResponse(tx_result) => {
                network_state.client.tx_status_response(tx_result).await;
                None
            }
            RoutedMessageBody::StateRequestHeader(shard_id, sync_hash) => network_state
                .client
                .state_request_header(shard_id, sync_hash)
                .await?
                .map(RoutedMessageBody::VersionedStateResponse),
            RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id) => network_state
                .client
                .state_request_part(shard_id, sync_hash, part_id)
                .await?
                .map(RoutedMessageBody::VersionedStateResponse),
            RoutedMessageBody::VersionedStateResponse(info) => {
                network_state.client.state_response(info).await;
                None
            }
            RoutedMessageBody::BlockApproval(approval) => {
                network_state.client.block_approval(approval, peer_id).await;
                None
            }
            RoutedMessageBody::ForwardTx(transaction) => {
                network_state.client.transaction(transaction, /*is_forwarded=*/ true).await;
                None
            }
            RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                network_state.client.partial_encoded_chunk_request(request, msg_hash).await;
                None
            }
            RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                network_state.client.partial_encoded_chunk_response(response, clock.now()).await;
                None
            }
            RoutedMessageBody::VersionedPartialEncodedChunk(chunk) => {
                network_state.client.partial_encoded_chunk(chunk).await;
                None
            }
            RoutedMessageBody::PartialEncodedChunkForward(msg) => {
                network_state.client.partial_encoded_chunk_forward(msg).await;
                None
            }
            RoutedMessageBody::ReceiptOutcomeRequest(_) => {
                // Silently ignore for the time being.  We’ve been still
                // sending those messages at protocol version 56 so we
                // need to wait until 59 before we can remove the
                // variant completely.
                None
            }
            body => {
                tracing::error!(target: "network", "Peer received unexpected message type: {:?}", body);
                None
            }
        })
    }

    fn receive_message(
        &self,
        ctx: &mut actix::Context<Self>,
        conn: &connection::Connection,
        msg: PeerMessage,
    ) {
        let _span = tracing::trace_span!(target: "network", "receive_message").entered();
        // This is a fancy way to clone the message iff event_sink is non-null.
        // If you have a better idea on how to achieve that, feel free to improve this.
        let message_processed_event = self
            .network_state
            .config
            .event_sink
            .delayed_push(|| Event::MessageProcessed(msg.clone()));
        let was_requested = match &msg {
            PeerMessage::Block(block) => {
                let hash = *block.hash();
                let height = block.header().height();
                conn.last_block.rcu(|last_block| {
                    if last_block.is_none() || last_block.unwrap().height <= height {
                        Arc::new(Some(BlockInfo { height, hash }))
                    } else {
                        last_block.clone()
                    }
                });
                let mut tracker = self.tracker.lock();
                tracker.push_received(hash);
                tracker.has_request(&hash)
            }
            _ => false,
        };
        let clock = self.clock.clone();
        let network_state = self.network_state.clone();
        let peer_id = conn.peer_info.id.clone();
        ctx.spawn(wrap_future(async move {
            Ok(match msg {
                PeerMessage::Routed(msg) => {
                    let msg_hash = msg.hash();
                    Self::receive_routed_message(&clock, &network_state, peer_id, msg_hash, msg.msg.body).await?.map(
                        |body| {
                            PeerMessage::Routed(network_state.sign_message(
                                &clock,
                                RawRoutedMessage { target: PeerIdOrHash::Hash(msg_hash), body },
                            ))
                        },
                    )
                }
                PeerMessage::BlockRequest(hash) => {
                    network_state.client.block_request(hash).await.map(|b|PeerMessage::Block(*b))
                }
                PeerMessage::BlockHeadersRequest(hashes) => {
                    network_state.client.block_headers_request(hashes).await.map(PeerMessage::BlockHeaders)
                }
                PeerMessage::Block(block) => {
                    network_state.client.block(block, peer_id, was_requested).await;
                    None
                }
                PeerMessage::Transaction(transaction) => {
                    network_state.client.transaction(transaction, /*is_forwarded=*/ false).await;
                    None
                }
                PeerMessage::BlockHeaders(headers) => {
                    network_state.client.block_headers(headers, peer_id).await?;
                    None
                }
                PeerMessage::Challenge(challenge) => {
                    network_state.client.challenge(challenge).await;
                    None
                }
                msg => {
                    tracing::error!(target: "network", "Peer received unexpected type: {:?}", msg);
                    None
                }
            })}.in_current_span())
            .map(|res, act: &mut PeerActor, ctx| {
                match res {
                    // TODO(gprusak): make sure that for routed messages we drop routeback info correctly.
                    Ok(Some(resp)) => act.send_message_or_log(&resp),
                    Ok(None) => {}
                    Err(ban_reason) => act.stop(ctx, ClosingReason::Ban(ban_reason)),
                }
                message_processed_event();
            }),
        );
    }

    fn handle_msg_ready(
        &mut self,
        ctx: &mut actix::Context<Self>,
        conn: Arc<connection::Connection>,
        peer_msg: PeerMessage,
    ) {
        let _span = tracing::trace_span!(
            target: "network",
            "handle_msg_ready")
        .entered();

        match peer_msg.clone() {
            PeerMessage::Disconnect => {
                tracing::debug!(target: "network", "Disconnect signal. Me: {:?} Peer: {:?}", self.my_node_info.id, self.other_peer_id());
                self.stop(ctx, ClosingReason::DisconnectMessage);
            }
            PeerMessage::Tier2Handshake(_) => {
                // Received handshake after already have seen handshake from this peer.
                tracing::debug!(target: "network", "Duplicate handshake from {}", self.peer_info);
            }
            PeerMessage::PeersRequest => {
                let peers = self
                    .network_state
                    .peer_store
                    .healthy_peers(self.network_state.config.max_send_peers as usize);
                if !peers.is_empty() {
                    tracing::debug!(target: "network", "Peers request from {}: sending {} peers.", self.peer_info, peers.len());
                    self.send_message_or_log(&PeerMessage::PeersResponse(peers));
                }
                self.network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
            }
            PeerMessage::PeersResponse(peers) => {
                tracing::debug!(target: "network", "Received peers from {}: {} peers.", self.peer_info, peers.len());
                let node_id = self.network_state.config.node_id();
                if let Err(err) = self.network_state.peer_store.add_indirect_peers(
                    &self.clock,
                    peers.into_iter().filter(|peer_info| peer_info.id != node_id),
                ) {
                    tracing::error!(target: "network", ?err, "Fail to update peer store");
                };
                self.network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
            }
            PeerMessage::RequestUpdateNonce(edge_info) => {
                let clock = self.clock.clone();
                let network_state = self.network_state.clone();
                ctx.spawn(wrap_future(async move {
                    let peer_id = &conn.peer_info.id;
                    let edge = match network_state.graph.load().local_edges.get(peer_id) {
                        Some(cur_edge)
                            if cur_edge.edge_type() == EdgeState::Active
                                && cur_edge.nonce() >= edge_info.nonce =>
                        {
                            cur_edge.clone()
                        }
                        _ => match network_state
                            .finalize_edge(&clock, peer_id.clone(), edge_info)
                            .await
                        {
                            Ok(edge) => edge,
                            Err(ban_reason) => {
                                conn.stop(Some(ban_reason));
                                return;
                            }
                        },
                    };
                    conn.send_message(Arc::new(PeerMessage::SyncRoutingTable(
                        RoutingTableUpdate::from_edges(vec![edge]),
                    )));
                    network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                }));
            }
            PeerMessage::SyncRoutingTable(rtu) => {
                let clock = self.clock.clone();
                let network_state = self.network_state.clone();
                ctx.spawn(wrap_future(async move {
                    Self::handle_sync_routing_table(&clock, &network_state, conn, rtu).await;
                    network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                }));
            }
            PeerMessage::SyncAccountsData(msg) => {
                let network_state = self.network_state.clone();
                // In case a full sync is requested, immediately send what we got.
                // It is a microoptimization: we do not send back the data we just received.
                if msg.requesting_full_sync {
                    self.send_message_or_log(&PeerMessage::SyncAccountsData(SyncAccountsData {
                        requesting_full_sync: false,
                        incremental: false,
                        accounts_data: network_state
                            .accounts_data
                            .load()
                            .data
                            .values()
                            .cloned()
                            .collect(),
                    }));
                }
                // Early exit, if there is no data in the message.
                if msg.accounts_data.is_empty() {
                    network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                    return;
                }
                let network_state = self.network_state.clone();
                ctx.spawn(wrap_future(async move {
                    if let Some(err) = network_state.add_accounts_data(msg.accounts_data).await {
                        conn.stop(Some(match err {
                            accounts_data::Error::InvalidSignature => {
                                ReasonForBan::InvalidSignature
                            }
                            accounts_data::Error::DataTooLarge => ReasonForBan::Abusive,
                            accounts_data::Error::SingleAccountMultipleData => {
                                ReasonForBan::Abusive
                            }
                        }));
                    }
                    network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                }));
            }
            PeerMessage::Routed(mut msg) => {
                tracing::trace!(
                    target: "network",
                    "Received routed message from {} to {:?}.",
                    self.peer_info,
                    msg.target);
                if !msg.verify() {
                    // Received invalid routed message from peer.
                    self.stop(ctx, ClosingReason::Ban(ReasonForBan::InvalidSignature));
                    return;
                }
                let from = &conn.peer_info.id;
                if msg.expect_response() {
                    tracing::trace!(target: "network", route_back = ?msg.clone(), "Received peer message that requires response");
                    self.network_state.graph.routing_table.add_route_back(
                        &self.clock,
                        msg.hash(),
                        from.clone(),
                    );
                }
                if self.network_state.message_for_me(&msg.target) {
                    metrics::record_routed_msg_metrics(&self.clock, &msg);
                    // Handle Ping and Pong message if they are for us without sending to client.
                    // i.e. Return false in case of Ping and Pong
                    match &msg.body {
                        RoutedMessageBody::Ping(ping) => {
                            self.network_state.send_pong(&self.clock, ping.nonce, msg.hash());
                            // TODO(gprusak): deprecate Event::Ping/Pong in favor of
                            // MessageProcessed.
                            self.network_state.config.event_sink.push(Event::Ping(ping.clone()));
                            self.network_state
                                .config
                                .event_sink
                                .push(Event::MessageProcessed(PeerMessage::Routed(msg)));
                        }
                        RoutedMessageBody::Pong(pong) => {
                            self.network_state.config.event_sink.push(Event::Pong(pong.clone()));
                            self.network_state
                                .config
                                .event_sink
                                .push(Event::MessageProcessed(PeerMessage::Routed(msg)));
                        }
                        _ => self.receive_message(ctx, &conn, PeerMessage::Routed(msg.clone())),
                    }
                } else {
                    if msg.decrease_ttl() {
                        self.network_state.send_message_to_peer(&self.clock, msg);
                    } else {
                        self.network_state.config.event_sink.push(Event::RoutedMessageDropped);
                        tracing::warn!(target: "network", ?msg, ?from, "Message dropped because TTL reached 0.");
                        metrics::ROUTED_MESSAGE_DROPPED
                            .with_label_values(&[msg.body_variant()])
                            .inc();
                    }
                }
            }
            msg => self.receive_message(ctx, &conn, msg),
        }
    }

    async fn handle_sync_routing_table(
        clock: &time::Clock,
        network_state: &Arc<NetworkState>,
        conn: Arc<connection::Connection>,
        rtu: RoutingTableUpdate,
    ) {
        let _span = tracing::trace_span!(target: "network", "handle_sync_routing_table").entered();
        if let Err(ban_reason) = network_state.add_edges(&clock, rtu.edges).await {
            conn.stop(Some(ban_reason));
        }
        // For every announce we received, we fetch the last announce with the same account_id
        // that we already broadcasted. Client actor will both verify signatures of the received announces
        // as well as filter out those which are older than the fetched ones (to avoid overriding
        // a newer announce with an older one).
        let old = network_state
            .graph
            .routing_table
            .get_broadcasted_announces(rtu.accounts.iter().map(|a| &a.account_id));
        let accounts: Vec<(AnnounceAccount, Option<EpochId>)> = rtu
            .accounts
            .into_iter()
            .map(|aa| {
                let id = aa.account_id.clone();
                (aa, old.get(&id).map(|old| old.epoch_id.clone()))
            })
            .collect();
        match network_state.client.announce_account(accounts).await {
            Err(ban_reason) => conn.stop(Some(ban_reason)),
            Ok(accounts) => network_state.add_accounts(accounts).await,
        }
    }
}

impl actix::Actor for PeerActor {
    type Context = actix::Context<PeerActor>;

    fn started(&mut self, ctx: &mut Self::Context) {
        metrics::PEER_CONNECTIONS_TOTAL.inc();
        tracing::debug!(target: "network", "{:?}: Peer {:?} {:?} started", self.my_node_info.id, self.peer_addr, self.peer_type);
        // Set Handshake timeout for stopping actor if peer is not ready after given period of time.

        near_performance_metrics::actix::run_later(
            ctx,
            self.network_state.config.handshake_timeout.try_into().unwrap(),
            move |act, ctx| match act.peer_status {
                PeerStatus::Connecting { .. } => {
                    tracing::info!(target: "network", "Handshake timeout expired for {}", act.peer_info);
                    act.stop(ctx, ClosingReason::HandshakeFailed);
                }
                _ => {}
            },
        );

        // If outbound peer, initiate handshake.
        if let PeerStatus::Connecting(ConnectingStatus::Outbound { handshake_spec, .. }) =
            &self.peer_status
        {
            self.send_handshake(handshake_spec.clone());
        }
        self.network_state
            .config
            .event_sink
            .push(Event::HandshakeStarted(HandshakeStartedEvent { stream_id: self.stream_id }));
    }

    fn stopping(&mut self, _: &mut Self::Context) -> actix::Running {
        actix::Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // closing_reason may be None in case the whole actix system is stopped.
        // It happens a lot in tests.
        metrics::PEER_CONNECTIONS_TOTAL.dec();
        tracing::debug!(target: "network", "{:?}: [status = {:?}] Peer {} disconnected.", self.my_node_info.id, self.peer_status, self.peer_info);
        if self.closing_reason.is_none() {
            // Due to Actix semantics, sometimes closing reason may be not set.
            // But it is only expected to happen in tests.
            tracing::error!(target:"network", "closing reason not set. This should happen only in tests.");
        }
        match &self.peer_status {
            // If PeerActor is in Connecting state, then
            // it was not registered in the NewtorkState,
            // so there is nothing to be done.
            PeerStatus::Connecting(..) => {
                // TODO(gprusak): reporting ConnectionClosed event is quite scattered right now and
                // it is very ugly: it may happen here, in spawn_inner, or in NetworkState::unregister().
                // Centralize it, once we get rid of actix.
                self.network_state.config.event_sink.push(Event::ConnectionClosed(
                    ConnectionClosedEvent {
                        stream_id: self.stream_id,
                        reason: self.closing_reason.clone().unwrap_or(ClosingReason::Unknown),
                    },
                ));
            }
            // Clean up the Connection from the NetworkState.
            PeerStatus::Ready(conn) => {
                let network_state = self.network_state.clone();
                let clock = self.clock.clone();
                let conn = conn.clone();
                network_state.unregister(
                    &clock,
                    &conn,
                    self.stream_id,
                    self.closing_reason.clone().unwrap_or(ClosingReason::Unknown),
                );
            }
        }
        actix::Arbiter::current().stop();
    }
}

impl actix::Handler<stream::Error> for PeerActor {
    type Result = ();
    fn handle(&mut self, err: stream::Error, ctx: &mut Self::Context) {
        let expected = match &err {
            stream::Error::Recv(stream::RecvError::MessageTooLarge { .. }) => {
                self.stop(ctx, ClosingReason::Ban(ReasonForBan::Abusive));
                true
            }
            // It is expected in a sense that the peer might be just slow.
            stream::Error::Send(stream::SendError::QueueOverflow { .. }) => true,
            stream::Error::Recv(stream::RecvError::IO(err))
            | stream::Error::Send(stream::SendError::IO(err)) => match err.kind() {
                // Connection has been closed.
                io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe => true,
                // When stopping tokio runtime, an "IO driver has terminated" is sometimes
                // returned.
                io::ErrorKind::Other => true,
                // It is unexpected in a sense that stream got broken in an unexpected way.
                // In case you encounter an error that was actually to be expected,
                // please add it here and document.
                _ => false,
            },
        };
        log_assert!(expected, "unexpected closing reason: {err}");
        tracing::info!(target: "network", ?err, "Closing connection to {}", self.peer_info);
        self.stop(ctx, ClosingReason::StreamError);
    }
}

impl actix::Handler<stream::Frame> for PeerActor {
    type Result = ();
    #[perf]
    fn handle(&mut self, stream::Frame(msg): stream::Frame, ctx: &mut Self::Context) {
        let _span = tracing::debug_span!(
            target: "network",
            "handle",
            handler = "bytes",
            actor = "PeerActor",
            msg_len = msg.len(),
            peer = %self.peer_info)
        .entered();
        // TODO(#5155) We should change our code to track size of messages received from Peer
        // as long as it travels to PeerManager, etc.

        if self.closing_reason.is_some() {
            tracing::warn!(target: "network", "Received message from closing connection {:?}. Ignoring", self.peer_type);
            return;
        }

        self.update_stats_on_receiving_message(msg.len());
        let mut peer_msg = match self.parse_message(&msg) {
            Ok(msg) => msg,
            Err(err) => {
                tracing::debug!(target: "network", "Received invalid data {} from {}: {}", pretty::AbbrBytes(&msg), self.peer_info, err);
                return;
            }
        };

        match &peer_msg {
            PeerMessage::Routed(msg) => {
                let key = (msg.author.clone(), msg.target.clone(), msg.signature.clone());
                let now = self.clock.now();
                // Drop duplicated messages routed within DROP_DUPLICATED_MESSAGES_PERIOD ms
                if let Some(&t) = self.routed_message_cache.get(&key) {
                    if now <= t + DROP_DUPLICATED_MESSAGES_PERIOD {
                        tracing::debug!(target: "network", "Dropping duplicated message from {} to {:?}", msg.author, msg.target);
                        self.network_state.config.event_sink.push(Event::RoutedMessageDropped);
                        return;
                    }
                }
                if let RoutedMessageBody::ForwardTx(_) = &msg.body {
                    // Check whenever we exceeded number of transactions we got since last block.
                    // If so, drop the transaction.
                    let r = self.network_state.txns_since_last_block.load(Ordering::Acquire);
                    if r > MAX_TRANSACTIONS_PER_BLOCK_MESSAGE {
                        return;
                    }
                    self.network_state.txns_since_last_block.fetch_add(1, Ordering::AcqRel);
                }
                self.routed_message_cache.put(key, now);
            }
            PeerMessage::Block(_) => {
                self.network_state.txns_since_last_block.store(0, Ordering::Release);
            }
            _ => {}
        }

        tracing::trace!(target: "network", "Received message: {}", peer_msg);

        {
            let labels = [peer_msg.msg_variant()];
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_TOTAL.with_label_values(&labels).inc();
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_BYTES
                .with_label_values(&labels)
                .inc_by(msg.len() as u64);
        }
        match &self.peer_status {
            PeerStatus::Connecting { .. } => self.handle_msg_connecting(ctx, peer_msg),
            PeerStatus::Ready(conn) => {
                if self.closing_reason.is_some() {
                    tracing::warn!(target: "network", "Received {} from closing connection {:?}. Ignoring", peer_msg, self.peer_type);
                    return;
                }
                conn.last_time_received_message.store(self.clock.now());
                // Optionally, ignore any received tombstones after startup. This is to
                // prevent overload from too much accumulated deleted edges.
                //
                // We have similar code to skip sending tombstones, here we handle the
                // case when our peer doesn't use that logic yet.
                if let Some(skip_tombstones) = self.network_state.config.skip_tombstones {
                    if let PeerMessage::SyncRoutingTable(routing_table) = &mut peer_msg {
                        if conn.connection_established_time + skip_tombstones > self.clock.now() {
                            routing_table
                                .edges
                                .retain(|edge| edge.edge_type() == EdgeState::Active);
                            metrics::EDGE_TOMBSTONE_RECEIVING_SKIPPED.inc();
                        }
                    }
                }
                // Handle the message.
                self.handle_msg_ready(ctx, conn.clone(), peer_msg);
            }
        }
    }
}

impl actix::Handler<WithSpanContext<SendMessage>> for PeerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<SendMessage>, _: &mut Self::Context) {
        let (_span, msg) = handler_debug_span!(target: "network", msg);
        let _d = delay_detector::DelayDetector::new(|| "send message".into());
        self.send_message_or_log(&msg.message);
    }
}

/// Messages from PeerManager to Peer
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct Stop {
    pub ban_reason: Option<ReasonForBan>,
}

impl actix::Handler<WithSpanContext<Stop>> for PeerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<Stop>, ctx: &mut Self::Context) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "network", msg);
        self.stop(
            ctx,
            match msg.ban_reason {
                Some(reason) => ClosingReason::Ban(reason),
                None => ClosingReason::PeerManager,
            },
        );
    }
}

type InboundHandshakePermit = tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
enum ConnectingStatus {
    Inbound(InboundHandshakePermit),
    Outbound { _permit: connection::OutboundHandshakePermit, handshake_spec: HandshakeSpec },
}

/// State machine of the PeerActor.
/// The transition graph for inbound connection is:
/// Connecting(Inbound) -> Ready
/// for outbound connection is:
/// Connecting(Outbound) -> Ready
///
/// From every state the PeerActor can be immediately shut down.
/// In the Connecting state only Handshake-related messages are allowed.
/// All the other messages can be exchanged only in the Ready state.
///
/// For the exact process of establishing a connection between peers,
/// see PoolSnapshot in chain/network/src/peer_manager/connection.rs.
#[derive(Debug)]
enum PeerStatus {
    /// Handshake in progress.
    Connecting(ConnectingStatus),
    /// Ready to go.
    Ready(Arc<connection::Connection>),
}
