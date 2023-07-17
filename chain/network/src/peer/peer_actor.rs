use crate::accounts_data::AccountDataError;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::concurrency::demux;
use crate::config::PEERS_RESPONSE_MAX_PEERS;
use crate::network_protocol::{
    DistanceVector, Edge, EdgeState, Encoding, OwnedAccount, ParsePeerMessageError,
    PartialEdgeInfo, PeerChainInfoV2, PeerIdOrHash, PeerInfo, PeersRequest, PeersResponse,
    RawRoutedMessage, RoutedMessageBody, RoutingTableUpdate, StateResponseInfo, SyncAccountsData,
};
use crate::peer::stream;
use crate::peer::tracker::Tracker;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::{NetworkState, PRUNE_EDGES_AFTER};
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::peer_manager_actor::MAX_TIER2_PEERS;
use crate::private_actix::{RegisterPeerError, SendMessage};
use crate::routing::edge::verify_nonce;
use crate::routing::NetworkTopologyChange;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::stats::metrics;
use crate::tcp;
use crate::types::{
    BlockInfo, Disconnect, Handshake, HandshakeFailureReason, PeerMessage, PeerType, ReasonForBan,
};
use actix::fut::future::wrap_future;
use actix::{Actor as _, ActorContext as _, ActorFutureExt as _, AsyncContext as _};
use lru::LruCache;
use near_async::time;
use near_crypto::Signature;
use near_o11y::{handler_debug_span, log_assert, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::EpochId;
use near_primitives::utils::DisplayOption;
use near_primitives::version::{
    ProtocolVersion, PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use parking_lot::Mutex;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::cmp::min;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument as _;

/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: time::Duration = time::Duration::seconds(60);

/// Maximal allowed UTC clock skew between this node and the peer.
const MAX_CLOCK_SKEW: time::Duration = time::Duration::minutes(30);

/// Maximum number of transaction messages we will accept between block messages.
/// The purpose of this constant is to ensure we do not spend too much time deserializing and
/// dispatching transactions when we should be focusing on consensus-related messages.
const MAX_TRANSACTIONS_PER_BLOCK_MESSAGE: usize = 1000;
/// Limit cache size of 1000 messages
const ROUTED_MESSAGE_CACHE_SIZE: usize = 1000;
/// Duplicated messages will be dropped if routed through the same peer multiple times.
pub(crate) const DROP_DUPLICATED_MESSAGES_PERIOD: time::Duration = time::Duration::milliseconds(50);
/// How often to send the latest block to peers.
const SYNC_LATEST_BLOCK_INTERVAL: time::Duration = time::Duration::seconds(60);
/// How often to perform a full sync of AccountsData with the peer.
const ACCOUNTS_DATA_FULL_SYNC_INTERVAL: time::Duration = time::Duration::minutes(10);

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
    pub(crate) tier: tcp::Tier,
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
    /// Read through `tcp::Tier::is_allowed()` to see which message types
    /// are allowed for a connection of each tier.
    #[error("Received a message of type not allowed on this connection.")]
    DisallowedMessage,
    #[error("PeerManager requested to close the connection")]
    PeerManagerRequest,
    #[error("Received DisconnectMessage from peer")]
    DisconnectMessage,
    #[error("Peer clock skew exceeded {MAX_CLOCK_SKEW}")]
    TooLargeClockSkew,
    #[error("owned_account.peer_id doesn't match handshake.sender_peer_id")]
    OwnedAccountMismatch,
    #[error("PeerActor stopped NOT via PeerActor::stop()")]
    Unknown,
}

impl ClosingReason {
    /// Used upon closing an outbound connection to decide whether to remove it from the ConnectionStore.
    /// If the inbound side is the one closing the connection, it evaluates this function on the closing
    /// reason and includes the result in the Disconnect message.
    pub(crate) fn remove_from_connection_store(&self) -> bool {
        match self {
            ClosingReason::TooManyInbound => false, // outbound may be still be OK
            ClosingReason::OutboundNotAllowed(_) => true, // outbound not allowed
            ClosingReason::Ban(_) => true,          // banned
            ClosingReason::HandshakeFailed => false, // handshake may simply time out
            ClosingReason::RejectedByPeerManager(_) => true, // rejected by peer manager
            ClosingReason::StreamError => false,    // connection issue
            ClosingReason::DisallowedMessage => true, // misbehaving peer
            ClosingReason::PeerManagerRequest => true, // closed intentionally
            ClosingReason::DisconnectMessage => false, // graceful disconnect
            ClosingReason::TooLargeClockSkew => true, // reconnect will fail for the same reason
            ClosingReason::OwnedAccountMismatch => true, // misbehaving peer
            ClosingReason::Unknown => false,        // only happens in tests
        }
    }
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
    tier: tcp::Tier,
    protocol_version: ProtocolVersion,
    partial_edge_info: PartialEdgeInfo,
}

type HandshakeSignalSender = tokio::sync::oneshot::Sender<std::convert::Infallible>;
pub type HandshakeSignal = tokio::sync::oneshot::Receiver<std::convert::Infallible>;

impl PeerActor {
    /// Spawns a PeerActor on a separate actix::Arbiter and awaits for the
    /// handshake to succeed/fail. The actual result is not returned because
    /// actix makes everything complicated.
    pub(crate) async fn spawn_and_handshake(
        clock: time::Clock,
        stream: tcp::Stream,
        force_encoding: Option<Encoding>,
        network_state: Arc<NetworkState>,
    ) -> anyhow::Result<actix::Addr<Self>> {
        let (addr, handshake_signal) = Self::spawn(clock, stream, force_encoding, network_state)?;
        // Await for the handshake to complete, by awaiting the handshake_signal channel.
        // This is a receiver of Infallible, so it only completes when the channel is closed.
        handshake_signal.await.err().unwrap();
        Ok(addr)
    }

    /// Spawns a PeerActor on a separate actix arbiter.
    /// Returns the actor address and a HandshakeSignal: an asynchronous channel
    /// which will be closed as soon as the handshake is finished (successfully or not).
    /// You can asynchronously await the returned HandshakeSignal.
    pub(crate) fn spawn(
        clock: time::Clock,
        stream: tcp::Stream,
        force_encoding: Option<Encoding>,
        network_state: Arc<NetworkState>,
    ) -> anyhow::Result<(actix::Addr<Self>, HandshakeSignal)> {
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
    ) -> Result<(actix::Addr<Self>, HandshakeSignal), ClosingReason> {
        let connecting_status = match &stream.type_ {
            tcp::StreamType::Inbound => ConnectingStatus::Inbound(
                network_state
                    .inbound_handshake_permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ClosingReason::TooManyInbound)?,
            ),
            tcp::StreamType::Outbound { tier, peer_id } => ConnectingStatus::Outbound {
                _permit: match tier {
                    tcp::Tier::T1 => network_state
                        .tier1
                        .start_outbound(peer_id.clone())
                        .map_err(ClosingReason::OutboundNotAllowed)?,
                    tcp::Tier::T2 => {
                        // A loop connection is not allowed on TIER2
                        // (it is allowed on TIER1 to verify node's public IP).
                        // TODO(gprusak): try to make this more consistent.
                        if peer_id == &network_state.config.node_id() {
                            return Err(ClosingReason::OutboundNotAllowed(
                                connection::PoolError::UnexpectedLoopConnection,
                            ));
                        }
                        network_state
                            .tier2
                            .start_outbound(peer_id.clone())
                            .map_err(ClosingReason::OutboundNotAllowed)?
                    }
                },
                handshake_spec: HandshakeSpec {
                    partial_edge_info: network_state.propose_edge(&clock, peer_id, None),
                    protocol_version: PROTOCOL_VERSION,
                    tier: *tier,
                    peer_id: peer_id.clone(),
                },
            },
        };
        // Override force_encoding for outbound Tier1 connections,
        // since Tier1Handshake is supported only with proto encoding.
        let force_encoding = match &stream.type_ {
            tcp::StreamType::Outbound { tier, .. } if tier == &tcp::Tier::T1 => {
                Some(Encoding::Proto)
            }
            _ => force_encoding,
        };
        let my_node_info = PeerInfo {
            id: network_state.config.node_id(),
            addr: network_state.config.node_addr.as_ref().map(|a| **a),
            account_id: network_state.config.validator.as_ref().map(|v| v.account_id()),
        };
        // recv is the HandshakeSignal returned by this spawn_inner() call.
        let (send, recv): (HandshakeSignalSender, HandshakeSignal) =
            tokio::sync::oneshot::channel();
        // Start PeerActor on separate thread.
        Ok((
            Self::start_in_arbiter(&actix::Arbiter::new().handle(), move |ctx| {
                let stream_id = stream.id();
                let peer_addr = stream.peer_addr;
                let stream_type = stream.type_.clone();
                let stats = Arc::new(connection::Stats::default());
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
                    peer_status: PeerStatus::Connecting(send, connecting_status),
                    framed,
                    tracker: Default::default(),
                    stats,
                    routed_message_cache: LruCache::new(ROUTED_MESSAGE_CACHE_SIZE),
                    protocol_buffers_supported: false,
                    force_encoding,
                    peer_info: match &stream_type {
                        tcp::StreamType::Inbound => None,
                        tcp::StreamType::Outbound { peer_id, .. } => Some(PeerInfo {
                            id: peer_id.clone(),
                            addr: Some(peer_addr),
                            account_id: None,
                        }),
                    }
                    .into(),
                    network_state,
                }
            }),
            recv,
        ))
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
        if let (PeerStatus::Ready(conn), PeerMessage::PeersRequest(_)) = (&self.peer_status, msg) {
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
            PeerMessage::SyncAccountsData(d) => metrics::SYNC_ACCOUNTS_DATA
                .with_label_values(&[
                    "sent",
                    metrics::bool_to_str(d.incremental),
                    metrics::bool_to_str(d.requesting_full_sync),
                ])
                .inc(),
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
            sender_listen_port: self.network_state.config.node_addr.as_ref().map(|a| a.port()),
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: self.network_state.genesis_id.clone(),
                // TODO: remove `height` from PeerChainInfo
                height,
                tracked_shards,
                archival: self.network_state.config.archive,
            },
            partial_edge_info: spec.partial_edge_info,
            owned_account: self.network_state.config.validator.as_ref().map(|vc| {
                OwnedAccount {
                    account_key: vc.signer.public_key(),
                    peer_id: self.network_state.config.node_id(),
                    timestamp: self.clock.now_utc(),
                }
                .sign(vc.signer.as_ref())
            }),
        };
        let msg = match spec.tier {
            tcp::Tier::T1 => PeerMessage::Tier1Handshake(handshake),
            tcp::Tier::T2 => PeerMessage::Tier2Handshake(handshake),
        };
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

    fn process_handshake(
        &mut self,
        ctx: &mut <PeerActor as actix::Actor>::Context,
        tier: tcp::Tier,
        handshake: Handshake,
    ) {
        tracing::debug!(target: "network", "{:?}: Received handshake {:?}", self.my_node_info.id, handshake);
        let cs = match &self.peer_status {
            PeerStatus::Connecting(_, it) => it,
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
                // This can happen only in case of a malicious node.
                // Outbound peer requests a connection of a given TIER, the inbound peer can just
                // confirm the TIER or drop connection. TIER is not negotiable during handshake.
                if tier != spec.tier {
                    tracing::warn!(target: "network", "Connection TIER mismatch. Disconnecting peer {}", handshake.sender_peer_id);
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

        // Verify that handshake.owned_account is valid.
        if let Some(owned_account) = &handshake.owned_account {
            if let Err(_) = owned_account.payload().verify(&owned_account.account_key) {
                self.stop(ctx, ClosingReason::Ban(ReasonForBan::InvalidSignature));
                return;
            }
            if owned_account.peer_id != handshake.sender_peer_id {
                self.stop(ctx, ClosingReason::OwnedAccountMismatch);
                return;
            }
            if (owned_account.timestamp - self.clock.now_utc()).abs() >= MAX_CLOCK_SKEW {
                self.stop(ctx, ClosingReason::TooLargeClockSkew);
                return;
            }
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
            tier,
            addr: ctx.address(),
            peer_info: peer_info.clone(),
            owned_account: handshake.owned_account.clone(),
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
            established_time: now,
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
            let edge = edge.clone();
            async move { network_state.register(&clock,edge,conn).await }
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
                                tier,
                                protocol_version: handshake.protocol_version,
                                partial_edge_info: partial_edge_info,
                            });
                        }
                        // TIER1 is strictly reserved for BFT consensensus messages,
                        // so all kinds of periodical syncs happen only on TIER2 connections.
                        if tier==tcp::Tier::T2 {
                            // Trigger a full accounts data sync periodically.
                            // Note that AccountsData is used to establish TIER1 network,
                            // it is broadcasted over TIER2 network. This is a bootstrapping
                            // mechanism, because TIER2 is established before TIER1.
                            //
                            // TODO(gprusak): consider whether it wouldn't be more uniform to just
                            // send full sync from both sides of the connection independently. Or
                            // perhaps make the full sync request a separate message which doesn't
                            // carry the accounts_data at all.
                            if conn.peer_type == PeerType::Outbound {
                                ctx.spawn(wrap_future({
                                    let clock = act.clock.clone();
                                    let conn = conn.clone();
                                    let network_state = act.network_state.clone();
                                    let mut interval = time::Interval::new(clock.now(),ACCOUNTS_DATA_FULL_SYNC_INTERVAL);
                                    async move {
                                        loop {
                                            interval.tick(&clock).await;
                                            conn.send_message(Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData{
                                                accounts_data: network_state.accounts_data.load().data.values().cloned().collect(),
                                                incremental: false,
                                                requesting_full_sync: true,
                                            })));
                                        }
                                    }
                                }));
                            }
                            // Exchange peers periodically.
                            ctx.spawn(wrap_future({
                                let clock = act.clock.clone();
                                let conn = conn.clone();
                                let mut interval = time::Interval::new(clock.now(),REQUEST_PEERS_INTERVAL);
                                async move {
                                    loop {
                                        interval.tick(&clock).await;
                                        conn.send_message(Arc::new(PeerMessage::PeersRequest( PeersRequest{
                                            max_peers: Some(PEERS_RESPONSE_MAX_PEERS as u32),
                                            max_direct_peers: Some(MAX_TIER2_PEERS as u32),
                                        })));
                                    }
                                }
                            }));
                            // Send latest block periodically
                            ctx.spawn(wrap_future({
                                let clock = act.clock.clone();
                                let conn = conn.clone();
                                let state = act.network_state.clone();
                                let mut interval = time::Interval::new(clock.now(), SYNC_LATEST_BLOCK_INTERVAL);
                                async move {
                                    loop {
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
                                    // How often should we refresh a nonce from a peer.
                                    // It should be smaller than PRUNE_EDGES_AFTER.
                                    let mut interval = time::Interval::new(start_time + PRUNE_EDGES_AFTER / 3, PRUNE_EDGES_AFTER / 3);
                                    async move {
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
                        }

                        act.network_state.config.event_sink.push(Event::HandshakeCompleted(HandshakeCompletedEvent{
                            stream_id: act.stream_id,
                            edge,
                            tier: conn.tier,
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
        let known_accounts = self.network_state.account_announcements.get_announcements();
        self.send_message_or_log(&PeerMessage::SyncRoutingTable(RoutingTableUpdate::new(
            known_edges,
            known_accounts,
        )));
    }

    fn handle_msg_connecting(&mut self, ctx: &mut actix::Context<Self>, msg: PeerMessage) {
        match (&mut self.peer_status, msg) {
            (
                PeerStatus::Connecting(_, ConnectingStatus::Outbound { handshake_spec, .. }),
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
                        self.network_state.peer_store.add_direct_peer(&self.clock, peer_info);
                        self.stop(ctx, ClosingReason::HandshakeFailed);
                    }
                }
            }
            // TODO(gprusak): LastEdge should rather be a variant of HandshakeFailure.
            // Clean this up (you don't have to modify the proto, just the translation layer).
            (
                PeerStatus::Connecting(_, ConnectingStatus::Outbound { handshake_spec, .. }),
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
            (PeerStatus::Connecting { .. }, PeerMessage::Tier1Handshake(msg)) => {
                self.process_handshake(ctx, tcp::Tier::T1, msg)
            }
            (PeerStatus::Connecting { .. }, PeerMessage::Tier2Handshake(msg)) => {
                self.process_handshake(ctx, tcp::Tier::T2, msg)
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
            RoutedMessageBody::StateResponse(info) => {
                network_state.client.state_response(StateResponseInfo::V1(info)).await;
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
                network_state.shards_manager_adapter.send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                        partial_encoded_chunk_request: request,
                        route_back: msg_hash,
                    },
                );
                None
            }
            RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                network_state.shards_manager_adapter.send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                        partial_encoded_chunk_response: response,
                        received_time: clock.now().into(),
                    },
                );
                None
            }
            RoutedMessageBody::VersionedPartialEncodedChunk(chunk) => {
                network_state
                    .shards_manager_adapter
                    .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(chunk));
                None
            }
            RoutedMessageBody::PartialEncodedChunkForward(msg) => {
                network_state
                    .shards_manager_adapter
                    .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(msg));
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
            .delayed_push(|| Event::MessageProcessed(conn.tier, msg.clone()));
        let was_requested = match &msg {
            PeerMessage::Block(block) => {
                self.network_state.txns_since_last_block.store(0, Ordering::Release);
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
            PeerMessage::Disconnect(d) => {
                tracing::debug!(target: "network", "Disconnect signal. Me: {:?} Peer: {:?}", self.my_node_info.id, self.other_peer_id());

                if d.remove_from_connection_store {
                    self.network_state
                        .connection_store
                        .remove_from_connection_store(self.other_peer_id().unwrap())
                }

                self.stop(ctx, ClosingReason::DisconnectMessage);
            }
            PeerMessage::Tier1Handshake(_) | PeerMessage::Tier2Handshake(_) => {
                // Received handshake after already have seen handshake from this peer.
                tracing::debug!(target: "network", "Duplicate handshake from {}", self.peer_info);
            }
            PeerMessage::PeersRequest(PeersRequest { max_peers, max_direct_peers }) => {
                let mut num_peers = self.network_state.config.max_send_peers;
                if let Some(max_peers) = max_peers {
                    num_peers = min(num_peers, max_peers);
                }
                let peers = self.network_state.peer_store.healthy_peers(num_peers as usize);

                let mut direct_peers = self.network_state.get_direct_peers();
                if let Some(max_direct_peers) = max_direct_peers {
                    if direct_peers.len() > max_direct_peers as usize {
                        direct_peers = direct_peers
                            .into_iter()
                            .choose_multiple(&mut thread_rng(), max_direct_peers as usize);
                    }
                }

                if !peers.is_empty() || !direct_peers.is_empty() {
                    tracing::debug!(target: "network", "Peers request from {}: sending {} peers and {} direct peers.", self.peer_info, peers.len(), direct_peers.len());
                    self.send_message_or_log(&PeerMessage::PeersResponse(PeersResponse {
                        peers,
                        direct_peers,
                    }));
                }
                self.network_state
                    .config
                    .event_sink
                    .push(Event::MessageProcessed(conn.tier, peer_msg));
            }
            PeerMessage::PeersResponse(PeersResponse { peers, direct_peers }) => {
                tracing::debug!(target: "network", "Received peers from {}: {} peers and {} direct peers.", self.peer_info, peers.len(), direct_peers.len());

                // Check for abusive behavior (sending too many peers)
                if peers.len() > PEERS_RESPONSE_MAX_PEERS.try_into().unwrap() {
                    self.stop(ctx, ClosingReason::Ban(ReasonForBan::Abusive));
                }
                // Check for abusive behavior (sending too many direct peers)
                if direct_peers.len() > MAX_TIER2_PEERS {
                    self.stop(ctx, ClosingReason::Ban(ReasonForBan::Abusive));
                }

                // Add received peers to the peer store
                let node_id = self.network_state.config.node_id();
                self.network_state.peer_store.add_indirect_peers(
                    &self.clock,
                    peers.into_iter().filter(|peer_info| peer_info.id != node_id),
                );
                // Direct peers of the responding peer are still indirect peers for this node.
                // However, we may treat them with more trust in the future.
                self.network_state.peer_store.add_indirect_peers(
                    &self.clock,
                    direct_peers.into_iter().filter(|peer_info| peer_info.id != node_id),
                );

                self.network_state
                    .config
                    .event_sink
                    .push(Event::MessageProcessed(conn.tier, peer_msg));
            }
            PeerMessage::RequestUpdateNonce(edge_info) => {
                let clock = self.clock.clone();
                let network_state = self.network_state.clone();
                ctx.spawn(wrap_future(async move {
                    let peer_id = &conn.peer_info.id;
                    match network_state.graph.load().local_edges.get(peer_id) {
                        Some(cur_edge)
                            if cur_edge.edge_type() == EdgeState::Active
                                && cur_edge.nonce() >= edge_info.nonce =>
                        {
                            // Found a newer local edge, so just send it to the peer.
                            conn.send_message(Arc::new(PeerMessage::SyncRoutingTable(
                                RoutingTableUpdate::from_edges(vec![cur_edge.clone()]),
                            )));
                        }
                        // Sign the edge and broadcast it to everyone (finalize_edge does both).
                        _ => {
                            if let Err(ban_reason) = network_state
                                .finalize_edge(&clock, peer_id.clone(), edge_info)
                                .await
                            {
                                conn.stop(Some(ban_reason));
                            }
                        }
                    };
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                }));
            }
            PeerMessage::SyncRoutingTable(rtu) => {
                let clock = self.clock.clone();
                let conn = conn.clone();
                let network_state = self.network_state.clone();
                ctx.spawn(wrap_future(async move {
                    Self::handle_sync_routing_table(&clock, &network_state, conn.clone(), rtu)
                        .await;
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                }));
            }
            PeerMessage::DistanceVector(dv) => {
                let clock = self.clock.clone();
                let conn = conn.clone();
                let network_state = self.network_state.clone();
                ctx.spawn(wrap_future(async move {
                    Self::handle_distance_vector(&clock, &network_state, conn.clone(), dv).await;
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                }));
            }
            PeerMessage::SyncAccountsData(msg) => {
                metrics::SYNC_ACCOUNTS_DATA
                    .with_label_values(&[
                        "received",
                        metrics::bool_to_str(msg.incremental),
                        metrics::bool_to_str(msg.requesting_full_sync),
                    ])
                    .inc();
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
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                    return;
                }
                let network_state = self.network_state.clone();
                let clock = self.clock.clone();
                ctx.spawn(wrap_future(async move {
                    if let Some(err) =
                        network_state.add_accounts_data(&clock, msg.accounts_data).await
                    {
                        conn.stop(Some(match err {
                            AccountDataError::InvalidSignature => ReasonForBan::InvalidSignature,
                            AccountDataError::DataTooLarge => ReasonForBan::Abusive,
                            AccountDataError::SingleAccountMultipleData => ReasonForBan::Abusive,
                        }));
                    }
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                }));
            }
            PeerMessage::Routed(mut msg) => {
                tracing::trace!(
                    target: "network",
                    "Received routed message from {} to {:?}.",
                    self.peer_info,
                    msg.target);
                let for_me = self.network_state.message_for_me(&msg.target);
                if for_me {
                    // Check if we have already received this message.
                    let fastest = self
                        .network_state
                        .recent_routed_messages
                        .lock()
                        .put(CryptoHash::hash_borsh(&msg.body), ())
                        .is_none();
                    // Register that the message has been received.
                    metrics::record_routed_msg_metrics(&self.clock, &msg, conn.tier, fastest);
                }

                // Drop duplicated messages routed within DROP_DUPLICATED_MESSAGES_PERIOD ms
                let key = (msg.author.clone(), msg.target.clone(), msg.signature.clone());
                let now = self.clock.now();
                if let Some(&t) = self.routed_message_cache.get(&key) {
                    if now <= t + DROP_DUPLICATED_MESSAGES_PERIOD {
                        metrics::MessageDropped::Duplicate.inc(&msg.body);
                        self.network_state.config.event_sink.push(Event::RoutedMessageDropped);
                        tracing::debug!(target: "network", "Dropping duplicated message from {} to {:?}", msg.author, msg.target);
                        return;
                    }
                }
                if let RoutedMessageBody::ForwardTx(_) = &msg.body {
                    // Check whenever we exceeded number of transactions we got since last block.
                    // If so, drop the transaction.
                    let r = self.network_state.txns_since_last_block.load(Ordering::Acquire);
                    // TODO(gprusak): this constraint doesn't take into consideration such
                    // parameters as number of nodes or number of shards. Reconsider why do we need
                    // this and whether this is really the right way of handling it.
                    if r > MAX_TRANSACTIONS_PER_BLOCK_MESSAGE {
                        metrics::MessageDropped::TransactionsPerBlockExceeded.inc(&msg.body);
                        return;
                    }
                    self.network_state.txns_since_last_block.fetch_add(1, Ordering::AcqRel);
                }
                self.routed_message_cache.put(key, now);

                if !msg.verify() {
                    // Received invalid routed message from peer.
                    self.stop(ctx, ClosingReason::Ban(ReasonForBan::InvalidSignature));
                    return;
                }

                self.network_state.add_route_back(&self.clock, &conn, msg.as_ref());
                if for_me {
                    // Handle Ping and Pong message if they are for us without sending to client.
                    // i.e. Return false in case of Ping and Pong
                    match &msg.body {
                        RoutedMessageBody::Ping(ping) => {
                            self.network_state.send_pong(
                                &self.clock,
                                conn.tier,
                                ping.nonce,
                                msg.hash(),
                            );
                            // TODO(gprusak): deprecate Event::Ping/Pong in favor of
                            // MessageProcessed.
                            self.network_state.config.event_sink.push(Event::Ping(ping.clone()));
                            self.network_state
                                .config
                                .event_sink
                                .push(Event::MessageProcessed(conn.tier, PeerMessage::Routed(msg)));
                        }
                        RoutedMessageBody::Pong(pong) => {
                            self.network_state.config.event_sink.push(Event::Pong(pong.clone()));
                            self.network_state
                                .config
                                .event_sink
                                .push(Event::MessageProcessed(conn.tier, PeerMessage::Routed(msg)));
                        }
                        _ => self.receive_message(ctx, &conn, PeerMessage::Routed(msg)),
                    }
                } else {
                    if msg.decrease_ttl() {
                        self.network_state.send_message_to_peer(&self.clock, conn.tier, msg);
                    } else {
                        self.network_state.config.event_sink.push(Event::RoutedMessageDropped);
                        tracing::warn!(target: "network", ?msg, from = ?conn.peer_info.id, "Message dropped because TTL reached 0.");
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
            .account_announcements
            .get_broadcasted_announcements(rtu.accounts.iter().map(|a| &a.account_id));
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

    async fn handle_distance_vector(
        clock: &time::Clock,
        network_state: &Arc<NetworkState>,
        conn: Arc<connection::Connection>,
        distance_vector: DistanceVector,
    ) {
        let _span = tracing::trace_span!(target: "network", "handle_distance_vector").entered();

        if conn.peer_info.id != distance_vector.root {
            conn.stop(Some(ReasonForBan::InvalidDistanceVector));
            return;
        }

        if let Err(ban_reason) = network_state
            .update_routes(&clock, NetworkTopologyChange::PeerAdvertisedDistances(distance_vector))
            .await
        {
            conn.stop(Some(ban_reason));
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
        if let PeerStatus::Connecting(_, ConnectingStatus::Outbound { handshake_spec, .. }) =
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
        match &self.closing_reason {
            None => {
                // Due to Actix semantics, sometimes closing reason may be not set.
                // But it is only expected to happen in tests.
                tracing::error!(target:"network", "closing reason not set. This should happen only in tests.");
            }
            Some(reason) => {
                tracing::info!(target: "network", "{:?}: Peer {} disconnected, reason: {reason}", self.my_node_info.id, self.peer_info);

                // If we are on the inbound side of the connection, set a flag in the disconnect
                // message advising the outbound side whether to attempt to re-establish the connection.
                let remove_from_connection_store =
                    self.peer_type == PeerType::Inbound && reason.remove_from_connection_store();

                self.send_message_or_log(&PeerMessage::Disconnect(Disconnect {
                    remove_from_connection_store,
                }));
            }
        }

        match &self.peer_status {
            // If PeerActor is in Connecting state, then
            // it was not registered in the NetworkState,
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
                | io::ErrorKind::BrokenPipe
                // libc::ETIIMEDOUT = 110, translates to io::ErrorKind::TimedOut.
                | io::ErrorKind::TimedOut => true,
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

        if self.closing_reason.is_some() {
            tracing::warn!(target: "network", "Received message from closing connection {:?}. Ignoring", self.peer_type);
            return;
        }

        // Message type agnostic stats.
        {
            metrics::PEER_DATA_RECEIVED_BYTES.inc_by(msg.len() as u64);
            tracing::trace!(target: "network", msg_len=msg.len());
            self.tracker.lock().increment_received(&self.clock, msg.len() as u64);
        }

        let mut peer_msg = match self.parse_message(&msg) {
            Ok(msg) => msg,
            Err(err) => {
                tracing::debug!(target: "network", "Received invalid data {} from {}: {}", near_fmt::AbbrBytes(&msg), self.peer_info, err);
                return;
            }
        };

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
                // Check if the message type is allowed given the TIER of the connection:
                // TIER1 connections are reserved exclusively for BFT consensus messages.
                if !conn.tier.is_allowed(&peer_msg) {
                    tracing::warn!(target: "network", "Received {} on {:?} connection, disconnecting",peer_msg.msg_variant(),conn.tier);
                    // TODO(gprusak): this is abusive behavior. Consider banning for it.
                    self.stop(ctx, ClosingReason::DisallowedMessage);
                    return;
                }

                // Optionally, ignore any received tombstones after startup. This is to
                // prevent overload from too much accumulated deleted edges.
                //
                // We have similar code to skip sending tombstones, here we handle the
                // case when our peer doesn't use that logic yet.
                if let Some(skip_tombstones) = self.network_state.config.skip_tombstones {
                    if let PeerMessage::SyncRoutingTable(routing_table) = &mut peer_msg {
                        if conn.established_time + skip_tombstones > self.clock.now() {
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
                None => ClosingReason::PeerManagerRequest,
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
    Connecting(HandshakeSignalSender, ConnectingStatus),
    /// Ready to go.
    Ready(Arc<connection::Connection>),
}
