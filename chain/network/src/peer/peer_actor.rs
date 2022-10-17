use crate::accounts_data;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::concurrency::demux;
use crate::network_protocol::{
    Edge, EdgeState, Encoding, ParsePeerMessageError, PartialEdgeInfo, PeerChainInfoV2, PeerInfo,
    RawRoutedMessage, RoutedMessageBody, SyncAccountsData,
};
use crate::peer::stream;
use crate::peer::tracker::Tracker;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor::Event;
use crate::private_actix::{
    PeerToManagerMsg, PeerToManagerMsgResp, PeersRequest, PeersResponse, RegisterPeer,
    RegisterPeerError, RegisterPeerResponse, SendMessage, Unregister,
};
use crate::routing::edge::verify_nonce;
use crate::stats::metrics;
use crate::tcp;
use crate::time;
use crate::types::{
    Ban, Handshake, HandshakeFailureReason, NetworkClientMessages, NetworkClientResponses,
    NetworkViewClientMessages, NetworkViewClientResponses, PeerIdOrHash, PeerMessage, PeerType,
    ReasonForBan, StateResponseInfo,
};
use actix::fut::future::wrap_future;
use actix::{Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, Running};
use lru::LruCache;
use near_crypto::Signature;
use near_o11y::OpenTelemetrySpanExt;
use near_o11y::{log_assert, WithSpanContext, WithSpanContextExt};
use near_performance_metrics_macros::perf;
use near_primitives::logging;
use near_primitives::network::PeerId;
use near_primitives::utils::DisplayOption;
use near_primitives::version::{
    ProtocolVersion, PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

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
    /// OUTBOUND-ONLY: Handshake specification. For outbound connections it is initialized
    /// in constructor and then can change as HandshakeFailure and LastEdge messages
    /// are received. For inbound connections, handshake is stateless.

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
                    partial_edge_info: network_state.propose_edge(peer_id, None),
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
        let _span = tracing::trace_span!(target: "network", "parse_message").entered();
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
        // Rate limit PeersRequest messages.
        // TODO(gprusak): upgrade it to a more general rate limiting.
        if let (PeerStatus::Ready(conn), PeerMessage::PeersRequest) = (&self.peer_status, msg) {
            let now = self.clock.now();
            match conn.last_time_peer_requested.load() {
                Some(last) if now < last + REQUEST_PEERS_INTERVAL => return,
                _ => conn.last_time_peer_requested.store(Some(now)),
            }
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
            PeerMessage::Block(b) if self.tracker.lock().has_received(b.hash()) => return,
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
        let chain_info = self.network_state.chain_info.load();
        let handshake = Handshake {
            protocol_version: spec.protocol_version,
            oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
            sender_peer_id: self.network_state.config.node_id(),
            target_peer_id: spec.peer_id,
            sender_listen_port: self.network_state.config.node_addr.map(|a| a.port()),
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: self.network_state.genesis_id.clone(),
                height: chain_info.height,
                tracked_shards: chain_info.tracked_shards.clone(),
                archival: self.network_state.config.archive,
            },
            partial_edge_info: spec.partial_edge_info,
        };
        let msg = PeerMessage::Handshake(handshake);
        self.send_message_or_log(&msg);
    }

    fn stop(&mut self, ctx: &mut Context<PeerActor>, reason: ClosingReason) {
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

    fn receive_message(
        &mut self,
        ctx: &mut Context<PeerActor>,
        conn: &connection::Connection,
        msg: PeerMessage,
    ) {
        if msg.is_view_client_message() {
            metrics::PEER_VIEW_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL
                .with_label_values(&[msg.msg_variant()])
                .inc();
            self.receive_view_client_message(ctx, msg);
        } else if msg.is_client_message() {
            metrics::PEER_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL
                .with_label_values(&[msg.msg_variant()])
                .inc();
            self.receive_client_message(ctx, conn, msg);
        } else {
            debug_assert!(false, "expected (view) client message, got: {}", msg.msg_variant());
        }
    }

    fn receive_view_client_message(&self, ctx: &mut Context<PeerActor>, msg: PeerMessage) {
        let mut msg_hash = None;
        let view_client_message = match msg {
            PeerMessage::Routed(message) => {
                msg_hash = Some(message.hash());
                match &message.msg.body {
                    RoutedMessageBody::TxStatusRequest(account_id, tx_hash) => {
                        NetworkViewClientMessages::TxStatus {
                            tx_hash: tx_hash.clone(),
                            signer_account_id: account_id.clone(),
                        }
                    }
                    RoutedMessageBody::TxStatusResponse(tx_result) => {
                        NetworkViewClientMessages::TxStatusResponse(Box::new(tx_result.clone()))
                    }
                    RoutedMessageBody::ReceiptOutcomeRequest(_receipt_id) => {
                        // Silently ignore for the time being.  We’ve been still
                        // sending those messages at protocol version 56 so we
                        // need to wait until 59 before we can remove the
                        // variant completely.
                        return;
                    }
                    RoutedMessageBody::StateRequestHeader(shard_id, sync_hash) => {
                        NetworkViewClientMessages::StateRequestHeader {
                            shard_id: *shard_id,
                            sync_hash: sync_hash.clone(),
                        }
                    }
                    RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id) => {
                        NetworkViewClientMessages::StateRequestPart {
                            shard_id: *shard_id,
                            sync_hash: sync_hash.clone(),
                            part_id: *part_id,
                        }
                    }
                    body => {
                        error!(target: "network", "Peer receive_view_client_message received unexpected type: {:?}", body);
                        return;
                    }
                }
            }
            PeerMessage::BlockRequest(hash) => NetworkViewClientMessages::BlockRequest(hash),
            PeerMessage::BlockHeadersRequest(hashes) => {
                NetworkViewClientMessages::BlockHeadersRequest(hashes)
            }
            PeerMessage::EpochSyncRequest(epoch_id) => {
                NetworkViewClientMessages::EpochSyncRequest { epoch_id }
            }
            PeerMessage::EpochSyncFinalizationRequest(epoch_id) => {
                NetworkViewClientMessages::EpochSyncFinalizationRequest { epoch_id }
            }
            peer_message => {
                error!(target: "network", "Peer receive_view_client_message received unexpected type: {:?}", peer_message);
                return;
            }
        };

        ctx.spawn(wrap_future(self.network_state.view_client_addr.send(view_client_message)).then(
            move |res, act: &mut PeerActor, _ctx| {
                // Ban peer if client thinks received data is bad.
                match res {
                    Ok(NetworkViewClientResponses::TxStatus(tx_result)) => {
                        let msg = act.network_state.sign_message(
                            &act.clock,
                            RawRoutedMessage {
                                // TODO(gprusak): Rename AccountIrPeerIdOrHash to
                                // RoutedMessageTarget for better readability.
                                target: PeerIdOrHash::Hash(msg_hash.unwrap()),
                                body: RoutedMessageBody::TxStatusResponse(*tx_result),
                            },
                        );
                        act.network_state.send_message_to_peer(&act.clock, msg);
                    }
                    Ok(NetworkViewClientResponses::StateResponse(state_response)) => {
                        let body = match *state_response {
                            StateResponseInfo::V1(state_response) => {
                                RoutedMessageBody::StateResponse(state_response)
                            }
                            state_response @ StateResponseInfo::V2(_) => {
                                RoutedMessageBody::VersionedStateResponse(state_response)
                            }
                        };
                        let msg = act.network_state.sign_message(
                            &act.clock,
                            RawRoutedMessage {
                                target: PeerIdOrHash::Hash(msg_hash.unwrap()),
                                body,
                            },
                        );
                        act.network_state.send_message_to_peer(&act.clock, msg);
                    }
                    Ok(NetworkViewClientResponses::Block(block)) => {
                        // MOO need protocol version
                        act.send_message_or_log(&PeerMessage::Block(*block));
                    }
                    Ok(NetworkViewClientResponses::BlockHeaders(headers)) => {
                        act.send_message_or_log(&PeerMessage::BlockHeaders(headers));
                    }
                    Ok(NetworkViewClientResponses::EpochSyncResponse(response)) => {
                        act.send_message_or_log(&PeerMessage::EpochSyncResponse(response));
                    }
                    Ok(NetworkViewClientResponses::EpochSyncFinalizationResponse(response)) => {
                        act.send_message_or_log(&PeerMessage::EpochSyncFinalizationResponse(
                            response,
                        ));
                    }
                    Err(err) => {
                        error!(
                            target: "network",
                            "Received error sending message to view client: {} for {}",
                            err, act.peer_info
                        );
                        return actix::fut::ready(());
                    }
                    _ => {}
                };
                actix::fut::ready(())
            },
        ));
    }

    /// Process non handshake/peer related messages.
    fn receive_client_message(
        &mut self,
        ctx: &mut Context<PeerActor>,
        conn: &connection::Connection,
        msg: PeerMessage,
    ) {
        let _span = tracing::trace_span!(target: "network", "receive_client_message").entered();
        let peer_id = conn.peer_info.id.clone();

        // This is a fancy way to clone the message iff event_sink is non-null.
        // If you have a better idea on how to achieve that, feel free to improve this.
        let message_processed_event = self
            .network_state
            .config
            .event_sink
            .delayed_push(|| Event::MessageProcessed(msg.clone()));

        // Wrap peer message into what client expects.
        let network_client_msg = match msg {
            PeerMessage::Block(block) => {
                let block_hash = *block.hash();
                self.tracker.lock().push_received(block_hash);
                conn.chain_height.fetch_max(block.header().height(), Ordering::Relaxed);
                NetworkClientMessages::Block(
                    block,
                    peer_id,
                    self.tracker.lock().has_request(&block_hash),
                )
            }
            PeerMessage::Transaction(transaction) => NetworkClientMessages::Transaction {
                transaction,
                is_forwarded: false,
                check_only: false,
            },
            PeerMessage::BlockHeaders(headers) => {
                NetworkClientMessages::BlockHeaders(headers, peer_id)
            }
            // All Routed messages received at this point are for us.
            PeerMessage::Routed(routed_message) => {
                let msg_hash = routed_message.hash();

                match &routed_message.msg.body {
                    RoutedMessageBody::BlockApproval(approval) => {
                        NetworkClientMessages::BlockApproval(approval.clone(), peer_id)
                    }
                    RoutedMessageBody::ForwardTx(transaction) => {
                        NetworkClientMessages::Transaction {
                            transaction: transaction.clone(),
                            is_forwarded: true,
                            check_only: false,
                        }
                    }

                    RoutedMessageBody::StateResponse(info) => {
                        NetworkClientMessages::StateResponse(StateResponseInfo::V1(info.clone()))
                    }
                    RoutedMessageBody::VersionedStateResponse(info) => {
                        NetworkClientMessages::StateResponse(info.clone())
                    }
                    RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                        NetworkClientMessages::PartialEncodedChunkRequest(request.clone(), msg_hash)
                    }
                    RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                        NetworkClientMessages::PartialEncodedChunkResponse(
                            response.clone(),
                            self.clock.now().into(),
                        )
                    }
                    RoutedMessageBody::VersionedPartialEncodedChunk(chunk) => {
                        NetworkClientMessages::PartialEncodedChunk(chunk.clone())
                    }
                    RoutedMessageBody::PartialEncodedChunkForward(forward) => {
                        NetworkClientMessages::PartialEncodedChunkForward(forward.clone())
                    }
                    RoutedMessageBody::Ping(_)
                    | RoutedMessageBody::Pong(_)
                    | RoutedMessageBody::TxStatusRequest(_, _)
                    | RoutedMessageBody::TxStatusResponse(_)
                    | RoutedMessageBody::_UnusedQueryRequest
                    | RoutedMessageBody::_UnusedQueryResponse
                    | RoutedMessageBody::_UnusedPartialEncodedChunk
                    | RoutedMessageBody::ReceiptOutcomeRequest(_)
                    | RoutedMessageBody::_UnusedReceiptOutcomeResponse
                    | RoutedMessageBody::StateRequestHeader(_, _)
                    | RoutedMessageBody::StateRequestPart(_, _, _) => {
                        error!(target: "network", "Peer receive_client_message received unexpected type: {:?}", routed_message);
                        return;
                    }
                }
            }
            PeerMessage::Challenge(challenge) => NetworkClientMessages::Challenge(challenge),
            PeerMessage::EpochSyncResponse(response) => {
                NetworkClientMessages::EpochSyncResponse(peer_id, response)
            }
            PeerMessage::EpochSyncFinalizationResponse(response) => {
                NetworkClientMessages::EpochSyncFinalizationResponse(peer_id, response)
            }
            PeerMessage::Handshake(_)
            | PeerMessage::HandshakeFailure(_, _)
            | PeerMessage::PeersRequest
            | PeerMessage::PeersResponse(_)
            | PeerMessage::SyncRoutingTable(_)
            | PeerMessage::LastEdge(_)
            | PeerMessage::Disconnect
            | PeerMessage::RequestUpdateNonce(_)
            | PeerMessage::ResponseUpdateNonce(_)
            | PeerMessage::BlockRequest(_)
            | PeerMessage::BlockHeadersRequest(_)
            | PeerMessage::EpochSyncRequest(_)
            | PeerMessage::EpochSyncFinalizationRequest(_)
            | PeerMessage::SyncAccountsData(_) => {
                error!(target: "network", "Peer receive_client_message received unexpected type: {:?}", msg);
                return;
            }
        }.with_span_context();

        ctx.spawn(wrap_future(self.network_state.client_addr.send(network_client_msg))
            .then(move |res, act: &mut PeerActor, ctx| {
                // Ban peer if client thinks received data is bad.
                match res {
                    Ok(NetworkClientResponses::InvalidTx(err)) => {
                        warn!(target: "network", "Received invalid tx from peer {}: {}", act.peer_info, err);
                        // TODO: count as malicious behavior?
                    }
                    Ok(NetworkClientResponses::Ban { ban_reason }) => {
                        act.stop(ctx, ClosingReason::Ban(ban_reason));
                    }
                    Err(err) => {
                        error!(
                            target: "network",
                            "Received error sending message to client: {} for {}",
                            err, act.peer_info
                        );
                        return actix::fut::ready(());
                    }
                    _ => {
                        message_processed_event();
                    }
                };
                actix::fut::ready(())
            })
        );
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
        debug!(target: "network", "{:?}: Received handshake {:?}", self.my_node_info.id, handshake);
        let cs = match &self.peer_status {
            PeerStatus::Connecting(it) => it,
            _ => panic!("process_handshake called in non-connecting state"),
        };
        match cs {
            ConnectingStatus::Outbound { handshake_spec: spec, .. } => {
                if handshake.protocol_version != spec.protocol_version {
                    warn!(target: "network", "Protocol version mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                if handshake.sender_chain_info.genesis_id != self.network_state.genesis_id {
                    warn!(target: "network", "Genesis mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                if handshake.sender_peer_id != spec.peer_id {
                    warn!(target: "network", "PeerId mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                if handshake.partial_edge_info.nonce != spec.partial_edge_info.nonce {
                    warn!(target: "network", "Nonce mismatch. Disconnecting peer {}", handshake.sender_peer_id);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
            }
            ConnectingStatus::Inbound { .. } => {
                if PEER_MIN_ALLOWED_PROTOCOL_VERSION > handshake.protocol_version
                    || handshake.protocol_version > PROTOCOL_VERSION
                {
                    debug!(
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
                    debug!(target: "network", "Received connection from node with different genesis.");
                    self.send_message_or_log(&PeerMessage::HandshakeFailure(
                        self.my_node_info.clone(),
                        HandshakeFailureReason::GenesisMismatch(genesis_id),
                    ));
                    return;
                }
                if handshake.target_peer_id != self.my_node_info.id {
                    debug!(target: "network", "Received handshake from {:?} to {:?} but I am {:?}", handshake.sender_peer_id, handshake.target_peer_id, self.my_node_info.id);
                    self.send_message_or_log(&PeerMessage::HandshakeFailure(
                        self.my_node_info.clone(),
                        HandshakeFailureReason::InvalidTarget,
                    ));
                    return;
                }
                // Verify if nonce is sane.
                if let Err(err) = verify_nonce(&self.clock, handshake.partial_edge_info.nonce) {
                    debug!(target: "network", nonce=?handshake.partial_edge_info.nonce, my_node_id = ?self.my_node_id(), peer_id=?handshake.sender_peer_id, "bad nonce, disconnecting: {err}");
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                // Check that the received nonce is greater than the current nonce of this connection.
                // If not (and this is an inbound connection) propose a new nonce.
                if let Some(last_edge) =
                    self.network_state.routing_table_view.get_local_edge(&handshake.sender_peer_id)
                {
                    if last_edge.nonce() >= handshake.partial_edge_info.nonce {
                        debug!(target: "network", "{:?}: Received too low nonce from peer {:?} sending evidence.", self.my_node_id(), self.peer_addr);
                        self.send_message_or_log(&PeerMessage::LastEdge(last_edge));
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
            warn!(target: "network", "partial edge with invalid signature, disconnecting");
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
                self.network_state.propose_edge(&handshake.sender_peer_id, Some(nonce))
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
            initial_chain_info: handshake.sender_chain_info.clone(),
            chain_height: AtomicU64::new(handshake.sender_chain_info.height),
            edge,
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
            tokio::time::interval(self.network_state.config.peer_stats_period.try_into().unwrap());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ctx.spawn({
            let conn = conn.clone();
            wrap_future(async move {
                loop {
                    interval.tick().await;
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

        // Here we stop processing any PeerActor events until PeerManager
        // decides whether to accept the connection or not: ctx.wait makes
        // the actor event loop poll on the future until it completes before
        // processing any other events.
        ctx.wait(wrap_future(self.network_state.peer_manager_addr
                .send(PeerToManagerMsg::RegisterPeer(RegisterPeer {
                    connection: conn.clone(),
                }))
            )
            .then(move |res, act: &mut PeerActor, ctx| {
                match res.map(|r|r.unwrap_consolidate_response()) {
                    Ok(RegisterPeerResponse::Accept) => {
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
                        }
                        act.network_state.config.event_sink.push(Event::HandshakeCompleted(HandshakeCompletedEvent{
                            stream_id: act.stream_id,
                            edge: conn.edge.clone(),
                        }));
                    },
                    Ok(RegisterPeerResponse::Reject(err)) => {
                        info!(target: "network", "{:?}: Connection with {:?} rejected by PeerManager: {:?}", act.my_node_id(),conn.peer_info.id,err);
                        act.stop(ctx,ClosingReason::RejectedByPeerManager(err));
                    }
                    Err(err) => {
                        // TODO(gprusak): this shouldn't happen at all.
                        info!(target: "network", "{:?}: Peer with handshake {:?} wasn't consolidated, disconnecting: {err:?}", act.my_node_id(), handshake);
                        act.stop(ctx,ClosingReason::HandshakeFailed);
                    }
                };
                actix::fut::ready(())
            })
        );
    }

    fn handle_msg_connecting(&mut self, ctx: &mut actix::Context<Self>, msg: PeerMessage) {
        match (&mut self.peer_status, msg) {
            (
                PeerStatus::Connecting(ConnectingStatus::Outbound { handshake_spec, .. }),
                PeerMessage::HandshakeFailure(peer_info, reason),
            ) => {
                match reason {
                    HandshakeFailureReason::GenesisMismatch(genesis) => {
                        warn!(target: "network", "Attempting to connect to a node ({}) with a different genesis block. Our genesis: {:?}, their genesis: {:?}", peer_info, self.network_state.genesis_id, genesis);
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
                            warn!(target: "network", "Unable to connect to a node ({}) due to a network protocol version mismatch. Our version: {:?}, their: {:?}", peer_info, (PROTOCOL_VERSION, PEER_MIN_ALLOWED_PROTOCOL_VERSION), (version, oldest_supported_version));
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
                        debug!(target: "network", "Peer found was not what expected. Updating peer info with {:?}", peer_info);
                        self.network_state
                            .peer_manager_addr
                            .do_send(PeerToManagerMsg::UpdatePeerInfo(peer_info));
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
                    info!(target: "network", "{:?}: Peer {:?} sent invalid edge. Disconnect.", self.my_node_id(), self.peer_addr);
                    self.stop(ctx, ClosingReason::HandshakeFailed);
                    return;
                }
                // Recreate the edge with a newer nonce.
                handshake_spec.partial_edge_info =
                    self.network_state.propose_edge(&handshake_spec.peer_id, Some(edge.next()));
                let spec = handshake_spec.clone();
                ctx.wait(actix::fut::ready(()).then(move |_, act: &mut Self, _| {
                    act.send_handshake(spec);
                    actix::fut::ready(())
                }));
            }
            (PeerStatus::Connecting { .. }, PeerMessage::Handshake(msg)) => {
                self.process_handshake(ctx, msg)
            }
            (_, msg) => {
                tracing::warn!(target:"network","unexpected message during handshake: {}",msg)
            }
        }
    }

    fn handle_msg_ready(
        &mut self,
        ctx: &mut actix::Context<Self>,
        conn: &connection::Connection,
        peer_msg: PeerMessage,
    ) {
        match peer_msg.clone() {
            PeerMessage::Disconnect => {
                debug!(target: "network", "Disconnect signal. Me: {:?} Peer: {:?}", self.my_node_info.id, self.other_peer_id());
                self.stop(ctx, ClosingReason::DisconnectMessage);
            }
            PeerMessage::Handshake(_) => {
                // Received handshake after already have seen handshake from this peer.
                debug!(target: "network", "Duplicate handshake from {}", self.peer_info);
            }
            PeerMessage::PeersRequest => {
                ctx.spawn(wrap_future(
                    self.network_state.peer_manager_addr.send(PeerToManagerMsg::PeersRequest(PeersRequest {}))
                ).then(|res, act: &mut PeerActor, _ctx| {
                    if let Ok(peers) = res.map(|f|f.unwrap_peers_request_result()) {
                        if !peers.peers.is_empty() {
                            debug!(target: "network", "Peers request from {}: sending {} peers.", act.peer_info, peers.peers.len());
                            act.send_message_or_log(&PeerMessage::PeersResponse(peers.peers));
                        }
                    }
                    actix::fut::ready(())
                })
                );
            }
            PeerMessage::PeersResponse(peers) => {
                debug!(target: "network", "Received peers from {}: {} peers.", self.peer_info, peers.len());
                self.network_state
                    .peer_manager_addr
                    .do_send(PeerToManagerMsg::PeersResponse(PeersResponse { peers }));
                self.network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
            }
            PeerMessage::RequestUpdateNonce(edge_info) => {
                ctx.spawn(
                    wrap_future(self.network_state.peer_manager_addr.send(
                        PeerToManagerMsg::RequestUpdateNonce(
                            self.other_peer_id().unwrap().clone(),
                            edge_info,
                        ),
                    ))
                    .then(|res, act: &mut PeerActor, ctx| {
                        match res.map(|f| f) {
                            Ok(PeerToManagerMsgResp::EdgeUpdate(edge)) => {
                                act.send_message_or_log(&PeerMessage::ResponseUpdateNonce(*edge));
                            }
                            Ok(PeerToManagerMsgResp::BanPeer(reason_for_ban)) => {
                                act.stop(ctx, ClosingReason::Ban(reason_for_ban));
                            }
                            _ => {}
                        }
                        act.network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                        actix::fut::ready(())
                    }),
                );
            }
            PeerMessage::ResponseUpdateNonce(edge) => {
                ctx.spawn(
                    wrap_future(
                        self.network_state
                            .peer_manager_addr
                            .send(PeerToManagerMsg::ResponseUpdateNonce(edge)),
                    )
                    .then(|res, act: &mut PeerActor, ctx| {
                        match res {
                            Ok(PeerToManagerMsgResp::BanPeer(reason_for_ban)) => {
                                act.stop(ctx, ClosingReason::Ban(reason_for_ban))
                            }
                            _ => {}
                        }
                        act.network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                        actix::fut::ready(())
                    }),
                );
            }
            PeerMessage::SyncRoutingTable(routing_table_update) => {
                self.network_state.peer_manager_addr.do_send(PeerToManagerMsg::SyncRoutingTable {
                    peer_id: conn.peer_info.id.clone(),
                    routing_table_update,
                });
            }
            PeerMessage::SyncAccountsData(msg) => {
                let peer_id = conn.peer_info.id.clone();
                let pms = self.network_state.clone();
                // In case a full sync is requested, immediately send what we got.
                // It is a microoptimization: we do not send back the data we just received.
                if msg.requesting_full_sync {
                    self.send_message_or_log(&PeerMessage::SyncAccountsData(SyncAccountsData {
                        requesting_full_sync: false,
                        incremental: false,
                        accounts_data: pms.accounts_data.load().data.values().cloned().collect(),
                    }));
                }
                ctx.spawn(
                    wrap_future(async move {
                        // Early exit, if there is no data in the message.
                        if msg.accounts_data.is_empty() {
                            return None;
                        }
                        // Verify and add the new data to the internal state.
                        let (new_data, err) =
                            pms.accounts_data.clone().insert(msg.accounts_data).await;
                        // Broadcast any new data we have found, even in presence of an error.
                        // This will prevent a malicious peer from forcing us to re-verify valid
                        // datasets. See accounts_data::Cache documentation for details.
                        if new_data.len() > 0 {
                            let handles: Vec<_> = pms
                                .tier2
                                .load()
                                .ready
                                .values()
                                // Do not send the data back.
                                .filter(|p| peer_id != p.peer_info.id)
                                .map(|p| p.send_accounts_data(new_data.clone()))
                                .collect();
                            futures_util::future::join_all(handles).await;
                        }
                        err.map(|err| match err {
                            accounts_data::Error::InvalidSignature => {
                                ReasonForBan::InvalidSignature
                            }
                            accounts_data::Error::DataTooLarge => ReasonForBan::Abusive,
                            accounts_data::Error::SingleAccountMultipleData => {
                                ReasonForBan::Abusive
                            }
                        })
                    })
                    .map(|ban_reason, act: &mut PeerActor, ctx| {
                        if let Some(ban_reason) = ban_reason {
                            act.stop(ctx, ClosingReason::Ban(ban_reason));
                        }
                        act.network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                    }),
                );
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
                    self.network_state.routing_table_view.add_route_back(
                        &self.clock,
                        msg.hash(),
                        from.clone(),
                    );
                }
                if self.network_state.message_for_me(&msg.target) {
                    metrics::record_routed_msg_latency(&self.clock, &msg);
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
                        _ => {
                            self.receive_message(ctx, conn, PeerMessage::Routed(msg.clone()));
                        }
                    }
                } else {
                    if msg.decrease_ttl() {
                        self.network_state.send_message_to_peer(&self.clock, msg);
                    } else {
                        self.network_state.config.event_sink.push(Event::RoutedMessageDropped);
                        warn!(target: "network", ?msg, ?from, "Message dropped because TTL reached 0.");
                        metrics::ROUTED_MESSAGE_DROPPED
                            .with_label_values(&[msg.body_variant()])
                            .inc();
                    }
                }
            }
            msg => self.receive_message(ctx, conn, msg),
        }
    }
}

impl Actor for PeerActor {
    type Context = Context<PeerActor>;

    fn started(&mut self, ctx: &mut Self::Context) {
        metrics::PEER_CONNECTIONS_TOTAL.inc();
        debug!(target: "network", "{:?}: Peer {:?} {:?} started", self.my_node_info.id, self.peer_addr, self.peer_type);
        // Set Handshake timeout for stopping actor if peer is not ready after given period of time.

        near_performance_metrics::actix::run_later(
            ctx,
            self.network_state.config.handshake_timeout.try_into().unwrap(),
            move |act, ctx| match act.peer_status {
                PeerStatus::Connecting { .. } => {
                    info!(target: "network", "Handshake timeout expired for {}", act.peer_info);
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

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        metrics::PEER_CONNECTIONS_TOTAL.dec();
        debug!(target: "network", "{:?}: [status = {:?}] Peer {} disconnected.", self.my_node_info.id, self.peer_status, self.peer_info);
        match &self.peer_status {
            // If PeerActor is in Connecting state, then
            // it was not registered in the NewtorkState,
            // so there is nothing to be done.
            PeerStatus::Connecting(..) => {}
            // Clean up the Connection from the NetworkState.
            PeerStatus::Ready(conn) => {
                self.network_state.unregister(conn);
                // Save the fact that we are disconnecting to the PeerStore.
                match &self.closing_reason {
                    Some(ClosingReason::Ban(ban_reason)) => {
                        warn!(target: "network", "Banning peer {} for {:?}", self.peer_info, ban_reason);
                        self.network_state.peer_manager_addr.do_send(PeerToManagerMsg::Ban(Ban {
                            peer_id: conn.peer_info.id.clone(),
                            ban_reason: *ban_reason,
                        }));
                    }
                    _ => {
                        self.network_state.peer_manager_addr.do_send(PeerToManagerMsg::Unregister(
                            Unregister { peer_id: conn.peer_info.id.clone() },
                        ))
                    }
                }
            }
        }
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // closing_reason may be None in case the whole actix system is stopped.
        // It happens a lot in tests.
        if let Some(reason) = self.closing_reason.take() {
            self.network_state.config.event_sink.push(Event::ConnectionClosed(
                ConnectionClosedEvent { stream_id: self.stream_id, reason },
            ));
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
        let _span = tracing::trace_span!(target: "network", "handle", handler = "bytes").entered();
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
                debug!(target: "network", "Received invalid data {:?} from {}: {}", logging::pretty_vec(&msg), self.peer_info, err);
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
                        debug!(target: "network", "Dropping duplicated message from {} to {:?}", msg.author, msg.target);
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
                self.handle_msg_ready(ctx, &conn.clone(), peer_msg);
            }
        }
    }
}

impl Handler<WithSpanContext<SendMessage>> for PeerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<SendMessage>, _: &mut Self::Context) {
        let span = tracing::debug_span!(
                target: "network",
                "handle",
                handler = "SendMessage",
                actor = "PeerActor")
        .entered();
        span.set_parent(msg.context);
        let _d = delay_detector::DelayDetector::new(|| "send message".into());
        self.send_message_or_log(&msg.msg.message);
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
        let span =
            tracing::trace_span!(target: "network", "handle", handler = "Stop", actor="PeerActor")
                .entered();
        span.set_parent(msg.context);
        self.stop(
            ctx,
            match msg.msg.ban_reason {
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
