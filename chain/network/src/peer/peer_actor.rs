use crate::accounts_data;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::concurrency::demux;
use crate::network_protocol::{Encoding, ParsePeerMessageError, SyncAccountsData};
use crate::peer::codec::Codec;
use crate::peer::tracker::Tracker;
use crate::peer_manager::connection;
use crate::peer_manager::peer_manager_actor::{Event, NetworkState};
use crate::private_actix::PeersResponse;
use crate::private_actix::{PeerToManagerMsg, PeerToManagerMsgResp};
use crate::private_actix::{
    PeersRequest, RegisterPeer, RegisterPeerResponse, SendMessage, Unregister,
};
use crate::stats::metrics;
use crate::types::{
    Handshake, HandshakeFailureReason, NetworkClientMessages, NetworkClientResponses, PeerMessage,
};
use actix::{
    Actor, ActorContext, ActorFutureExt, Arbiter, AsyncContext, Context, ContextFutureSpawner,
    Handler, Recipient, Running, StreamHandler, WrapFuture,
};
use lru::LruCache;
use near_crypto::Signature;
use near_network_primitives::time;
use near_network_primitives::types::{
    Ban, EdgeState, NetworkViewClientMessages, NetworkViewClientResponses, PeerChainInfoV2,
    PeerIdOrHash, PeerInfo, PeerManagerRequest, PeerManagerRequestWithContext, PeerType,
    ReasonForBan, RoutedMessage, RoutedMessageBody, RoutedMessageFrom, StateResponseInfo,
};
use near_network_primitives::types::{Edge, PartialEdgeInfo};
use near_performance_metrics::framed_write::{FramedWrite, WriteHandler};
use near_performance_metrics_macros::perf;
use near_primitives::logging;
use near_primitives::network::PeerId;
use near_primitives::sharding::PartialEncodedChunk;
use near_primitives::utils::DisplayOption;
use near_primitives::version::{
    ProtocolVersion, PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use near_rate_limiter::ThrottleController;
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

type WriteHalf = tokio::io::WriteHalf<tokio::net::TcpStream>;

/// Maximum number of messages per minute from single peer.
// TODO(#5453): current limit is way to high due to us sending lots of messages during sync.
const MAX_PEER_MSG_PER_MIN: usize = usize::MAX;

/// Maximum number of transaction messages we will accept between block messages.
/// The purpose of this constant is to ensure we do not spend too much time deserializing and
/// dispatching transactions when we should be focusing on consensus-related messages.
const MAX_TRANSACTIONS_PER_BLOCK_MESSAGE: usize = 1000;
/// Limit cache size of 1000 messages
const ROUTED_MESSAGE_CACHE_SIZE: usize = 1000;
/// Duplicated messages will be dropped if routed through the same peer multiple times.
const DROP_DUPLICATED_MESSAGES_PERIOD: time::Duration = time::Duration::milliseconds(50);

pub(crate) struct PeerActor {
    clock: time::Clock,

    /// Shared state of the network module.
    network_state: Arc<NetworkState>,
    /// This node's id and address (either listening or socket address).
    my_node_info: PeerInfo,

    /// Peer address from connection.
    peer_addr: SocketAddr,
    /// Peer id and info. Present if outbound or ready.
    peer_info: DisplayOption<PeerInfo>,
    /// Peer type.
    peer_type: PeerType,
    /// Peer status.
    peer_status: PeerStatus,
    /// Protocol version to communicate with this peer.
    protocol_version: ProtocolVersion,
    /// Framed wrapper to send messages through the TCP connection.
    framed: FramedWrite<Vec<u8>, WriteHalf, Codec, Codec>,

    /// Peer manager recipient to break the dependency loop.
    /// PeerManager is a recipient of 2 types of messages, therefore
    /// to inject a fake PeerManager in tests, we need a separate
    /// recipient address for each message type.
    peer_manager_addr: Recipient<PeerToManagerMsg>,
    /// Tracker for requests and responses.
    tracker: Arc<Mutex<Tracker>>,
    /// Edge information needed to build the real edge. This is relevant for handshake.
    partial_edge_info: Option<PartialEdgeInfo>,
    /// Cache of recently routed messages, this allows us to drop duplicates
    routed_message_cache: LruCache<(PeerId, PeerIdOrHash, Signature), time::Instant>,
    /// A helper data structure for limiting reading
    throttle_controller: ThrottleController,
    /// Whether we detected support for protocol buffers during handshake.
    protocol_buffers_supported: bool,
    /// Whether the PeerActor should skip protobuf support detection and use
    /// a given encoding right away.
    force_encoding: Option<Encoding>,

    /// Shared state of the connection. Populated once the connection is established.
    connection: Option<Arc<connection::Connection>>,
}

impl Debug for PeerActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.my_node_info)
    }
}

/// A custom IOError type because FramedWrite is hiding the actual
/// underlying std::io::Error.
/// TODO: replace FramedWrite with sth more reasonable.
#[derive(Error, Debug)]
pub enum IOError {
    #[error("{tid} Failed to send message {message_type} of size {size}")]
    Send { tid: usize, message_type: String, size: usize },
}

impl PeerActor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        clock: time::Clock,
        my_node_info: PeerInfo,
        peer_addr: SocketAddr,
        peer_info: Option<PeerInfo>,
        connecting_status: ConnectingStatus,
        framed: FramedWrite<Vec<u8>, WriteHalf, Codec, Codec>,
        peer_manager_addr: Recipient<PeerToManagerMsg>,
        throttle_controller: ThrottleController,
        force_encoding: Option<Encoding>,
        network_state: Arc<NetworkState>,
    ) -> Self {
        PeerActor {
            clock,
            my_node_info,
            peer_addr,
            peer_type: match &connecting_status {
                ConnectingStatus::Inbound { .. } => PeerType::Inbound,
                ConnectingStatus::Outbound { .. } => PeerType::Outbound,
            },
            peer_status: PeerStatus::Connecting(connecting_status),
            protocol_version: PROTOCOL_VERSION,
            framed,
            peer_manager_addr,
            tracker: Default::default(),
            partial_edge_info: peer_info
                .as_ref()
                .map(|info| network_state.propose_edge(&info.id, None)),
            peer_info: peer_info.into(),
            routed_message_cache: LruCache::new(ROUTED_MESSAGE_CACHE_SIZE),
            throttle_controller,
            protocol_buffers_supported: false,
            force_encoding,
            connection: None,
            network_state,
        }
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
        match &self.peer_status {
            PeerStatus::Connecting { .. } => None,
            _ => Some(Encoding::Borsh),
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

    fn send_message_or_log(&mut self, msg: &PeerMessage) {
        if let Err(err) = self.send_message(msg) {
            warn!(target: "network", "send_message(): {}", err);
        }
    }

    fn send_message(&mut self, msg: &PeerMessage) -> Result<(), IOError> {
        if let PeerMessage::PeersRequest = msg {
            self.connection.as_mut().unwrap().last_time_peer_requested.store(self.clock.now());
        }
        if let Some(enc) = self.encoding() {
            return self.send_message_with_encoding(msg, enc);
        }
        self.send_message_with_encoding(msg, Encoding::Proto)?;
        self.send_message_with_encoding(msg, Encoding::Borsh)?;
        Ok(())
    }

    fn send_message_with_encoding(
        &mut self,
        msg: &PeerMessage,
        enc: Encoding,
    ) -> Result<(), IOError> {
        let msg_type: &str = msg.into();
        let _span = tracing::trace_span!(
            target: "network",
            "send_message_with_encoding",
            msg_type)
        .entered();
        // Skip sending block and headers if we received it or header from this peer.
        // Record block requests in tracker.
        match msg {
            PeerMessage::Block(b) if self.tracker.lock().has_received(b.hash()) => return Ok(()),
            PeerMessage::BlockRequest(h) => self.tracker.lock().push_request(*h),
            _ => (),
        };

        let bytes = msg.serialize(enc);
        self.tracker.lock().increment_sent(&self.clock, bytes.len() as u64);
        let bytes_len = bytes.len();
        tracing::trace!(target: "network", msg_len = bytes_len);
        metrics::PEER_DATA_SENT_BYTES.inc_by(bytes_len as u64);
        if !self.framed.write(bytes) {
            #[cfg(feature = "performance_stats")]
            let tid = near_rust_allocator_proxy::get_tid();
            #[cfg(not(feature = "performance_stats"))]
            let tid = 0;
            return Err(IOError::Send { tid, message_type: msg_type.to_string(), size: bytes_len });
        }
        Ok(())
    }

    fn send_handshake(&mut self) {
        if self.other_peer_id().is_none() {
            error!(target: "network", "Sending handshake to an unknown peer");
            return;
        }
        let chain_info = self.network_state.chain_info.load();
        let handshake = match self.protocol_version {
            39..=PROTOCOL_VERSION => PeerMessage::Handshake(Handshake::new(
                self.protocol_version,
                self.my_node_id().clone(),
                self.other_peer_id().unwrap().clone(),
                self.my_node_info.addr_port(),
                PeerChainInfoV2 {
                    genesis_id: self.network_state.genesis_id.clone(),
                    height: chain_info.height,
                    tracked_shards: chain_info.tracked_shards.clone(),
                    archival: self.network_state.config.archive,
                },
                self.partial_edge_info.as_ref().unwrap().clone(),
            )),
            _ => {
                error!(target: "network", "Trying to talk with peer with no supported version: {}", self.protocol_version);
                return;
            }
        };
        self.send_message_or_log(&handshake);
    }

    fn ban_peer(&mut self, ctx: &mut Context<PeerActor>, ban_reason: ReasonForBan) {
        warn!(target: "network", "Banning peer {} for {:?}", self.peer_info, ban_reason);
        self.peer_status = PeerStatus::Banned(ban_reason);
        // On stopping Banned signal will be sent to PeerManager
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

    fn receive_message(&mut self, ctx: &mut Context<PeerActor>, msg: PeerMessage) {
        if msg.is_view_client_message() {
            metrics::PEER_VIEW_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL
                .with_label_values(&[msg.msg_variant()])
                .inc();
            self.receive_view_client_message(ctx, msg);
        } else if msg.is_client_message() {
            metrics::PEER_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL
                .with_label_values(&[msg.msg_variant()])
                .inc();
            self.receive_client_message(ctx, msg);
        } else {
            debug_assert!(false, "expected (view) client message, got: {}", msg.msg_variant());
        }
    }

    fn receive_view_client_message(&self, ctx: &mut Context<PeerActor>, msg: PeerMessage) {
        let mut msg_hash = None;
        let view_client_message = match msg {
            PeerMessage::Routed(message) => {
                msg_hash = Some(message.hash());
                match message.msg.body {
                    RoutedMessageBody::TxStatusRequest(account_id, tx_hash) => {
                        NetworkViewClientMessages::TxStatus {
                            tx_hash,
                            signer_account_id: account_id,
                        }
                    }
                    RoutedMessageBody::TxStatusResponse(tx_result) => {
                        NetworkViewClientMessages::TxStatusResponse(Box::new(tx_result))
                    }
                    RoutedMessageBody::ReceiptOutcomeRequest(_receipt_id) => {
                        // Silently ignore for the time being.  We’ve been still
                        // sending those messages at protocol version 56 so we
                        // need to wait until 59 before we can remove the
                        // variant completely.
                        return;
                    }
                    RoutedMessageBody::StateRequestHeader(shard_id, sync_hash) => {
                        NetworkViewClientMessages::StateRequestHeader { shard_id, sync_hash }
                    }
                    RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id) => {
                        NetworkViewClientMessages::StateRequestPart { shard_id, sync_hash, part_id }
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

        self.network_state
            .view_client_addr
            .send(view_client_message)
            .into_actor(self)
            .then(move |res, act, _ctx| {
                // Ban peer if client thinks received data is bad.
                match res {
                    Ok(NetworkViewClientResponses::TxStatus(tx_result)) => {
                        let body = Box::new(RoutedMessageBody::TxStatusResponse(*tx_result));
                        let _ = act
                            .peer_manager_addr
                            .do_send(PeerToManagerMsg::RouteBack(body, msg_hash.unwrap()));
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
                        let _ = act.peer_manager_addr.do_send(PeerToManagerMsg::RouteBack(
                            Box::new(body),
                            msg_hash.unwrap(),
                        ));
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
            })
            .spawn(ctx);
    }

    /// Process non handshake/peer related messages.
    fn receive_client_message(&mut self, ctx: &mut Context<PeerActor>, msg: PeerMessage) {
        let _span = tracing::trace_span!(target: "network", "receive_client_message").entered();
        let peer_id =
            if let Some(peer_id) = self.other_peer_id() { peer_id.clone() } else { return };

        // Wrap peer message into what client expects.
        let network_client_msg = match msg {
            PeerMessage::Block(block) => {
                let block_hash = *block.hash();
                self.tracker.lock().push_received(block_hash);
                if let Some(cs) = &self.connection {
                    cs.chain_height.fetch_max(block.header().height(), Ordering::Relaxed);
                }
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

                match routed_message.msg.body {
                    RoutedMessageBody::BlockApproval(approval) => {
                        NetworkClientMessages::BlockApproval(approval, peer_id)
                    }
                    RoutedMessageBody::ForwardTx(transaction) => {
                        NetworkClientMessages::Transaction {
                            transaction,
                            is_forwarded: true,
                            check_only: false,
                        }
                    }

                    RoutedMessageBody::StateResponse(info) => {
                        NetworkClientMessages::StateResponse(StateResponseInfo::V1(info))
                    }
                    RoutedMessageBody::VersionedStateResponse(info) => {
                        NetworkClientMessages::StateResponse(info)
                    }
                    RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                        NetworkClientMessages::PartialEncodedChunkRequest(request, msg_hash)
                    }
                    RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                        NetworkClientMessages::PartialEncodedChunkResponse(
                            response,
                            self.clock.now().into(),
                        )
                    }
                    RoutedMessageBody::PartialEncodedChunk(partial_encoded_chunk) => {
                        NetworkClientMessages::PartialEncodedChunk(PartialEncodedChunk::V1(
                            partial_encoded_chunk,
                        ))
                    }
                    RoutedMessageBody::VersionedPartialEncodedChunk(chunk) => {
                        NetworkClientMessages::PartialEncodedChunk(chunk)
                    }
                    RoutedMessageBody::PartialEncodedChunkForward(forward) => {
                        NetworkClientMessages::PartialEncodedChunkForward(forward)
                    }
                    RoutedMessageBody::Ping(_)
                    | RoutedMessageBody::Pong(_)
                    | RoutedMessageBody::TxStatusRequest(_, _)
                    | RoutedMessageBody::TxStatusResponse(_)
                    | RoutedMessageBody::_UnusedQueryRequest
                    | RoutedMessageBody::_UnusedQueryResponse
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
        };

        self.network_state.client_addr
            .send(network_client_msg)
            .into_actor(self)
            .then(move |res, act, ctx| {
                // Ban peer if client thinks received data is bad.
                match res {
                    Ok(NetworkClientResponses::InvalidTx(err)) => {
                        warn!(target: "network", "Received invalid tx from peer {}: {}", act.peer_info, err);
                        // TODO: count as malicious behavior?
                    }
                    Ok(NetworkClientResponses::Ban { ban_reason }) => {
                        act.ban_peer(ctx, ban_reason);
                    }
                    Err(err) => {
                        error!(
                            target: "network",
                            "Received error sending message to client: {} for {}",
                            err, act.peer_info
                        );
                        return actix::fut::ready(());
                    }
                    _ => {}
                };
                actix::fut::ready(())
            })
            .spawn(ctx);
    }

    /// Hook called on every valid message received from this peer from the network.
    fn on_receive_message(&mut self) {
        if let Some(cs) = &self.connection {
            cs.last_time_received_message.store(self.clock.now());
        }
    }

    /// Update stats when receiving msg
    fn update_stats_on_receiving_message(&mut self, msg_len: usize) {
        metrics::PEER_DATA_RECEIVED_BYTES.inc_by(msg_len as u64);
        metrics::PEER_MESSAGE_RECEIVED_TOTAL.inc();
        tracing::trace!(target: "network", msg_len);
        self.tracker.lock().increment_received(&self.clock, msg_len as u64);
    }

    /// Check whenever we exceeded number of transactions we got since last block.
    /// If so, drop the transaction.
    fn should_we_drop_msg(&self, msg: &PeerMessage) -> bool {
        let m = if let PeerMessage::Routed(m) = msg {
            &m.msg
        } else {
            return false;
        };
        let _ = if let RoutedMessageBody::ForwardTx(t) = &m.body {
            t
        } else {
            return false;
        };
        let r = self.network_state.txns_since_last_block.load(Ordering::Acquire);
        r > MAX_TRANSACTIONS_PER_BLOCK_MESSAGE
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
            move |act, ctx| match &act.peer_status {
                PeerStatus::Connecting { .. } => {
                    info!(target: "network", "Handshake timeout expired for {}", act.peer_info);
                    ctx.stop();
                }
                _ => {}
            },
        );

        // If outbound peer, initiate handshake.
        if self.peer_type == PeerType::Outbound {
            self.send_handshake();
        }
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        metrics::PEER_CONNECTIONS_TOTAL.dec();
        debug!(target: "network", "{:?}: Peer {} disconnected. {:?}", self.my_node_info.id, self.peer_info, self.peer_status);
        if let Some(peer_info) = self.peer_info.as_ref() {
            if let PeerStatus::Banned(ban_reason) = self.peer_status {
                let _ = self.peer_manager_addr.do_send(PeerToManagerMsg::Ban(Ban {
                    peer_id: peer_info.id.clone(),
                    ban_reason,
                }));
            } else {
                let _ = self.peer_manager_addr.do_send(PeerToManagerMsg::Unregister(Unregister {
                    peer_id: peer_info.id.clone(),
                    peer_type: self.peer_type,
                    // If the PeerActor is no longer in the Connecting state this means
                    // that the connection was consolidated at some point in the past.
                    // Only if the connection was consolidated try to remove this peer from the
                    // peer store. This avoids a situation in which both peers are connecting to
                    // each other, and after resolving the tie, a peer tries to remove the other
                    // peer from the active connection if it was added in the parallel connection.
                    remove_from_peer_store: !matches!(
                        &self.peer_status,
                        PeerStatus::Connecting { .. }
                    ),
                }));
            }
        }
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        Arbiter::current().stop();
        self.network_state.config.event_sink.push(Event::PeerActorStopped)
    }
}

impl WriteHandler<io::Error> for PeerActor {}

impl StreamHandler<Result<Vec<u8>, ReasonForBan>> for PeerActor {
    #[perf]
    fn handle(&mut self, msg: Result<Vec<u8>, ReasonForBan>, ctx: &mut Self::Context) {
        let _span = tracing::trace_span!(target: "network", "handle", handler = "bytes").entered();
        let msg = match msg {
            Ok(msg) => msg,
            Err(ban_reason) => {
                self.ban_peer(ctx, ban_reason);
                return;
            }
        };
        // TODO(#5155) We should change our code to track size of messages received from Peer
        // as long as it travels to PeerManager, etc.

        self.update_stats_on_receiving_message(msg.len());
        let mut peer_msg = match self.parse_message(&msg) {
            Ok(msg) => msg,
            Err(err) => {
                debug!(target: "network", "Received invalid data {:?} from {}: {}", logging::pretty_vec(&msg), self.peer_info, err);
                return;
            }
        };

        if self.should_we_drop_msg(&peer_msg) {
            return;
        }

        // Drop duplicated messages routed within DROP_DUPLICATED_MESSAGES_PERIOD ms
        if let PeerMessage::Routed(msg) = &peer_msg {
            let msg = &msg.msg;
            let key = (msg.author.clone(), msg.target.clone(), msg.signature.clone());
            let now = self.clock.now();
            if let Some(&t) = self.routed_message_cache.get(&key) {
                if now <= t + DROP_DUPLICATED_MESSAGES_PERIOD {
                    debug!(target: "network", "Dropping duplicated message from {} to {:?}", msg.author, msg.target);
                    return;
                }
            }
            self.routed_message_cache.put(key, now);
        }
        if let PeerMessage::Routed(routed) = &peer_msg {
            if let RoutedMessage { body: RoutedMessageBody::ForwardTx(_), .. } = routed.as_ref().msg
            {
                self.network_state.txns_since_last_block.fetch_add(1, Ordering::AcqRel);
            }
        } else if let PeerMessage::Block(_) = &peer_msg {
            self.network_state.txns_since_last_block.store(0, Ordering::Release);
        }

        tracing::trace!(target: "network", "Received message: {}", peer_msg);

        self.on_receive_message();

        {
            let labels = [peer_msg.msg_variant()];
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_TOTAL.with_label_values(&labels).inc();
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_BYTES
                .with_label_values(&labels)
                .inc_by(msg.len() as u64);
        }

        // Optionally, ignore any received tombstones after startup. This is to
        // prevent overload from too much accumulated deleted edges.
        //
        // We have similar code to skip sending tombstones, here we handle the
        // case when our peer doesn't use that logic yet.
        if let Some(skip_tombstones) = self.network_state.config.skip_tombstones {
            if let PeerMessage::SyncRoutingTable(routing_table) = &mut peer_msg {
                if let Some(connection) = &self.connection {
                    if connection.connection_established_time + skip_tombstones > self.clock.now() {
                        routing_table.edges.retain(|edge| edge.edge_type() == EdgeState::Active);
                        metrics::EDGE_TOMBSTONE_RECEIVING_SKIPPED.inc();
                    }
                }
            }
        }

        match (&self.peer_status, peer_msg.clone()) {
            (_, PeerMessage::HandshakeFailure(peer_info, reason)) => {
                match reason {
                    HandshakeFailureReason::GenesisMismatch(genesis) => {
                        warn!(target: "network", "Attempting to connect to a node ({}) with a different genesis block. Our genesis: {:?}, their genesis: {:?}", peer_info, self.network_state.genesis_id, genesis);
                    }
                    HandshakeFailureReason::ProtocolVersionMismatch {
                        version,
                        oldest_supported_version,
                    } => {
                        let target_version = std::cmp::min(version, PROTOCOL_VERSION);

                        if target_version
                            >= std::cmp::max(
                                oldest_supported_version,
                                PEER_MIN_ALLOWED_PROTOCOL_VERSION,
                            )
                        {
                            // Use target_version as protocol_version to talk with this peer
                            self.protocol_version = target_version;
                            self.send_handshake();
                            return;
                        } else {
                            warn!(target: "network", "Unable to connect to a node ({}) due to a network protocol version mismatch. Our version: {:?}, their: {:?}", peer_info, (PROTOCOL_VERSION, PEER_MIN_ALLOWED_PROTOCOL_VERSION), (version, oldest_supported_version));
                        }
                    }
                    HandshakeFailureReason::InvalidTarget => {
                        debug!(target: "network", "Peer found was not what expected. Updating peer info with {:?}", peer_info);
                        self.peer_manager_addr.do_send(PeerToManagerMsg::UpdatePeerInfo(peer_info));
                    }
                }
                ctx.stop();
            }
            (PeerStatus::Connecting { .. }, PeerMessage::Handshake(handshake)) => {
                debug!(target: "network", "{:?}: Received handshake {:?}", self.my_node_info.id, handshake);

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
                    // Connection will be closed by a handshake timeout
                }
                let target_version = std::cmp::min(handshake.protocol_version, PROTOCOL_VERSION);
                self.protocol_version = target_version;

                let genesis_id = self.network_state.genesis_id.clone();
                if handshake.sender_chain_info.genesis_id != genesis_id {
                    debug!(target: "network", "Received connection from node with different genesis.");
                    self.send_message_or_log(&PeerMessage::HandshakeFailure(
                        self.my_node_info.clone(),
                        HandshakeFailureReason::GenesisMismatch(genesis_id),
                    ));
                    return;
                    // Connection will be closed by a handshake timeout
                }

                if handshake.sender_peer_id == self.my_node_info.id {
                    metrics::RECEIVED_INFO_ABOUT_ITSELF.inc();
                    debug!(target: "network", "Received info about itself. Disconnecting this peer.");
                    ctx.stop();
                    return;
                }

                if handshake.target_peer_id != self.my_node_info.id {
                    debug!(target: "network", "Received handshake from {:?} to {:?} but I am {:?}", handshake.sender_peer_id, handshake.target_peer_id, self.my_node_info.id);
                    self.send_message_or_log(&PeerMessage::HandshakeFailure(
                        self.my_node_info.clone(),
                        HandshakeFailureReason::InvalidTarget,
                    ));
                    return;
                    // Connection will be closed by a handshake timeout
                }

                // Verify signature of the new edge in handshake.
                if !Edge::partial_verify(
                    self.my_node_id(),
                    &handshake.sender_peer_id,
                    &handshake.partial_edge_info,
                ) {
                    warn!(target: "network", "Received invalid signature on handshake. Disconnecting peer {}", handshake.sender_peer_id);
                    self.ban_peer(ctx, ReasonForBan::InvalidSignature);
                    return;
                }

                // Check that received nonce on handshake match our proposed nonce.
                if self.peer_type == PeerType::Outbound
                    && handshake.partial_edge_info.nonce
                        != self.partial_edge_info.as_ref().map(|edge_info| edge_info.nonce).unwrap()
                {
                    warn!(target: "network", "Received invalid nonce on handshake. Disconnecting peer {}", handshake.sender_peer_id);
                    ctx.stop();
                    return;
                }

                let peer_info = PeerInfo {
                    id: handshake.sender_peer_id.clone(),
                    addr: handshake
                        .sender_listen_port
                        .map(|port| SocketAddr::new(self.peer_addr.ip(), port)),
                    account_id: None,
                };
                let connection = Arc::new(connection::Connection {
                    addr: ctx.address(),
                    peer_info: peer_info.clone(),
                    initial_chain_info: handshake.sender_chain_info.clone(),
                    chain_height: AtomicU64::new(handshake.sender_chain_info.height),
                    partial_edge_info: handshake.partial_edge_info.clone(),
                    peer_type: self.peer_type,
                    stats: AtomicCell::new(connection::Stats {
                        sent_bytes_per_sec: 0,
                        received_bytes_per_sec: 0,
                    }),
                    _peer_connections_metric: metrics::PEER_CONNECTIONS.new_point(
                        &metrics::Connection { type_: self.peer_type, encoding: self.encoding() },
                    ),
                    last_time_peer_requested: AtomicCell::new(self.clock.now()),
                    last_time_received_message: AtomicCell::new(self.clock.now()),
                    connection_established_time: self.clock.now(),
                    throttle_controller: self.throttle_controller.clone(),
                    send_accounts_data_demux: demux::Demux::new(
                        self.network_state.send_accounts_data_rl,
                    ),
                });
                self.connection = Some(connection.clone());

                let tracker = self.tracker.clone();
                let clock = self.clock.clone();
                let mut interval = tokio::time::interval(
                    self.network_state.config.peer_stats_period.try_into().unwrap(),
                );
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                ctx.spawn(
                    async move {
                        loop {
                            interval.tick().await;
                            let sent = tracker.lock().sent_bytes.minute_stats(&clock);
                            let received = tracker.lock().received_bytes.minute_stats(&clock);
                            // TODO(gprusak): this stuff requires cleanup: only chain_info.height is
                            // expected to change. Rest of the content of chain_info is not relevant
                            // after handshake.
                            connection.stats.store(connection::Stats {
                                received_bytes_per_sec: received.bytes_per_min / 60,
                                sent_bytes_per_sec: sent.bytes_per_min / 60,
                            });
                            // Whether the peer is considered abusive due to sending too many messages.
                            // I am allowing this for now because I assume `MAX_PEER_MSG_PER_MIN` will
                            // some day be less than `u64::MAX`.
                            let is_abusive = received.count_per_min > MAX_PEER_MSG_PER_MIN
                                || sent.count_per_min > MAX_PEER_MSG_PER_MIN;
                            if is_abusive {
                                tracing::trace!(
                                target: "network",
                                peer_id = ?connection.peer_info.id,
                                sent = sent.count_per_min,
                                recv = received.count_per_min,
                                "Banning peer for abuse");
                                // TODO(MarX, #1586): Ban peer if we found them abusive. Fix issue with heavy
                                //  network traffic that flags honest peers.
                                // Send ban signal to peer instance. It should send ban signal back and stop the instance.
                                // if let Some(connected_peer) = act.tier2.get(&peer_id1) {
                                //     connected_peer.addr.do_send(PeerManagerRequest::BanPeer(ReasonForBan::Abusive));
                                // }
                            }
                        }
                    }
                    .into_actor(self),
                );

                self.peer_manager_addr
                    .send(PeerToManagerMsg::RegisterPeer(RegisterPeer {
                        connection: self.connection.clone().unwrap(),
                        this_edge_info: self.partial_edge_info.clone(),
                        peer_protocol_version: self.protocol_version,
                    }))
                    .into_actor(self)
                    .then(move |res, act, ctx| {
                        match res.map(|f|f.unwrap_consolidate_response()) {
                            Ok(RegisterPeerResponse::Accept(edge_info)) => {
                                act.peer_info = Some(peer_info).into();
                                act.peer_status = PeerStatus::Ready;
                                // Respond to handshake if it's inbound and connection was consolidated.
                                if act.peer_type == PeerType::Inbound {
                                    act.partial_edge_info = edge_info;
                                    act.send_handshake();
                                } else {
                                    // Outbound peer triggers the inital full accounts data sync.
                                    // TODO(gprusak): implement triggering the periodic full sync.
                                    act.send_message_or_log(&PeerMessage::SyncAccountsData(SyncAccountsData{
                                        accounts_data: act.network_state.accounts_data.load().data.values().cloned().collect(),
                                        incremental: false,
                                        requesting_full_sync: true,
                                    }));
                                }
                                actix::fut::ready(())
                            },
                            Ok(RegisterPeerResponse::InvalidNonce(edge)) => {
                                debug!(target: "network", "{:?}: Received invalid nonce from peer {:?} sending evidence.", act.my_node_id(), act.peer_addr);
                                act.send_message_or_log(&PeerMessage::LastEdge(*edge));
                                actix::fut::ready(())
                            }
                            _ => {
                                info!(target: "network", "{:?}: Peer with handshake {:?} wasn't consolidated, disconnecting.", act.my_node_id(), handshake);
                                ctx.stop();
                                actix::fut::ready(())
                            }
                        }
                    })
                    .wait(ctx);
            }
            (PeerStatus::Connecting { .. }, PeerMessage::LastEdge(edge)) => {
                // This message will be received only if we started the connection.
                if self.peer_type == PeerType::Inbound {
                    info!(target: "network", "{:?}: Inbound peer {:?} sent invalid message. Disconnect.", self.my_node_id(), self.peer_addr);
                    ctx.stop();
                    return;
                }

                // Disconnect if neighbor propose invalid edge.
                if !edge.verify() {
                    info!(target: "network", "{:?}: Peer {:?} sent invalid edge. Disconnect.", self.my_node_id(), self.peer_addr);
                    ctx.stop();
                    return;
                }

                self.peer_manager_addr
                    .send(PeerToManagerMsg::UpdateEdge((
                        self.other_peer_id().unwrap().clone(),
                        edge.next(),
                    )))
                    .into_actor(self)
                    .then(|res, act, _ctx| {
                        if let Ok(PeerToManagerMsgResp::UpdatedEdge(edge_info)) = res {
                            act.partial_edge_info = Some(edge_info);
                            act.send_handshake();
                        }
                        actix::fut::ready(())
                    })
                    .spawn(ctx);
            }
            (PeerStatus::Ready, PeerMessage::Disconnect) => {
                debug!(target: "network", "Disconnect signal. Me: {:?} Peer: {:?}", self.my_node_info.id, self.other_peer_id());
                ctx.stop();
            }
            (PeerStatus::Ready, PeerMessage::Handshake(_)) => {
                // Received handshake after already have seen handshake from this peer.
                debug!(target: "network", "Duplicate handshake from {}", self.peer_info);
            }
            (PeerStatus::Ready, PeerMessage::PeersRequest) => {
                self.peer_manager_addr.send(PeerToManagerMsg::PeersRequest(PeersRequest {}))
                .into_actor(self).then(|res, act, _ctx| {
                    if let Ok(peers) = res.map(|f|f.unwrap_peers_request_result()) {
                        if !peers.peers.is_empty() {
                            debug!(target: "network", "Peers request from {}: sending {} peers.", act.peer_info, peers.peers.len());
                            act.send_message_or_log(&PeerMessage::PeersResponse(peers.peers));
                        }
                    }
                    actix::fut::ready(())
                }).spawn(ctx);
            }
            (PeerStatus::Ready, PeerMessage::PeersResponse(peers)) => {
                debug!(target: "network", "Received peers from {}: {} peers.", self.peer_info, peers.len());
                self.peer_manager_addr
                    .do_send(PeerToManagerMsg::PeersResponse(PeersResponse { peers }));
            }
            (PeerStatus::Ready, PeerMessage::RequestUpdateNonce(edge_info)) => self
                .peer_manager_addr
                .send(PeerToManagerMsg::RequestUpdateNonce(
                    self.other_peer_id().unwrap().clone(),
                    edge_info,
                ))
                .into_actor(self)
                .then(|res, act, ctx| {
                    match res.map(|f| f) {
                        Ok(PeerToManagerMsgResp::EdgeUpdate(edge)) => {
                            act.send_message_or_log(&PeerMessage::ResponseUpdateNonce(*edge));
                        }
                        Ok(PeerToManagerMsgResp::BanPeer(reason_for_ban)) => {
                            act.ban_peer(ctx, reason_for_ban);
                        }
                        _ => {}
                    }
                    actix::fut::ready(())
                })
                .spawn(ctx),
            (PeerStatus::Ready, PeerMessage::ResponseUpdateNonce(edge)) => self
                .peer_manager_addr
                .send(PeerToManagerMsg::ResponseUpdateNonce(edge))
                .into_actor(self)
                .then(|res, act, ctx| {
                    match res {
                        Ok(PeerToManagerMsgResp::BanPeer(reason_for_ban)) => {
                            act.ban_peer(ctx, reason_for_ban)
                        }
                        _ => {}
                    }
                    actix::fut::ready(())
                })
                .spawn(ctx),
            (PeerStatus::Ready, PeerMessage::SyncRoutingTable(routing_table_update)) => {
                self.peer_manager_addr.do_send(PeerToManagerMsg::SyncRoutingTable {
                    peer_id: self.other_peer_id().unwrap().clone(),
                    routing_table_update,
                });
            }
            (PeerStatus::Ready, PeerMessage::SyncAccountsData(msg)) => {
                let peer_id = self.other_peer_id().unwrap().clone();
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
                async move {
                    // Early exit, if there is no data in the message.
                    if msg.accounts_data.is_empty() {
                        return None;
                    }
                    // Verify and add the new data to the internal state.
                    let (new_data, err) = pms.accounts_data.clone().insert(msg.accounts_data).await;
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
                        accounts_data::Error::InvalidSignature => ReasonForBan::InvalidSignature,
                        accounts_data::Error::DataTooLarge => ReasonForBan::Abusive,
                        accounts_data::Error::SingleAccountMultipleData => ReasonForBan::Abusive,
                    })
                }
                .into_actor(self)
                .map(|ban_reason, act, ctx| {
                    if let Some(ban_reason) = ban_reason {
                        act.ban_peer(ctx, ban_reason);
                    }
                    act.network_state.config.event_sink.push(Event::MessageProcessed(peer_msg));
                })
                .spawn(ctx);
            }
            (PeerStatus::Ready, PeerMessage::Routed(routed_message)) => {
                tracing::trace!(
                    target: "network",
                    "Received routed message from {} to {:?}.",
                    self.peer_info,
                    routed_message.msg.target);

                // Receive invalid routed message from peer.
                if !routed_message.verify() {
                    self.ban_peer(ctx, ReasonForBan::InvalidSignature);
                } else {
                    self.peer_manager_addr
                        .send(PeerToManagerMsg::RoutedMessageFrom(RoutedMessageFrom {
                            msg: routed_message.clone(),
                            from: self.other_peer_id().unwrap().clone(),
                        }))
                        .into_actor(self)
                        .then(move |res, act, ctx| {
                            if res.map(|f| f.unwrap_routed_message_from()).unwrap_or(false) {
                                act.receive_message(ctx, PeerMessage::Routed(routed_message));
                            }
                            actix::fut::ready(())
                        })
                        .spawn(ctx);
                }
            }
            (PeerStatus::Ready, msg) => {
                self.receive_message(ctx, msg);
            }
            (_, msg) => {
                warn!(target: "network", "Received {} while {:?} from {:?} connection.", msg, self.peer_status, self.peer_type);
            }
        }
    }
}

impl Handler<SendMessage> for PeerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: SendMessage, _: &mut Self::Context) {
        let span =
            tracing::trace_span!(target: "network", "handle", handler = "SendMessage").entered();
        span.set_parent(msg.context);
        let _d = delay_detector::DelayDetector::new(|| "send message".into());
        self.send_message_or_log(&msg.message);
    }
}

impl Handler<PeerManagerRequestWithContext> for PeerActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: PeerManagerRequestWithContext,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let span =
            tracing::trace_span!(target: "network", "handle", handler = "PeerManagerRequest")
                .entered();
        span.set_parent(msg.context);
        let msg = msg.msg;
        let _d =
            delay_detector::DelayDetector::new(|| format!("peer manager request {:?}", msg).into());
        match msg {
            PeerManagerRequest::BanPeer(ban_reason) => {
                self.ban_peer(ctx, ban_reason);
            }
            PeerManagerRequest::UnregisterPeer => {
                ctx.stop();
            }
        }
    }
}

type InboundHandshakePermit = tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub(crate) enum ConnectingStatus {
    Inbound(InboundHandshakePermit),
    Outbound(connection::OutboundHandshakePermit),
}

/// State machine of the PeerActor.
/// The transition graph for inbound connection is:
/// Connecting(Inbound) -> Ready -> Banned
/// for outbound connection is:
/// Connecting(Outbound) -> Ready -> Banned
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
    Ready,
    /// Banned, should shutdown this peer.
    Banned(ReasonForBan),
}
