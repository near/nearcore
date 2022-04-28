use crate::peer::codec::Codec;
use crate::peer::tracker::Tracker;
use crate::peer::utils;
use crate::private_actix::{
    PeersRequest, RegisterPeer, RegisterPeerResponse, SendMessage, Unregister,
};
use crate::stats::metrics;
use crate::types::{
    Handshake, HandshakeFailureReason, NetworkClientMessages, NetworkClientResponses,
    NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerMessage, PeerRequest,
    PeerResponse, PeersResponse,
};
use crate::PeerManagerActor;
use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, Arbiter, AsyncContext, Context,
    ContextFutureSpawner, Handler, Recipient, Running, StreamHandler, WrapFuture,
};
use borsh::{BorshDeserialize, BorshSerialize};
use lru::LruCache;
use near_crypto::Signature;
use near_network_primitives::types::{
    Ban, NetworkViewClientMessages, NetworkViewClientResponses, PeerChainInfoV2, PeerIdOrHash,
    PeerInfo, PeerManagerRequest, PeerStatsResult, PeerType, QueryPeerStats, ReasonForBan,
    RoutedMessage, RoutedMessageBody, RoutedMessageFrom, StateResponseInfo,
    UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE,
};

use near_network_primitives::types::{Edge, PartialEdgeInfo};
use near_performance_metrics::framed_write::{FramedWrite, WriteHandler};
use near_performance_metrics_macros::perf;
use near_primitives::block::GenesisId;
use near_primitives::borsh::maybestd::io::Error;
use near_primitives::logging;
use near_primitives::network::PeerId;
use near_primitives::sharding::PartialEncodedChunk;
use near_primitives::time::Clock;
use near_primitives::utils::DisplayOption;
use near_primitives::version::{
    ProtocolVersion, PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use near_rate_limiter::{ActixMessageWrapper, ThrottleController};
use std::cmp::max;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};

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
const DROP_DUPLICATED_MESSAGES_PERIOD: Duration = Duration::from_millis(50);

pub(crate) struct PeerActor {
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
    /// Handshake timeout.
    handshake_timeout: Duration,
    /// Peer manager recipient to break the dependency loop.
    peer_manager_addr: Addr<PeerManagerActor>,
    /// Addr for client to send messages related to the chain.
    client_addr: Recipient<NetworkClientMessages>,
    /// Addr for view client to send messages related to the chain.
    view_client_addr: Recipient<NetworkViewClientMessages>,
    /// Tracker for requests and responses.
    tracker: Tracker,
    /// This node genesis id.
    genesis_id: GenesisId,
    /// Latest chain info from the peer.
    chain_info: PeerChainInfoV2,
    /// Edge information needed to build the real edge. This is relevant for handshake.
    partial_edge_info: Option<PartialEdgeInfo>,
    /// Last time an update of received message was sent to PeerManager
    last_time_received_message_update: Instant,
    /// How many transactions we have received since the last block message
    /// Note: Shared between multiple Peers.
    txns_since_last_block: Arc<AtomicUsize>,
    /// How many peer actors are created
    peer_counter: Arc<AtomicUsize>,
    /// Cache of recently routed messages, this allows us to drop duplicates
    routed_message_cache: LruCache<(PeerId, PeerIdOrHash, Signature), Instant>,
    /// A helper data structure for limiting reading
    throttle_controller: ThrottleController,
}

impl Debug for PeerActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.my_node_info)
    }
}

impl PeerActor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        my_node_info: PeerInfo,
        peer_addr: SocketAddr,
        peer_info: Option<PeerInfo>,
        peer_type: PeerType,
        framed: FramedWrite<Vec<u8>, WriteHalf, Codec, Codec>,
        handshake_timeout: Duration,
        peer_manager_addr: Addr<PeerManagerActor>,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
        partial_edge_info: Option<PartialEdgeInfo>,
        txns_since_last_block: Arc<AtomicUsize>,
        peer_counter: Arc<AtomicUsize>,
        throttle_controller: ThrottleController,
    ) -> Self {
        PeerActor {
            my_node_info,
            peer_addr,
            peer_info: peer_info.into(),
            peer_type,
            peer_status: PeerStatus::Connecting,
            protocol_version: PROTOCOL_VERSION,
            framed,
            handshake_timeout,
            peer_manager_addr,
            client_addr,
            view_client_addr,
            tracker: Default::default(),
            genesis_id: Default::default(),
            chain_info: Default::default(),
            partial_edge_info,
            last_time_received_message_update: Clock::instant(),
            txns_since_last_block,
            peer_counter,
            routed_message_cache: LruCache::new(ROUTED_MESSAGE_CACHE_SIZE),
            throttle_controller,
        }
    }

    fn send_message(&mut self, msg: &PeerMessage) {
        // Skip sending block and headers if we received it or header from this peer.
        // Record block requests in tracker.
        match msg {
            PeerMessage::Block(b) if self.tracker.has_received(b.hash()) => return,
            PeerMessage::BlockRequest(h) => self.tracker.push_request(*h),
            _ => (),
        };

        match msg.try_to_vec() {
            Ok(bytes) => {
                self.tracker.increment_sent(bytes.len() as u64);
                let bytes_len = bytes.len();
                if !self.framed.write(bytes) {
                    #[cfg(feature = "performance_stats")]
                    let tid = near_rust_allocator_proxy::get_tid();
                    #[cfg(not(feature = "performance_stats"))]
                    let tid = 0;
                    error!("{} Failed to send message {} of size {}", tid, msg.as_ref(), bytes_len,)
                }
            }
            Err(err) => error!(target: "network", "Error converting message to bytes: {}", err),
        };
    }

    fn fetch_client_chain_info(&self, ctx: &mut Context<PeerActor>) {
        ctx.wait(
            self.view_client_addr
                .send(NetworkViewClientMessages::GetChainInfo)
                .into_actor(self)
                .then(move |res, act, _ctx| match res {
                    Ok(NetworkViewClientResponses::ChainInfo { genesis_id, .. }) => {
                        act.genesis_id = genesis_id;
                        actix::fut::ready(())
                    }
                    Err(err) => {
                        error!(target: "network", "Failed sending GetChain to client: {}", err);
                        actix::fut::ready(())
                    }
                    _ => actix::fut::ready(()),
                }),
        );
    }

    fn send_handshake(&self, ctx: &mut Context<PeerActor>) {
        if self.other_peer_id().is_none() {
            error!(target: "network", "Sending handshake to an unknown peer");
            return;
        }

        self.view_client_addr
            .send(NetworkViewClientMessages::GetChainInfo)
            .into_actor(self)
            .then(move |res, act, _ctx| match res {
                Ok(NetworkViewClientResponses::ChainInfo {
                    genesis_id,
                    height,
                    tracked_shards,
                    archival,
                }) => {
                    let handshake = match act.protocol_version {
                        39..=PROTOCOL_VERSION => PeerMessage::Handshake(Handshake::new(
                            act.protocol_version,
                            act.my_node_id().clone(),
                            act.other_peer_id().unwrap().clone(),
                            act.my_node_info.addr_port(),
                            PeerChainInfoV2 { genesis_id, height, tracked_shards, archival },
                            act.partial_edge_info.as_ref().unwrap().clone(),
                        )),
                        _ => {
                            error!(target: "network", "Trying to talk with peer with no supported version: {}", act.protocol_version);
                            return actix::fut::ready(());
                        }
                    };

                    act.send_message(&handshake);
                    actix::fut::ready(())
                }
                Err(err) => {
                    error!(target: "network", "Failed sending GetChain to client: {}", err);
                    actix::fut::ready(())
                }
                _ => actix::fut::ready(()),
            })
            .spawn(ctx);
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
            self.receive_view_client_message(ctx, msg);
        } else if msg.is_client_message() {
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
                match message.body {
                    RoutedMessageBody::TxStatusRequest(account_id, tx_hash) => {
                        NetworkViewClientMessages::TxStatus {
                            tx_hash,
                            signer_account_id: account_id,
                        }
                    }
                    RoutedMessageBody::TxStatusResponse(tx_result) => {
                        NetworkViewClientMessages::TxStatusResponse(Box::new(tx_result))
                    }
                    RoutedMessageBody::ReceiptOutcomeRequest(receipt_id) => {
                        NetworkViewClientMessages::ReceiptOutcomeRequest(receipt_id)
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

        self.view_client_addr
            .send(view_client_message)
            .into_actor(self)
            .then(move |res, act, _ctx| {
                // Ban peer if client thinks received data is bad.
                match res {
                    Ok(NetworkViewClientResponses::TxStatus(tx_result)) => {
                        let body = Box::new(RoutedMessageBody::TxStatusResponse(*tx_result));
                        act.peer_manager_addr.do_send(PeerManagerMessageRequest::PeerRequest(
                            PeerRequest::RouteBack(body, msg_hash.unwrap()),
                        ));
                    }
                    Ok(NetworkViewClientResponses::QueryResponse { query_id, response }) => {
                        let body =
                            Box::new(RoutedMessageBody::QueryResponse { query_id, response });
                        act.peer_manager_addr.do_send(PeerManagerMessageRequest::PeerRequest(
                            PeerRequest::RouteBack(body, msg_hash.unwrap()),
                        ));
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
                        act.peer_manager_addr.do_send(PeerManagerMessageRequest::PeerRequest(
                            PeerRequest::RouteBack(Box::new(body), msg_hash.unwrap()),
                        ));
                    }
                    Ok(NetworkViewClientResponses::Block(block)) => {
                        // MOO need protocol version
                        act.send_message(&PeerMessage::Block(*block))
                    }
                    Ok(NetworkViewClientResponses::BlockHeaders(headers)) => {
                        act.send_message(&PeerMessage::BlockHeaders(headers))
                    }
                    Ok(NetworkViewClientResponses::EpochSyncResponse(response)) => {
                        act.send_message(&PeerMessage::EpochSyncResponse(response))
                    }
                    Ok(NetworkViewClientResponses::EpochSyncFinalizationResponse(response)) => {
                        act.send_message(&PeerMessage::EpochSyncFinalizationResponse(response))
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
        metrics::PEER_CLIENT_MESSAGE_RECEIVED_TOTAL.inc();
        let peer_id =
            if let Some(peer_id) = self.other_peer_id() { peer_id.clone() } else { return };

        metrics::PEER_CLIENT_MESSAGE_RECEIVED_BY_TYPE_TOTAL
            .with_label_values(&[msg.msg_variant()])
            .inc();
        // Wrap peer message into what client expects.
        let network_client_msg = match msg {
            PeerMessage::Block(block) => {
                let block_hash = *block.hash();
                self.tracker.push_received(block_hash);
                self.chain_info.height = max(self.chain_info.height, block.header().height());
                NetworkClientMessages::Block(block, peer_id, self.tracker.has_request(&block_hash))
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

                match routed_message.body {
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
                            Clock::instant(),
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
                    | RoutedMessageBody::QueryRequest { .. }
                    | RoutedMessageBody::QueryResponse { .. }
                    | RoutedMessageBody::ReceiptOutcomeRequest(_)
                    | RoutedMessageBody::StateRequestHeader(_, _)
                    | RoutedMessageBody::StateRequestPart(_, _, _)
                    | RoutedMessageBody::Unused => {
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
            | PeerMessage::_HandshakeV2
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
            | PeerMessage::EpochSyncFinalizationRequest(_) => {
                error!(target: "network", "Peer receive_client_message received unexpected type: {:?}", msg);
                return;
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            PeerMessage::RoutingTableSyncV2(_) => {
                error!(target: "network", "Peer receive_client_message received unexpected type: {:?}", msg);
                return;
            }
        };

        self.client_addr
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
        if let Some(peer_id) = self.other_peer_id().cloned() {
            if self.last_time_received_message_update.elapsed()
                > UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE
            {
                self.last_time_received_message_update = Clock::instant();
                self.peer_manager_addr.do_send(PeerManagerMessageRequest::PeerRequest(
                    PeerRequest::ReceivedMessage(peer_id, self.last_time_received_message_update),
                ));
            }
        }
    }

    /// Update stats when receiving msg
    fn update_stats_on_receiving_message(&mut self, msg_len: usize) {
        metrics::PEER_DATA_RECEIVED_BYTES.inc_by(msg_len as u64);
        metrics::PEER_MESSAGE_RECEIVED_TOTAL.inc();
        self.tracker.increment_received(msg_len as u64);
    }

    /// Check whenever we exceeded number of transactions we got since last block.
    /// If so, drop the transaction.
    fn should_we_drop_msg_without_decoding(&self, msg: &[u8]) -> bool {
        if utils::is_forward_transaction(msg).unwrap_or(false) {
            let r = self.txns_since_last_block.load(Ordering::Acquire);
            if r > MAX_TRANSACTIONS_PER_BLOCK_MESSAGE {
                return true;
            }
        }
        false
    }

    // Checks errors from decoding a message.
    // We may send `HandshakeFailure` to the other peer.
    fn handle_peer_message_decode_error(&mut self, msg: &[u8], err: Error) {
        if let Some(version) = err
            .get_ref()
            .and_then(|err| err.downcast_ref::<HandshakeFailureReason>())
            .and_then(|inner| {
                if let HandshakeFailureReason::ProtocolVersionMismatch { version, .. } = *inner {
                    Some(version)
                } else {
                    None
                }
            })
        {
            debug!(target: "network", "Received connection from node with unsupported version: {}", version);
            self.send_message(&PeerMessage::HandshakeFailure(
                self.my_node_info.clone(),
                HandshakeFailureReason::ProtocolVersionMismatch {
                    version: PROTOCOL_VERSION,
                    oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
                },
            ));
        } else {
            info!(target: "network", "Received invalid data {:?} from {}: {}", logging::pretty_vec(msg), self.peer_info, err);
        }
    }
}

impl Actor for PeerActor {
    type Context = Context<PeerActor>;

    fn started(&mut self, ctx: &mut Self::Context) {
        metrics::PEER_CONNECTIONS_TOTAL.inc();
        // Fetch genesis hash from the client.
        self.fetch_client_chain_info(ctx);

        debug!(target: "network", "{:?}: Peer {:?} {:?} started", self.my_node_info.id, self.peer_addr, self.peer_type);
        // Set Handshake timeout for stopping actor if peer is not ready after given period of time.

        near_performance_metrics::actix::run_later(ctx, self.handshake_timeout, move |act, ctx| {
            if act.peer_status != PeerStatus::Ready {
                info!(target: "network", "Handshake timeout expired for {}", act.peer_info);
                ctx.stop();
            }
        });

        // If outbound peer, initiate handshake.
        if self.peer_type == PeerType::Outbound {
            self.send_handshake(ctx);
        }
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        self.peer_counter.fetch_sub(1, Ordering::SeqCst);
        metrics::PEER_CONNECTIONS_TOTAL.dec();
        debug!(target: "network", "{:?}: Peer {} disconnected. {:?}", self.my_node_info.id, self.peer_info, self.peer_status);
        if let Some(peer_info) = self.peer_info.as_ref() {
            if let PeerStatus::Banned(ban_reason) = self.peer_status {
                self.peer_manager_addr.do_send(PeerManagerMessageRequest::Ban(Ban {
                    peer_id: peer_info.id.clone(),
                    ban_reason,
                }));
            } else {
                self.peer_manager_addr.do_send(PeerManagerMessageRequest::Unregister(Unregister {
                    peer_id: peer_info.id.clone(),
                    peer_type: self.peer_type,
                    // If the PeerActor is no longer in the Connecting state this means
                    // that the connection was consolidated at some point in the past.
                    // Only if the connection was consolidated try to remove this peer from the
                    // peer store. This avoids a situation in which both peers are connecting to
                    // each other, and after resolving the tie, a peer tries to remove the other
                    // peer from the active connection if it was added in the parallel connection.
                    remove_from_peer_store: self.peer_status != PeerStatus::Connecting,
                }))
            }
        }
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        Arbiter::current().stop();
    }
}

impl WriteHandler<io::Error> for PeerActor {}

impl StreamHandler<Result<Vec<u8>, ReasonForBan>> for PeerActor {
    #[perf]
    fn handle(&mut self, msg: Result<Vec<u8>, ReasonForBan>, ctx: &mut Self::Context) {
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

        if self.should_we_drop_msg_without_decoding(&msg) {
            return;
        }
        let peer_msg = match PeerMessage::try_from_slice(&msg) {
            Ok(peer_msg) => peer_msg,
            Err(err) => {
                // This may send `HandshakeFailure` to the other peer.
                self.handle_peer_message_decode_error(&msg, err);
                return;
            }
        };

        // Drop duplicated messages routed within DROP_DUPLICATED_MESSAGES_PERIOD ms
        if let PeerMessage::Routed(msg) = &peer_msg {
            let key = (msg.author.clone(), msg.target.clone(), msg.signature.clone());
            let now = Clock::instant();
            if let Some(time) = self.routed_message_cache.get(&key) {
                if now.saturating_duration_since(*time) <= DROP_DUPLICATED_MESSAGES_PERIOD {
                    debug!(target: "network", "Dropping duplicated message from {} to {:?}", msg.author, msg.target);
                    return;
                }
            }
            self.routed_message_cache.put(key, now);
        }
        if let PeerMessage::Routed(routed) = &peer_msg {
            if let RoutedMessage { body: RoutedMessageBody::ForwardTx(_), .. } = routed.as_ref() {
                self.txns_since_last_block.fetch_add(1, Ordering::AcqRel);
            }
        } else if let PeerMessage::Block(_) = &peer_msg {
            self.txns_since_last_block.store(0, Ordering::Release);
        }

        trace!(target: "network", "Received message: {}", peer_msg);

        self.on_receive_message();

        {
            let labels = [peer_msg.msg_variant()];
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_TOTAL.with_label_values(&labels).inc();
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_BYTES
                .with_label_values(&labels)
                .inc_by(msg.len() as u64);
        }

        match (self.peer_status, peer_msg) {
            (_, PeerMessage::HandshakeFailure(peer_info, reason)) => {
                match reason {
                    HandshakeFailureReason::GenesisMismatch(genesis) => {
                        warn!(target: "network", "Attempting to connect to a node ({}) with a different genesis block. Our genesis: {:?}, their genesis: {:?}", peer_info, self.genesis_id, genesis);
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
                            self.send_handshake(ctx);
                            return;
                        } else {
                            warn!(target: "network", "Unable to connect to a node ({}) due to a network protocol version mismatch. Our version: {:?}, their: {:?}", peer_info, (PROTOCOL_VERSION, PEER_MIN_ALLOWED_PROTOCOL_VERSION), (version, oldest_supported_version));
                        }
                    }
                    HandshakeFailureReason::InvalidTarget => {
                        debug!(target: "network", "Peer found was not what expected. Updating peer info with {:?}", peer_info);
                        self.peer_manager_addr.do_send(ActixMessageWrapper::new_without_size(
                            PeerManagerMessageRequest::PeerRequest(PeerRequest::UpdatePeerInfo(
                                peer_info,
                            )),
                            Some(self.throttle_controller.clone()),
                        ));
                    }
                }
                ctx.stop();
            }
            (PeerStatus::Connecting, PeerMessage::Handshake(handshake)) => {
                debug!(target: "network", "{:?}: Received handshake {:?}", self.my_node_info.id, handshake);

                debug_assert!(
                    PEER_MIN_ALLOWED_PROTOCOL_VERSION <= handshake.protocol_version
                        && handshake.protocol_version <= PROTOCOL_VERSION
                );

                let target_version = std::cmp::min(handshake.protocol_version, PROTOCOL_VERSION);
                self.protocol_version = target_version;

                if handshake.sender_chain_info.genesis_id != self.genesis_id {
                    debug!(target: "network", "Received connection from node with different genesis.");
                    ctx.address().do_send(SendMessage {
                        message: PeerMessage::HandshakeFailure(
                            self.my_node_info.clone(),
                            HandshakeFailureReason::GenesisMismatch(self.genesis_id.clone()),
                        ),
                    });
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
                    self.send_message(&PeerMessage::HandshakeFailure(
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
                self.chain_info = handshake.sender_chain_info.clone();
                self.peer_manager_addr
                    .send(ActixMessageWrapper::new_without_size(PeerManagerMessageRequest::RegisterPeer(RegisterPeer {
                        actor: ctx.address(),
                        peer_info: peer_info.clone(),
                        peer_type: self.peer_type,
                        chain_info: handshake.sender_chain_info.clone(),
                        this_edge_info: self.partial_edge_info.clone(),
                        other_edge_info: handshake.partial_edge_info.clone(),
                        peer_protocol_version: self.protocol_version,
                        throttle_controller: self.throttle_controller.clone(),
                    }), Some(self.throttle_controller.clone())))
                    .into_actor(self)
                    .then(move |res, act, ctx| {
                        match res.map(|f|f.into_inner().as_consolidate_response()) {
                            Ok(RegisterPeerResponse::Accept(edge_info)) => {
                                act.peer_info = Some(peer_info).into();
                                act.peer_status = PeerStatus::Ready;
                                // Respond to handshake if it's inbound and connection was consolidated.
                                if act.peer_type == PeerType::Inbound {
                                    act.partial_edge_info = edge_info;
                                    act.send_handshake(ctx);
                                }
                                actix::fut::ready(())
                            },
                            Ok(RegisterPeerResponse::InvalidNonce(edge)) => {
                                debug!(target: "network", "{:?}: Received invalid nonce from peer {:?} sending evidence.", act.my_node_id(), act.peer_addr);
                                act.send_message(&PeerMessage::LastEdge(*edge));
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
            (PeerStatus::Connecting, PeerMessage::LastEdge(edge)) => {
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
                    .send(ActixMessageWrapper::new_without_size(
                        PeerManagerMessageRequest::PeerRequest(PeerRequest::UpdateEdge((
                            self.other_peer_id().unwrap().clone(),
                            edge.next(),
                        ))),
                        Some(self.throttle_controller.clone()),
                    ))
                    .into_actor(self)
                    .then(|res, act, ctx| {
                        if let Ok(PeerResponse::UpdatedEdge(edge_info)) =
                            res.map(|f| f.into_inner().as_peer_response())
                        {
                            act.partial_edge_info = Some(edge_info);
                            act.send_handshake(ctx);
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
                self.peer_manager_addr.send(ActixMessageWrapper::new_without_size(PeerManagerMessageRequest::PeersRequest(PeersRequest {}),
                                                                     Some(self.throttle_controller.clone()),

                )).into_actor(self).then(|res, act, _ctx| {
                    if let Ok(peers) = res.map(|f|f.into_inner().as_peers_request_result()) {
                        if !peers.peers.is_empty() {
                            debug!(target: "network", "Peers request from {}: sending {} peers.", act.peer_info, peers.peers.len());
                            act.send_message(&PeerMessage::PeersResponse(peers.peers));
                        }
                    }
                    actix::fut::ready(())
                }).spawn(ctx);
            }
            (PeerStatus::Ready, PeerMessage::PeersResponse(peers)) => {
                debug!(target: "network", "Received peers from {}: {} peers.", self.peer_info, peers.len());
                self.peer_manager_addr.do_send(ActixMessageWrapper::new_without_size(
                    PeerManagerMessageRequest::PeersResponse(PeersResponse { peers }),
                    Some(self.throttle_controller.clone()),
                ));
            }
            (PeerStatus::Ready, PeerMessage::RequestUpdateNonce(edge_info)) => self
                .peer_manager_addr
                .send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::RequestUpdateNonce(
                        self.other_peer_id().unwrap().clone(),
                        edge_info,
                    ),
                ))
                .into_actor(self)
                .then(|res, act, ctx| {
                    match res.map(|f| f.as_network_response()) {
                        Ok(NetworkResponses::EdgeUpdate(edge)) => {
                            act.send_message(&PeerMessage::ResponseUpdateNonce(*edge));
                        }
                        Ok(NetworkResponses::BanPeer(reason_for_ban)) => {
                            act.ban_peer(ctx, reason_for_ban);
                        }
                        _ => {}
                    }
                    actix::fut::ready(())
                })
                .spawn(ctx),
            (PeerStatus::Ready, PeerMessage::ResponseUpdateNonce(edge)) => self
                .peer_manager_addr
                .send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::ResponseUpdateNonce(edge),
                ))
                .into_actor(self)
                .then(|res, act, ctx| {
                    if let Ok(NetworkResponses::BanPeer(reason_for_ban)) =
                        res.map(|f| f.as_network_response())
                    {
                        act.ban_peer(ctx, reason_for_ban);
                    }
                    actix::fut::ready(())
                })
                .spawn(ctx),
            (PeerStatus::Ready, PeerMessage::SyncRoutingTable(routing_table_update)) => {
                self.peer_manager_addr.do_send(ActixMessageWrapper::new_without_size(
                    PeerManagerMessageRequest::NetworkRequests(NetworkRequests::SyncRoutingTable {
                        peer_id: self.other_peer_id().unwrap().clone(),
                        routing_table_update,
                    }),
                    Some(self.throttle_controller.clone()),
                ));
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            (PeerStatus::Ready, PeerMessage::RoutingTableSyncV2(ibf_message)) => {
                // TODO(#5155) Add wrapper to be something like this for all messages.
                // self.peer_manager_addr.do_send(ActixMessageWrapper<NetworkRequests>::new(
                //        self.rate_limiter.clone, NetworkRequests::IbfMessage {
                //         ...

                self.peer_manager_addr.do_send(ActixMessageWrapper::new_without_size(
                    PeerManagerMessageRequest::NetworkRequests(NetworkRequests::IbfMessage {
                        peer_id: self.other_peer_id().unwrap().clone(),
                        ibf_msg: ibf_message,
                    }),
                    Some(self.throttle_controller.clone()),
                ));
            }
            (PeerStatus::Ready, PeerMessage::Routed(routed_message)) => {
                trace!(target: "network", "Received routed message from {} to {:?}.", self.peer_info, routed_message.target);

                // Receive invalid routed message from peer.
                if !routed_message.verify() {
                    self.ban_peer(ctx, ReasonForBan::InvalidSignature);
                } else {
                    self.peer_manager_addr
                        .send(ActixMessageWrapper::new_without_size(
                            PeerManagerMessageRequest::RoutedMessageFrom(RoutedMessageFrom {
                                msg: routed_message.clone(),
                                from: self.other_peer_id().unwrap().clone(),
                            }),
                            Some(self.throttle_controller.clone()),
                        ))
                        .into_actor(self)
                        .then(move |res, act, ctx| {
                            if res.map(|f| f.into_inner().as_routed_message_from()).unwrap_or(false)
                            {
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
        let _d = delay_detector::DelayDetector::new(|| "send message".into());
        self.send_message(&msg.message);
    }
}

impl Handler<Arc<SendMessage>> for PeerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: Arc<SendMessage>, _: &mut Self::Context) {
        let _d = delay_detector::DelayDetector::new(|| "send message".into());
        self.send_message(&msg.as_ref().message);
    }
}

impl Handler<QueryPeerStats> for PeerActor {
    type Result = PeerStatsResult;

    #[perf]
    fn handle(&mut self, _msg: QueryPeerStats, _: &mut Self::Context) -> Self::Result {
        let _d = delay_detector::DelayDetector::new(|| "query peer stats".into());

        // TODO(#5218) Refactor this code to use `SystemTime`
        let now = Instant::now();
        let sent = self.tracker.sent_bytes.minute_stats(now);
        let received = self.tracker.received_bytes.minute_stats(now);

        // Whether the peer is considered abusive due to sending too many messages.
        // I am allowing this for now because I assume `MAX_PEER_MSG_PER_MIN` will
        // some day be less than `u64::MAX`.
        let is_abusive = received.count_per_min > MAX_PEER_MSG_PER_MIN
            || sent.count_per_min > MAX_PEER_MSG_PER_MIN;

        PeerStatsResult {
            chain_info: self.chain_info.clone(),
            received_bytes_per_sec: received.bytes_per_min / 60,
            sent_bytes_per_sec: sent.bytes_per_min / 60,
            is_abusive,
            message_counts: (sent.count_per_min, received.count_per_min),
        }
    }
}

impl Handler<PeerManagerRequest> for PeerActor {
    type Result = ();

    #[perf]
    fn handle(&mut self, msg: PeerManagerRequest, ctx: &mut Self::Context) -> Self::Result {
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

/// Peer status.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PeerStatus {
    /// Waiting for handshake.
    Connecting,
    /// Ready to go.
    Ready,
    /// Banned, should shutdown this peer.
    Banned(ReasonForBan),
}
