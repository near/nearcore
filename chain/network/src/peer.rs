use std::cmp::max;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use actix::io::{FramedWrite, WriteHandler};
use actix::{
    Actor, ActorContext, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner,
    Handler, Recipient, Running, StreamHandler, WrapFuture,
};
use tracing::{debug, error, info, trace, warn};

use near_metrics;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::unwrap_option_or_return;
use near_primitives::utils::DisplayOption;
use near_primitives::version::{OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION, PROTOCOL_VERSION};

use crate::codec::{bytes_to_peer_message, peer_message_to_bytes, Codec};
use crate::rate_counter::RateCounter;
#[cfg(feature = "metric_recorder")]
use crate::recorder::{PeerMessageMetadata, Status};
use crate::routing::{Edge, EdgeInfo};
use crate::types::{
    Ban, Consolidate, ConsolidateResponse, HandshakeFailureReason, HandshakeV2,
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkViewClientMessages,
    NetworkViewClientResponses, PeerChainInfo, PeerInfo, PeerManagerRequest, PeerMessage,
    PeerRequest, PeerResponse, PeerStatsResult, PeerStatus, PeerType, PeersRequest, PeersResponse,
    QueryPeerStats, ReasonForBan, RoutedMessageBody, RoutedMessageFrom, SendMessage, Unregister,
    UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE,
};
use crate::PeerManagerActor;
use crate::{metrics, NetworkResponses};
#[cfg(feature = "delay_detector")]
use delay_detector::DelayDetector;
use metrics::NetworkMetrics;

type WriteHalf = tokio::io::WriteHalf<tokio::net::TcpStream>;

/// Maximum number of requests and responses to track.
const MAX_TRACK_SIZE: usize = 30;
/// Maximum number of messages per minute from single peer.
// TODO: current limit is way to high due to us sending lots of messages during sync.
const MAX_PEER_MSG_PER_MIN: u64 = std::u64::MAX;

/// Internal structure to keep a circular queue within a tracker with unique hashes.
struct CircularUniqueQueue {
    v: Vec<CryptoHash>,
    index: usize,
    limit: usize,
}

impl CircularUniqueQueue {
    pub fn new(limit: usize) -> Self {
        assert!(limit > 0);
        Self { v: Vec::with_capacity(limit), index: 0, limit }
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.v.contains(hash)
    }

    /// Pushes an element if it's not in the queue already. The queue will pop the oldest element.
    pub fn push(&mut self, hash: CryptoHash) {
        if !self.contains(&hash) {
            if self.v.len() < self.limit {
                self.v.push(hash);
            } else {
                self.v[self.index] = hash;
                self.index += 1;
                if self.index == self.limit {
                    self.index = 0;
                }
            }
        }
    }
}

/// Keeps track of requests and received hashes of transactions and blocks.
/// Also keeps track of number of bytes sent and received from this peer to prevent abuse.
pub struct Tracker {
    /// Bytes we've sent.
    sent_bytes: RateCounter,
    /// Bytes we've received.
    received_bytes: RateCounter,
    /// Sent requests.
    requested: CircularUniqueQueue,
    /// Received elements.
    received: CircularUniqueQueue,
}

impl Default for Tracker {
    fn default() -> Self {
        Tracker {
            sent_bytes: RateCounter::new(),
            received_bytes: RateCounter::new(),
            requested: CircularUniqueQueue::new(MAX_TRACK_SIZE),
            received: CircularUniqueQueue::new(MAX_TRACK_SIZE),
        }
    }
}

impl Tracker {
    fn increment_received(&mut self, size: u64) {
        self.received_bytes.increment(size);
    }

    fn increment_sent(&mut self, size: u64) {
        self.sent_bytes.increment(size);
    }

    fn has_received(&self, hash: &CryptoHash) -> bool {
        self.received.contains(hash)
    }

    fn push_received(&mut self, hash: CryptoHash) {
        self.received.push(hash);
    }

    fn has_request(&self, hash: &CryptoHash) -> bool {
        self.requested.contains(hash)
    }

    fn push_request(&mut self, hash: CryptoHash) {
        self.requested.push(hash);
    }
}

pub struct Peer {
    /// This node's id and address (either listening or socket address).
    pub node_info: PeerInfo,
    /// Peer address from connection.
    pub peer_addr: SocketAddr,
    /// Peer id and info. Present if outbound or ready.
    pub peer_info: DisplayOption<PeerInfo>,
    /// Peer type.
    pub peer_type: PeerType,
    /// Peer status.
    pub peer_status: PeerStatus,
    /// Framed wrapper to send messages through the TCP connection.
    framed: FramedWrite<WriteHalf, Codec>,
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
    chain_info: PeerChainInfo,
    /// Edge information needed to build the real edge. This is relevant for handshake.
    edge_info: Option<EdgeInfo>,
    /// Last time an update of received message was sent to PeerManager
    last_time_received_message_update: Instant,
    /// Dynamic Prometheus metrics
    network_metrics: NetworkMetrics,
}

impl Peer {
    pub fn new(
        node_info: PeerInfo,
        peer_addr: SocketAddr,
        peer_info: Option<PeerInfo>,
        peer_type: PeerType,
        framed: FramedWrite<WriteHalf, Codec>,
        handshake_timeout: Duration,
        peer_manager_addr: Addr<PeerManagerActor>,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
        edge_info: Option<EdgeInfo>,
        network_metrics: NetworkMetrics,
    ) -> Self {
        Peer {
            node_info,
            peer_addr,
            peer_info: peer_info.into(),
            peer_type,
            peer_status: PeerStatus::Connecting,
            framed,
            handshake_timeout,
            peer_manager_addr,
            client_addr,
            view_client_addr,
            tracker: Default::default(),
            genesis_id: Default::default(),
            chain_info: Default::default(),
            edge_info,
            last_time_received_message_update: Instant::now(),
            network_metrics,
        }
    }

    /// Whether the peer is considered abusive due to sending too many messages.
    // I am allowing this for now because I assume `MAX_PEER_MSG_PER_MIN` will
    // some day be less than `u64::MAX`.
    #[allow(clippy::absurd_extreme_comparisons)]
    fn is_abusive(&self) -> bool {
        self.tracker.received_bytes.count_per_min() > MAX_PEER_MSG_PER_MIN
            || self.tracker.sent_bytes.count_per_min() > MAX_PEER_MSG_PER_MIN
    }

    fn send_message(&mut self, msg: PeerMessage) {
        // Skip sending block and headers if we received it or header from this peer.
        // Record block requests in tracker.
        match &msg {
            PeerMessage::Block(b) if self.tracker.has_received(b.hash()) => return,
            PeerMessage::BlockRequest(h) => self.tracker.push_request(*h),
            _ => (),
        };
        #[cfg(feature = "metric_recorder")]
        let metadata = {
            let mut metadata: PeerMessageMetadata = (&msg).into();
            metadata = metadata.set_source(self.node_id()).set_status(Status::Sent);
            if let Some(target) = self.peer_id() {
                metadata = metadata.set_target(target);
            }
            metadata
        };

        match peer_message_to_bytes(msg) {
            Ok(bytes) => {
                #[cfg(feature = "metric_recorder")]
                self.peer_manager_addr.do_send(metadata.set_size(bytes.len()));
                self.tracker.increment_sent(bytes.len() as u64);
                self.framed.write(bytes);
            }
            Err(err) => error!(target: "network", "Error converting message to bytes: {}", err),
        };
    }

    fn fetch_client_chain_info(&mut self, ctx: &mut Context<Peer>) {
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

    fn send_handshake(&mut self, ctx: &mut Context<Peer>) {
        if self.peer_id().is_none() {
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
                }) => {
                    let handshake = HandshakeV2::new(
                        act.node_id(),
                        act.peer_id().unwrap(),
                        act.node_info.addr_port(),
                        PeerChainInfo { genesis_id, height, tracked_shards },
                        act.edge_info.as_ref().unwrap().clone(),
                    );
                    act.send_message(PeerMessage::HandshakeV2(handshake));
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

    fn ban_peer(&mut self, ctx: &mut Context<Peer>, ban_reason: ReasonForBan) {
        warn!(target: "network", "Banning peer {} for {:?}", self.peer_info, ban_reason);
        self.peer_status = PeerStatus::Banned(ban_reason);
        // On stopping Banned signal will be sent to PeerManager
        ctx.stop();
    }

    fn node_id(&self) -> PeerId {
        self.node_info.id.clone()
    }

    fn peer_id(&self) -> Option<PeerId> {
        self.peer_info.as_ref().as_ref().map(|peer_info| peer_info.id.clone())
    }

    fn receive_message(&mut self, ctx: &mut Context<Peer>, msg: PeerMessage) {
        if msg.is_view_client_message() {
            self.receive_view_client_message(ctx, msg);
        } else if msg.is_client_message() {
            self.receive_client_message(ctx, msg);
        } else {
            debug_assert!(false);
        }
    }

    fn receive_view_client_message(&mut self, ctx: &mut Context<Peer>, msg: PeerMessage) {
        let mut msg_hash = None;
        let view_client_message = match msg {
            PeerMessage::Routed(message) => {
                msg_hash = Some(message.hash());
                match message.body {
                    RoutedMessageBody::QueryRequest { query_id, block_reference, request } => {
                        NetworkViewClientMessages::Query { query_id, block_reference, request }
                    }
                    RoutedMessageBody::QueryResponse { query_id, response } => {
                        NetworkViewClientMessages::QueryResponse { query_id, response }
                    }
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
                    RoutedMessageBody::ReceiptOutComeResponse(response) => {
                        NetworkViewClientMessages::ReceiptOutcomeResponse(Box::new(response))
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
                        act.peer_manager_addr
                            .do_send(PeerRequest::RouteBack(body, msg_hash.unwrap()));
                    }
                    Ok(NetworkViewClientResponses::QueryResponse { query_id, response }) => {
                        let body =
                            Box::new(RoutedMessageBody::QueryResponse { query_id, response });
                        act.peer_manager_addr
                            .do_send(PeerRequest::RouteBack(body, msg_hash.unwrap()));
                    }
                    Ok(NetworkViewClientResponses::ReceiptOutcomeResponse(response)) => {
                        let body = Box::new(RoutedMessageBody::ReceiptOutComeResponse(*response));
                        act.peer_manager_addr
                            .do_send(PeerRequest::RouteBack(body, msg_hash.unwrap()));
                    }
                    Ok(NetworkViewClientResponses::StateResponse(state_response)) => {
                        let body = Box::new(RoutedMessageBody::StateResponse(*state_response));
                        act.peer_manager_addr
                            .do_send(PeerRequest::RouteBack(body, msg_hash.unwrap()));
                    }
                    Ok(NetworkViewClientResponses::Block(block)) => {
                        // MOO need protocol version
                        act.send_message(PeerMessage::Block(*block))
                    }
                    Ok(NetworkViewClientResponses::BlockHeaders(headers)) => {
                        act.send_message(PeerMessage::BlockHeaders(headers))
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
    fn receive_client_message(&mut self, ctx: &mut Context<Peer>, msg: PeerMessage) {
        near_metrics::inc_counter(&metrics::PEER_CLIENT_MESSAGE_RECEIVED_TOTAL);
        let peer_id = unwrap_option_or_return!(self.peer_id());

        // Wrap peer message into what client expects.
        let network_client_msg = match msg {
            PeerMessage::Block(block) => {
                near_metrics::inc_counter(&metrics::PEER_BLOCK_RECEIVED_TOTAL);
                let block_hash = *block.hash();
                self.tracker.push_received(block_hash);
                self.chain_info.height = max(self.chain_info.height, block.header().height());
                NetworkClientMessages::Block(block, peer_id, self.tracker.has_request(&block_hash))
            }
            PeerMessage::Transaction(transaction) => {
                near_metrics::inc_counter(&metrics::PEER_TRANSACTION_RECEIVED_TOTAL);
                NetworkClientMessages::Transaction {
                    transaction,
                    is_forwarded: false,
                    check_only: false,
                }
            }
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
                        NetworkClientMessages::StateResponse(info)
                    }
                    RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                        NetworkClientMessages::PartialEncodedChunkRequest(request, msg_hash)
                    }
                    RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                        NetworkClientMessages::PartialEncodedChunkResponse(response)
                    }
                    RoutedMessageBody::PartialEncodedChunk(partial_encoded_chunk) => {
                        NetworkClientMessages::PartialEncodedChunk(partial_encoded_chunk)
                    }
                    RoutedMessageBody::Ping(_)
                    | RoutedMessageBody::Pong(_)
                    | RoutedMessageBody::TxStatusRequest(_, _)
                    | RoutedMessageBody::TxStatusResponse(_)
                    | RoutedMessageBody::QueryRequest { .. }
                    | RoutedMessageBody::QueryResponse { .. }
                    | RoutedMessageBody::ReceiptOutcomeRequest(_)
                    | RoutedMessageBody::ReceiptOutComeResponse(_)
                    | RoutedMessageBody::StateRequestHeader(_, _)
                    | RoutedMessageBody::StateRequestPart(_, _, _) => {
                        error!(target: "network", "Peer receive_client_message received unexpected type: {:?}", routed_message);
                        return;
                    }
                }
            }
            PeerMessage::Challenge(challenge) => NetworkClientMessages::Challenge(challenge),
            PeerMessage::Handshake(_)
            | PeerMessage::HandshakeV2(_)
            | PeerMessage::HandshakeFailure(_, _)
            | PeerMessage::PeersRequest
            | PeerMessage::PeersResponse(_)
            | PeerMessage::RoutingTableSync(_)
            | PeerMessage::LastEdge(_)
            | PeerMessage::Disconnect
            | PeerMessage::RequestUpdateNonce(_)
            | PeerMessage::ResponseUpdateNonce(_)
            | PeerMessage::BlockRequest(_)
            | PeerMessage::BlockHeadersRequest(_) => {
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
        if let Some(peer_id) = self.peer_id() {
            if self.last_time_received_message_update.elapsed()
                > UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE
            {
                self.last_time_received_message_update = Instant::now();
                self.peer_manager_addr.do_send(PeerRequest::ReceivedMessage(
                    peer_id,
                    self.last_time_received_message_update,
                ));
            }
        }
    }
}

impl Actor for Peer {
    type Context = Context<Peer>;

    fn started(&mut self, ctx: &mut Self::Context) {
        near_metrics::inc_gauge(&metrics::PEER_CONNECTIONS_TOTAL);
        // Fetch genesis hash from the client.
        self.fetch_client_chain_info(ctx);

        debug!(target: "network", "{:?}: Peer {:?} {:?} started", self.node_info.id, self.peer_addr, self.peer_type);
        // Set Handshake timeout for stopping actor if peer is not ready after given period of time.
        ctx.run_later(self.handshake_timeout, move |act, ctx| {
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
        near_metrics::dec_gauge(&metrics::PEER_CONNECTIONS_TOTAL);
        debug!(target: "network", "{:?}: Peer {} disconnected.", self.node_info.id, self.peer_info);
        if let Some(peer_info) = self.peer_info.as_ref() {
            if let PeerStatus::Banned(ban_reason) = self.peer_status {
                self.peer_manager_addr.do_send(Ban { peer_id: peer_info.id.clone(), ban_reason });
            } else {
                self.peer_manager_addr.do_send(Unregister {
                    peer_id: peer_info.id.clone(),
                    peer_type: self.peer_type,
                })
            }
        }
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        Arbiter::current().stop();
    }
}

impl WriteHandler<io::Error> for Peer {}

impl StreamHandler<Result<Vec<u8>, ReasonForBan>> for Peer {
    fn handle(&mut self, msg: Result<Vec<u8>, ReasonForBan>, ctx: &mut Self::Context) {
        let msg = match msg {
            Ok(msg) => msg,
            Err(ban_reason) => {
                self.ban_peer(ctx, ban_reason);
                return ();
            }
        };

        near_metrics::inc_counter_by(&metrics::PEER_DATA_RECEIVED_BYTES, msg.len() as i64);
        near_metrics::inc_counter(&metrics::PEER_MESSAGE_RECEIVED_TOTAL);

        #[cfg(feature = "metric_recorder")]
        let msg_size = msg.len();

        self.tracker.increment_received(msg.len() as u64);
        let mut peer_msg = match bytes_to_peer_message(&msg) {
            Ok(peer_msg) => peer_msg,
            Err(err) => {
                info!(target: "network", "Received invalid data {:?} from {}: {}", msg, self.peer_info, err);
                return;
            }
        };

        trace!(target: "network", "Received message: {}", peer_msg);

        self.on_receive_message();

        #[cfg(feature = "metric_recorder")]
        {
            let mut metadata: PeerMessageMetadata = (&peer_msg).into();
            metadata =
                metadata.set_size(msg_size).set_target(self.node_id()).set_status(Status::Received);

            if let Some(peer_id) = self.peer_id() {
                metadata = metadata.set_source(peer_id);
            }

            self.peer_manager_addr.do_send(metadata);
        }

        self.network_metrics
            .inc(NetworkMetrics::peer_message_total_rx(&peer_msg.msg_variant()).as_ref());

        self.network_metrics.inc_by(
            NetworkMetrics::peer_message_bytes_rx(&peer_msg.msg_variant()).as_ref(),
            msg.len() as i64,
        );

        if let PeerMessage::Handshake(handshake) = peer_msg {
            peer_msg = PeerMessage::HandshakeV2(handshake.into());
        }

        match (self.peer_type, self.peer_status, peer_msg) {
            (_, PeerStatus::Connecting, PeerMessage::HandshakeFailure(peer_info, reason)) => {
                match reason {
                    HandshakeFailureReason::GenesisMismatch(genesis) => {
                        warn!(target: "network", "Attempting to connect to a node ({}) with a different genesis block. Our genesis: {:?}, their genesis: {:?}", peer_info, self.genesis_id, genesis);
                    }
                    HandshakeFailureReason::ProtocolVersionMismatch {
                        version,
                        oldest_supported_version,
                    } => {
                        warn!(target: "network", "Unable to connect to a node ({}) due to a network protocol version mismatch. Our version: {:?}, their: {:?}", peer_info, (PROTOCOL_VERSION, OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION), (version, oldest_supported_version));
                    }
                    HandshakeFailureReason::InvalidTarget => {
                        debug!(target: "network", "Peer found was not what expected. Updating peer info with {:?}", peer_info);
                        self.peer_manager_addr.do_send(PeerRequest::UpdatePeerInfo(peer_info));
                    }
                }
                ctx.stop();
            }
            (_, PeerStatus::Connecting, PeerMessage::HandshakeV2(handshake)) => {
                debug!(target: "network", "{:?}: Received handshake {:?}", self.node_info.id, handshake);

                if handshake.chain_info.genesis_id != self.genesis_id {
                    debug!(target: "network", "Received connection from node with different genesis.");
                    ctx.address().do_send(SendMessage {
                        message: PeerMessage::HandshakeFailure(
                            self.node_info.clone(),
                            HandshakeFailureReason::GenesisMismatch(self.genesis_id.clone()),
                        ),
                    });
                    return;
                    // Connection will be closed by a handshake timeout
                }

                if handshake.version < OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION
                    || PROTOCOL_VERSION < handshake.oldest_supported_version
                {
                    debug!(target: "network", "Received connection from node with incompatible network protocol version.");
                    self.send_message(PeerMessage::HandshakeFailure(
                        self.node_info.clone(),
                        HandshakeFailureReason::ProtocolVersionMismatch {
                            version: PROTOCOL_VERSION,
                            oldest_supported_version: OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION,
                        },
                    ));
                    return;
                    // Connection will be closed by a handshake timeout
                }

                if handshake.peer_id == self.node_info.id {
                    near_metrics::inc_counter(&metrics::RECEIVED_INFO_ABOUT_ITSELF);
                    debug!(target: "network", "Received info about itself. Disconnecting this peer.");
                    ctx.stop();
                    return;
                }

                if handshake.target_peer_id != self.node_info.id {
                    debug!(target: "network", "Received handshake from {:?} to {:?} but I am {:?}", handshake.peer_id, handshake.target_peer_id, self.node_info.id);
                    self.send_message(PeerMessage::HandshakeFailure(
                        self.node_info.clone(),
                        HandshakeFailureReason::InvalidTarget,
                    ));
                    return;
                    // Connection will be closed by a handshake timeout
                }

                // Verify signature of the new edge in handshake.
                if !Edge::partial_verify(
                    self.node_id(),
                    handshake.peer_id.clone(),
                    &handshake.edge_info,
                ) {
                    warn!(target: "network", "Received invalid signature on handshake. Disconnecting peer {}", handshake.peer_id);
                    self.ban_peer(ctx, ReasonForBan::InvalidSignature);
                    return;
                }

                // Check that received nonce on handshake match our proposed nonce.
                if self.peer_type == PeerType::Outbound {
                    if handshake.edge_info.nonce
                        != self.edge_info.as_ref().map(|edge_info| edge_info.nonce).unwrap()
                    {
                        warn!(target: "network", "Received invalid nonce on handshake. Disconnecting peer {}", handshake.peer_id);
                        ctx.stop();
                        return;
                    }
                }

                let peer_info = PeerInfo {
                    id: handshake.peer_id.clone(),
                    addr: handshake
                        .listen_port
                        .map(|port| SocketAddr::new(self.peer_addr.ip(), port)),
                    account_id: None,
                };
                self.chain_info = handshake.chain_info.clone();
                self.peer_manager_addr
                    .send(Consolidate {
                        actor: ctx.address(),
                        peer_info: peer_info.clone(),
                        peer_type: self.peer_type,
                        chain_info: handshake.chain_info.clone(),
                        this_edge_info: self.edge_info.clone(),
                        other_edge_info: handshake.edge_info.clone(),
                    })
                    .into_actor(self)
                    .then(move |res, act, ctx| {
                        match res {
                            Ok(ConsolidateResponse::Accept(edge_info)) => {
                                act.peer_info = Some(peer_info).into();
                                act.peer_status = PeerStatus::Ready;
                                // Respond to handshake if it's inbound and connection was consolidated.
                                if act.peer_type == PeerType::Inbound {
                                    act.edge_info = edge_info;
                                    act.send_handshake(ctx);
                                }
                                actix::fut::ready(())
                            },
                            Ok(ConsolidateResponse::InvalidNonce(edge)) => {
                                debug!(target: "network", "{:?}: Received invalid nonce from peer {:?} sending evidence.", act.node_id(), act.peer_addr);
                                act.send_message(PeerMessage::LastEdge(*edge));
                                actix::fut::ready(())
                            }
                            _ => {
                                info!(target: "network", "{:?}: Peer with handshake {:?} wasn't consolidated, disconnecting.", act.node_id(), handshake);
                                ctx.stop();
                                actix::fut::ready(())
                            }
                        }
                    })
                    .wait(ctx);
            }
            (_, PeerStatus::Connecting, PeerMessage::LastEdge(edge)) => {
                // This message will be received only if we started the connection.
                if self.peer_type == PeerType::Inbound {
                    info!(target: "network", "{:?}: Inbound peer {:?} sent invalid message. Disconnect.", self.node_id(), self.peer_addr);
                    ctx.stop();
                    return ();
                }

                // Disconnect if neighbor propose invalid edge.
                if !edge.verify() {
                    info!(target: "network", "{:?}: Peer {:?} sent invalid edge. Disconnect.", self.node_id(), self.peer_addr);
                    ctx.stop();
                    return ();
                }

                self.peer_manager_addr
                    .send(PeerRequest::UpdateEdge((self.peer_id().unwrap(), edge.next())))
                    .into_actor(self)
                    .then(|res, act, ctx| {
                        match res {
                            Ok(PeerResponse::UpdatedEdge(edge_info)) => {
                                act.edge_info = Some(edge_info);
                                act.send_handshake(ctx);
                            }
                            _ => {}
                        }
                        actix::fut::ready(())
                    })
                    .spawn(ctx);
            }
            (_, PeerStatus::Ready, PeerMessage::Disconnect) => {
                debug!(target: "network", "Disconnect signal. Me: {:?} Peer: {:?}", self.node_info.id, self.peer_id());
                ctx.stop();
            }
            (_, PeerStatus::Ready, PeerMessage::Handshake(_)) => {
                // Received handshake after already have seen handshake from this peer.
                debug!(target: "network", "Duplicate handshake from {}", self.peer_info);
            }
            (_, PeerStatus::Ready, PeerMessage::PeersRequest) => {
                self.peer_manager_addr.send(PeersRequest {}).into_actor(self).then(|res, act, _ctx| {
                    if let Ok(peers) = res {
                        if !peers.peers.is_empty() {
                            debug!(target: "network", "Peers request from {}: sending {} peers.", act.peer_info, peers.peers.len());
                            act.send_message(PeerMessage::PeersResponse(peers.peers));
                        }
                    }
                    actix::fut::ready(())
                }).spawn(ctx);
            }
            (_, PeerStatus::Ready, PeerMessage::PeersResponse(peers)) => {
                debug!(target: "network", "Received peers from {}: {} peers.", self.peer_info, peers.len());
                self.peer_manager_addr.do_send(PeersResponse { peers });
            }
            (_, PeerStatus::Ready, PeerMessage::RequestUpdateNonce(edge_info)) => self
                .peer_manager_addr
                .send(NetworkRequests::RequestUpdateNonce(self.peer_id().unwrap(), edge_info))
                .into_actor(self)
                .then(|res, act, ctx| {
                    match res {
                        Ok(NetworkResponses::EdgeUpdate(edge)) => {
                            act.send_message(PeerMessage::ResponseUpdateNonce(*edge));
                        }
                        Ok(NetworkResponses::BanPeer(reason_for_ban)) => {
                            act.ban_peer(ctx, reason_for_ban);
                        }
                        _ => {}
                    }
                    actix::fut::ready(())
                })
                .spawn(ctx),
            (_, PeerStatus::Ready, PeerMessage::ResponseUpdateNonce(edge)) => self
                .peer_manager_addr
                .send(NetworkRequests::ResponseUpdateNonce(edge))
                .into_actor(self)
                .then(|res, act, ctx| {
                    match res {
                        Ok(NetworkResponses::BanPeer(reason_for_ban)) => {
                            act.ban_peer(ctx, reason_for_ban);
                        }
                        _ => {}
                    }
                    actix::fut::ready(())
                })
                .spawn(ctx),
            (_, PeerStatus::Ready, PeerMessage::RoutingTableSync(sync_data)) => {
                self.peer_manager_addr
                    .do_send(NetworkRequests::Sync { peer_id: self.peer_id().unwrap(), sync_data });
            }
            (_, PeerStatus::Ready, PeerMessage::Routed(routed_message)) => {
                trace!(target: "network", "Received routed message from {} to {:?}.", self.peer_info, routed_message.target);

                // Receive invalid routed message from peer.
                if !routed_message.verify() {
                    self.ban_peer(ctx, ReasonForBan::InvalidSignature);
                } else {
                    self.peer_manager_addr
                        .send(RoutedMessageFrom {
                            msg: routed_message.clone(),
                            from: self.peer_id().unwrap(),
                        })
                        .into_actor(self)
                        .then(move |res, act, ctx| {
                            if res.unwrap_or(false) {
                                act.receive_message(ctx, PeerMessage::Routed(routed_message));
                            }
                            actix::fut::ready(())
                        })
                        .spawn(ctx);
                }
            }
            (_, PeerStatus::Ready, msg) => {
                self.receive_message(ctx, msg);
            }
            (_, _, msg) => {
                warn!(target: "network", "Received {} while {:?} from {:?} connection.", msg, self.peer_status, self.peer_type);
            }
        }
    }
}

impl Handler<SendMessage> for Peer {
    type Result = ();

    fn handle(&mut self, msg: SendMessage, _: &mut Self::Context) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("send message".into());
        self.send_message(msg.message);
    }
}

impl Handler<QueryPeerStats> for Peer {
    type Result = PeerStatsResult;

    fn handle(&mut self, _: QueryPeerStats, _: &mut Self::Context) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("query peer stats".into());
        PeerStatsResult {
            chain_info: self.chain_info.clone(),
            received_bytes_per_sec: self.tracker.received_bytes.bytes_per_min() / 60,
            sent_bytes_per_sec: self.tracker.sent_bytes.bytes_per_min() / 60,
            is_abusive: self.is_abusive(),
            message_counts: (
                self.tracker.sent_bytes.count_per_min(),
                self.tracker.received_bytes.count_per_min(),
            ),
        }
    }
}

impl Handler<PeerManagerRequest> for Peer {
    type Result = ();

    fn handle(&mut self, pm_request: PeerManagerRequest, ctx: &mut Self::Context) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new(format!("peer manager request {:?}", pm_request).into());
        match pm_request {
            PeerManagerRequest::BanPeer(ban_reason) => {
                self.ban_peer(ctx, ban_reason);
            }
            PeerManagerRequest::UnregisterPeer => {
                ctx.stop();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::hash;

    use super::*;

    #[test]
    #[should_panic]
    fn test_circular_queue_zero_capacity() {
        let _ = CircularUniqueQueue::new(0);
    }

    #[test]
    fn test_circular_queue_empty_queue() {
        let q = CircularUniqueQueue::new(5);

        assert!(!q.contains(&hash(&[0])));
    }

    #[test]
    fn test_circular_queue_partially_full_queue() {
        let mut q = CircularUniqueQueue::new(5);
        for i in 1..=3 {
            q.push(hash(&[i]));
        }

        for i in 1..=3 {
            assert!(q.contains(&hash(&[i])));
        }
    }

    #[test]
    fn test_circular_queue_full_queue() {
        let mut q = CircularUniqueQueue::new(5);
        for i in 1..=5 {
            q.push(hash(&[i]));
        }

        for i in 1..=5 {
            assert!(q.contains(&hash(&[i])));
        }
    }

    #[test]
    fn test_circular_queue_over_full_queue() {
        let mut q = CircularUniqueQueue::new(5);
        for i in 1..=7 {
            q.push(hash(&[i]));
        }

        for i in 1..=2 {
            assert!(!q.contains(&hash(&[i])));
        }
        for i in 3..=7 {
            assert!(q.contains(&hash(&[i])));
        }
    }

    #[test]
    fn test_circular_queue_similar_inputs() {
        let mut q = CircularUniqueQueue::new(5);
        q.push(hash(&[5]));
        for _ in 0..3 {
            for i in 1..=3 {
                for _ in 0..5 {
                    q.push(hash(&[i]));
                }
            }
        }
        for i in 1..=3 {
            assert!(q.contains(&hash(&[i])));
        }
        assert!(q.contains(&hash(&[5])));
    }
}
