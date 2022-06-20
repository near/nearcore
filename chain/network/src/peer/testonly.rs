use crate::network_protocol::testonly as data;
use crate::peer::codec::Codec;
use crate::peer::peer_actor::PeerActor;
use crate::private_actix::{PeerRequestResult, RegisterPeerResponse, SendMessage};
use crate::tests::actix::ActixSystem;
use crate::types::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
    PeerManagerMessageRequest, PeerManagerMessageResponse, PeerMessage, RoutingTableUpdate,
};
use actix::{Actor, Context, Handler, StreamHandler as _};
use near_crypto::InMemorySigner;
use near_network_primitives::time;
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, Edge, NetworkViewClientMessages, NetworkViewClientResponses,
    PartialEdgeInfo, PeerInfo, PeerType, RawRoutedMessage, RoutedMessageBody, RoutedMessageV2,
};
use near_performance_metrics::framed_write::FramedWrite;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::sharding::{ChunkHash, PartialEncodedChunkPart};
use near_primitives::syncing::EpochSyncResponse;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::EpochId;
use near_rate_limiter::{
    ActixMessageResponse, ActixMessageWrapper, ThrottleController, ThrottleFramedRead,
    ThrottleToken,
};

use near_network_primitives::time::Utc;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct PeerConfig {
    pub signer: InMemorySigner,
    pub chain: Arc<data::Chain>,
    pub peers: Vec<PeerInfo>,
    pub start_handshake_with: Option<PeerId>,
    pub force_encoding: Option<crate::network_protocol::Encoding>,
}

impl PeerConfig {
    pub fn id(&self) -> PeerId {
        PeerId::new(self.signer.public_key.clone())
    }

    pub fn peer_type(&self) -> PeerType {
        match self.start_handshake_with {
            Some(_) => PeerType::Outbound,
            None => PeerType::Inbound,
        }
    }

    pub fn partial_edge_info(&self, other: &PeerId, nonce: u64) -> PartialEdgeInfo {
        PartialEdgeInfo::new(&self.id(), other, nonce, &self.signer.secret_key)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    HandshakeDone,
    BlockRequest(CryptoHash),
    Block(Block),
    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),
    Chunk(Vec<PartialEncodedChunkPart>),
    ChunkRequest(ChunkHash),
    RoutingTable(RoutingTableUpdate),
    RequestUpdateNonce(PartialEdgeInfo),
    ResponseUpdateNonce(Edge),
    PeersResponse(Vec<PeerInfo>),
    Transaction(SignedTransaction),
    Challenge(Challenge),
    EpochSyncRequest(EpochId),
    EpochSyncResponse(EpochSyncResponse),
    EpochSyncFinalizationRequest(EpochId),
}

struct FakeActor {
    cfg: Arc<PeerConfig>,
    responses: tokio::sync::mpsc::UnboundedSender<Response>,
}

impl Actor for FakeActor {
    type Context = Context<Self>;
}

impl Handler<ActixMessageWrapper<PeerManagerMessageRequest>> for FakeActor {
    type Result = ActixMessageResponse<PeerManagerMessageResponse>;

    fn handle(
        &mut self,
        msg: ActixMessageWrapper<PeerManagerMessageRequest>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let (msg, throttle_token) = msg.take();
        ActixMessageResponse::new(
            self.handle(msg, ctx),
            ThrottleToken::new_without_size(throttle_token.throttle_controller().cloned()),
        )
    }
}

impl Handler<NetworkViewClientMessages> for FakeActor {
    type Result = NetworkViewClientResponses;
    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        println!("{}: view client message {}", self.cfg.id(), Into::<&'static str>::into(&msg));
        match msg {
            NetworkViewClientMessages::GetChainInfo => {
                let ci = self.cfg.chain.get_info();
                NetworkViewClientResponses::ChainInfo {
                    genesis_id: ci.genesis_id,
                    height: ci.height,
                    tracked_shards: ci.tracked_shards,
                    archival: ci.archival,
                }
            }
            NetworkViewClientMessages::BlockRequest(block_hash) => {
                self.responses.send(Response::BlockRequest(block_hash)).unwrap();
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::BlockHeadersRequest(req) => {
                self.responses.send(Response::BlockHeadersRequest(req)).unwrap();
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::EpochSyncRequest { epoch_id } => {
                self.responses.send(Response::EpochSyncRequest(epoch_id)).unwrap();
                NetworkViewClientResponses::NoResponse
            }
            NetworkViewClientMessages::EpochSyncFinalizationRequest { epoch_id } => {
                self.responses.send(Response::EpochSyncFinalizationRequest(epoch_id)).unwrap();
                NetworkViewClientResponses::NoResponse
            }
            _ => panic!("unsupported message"),
        }
    }
}

impl Handler<NetworkClientMessages> for FakeActor {
    type Result = NetworkClientResponses;
    fn handle(&mut self, msg: NetworkClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        println!("{}: client message {}", self.cfg.id(), Into::<&'static str>::into(&msg));
        let mut resp = NetworkClientResponses::NoResponse;
        match msg {
            NetworkClientMessages::Block(b, _, _) => {
                self.responses.send(Response::Block(b)).unwrap()
            }
            NetworkClientMessages::BlockHeaders(bhs, _) => {
                self.responses.send(Response::BlockHeaders(bhs)).unwrap()
            }
            NetworkClientMessages::PartialEncodedChunkResponse(resp, _) => {
                self.responses.send(Response::Chunk(resp.parts)).unwrap()
            }
            NetworkClientMessages::PartialEncodedChunkRequest(req, _) => {
                self.responses.send(Response::ChunkRequest(req.chunk_hash)).unwrap()
            }
            NetworkClientMessages::Transaction { transaction, .. } => {
                self.responses.send(Response::Transaction(transaction)).unwrap();
                resp = NetworkClientResponses::ValidTx;
            }
            NetworkClientMessages::Challenge(c) => {
                self.responses.send(Response::Challenge(c)).unwrap()
            }
            NetworkClientMessages::EpochSyncResponse(_, resp) => {
                self.responses.send(Response::EpochSyncResponse(*resp)).unwrap()
            }
            _ => panic!("unsupported message"),
        };
        resp
    }
}

impl Handler<PeerManagerMessageRequest> for FakeActor {
    type Result = PeerManagerMessageResponse;
    fn handle(&mut self, msg: PeerManagerMessageRequest, _ctx: &mut Self::Context) -> Self::Result {
        let msg_type: &str = (&msg).into();
        println!("{}: PeerManager message {}", self.cfg.id(), msg_type);
        match msg {
            PeerManagerMessageRequest::RegisterPeer(msg) => {
                self.responses.send(Response::HandshakeDone).unwrap();
                PeerManagerMessageResponse::RegisterPeerResponse(RegisterPeerResponse::Accept(
                    match msg.this_edge_info {
                        Some(_) => None,
                        None => Some(
                            self.cfg
                                .partial_edge_info(&msg.peer_info.id, msg.other_edge_info.nonce),
                        ),
                    },
                ))
            }
            PeerManagerMessageRequest::RoutedMessageFrom(_) => {
                // Accept all incoming routed messages
                PeerManagerMessageResponse::RoutedMessageFrom(true)
            }
            PeerManagerMessageRequest::NetworkRequests(req) => {
                self.responses
                    .send(match req {
                        NetworkRequests::SyncRoutingTable { routing_table_update, .. } => {
                            Response::RoutingTable(routing_table_update)
                        }
                        NetworkRequests::RequestUpdateNonce(_, edge) => {
                            Response::RequestUpdateNonce(edge)
                        }
                        NetworkRequests::ResponseUpdateNonce(edge) => {
                            Response::ResponseUpdateNonce(edge)
                        }
                        _ => panic!("unsupported message"),
                    })
                    .unwrap();
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }
            PeerManagerMessageRequest::PeersRequest(_) => {
                // PeerActor would panic if we returned a different response.
                // This also triggers sending a message to the peer.
                PeerManagerMessageResponse::PeerRequestResult(PeerRequestResult {
                    peers: self.cfg.peers.clone(),
                })
            }
            PeerManagerMessageRequest::PeersResponse(resp) => {
                self.responses.send(Response::PeersResponse(resp.peers)).unwrap();
                PeerManagerMessageResponse::PeersResponseResult(())
            }
            _ => panic!("unsupported message"),
        }
    }
}

pub struct PeerHandle {
    pub cfg: Arc<PeerConfig>,
    actix: ActixSystem<PeerActor>,
    responses: tokio::sync::mpsc::UnboundedReceiver<Response>,
}

impl PeerHandle {
    pub async fn recv(&mut self) -> Response {
        self.responses.recv().await.unwrap()
    }

    pub async fn send(&self, message: PeerMessage) {
        self.actix
            .addr
            .send(SendMessage { message, context: Span::current().context() })
            .await
            .unwrap();
    }

    pub fn routed_message(
        &self,
        body: RoutedMessageBody,
        peer_id: PeerId,
        utc: Utc,
    ) -> Box<RoutedMessageV2> {
        RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(peer_id), body }.sign(
            self.cfg.id(),
            &self.cfg.signer.secret_key,
            /*ttl=*/ 1,
            utc,
        )
    }

    pub async fn start_endpoint(
        clock: time::Clock,
        cfg: PeerConfig,
        stream: TcpStream,
    ) -> PeerHandle {
        let cfg = Arc::new(cfg);
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();

        let cfg_ = cfg.clone();
        let actix = ActixSystem::spawn(move || {
            let my_addr = stream.local_addr().unwrap();
            let peer_addr = stream.peer_addr().unwrap();
            let (read, write) = tokio::io::split(stream);
            let handshake_timeout = time::Duration::seconds(5);
            let fa = FakeActor { cfg: cfg.clone(), responses: send }.start();
            let rate_limiter = ThrottleController::new(usize::MAX, usize::MAX);
            let read = ThrottleFramedRead::new(read, Codec::default(), rate_limiter.clone())
                .take_while(|x| match x {
                    Ok(_) => true,
                    Err(_) => false,
                })
                .map(Result::unwrap);
            PeerActor::create(move |ctx| {
                PeerActor::add_stream(read, ctx);
                PeerActor::new(
                    clock,
                    PeerInfo { id: cfg.id(), addr: Some(my_addr), account_id: None },
                    peer_addr.clone(),
                    cfg.start_handshake_with.as_ref().map(|id| PeerInfo {
                        id: id.clone(),
                        addr: Some(peer_addr.clone()),
                        account_id: None,
                    }),
                    cfg.peer_type(),
                    FramedWrite::new(write, Codec::default(), Codec::default(), ctx),
                    handshake_timeout,
                    fa.clone().recipient(),
                    fa.clone().recipient(),
                    fa.clone().recipient(),
                    fa.clone().recipient(),
                    cfg.start_handshake_with.as_ref().map(|id| cfg.partial_edge_info(id, 1)),
                    Arc::new(AtomicUsize::new(0)),
                    Arc::new(AtomicUsize::new(0)),
                    rate_limiter,
                    cfg.force_encoding,
                )
            })
        })
        .await;
        Self { actix, cfg: cfg_, responses: recv }
    }

    pub async fn start_connection() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connect_future = TcpStream::connect(listener.local_addr().unwrap());
        let accept_future = listener.accept();
        let (connect_result, accept_result) = tokio::join!(connect_future, accept_future);
        let outbound_stream = connect_result.unwrap();
        let (inbound_stream, _) = accept_result.unwrap();
        (outbound_stream, inbound_stream)
    }
}
