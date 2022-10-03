use crate::broadcast;
use crate::concurrency::demux;
use crate::config::NetworkConfig;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Edge, PartialEdgeInfo, PeerInfo, PeerMessage, RawRoutedMessage, RoutedMessageBody,
    RoutedMessageV2, RoutingTableUpdate,
};
use crate::peer::peer_actor::{PeerActor, StreamConfig};
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor;
use crate::private_actix::{PeerRequestResult, RegisterPeerResponse, SendMessage};
use crate::private_actix::{PeerToManagerMsg, PeerToManagerMsgResp};
use crate::routing::routing_table_view::RoutingTableView;
use crate::store;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::time;
use crate::types::AccountOrPeerIdOrHash;
use actix::{Actor, Context, Handler};
use near_crypto::{InMemorySigner, Signature};
use near_primitives::network::PeerId;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct PeerConfig {
    pub chain: Arc<data::Chain>,
    pub network: NetworkConfig,
    pub peers: Vec<PeerInfo>,
    pub start_handshake_with: Option<PeerId>,
    pub force_encoding: Option<crate::network_protocol::Encoding>,
    /// If both start_handshake_with and nonce are set, PeerActor
    /// will use this nonce in the handshake.
    /// WARNING: it has to be >0.
    /// WARNING: currently nonce is decided by a lookup in the RoutingTableView,
    ///   so to enforce the nonce below, we add an artificial edge to RoutingTableView.
    ///   Once we switch to generating nonce from timestamp, this field should be deprecated
    ///   in favor of passing a fake clock.
    pub nonce: Option<u64>,
}

impl PeerConfig {
    pub fn id(&self) -> PeerId {
        self.network.node_id()
    }

    pub fn partial_edge_info(&self, other: &PeerId, nonce: u64) -> PartialEdgeInfo {
        PartialEdgeInfo::new(&self.id(), other, nonce, &self.network.node_key)
    }

    pub fn signer(&self) -> InMemorySigner {
        InMemorySigner::from_secret_key("node".parse().unwrap(), self.network.node_key.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
    HandshakeDone(Edge),
    RoutingTable(RoutingTableUpdate),
    RequestUpdateNonce(PartialEdgeInfo),
    ResponseUpdateNonce(Edge),
    PeersResponse(Vec<PeerInfo>),
    Client(fake_client::Event),
    Network(peer_manager_actor::Event),
}

struct FakePeerManagerActor {
    cfg: Arc<PeerConfig>,
    event_sink: crate::sink::Sink<Event>,
}

impl Actor for FakePeerManagerActor {
    type Context = Context<Self>;
}

impl Handler<PeerToManagerMsg> for FakePeerManagerActor {
    type Result = PeerToManagerMsgResp;
    fn handle(&mut self, msg: PeerToManagerMsg, _ctx: &mut Self::Context) -> Self::Result {
        let msg_type: &str = (&msg).into();
        println!("{}: PeerManager message {}", self.cfg.id(), msg_type);
        match msg {
            PeerToManagerMsg::RegisterPeer(msg) => {
                self.event_sink.push(Event::HandshakeDone(msg.connection.edge.clone()));
                PeerToManagerMsgResp::RegisterPeer(RegisterPeerResponse::Accept)
            }
            PeerToManagerMsg::SyncRoutingTable { routing_table_update, .. } => {
                self.event_sink.push(Event::RoutingTable(routing_table_update));
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::RequestUpdateNonce(_, edge) => {
                self.event_sink.push(Event::RequestUpdateNonce(edge));
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::ResponseUpdateNonce(edge) => {
                self.event_sink.push(Event::ResponseUpdateNonce(edge));
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::PeersRequest(_) => {
                // PeerActor would panic if we returned a different response.
                // This also triggers sending a message to the peer.
                PeerToManagerMsgResp::PeersRequest(PeerRequestResult {
                    peers: self.cfg.peers.clone(),
                })
            }
            PeerToManagerMsg::PeersResponse(resp) => {
                self.event_sink.push(Event::PeersResponse(resp.peers));
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::Unregister(_) => PeerToManagerMsgResp::Empty,
            _ => panic!("unsupported message"),
        }
    }
}

pub struct PeerHandle {
    pub(crate) cfg: Arc<PeerConfig>,
    actix: ActixSystem<PeerActor>,
    pub(crate) events: broadcast::Receiver<Event>,
}

impl PeerHandle {
    pub async fn send(&self, message: PeerMessage) {
        self.actix
            .addr
            .send(SendMessage { message: Arc::new(message), context: Span::current().context() })
            .await
            .unwrap();
    }

    pub async fn complete_handshake(&mut self) -> Edge {
        self.events
            .recv_until(|ev| match ev {
                Event::HandshakeDone(edge) => Some(edge),
                Event::Network(peer_manager_actor::Event::ConnectionClosed(_)) => {
                    panic!("handshake failed")
                }
                _ => None,
            })
            .await
    }
    pub async fn fail_handshake(&mut self) {
        self.events
            .recv_until(|ev| match ev {
                Event::Network(peer_manager_actor::Event::ConnectionClosed(_)) => Some(()),
                // HandshakeDone means that handshake succeeded locally,
                // but in case this is an inbound connection, it can still
                // fail on the other side. Therefore we cannot panic on HandshakeDone.
                _ => None,
            })
            .await
    }

    pub fn routed_message(
        &self,
        body: RoutedMessageBody,
        peer_id: PeerId,
        ttl: u8,
        utc: Option<time::Utc>,
    ) -> RoutedMessageV2 {
        RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(peer_id), body }.sign(
            &self.cfg.network.node_key,
            ttl,
            utc,
        )
    }

    pub async fn start_endpoint(
        clock: time::Clock,
        cfg: PeerConfig,
        stream: TcpStream,
    ) -> PeerHandle {
        let cfg = Arc::new(cfg);
        let cfg_ = cfg.clone();
        let (send, recv) = broadcast::unbounded_channel();
        let actix = ActixSystem::spawn(move || {
            let fpm = FakePeerManagerActor { cfg: cfg.clone(), event_sink: send.sink() }.start();
            let fc = fake_client::start(send.sink().compose(Event::Client));
            let store = store::Store::from(near_store::db::TestDB::new());
            let routing_table_view = RoutingTableView::new(store, cfg.id());
            // WARNING: this is a hack to make PeerActor use a specific nonce
            if let (Some(nonce), Some(peer_id)) = (&cfg.nonce, &cfg.start_handshake_with) {
                routing_table_view.add_local_edges(&[Edge::new(
                    cfg.id(),
                    peer_id.clone(),
                    nonce - 1,
                    Signature::default(),
                    Signature::default(),
                )]);
            }
            let mut network_cfg = cfg.network.clone();
            network_cfg.event_sink = send.sink().compose(Event::Network);
            let network_state = Arc::new(NetworkState::new(
                Arc::new(network_cfg.verify().unwrap()),
                cfg.chain.genesis_id.clone(),
                fc.clone().recipient(),
                fc.clone().recipient(),
                fpm.recipient(),
                routing_table_view,
                demux::RateLimit { qps: 100., burst: 1 },
            ));
            PeerActor::spawn(
                clock,
                stream,
                match &cfg.start_handshake_with {
                    None => StreamConfig::Inbound,
                    Some(id) => StreamConfig::Outbound { peer_id: id.clone() },
                },
                cfg.force_encoding,
                network_state,
            )
            .unwrap()
        })
        .await;
        Self { actix, cfg: cfg_, events: recv }
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
