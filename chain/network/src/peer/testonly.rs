use crate::broadcast;
use crate::config::NetworkConfig;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Edge, PartialEdgeInfo, PeerMessage, RawRoutedMessage, RoutedMessageBody, RoutedMessageV2,
};
use crate::peer::peer_actor::{ClosingReason, PeerActor};
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor;
use crate::peer_manager::peer_store;
use crate::private_actix::SendMessage;
use crate::store;
use crate::tcp;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::time;
use crate::types::PeerIdOrHash;
use near_crypto::{InMemorySigner, Signature};
use near_o11y::WithSpanContextExt;
use near_primitives::network::PeerId;
use std::sync::Arc;

pub struct PeerConfig {
    pub chain: Arc<data::Chain>,
    pub network: NetworkConfig,
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
    Client(fake_client::Event),
    Network(peer_manager_actor::Event),
}

pub(crate) struct PeerHandle {
    pub cfg: Arc<PeerConfig>,
    actix: ActixSystem<PeerActor>,
    pub events: broadcast::Receiver<Event>,
}

impl PeerHandle {
    pub async fn send(&self, message: PeerMessage) {
        self.actix
            .addr
            .send(SendMessage { message: Arc::new(message) }.with_span_context())
            .await
            .unwrap();
    }

    pub async fn complete_handshake(&mut self) -> Edge {
        self.events
            .recv_until(|ev| match ev {
                Event::Network(peer_manager_actor::Event::HandshakeCompleted(ev)) => Some(ev.edge),
                Event::Network(peer_manager_actor::Event::ConnectionClosed(ev)) => {
                    panic!("handshake failed: {}", ev.reason)
                }
                _ => None,
            })
            .await
    }
    pub async fn fail_handshake(&mut self) -> ClosingReason {
        self.events
            .recv_until(|ev| match ev {
                Event::Network(peer_manager_actor::Event::ConnectionClosed(ev)) => Some(ev.reason),
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
        RawRoutedMessage { target: PeerIdOrHash::PeerId(peer_id), body }.sign(
            &self.cfg.network.node_key,
            ttl,
            utc,
        )
    }

    pub async fn start_endpoint(
        clock: time::Clock,
        cfg: PeerConfig,
        stream: tcp::Stream,
    ) -> PeerHandle {
        let cfg = Arc::new(cfg);
        let cfg_ = cfg.clone();
        let (send, recv) = broadcast::unbounded_channel();
        let actix = ActixSystem::spawn(move || {
            let fc = Arc::new(fake_client::Fake { event_sink: send.sink().compose(Event::Client) });
            let store = store::Store::from(near_store::db::TestDB::new());
            let mut network_cfg = cfg.network.clone();
            network_cfg.event_sink = send.sink().compose(Event::Network);
            let network_state = Arc::new(NetworkState::new(
                &clock,
                store.clone(),
                peer_store::PeerStore::new(&clock, network_cfg.peer_store.clone(), store.clone())
                    .unwrap(),
                Arc::new(network_cfg.verify().unwrap()),
                cfg.chain.genesis_id.clone(),
                fc,
                vec![],
            ));
            // WARNING: this is a hack to make PeerActor use a specific nonce
            if let (Some(nonce), tcp::StreamType::Outbound { peer_id }) =
                (&cfg.nonce, &stream.type_)
            {
                network_state.routing_table_view.add_local_edges(&[Edge::new(
                    cfg.id(),
                    peer_id.clone(),
                    nonce - 1,
                    Signature::default(),
                    Signature::default(),
                )]);
            }
            PeerActor::spawn(clock, stream, cfg.force_encoding, network_state).unwrap()
        })
        .await;
        Self { actix, cfg: cfg_, events: recv }
    }
}
