use crate::broadcast;
use crate::config::NetworkConfig;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Edge, PartialEdgeInfo, PeerIdOrHash, PeerMessage, RawRoutedMessage, RoutedMessageBody,
    RoutedMessageV2,
};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor;
use crate::peer_manager::peer_store;
use crate::private_actix::SendMessage;
use crate::store;
use crate::tcp;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use near_async::messaging::IntoSender;
use near_async::time;
use near_o11y::WithSpanContextExt;
use near_primitives::network::PeerId;
use std::sync::Arc;

pub struct PeerConfig {
    pub chain: Arc<data::Chain>,
    pub network: NetworkConfig,
    pub force_encoding: Option<crate::network_protocol::Encoding>,
}

impl PeerConfig {
    pub fn id(&self) -> PeerId {
        self.network.node_id()
    }

    pub fn partial_edge_info(&self, other: &PeerId, nonce: u64) -> PartialEdgeInfo {
        PartialEdgeInfo::new(&self.id(), other, nonce, &self.network.node_key)
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
    pub edge: Option<Edge>,
}

impl PeerHandle {
    pub async fn send(&self, message: PeerMessage) {
        self.actix
            .addr
            .send(SendMessage { message: Arc::new(message) }.with_span_context())
            .await
            .unwrap();
    }

    pub async fn complete_handshake(&mut self) {
        self.edge = Some(
            self.events
                .recv_until(|ev| match ev {
                    Event::Network(peer_manager_actor::Event::HandshakeCompleted(ev)) => {
                        Some(ev.edge)
                    }
                    Event::Network(peer_manager_actor::Event::ConnectionClosed(ev)) => {
                        panic!("handshake failed: {}", ev.reason)
                    }
                    _ => None,
                })
                .await,
        );
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
        let (send, recv) = broadcast::unbounded_channel();

        let fc = Arc::new(fake_client::Fake { event_sink: send.sink().compose(Event::Client) });
        let store = store::Store::from(near_store::db::TestDB::new());
        let mut network_cfg = cfg.network.clone();
        network_cfg.event_sink = send.sink().compose(Event::Network);
        let network_state = Arc::new(NetworkState::new(
            &clock,
            store.clone(),
            peer_store::PeerStore::new(&clock, network_cfg.peer_store.clone()).unwrap(),
            network_cfg.verify().unwrap(),
            cfg.chain.genesis_id.clone(),
            fc.clone(),
            fc.as_sender(),
            vec![],
        ));
        let actix = ActixSystem::spawn({
            let clock = clock.clone();
            let cfg = cfg.clone();
            move || PeerActor::spawn(clock, stream, cfg.force_encoding, network_state).unwrap().0
        })
        .await;
        Self { actix, cfg, events: recv, edge: None }
    }
}
