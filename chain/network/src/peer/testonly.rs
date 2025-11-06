use crate::auto_stop::AutoStopActor;
use crate::broadcast;
use crate::config::NetworkConfig;
use crate::network_protocol::{
    Edge, PartialEdgeInfo, PeerIdOrHash, PeerMessage, RawRoutedMessage, TieredMessageBody,
};
use crate::network_protocol::{RoutedMessage, testonly as data};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::peer_store;
use crate::private_messages::SendMessage;
use crate::store;
use crate::tcp;
use near_async::messaging::{CanSendAsync, IntoMultiSender, IntoSender, Sender, noop};
use near_async::{ActorSystem, time};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::network::PeerId;
use std::sync::Arc;

pub struct PeerConfig {
    pub chain: Arc<data::Chain>,
    pub network: NetworkConfig,
}

impl PeerConfig {
    pub fn id(&self) -> PeerId {
        self.network.node_id()
    }

    pub fn partial_edge_info(&self, other: &PeerId, nonce: u64) -> PartialEdgeInfo {
        PartialEdgeInfo::new(&self.id(), other, nonce, &self.network.node_key)
    }
}

pub(crate) struct PeerHandle {
    pub cfg: Arc<PeerConfig>,
    actor: AutoStopActor<PeerActor>,
    pub events: broadcast::Receiver<Event>,
    pub edge: Option<Edge>,
}

impl PeerHandle {
    pub async fn send(&self, message: PeerMessage) {
        self.actor
            .send_async(SendMessage { message: Arc::new(message) }.span_wrap())
            .await
            .unwrap();
    }

    pub async fn complete_handshake(&mut self) {
        self.edge = Some(
            self.events
                .recv_until(|ev| match ev {
                    Event::HandshakeCompleted(ev) => Some(ev.edge),
                    Event::ConnectionClosed(ev) => {
                        panic!("handshake failed: {}", ev.reason)
                    }
                    _ => None,
                })
                .await,
        );
    }

    pub fn routed_message(
        &self,
        body: TieredMessageBody,
        peer_id: PeerId,
        ttl: u8,
        utc: Option<time::Utc>,
    ) -> RoutedMessage {
        RawRoutedMessage { target: PeerIdOrHash::PeerId(peer_id), body }.sign(
            &self.cfg.network.node_key,
            ttl,
            utc,
        )
    }

    pub fn start_endpoint(
        clock: time::Clock,
        actor_system: ActorSystem,
        cfg: PeerConfig,
        stream: tcp::Stream,
    ) -> PeerHandle {
        let cfg = Arc::new(cfg);
        let (send, recv) = broadcast::unbounded_channel::<Event>();

        let store = store::Store::from(near_store::db::TestDB::new());
        let mut network_cfg = cfg.network.clone();
        network_cfg.event_sink = Sender::from_fn(move |event| {
            send.send(event);
        });
        let network_state = Arc::new(NetworkState::new(
            &clock,
            &*actor_system.new_future_spawner("network demux"),
            store,
            peer_store::PeerStore::new(&clock, network_cfg.peer_store.clone()).unwrap(),
            network_cfg.verify().unwrap(),
            cfg.chain.genesis_id.clone(),
            noop().into_multi_sender(),
            noop().into_multi_sender(),
            noop().into_multi_sender(),
            noop().into_sender(),
            noop().into_multi_sender(),
            vec![],
            noop().into_multi_sender(),
            noop().into_sender(),
        ));
        let actor = AutoStopActor(
            PeerActor::spawn(clock, actor_system, stream, network_state)
                .unwrap()
                .0,
        );
        Self { actor, cfg, events: recv, edge: None }
    }
}
