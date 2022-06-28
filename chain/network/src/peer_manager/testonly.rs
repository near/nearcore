use crate::broadcast;
use crate::network_protocol::testonly as data;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::PeerManagerActor;
use actix::Actor;
use near_network_primitives::types::NetworkConfig;
use near_store::test_utils::create_test_store;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    Client(fake_client::Event),
    PeerManager(crate::peer_manager::peer_manager_actor::Event),
}

pub struct ActorHandler {
    pub cfg: NetworkConfig,
    pub events: broadcast::Receiver<Event>,
    _actix: ActixSystem<PeerManagerActor>,
}

pub async fn start(chain: Arc<data::Chain>, cfg: NetworkConfig) -> ActorHandler {
    let (send, recv) = broadcast::unbounded_channel();
    let actix = ActixSystem::spawn({
        let cfg = cfg.clone();
        move || {
            let store = create_test_store();
            let fc = fake_client::start(chain, send.sink().compose(Event::Client));
            PeerManagerActor::new(store, cfg, fc.clone().recipient(), fc.clone().recipient())
                .unwrap()
                .with_event_sink(send.sink().compose(Event::PeerManager))
                .start()
        }
    })
    .await;
    let mut h = ActorHandler { cfg, _actix: actix, events: recv };
    // Wait for the server to start.
    assert_eq!(Event::PeerManager(PME::ServerStarted), h.events.recv().await);
    h
}
