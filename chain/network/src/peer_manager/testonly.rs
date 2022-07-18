use crate::broadcast;
use crate::network_protocol::testonly as data;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::PeerManagerActor;
use actix::Actor;
use near_network_primitives::types::{NetworkConfig, SetChainInfo};
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
    pub actix: ActixSystem<PeerManagerActor>,
}

pub async fn start(chain: Arc<data::Chain>, cfg: NetworkConfig) -> ActorHandler {
    let (send, recv) = broadcast::unbounded_channel();
    let actix = ActixSystem::spawn({
        let cfg = cfg.clone();
        let chain = chain.clone();
        move || {
            let genesis_id = chain.genesis_id.clone();
            let store = create_test_store();
            let fc = fake_client::start(send.sink().compose(Event::Client));
            PeerManagerActor::new(
                store,
                cfg,
                fc.clone().recipient(),
                fc.clone().recipient(),
                genesis_id,
            )
            .unwrap()
            .with_event_sink(send.sink().compose(Event::PeerManager))
            .start()
        }
    })
    .await;
    let mut h = ActorHandler { cfg, actix, events: recv };
    // Wait for the server to start.
    assert_eq!(Event::PeerManager(PME::ServerStarted), h.events.recv().await);
    h.actix.addr.send(SetChainInfo(chain.get_chain_info())).await.unwrap();
    h
}
