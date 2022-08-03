use crate::broadcast;
use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::PeerAddr;
use crate::peer_manager::peer_manager_actor;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::testonly::Rng;
use crate::types::{ChainInfo, GetNetworkInfo, PeerManagerMessageRequest, SetChainInfo};
use crate::PeerManagerActor;
use actix::Actor;
use near_network_primitives::types::{OutboundTcpConnect, PeerInfo};
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, EpochId};
use near_store::test_utils::create_test_store;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    Client(fake_client::Event),
    PeerManager(peer_manager_actor::Event),
}

pub struct ActorHandler {
    pub cfg: config::NetworkConfig,
    pub events: broadcast::Receiver<Event>,
    pub actix: ActixSystem<PeerManagerActor>,
}

impl ActorHandler {
    pub fn peer_info(&self) -> PeerInfo {
        PeerInfo {
            id: PeerId::new(self.cfg.node_key.public_key()),
            addr: self.cfg.node_addr.clone(),
            account_id: None,
        }
    }
    pub async fn connect_to(&mut self, peer_info: &PeerInfo) {
        self.actix
            .addr
            .send(PeerManagerMessageRequest::OutboundTcpConnect(OutboundTcpConnect {
                peer_info: peer_info.clone(),
            }))
            .await
            .unwrap();
        self.events
            .recv_until(|ev| match &ev {
                Event::PeerManager(peer_manager_actor::Event::PeerRegistered(info))
                    if peer_info == info =>
                {
                    Some(())
                }
                _ => None,
            })
            .await;
    }

    pub async fn set_chain_info(&mut self, chain_info: ChainInfo) {
        self.actix.addr.send(SetChainInfo(chain_info)).await.unwrap();
        self.events
            .recv_until(|ev| match ev {
                Event::PeerManager(peer_manager_actor::Event::SetChainInfo) => Some(()),
                _ => None,
            })
            .await;
    }

    pub async fn wait_for_accounts_data(
        &mut self,
        want: &HashMap<(EpochId, AccountId), Vec<PeerAddr>>,
    ) {
        // WARNING: this loop might become a spin-lock if any of the calls in the loop iteration
        // was generating an event. To fix that, wait for specific events.
        loop {
            let info = self.actix.addr.send(GetNetworkInfo).await.unwrap();
            let got: HashMap<_, _> = info
                .tier1_accounts
                .into_iter()
                .map(|d| ((d.epoch_id.clone(), d.account_id.clone()), d.peers.clone()))
                .collect();
            if &got == want {
                break;
            }
            self.events.recv().await;
        }
    }
}

pub async fn start(rng: &mut Rng, chain: Arc<data::Chain>) -> ActorHandler {
    let cfg = chain.make_config(rng);
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
