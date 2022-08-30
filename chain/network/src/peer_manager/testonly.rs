use crate::broadcast;
use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, PeerAddr, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::testonly::Rng;
use crate::types::{ChainInfo, GetNetworkInfo, PeerManagerMessageRequest, SetChainInfo};
use crate::PeerManagerActor;
use actix::Actor;
use near_network_primitives::time;
use near_network_primitives::types::{OutboundTcpConnect, PeerInfo};
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, EpochId};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    Client(fake_client::Event),
    PeerManager(PME),
}

pub(crate) struct ActorHandler {
    pub cfg: config::NetworkConfig,
    pub events: broadcast::Receiver<Event>,
    pub actix: ActixSystem<PeerManagerActor>,
}

pub fn unwrap_sync_accounts_data_processed(ev: Event) -> Option<SyncAccountsData> {
    match ev {
        Event::PeerManager(PME::MessageProcessed(PeerMessage::SyncAccountsData(msg))) => Some(msg),
        _ => None,
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct NormalAccountData {
    pub epoch_id: EpochId,
    pub account_id: AccountId,
    pub peers: Vec<PeerAddr>,
}

impl From<&Arc<SignedAccountData>> for NormalAccountData {
    fn from(d: &Arc<SignedAccountData>) -> Self {
        Self {
            epoch_id: d.epoch_id.clone(),
            account_id: d.account_id.clone(),
            peers: d.peers.clone(),
        }
    }
}

pub(crate) struct RawConnection {
    stream: tokio::net::TcpStream,
    cfg: peer::testonly::PeerConfig,
}

impl RawConnection {
    pub async fn handshake(
        self,
        clock: &time::Clock,
    ) -> (peer::testonly::PeerHandle, SyncAccountsData) {
        let mut peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;
        peer.complete_handshake().await;
        // TODO(gprusak): this should be part of complete_handshake, once Borsh support is removed.
        let msg = match peer.events.recv().await {
            peer::testonly::Event::Network(PME::MessageProcessed(
                PeerMessage::SyncAccountsData(msg),
            )) => msg,
            ev => panic!("expected SyncAccountsData, got {ev:?}"),
        };
        (peer, msg)
    }

    pub async fn fail_handshake(self, clock: &time::Clock) {
        let mut peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;
        peer.fail_handshake().await;
    }
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
                Event::PeerManager(PME::PeerRegistered(info)) if peer_info == info => Some(()),
                _ => None,
            })
            .await;
    }

    pub async fn start_connection(&self, rng: &mut Rng, chain: Arc<data::Chain>) -> RawConnection {
        RawConnection {
            stream: tokio::net::TcpStream::connect(self.cfg.node_addr.unwrap()).await.unwrap(),
            cfg: peer::testonly::PeerConfig {
                network: chain.make_config(rng),
                chain,
                peers: vec![],
                start_handshake_with: Some(PeerId::new(self.cfg.node_key.public_key())),
                force_encoding: Some(Encoding::Proto),
                nonce: None,
            },
        }
    }

    pub async fn set_chain_info(&mut self, chain_info: ChainInfo) {
        self.actix.addr.send(SetChainInfo(chain_info)).await.unwrap();
        self.events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::SetChainInfo) => Some(()),
                _ => None,
            })
            .await;
    }

    // Awaits until the accounts_data state matches `want`.
    pub async fn wait_for_accounts_data(&mut self, want: &HashSet<NormalAccountData>) {
        loop {
            let info = self.actix.addr.send(GetNetworkInfo).await.unwrap();
            let got: HashSet<_> = info.tier1_accounts.iter().map(|d| d.into()).collect();
            if &got == want {
                break;
            }
            // It is important that we wait for the next PeerMessage::SyncAccountsData to get
            // PROCESSED, not just RECEIVED. Otherwise we would get a race condition.
            self.events.recv_until(unwrap_sync_accounts_data_processed).await;
        }
    }
}

pub(crate) async fn start(
    clock: time::Clock,
    store: near_store::NodeStorage,
    cfg: config::NetworkConfig,
    chain: Arc<data::Chain>,
) -> ActorHandler {
    let (send, recv) = broadcast::unbounded_channel();
    let actix = ActixSystem::spawn({
        let mut cfg = cfg.clone();
        let chain = chain.clone();
        move || {
            let genesis_id = chain.genesis_id.clone();
            let fc = fake_client::start(send.sink().compose(Event::Client));
            cfg.event_sink = send.sink().compose(Event::PeerManager);
            PeerManagerActor::new(
                clock,
                store,
                cfg,
                fc.clone().recipient(),
                fc.clone().recipient(),
                genesis_id,
            )
            .unwrap()
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
