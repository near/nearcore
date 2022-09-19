use crate::broadcast;
use crate::config;
use crate::peer_manager::connection;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, PeerAddr, PeerInfo, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::time;
use crate::types::OutboundTcpConnect;
use crate::types::{ChainInfo, GetNetworkInfo, PeerManagerMessageRequest, SetChainInfo};
use crate::PeerManagerActor;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, EpochId};
use std::collections::HashSet;
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;

#[derive(actix::Message)]
#[rtype("()")]
struct WithNetworkState(Box<dyn Send + FnOnce(Arc<NetworkState>) -> Pin<Box<dyn Send + 'static + Future<Output=()>>>>);

impl actix::Handler<WithNetworkState> for PeerManagerActor {
    type Result = ();
    fn handle(&mut self, WithNetworkState(f): WithNetworkState, _: &mut Self::Context) -> Self::Result {
        assert!(actix::Arbiter::current().spawn(f(self.state.clone())));
    }
}

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
    events: broadcast::Receiver<Event>,
    stream: tokio::net::TcpStream,
    cfg: peer::testonly::PeerConfig,
}

impl RawConnection {
    pub async fn handshake(mut self, clock: &time::Clock) -> peer::testonly::PeerHandle {
        let node_id = self.cfg.network.node_id();
        let mut peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;

        // Wait for the new peer to complete the handshake.
        peer.complete_handshake().await;

        // Wait for the peer manager to complete the handshake.
        self.events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::PeerRegistered(info,connection::Tier::T2)) if node_id == info.id => Some(()),
                _ => None,
            })
            .await;
        peer
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

    pub async fn connect_to(&self, peer_info: &PeerInfo) {
        let mut events = self.events.from_now();
        self.actix
            .addr
            .send(PeerManagerMessageRequest::OutboundTcpConnect(OutboundTcpConnect(
                peer_info.clone(),
            )))
            .await
            .unwrap();
        events
            .recv_until(|ev| match &ev {
                Event::PeerManager(PME::PeerRegistered(info,connection::Tier::T2)) if peer_info == info => Some(()),
                _ => None,
            })
            .await;
    }

    pub async fn with_state<R:'static+Send,Fut:'static+Send+Future<Output=R>>(&self,f:impl 'static + Send + FnOnce(Arc<NetworkState>) -> Fut) -> R
    {
        let (send,recv) = tokio::sync::oneshot::channel();
        self.actix.addr.send(WithNetworkState(Box::new(|s| Box::pin(async {
            send.send(f(s).await).ok().unwrap()
        })))).await.unwrap();
        recv.await.unwrap()
    }

    pub async fn start_inbound(
        &self,
        chain: Arc<data::Chain>,
        network_cfg: config::NetworkConfig,
    ) -> RawConnection {
        // To avoid race condition:
        // 1. reserve a TCP port
        // 2. snapshot event stream
        // 3. establish connection.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let local_addr = socket.local_addr().unwrap();
        let events = self.events.from_now();
        let conn = RawConnection {
            events,
            stream: socket.connect(self.cfg.node_addr.unwrap()).await.unwrap(),
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                peers: vec![],
                start_handshake_with: Some(PeerId::new(self.cfg.node_key.public_key())),
                force_encoding: Some(Encoding::Proto),
                nonce: None,
            },
        };
        // Wait until the TCP connection is accepted or rejected.
        // The Handshake is not performed yet.
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::PeerActorStarted(addr))
                | Event::PeerManager(PME::ConnectionClosed(addr))
                    if addr == local_addr =>
                {
                    Some(())
                }
                _ => None,
            })
            .await;
        tracing::debug!("PHASE handshake started");
        conn
    }

    pub async fn start_outbound(
        &self,
        chain: Arc<data::Chain>,
        network_cfg: config::NetworkConfig,
    ) -> RawConnection {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let events = self.events.from_now();
        let peer_info = PeerInfo {
            id: network_cfg.node_id(),
            addr: Some(listener.local_addr().unwrap()),
            account_id: None,
        };
        self.actix.addr.do_send(PeerManagerMessageRequest::OutboundTcpConnect(OutboundTcpConnect(
            peer_info.clone(),
        )));
        let (stream, _) = listener.accept().await.unwrap();
        let conn = RawConnection {
            events,
            stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                peers: vec![],
                start_handshake_with: None,
                force_encoding: Some(Encoding::Proto),
                nonce: None,
            },
        };
        // Wait until the handshake started or connection is closed.
        // The Handshake is not performed yet.
        let local_addr = listener.local_addr().unwrap();
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::PeerActorStarted(addr))
                | Event::PeerManager(PME::ConnectionClosed(addr))
                    if addr == local_addr =>
                {
                    Some(())
                }
                _ => None,
            })
            .await;
        conn
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
    store: Arc<dyn near_store::db::Database>,
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
            PeerManagerActor::spawn(
                clock,
                store,
                cfg,
                fc.clone().recipient(),
                fc.clone().recipient(),
                genesis_id,
            )
            .unwrap()
        }
    })
    .await;
    let mut h = ActorHandler { cfg, actix, events: recv };
    // Wait for the server to start.
    assert_eq!(Event::PeerManager(PME::ServerStarted), h.events.recv().await);
    h.actix.addr.send(SetChainInfo(chain.get_chain_info())).await.unwrap();
    h
}
