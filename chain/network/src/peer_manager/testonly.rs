use crate::broadcast;
use crate::client;
use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, PeerAddr, PeerInfo, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer;
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::time;
use crate::types::{
    ChainInfo, GetNetworkInfo, KnownPeerStatus, PeerManagerMessageRequest, SetChainInfo,
};
use crate::PeerManagerActor;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, EpochId};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(actix::Message)]
#[rtype("()")]
struct WithNetworkState(
    Box<dyn Send + FnOnce(Arc<NetworkState>) -> Pin<Box<dyn Send + 'static + Future<Output = ()>>>>,
);

impl actix::Handler<WithNetworkState> for PeerManagerActor {
    type Result = ();
    fn handle(
        &mut self,
        WithNetworkState(f): WithNetworkState,
        _: &mut Self::Context,
    ) -> Self::Result {
        assert!(actix::Arbiter::current().spawn(f(self.state.clone())));
    }
}

#[derive(actix::Message, Debug)]
#[rtype("()")]
struct CheckConsistency;

impl actix::Handler<CheckConsistency> for PeerManagerActor {
    type Result = ();
    /// Checks internal consistency of the PeerManagerActor.
    /// This is a partial implementation, add more invariant checks
    /// if needed.
    fn handle(&mut self, _: CheckConsistency, _: &mut actix::Context<Self>) {
        // Check that the set of ready connections matches the PeerStore state.
        let tier2: HashSet<_> = self.state.tier2.load().ready.keys().cloned().collect();
        let store: HashSet<_> = self.state
            .peer_store.dump().into_iter()
            .filter_map(|state| {
                if state.status == KnownPeerStatus::Connected {
                    Some(state.peer_info.id)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(tier2, store);
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
        Event::PeerManager(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncAccountsData(msg),
        )) => Some(msg),
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
    stream: tcp::Stream,
    cfg: peer::testonly::PeerConfig,
}

impl RawConnection {
    pub async fn handshake(mut self, clock: &time::Clock) -> peer::testonly::PeerHandle {
        let stream_id = self.stream.id();
        let mut peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;

        // Wait for the new peer to complete the handshake.
        peer.complete_handshake().await;

        // Wait for the peer manager to complete the handshake.
        self.events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    panic!("handshake aborted: {}", ev.reason)
                }
                _ => None,
            })
            .await;
        peer
    }

    // Try to perform a handshake. PeerManager is expected to reject the handshake.
    pub async fn manager_fail_handshake(mut self, clock: &time::Clock) -> ClosingReason {
        let stream_id = self.stream.id();
        let peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;
        let reason = self
            .events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(ev.reason)
                }
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    panic!("PeerManager accepted the handshake")
                }
                _ => None,
            })
            .await;
        drop(peer);
        reason
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
        let stream = tcp::Stream::connect(peer_info, tcp::Tier::T2).await.unwrap();
        let mut events = self.events.from_now();
        let stream_id = stream.id();
        self.actix.addr.do_send(PeerManagerMessageRequest::OutboundTcpConnect(stream));
        events
            .recv_until(|ev| match &ev {
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    panic!("PeerManager accepted the handshake")
                }
                _ => None,
            })
            .await;
    }

    pub async fn with_state<R: 'static + Send, Fut: 'static + Send + Future<Output = R>>(
        &self,
        f: impl 'static + Send + FnOnce(Arc<NetworkState>) -> Fut,
    ) -> R {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.actix
            .addr
            .send(WithNetworkState(Box::new(|s| {
                Box::pin(async { send.send(f(s).await).ok().unwrap() })
            })))
            .await
            .unwrap();
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
        let socket = tcp::Socket::bind_v4();
        let events = self.events.from_now();
        let stream = socket.connect(&self.peer_info(), tcp::Tier::T2).await;
        let stream_id = stream.id();
        let conn = RawConnection {
            events,
            stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                peers: vec![],
                force_encoding: Some(Encoding::Proto),
                nonce: None,
            },
        };
        // Wait until the TCP connection is accepted or rejected.
        // The Handshake is not performed yet.
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeStarted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                _ => None,
            })
            .await;
        conn
    }

    pub async fn start_outbound(
        &self,
        chain: Arc<data::Chain>,
        network_cfg: config::NetworkConfig,
        tier: tcp::Tier,
    ) -> RawConnection {
        let (outbound_stream, inbound_stream) =
            tcp::Stream::loopback(network_cfg.node_id(), tier).await;
        let stream_id = outbound_stream.id();
        let events = self.events.from_now();
        self.actix.addr.do_send(PeerManagerMessageRequest::OutboundTcpConnect(outbound_stream));
        let conn = RawConnection {
            events,
            stream: inbound_stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                peers: vec![],
                force_encoding: Some(Encoding::Proto),
                nonce: None,
            },
        };
        // Wait until the handshake started or connection is closed.
        // The Handshake is not performed yet.
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeStarted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                _ => None,
            })
            .await;
        conn
    }

    pub async fn check_consistency(&self) {
        self.actix.addr.send(CheckConsistency).await.unwrap();
    }

    pub async fn set_chain_info(&self, chain_info: ChainInfo) {
        self.actix.addr.send(SetChainInfo(chain_info)).await.unwrap();
    }

    pub async fn tier1_connect_to_proxies(&self, clock: &time::Clock) {
        let mut events = self.events.from_now();
        let clock = clock.clone();
        self.with_state(move |s| async move {
            if let Some(vc) = s.tier1_validator_config(&s.accounts_data.load()) {
                tracing::debug!(target:"test","tier1_connect_to_proxies");
                s.tier1_connect_to_proxies(&clock).await;
                tracing::debug!(target:"test","tier1_connect_to_proxies DONE");
                let want = match &vc.endpoints {
                    config::ValidatorEndpoints::TrustedStunServers(_) => HashSet::<_>::default(),
                    config::ValidatorEndpoints::PublicAddrs(proxies) => {
                        proxies.iter().map(|p| &p.peer_id).collect()
                    }
                };
                tracing::debug!(target:"test","want = {want:?}");
                loop {
                    if want.is_subset(&s.tier1.load().ready.keys().collect()) {
                        break;
                    }
                    events
                        .recv_until(|ev| match ev {
                            Event::PeerManager(PME::HandshakeCompleted(ev))
                                if ev.tier == tcp::Tier::T1 =>
                            {
                                Some(())
                            }
                            Event::PeerManager(PME::ConnectionClosed(ev)) => {
                                tracing::debug!(target:"test","connection closed: {:?}",ev.reason);
                                Some(())
                            }
                            _ => None,
                        })
                        .await;
                }
                s.tier1_broadcast_proxies(&clock).await;
            }
        })
        .await;
    }

    // Awaits until the accounts_data state matches `want`.
    pub async fn wait_for_accounts_data(&self, want: &HashSet<NormalAccountData>) {
        let mut events = self.events.from_now();
        loop {
            let info = self.actix.addr.send(GetNetworkInfo).await.unwrap();
            let got: HashSet<_> = info.tier1_accounts.iter().map(|d| d.into()).collect();
            if &got == want {
                break;
            }
            // It is important that we wait for the next PeerMessage::SyncAccountsData to get
            // PROCESSED, not just RECEIVED. Otherwise we would get a race condition.
            events.recv_until(unwrap_sync_accounts_data_processed).await;
        }
    }

    pub async fn establish_tier1_connections(&self, clock: &time::Clock) {
        let mut events = self.events.from_now();
        let my_account_id = match &self.cfg.validator {
            Some(v) => v.signer.validator_id().clone(),
            None => return,
        };
        let clock = clock.clone();
        self.with_state(move |s| async move {
            // Start the connections.
            s.tier1_connect_to_others(&clock).await;
            let ad = s.accounts_data.load();
            // Wait for all the connections to be established.
            loop {
                if ad.keys.keys().all(|(_, account_id)| {
                    account_id == &my_account_id || s.get_tier1_proxy(account_id).is_some()
                }) {
                    break;
                }
                events
                    .recv_until(|ev| match ev {
                        Event::PeerManager(PME::HandshakeCompleted(ev))
                            if ev.tier == tcp::Tier::T1 =>
                        {
                            Some(())
                        }
                        _ => None,
                    })
                    .await;
            }
        })
        .await;
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
                client::Client {
                    client_addr: fc.clone().recipient(),
                    view_client_addr: fc.clone().recipient(),
                },
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
