use crate::broadcast;
use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    EdgeState, Encoding, PeerAddr, PeerInfo, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer;
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::network_state::NetworkState;
//use crate::peer_manager::peer_manager_actor;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::test_utils;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::time;
use crate::types::{
    ChainInfo, KnownPeerStatus, NetworkRequests, PeerManagerMessageRequest, SetChainInfo,
};
use crate::PeerManagerActor;
use near_o11y::WithSpanContextExt;
use near_primitives::network::{AnnounceAccount, PeerId};
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
        let stream = tcp::Stream::connect(peer_info).await.unwrap();
        let mut events = self.events.from_now();
        let stream_id = stream.id();
        self.actix
            .addr
            .do_send(PeerManagerMessageRequest::OutboundTcpConnect(stream).with_span_context());
        events
            .recv_until(|ev| match &ev {
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    panic!("PeerManager rejected the handshake")
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
        let stream = socket.connect(&self.peer_info()).await;
        let stream_id = stream.id();
        let conn = RawConnection {
            events,
            stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
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
    ) -> RawConnection {
        let (outbound_stream, inbound_stream) = tcp::Stream::loopback(network_cfg.node_id()).await;
        let stream_id = outbound_stream.id();
        let events = self.events.from_now();
        self.actix.addr.do_send(
            PeerManagerMessageRequest::OutboundTcpConnect(outbound_stream).with_span_context(),
        );
        let conn = RawConnection {
            events,
            stream: inbound_stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
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

    /// Checks internal consistency of the PeerManagerActor.
    /// This is a partial implementation, add more invariant checks
    /// if needed.
    pub async fn check_consistency(&self) {
        self.with_state(|s| async move {
            // Check that the set of ready connections matches the PeerStore state.
            let tier2: HashSet<_> = s.tier2.load().ready.keys().cloned().collect();
            let store: HashSet<_> = s
                .peer_store
                .dump()
                .into_iter()
                .filter_map(|state| {
                    if state.status == KnownPeerStatus::Connected {
                        Some(state.peer_info.id)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(tier2, store);

            // Check that the local_edges of the graph match the TIER2 connection pool.
            let node_id = s.config.node_id();
            let local_edges: HashSet<_> = s
                .routing_table_view
                .get_local_edges()
                .iter()
                .filter_map(|e| match e.edge_type() {
                    EdgeState::Active => Some(e.other(&node_id).unwrap().clone()),
                    EdgeState::Removed => None,
                })
                .collect();
            assert_eq!(tier2, local_edges);
        })
        .await
    }

    pub async fn fix_local_edges(&self, clock: &time::Clock, timeout: time::Duration) {
        let clock = clock.clone();
        self.with_state(move |s| async move { s.fix_local_edges(&clock, timeout).await }).await
    }

    pub async fn set_chain_info(&mut self, chain_info: ChainInfo) {
        self.actix.addr.send(SetChainInfo(chain_info).with_span_context()).await.unwrap();
        self.events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::SetChainInfo) => Some(()),
                _ => None,
            })
            .await;
    }

    pub async fn announce_account(&self, aa: AnnounceAccount) {
        self.actix
            .addr
            .send(
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::AnnounceAccount(aa))
                    .with_span_context(),
            )
            .await
            .unwrap();
    }

    // Awaits until the accounts_data state matches `want`.
    pub async fn wait_for_accounts_data(&mut self, want: &HashSet<NormalAccountData>) {
        loop {
            let got: HashSet<_> = self
                .with_state(|s| async move {
                    s.accounts_data.load().data.values().map(|d| d.into()).collect()
                })
                .await;
            if &got == want {
                break;
            }
            // It is important that we wait for the next PeerMessage::SyncAccountsData to get
            // PROCESSED, not just RECEIVED. Otherwise we would get a race condition.
            self.events.recv_until(unwrap_sync_accounts_data_processed).await;
        }
    }

    // Awaits until the routing_table matches `want`.
    pub async fn wait_for_routing_table(
        &self,
        clock: &mut time::FakeClock,
        want: &[(PeerId, Vec<PeerId>)],
    ) {
        let mut events = self.events.from_now();
        loop {
            let got =
                self.with_state(|s| async move { s.routing_table_view.info().next_hops }).await;
            if test_utils::expected_routing_tables(&got, want) {
                return;
            }
            events
                .recv_until(|ev| match ev {
                    // Event::PeerManager(PME::RoutingTableUpdate { .. }) => Some(()),
                    Event::PeerManager(PME::MessageProcessed(PeerMessage::SyncRoutingTable {
                        ..
                    })) => Some(()),
                    _ => None,
                })
                .await;
        }
    }

    pub async fn wait_for_account_owner(&self, account: &AccountId) -> PeerId {
        let mut events = self.events.from_now();
        loop {
            let account = account.clone();
            let got = self
                .with_state(|s| async move { s.routing_table_view.account_owner(&account).clone() })
                .await;
            if let Some(got) = got {
                return got;
            }
            events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::AccountsAdded(_)) => Some(()),
                    _ => None,
                })
                .await;
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
            let fc = Arc::new(fake_client::Fake { event_sink: send.sink().compose(Event::Client) });
            cfg.event_sink = send.sink().compose(Event::PeerManager);
            PeerManagerActor::spawn(clock, store, cfg, fc, genesis_id).unwrap()
        }
    })
    .await;
    let mut h = ActorHandler { cfg, actix, events: recv };
    // Wait for the server to start.
    assert_eq!(Event::PeerManager(PME::ServerStarted), h.events.recv().await);
    h.actix.addr.send(SetChainInfo(chain.get_chain_info()).with_span_context()).await.unwrap();
    h
}
