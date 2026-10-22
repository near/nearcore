use crate::broadcast;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{PeerInfo, PeersResponse};
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::testonly::ActorHandler;
use crate::tcp;
use crate::testonly::make_rng;
use crate::types::PeerMessage;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use near_store::db::TestDB;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

async fn my_public_addr(pm: &ActorHandler) -> Option<SocketAddr> {
    pm.with_state(|s| async move { *s.my_public_addr.read() }).await
}

/// PeersResponse in which a peer reports that it sees the node `id` at `addr`.
fn report(id: PeerId, addr: SocketAddr) -> PeerMessage {
    PeerMessage::PeersResponse(PeersResponse {
        peers: vec![],
        direct_peers: vec![PeerInfo { id, addr: Some(addr), account_id: None }],
    })
}

/// Waits until the peer manager processes a PeersResponse.
async fn wait_for_report(events: &mut broadcast::Receiver<Event>) {
    events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(tcp::Tier::T2, PeerMessage::PeersResponse(_)) => Some(()),
            _ => None,
        })
        .await
}

// Verify that a node takes its public address from the reports of its connected peers.
#[tokio::test]
async fn public_addr_follows_peer_reports() {
    init_test_logger();
    let mut rng = make_rng(921853250);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let pm = peer_manager::testonly::start(
        clock.clone(),
        TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let node_addr = **pm.cfg.node_addr.as_ref().unwrap();
    let mut events = pm.events.from_now();
    assert_eq!(my_public_addr(&pm).await, None);

    tracing::info!(target:"test", "connect a peer, which reports the address it dialed");
    let peer = pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    wait_for_report(&mut events).await;
    assert_eq!(my_public_addr(&pm).await, Some(node_addr));

    tracing::info!(target:"test", "a new report replaces the previous one");
    let ip: IpAddr = "203.0.113.1".parse().unwrap();
    peer.send(report(pm.cfg.node_id(), SocketAddr::new(ip, node_addr.port()))).await;
    wait_for_report(&mut events).await;
    assert_eq!(my_public_addr(&pm).await, Some(SocketAddr::new(ip, node_addr.port())));

    tracing::info!(target:"test", "the report stops counting once the peer disconnects");
    drop(peer);
    events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(_) => Some(()),
            _ => None,
        })
        .await;
    assert_eq!(my_public_addr(&pm).await, None);
}

// Verify that a node doesn't take a private IP reported by its peers as its public address,
// unless the config allows it.
#[tokio::test]
async fn public_addr_ignores_private_ips() {
    init_test_logger();
    let mut rng = make_rng(921853251);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let mut cfg = chain.make_config(rng);
    cfg.allow_private_ip_in_public_addrs = false;
    let pm = peer_manager::testonly::start(clock.clone(), TestDB::new(), cfg, chain.clone()).await;
    let mut events = pm.events.from_now();

    tracing::info!(target:"test", "connect a peer, which sees the node at a loopback address");
    let _peer =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    wait_for_report(&mut events).await;
    assert_eq!(my_public_addr(&pm).await, None);
}

// Verify that the addresses reported by peers don't replace a configured public address.
#[tokio::test]
async fn configured_public_addr_ignores_peer_reports() {
    init_test_logger();
    let mut rng = make_rng(921853252);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let mut cfg = chain.make_config(rng);
    let configured: SocketAddr = "198.51.100.1:24567".parse().unwrap();
    cfg.tier3_public_addr = Some(configured);
    let pm = peer_manager::testonly::start(clock.clone(), TestDB::new(), cfg, chain.clone()).await;
    let mut events = pm.events.from_now();

    tracing::info!(target:"test", "connect a peer, which reports the address it dialed");
    let _peer =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    wait_for_report(&mut events).await;
    assert_eq!(my_public_addr(&pm).await, Some(configured));
}
