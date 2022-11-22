use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, EDGE_MIN_TIMESTAMP_NONCE};
use crate::peer;
use crate::peer_manager::testonly::{ActorHandler, Event};
use crate::peer_manager::{self, peer_manager_actor};
use crate::tcp;
use crate::testonly::make_rng;
use crate::time;
use crate::types::Edge;
use ::time::Duration;
use near_o11y::testonly::init_test_logger;
use std::sync::Arc;

// Nonces must be odd (as even ones are reserved for tombstones).
fn to_active_nonce(timestamp: time::Utc) -> u64 {
    let value = timestamp.unix_timestamp() as u64;
    if value % 2 == 0 {
        value + 1
    } else {
        value
    }
}

// Test connecting to peer manager with timestamp-like nonces.
#[tokio::test]
async fn test_nonces() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::new(*EDGE_MIN_TIMESTAMP_NONCE + time::Duration::days(2));
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    // Start a PeerManager and connect a peer to it.
    let pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    let test_cases = [
        // Try to connect with peer with a valid nonce (current timestamp).
        (Some(to_active_nonce(clock.now_utc())), true, "current timestamp"),
        // Now try the peer with invalid timestamp (in the past)
        (Some(to_active_nonce(clock.now_utc() - time::Duration::days(1))), false, "past timestamp"),
        // Now try the peer with invalid timestamp (in the future)
        (
            Some(to_active_nonce(clock.now_utc() + time::Duration::days(1))),
            false,
            "future timestamp",
        ),
        (Some(u64::MAX), false, "u64 max"),
        (Some(i64::MAX as u64), false, "i64 max"),
        (Some((i64::MAX - 1) as u64), false, "i64 max - 1"),
        (Some(253402300799), false, "Max time"),
        (Some(253402300799 + 2), false, "Over max time"),
        (Some(0), false, "Nonce 0"),
        (None, true, "Nonce 1"),
    ];

    for test in test_cases {
        tracing::info!(target: "test", "Running test {:?}", test.2);
        let cfg = peer::testonly::PeerConfig {
            network: chain.make_config(rng),
            chain: chain.clone(),
            force_encoding: Some(Encoding::Proto),
            // Connect with nonce equal to unix timestamp
            nonce: test.0,
        };
        let stream = tcp::Stream::connect(&pm.peer_info()).await.unwrap();
        let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
        if test.1 {
            peer.complete_handshake().await;
        } else {
            peer.fail_handshake().await;
        }
    }
}

async fn wait_for_edge(actor_handler: &mut ActorHandler) -> Edge {
    actor_handler
        .events
        .recv_until(|ev| match ev {
            Event::PeerManager(peer_manager_actor::Event::EdgesVerified(ev)) => Some(ev[0].clone()),
            _ => None,
        })
        .await
}

#[tokio::test]
/// Create 2 peer managers, that connect to each other.
/// Verify that the will refresh their nonce after some time.
async fn test_nonce_refresh() {
    init_test_logger();
    let mut rng = make_rng(921853255);
    let rng = &mut rng;
    let mut clock = time::FakeClock::new(*EDGE_MIN_TIMESTAMP_NONCE + time::Duration::days(2));
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    // Start a PeerManager.
    let pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    let mut other_config = chain.make_config(rng);
    // Make sure that other peer manager has the first one as the boot node, and that it is ready to connect to it.
    other_config.peer_store.boot_nodes.push(pm.peer_info());
    other_config.outbound_disabled = false;

    // Start another peer manager.
    let mut pm2 = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        other_config,
        chain.clone(),
    )
    .await;

    let edge = wait_for_edge(&mut pm2).await;
    let start_time = clock.now_utc();
    // First edge between them should have the nonce equal to the current time.
    assert_eq!(Edge::nonce_to_utc(edge.nonce()).unwrap().unwrap(), start_time);

    // Advance a clock by 1h - and check that nonce was updated.
    clock.advance(Duration::HOUR);
    let edge = wait_for_edge(&mut pm2).await;
    assert_eq!(Edge::nonce_to_utc(edge.nonce()).unwrap().unwrap(), clock.now_utc());
}
