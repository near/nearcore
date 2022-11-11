use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, EDGE_MIN_TIMESTAMP_NONCE};
use crate::peer;
use crate::peer_manager;
use crate::tcp;
use crate::testonly::make_rng;
use crate::time;
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
    clock.set_auto_advance(time::Duration::ZERO);
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
        //(Some(0), false, "Nonce 0"),
        (None, true, "Nonce 1"),
    ];

    for test in test_cases {
        println!("Running test {:?}", test.2);
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
