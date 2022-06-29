use crate::network_protocol::testonly as data;
use crate::network_protocol::Encoding;
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::Event;
use crate::testonly::make_rng;
use crate::types::{PeerMessage, RoutingTableUpdate};
use near_logger_utils::init_test_logger;
use near_network_primitives::time;
use near_network_primitives::types::NetworkConfig;
use near_network_primitives::types::{Ping, RoutedMessageBody};
use near_primitives::network::PeerId;
use rand::Rng as _;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::net::TcpStream;

// After the initial exchange, all subsequent SyncRoutingTable messages are
// expected to contain only the diff of the known data.
#[tokio::test]
async fn repeated_data_in_sync_routing_table() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let port = crate::test_utils::open_port();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm =
        peer_manager::testonly::start(chain.clone(), NetworkConfig::from_seed("test1", port)).await;
    let cfg = peer::testonly::PeerConfig {
        signer: data::make_signer(rng),
        chain,
        peers: vec![],
        start_handshake_with: Some(PeerId::new(pm.cfg.node_key.public_key())),
        force_encoding: Some(Encoding::Proto),
    };
    let stream = TcpStream::connect(pm.cfg.node_addr.unwrap()).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    let edge = peer.complete_handshake().await;

    let mut edges_got = HashSet::new();
    let mut edges_want = HashSet::new();
    let mut accounts_got = HashSet::new();
    let mut accounts_want = HashSet::new();
    edges_want.insert(edge);

    // Gradually increment the amount of data in the system and then broadcast it.
    for _ in 0..10 {
        // Wait for the new data to be broadcasted.
        // Note that in the first iteration we expect just 1 edge, without sending anything before.
        // It is important because the first SyncRoutingTable contains snapshot of all data known to
        // the node (not just the diff), so we expect incremental behavior only after the first
        // SyncRoutingTable.
        // TODO(gprusak): the first SyncRoutingTable will be delayed, until we replace actix
        // internal clock with a fake clock.
        while edges_got != edges_want || accounts_got != accounts_want {
            match peer.events.recv().await {
                peer::testonly::Event::RoutingTable(got) => {
                    for a in got.accounts {
                        assert!(!accounts_got.contains(&a), "repeated broadcast: {a:?}");
                        assert!(accounts_want.contains(&a), "unexpected broadcast: {a:?}");
                        accounts_got.insert(a);
                    }
                    for e in got.edges {
                        assert!(!edges_got.contains(&e), "repeated broadcast: {e:?}");
                        assert!(edges_want.contains(&e), "unexpected broadcast: {e:?}");
                        edges_got.insert(e);
                    }
                }
                // Ignore other messages.
                _ => {}
            }
        }
        // Add more data.
        let signer = data::make_signer(rng);
        edges_want.insert(data::make_edge(&peer.cfg.signer, &signer));
        accounts_want.insert(data::make_announce_account(rng));
        // Send all the data created so far. PeerManager is expected to discard the duplicates.
        peer.send(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
            edges: edges_want.iter().cloned().collect(),
            accounts: accounts_want.iter().cloned().collect(),
            // TODO(gprusak): once implemented, test validator broadcasting as well.
            validators: vec![],
        }))
        .await;
    }
}

// test that TTL is handled property.
#[tokio::test]
async fn ttl() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let port = crate::test_utils::open_port();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let mut pm =
        peer_manager::testonly::start(chain.clone(), NetworkConfig::from_seed("test1", port)).await;
    let cfg = peer::testonly::PeerConfig {
        signer: data::make_signer(rng),
        chain,
        peers: vec![],
        start_handshake_with: Some(PeerId::new(pm.cfg.node_key.public_key())),
        force_encoding: Some(Encoding::Proto),
    };
    let stream = TcpStream::connect(pm.cfg.node_addr.unwrap()).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    peer.complete_handshake().await;
    // await for peer manager to compute the routing table.
    // TODO(gprusak): probably extract it to a separate function when migrating other tests from
    // integration-tests to near_network.
    pm.events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::RoutingTableUpdate(rt)) => {
                if rt.get(&peer.cfg.id()).map_or(false, |v| v.len() > 0) {
                    Some(())
                } else {
                    None
                }
            }
            _ => None,
        })
        .await;

    for ttl in 0..5 {
        let msg = RoutedMessageBody::Ping(Ping { nonce: rng.gen(), source: peer.cfg.id() });
        let msg = peer.routed_message(msg, peer.cfg.id(), ttl, Some(clock.now_utc()));
        peer.send(PeerMessage::Routed(msg.clone())).await;
        // If TTL is <2, then the message will be dropped (at least 2 hops are required).
        if ttl < 2 {
            pm.events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::RoutedMessageDropped) => Some(()),
                    _ => None,
                })
                .await;
        } else {
            let got = peer
                .events
                .recv_until(|ev| match ev {
                    peer::testonly::Event::Routed(msg) => Some(msg),
                    _ => None,
                })
                .await;
            assert_eq!(msg.body, got.body);
            assert_eq!(msg.ttl - 1, got.ttl);
        }
    }
}
