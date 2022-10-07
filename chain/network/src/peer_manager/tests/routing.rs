use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, Ping, RoutedMessageBody, RoutingTableUpdate};
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::Event;
use crate::tcp;
use crate::testonly::{assert_is_superset, make_rng, AsSet as _};
use crate::time;
use crate::types::PeerMessage;
use near_o11y::testonly::init_test_logger;
use pretty_assertions::assert_eq;
use rand::Rng as _;
use std::collections::HashSet;
use std::sync::Arc;

// test that TTL is handled property.
#[tokio::test]
async fn ttl() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let mut pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let cfg = peer::testonly::PeerConfig {
        network: chain.make_config(rng),
        chain,
        peers: vec![],
        force_encoding: Some(Encoding::Proto),
        nonce: None,
    };
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    peer.complete_handshake().await;
    // await for peer manager to compute the routing table.
    // TODO(gprusak): probably extract it to a separate function when migrating other tests from
    // integration-tests to crate.
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
        let msg = Box::new(peer.routed_message(msg, peer.cfg.id(), ttl, Some(clock.now_utc())));
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
                    peer::testonly::Event::Network(PME::MessageProcessed(PeerMessage::Routed(
                        msg,
                    ))) => Some(msg),
                    _ => None,
                })
                .await;
            assert_eq!(msg.body, got.body);
            assert_eq!(msg.ttl - 1, got.ttl);
        }
    }
}

// After the initial exchange, all subsequent SyncRoutingTable messages are
// expected to contain only the diff of the known data.
#[tokio::test]
async fn repeated_data_in_sync_routing_table() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let cfg = peer::testonly::PeerConfig {
        network: chain.make_config(rng),
        chain,
        peers: vec![],
        force_encoding: Some(Encoding::Proto),
        nonce: None,
    };
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
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
                peer::testonly::Event::Network(PME::MessageProcessed(
                    PeerMessage::SyncRoutingTable(got),
                )) => {
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
        edges_want.insert(data::make_edge(&peer.cfg.signer(), &signer));
        accounts_want.insert(data::make_announce_account(rng));
        // Send all the data created so far. PeerManager is expected to discard the duplicates.
        peer.send(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
            edges: edges_want.iter().cloned().collect(),
            accounts: accounts_want.iter().cloned().collect(),
        }))
        .await;
    }
}

// After each handshake a full sync of routing table is performed with the peer.
// After a restart, all the edges reside in storage. The node shouldn't broadcast
// edges which it learned about before the restart.
// This test takes ~6s because of delays enforced in the PeerManager.
#[tokio::test]
async fn no_edge_broadcast_after_restart() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut total_edges = vec![];
    let store = near_store::db::TestDB::new();

    for i in 0..3 {
        println!("iteration {i}");
        // Start a PeerManager and connect a peer to it.
        let pm = peer_manager::testonly::start(
            clock.clock(),
            store.clone(),
            chain.make_config(rng),
            chain.clone(),
        )
        .await;
        let cfg = peer::testonly::PeerConfig {
            network: chain.make_config(rng),
            chain: chain.clone(),
            peers: vec![],
            force_encoding: Some(Encoding::Proto),
            nonce: None,
        };
        let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
        let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
        let edge = peer.complete_handshake().await;

        // Create a bunch of fresh unreachable edges, then send all the edges created so far.
        let fresh_edges = vec![
            data::make_edge(&data::make_signer(rng), &data::make_signer(rng)),
            data::make_edge(&data::make_signer(rng), &data::make_signer(rng)),
            data::make_edge_tombstone(&data::make_signer(rng), &data::make_signer(rng)),
        ];
        total_edges.extend(fresh_edges.clone());
        peer.send(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
            edges: total_edges.clone(),
            accounts: vec![],
        }))
        .await;

        // Expect just the fresh edges (and the pm <-> peer edge) to be broadcasted back.
        let mut edges_want = fresh_edges.clone();
        edges_want.push(edge);
        let mut edges_got = vec![];

        while edges_got != edges_want {
            match peer.events.recv().await {
                peer::testonly::Event::Network(PME::MessageProcessed(
                    PeerMessage::SyncRoutingTable(got),
                )) => {
                    edges_got.extend(got.edges);
                    assert_is_superset(&edges_want.as_set(), &edges_got.as_set());
                }
                // Ignore other messages.
                _ => {}
            }
        }
    }
}
