use crate::network_protocol::testonly as data;
use crate::network_protocol::{Edge, Encoding, Ping, RoutedMessageBody, RoutingTableUpdate};
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::start as start_pm;
use crate::peer_manager::testonly::Event;
use crate::tcp;
use crate::testonly::make_rng;
use crate::time;
use crate::types::PeerMessage;
use near_o11y::testonly::init_test_logger;
use near_store::db::TestDB;
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
        force_encoding: Some(Encoding::Proto),
        nonce: None,
    };
    let stream = tcp::Stream::connect(&pm.peer_info()).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    peer.complete_handshake().await;
    // await for peer manager to compute the routing table.
    // TODO(gprusak): probably extract it to a separate function when migrating other tests from
    // integration-tests to near_network.
    pm.events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::RoutingTableUpdate { next_hops, .. }) => {
                if next_hops.get(&peer.cfg.id()).map_or(false, |v| v.len() > 0) {
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
        force_encoding: Some(Encoding::Proto),
        nonce: None,
    };
    let stream = tcp::Stream::connect(&pm.peer_info()).await.unwrap();
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

/// Awaits for SyncRoutingTable messages until all edges from `want` arrive.
/// Panics if any other edges arrive.
async fn wait_for_edges(peer: &mut peer::testonly::PeerHandle, want: &HashSet<Edge>) {
    let mut got = HashSet::new();
    while &got != want {
        match peer.events.recv().await {
            peer::testonly::Event::Network(PME::MessageProcessed(
                PeerMessage::SyncRoutingTable(msg),
            )) => {
                got.extend(msg.edges);
                assert!(want.is_superset(&got));
            }
            // Ignore other messages.
            _ => {}
        }
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

    let mut total_edges = HashSet::new();
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
            force_encoding: Some(Encoding::Proto),
            nonce: None,
        };
        let stream = tcp::Stream::connect(&pm.peer_info()).await.unwrap();
        let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
        let edge = peer.complete_handshake().await;

        // Receive the initial sync, which will consist just of the current edge:
        // - the disconnected edges from the previous iterations are not loaded yet.
        // - the local edges weren't stored at all.
        tracing::info!(target: "test", "wait_for_edges(<first edge>)");
        wait_for_edges(&mut peer, &[edge.clone()].into()).await;

        // Create a bunch of fresh unreachable edges, then send all the edges created so far.
        let fresh_edges: HashSet<_> = [
            data::make_edge(&data::make_signer(rng), &data::make_signer(rng)),
            data::make_edge(&data::make_signer(rng), &data::make_signer(rng)),
            data::make_edge_tombstone(&data::make_signer(rng), &data::make_signer(rng)),
        ]
        .into();
        total_edges.extend(fresh_edges.clone());
        // We capture the events starting here to record all the edge prunnings after the
        // SyncRoutingTable below is processed.
        let mut events = pm.events.from_now();
        peer.send(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
            edges: total_edges.iter().cloned().collect::<Vec<_>>(),
            accounts: vec![],
        }))
        .await;

        // Wait for the fresh edges to be broadcasted back.
        tracing::info!(target: "test", "wait_for_edges(<fresh edges>)");
        wait_for_edges(&mut peer, &fresh_edges).await;

        // Wait for all the disconnected edges created so far to be saved to storage.
        tracing::info!(target: "test", "wait for pruning");
        let mut pruned = HashSet::new();
        while pruned != total_edges {
            match events.recv().await {
                Event::PeerManager(PME::RoutingTableUpdate { pruned_edges, .. }) => {
                    pruned.extend(pruned_edges)
                }
                _ => {}
            }
        }
    }
}

#[tokio::test]
async fn square() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "connect 4 nodes in a square");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm3 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    pm0.connect_to(&pm1.peer_info()).await;
    pm1.connect_to(&pm2.peer_info()).await;
    pm2.connect_to(&pm3.peer_info()).await;
    pm3.connect_to(&pm0.peer_info()).await;
    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();
    let id3 = pm3.cfg.node_id();

    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id3.clone(), vec![id3.clone()]),
        (id2.clone(), vec![id1.clone(), id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test","stop {id1}");
    drop(pm1);
    tracing::info!(target:"test","wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id3.clone(), vec![id3.clone()]),
        (id2.clone(), vec![id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test","wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id3.clone(), vec![id3.clone()]),
        (id0.clone(), vec![id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test","wait for {id3} routing table");
    pm3.wait_for_routing_table(&[
        (id2.clone(), vec![id2.clone()]),
        (id0.clone(), vec![id0.clone()]),
    ])
    .await;
    drop(pm0);
    drop(pm2);
    drop(pm3);
}
