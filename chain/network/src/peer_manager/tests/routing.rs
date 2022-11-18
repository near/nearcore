use crate::broadcast;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{Edge, Encoding, Ping, RoutedMessageBody, RoutingTableUpdate};
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::start as start_pm;
use crate::peer_manager::testonly::Event;
use crate::store;
use crate::tcp;
use crate::testonly::{make_rng, Rng};
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
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    peer.complete_handshake().await;
    pm.wait_for_routing_table(&mut clock, &[(peer.cfg.id(), vec![peer.cfg.id()])]).await;

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
                    peer::testonly::Event::Network(PME::MessageProcessed(
                        tcp::Tier::T2,
                        PeerMessage::Routed(msg),
                    )) => Some(msg),
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
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    peer.complete_handshake().await;

    let mut edges_got = HashSet::new();
    let mut edges_want = HashSet::new();
    let mut accounts_got = HashSet::new();
    let mut accounts_want = HashSet::new();
    edges_want.insert(peer.edge.clone().unwrap());

    // Gradually increment the amount of data in the system and then broadcast it.
    for i in 0..10 {
        tracing::info!(target: "test", "iteration {i}");
        // Wait for the new data to be broadcasted.
        // Note that in the first iteration we expect just 1 edge, without sending anything before.
        // It is important because the first SyncRoutingTable contains snapshot of all data known to
        // the node (not just the diff), so we expect incremental behavior only after the first
        // SyncRoutingTable.
        while edges_got != edges_want || accounts_got != accounts_want {
            match peer.events.recv().await {
                peer::testonly::Event::Network(PME::MessageProcessed(
                    tcp::Tier::T2,
                    PeerMessage::SyncRoutingTable(got),
                )) => {
                    for a in got.accounts {
                        assert!(!accounts_got.contains(&a), "repeated broadcast: {a:?}");
                        assert!(accounts_want.contains(&a), "unexpected broadcast: {a:?}");
                        accounts_got.insert(a);
                    }
                    for e in got.edges {
                        // TODO(gprusak): Currently there is a race condition between
                        // initial full sync and broadcasting the new connection edge,
                        // which may cause the new connection edge to be broadcasted twice.
                        // Synchronize those 2 events, so that behavior here is deterministic.
                        if &e != peer.edge.as_ref().unwrap() {
                            assert!(!edges_got.contains(&e), "repeated broadcast: {e:?}");
                        }
                        assert!(edges_want.contains(&e), "unexpected broadcast: {e:?}");
                        edges_got.insert(e);
                    }
                }
                // Ignore other messages.
                _ => {}
            }
        }
        // Add more data.
        let key = data::make_secret_key(rng);
        edges_want.insert(data::make_edge(&peer.cfg.network.node_key, &key, 1));
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
async fn wait_for_edges(
    mut events: broadcast::Receiver<peer::testonly::Event>,
    want: &HashSet<Edge>,
) {
    let mut got = HashSet::new();
    while &got != want {
        match events.recv().await {
            peer::testonly::Event::Network(PME::MessageProcessed(
                tcp::Tier::T2,
                PeerMessage::SyncRoutingTable(msg),
            )) => {
                got.extend(msg.edges);
                assert!(want.is_superset(&got), "want: {:#?}, got: {:#?}", want, got);
            }
            // Ignore other messages.
            _ => {}
        }
    }
}

// After each handshake a full sync of routing table is performed with the peer.
// After a restart, some edges reside in storage. The node shouldn't broadcast
// edges which it learned about before the restart.
#[tokio::test]
async fn no_edge_broadcast_after_restart() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let make_edges = |rng: &mut Rng| {
        vec![
            data::make_edge(&data::make_secret_key(rng), &data::make_secret_key(rng), 1),
            data::make_edge(&data::make_secret_key(rng), &data::make_secret_key(rng), 1),
            data::make_edge_tombstone(&data::make_secret_key(rng), &data::make_secret_key(rng)),
        ]
    };

    // Create a bunch of fresh unreachable edges, then send all the edges created so far.
    let stored_edges = make_edges(rng);

    // We are preparing the initial storage by hand (rather than simulating the restart),
    // because semantics of the RoutingTable protocol are very poorly defined, and it
    // is hard to write a solid test for it without literally assuming the implementation details.
    let store = near_store::db::TestDB::new();
    {
        let mut stored_peers = HashSet::new();
        for e in &stored_edges {
            stored_peers.insert(e.key().0.clone());
            stored_peers.insert(e.key().1.clone());
        }
        let mut store: store::Store = store.clone().into();
        store.push_component(&stored_peers, &stored_edges).unwrap();
    }

    // Start a PeerManager and connect a peer to it.
    let pm =
        peer_manager::testonly::start(clock.clock(), store, chain.make_config(rng), chain.clone())
            .await;
    let peer = pm
        .start_inbound(chain.clone(), chain.make_config(rng))
        .await
        .handshake(&clock.clock())
        .await;
    tracing::info!(target:"test","pm = {}",pm.cfg.node_id());
    tracing::info!(target:"test","peer = {}",peer.cfg.id());
    // Wait for the initial sync, which will contain just 1 edge.
    // Only incremental sync are guaranteed to not contain already known edges.
    wait_for_edges(peer.events.clone(), &[peer.edge.clone().unwrap()].into()).await;

    let fresh_edges = make_edges(rng);
    let mut total_edges = stored_edges.clone();
    total_edges.extend(fresh_edges.iter().cloned());
    let events = peer.events.from_now();
    peer.send(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
        edges: total_edges,
        accounts: vec![],
    }))
    .await;

    // Wait for the fresh edges to be broadcasted back.
    tracing::info!(target: "test", "wait_for_edges(<fresh edges>)");
    wait_for_edges(events, &fresh_edges.into_iter().collect()).await;
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
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;
    pm2.connect_to(&pm3.peer_info(), tcp::Tier::T2).await;
    pm3.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();
    let id3 = pm3.cfg.node_id();

    pm0.wait_for_routing_table(
        &mut clock,
        &[
            (id1.clone(), vec![id1.clone()]),
            (id3.clone(), vec![id3.clone()]),
            (id2.clone(), vec![id1.clone(), id3.clone()]),
        ],
    )
    .await;
    tracing::info!(target:"test","stop {id1}");
    drop(pm1);
    tracing::info!(target:"test","wait for {id0} routing table");
    pm0.wait_for_routing_table(
        &mut clock,
        &[(id3.clone(), vec![id3.clone()]), (id2.clone(), vec![id3.clone()])],
    )
    .await;
    tracing::info!(target:"test","wait for {id2} routing table");
    pm2.wait_for_routing_table(
        &mut clock,
        &[(id3.clone(), vec![id3.clone()]), (id0.clone(), vec![id3.clone()])],
    )
    .await;
    tracing::info!(target:"test","wait for {id3} routing table");
    pm3.wait_for_routing_table(
        &mut clock,
        &[(id2.clone(), vec![id2.clone()]), (id0.clone(), vec![id0.clone()])],
    )
    .await;
    drop(pm0);
    drop(pm2);
    drop(pm3);
}

#[tokio::test]
async fn fix_local_edges() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut pm =
        start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let conn = pm
        .start_inbound(chain.clone(), chain.make_config(rng))
        .await
        .handshake(&clock.clock())
        .await;
    // TODO(gprusak): the case when the edge is updated via UpdateNondeRequest is not covered yet,
    // as it requires awaiting for the RPC roundtrip which is not implemented yet.
    let edge1 = data::make_edge(&pm.cfg.node_key, &data::make_secret_key(rng), 1);
    let edge2 = conn
        .edge
        .as_ref()
        .unwrap()
        .remove_edge(conn.cfg.network.node_id(), &conn.cfg.network.node_key);
    let msg = PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_edges(vec![
        edge1.clone(),
        edge2.clone(),
    ]));

    tracing::info!(target:"test", "waiting for fake edges to be processed");
    conn.send(msg).await;
    let mut got = HashSet::new();
    let want = [&edge1, &edge2].into_iter().cloned().collect();
    while !got.is_superset(&want) {
        pm.events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::EdgesVerified(edges)) => Some(got.extend(edges)),
                _ => None,
            })
            .await;
    }

    tracing::info!(target:"test","waiting for fake edges to be fixed");
    pm.fix_local_edges(&clock.clock(), time::Duration::ZERO).await;
    // TODO(gprusak): make fix_local_edges await closing of the connections, so
    // that we don't have to wait for it explicitly here.
    pm.events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::ConnectionClosed { .. }) => Some(()),
            _ => None,
        })
        .await;

    tracing::info!(target:"test","checking the consistency");
    pm.check_consistency().await;
    drop(conn);
}

#[tokio::test]
async fn do_not_block_announce_account_broadcast() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let db0 = TestDB::new();
    let db1 = TestDB::new();
    let aa = data::make_announce_account(rng);

    tracing::info!(target:"test", "spawn 2 nodes and announce the account.");
    let pm0 = start_pm(clock.clock(), db0.clone(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), db1.clone(), chain.make_config(rng), chain.clone()).await;
    pm1.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    pm1.announce_account(aa.clone()).await;
    assert_eq!(&aa.peer_id, &pm0.wait_for_account_owner(&aa.account_id).await);
    drop(pm0);
    drop(pm1);

    tracing::info!(target:"test", "spawn 3 nodes and re-announce the account.");
    // Even though the account was previously announced (pm0 and pm1 have it in DB),
    // the nodes should allow to let the broadcast through.
    let pm0 = start_pm(clock.clock(), db0, chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), db1, chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    pm1.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    pm2.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    pm1.announce_account(aa.clone()).await;
    assert_eq!(&aa.peer_id, &pm2.wait_for_account_owner(&aa.account_id).await);
}
