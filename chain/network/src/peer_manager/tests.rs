use crate::concurrency::demux;
use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, PeerAddr, SyncAccountsData};
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::{Event, NormalAccountData};
use crate::testonly::{assert_is_superset, make_rng, AsSet as _, Rng};
use crate::types::{PeerMessage, RoutingTableUpdate};
use itertools::Itertools;
use near_logger_utils::init_test_logger;
use near_network_primitives::time;
use near_network_primitives::types::{Ping, RoutedMessageBody, EDGE_MIN_TIMESTAMP_NONCE};
use near_primitives::network::PeerId;
use near_store::test_utils::create_test_store;
use pretty_assertions::assert_eq;
use rand::seq::SliceRandom as _;
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
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm = peer_manager::testonly::start(
        clock.clock(),
        create_test_store(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let cfg = peer::testonly::PeerConfig {
        network: chain.make_config(rng),
        chain,
        peers: vec![],
        start_handshake_with: Some(PeerId::new(pm.cfg.node_key.public_key())),
        force_encoding: Some(Encoding::Proto),
        nonce: None,
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
    let store = create_test_store();

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
            start_handshake_with: Some(PeerId::new(pm.cfg.node_key.public_key())),
            force_encoding: Some(Encoding::Proto),
            nonce: None,
        };
        let stream = TcpStream::connect(pm.cfg.node_addr.unwrap()).await.unwrap();
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
                peer::testonly::Event::RoutingTable(got) => {
                    edges_got.extend(got.edges);
                    assert_is_superset(&edges_want.as_set(), &edges_got.as_set());
                }
                // Ignore other messages.
                _ => {}
            }
        }
    }
}

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

    //let mut total_edges = vec![];
    let store = create_test_store();

    // Start a PeerManager and connect a peer to it.
    let pm = peer_manager::testonly::start(
        clock.clock(),
        store.clone(),
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
            peers: vec![],
            start_handshake_with: Some(PeerId::new(pm.cfg.node_key.public_key())),
            force_encoding: Some(Encoding::Proto),
            // Connect with nonce equal to unix timestamp
            nonce: test.0,
        };
        let stream = TcpStream::connect(pm.cfg.node_addr.unwrap()).await.unwrap();
        let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
        if test.1 {
            peer.complete_handshake().await;
        } else {
            peer.fail_handshake().await;
        }
    }
}

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
        create_test_store(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let cfg = peer::testonly::PeerConfig {
        network: chain.make_config(rng),
        chain,
        peers: vec![],
        start_handshake_with: Some(PeerId::new(pm.cfg.node_key.public_key())),
        force_encoding: Some(Encoding::Proto),
        nonce: None,
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

async fn add_peer(
    clock: &time::Clock,
    rng: &mut Rng,
    chain: Arc<data::Chain>,
    cfg: &config::NetworkConfig,
) -> (peer::testonly::PeerHandle, SyncAccountsData) {
    let peer_cfg = peer::testonly::PeerConfig {
        network: chain.make_config(rng),
        chain,
        peers: vec![],
        start_handshake_with: Some(PeerId::new(cfg.node_key.public_key())),
        force_encoding: Some(Encoding::Proto),
        nonce: None,
    };
    let stream = TcpStream::connect(cfg.node_addr.unwrap()).await.unwrap();
    let mut peer =
        peer::testonly::PeerHandle::start_endpoint(clock.clone(), peer_cfg, stream).await;
    peer.complete_handshake().await;
    // TODO(gprusak): this should be part of complete_handshake, once Borsh support is removed.
    let msg = match peer.events.recv().await {
        peer::testonly::Event::Network(PME::MessageProcessed(PeerMessage::SyncAccountsData(
            msg,
        ))) => msg,
        ev => panic!("expected SyncAccountsData, got {ev:?}"),
    };
    (peer, msg)
}

#[tokio::test]
async fn accounts_data_broadcast() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let mut pm = peer_manager::testonly::start(
        clock.clone(),
        create_test_store(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    let take_sync = |ev| match ev {
        peer::testonly::Event::Network(PME::MessageProcessed(PeerMessage::SyncAccountsData(
            msg,
        ))) => Some(msg),
        _ => None,
    };

    let data = chain.make_tier1_data(rng, clock);

    // Connect peer, expect initial sync to be empty.
    let (mut peer1, got1) = add_peer(clock, rng, chain.clone(), &pm.cfg).await;
    assert_eq!(got1.accounts_data, vec![]);

    // Send some data. It won't be broadcasted back.
    let msg = SyncAccountsData {
        accounts_data: vec![data[0].clone(), data[1].clone()],
        incremental: true,
        requesting_full_sync: false,
    };
    let want = msg.accounts_data.clone();
    peer1.send(PeerMessage::SyncAccountsData(msg)).await;
    pm.wait_for_accounts_data(&want.iter().map(|d| d.into()).collect()).await;

    // Connect another peer and perform initial full sync.
    let (mut peer2, got2) = add_peer(clock, rng, chain.clone(), &pm.cfg).await;
    assert_eq!(got2.accounts_data.as_set(), want.as_set());

    // Send a mix of new and old data. Only new data should be broadcasted.
    let msg = SyncAccountsData {
        accounts_data: vec![data[1].clone(), data[2].clone()],
        incremental: true,
        requesting_full_sync: false,
    };
    let want = vec![data[2].clone()];
    peer1.send(PeerMessage::SyncAccountsData(msg)).await;
    let got2 = peer2.events.recv_until(take_sync).await;
    assert_eq!(got2.accounts_data.as_set(), want.as_set());

    // Send a request for a full sync.
    let want = vec![data[0].clone(), data[1].clone(), data[2].clone()];
    peer1
        .send(PeerMessage::SyncAccountsData(SyncAccountsData {
            accounts_data: vec![],
            incremental: true,
            requesting_full_sync: true,
        }))
        .await;
    let got1 = peer1.events.recv_until(take_sync).await;
    assert_eq!(got1.accounts_data.as_set(), want.as_set());
}

fn peer_addrs(vc: &config::ValidatorConfig) -> Vec<PeerAddr> {
    match &vc.endpoints {
        config::ValidatorEndpoints::PublicAddrs(peer_addrs) => peer_addrs.clone(),
        config::ValidatorEndpoints::TrustedStunServers(_) => {
            panic!("tests only support PublicAddrs in validator config")
        }
    }
}

// Test with 3 peer managers connected sequentially: 1-2-3
// All of them are validators.
// No matter what the order of shifting into the epoch,
// all of them should receive all the AccountDatas eventually.
#[tokio::test]
async fn accounts_data_gradual_epoch_change() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut pms = vec![];
    for _ in 0..3 {
        pms.push(
            peer_manager::testonly::start(
                clock.clock(),
                create_test_store(),
                chain.make_config(rng),
                chain.clone(),
            )
            .await,
        );
    }

    // 0 <-> 1 <-> 2
    let pm1 = pms[1].peer_info();
    let pm2 = pms[2].peer_info();
    pms[0].connect_to(&pm1).await;
    pms[1].connect_to(&pm2).await;

    // Validator configs.
    let vs: Vec<_> = pms.iter().map(|pm| pm.cfg.validator.clone().unwrap()).collect();

    // For every order of nodes.
    for ids in (0..pms.len()).permutations(3) {
        // Construct ChainInfo for a new epoch,
        // with tier1_accounts containing all validators.
        let e = data::make_epoch_id(rng);
        let mut chain_info = chain.get_chain_info();
        chain_info.tier1_accounts = Arc::new(
            vs.iter()
                .map(|v| ((e.clone(), v.signer.validator_id().clone()), v.signer.public_key()))
                .collect(),
        );

        // Advance epoch in the given order.
        for id in ids {
            pms[id].set_chain_info(chain_info.clone()).await;
        }

        // Wait for data to arrive.
        let want = vs
            .iter()
            .map(|v| NormalAccountData {
                epoch_id: e.clone(),
                account_id: v.signer.validator_id().clone(),
                peers: peer_addrs(v),
            })
            .collect();
        for pm in &mut pms {
            pm.wait_for_accounts_data(&want).await;
        }
    }
}

// Test is expected to take ~5s.
// Test with 20 peer managers connected in layers:
// - 1st 5 and 2nd 5 are connected in full bipartite graph.
// - 2nd 5 and 3rd 5 ...
// - 3rd 5 and 4th 5 ...
// All of them are validators.
#[tokio::test(flavor = "multi_thread")]
async fn accounts_data_rate_limiting() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    // TODO(gprusak) 10 connections per peer is not much, try to scale up this test 2x (some config
    // tweaking might be required).
    let n = 4; // layers
    let m = 5; // peer managers per layer
    let mut pms = vec![];
    for _ in 0..n * m {
        let mut cfg = chain.make_config(rng);
        cfg.accounts_data_broadcast_rate_limit = demux::RateLimit { qps: 0.5, burst: 1 };
        pms.push(
            peer_manager::testonly::start(clock.clock(), create_test_store(), cfg, chain.clone())
                .await,
        );
    }
    // Construct a 4-layer bipartite graph.
    let mut connections = 0;
    for i in 0..n - 1 {
        for j in 0..m {
            for k in 0..m {
                let pi = pms[(i + 1) * m + k].peer_info();
                pms[i * m + j].connect_to(&pi).await;
                connections += 1;
            }
        }
    }

    // Validator configs.
    let vs: Vec<_> = pms.iter().map(|pm| pm.cfg.validator.clone().unwrap()).collect();

    // Construct ChainInfo for a new epoch,
    // with tier1_accounts containing all validators.
    let e = data::make_epoch_id(rng);
    let mut chain_info = chain.get_chain_info();
    chain_info.tier1_accounts = Arc::new(
        vs.iter()
            .map(|v| ((e.clone(), v.signer.validator_id().clone()), v.signer.public_key()))
            .collect(),
    );

    // Advance epoch in random order.
    pms.shuffle(rng);
    for pm in &mut pms {
        pm.set_chain_info(chain_info.clone()).await;
    }

    // Capture the event streams at the start, so that we can compute
    // the total number of SyncAccountsData messages exchanged in the process.
    let events: Vec<_> = pms.iter().map(|pm| pm.events.clone()).collect();

    // Wait for data to arrive.
    let want = vs
        .iter()
        .map(|v| NormalAccountData {
            epoch_id: e.clone(),
            account_id: v.signer.validator_id().clone(),
            peers: peer_addrs(&v),
        })
        .collect();
    for pm in &mut pms {
        pm.wait_for_accounts_data(&want).await;
    }

    // Count the SyncAccountsData messages exchanged.
    let mut msgs = 0;
    for mut es in events {
        while let Some(ev) = es.try_recv() {
            if peer_manager::testonly::unwrap_sync_accounts_data_processed(ev).is_some() {
                msgs += 1;
            }
        }
    }

    // We expect 3 rounds communication to cover the distance from 1st layer to 4th layer
    // and +1 full sync at handshake.
    // The communication is bidirectional, which gives 8 messages per connection.
    // Then add +50% to accomodate for test execution flakiness (12 messages per connection).
    // TODO(gprusak): if the test is still flaky, upgrade FakeClock for stricter flow control.
    let want_max = connections * 12;
    println!("got {msgs}, want <= {want_max}");
    assert!(msgs <= want_max, "got {msgs} messages, want at most {want_max}");
}
