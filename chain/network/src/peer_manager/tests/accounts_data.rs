use crate::concurrency::demux;
use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{PeerAddr, SyncAccountsData};
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::NormalAccountData;
use crate::testonly::{make_rng, AsSet as _};
use crate::time;
use crate::types::PeerMessage;
use itertools::Itertools;
use near_o11y::testonly::init_test_logger;
use pretty_assertions::assert_eq;
use rand::seq::SliceRandom as _;
use std::sync::Arc;

fn peer_addrs(vc: &config::ValidatorConfig) -> Vec<PeerAddr> {
    match &vc.endpoints {
        config::ValidatorEndpoints::PublicAddrs(peer_addrs) => peer_addrs.clone(),
        config::ValidatorEndpoints::TrustedStunServers(_) => {
            panic!("tests only support PublicAddrs in validator config")
        }
    }
}

#[tokio::test]
async fn broadcast() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let mut pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
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

    tracing::info!(target:"test", "Connect peer, expect initial sync to be empty.");
    let mut peer1 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let got1 = peer1.events.recv_until(take_sync).await;
    assert_eq!(got1.accounts_data, vec![]);

    tracing::info!(target:"test", "Send some data. It won't be broadcasted back.");
    let msg = SyncAccountsData {
        accounts_data: vec![data[0].clone(), data[1].clone()],
        incremental: true,
        requesting_full_sync: false,
    };
    let want = msg.accounts_data.clone();
    peer1.send(PeerMessage::SyncAccountsData(msg)).await;
    pm.wait_for_accounts_data(&want.iter().map(|d| d.into()).collect()).await;

    tracing::info!(target:"test", "Connect another peer and perform initial full sync.");
    let mut peer2 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let got2 = peer2.events.recv_until(take_sync).await;
    assert_eq!(got2.accounts_data.as_set(), want.as_set());

    tracing::info!(target:"test", "Send a mix of new and old data. Only new data should be broadcasted.");
    let msg = SyncAccountsData {
        accounts_data: vec![data[1].clone(), data[2].clone()],
        incremental: true,
        requesting_full_sync: false,
    };
    let want = vec![data[2].clone()];
    peer1.send(PeerMessage::SyncAccountsData(msg)).await;
    let got2 = peer2.events.recv_until(take_sync).await;
    assert_eq!(got2.accounts_data.as_set(), want.as_set());

    tracing::info!(target:"test", "Send a request for a full sync.");
    let want = vec![data[0].clone(), data[1].clone(), data[2].clone()];
    let mut events = peer1.events.from_now();
    peer1
        .send(PeerMessage::SyncAccountsData(SyncAccountsData {
            accounts_data: vec![],
            incremental: true,
            requesting_full_sync: true,
        }))
        .await;
    let got1 = events.recv_until(take_sync).await;
    assert_eq!(got1.accounts_data.as_set(), want.as_set());
}

// Test with 3 peer managers connected sequentially: 1-2-3
// All of them are validators.
// No matter what the order of shifting into the epoch,
// all of them should receive all the AccountDatas eventually.
#[tokio::test]
async fn gradual_epoch_change() {
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
                near_store::db::TestDB::new(),
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
    for ids in (0..pms.len()).permutations(pms.len()) {
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
async fn rate_limiting() {
    init_test_logger();
    // Each actix arbiter (in fact, the underlying tokio runtime) creates 4 file descriptors:
    // 1. eventfd2()
    // 2. epoll_create1()
    // 3. fcntl() duplicating one end of some globally shared socketpair()
    // 4. fcntl() duplicating epoll socket created in (2)
    // This gives 5 file descriptors per PeerActor (4 + 1 TCP socket).
    // PeerManager (together with the whole ActixSystem) creates 13 file descriptors.
    // The usual default soft limit on the number of file descriptors on linux is 1024.
    // Here we adjust it appropriately to account for test requirements.
    let limit = rlimit::Resource::NOFILE.get().unwrap();
    rlimit::Resource::NOFILE.set(std::cmp::min(limit.1, 3000), limit.1).unwrap();

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
            peer_manager::testonly::start(
                clock.clock(),
                near_store::db::TestDB::new(),
                cfg,
                chain.clone(),
            )
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
