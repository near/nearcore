use crate::concurrency::rate;
use crate::network_protocol::testonly as data;
use crate::network_protocol::SyncAccountsData;
use crate::peer;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly;
use crate::peer_manager::testonly::start as start_pm;
use crate::tcp;
use crate::testonly::{make_rng, AsSet as _};
use crate::types::PeerMessage;
use itertools::Itertools;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_store::db::TestDB;
use pretty_assertions::assert_eq;
use rand::seq::SliceRandom as _;
use std::collections::HashSet;
use std::sync::Arc;

/// Each actix arbiter (in fact, the underlying tokio runtime) creates 4 file descriptors:
/// 1. eventfd2()
/// 2. epoll_create1()
/// 3. fcntl() duplicating one end of some globally shared socketpair()
/// 4. fcntl() duplicating epoll socket created in (2)
/// This gives 5 file descriptors per PeerActor (4 + 1 TCP socket).
const FDS_PER_PEER: usize = 5;

#[tokio::test]
async fn broadcast() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();
    let clock = &clock;

    let pm = peer_manager::testonly::start(
        clock.clone(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    let take_incremental_sync = |ev| match ev {
        peer::testonly::Event::Network(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncAccountsData(msg),
        )) if msg.incremental => Some(msg),
        _ => None,
    };
    let take_full_sync = |ev| match ev {
        peer::testonly::Event::Network(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncAccountsData(msg),
        )) if !msg.incremental => Some(msg),
        _ => None,
    };

    let data = chain.make_tier1_data(rng, clock);
    tracing::info!(target:"test", "Connect peer, expect initial sync to be empty.");
    let mut peer1 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let got1 = peer1.events.recv_until(take_full_sync).await;
    assert_eq!(got1.accounts_data, vec![]);

    tracing::info!(target:"test", "Send some data.");
    let msg = SyncAccountsData {
        accounts_data: vec![data[0].clone(), data[1].clone()],
        incremental: true,
        requesting_full_sync: false,
    };
    let want: HashSet<_> = msg.accounts_data.iter().cloned().collect();
    peer1.send(PeerMessage::SyncAccountsData(msg)).await;
    pm.wait_for_accounts_data(&want).await;

    tracing::info!(target:"test", "Connect another peer and perform initial full sync.");
    let mut peer2 =
        pm.start_inbound(chain.clone(), chain.make_config(rng)).await.handshake(clock).await;
    let got2 = peer2.events.recv_until(take_full_sync).await;
    assert_eq!(got2.accounts_data.as_set(), want.iter().collect::<HashSet<_>>());

    tracing::info!(target:"test", "Send a mix of new and old data. Only new data should be broadcasted.");
    let msg = SyncAccountsData {
        accounts_data: vec![data[1].clone(), data[2].clone()],
        incremental: true,
        requesting_full_sync: false,
    };
    let want = vec![data[2].clone()];
    peer1.send(PeerMessage::SyncAccountsData(msg)).await;
    let got2 = peer2.events.recv_until(take_incremental_sync).await;
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
    let got1 = events.recv_until(take_full_sync).await;
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
    pms[0].connect_to(&pm1, tcp::Tier::T2).await;
    pms[1].connect_to(&pm2, tcp::Tier::T2).await;

    // For every order of nodes.
    for ids in (0..pms.len()).permutations(pms.len()) {
        tracing::info!(target:"test", "permutation {ids:?}");
        clock.advance(time::Duration::hours(1));
        let chain_info = testonly::make_chain_info(
            &chain,
            &pms.iter().map(|pm| &pm.cfg).collect::<Vec<_>>()[..],
        );

        let mut want = HashSet::new();
        tracing::info!(target:"test", "advance epoch in the given order.");
        for id in ids {
            pms[id].set_chain_info(chain_info.clone()).await;
            // In this tests each node is its own proxy, so it can immediately
            // connect to itself (to verify the public addr) and advertise it.
            // If some other node B was a proxy for a node A, then first both
            // A and B would have to update their chain_info, and only then A
            // would be able to connect to B and advertise B as proxy afterwards.
            want.extend(pms[id].tier1_advertise_proxies(&clock.clock()).await);
        }
        for pm in &mut pms {
            tracing::info!(target:"test", "wait for data to arrive to {}.",pm.cfg.node_id());
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
    // Adjust the file descriptors limit, so that we can create many connection in the test.
    const MAX_CONNECTIONS: usize = 300;
    let limit = rlimit::Resource::NOFILE.get().unwrap();
    rlimit::Resource::NOFILE
        .set(std::cmp::min(limit.1, (1000 + 2 * FDS_PER_PEER * MAX_CONNECTIONS) as u64), limit.1)
        .unwrap();

    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    // TODO(gprusak): 10 connections per peer is not much, try to scale up this test 2x (some config
    // tweaking might be required).
    let n = 4; // layers
    let m = 5; // peer managers per layer
    let mut pms = vec![];
    for _ in 0..n * m {
        let mut cfg = chain.make_config(rng);
        cfg.accounts_data_broadcast_rate_limit = rate::Limit { qps: 0.5, burst: 1 };
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
    tracing::info!(target:"test", "Construct a 4-layer bipartite graph.");

    let mut connections = 0;
    let mut tasks = vec![];
    for i in 0..n - 1 {
        for j in 0..m {
            for k in 0..m {
                let pi = pms[(i + 1) * m + k].peer_info();
                tasks.push(tokio::spawn(pms[i * m + j].connect_to(&pi, tcp::Tier::T2)));
                connections += 1;
            }
        }
    }
    for t in tasks {
        t.await.unwrap();
    }

    // Construct ChainInfo with tier1_accounts containing all validators.
    let chain_info =
        testonly::make_chain_info(&chain, &pms.iter().map(|pm| &pm.cfg).collect::<Vec<_>>()[..]);

    clock.advance(time::Duration::hours(1));

    // Capture the event streams now, so that we can compute
    // the total number of SyncAccountsData messages exchanged in the process.
    let events: Vec<_> = pms.iter().map(|pm| pm.events.from_now()).collect();

    tracing::info!(target:"test","Advance epoch in random order.");
    pms.shuffle(rng);
    let mut want = HashSet::new();
    for pm in &mut pms {
        pm.set_chain_info(chain_info.clone()).await;
        want.extend(pm.tier1_advertise_proxies(&clock.clock()).await);
    }

    tracing::info!(target:"test","Wait for data to arrive.");
    for pm in &mut pms {
        pm.wait_for_accounts_data(&want).await;
    }

    tracing::info!(target:"test","Count the SyncAccountsData messages exchanged.");
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

    drop(pms);
}

/// When validator node is restarted, it should immediately override
/// the AccountData that is present in the network from the previous run.
#[tokio::test]
async fn validator_node_restart() {
    init_test_logger();

    const ZERO: time::Duration = time::Duration::ZERO;
    const SEC: time::Duration = time::Duration::seconds(1);

    // We test restarting node with various number of AccountsData versions
    // broadcasted before restart, and various downtime (represented as UTC clock shift, which may
    // be negative).
    for (n, downtime) in [
        // If no version was emitted, timestamp is not important
        (0, -SEC),
        (0, ZERO),
        (0, SEC),
        // If 1 version was emitted, the version will be immediately correctly
        // overriden as long as timestamp has changed.
        (1, -SEC),
        (1, SEC),
        // If multiple versions were emitted, timestamp is not important
        (2, -SEC),
        (2, ZERO),
        (2, SEC),
    ] {
        tracing::info!(target:"test", "test case (n = {n}, downtime = {downtime})");

        let mut rng = make_rng(921853233);
        let rng = &mut rng;
        let mut clock = time::FakeClock::default();
        let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

        // Start 2 nodes with node pm0 being a validator.
        let mut cfg = chain.make_config(rng);
        let chain_info = testonly::make_chain_info(&chain, &[&cfg]);
        let pm0 = start_pm(clock.clock(), TestDB::new(), cfg.clone(), chain.clone()).await;
        let pm1 =
            start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
        pm0.set_chain_info(chain_info.clone()).await;
        pm1.set_chain_info(chain_info.clone()).await;

        // Connect the nodes and make pm0 broadcast `n` versions of the AccountData.
        pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
        for _ in 0..n {
            let want = pm0.tier1_advertise_proxies(&clock.clock()).await.unwrap();
            pm1.wait_for_accounts_data(&HashSet::from([want.clone()])).await;
        }

        // Shut down pm0 and restart it after `downtime` with a different node_key.
        drop(pm0);
        clock.set_utc(clock.now_utc() + downtime);
        cfg.node_key = data::make_secret_key(rng);
        let pm0 = start_pm(clock.clock(), TestDB::new(), cfg.clone(), chain.clone()).await;
        pm0.set_chain_info(chain_info.clone()).await;

        // Sign AccountData before even connecting to the network.
        assert!(pm0.tier1_advertise_proxies(&clock.clock()).await.is_some());
        // Connect to the node which knows the old AccountData.
        pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

        // Now pm0 should learn from pm1 about the conflicting version and should broadcast
        // new AccountData (with higher version) to override the old AccountData.
        let pm0_account_key = cfg.validator.as_ref().unwrap().signer.public_key();
        pm1.wait_for_accounts_data_pred(|accounts_data| {
            let data = match accounts_data.data.get(&pm0_account_key) {
                Some(it) => it,
                None => return false,
            };
            data.peer_id == cfg.node_id()
        })
        .await;
    }
}
