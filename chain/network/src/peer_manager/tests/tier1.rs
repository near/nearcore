use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    PartialEncodedChunkForwardMsg, PeerAddr, PeerMessage, RoutedMessage, T1MessageBody,
    T2MessageBody, TieredMessageBody,
};
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::testonly::start as start_pm;
use crate::stats::metrics;
use crate::stun;
use crate::tcp;
use crate::testonly::{Rng, make_rng};
use near_async::time;
use near_crypto::Signature;
use near_o11y::testonly::init_test_logger;
use near_primitives::block_header::{Approval, ApprovalInner};
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{AccountId, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::db::TestDB;
use rand::Rng as _;
use std::collections::HashSet;
use std::sync::Arc;

/// Constructs a random TIER1 message.
fn make_block_approval(rng: &mut Rng, signer: &ValidatorSigner) -> Approval {
    let inner = ApprovalInner::Endorsement(data::make_hash(rng));
    let target_height = rng.gen_range(0..100000);
    Approval {
        signature: signer.sign_bytes(&Approval::get_data_for_sig(&inner, target_height)),
        account_id: signer.validator_id().clone(),
        target_height,
        inner,
    }
}

async fn establish_connections(clock: &time::Clock, pms: &[&peer_manager::testonly::ActorHandler]) {
    // Make TIER1 validators connect to proxies.
    let mut data = HashSet::new();
    for pm in pms {
        data.extend(pm.tier1_advertise_proxies(clock).await);
    }
    tracing::info!(target:"test", "tier1_advertise_proxies() done");

    // Wait for accounts data to propagate.
    for pm in pms {
        tracing::info!(target:"test", node_id = %pm.cfg.node_id(), "wait_for_accounts_data()");
        pm.wait_for_accounts_data(&data).await;
        tracing::info!(target:"test", node_id = %pm.cfg.node_id(), "wait_for_accounts_data() done");
        pm.tier1_connect(clock).await;
        tracing::info!(target:"test", node_id = %pm.cfg.node_id(), "tier1_connect() done");
    }
}

// Sends a routed message from `from` to the account `target`.
// Returns false if the routing information was missing.
async fn send_message_to_account(
    clock: &time::Clock,
    from: &peer_manager::testonly::ActorHandler,
    target: AccountId,
    body: TieredMessageBody,
) -> bool {
    let clock = clock.clone();
    from.with_state_and_transport(move |s, transport| async move {
        s.send_message_to_account(&clock, &target, body, &*transport)
    })
    .await
}

// Sends a routed message from `from` to the account of `to`.
// Returns false if the routing information was missing.
async fn send_message(
    clock: &time::Clock,
    from: &peer_manager::testonly::ActorHandler,
    to: &peer_manager::testonly::ActorHandler,
    body: TieredMessageBody,
) -> bool {
    let target = to.cfg.validator.signer.get().unwrap().validator_id().clone();
    send_message_to_account(clock, from, target, body).await
}

// Sends a routed message from `from` to `to`, then waits until `to` receives it over `recv_tier`
// and returns the received message.
async fn send_and_recv_message(
    clock: &time::Clock,
    from: &peer_manager::testonly::ActorHandler,
    to: &peer_manager::testonly::ActorHandler,
    body: TieredMessageBody,
    recv_tier: tcp::Tier,
) -> Box<RoutedMessage> {
    let mut events = to.events.from_now();
    assert!(send_message(clock, from, to, body.clone()).await, "routing info not available");
    let got = events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(tier, PeerMessage::Routed(got)) if tier == recv_tier => {
                Some(got)
            }
            _ => None,
        })
        .await;
    assert_eq!(from.cfg.node_id(), got.author().clone());
    assert_eq!(&body, got.body());
    got
}

// Sends a routed TIER1 message from `from` to `to`, then waits until `to` receives it and returns
// the received message.
async fn send_and_recv_tier1_message(
    rng: &mut Rng,
    clock: &time::Clock,
    from: &peer_manager::testonly::ActorHandler,
    to: &peer_manager::testonly::ActorHandler,
) -> Box<RoutedMessage> {
    let signer = from.cfg.validator.signer.get().unwrap();
    let body = T1MessageBody::BlockApproval(make_block_approval(rng, signer.as_ref())).into();
    send_and_recv_message(clock, from, to, body, tcp::Tier::T1).await
}

/// Send a message over each connection.
async fn test_clique(
    rng: &mut Rng,
    clock: &time::Clock,
    pms: &[&peer_manager::testonly::ActorHandler],
) {
    for from in pms {
        for to in pms {
            if from.cfg.node_id() == to.cfg.node_id() {
                continue;
            }
            send_and_recv_tier1_message(rng, clock, from, to).await;
        }
    }
}

// In case a node is its own proxy, it should advertise its address as soon as
// it becomes a TIER1 node.
#[tokio::test]
async fn first_proxy_advertisement() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm = start_pm(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&pm.cfg]);
    tracing::info!(target:"test", "set_chain_info()");
    // TODO(gprusak): The default config constructed via chain.make_config(),
    // currently returns a validator config with its own server addr in the list of TIER1 proxies.
    // You might want to set it explicitly within this test to not rely on defaults.
    pm.set_chain_info(chain_info).await;
    let got = pm.tier1_advertise_proxies(&clock.clock()).await;
    assert_eq!(
        got.unwrap().proxies,
        vec![PeerAddr { peer_id: pm.cfg.node_id(), addr: **pm.cfg.node_addr.as_ref().unwrap() }]
    );
}

#[tokio::test]
async fn direct_connections() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut pms = vec![];
    for _ in 0..5 {
        pms.push(
            start_pm(
                clock.clock(),
                near_store::db::TestDB::new(),
                chain.make_config(rng),
                chain.clone(),
            )
            .await,
        );
    }
    let pms: Vec<_> = pms.iter().collect();

    tracing::info!(target:"test", "connect peers serially");
    for i in 1..pms.len() {
        pms[i - 1].connect_to(&pms[i].peer_info(), tcp::Tier::T2).await;
    }

    tracing::info!(target:"test", "set chain info");
    let chain_info = peer_manager::testonly::make_chain_info(
        &chain,
        &pms.iter().map(|pm| &pm.cfg).collect::<Vec<_>>(),
    );
    for pm in &pms {
        pm.set_chain_info(chain_info.clone()).await;
    }
    tracing::info!(target:"test", "establish connections");
    establish_connections(&clock.clock(), &pms[..]).await;
    tracing::info!(target:"test", "test clique");
    test_clique(rng, &clock.clock(), &pms[..]).await;
}

/// Test which spawns N validators, each with 1 proxy.
/// All the nodes are connected in TIER2 star topology.
/// Then all validators connect to the proxy of each other validator.
#[tokio::test]
async fn proxy_connections() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    const N: usize = 5;

    let mut proxies = vec![];
    for _ in 0..N {
        proxies.push(
            start_pm(
                clock.clock(),
                near_store::db::TestDB::new(),
                chain.make_config(rng),
                chain.clone(),
            )
            .await,
        );
    }
    let proxies: Vec<_> = proxies.iter().collect();

    let mut validators = vec![];
    for i in 0..N {
        let mut cfg = chain.make_config(rng);
        cfg.validator.proxies = config::ValidatorProxies::Static(vec![PeerAddr {
            peer_id: proxies[i].cfg.node_id(),
            addr: **proxies[i].cfg.node_addr.as_ref().unwrap(),
        }]);
        validators
            .push(start_pm(clock.clock(), near_store::db::TestDB::new(), cfg, chain.clone()).await);
    }
    let validators: Vec<_> = validators.iter().collect();

    // Connect validators and proxies in a star topology. Any connected graph would do.
    let hub = start_pm(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    for pm in &validators {
        pm.connect_to(&hub.peer_info(), tcp::Tier::T2).await;
    }
    for pm in &proxies {
        pm.connect_to(&hub.peer_info(), tcp::Tier::T2).await;
    }

    let mut all = vec![];
    all.extend(validators.clone());
    all.extend(proxies.clone());
    all.push(&hub);

    let chain_info = peer_manager::testonly::make_chain_info(
        &chain,
        &validators.iter().map(|pm| &pm.cfg).collect::<Vec<_>>(),
    );
    for pm in &all {
        pm.set_chain_info(chain_info.clone()).await;
    }
    establish_connections(&clock.clock(), &all[..]).await;
    test_clique(rng, &clock.clock(), &validators[..]).await;
}

#[tokio::test]
async fn account_keys_change() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let v0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let v1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let v2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let hub = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    hub.connect_to(&v0.peer_info(), tcp::Tier::T2).await;
    hub.connect_to(&v1.peer_info(), tcp::Tier::T2).await;
    hub.connect_to(&v2.peer_info(), tcp::Tier::T2).await;

    // TIER1 nodes in 1st epoch are {v0,v1}.
    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&v0.cfg, &v1.cfg]);
    for pm in [&v0, &v1, &v2, &hub] {
        pm.set_chain_info(chain_info.clone()).await;
    }
    establish_connections(&clock.clock(), &[&v0, &v1, &v2, &hub]).await;
    test_clique(rng, &clock.clock(), &[&v0, &v1]).await;

    // TIER1 nodes in 2nd epoch are {v0,v2}.
    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&v0.cfg, &v2.cfg]);
    for pm in [&v0, &v1, &v2, &hub] {
        pm.set_chain_info(chain_info.clone()).await;
    }
    establish_connections(&clock.clock(), &[&v0, &v1, &v2, &hub]).await;
    test_clique(rng, &clock.clock(), &[&v0, &v2]).await;

    drop(v0);
    drop(v1);
    drop(v2);
    drop(hub);
}

// Let's say that a validator has 2 proxies configured. At first proxy0 is available and proxy1 is not,
// then proxy1 is available and proxy0 is not. In both situations validator should be reachable,
// as long as it manages to advertise the currently available proxy and the TIER1 nodes connect to
// that proxy.
#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn proxy_change() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    // v0 has proxies {p0,p1}
    // v1 has no proxies.
    let p0cfg = chain.make_config(rng);
    let p1cfg = chain.make_config(rng);
    let mut v0cfg = chain.make_config(rng);
    v0cfg.validator.proxies = config::ValidatorProxies::Static(vec![
        PeerAddr { peer_id: p0cfg.node_id(), addr: **p0cfg.node_addr.as_ref().unwrap() },
        PeerAddr { peer_id: p1cfg.node_id(), addr: **p1cfg.node_addr.as_ref().unwrap() },
    ]);
    let mut v1cfg = chain.make_config(rng);
    v1cfg.validator.proxies = config::ValidatorProxies::Static(vec![]);

    tracing::info!(target:"test", "start all nodes");
    let p0 = start_pm(clock.clock(), TestDB::new(), p0cfg.clone(), chain.clone()).await;
    let p1 = start_pm(clock.clock(), TestDB::new(), p1cfg.clone(), chain.clone()).await;
    let v0 = start_pm(clock.clock(), TestDB::new(), v0cfg.clone(), chain.clone()).await;
    let v1 = start_pm(clock.clock(), TestDB::new(), v1cfg.clone(), chain.clone()).await;
    let hub = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    hub.connect_to(&p0.peer_info(), tcp::Tier::T2).await;
    hub.connect_to(&p1.peer_info(), tcp::Tier::T2).await;
    hub.connect_to(&v0.peer_info(), tcp::Tier::T2).await;
    hub.connect_to(&v1.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "p0 goes down");
    drop(p0);
    tracing::info!(target:"test", "remaining nodes learn that [v0,v1] are TIER1 nodes");
    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&v0.cfg, &v1.cfg]);
    for pm in [&v0, &v1, &p1, &hub] {
        pm.set_chain_info(chain_info.clone()).await;
    }
    tracing::info!(target:"test", "TIER1 connections get established: v0 -> p1 <- v1");
    establish_connections(&clock.clock(), &[&v0, &v1, &p1, &hub]).await;
    tracing::info!(target:"test", "send message v1 -> v0 over tier1");
    send_and_recv_tier1_message(rng, &clock.clock(), &v1, &v0).await;

    // Advance time, so that the new AccountsData has newer timestamp.
    clock.advance(time::Duration::hours(1));

    tracing::info!(target:"test", "p1 goes down");
    drop(p1);
    tracing::info!(target:"test", "p0 goes up and learns that [v0,v1] are TIER1 nodes");
    let p0 = start_pm(clock.clock(), TestDB::new(), p0cfg.clone(), chain.clone()).await;
    p0.set_chain_info(chain_info).await;
    hub.connect_to(&p0.peer_info(), tcp::Tier::T2).await;
    tracing::info!(target:"test", "TIER1 connections get established: v0 -> p0 <- v1");
    establish_connections(&clock.clock(), &[&v0, &v1, &p0, &hub]).await;
    tracing::info!(target:"test", "send message v1 -> v0 over tier1");
    send_and_recv_tier1_message(rng, &clock.clock(), &v1, &v0).await;

    drop(hub);
    drop(v0);
    drop(v1);
    drop(p0);
}

#[tokio::test]
async fn tier2_routing_using_accounts_data() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start 2 nodes and connect them");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    let tx: TieredMessageBody =
        T2MessageBody::ForwardTx(data::make_signed_transaction(rng).into()).into();

    tracing::info!(target:"test", "try to send a transaction pm0 -> pm1");
    // It should fail due to missing routing information: AccountData is not broadcasted by
    // default in tests.
    assert!(!send_message(&clock.clock(), &pm0, &pm1, tx.clone()).await);

    tracing::info!(target:"test", "propagate accounts data");
    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&pm1.cfg]);
    for pm in [&pm0, &pm1] {
        pm.set_chain_info(chain_info.clone()).await;
    }
    let data: HashSet<_> = pm1.tier1_advertise_proxies(&clock.clock()).await.into_iter().collect();
    pm0.wait_for_accounts_data(&data).await;

    tracing::info!(target:"test", "send a transaction pm0 -> pm1 over tier2");
    send_and_recv_message(&clock.clock(), &pm0, &pm1, tx, tcp::Tier::T2).await;
}

#[tokio::test]
async fn relay_through_validator() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start 3 validators, only v1 advertises an address");
    let mut v0cfg = chain.make_config(rng);
    v0cfg.validator.proxies = config::ValidatorProxies::Static(vec![]);
    let mut v2cfg = chain.make_config(rng);
    v2cfg.validator.proxies = config::ValidatorProxies::Static(vec![]);
    let v0 = start_pm(clock.clock(), TestDB::new(), v0cfg, chain.clone()).await;
    let v1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let v2 = start_pm(clock.clock(), TestDB::new(), v2cfg, chain.clone()).await;
    v0.connect_to(&v1.peer_info(), tcp::Tier::T2).await;
    v1.connect_to(&v2.peer_info(), tcp::Tier::T2).await;

    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&v0.cfg, &v1.cfg, &v2.cfg]);
    for pm in [&v0, &v1, &v2] {
        pm.set_chain_info(chain_info.clone()).await;
    }
    tracing::info!(target:"test", "tier1 connections get established: v0 -> v1 <- v2");
    establish_connections(&clock.clock(), &[&v0, &v1, &v2]).await;

    tracing::info!(target:"test", "send message v0 -> v2, relayed by v1");
    let got = send_and_recv_tier1_message(rng, &clock.clock(), &v0, &v2).await;
    assert_eq!(got.num_hops(), 1);
}

const CHUNK_FORWARD: &str = "PartialEncodedChunkForward";

/// Constructs a random TIER1 message of a type which only `route_metrics` sends, as all the
/// tests of a process share the metrics.
fn make_chunk_forward(rng: &mut Rng) -> TieredMessageBody {
    T1MessageBody::PartialEncodedChunkForward(PartialEncodedChunkForwardMsg {
        chunk_hash: ChunkHash(data::make_hash(rng)),
        inner_header_hash: data::make_hash(rng),
        merkle_root: data::make_hash(rng),
        signature: Signature::default(),
        prev_block_hash: data::make_hash(rng),
        height_created: rng.r#gen(),
        shard_id: ShardId::new(0),
        parts: vec![],
    })
    .into()
}

/// Chunk forwards created by the nodes of this process, by route: direct, relay, none.
fn sent_counts() -> [u64; 3] {
    [metrics::Tier1Route::Direct, metrics::Tier1Route::Relay, metrics::Tier1Route::None]
        .map(|route| metrics::TIER1_SENT.with_label_values(&[CHUNK_FORWARD, route.as_ref()]).get())
}

/// Chunk forwards forwarded by the nodes of this process, by result: sent, not connected,
/// unreachable.
fn forwarded_counts() -> [u64; 3] {
    [
        metrics::Tier1Forward::Sent,
        metrics::Tier1Forward::NotConnected,
        metrics::Tier1Forward::Unreachable,
    ]
    .map(|result| {
        metrics::TIER1_FORWARDED.with_label_values(&[CHUNK_FORWARD, result.as_ref()]).get()
    })
}

/// Chunk forwards received over TIER1 by the nodes of this process: from their author, from
/// another peer.
fn received_counts() -> [u64; 2] {
    [true, false].map(|direct| {
        metrics::NETWORK_ROUTED_MSG_LATENCY
            .with_label_values(&[
                CHUNK_FORWARD,
                tcp::Tier::T1.as_ref(),
                metrics::bool_to_str(/*fastest=*/ true),
                metrics::bool_to_str(direct),
            ])
            .get_sample_count()
    })
}

#[tokio::test]
async fn route_metrics() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start 4 validators, only v1 advertises an address");
    let mut v0cfg = chain.make_config(rng);
    v0cfg.validator.proxies = config::ValidatorProxies::Static(vec![]);
    let mut v2cfg = chain.make_config(rng);
    v2cfg.validator.proxies = config::ValidatorProxies::Static(vec![]);
    let mut v3cfg = chain.make_config(rng);
    v3cfg.validator.proxies = config::ValidatorProxies::Static(vec![]);
    let v0 = start_pm(clock.clock(), TestDB::new(), v0cfg, chain.clone()).await;
    let v1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let v2 = start_pm(clock.clock(), TestDB::new(), v2cfg, chain.clone()).await;
    let v3 = start_pm(clock.clock(), TestDB::new(), v3cfg, chain.clone()).await;
    v3.connect_to(&v0.peer_info(), tcp::Tier::T2).await;
    v0.connect_to(&v1.peer_info(), tcp::Tier::T2).await;
    v1.connect_to(&v2.peer_info(), tcp::Tier::T2).await;

    let chain_info =
        peer_manager::testonly::make_chain_info(&chain, &[&v0.cfg, &v1.cfg, &v2.cfg, &v3.cfg]);
    for pm in [&v0, &v1, &v2, &v3] {
        pm.set_chain_info(chain_info.clone()).await;
    }
    let mut accounts_data = HashSet::new();
    for pm in [&v0, &v1, &v2, &v3] {
        accounts_data.extend(pm.tier1_advertise_proxies(&clock.clock()).await);
    }
    for pm in [&v0, &v1, &v2] {
        pm.wait_for_accounts_data(&accounts_data).await;
    }

    tracing::info!(target:"test", "v3 goes down, the other validators keep its account data");
    let v3_account = v3.cfg.validator.signer.get().unwrap().validator_id().clone();
    drop(v3);
    let (id0, id2) = (v0.cfg.node_id(), v2.cfg.node_id());
    v1.wait_for_routing_table(&[(id0.clone(), vec![id0]), (id2.clone(), vec![id2])]).await;

    tracing::info!(target:"test", "send message v0 -> v2, v0 has no tier1 connections");
    assert!(!send_message(&clock.clock(), &v0, &v2, make_chunk_forward(rng)).await);
    assert_eq!([0, 0, 1], sent_counts());

    tracing::info!(target:"test", "tier1 connection gets established: v0 -> v1");
    v0.tier1_connect(&clock.clock()).await;
    // A message which v1 fails to forward triggers no event. v1 handles the messages of v0 in
    // order, so once it has received this one, it has handled the ones which v0 sent before.
    let sync = async |rng: &mut Rng| {
        send_and_recv_message(&clock.clock(), &v0, &v1, make_chunk_forward(rng), tcp::Tier::T1)
            .await;
    };

    tracing::info!(target:"test", "send message v0 -> v3, v1 has no route to v3");
    let body = make_chunk_forward(rng);
    assert!(send_message_to_account(&clock.clock(), &v0, v3_account, body).await);
    sync(rng).await;
    assert_eq!([0, 0, 1], forwarded_counts());

    tracing::info!(target:"test", "send message v0 -> v2, v1 has a tier2 route to v2");
    assert!(send_message(&clock.clock(), &v0, &v2, make_chunk_forward(rng)).await);
    sync(rng).await;
    assert_eq!([0, 1, 1], forwarded_counts());

    tracing::info!(target:"test", "tier1 connection gets established: v1 <- v2");
    v2.tier1_connect(&clock.clock()).await;
    tracing::info!(target:"test", "send message v0 -> v2, relayed by v1");
    send_and_recv_message(&clock.clock(), &v0, &v2, make_chunk_forward(rng), tcp::Tier::T1).await;
    assert_eq!([1, 1, 1], forwarded_counts());

    assert_eq!([2, 3, 1], sent_counts());
    assert_eq!([2, 1], received_counts());
}

#[tokio::test]
async fn stun_self_discovery() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "configure tier1 self discovery to use 2 local stun servers");
    let stun_server1 = stun::testonly::Server::new().await;
    let stun_server2 = stun::testonly::Server::new().await;
    let mut cfg = chain.make_config(rng);
    cfg.validator.proxies = config::ValidatorProxies::Dynamic(vec![
        stun_server1.addr().to_string(),
        stun_server2.addr().to_string(),
    ]);

    tracing::info!(target:"test", "spawn a node and advertize account data");
    let pm = start_pm(clock.clock(), TestDB::new(), cfg, chain.clone()).await;
    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&pm.cfg]);
    pm.set_chain_info(chain_info).await;
    let got = pm.tier1_advertise_proxies(&clock.clock()).await.unwrap();
    let want = vec![PeerAddr { peer_id: pm.cfg.node_id(), addr: *pm.cfg.node_addr.unwrap() }];
    assert_eq!(want, got.proxies);

    tracing::info!(target:"test", "close the stun servers");
    drop(pm);
    stun_server1.close().await;
    stun_server2.close().await;
}
