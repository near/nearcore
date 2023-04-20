use crate::broadcast;
use crate::network_protocol::testonly as data;
use crate::peer_manager::connection_store::STORED_CONNECTIONS_MIN_DURATION;
use crate::peer_manager::network_state::RECONNECT_ATTEMPT_INTERVAL;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::peer_manager_actor::POLL_CONNECTION_STORE_INTERVAL;
use crate::peer_manager::testonly::start as start_pm;
use crate::peer_manager::testonly::ActorHandler;
use crate::peer_manager::testonly::Event;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::AsSet;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use near_store::db::TestDB;
use std::sync::Arc;

async fn check_recent_outbound_connections(pm: &ActorHandler, want: Vec<PeerId>) {
    let got: Vec<PeerId> = pm
        .with_state(move |s| async move {
            s.connection_store
                .get_recent_outbound_connections()
                .iter()
                .map(|c| c.peer_info.id.clone())
                .collect()
        })
        .await;

    assert_eq!(got.as_set(), want.as_set());
}

async fn wait_for_connection_closed(events: &mut broadcast::Receiver<Event>) {
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::ConnectionClosed(_)) => Some(()),
            _ => None,
        })
        .await
}

#[tokio::test]
async fn test_store_outbound_connection() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "connect pm0 to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm2.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    clock.advance(STORED_CONNECTIONS_MIN_DURATION);

    tracing::info!(target:"test", "check that pm0 stores the outbound connection to pm1");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test", "check that pm1 does not store anything, as it has no outbound connections");
    check_recent_outbound_connections(&pm1, vec![]).await;

    tracing::info!(target:"test", "check that pm2 stores the outbound connection to pm0");
    pm2.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm2, vec![id0.clone()]).await;
}

#[tokio::test]
async fn test_storage_after_disconnect() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "connect pm0 to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    clock.advance(STORED_CONNECTIONS_MIN_DURATION);

    tracing::info!(target:"test", "check that pm0 stores the outbound connection to pm1");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test", "have pm1 disconnect from pm0");
    let mut pm0_ev = pm0.events.from_now();
    pm1.disconnect(&id0).await;
    wait_for_connection_closed(&mut pm0_ev).await;

    tracing::info!(target:"test", "check that pm0 retains the stored outbound connection to pm1 after disconnect");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;
}

#[tokio::test]
async fn test_reconnect_after_restart_outbound_side() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0_db = TestDB::new();
    let pm0 = start_pm(clock.clock(), pm0_db.clone(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "connect pm0 to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    clock.advance(STORED_CONNECTIONS_MIN_DURATION);

    tracing::info!(target:"test", "check that pm0 stores the outbound connection to pm1");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test", "drop pm0 and start it again with the same db");
    drop(pm0);
    let pm0 = start_pm(clock.clock(), pm0_db, chain.make_config(rng), chain.clone()).await;

    tracing::info!(target:"test", "check that pm0 reconnects to pm1");
    pm0.wait_for_direct_connection(id1.clone()).await;
}

#[tokio::test]
async fn test_skip_reconnect_after_restart_outbound_side() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0_db = TestDB::new();
    let mut pm0_cfg = chain.make_config(rng);
    pm0_cfg.connect_to_reliable_peers_on_startup = false;

    let pm0 = start_pm(clock.clock(), pm0_db.clone(), pm0_cfg.clone(), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "connect pm0 to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    clock.advance(STORED_CONNECTIONS_MIN_DURATION);

    tracing::info!(target:"test", "check that pm0 stores the outbound connection to pm1");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test", "drop pm0 and start it again with the same db");
    drop(pm0);
    let pm0 = start_pm(clock.clock(), pm0_db.clone(), pm0_cfg.clone(), chain.clone()).await;

    tracing::info!(target:"test", "check that pm0 starts without attempting to reconnect to pm1");
    let mut pm0_ev = pm0.events.clone();
    pm0_ev
        .recv_until(|ev| match &ev {
            Event::PeerManager(PME::ReconnectLoopSpawned(_)) => {
                panic!("PeerManager spawned a reconnect loop during startup");
            }
            Event::PeerManager(PME::PeerManagerStarted) => Some(()),
            _ => None,
        })
        .await;

    tracing::info!(target:"test", "check that pm0 has pm1 as a recent connection");
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;
}

#[tokio::test]
async fn test_reconnect_after_restart_inbound_side() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let pm1_cfg = chain.make_config(rng);
    let pm1 = start_pm(clock.clock(), TestDB::new(), pm1_cfg.clone(), chain.clone()).await;

    let id1 = pm1.cfg.node_id().clone();

    tracing::info!(target:"test", "connect pm0 to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    clock.advance(STORED_CONNECTIONS_MIN_DURATION);

    tracing::info!(target:"test", "check that pm0 stores the outbound connection to pm1");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test", "drop pm1");
    let mut pm0_ev = pm0.events.from_now();
    drop(pm1);
    wait_for_connection_closed(&mut pm0_ev).await;

    tracing::info!(target:"test", "start pm1 again with the same config, check that pm0 reconnects");
    let _pm1 = start_pm(clock.clock(), TestDB::new(), pm1_cfg.clone(), chain.clone()).await;
    clock.advance(POLL_CONNECTION_STORE_INTERVAL + RECONNECT_ATTEMPT_INTERVAL);
    pm0.wait_for_direct_connection(id1.clone()).await;
}

#[tokio::test]
async fn test_reconnect_after_disconnect_inbound_side() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let pm1_cfg = chain.make_config(rng);
    let pm1 = start_pm(clock.clock(), TestDB::new(), pm1_cfg.clone(), chain.clone()).await;

    let id0 = pm0.cfg.node_id().clone();
    let id1 = pm1.cfg.node_id().clone();

    tracing::info!(target:"test", "connect pm0 to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    clock.advance(STORED_CONNECTIONS_MIN_DURATION);

    tracing::info!(target:"test", "check that pm0 stores the outbound connection to pm1");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test", "have pm1 disconnect gracefully from pm0");
    let mut pm0_ev = pm0.events.from_now();
    pm1.disconnect(&id0).await;
    wait_for_connection_closed(&mut pm0_ev).await;

    tracing::info!(target:"test", "check that pm0 reconnects");
    clock.advance(POLL_CONNECTION_STORE_INTERVAL + RECONNECT_ATTEMPT_INTERVAL);
    pm0.wait_for_direct_connection(id1.clone()).await;
}

#[tokio::test]
async fn test_reconnect_after_restart_outbound_side_multi() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0_db = TestDB::new();
    let pm0 = start_pm(clock.clock(), pm0_db.clone(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm3 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm4 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();
    let id3 = pm3.cfg.node_id();
    let id4 = pm4.cfg.node_id();

    tracing::info!(target:"test", "connect pm0 to other pms");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm0.wait_for_direct_connection(id1.clone()).await;
    pm0.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;
    pm0.wait_for_direct_connection(id2.clone()).await;
    pm0.connect_to(&pm3.peer_info(), tcp::Tier::T2).await;
    pm0.wait_for_direct_connection(id3.clone()).await;
    pm0.connect_to(&pm4.peer_info(), tcp::Tier::T2).await;
    pm0.wait_for_direct_connection(id4.clone()).await;
    clock.advance(STORED_CONNECTIONS_MIN_DURATION);

    tracing::info!(target:"test", "check that pm0 stores the outbound connections");
    pm0.update_connection_store(&clock.clock()).await;
    check_recent_outbound_connections(
        &pm0,
        vec![id1.clone(), id2.clone(), id3.clone(), id4.clone()],
    )
    .await;

    tracing::info!(target:"test", "drop pm0 and start it again with the same db");
    drop(pm0);
    let pm0 = start_pm(clock.clock(), pm0_db, chain.make_config(rng), chain.clone()).await;

    tracing::info!(target:"test", "wait for pm0 to reconnect to the other nodes");
    pm0.wait_for_direct_connection(id1.clone()).await;
    pm0.wait_for_direct_connection(id2.clone()).await;
    pm0.wait_for_direct_connection(id3.clone()).await;
    pm0.wait_for_direct_connection(id4.clone()).await;
}
