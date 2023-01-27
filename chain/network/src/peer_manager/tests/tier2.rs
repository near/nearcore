use crate::network_protocol::testonly as data;
use crate::peer_manager::network_state::RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION;
use crate::peer_manager::network_state::RECONNECT_ATTEMPT_INTERVAL;
use crate::peer_manager::peer_manager_actor::UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL;
use crate::peer_manager::testonly::start as start_pm;
use crate::peer_manager::testonly::ActorHandler;
use crate::tcp;
use crate::testonly::make_rng;
use crate::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use near_store::db::TestDB;
use std::sync::Arc;

async fn check_recent_outbound_connections(pm: &ActorHandler, wanted: Vec<PeerId>) {
    let got: Vec<PeerId> = pm
        .with_state(move |s| async move {
            s.get_recent_outbound_connections().iter().map(|c| c.peer_info.id.clone()).collect()
        })
        .await;

    assert!(got == wanted, "Expected {:?} but got {:?}", wanted, got);
}

#[tokio::test]
async fn test_recent_outbound_connection() {
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

    // connect pm0 to pm1
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm2.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;

    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);

    // check that pm0 stores the outbound connection to pm1
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    // check that pm1 does not store anything, as it has no outbound connections
    check_recent_outbound_connections(&pm1, vec![]).await;

    // check that pm2 stores the outbound connection to pm0
    check_recent_outbound_connections(&pm2, vec![id0.clone()]).await;
}

#[tokio::test]
async fn test_recent_outbound_connection_after_disconnect() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    // connect pm0 to pm1
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);

    // check that pm0 stores the outbound connection to pm1
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    pm1.disconnect(&id0).await;
    pm0.wait_for_num_connected_peers(0).await;

    // check that pm0 retains the stored outbound connection to pm1 after disconnect
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;
}

#[tokio::test]
async fn test_outbound_connection_storage_order() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm3 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();
    let id3 = pm3.cfg.node_id();

    // connect pm0 to other pms, in order
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm0.wait_for_direct_connection(id1.clone()).await;
    clock.advance(time::Duration::seconds(1));
    pm0.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;
    pm0.wait_for_direct_connection(id2.clone()).await;
    clock.advance(time::Duration::seconds(1));
    pm0.connect_to(&pm3.peer_info(), tcp::Tier::T2).await;
    pm0.wait_for_direct_connection(id3.clone()).await;

    // check that the outbound connections are stored
    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);
    check_recent_outbound_connections(&pm0, vec![id3.clone(), id2.clone(), id1.clone()]).await;

    // disconnect pm2 and check that it remains in storage but moves to the end of the order
    pm2.disconnect(&id0).await;
    pm0.wait_for_num_connected_peers(2).await;
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);
    check_recent_outbound_connections(&pm0, vec![id3.clone(), id1.clone(), id2.clone()]).await;
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

    // connect pm0 to pm1
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);

    // check that pm0 stores the outbound connection to pm1
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    // drop pm0 and start it again with the same db, check that it reconnects
    drop(pm0);
    let pm0 = start_pm(clock.clock(), pm0_db, chain.make_config(rng), chain.clone()).await;
    pm0.wait_for_direct_connection(id1.clone()).await;
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

    tracing::info!(target:"test","connect pm0 to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);

    tracing::info!(target:"test","check that {} stores the outbound connection to {}",pm0.cfg.node_id(),pm1.cfg.node_id());
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test","drop {} and start it again with the same config, check that pm0 reconnects",pm1.cfg.node_id());
    drop(pm1);
    let _pm1 = start_pm(clock.clock(), TestDB::new(), pm1_cfg.clone(), chain.clone()).await;
    tracing::info!(target:"test","awaiting reconnect attempt");
    clock.advance(RECONNECT_ATTEMPT_INTERVAL);
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

    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);

    tracing::info!(target:"test", "check that pm0 stores the outbound connection to pm1");
    check_recent_outbound_connections(&pm0, vec![id1.clone()]).await;

    tracing::info!(target:"test", "have pm1 disconnect gracefully from pm0");
    pm1.disconnect(&id0).await;
    pm0.wait_for_num_connected_peers(0).await;

    tracing::info!(target:"test", "check that pm0 reconnects");
    clock.advance(RECONNECT_ATTEMPT_INTERVAL);
    pm0.wait_for_direct_connection(id1.clone()).await;
}
