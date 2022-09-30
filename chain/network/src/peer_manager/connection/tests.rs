use crate::network_protocol::testonly as data;
use crate::peer_manager;
use crate::testonly::make_rng;
use crate::time;
use near_o11y::testonly::init_test_logger;
use std::sync::Arc;

#[tokio::test]
async fn connection_tie_break() {
    init_test_logger();
    let mut rng = make_rng(33955575545);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut cfgs: Vec<_> = (0..3).map(|_| chain.make_config(rng)).collect();
    cfgs.sort_by_key(|c| c.node_id());

    let pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        cfgs[1].clone(),
        chain.clone(),
    )
    .await;

    // pm.id is lower
    tracing::debug!("PHASE1");
    let outbound_conn = pm.start_outbound(chain.clone(), cfgs[2].clone()).await;
    let inbound_conn = pm.start_inbound(chain.clone(), cfgs[2].clone()).await;
    // inbound should be rejected, outbound accepted.
    inbound_conn.fail_handshake(&clock.clock()).await;
    outbound_conn.handshake(&clock.clock()).await;

    // pm.id is higher
    tracing::debug!("PHASE2");
    let outbound_conn = pm.start_outbound(chain.clone(), cfgs[0].clone()).await;
    let inbound_conn = pm.start_inbound(chain.clone(), cfgs[0].clone()).await;
    // inbound should be accepted, outbound rejected by PM.
    tracing::debug!("PHASE2b");
    let inbound = inbound_conn.handshake(&clock.clock()).await;
    tracing::debug!("PHASE2c");
    outbound_conn.fail_handshake(&clock.clock()).await;
    drop(inbound);

    tracing::debug!("PHASE3");
}

#[tokio::test]
async fn duplicate_connections() {
    init_test_logger();
    let mut rng = make_rng(33955575545);
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

    // Double outbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_outbound(chain.clone(), cfg.clone()).await;
    let conn2 = pm.start_outbound(chain.clone(), cfg.clone()).await;
    // conn2 shouldn't even be started, so it should fail before conn1 completes.
    conn2.fail_handshake(&clock.clock()).await;
    conn1.handshake(&clock.clock()).await;

    // Double inbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    let conn1 = conn1.handshake(&clock.clock()).await;
    // Second inbound should be rejected. conn1 handshake has to
    // be completed first though, otherwise we would have a race condition.
    let conn2 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    conn2.fail_handshake(&clock.clock()).await;
    drop(conn1);

    // Inbound then outbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    let conn1 = conn1.handshake(&clock.clock()).await;
    let conn2 = pm.start_outbound(chain.clone(), cfg.clone()).await;
    conn2.fail_handshake(&clock.clock()).await;
    drop(conn1);

    // Outbound then inbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_outbound(chain.clone(), cfg.clone()).await;
    let conn1 = conn1.handshake(&clock.clock()).await;
    let conn2 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    conn2.fail_handshake(&clock.clock()).await;
    drop(conn1);
}
