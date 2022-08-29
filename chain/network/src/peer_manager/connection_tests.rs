use crate::peer_manager;
use near_logger_utils::init_test_logger;
use crate::testonly::make_rng;
use crate::network_protocol::testonly as data;
use crate::time;
use std::sync::Arc;
use near_store::test_utils::create_test_store;

#[tokio::test]
async fn connection_tie_break() {
    init_test_logger();
    let mut rng = make_rng(33955575545);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut cfgs : Vec<_> = (0..3).map(|_|chain.make_config(rng)).collect();
    cfgs.sort_by_key(|c|c.node_id());

    let pm = peer_manager::testonly::start(
        clock.clock(),
        create_test_store(),
        cfgs[1].clone(),
        chain.clone(),
    ).await;
   
    // pm.id is lower
    let outbound_conn = pm.start_outbound(chain.clone(),cfgs[2].clone()).await;
    let inbound_conn = pm.start_inbound(chain.clone(),cfgs[2].clone()).await;
    // inbound should be rejected, outbound accepted.
    inbound_conn.fail_handshake(&clock.clock()).await;
    outbound_conn.handshake(&clock.clock()).await;

    // pm.id is higher
    let outbound_conn = pm.start_outbound(chain.clone(),cfgs[0].clone()).await;
    let inbound_conn = pm.start_inbound(chain.clone(),cfgs[0].clone()).await;
    // inbound should be accepted, outbound rejected by PM.
    inbound_conn.handshake(&clock.clock()).await;
    outbound_conn.fail_handshake(&clock.clock()).await;
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
        create_test_store(),
        chain.make_config(rng),
        chain.clone(),
    ).await;

    // Double outbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_outbound(chain.clone(),cfg.clone()).await;
    let conn2 = pm.start_outbound(chain.clone(),cfg.clone()).await;
    // conn2 shouldn't even be started, so it should fail before conn1 completes.
    conn2.fail_handshake(&clock.clock()).await;
    conn1.handshake(&clock.clock()).await;
    
    // Double inbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_inbound(chain.clone(),cfg.clone()).await;
    conn1.handshake(&clock.clock()).await;
    // Second inbound should be rejected. conn1 handshake has to
    // be completed first though, otherwise we would have a race condition.
    let conn2 = pm.start_inbound(chain.clone(),cfg.clone()).await;
    conn2.fail_handshake(&clock.clock()).await;

    // Inbound then outbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_inbound(chain.clone(),cfg.clone()).await;
    conn1.handshake(&clock.clock()).await;
    let conn2 = pm.start_outbound(chain.clone(),cfg.clone()).await;
    conn2.fail_handshake(&clock.clock()).await;

    // Outbound then inbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_outbound(chain.clone(),cfg.clone()).await;
    conn1.handshake(&clock.clock()).await;
    let conn2 = pm.start_inbound(chain.clone(),cfg.clone()).await;
    conn2.fail_handshake(&clock.clock()).await;
}
