use crate::network_protocol::testonly as data;
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager;
use crate::peer_manager::connection;
use crate::private_actix::RegisterPeerError;
use crate::tcp;
use crate::testonly::make_rng;
use near_async::time;
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
    let outbound_conn = pm.start_outbound(chain.clone(), cfgs[2].clone(), tcp::Tier::T2).await;
    let inbound_conn = pm.start_inbound(chain.clone(), cfgs[2].clone()).await;
    // inbound should be rejected, outbound accepted.
    assert_eq!(
        ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
            connection::PoolError::AlreadyStartedConnecting
        )),
        inbound_conn.manager_fail_handshake(&clock.clock()).await,
    );
    outbound_conn.handshake(&clock.clock()).await;

    // pm.id is higher
    let outbound_conn = pm.start_outbound(chain.clone(), cfgs[0].clone(), tcp::Tier::T2).await;
    let inbound_conn = pm.start_inbound(chain.clone(), cfgs[0].clone()).await;
    // inbound should be accepted, outbound rejected by PM.
    let inbound = inbound_conn.handshake(&clock.clock()).await;
    assert_eq!(
        ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
            connection::PoolError::AlreadyConnected
        )),
        outbound_conn.manager_fail_handshake(&clock.clock()).await,
    );
    drop(inbound);
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
    let conn1 = pm.start_outbound(chain.clone(), cfg.clone(), tcp::Tier::T2).await;
    let conn2 = pm.start_outbound(chain.clone(), cfg.clone(), tcp::Tier::T2).await;
    // conn2 shouldn't even be started, so it should fail before conn1 completes.
    assert_eq!(
        ClosingReason::OutboundNotAllowed(connection::PoolError::AlreadyStartedConnecting),
        conn2.manager_fail_handshake(&clock.clock()).await,
    );
    conn1.handshake(&clock.clock()).await;

    // Double inbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    let conn2 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    // Second inbound should be rejected.
    let conn1 = conn1.handshake(&clock.clock()).await;
    assert_eq!(
        ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
            connection::PoolError::AlreadyConnected
        )),
        conn2.manager_fail_handshake(&clock.clock()).await,
    );
    pm.check_consistency().await;
    drop(conn1);

    // Inbound then outbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    let conn1 = conn1.handshake(&clock.clock()).await;
    let conn2 = pm.start_outbound(chain.clone(), cfg.clone(), tcp::Tier::T2).await;
    assert_eq!(
        ClosingReason::OutboundNotAllowed(connection::PoolError::AlreadyConnected),
        conn2.manager_fail_handshake(&clock.clock()).await,
    );
    drop(conn1);

    // Outbound then inbound.
    let cfg = chain.make_config(rng);
    let conn1 = pm.start_outbound(chain.clone(), cfg.clone(), tcp::Tier::T2).await;
    let conn1 = conn1.handshake(&clock.clock()).await;
    let conn2 = pm.start_inbound(chain.clone(), cfg.clone()).await;
    assert_eq!(
        ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
            connection::PoolError::AlreadyConnected
        )),
        conn2.manager_fail_handshake(&clock.clock()).await,
    );
    drop(conn1);
}
