use crate::network_protocol::testonly as data;
use crate::raw;
use crate::testonly;
use crate::time;
use near_o11y::testonly::init_test_logger;
use std::sync::Arc;

#[tokio::test]
async fn test_raw_conn_pings() {
    init_test_logger();
    let mut rng = testonly::make_rng(33955575545);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let cfg = chain.make_config(rng);
    let peer_id = cfg.node_id();
    let addr = cfg.node_addr.unwrap();
    let genesis_id = chain.genesis_id.clone();
    let _pm = crate::peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        cfg,
        chain,
    )
    .await;

    let mut conn = raw::Connection::connect(
        addr,
        peer_id.clone(),
        None,
        &genesis_id.chain_id,
        genesis_id.hash,
        0,
        time::Duration::SECOND,
    )
    .await
    .unwrap();

    let num_pings = 5;
    for nonce in 1..num_pings {
        conn.send_ping(&peer_id, nonce, 2).await.unwrap();
    }

    let mut nonce_received = 0;
    loop {
        let (msg, _timestamp) = conn.recv().await.unwrap();

        if let raw::ReceivedMessage::Pong { nonce, .. } = msg {
            if nonce != nonce_received + 1 {
                panic!(
                    "received out of order nonce {} when {} was expected",
                    nonce,
                    nonce_received + 1
                );
            }
            nonce_received = nonce;
            if nonce == num_pings - 1 {
                break;
            }
        }
    }
}
