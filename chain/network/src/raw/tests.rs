use crate::network_protocol::testonly as data;
use crate::raw;
use crate::tcp;
use crate::testonly;
use crate::types::PeerInfo;
use near_async::time;
use near_crypto::{KeyType, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::ShardId;
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
    let addr = **cfg.node_addr.as_ref().unwrap();
    let genesis_id = chain.genesis_id.clone();
    let _pm = crate::peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        cfg,
        chain,
    )
    .await;

    let mut conn = raw::Connection::connect(
        &clock.clock(),
        addr,
        peer_id.clone(),
        None,
        &genesis_id.chain_id,
        genesis_id.hash,
        0,
        vec![ShardId::new(0)],
        Some(time::Duration::SECOND),
    )
    .await
    .unwrap();

    let num_pings = 5;
    for nonce in 1..num_pings {
        conn.send_routed_message(raw::RoutedMessage::Ping { nonce }, peer_id.clone(), 2)
            .await
            .unwrap();
    }

    let mut nonce_received = 0;
    loop {
        let (msg, _timestamp) = conn.recv().await.unwrap();

        if let raw::Message::Routed(raw::RoutedMessage::Pong { nonce, .. }) = msg {
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

#[tokio::test]
async fn test_raw_conn_state_parts() {
    init_test_logger();
    let mut rng = testonly::make_rng(33955575545);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let cfg = chain.make_config(rng);
    let peer_id = cfg.node_id();
    let addr = **cfg.node_addr.as_ref().unwrap();
    let genesis_id = chain.genesis_id.clone();
    let _pm = crate::peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        cfg,
        chain,
    )
    .await;

    let mut conn = raw::Connection::connect(
        &clock.clock(),
        addr,
        peer_id.clone(),
        None,
        &genesis_id.chain_id,
        genesis_id.hash,
        0,
        vec![ShardId::new(0)],
        Some(time::Duration::SECOND),
    )
    .await
    .unwrap();

    let num_parts = 5;
    // Block hash needs to correspond to the hash of the first block of an epoch.
    // But the fake node simply ignores the block hash.
    let block_hash = CryptoHash::new();
    for part_id in 0..num_parts {
        conn.send_message(raw::DirectMessage::StateRequestPart(
            ShardId::new(0),
            block_hash,
            part_id,
        ))
        .await
        .unwrap();
    }

    let mut part_id_received = -1i64;
    loop {
        match conn.recv().await {
            Ok((msg, _timestamp)) => {
                if let raw::Message::Direct(raw::DirectMessage::VersionedStateResponse(
                    state_response,
                )) = msg
                {
                    let response = state_response.take_state_response();
                    let part_id = response.part_id();
                    if part_id.is_none() || part_id.unwrap() as i64 != (part_id_received + 1) {
                        panic!(
                            "received out of order part_id {:?} when {} was expected",
                            part_id,
                            part_id_received + 1
                        );
                    }
                    part_id_received = part_id.unwrap() as i64;
                    if part_id_received + 1 == num_parts as i64 {
                        break;
                    }
                }
            }
            Err(e) => {
                panic!("error receiving part: {:?}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_listener() {
    init_test_logger();
    let mut rng = testonly::make_rng(33955575545);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let mut cfg = chain.make_config(rng);
    let genesis_id = chain.genesis_id.clone();

    let addr = tcp::ListenerAddr::reserve_for_test();
    let secret_key = SecretKey::from_random(KeyType::ED25519);
    let peer_id = PeerId::new(secret_key.public_key());
    cfg.peer_store.boot_nodes.push(PeerInfo::new(peer_id, *addr));
    cfg.outbound_disabled = false;
    let _pm = crate::peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        cfg,
        chain,
    )
    .await;

    let mut l = raw::Listener::bind(
        addr,
        secret_key,
        &genesis_id.chain_id,
        genesis_id.hash,
        0,
        vec![ShardId::new(0)],
        false,
        Some(time::Duration::SECOND),
        None,
    )
    .unwrap();

    let mut conn = l.accept().await.unwrap();
    // just test that we can receive some message, which checks that
    // at least the handshake logic has gotten exercised somewhat
    let _ = conn.recv().await.unwrap();
}
