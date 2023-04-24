use crate::network_protocol::testonly as data;
use crate::network_protocol::PeerMessage;
use crate::network_protocol::{Encoding, Handshake, OwnedAccount, PartialEdgeInfo};
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::LIMIT_PENDING_PEERS;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::Event;
use crate::private_actix::RegisterPeerError;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::version::PROTOCOL_VERSION;
use std::sync::Arc;

#[tokio::test]
async fn connection_spam_security_test() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    const PEERS_OVER_LIMIT: usize = 10;
    let mut cfg = chain.make_config(rng);
    cfg.max_num_peers = (LIMIT_PENDING_PEERS + PEERS_OVER_LIMIT) as u32;
    let mut cfg = chain.make_config(rng);
    // Make sure that connections will never get dropped.
    cfg.handshake_timeout = time::Duration::hours(1);
    let pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        cfg,
        chain.clone(),
    )
    .await;

    // Saturate the pending connections limit.
    let mut conns = vec![];
    for _ in 0..LIMIT_PENDING_PEERS {
        conns.push(pm.start_inbound(chain.clone(), chain.make_config(rng)).await);
    }
    // Try to establish additional connections. Should fail.
    for _ in 0..PEERS_OVER_LIMIT {
        let conn = pm.start_inbound(chain.clone(), chain.make_config(rng)).await;
        assert_eq!(
            ClosingReason::TooManyInbound,
            conn.manager_fail_handshake(&clock.clock()).await
        );
    }
    // Terminate the pending connections. Should succeed.
    for c in conns {
        c.handshake(&clock.clock()).await;
    }
}

#[tokio::test]
async fn loop_connection() {
    init_test_logger();
    let mut rng = make_rng(921853233);
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
    let mut cfg = chain.make_config(rng);
    cfg.node_key = pm.cfg.node_key.clone();

    // Starting an outbound loop connection on TIER2 should be stopped without sending the handshake.
    let conn = pm.start_outbound(chain.clone(), cfg, tcp::Tier::T2).await;
    assert_eq!(
        ClosingReason::OutboundNotAllowed(connection::PoolError::UnexpectedLoopConnection),
        conn.manager_fail_handshake(&clock.clock()).await
    );

    // An inbound connection pretending to be a loop should be rejected.
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(Some(Encoding::Proto), stream);
    stream
        .write(&PeerMessage::Tier2Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PROTOCOL_VERSION,
            sender_peer_id: pm.cfg.node_id(),
            target_peer_id: pm.cfg.node_id(),
            sender_listen_port: Some(port),
            sender_chain_info: chain.get_peer_chain_info(),
            partial_edge_info: PartialEdgeInfo::new(
                &pm.cfg.node_id(),
                &pm.cfg.node_id(),
                1,
                &pm.cfg.node_key,
            ),
            owned_account: None,
        }))
        .await;
    let reason = events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                Some(ev.reason)
            }
            Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                panic!("PeerManager accepted the handshake")
            }
            _ => None,
        })
        .await;
    assert_eq!(
        ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
            connection::PoolError::UnexpectedLoopConnection
        )),
        reason
    );
}

#[tokio::test]
async fn owned_account_mismatch() {
    init_test_logger();
    let mut rng = make_rng(921853233);
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

    // An inbound connection pretending to be a loop should be rejected.
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(Some(Encoding::Proto), stream);
    let cfg = chain.make_config(rng);
    let vc = cfg.validator.clone().unwrap();
    stream
        .write(&PeerMessage::Tier2Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PROTOCOL_VERSION,
            sender_peer_id: cfg.node_id(),
            target_peer_id: pm.cfg.node_id(),
            sender_listen_port: Some(port),
            sender_chain_info: chain.get_peer_chain_info(),
            partial_edge_info: PartialEdgeInfo::new(
                &cfg.node_id(),
                &pm.cfg.node_id(),
                1,
                &cfg.node_key,
            ),
            owned_account: Some(
                OwnedAccount {
                    account_key: vc.signer.public_key().clone(),
                    // random peer_id, different than the expected cfg.node_id().
                    peer_id: data::make_peer_id(rng),
                    timestamp: clock.now_utc(),
                }
                .sign(vc.signer.as_ref()),
            ),
        }))
        .await;
    let reason = events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                Some(ev.reason)
            }
            Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                panic!("PeerManager accepted the handshake")
            }
            _ => None,
        })
        .await;
    assert_eq!(ClosingReason::OwnedAccountMismatch, reason);
}

#[tokio::test]
async fn owned_account_conflict() {
    init_test_logger();
    let mut rng = make_rng(921853233);
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

    let cfg1 = chain.make_config(rng);
    let mut cfg2 = chain.make_config(rng);
    cfg2.validator = cfg1.validator.clone();

    // Start 2 connections with the same account_key.
    // The second should be rejected.
    let conn1 = pm.start_inbound(chain.clone(), cfg1.clone()).await.handshake(&clock.clock()).await;
    let reason =
        pm.start_inbound(chain.clone(), cfg2).await.manager_fail_handshake(&clock.clock()).await;
    assert_eq!(
        reason,
        ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
            connection::PoolError::AlreadyConnectedAccount {
                peer_id: cfg1.node_id(),
                account_key: cfg1.validator.as_ref().unwrap().signer.public_key(),
            }
        ))
    );
    drop(conn1);
    drop(pm);
}

#[tokio::test]
async fn invalid_edge() {
    init_test_logger();
    let mut rng = make_rng(921853233);
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
    let cfg = chain.make_config(rng);
    let chain_info = peer_manager::testonly::make_chain_info(&chain, &[&cfg]);
    pm.set_chain_info(chain_info).await;

    let testcases = [
        (
            "wrong key",
            PartialEdgeInfo::new(&cfg.node_id(), &pm.cfg.node_id(), 1, &data::make_secret_key(rng)),
        ),
        (
            "wrong source",
            PartialEdgeInfo::new(&data::make_peer_id(rng), &pm.cfg.node_id(), 1, &cfg.node_key),
        ),
        (
            "wrong target",
            PartialEdgeInfo::new(&cfg.node_id(), &data::make_peer_id(rng), 1, &cfg.node_key),
        ),
    ];

    for (name, edge) in &testcases {
        for tier in [tcp::Tier::T1, tcp::Tier::T2] {
            tracing::info!(target:"test","{name} {tier:?}");
            let stream = tcp::Stream::connect(&pm.peer_info(), tier).await.unwrap();
            let stream_id = stream.id();
            let port = stream.local_addr.port();
            let mut events = pm.events.from_now();
            let mut stream = Stream::new(Some(Encoding::Proto), stream);
            let vc = cfg.validator.clone().unwrap();
            let handshake = Handshake {
                protocol_version: PROTOCOL_VERSION,
                oldest_supported_version: PROTOCOL_VERSION,
                sender_peer_id: cfg.node_id(),
                target_peer_id: pm.cfg.node_id(),
                sender_listen_port: Some(port),
                sender_chain_info: chain.get_peer_chain_info(),
                partial_edge_info: edge.clone(),
                owned_account: Some(
                    OwnedAccount {
                        account_key: vc.signer.public_key().clone(),
                        peer_id: cfg.node_id(),
                        timestamp: clock.now_utc(),
                    }
                    .sign(vc.signer.as_ref()),
                ),
            };
            let handshake = match tier {
                tcp::Tier::T1 => PeerMessage::Tier1Handshake(handshake),
                tcp::Tier::T2 => PeerMessage::Tier2Handshake(handshake),
            };
            stream.write(&handshake).await;
            let reason = events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                        Some(ev.reason)
                    }
                    Event::PeerManager(PME::HandshakeCompleted(ev))
                        if ev.stream_id == stream_id =>
                    {
                        panic!("PeerManager accepted the handshake")
                    }
                    _ => None,
                })
                .await;
            assert_eq!(
                ClosingReason::RejectedByPeerManager(RegisterPeerError::InvalidEdge),
                reason
            );
        }
    }
}
