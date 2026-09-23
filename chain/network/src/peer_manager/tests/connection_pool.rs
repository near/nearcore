use crate::config::SocketOptions;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{Disconnect, PeerMessage};
use crate::network_protocol::{Handshake, OwnedAccount, PartialEdgeInfo};
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager;
use crate::peer_manager::connection;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::tcp_transport::LIMIT_PENDING_PEERS;
use crate::private_messages::RegisterPeerError;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use crate::types::Edge;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::version::PROTOCOL_VERSION;
use std::sync::Arc;

// Verify that unsolicited inbound Tier3 connections are rejected.
#[tokio::test]
async fn unsolicited_tier3_rejected() {
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

    // Attempt an inbound Tier3 connection without any prior state sync request.
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T3, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(stream);
    stream
        .write(&PeerMessage::Tier3Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PROTOCOL_VERSION,
            sender_peer_id: cfg.node_id(),
            target_peer_id: pm.cfg.node_id(),
            sender_listen_port: Some(port),
            sender_chain_info: chain.get_peer_chain_info(),
            partial_edge_info: PartialEdgeInfo::new(
                &cfg.node_id(),
                &pm.cfg.node_id(),
                Edge::create_fresh_nonce(&clock.clock()),
                &cfg.node_key,
            ),
            owned_account: None,
        }))
        .await;
    let reason = events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(ev.reason),
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => {
                panic!("PeerManager accepted unsolicited Tier3 handshake")
            }
            _ => None,
        })
        .await;
    assert_eq!(
        ClosingReason::RejectedByPeerManager(RegisterPeerError::UnexpectedTier3Connection),
        reason,
    );
}

// Verify that an inbound Tier3 connection is accepted when we have a pending state sync
// request for that peer.
#[tokio::test]
async fn expected_tier3_accepted() {
    init_test_logger();
    let mut rng = make_rng(921853234);
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
    let peer_id = cfg.node_id();

    // Simulate having sent a state sync request to this peer by inserting into
    // pending_tier3_requests.
    let now = clock.clock().now();
    pm.with_state(move |s| async move {
        s.pending_tier3_requests.insert(peer_id, now);
    })
    .await;

    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T3, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(stream);
    stream
        .write(&PeerMessage::Tier3Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PROTOCOL_VERSION,
            sender_peer_id: cfg.node_id(),
            target_peer_id: pm.cfg.node_id(),
            sender_listen_port: Some(port),
            sender_chain_info: chain.get_peer_chain_info(),
            partial_edge_info: PartialEdgeInfo::new(
                &cfg.node_id(),
                &pm.cfg.node_id(),
                Edge::create_fresh_nonce(&clock.clock()),
                &cfg.node_key,
            ),
            owned_account: None,
        }))
        .await;
    events
        .recv_until(|ev| match ev {
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => Some(()),
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => {
                panic!("PeerManager rejected expected Tier3 handshake: {:?}", ev.reason)
            }
            _ => None,
        })
        .await;
}

// Verify that duplicate handshakes on established sessions close the connection immediately,
// so the keepalive attempt itself terminates the session.
#[tokio::test]
async fn duplicate_handshake_closes_tier3() {
    init_test_logger();
    let mut rng = make_rng(921853235);
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
    let peer_id = cfg.node_id();

    // establish a legitimate T3 connection (we "sent" a state request).
    let now = clock.clock().now();
    pm.with_state(move |s| async move {
        s.pending_tier3_requests.insert(peer_id, now);
    })
    .await;

    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T3, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(stream);
    let handshake = Handshake {
        protocol_version: PROTOCOL_VERSION,
        oldest_supported_version: PROTOCOL_VERSION,
        sender_peer_id: cfg.node_id(),
        target_peer_id: pm.cfg.node_id(),
        sender_listen_port: Some(port),
        sender_chain_info: chain.get_peer_chain_info(),
        partial_edge_info: PartialEdgeInfo::new(
            &cfg.node_id(),
            &pm.cfg.node_id(),
            Edge::create_fresh_nonce(&clock.clock()),
            &cfg.node_key,
        ),
        owned_account: None,
    };
    stream.write(&PeerMessage::Tier3Handshake(handshake.clone())).await;
    events
        .recv_until(|ev| match ev {
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => Some(()),
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => {
                panic!("handshake rejected: {:?}", ev.reason)
            }
            _ => None,
        })
        .await;

    // a duplicate handshake just before the 15-second idle timeout would close the connection
    clock.advance(time::Duration::seconds(14));
    stream.write(&PeerMessage::Tier3Handshake(handshake)).await;

    // the connection should be closed because duplicate handshakes are not tolerated
    let reason = events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(ev.reason),
            _ => None,
        })
        .await;
    assert_eq!(reason, ClosingReason::DisallowedMessage);
}

// Verify that a Disconnect message received on a T3 connection is accepted
// (not rejected as a disallowed message).
#[tokio::test]
async fn t3_disconnect() {
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
    let peer_id = cfg.node_id();

    // Register the peer as expected so the inbound T3 connection is accepted.
    let now = clock.clock().now();
    pm.with_state(move |s| async move {
        s.pending_tier3_requests.insert(peer_id, now);
    })
    .await;

    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T3, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(stream);
    stream
        .write(&PeerMessage::Tier3Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PROTOCOL_VERSION,
            sender_peer_id: cfg.node_id(),
            target_peer_id: pm.cfg.node_id(),
            sender_listen_port: Some(port),
            sender_chain_info: chain.get_peer_chain_info(),
            partial_edge_info: PartialEdgeInfo::new(
                &cfg.node_id(),
                &pm.cfg.node_id(),
                Edge::create_fresh_nonce(&clock.clock()),
                &cfg.node_key,
            ),
            owned_account: None,
        }))
        .await;

    // Wait for the handshake to complete.
    events
        .recv_until(|ev| match ev {
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => Some(()),
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => {
                panic!("handshake rejected: {:?}", ev.reason)
            }
            _ => None,
        })
        .await;

    // Send a Disconnect message from the peer side over T3.
    stream
        .write(&PeerMessage::Disconnect(Disconnect { remove_from_connection_store: false }))
        .await;

    let reason = events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(ev.reason),
            _ => None,
        })
        .await;
    assert_eq!(reason, ClosingReason::DisconnectMessage);
}

#[tokio::test]
async fn slow_test_connection_spam_security_test() {
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
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(stream);
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
                Edge::create_fresh_nonce(&clock.clock()),
                &pm.cfg.node_key,
            ),
            owned_account: None,
        }))
        .await;
    let reason = events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(ev.reason),
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => {
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
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let mut events = pm.events.from_now();
    let mut stream = Stream::new(stream);
    let cfg = chain.make_config(rng);
    let signer = cfg.validator.signer.get().unwrap();
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
                Edge::create_fresh_nonce(&clock.clock()),
                &cfg.node_key,
            ),
            owned_account: Some(
                OwnedAccount {
                    account_key: signer.public_key(),
                    // random peer_id, different than the expected cfg.node_id().
                    peer_id: data::make_peer_id(rng),
                    timestamp: clock.now_utc(),
                }
                .sign(&signer),
            ),
        }))
        .await;
    let reason = events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(ev.reason),
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => {
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
    cfg2.validator.clone_from(&cfg1.validator);

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
                account_key: cfg1.validator.signer.get().unwrap().public_key(),
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

    let nonce = Edge::create_fresh_nonce(&clock.clock());

    let testcases = [
        (
            "wrong key",
            PartialEdgeInfo::new(
                &cfg.node_id(),
                &pm.cfg.node_id(),
                nonce,
                &data::make_secret_key(rng),
            ),
        ),
        (
            "wrong source",
            PartialEdgeInfo::new(&data::make_peer_id(rng), &pm.cfg.node_id(), nonce, &cfg.node_key),
        ),
        (
            "wrong target",
            PartialEdgeInfo::new(&cfg.node_id(), &data::make_peer_id(rng), nonce, &cfg.node_key),
        ),
    ];

    for (name, edge) in &testcases {
        for tier in [tcp::Tier::T1, tcp::Tier::T2, tcp::Tier::T3] {
            tracing::info!(target:"test", %name, ?tier, "test case");
            let stream = tcp::Stream::connect(&pm.peer_info(), tier, &SocketOptions::default())
                .await
                .unwrap();
            let stream_id = stream.id();
            let port = stream.local_addr.port();
            let mut events = pm.events.from_now();
            let mut stream = Stream::new(stream);
            let signer = cfg.validator.signer.get().unwrap();
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
                        account_key: signer.public_key(),
                        peer_id: cfg.node_id(),
                        timestamp: clock.now_utc(),
                    }
                    .sign(&signer),
                ),
            };
            let handshake = match tier {
                tcp::Tier::T1 => PeerMessage::Tier1Handshake(handshake),
                tcp::Tier::T2 => PeerMessage::Tier2Handshake(handshake),
                tcp::Tier::T3 => PeerMessage::Tier3Handshake(handshake),
            };
            stream.write(&handshake).await;
            let reason = events
                .recv_until(|ev| match ev {
                    Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(ev.reason),
                    Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => {
                        panic!("PeerManager accepted the handshake")
                    }
                    _ => None,
                })
                .await;
            assert_eq!(
                ClosingReason::RejectedByPeerManager(RegisterPeerError::InvalidEdge),
                reason,
            );
        }
    }

    // Dedicated T3 sub-case: insert the peer into pending_tier3_requests so the
    // connection passes the authorization check, then verify the invalid edge is
    // still caught, and that the pending entry is NOT consumed by the failed attempt.
    for (name, edge) in &testcases {
        tracing::info!(target:"test", %name, tier=?tcp::Tier::T3, "test case (pending T3)");

        let peer_id = cfg.node_id();
        let now = clock.clock().now();
        pm.with_state(move |s| async move {
            s.pending_tier3_requests.insert(peer_id, now);
        })
        .await;

        let stream =
            tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T3, &SocketOptions::default())
                .await
                .unwrap();
        let stream_id = stream.id();
        let port = stream.local_addr.port();
        let mut events = pm.events.from_now();
        let mut stream = Stream::new(stream);
        let signer = cfg.validator.signer.get().unwrap();
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
                    account_key: signer.public_key(),
                    peer_id: cfg.node_id(),
                    timestamp: clock.now_utc(),
                }
                .sign(&signer),
            ),
        };
        stream.write(&PeerMessage::Tier3Handshake(handshake)).await;
        let reason = events
            .recv_until(|ev| match ev {
                Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(ev.reason),
                Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => {
                    panic!("PeerManager accepted the handshake")
                }
                _ => None,
            })
            .await;
        assert_eq!(ClosingReason::RejectedByPeerManager(RegisterPeerError::InvalidEdge), reason,);

        // The pending entry must survive the failed attempt so the real peer
        // can still connect.
        let peer_id = cfg.node_id();
        let still_pending = pm
            .with_state(move |s| async move { s.pending_tier3_requests.contains_key(&peer_id) })
            .await;
        assert!(still_pending, "pending_tier3_requests entry was consumed by a failed edge check");
    }
}
