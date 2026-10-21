use crate::broadcast;
use crate::client::{StatePartOrHeader, StateRequestHeader, StateRequestPart};
use crate::config::SocketOptions;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Handshake, PartialEdgeInfo, PeerMessage, SnapshotHostInfo, StateRequestAck, StateResponseInfo,
    StateResponseInfoV2, T2MessageBody, TieredMessageBody,
};
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager;
use crate::peer_manager::connection::PoolError;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::testonly::ActorHandler;
use crate::private_messages::RegisterPeerError;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use crate::types::{
    Edge, NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
    StateRequestSenderForNetwork,
};
use futures::future::join_all;
use near_async::messaging::{AsyncSender, CanSendAsync};
use near_async::time;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_part::StatePartIndex;
use near_primitives::state_sync::{
    PartOrHeader, ShardStateSyncResponse, ShardStateSyncResponseV2, StateRequestAckBody,
};
use near_primitives::types::ShardId;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::db::TestDB;
use rand::Rng;
use std::sync::Arc;

/// Limit on inbound Tier3 connections, kept low so that the tests don't need many file
/// descriptors.
const MAX_INBOUND_TIER3_CONNECTIONS: usize = 8;

/// The response which a host started with `start_pm` sends for a state request. The network
/// doesn't look into the response, so it is left empty.
fn state_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    part_idx: Option<StatePartIndex>,
) -> StateResponseInfo {
    let state_response = ShardStateSyncResponse::V2(ShardStateSyncResponseV2 {
        header: None,
        part: part_idx.map(|part_idx| (part_idx, vec![])),
    });
    StateResponseInfo::V2(Box::new(StateResponseInfoV2 { shard_id, sync_hash, state_response }))
}

/// Starts a peer manager to which the tests connect. It answers all state requests.
async fn start_pm(
    clock: &time::Clock,
    chain: &Arc<data::Chain>,
    rng: &mut impl Rng,
) -> ActorHandler {
    let mut cfg = chain.make_config(rng);
    cfg.max_inbound_tier3_connections = MAX_INBOUND_TIER3_CONNECTIONS as u32;
    let state_request_sender = StateRequestSenderForNetwork {
        state_request_header: AsyncSender::from_fn(|StateRequestHeader { shard_id, sync_hash }| {
            Some(StatePartOrHeader(Box::new(state_response(shard_id, sync_hash, None))))
        }),
        state_request_part: AsyncSender::from_fn(
            |StateRequestPart { shard_id, sync_hash, part_idx }| {
                Some(StatePartOrHeader(Box::new(state_response(
                    shard_id,
                    sync_hash,
                    Some(part_idx),
                ))))
            },
        ),
    };
    peer_manager::testonly::start_with_state_request_sender(
        clock.clone(),
        TestDB::new(),
        cfg,
        chain.clone(),
        state_request_sender,
    )
    .await
}

/// Opens a Tier3 connection to `pm` and sends a handshake as the owner of `key`.
async fn connect_tier3(
    pm: &ActorHandler,
    chain: &data::Chain,
    key: &SecretKey,
    clock: &time::Clock,
) -> (tcp::StreamId, Stream) {
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T3, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let peer_id = PeerId::new(key.public_key());
    let mut stream = Stream::new(stream);
    stream
        .write(&PeerMessage::Tier3Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PROTOCOL_VERSION,
            sender_peer_id: peer_id.clone(),
            target_peer_id: pm.cfg.node_id(),
            sender_listen_port: Some(port),
            sender_chain_info: chain.get_peer_chain_info(),
            partial_edge_info: PartialEdgeInfo::new(
                &peer_id,
                &pm.cfg.node_id(),
                Edge::create_fresh_nonce(clock),
                key,
            ),
            owned_account: None,
        }))
        .await;
    (stream_id, stream)
}

/// Waits until the peer manager accepts or rejects the handshake on the stream.
async fn handshake_result(
    events: &mut broadcast::Receiver<Event>,
    stream_id: tcp::StreamId,
) -> Result<(), ClosingReason> {
    events
        .recv_until(|ev| match ev {
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => Some(Ok(())),
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(Err(ev.reason)),
            _ => None,
        })
        .await
}

/// Connects to `pm` as a new requester and waits for its handshake.
async fn connect_requester(
    pm: &ActorHandler,
    chain: &data::Chain,
    clock: &time::Clock,
    rng: &mut impl Rng,
) -> (tcp::StreamId, Stream) {
    let mut events = pm.events.from_now();
    let (stream_id, mut stream) =
        connect_tier3(pm, chain, &data::make_secret_key(rng), clock).await;
    assert_eq!(handshake_result(&mut events, stream_id).await, Ok(()));
    let msg = stream.read().await.unwrap();
    assert!(matches!(msg, PeerMessage::Tier3Handshake(_)), "unexpected message: {msg}");
    (stream_id, stream)
}

// Verify that a node accepts Tier3 connections from peers which request state from it.
#[tokio::test]
async fn requester_accepted() {
    init_test_logger();
    let mut rng = make_rng(921853240);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    connect_requester(&pm, &chain, &clock, rng).await;
}

// Verify that a node accepts at most MAX_INBOUND_TIER3_CONNECTIONS requesters at a time,
// that hosts answering our own requests aren't limited, and that closing a requester's
// connection makes room for another.
#[tokio::test]
async fn requesters_limited() {
    init_test_logger();
    let mut rng = make_rng(921853241);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let mut requesters = vec![];
    for _ in 0..MAX_INBOUND_TIER3_CONNECTIONS {
        requesters.push(connect_requester(&pm, &chain, &clock, rng).await);
    }

    let mut events = pm.events.from_now();
    let (stream_id, _stream) =
        connect_tier3(&pm, &chain, &data::make_secret_key(rng), &clock).await;
    assert_eq!(
        handshake_result(&mut events, stream_id).await,
        Err(ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
            PoolError::TooManyTier3Requesters
        ))),
    );

    // A host answering our own state request is accepted regardless of the limit.
    let host_key = data::make_secret_key(rng);
    let host = PeerId::new(host_key.public_key());
    let now = clock.now();
    pm.with_state(move |s| async move {
        s.pending_tier3_requests.insert(host, now);
    })
    .await;
    let mut events = pm.events.from_now();
    let (stream_id, _host_stream) = connect_tier3(&pm, &chain, &host_key, &clock).await;
    assert_eq!(handshake_result(&mut events, stream_id).await, Ok(()));

    let (closed_id, closed) = requesters.pop().unwrap();
    let mut events = pm.events.from_now();
    drop(closed);
    events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == closed_id => Some(()),
            _ => None,
        })
        .await;
    connect_requester(&pm, &chain, &clock, rng).await;
}

// Verify that requesters handshaking concurrently can't exceed MAX_INBOUND_TIER3_CONNECTIONS.
#[tokio::test]
async fn concurrent_requesters_limited() {
    init_test_logger();
    let mut rng = make_rng(921853245);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    // Leave room for a few requesters, then let twice as many handshake at once.
    const FREE: usize = 4;
    let pm = start_pm(&clock, &chain, rng).await;
    let mut requesters = vec![];
    for _ in 0..MAX_INBOUND_TIER3_CONNECTIONS - FREE {
        requesters.push(connect_requester(&pm, &chain, &clock, rng).await);
    }
    let events = pm.events.from_now();
    let keys: Vec<SecretKey> = (0..2 * FREE).map(|_| data::make_secret_key(rng)).collect();
    let racing = join_all(keys.iter().map(|key| connect_tier3(&pm, &chain, key, &clock))).await;

    let mut accepted = 0;
    for (stream_id, _) in &racing {
        match handshake_result(&mut events.clone(), *stream_id).await {
            Ok(()) => accepted += 1,
            Err(reason) => assert_eq!(
                reason,
                ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(
                    PoolError::TooManyTier3Requesters
                ))
            ),
        }
    }
    assert_eq!(accepted, FREE);
}

// Verify that a host sends the acks and the responses for state header and part requests over
// the Tier3 connection on which the requests arrived.
#[tokio::test]
async fn requests_answered_over_tier3() {
    init_test_logger();
    let mut rng = make_rng(921853242);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let (_, mut stream) = connect_requester(&pm, &chain, &clock, rng).await;

    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let part_idx = 7;
    for (request, part_or_header, part_idx) in [
        (PeerMessage::StateRequestHeader(shard_id, sync_hash), PartOrHeader::Header, None),
        (
            PeerMessage::StateRequestPart(shard_id, sync_hash, part_idx),
            PartOrHeader::Part { part_idx },
            Some(part_idx),
        ),
    ] {
        stream.write(&request).await;
        let ack = StateRequestAck {
            shard_id,
            sync_hash,
            part_or_header,
            body: StateRequestAckBody::WillRespond,
        };
        assert_eq!(stream.read().await.unwrap(), PeerMessage::StateRequestAck(ack));
        assert_eq!(
            stream.read().await.unwrap(),
            PeerMessage::VersionedStateResponse(state_response(shard_id, sync_hash, part_idx)),
        );
    }
}

// Verify that a host acks over the Tier3 connection a request which it fails to answer.
#[tokio::test]
async fn failed_request_acknowledged_over_tier3() {
    init_test_logger();
    let mut rng = make_rng(921853243);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    // Without a state request handler, the host fails all requests.
    let pm = peer_manager::testonly::start(
        clock.clone(),
        TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let (_, mut stream) = connect_requester(&pm, &chain, &clock, rng).await;

    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    stream.write(&PeerMessage::StateRequestHeader(shard_id, sync_hash)).await;
    let ack = StateRequestAck {
        shard_id,
        sync_hash,
        part_or_header: PartOrHeader::Header,
        body: StateRequestAckBody::Error,
    };
    assert_eq!(stream.read().await.unwrap(), PeerMessage::StateRequestAck(ack));
}

// Verify that a node accepts an ack for its state request over a Tier3 connection.
#[tokio::test]
async fn ack_accepted_over_tier3() {
    init_test_logger();
    let mut rng = make_rng(921853244);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = peer_manager::testonly::start(
        clock.clone(),
        TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    // Simulate having sent a state request to the host, so that its connection is accepted.
    let host_key = data::make_secret_key(rng);
    let host = PeerId::new(host_key.public_key());
    let now = clock.now();
    pm.with_state(move |s| async move {
        s.pending_tier3_requests.insert(host, now);
    })
    .await;
    let mut events = pm.events.from_now();
    let (stream_id, mut stream) = connect_tier3(&pm, &chain, &host_key, &clock).await;
    assert_eq!(handshake_result(&mut events, stream_id).await, Ok(()));

    let ack = StateRequestAck {
        shard_id: ShardId::new(0),
        sync_hash: CryptoHash::hash_borsh(rng.r#gen::<u64>()),
        part_or_header: PartOrHeader::Header,
        body: StateRequestAckBody::Busy,
    };
    stream.write(&PeerMessage::StateRequestAck(ack.clone())).await;
    let processed = events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(tcp::Tier::T3, PeerMessage::StateRequestAck(ack)) => Some(ack),
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => {
                panic!("connection closed: {}", ev.reason)
            }
            _ => None,
        })
        .await;
    assert_eq!(processed, ack);
}

// Verify that a host acks routed state requests over Tier2, and sends the responses over a Tier3
// connection which it opens to the requester.
#[tokio::test]
async fn routed_requests_answered_over_tier3() {
    init_test_logger();
    let mut rng = make_rng(921853246);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let host = start_pm(&clock, &chain, rng).await;
    // The requester puts its address in the requests, so that the host can connect to it.
    let mut cfg = chain.make_config(rng);
    cfg.tier3_public_addr = cfg.node_addr.as_ref().map(|addr| **addr);
    let requester =
        peer_manager::testonly::start(clock.clone(), TestDB::new(), cfg, chain.clone()).await;
    let host_id = host.cfg.node_id();
    let requester_id = requester.cfg.node_id();
    requester.connect_to(&host.peer_info(), tcp::Tier::T2).await;
    requester.wait_for_routing_table(&[(host_id.clone(), vec![host_id.clone()])]).await;
    host.wait_for_routing_table(&[(requester_id.clone(), vec![requester_id])]).await;

    // Let the requester know that the host has a snapshot of the shard.
    let shard_id = ShardId::new(0);
    let snapshot_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let info = SnapshotHostInfo::new(
        host_id.clone(),
        snapshot_hash,
        0,
        vec![shard_id],
        None,
        &host.cfg.node_key,
    );
    requester
        .with_state(move |s| async move { s.snapshot_hosts.insert_skip_verify(Arc::new(info)) })
        .await;

    // The host connects to the requester to answer the first request, and reuses the connection
    // for the second.
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let part_idx = 7;
    for (request, part_or_header, part_idx) in [
        (
            NetworkRequests::StateRequestPart { shard_id, sync_hash, snapshot_hash, part_idx },
            PartOrHeader::Part { part_idx },
            Some(part_idx),
        ),
        (
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, snapshot_hash },
            PartOrHeader::Header,
            None,
        ),
    ] {
        let events = requester.events.from_now();
        let response: PeerManagerMessageResponse = requester
            .actor
            .send_async(PeerManagerMessageRequest::NetworkRequests(request))
            .await
            .unwrap();
        assert_eq!(
            response.as_network_response(),
            NetworkResponses::SelectedDestination(host_id.clone())
        );

        let ack = StateRequestAck {
            shard_id,
            sync_hash,
            part_or_header,
            body: StateRequestAckBody::WillRespond,
        };
        let want_ack: TieredMessageBody = T2MessageBody::StateRequestAck(ack).into();
        events
            .clone()
            .recv_until(|ev| match ev {
                Event::MessageProcessed(tcp::Tier::T2, PeerMessage::Routed(msg))
                    if msg.body() == &want_ack =>
                {
                    Some(())
                }
                _ => None,
            })
            .await;
        let want_response = state_response(shard_id, sync_hash, part_idx);
        events
            .clone()
            .recv_until(|ev| match ev {
                Event::MessageProcessed(
                    tcp::Tier::T3,
                    PeerMessage::VersionedStateResponse(response),
                ) if response == want_response => Some(()),
                _ => None,
            })
            .await;
    }
}
