use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Handshake, HandshakeFailureReason, PartialEdgeInfo, PeerMessage, PeersRequest, PeersResponse,
    T2MessageBody,
};
use crate::peer::peer_actor::ClosingReason;
use crate::peer::testonly::{PeerConfig, PeerHandle};
use crate::peer_manager::peer_manager_actor::Event;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use crate::types::{
    Edge, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, ReasonForBan,
};
use assert_matches::assert_matches;
use near_async::{ActorSystem, time};
use near_o11y::testonly::init_test_logger;
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, PROTOCOL_VERSION};
use std::sync::Arc;

#[allow(clippy::large_stack_frames)]
#[tokio::test]
async fn test_peer_communication() -> anyhow::Result<()> {
    init_test_logger();
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let outbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let mut inbound = PeerHandle::start_endpoint(
        clock.clock(),
        actor_system.clone(),
        inbound_cfg,
        inbound_stream,
    );
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), actor_system, outbound_cfg, outbound_stream);

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;

    let message_processed = |want| {
        move |ev| match ev {
            Event::MessageProcessed(_, got) if got == want => Some(()),
            _ => None,
        }
    };

    tracing::info!("request update nonce");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
        &outbound.cfg.network.node_id(),
        &inbound.cfg.network.node_id(),
        clock.now_utc().unix_timestamp() as u64,
        &outbound.cfg.network.node_key,
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("peers request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None });
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("peers response");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::PeersResponse(PeersResponse {
        peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
        direct_peers: vec![],
    });
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("block request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockRequest(*chain.blocks[5].hash());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("block");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::Block(chain.blocks[5].clone());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("block headers request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockHeadersRequest(chain.blocks.iter().map(|b| *b.hash()).collect());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("block headers");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockHeaders(chain.get_block_headers().map(Into::into).collect());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("sync routing table");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::SyncRoutingTable(data::make_routing_table(&mut rng));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("partial encoded chunk request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::Routed(Box::new(
        outbound.routed_message(
            T2MessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
                chunk_hash: chain.blocks[5].chunks()[2].chunk_hash().clone(),
                part_ords: vec![],
                tracking_shards: Default::default(),
            })
            .into(),
            inbound.cfg.id(),
            1, // ttl
            Some(clock.now_utc()),
        ),
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("partial encoded chunk response");
    let mut events = inbound.events.from_now();
    let want_hash = chain.blocks[3].chunks()[0].chunk_hash().clone();
    let want_parts = data::make_chunk_parts(chain.chunks[&want_hash].clone());
    let want = PeerMessage::Routed(Box::new(
        outbound.routed_message(
            T2MessageBody::PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg {
                chunk_hash: want_hash,
                parts: want_parts.clone(),
                receipts: vec![],
            })
            .into(),
            inbound.cfg.id(),
            1, // ttl
            Some(clock.now_utc()),
        ),
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!("transaction");
    let mut events = inbound.events.from_now();
    let want = data::make_signed_transaction(&mut rng);
    let want = PeerMessage::Transaction(want);
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    // TODO:
    // LastEdge, HandshakeFailure, Disconnect - affect the state of the PeerActor and are
    // observable only under specific conditions.
    Ok(())
}

#[tokio::test]
async fn test_request_update_nonce_rejects_invalid_nonce() -> anyhow::Result<()> {
    init_test_logger();
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let outbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let mut inbound = PeerHandle::start_endpoint(
        clock.clock(),
        actor_system.clone(),
        inbound_cfg,
        inbound_stream,
    );
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), actor_system, outbound_cfg, outbound_stream);

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;

    let mut events = inbound.events.from_now();
    let msg = PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
        &outbound.cfg.network.node_id(),
        &inbound.cfg.network.node_id(),
        u64::MAX,
        &outbound.cfg.network.node_key,
    ));
    outbound.send(msg).await;

    events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev)
                if ev.reason == ClosingReason::Ban(ReasonForBan::InvalidEdge) =>
            {
                Some(())
            }
            _ => None,
        })
        .await;

    Ok(())
}

#[tokio::test]
// Verifies that HandshakeFailures are served correctly.
async fn test_handshake() {
    init_test_logger();
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    let outbound_cfg = PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let inbound =
        PeerHandle::start_endpoint(clock.clock(), ActorSystem::new(), inbound_cfg, inbound_stream);
    let outbound_port = outbound_stream.local_addr.port();
    let mut outbound = Stream::new(outbound_stream);

    // Send too old PROTOCOL_VERSION, expect ProtocolVersionMismatch
    let mut handshake = Handshake {
        protocol_version: MIN_SUPPORTED_PROTOCOL_VERSION - 1,
        oldest_supported_version: MIN_SUPPORTED_PROTOCOL_VERSION - 1,
        sender_peer_id: outbound_cfg.id(),
        target_peer_id: inbound.cfg.id(),
        sender_listen_port: Some(outbound_port),
        sender_chain_info: outbound_cfg.chain.get_peer_chain_info(),
        partial_edge_info: outbound_cfg
            .partial_edge_info(&inbound.cfg.id(), Edge::create_fresh_nonce(&clock.clock())),
        owned_account: None,
    };
    // We will also introduce chain_id mismatch, but ProtocolVersionMismatch is expected to take priority.
    handshake.sender_chain_info.genesis_id.chain_id = "unknown_chain".to_string();
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let resp = outbound.read().await.unwrap();
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::ProtocolVersionMismatch { .. })
    );

    // Send too new PROTOCOL_VERSION, expect ProtocolVersionMismatch
    handshake.protocol_version = PROTOCOL_VERSION + 1;
    handshake.oldest_supported_version = PROTOCOL_VERSION + 1;
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let resp = outbound.read().await.unwrap();
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::ProtocolVersionMismatch { .. })
    );

    // Send mismatching chain_id, expect GenesisMismatch.
    // We fix protocol_version, but chain_id is still mismatching.
    handshake.protocol_version = PROTOCOL_VERSION;
    handshake.oldest_supported_version = PROTOCOL_VERSION;
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let resp = outbound.read().await.unwrap();
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::GenesisMismatch(_))
    );

    // Send a correct Handshake, expect a matching Handshake response.
    handshake.sender_chain_info = chain.get_peer_chain_info();
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let resp = outbound.read().await.unwrap();
    assert_matches!(resp, PeerMessage::Tier2Handshake(_));
}
