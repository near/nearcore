use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, Handshake, HandshakeFailureReason, PartialEdgeInfo, PeerMessage, PeersRequest,
    PeersResponse, RoutedMessageBody,
};
use crate::peer::testonly::{Event, PeerConfig, PeerHandle};
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use crate::types::{PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg};
use anyhow::Context as _;
use assert_matches::assert_matches;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION};
use std::sync::Arc;

async fn test_peer_communication(
    outbound_encoding: Option<Encoding>,
    inbound_encoding: Option<Encoding>,
) -> anyhow::Result<()> {
    tracing::info!("test_peer_communication({outbound_encoding:?},{inbound_encoding:?})");

    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        force_encoding: inbound_encoding,
    };
    let outbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        force_encoding: outbound_encoding,
    };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let mut inbound = PeerHandle::start_endpoint(clock.clock(), inbound_cfg, inbound_stream).await;
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), outbound_cfg, outbound_stream).await;

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;

    let message_processed = |want| {
        move |ev| match ev {
            Event::Network(PME::MessageProcessed(_, got)) if got == want => Some(()),
            _ => None,
        }
    };

    tracing::info!(target:"test","RequestUpdateNonce");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
        &outbound.cfg.network.node_id(),
        &inbound.cfg.network.node_id(),
        15,
        &outbound.cfg.network.node_key,
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","PeersRequest");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None });
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","PeersResponse");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::PeersResponse(PeersResponse {
        peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
        direct_peers: vec![],
    });
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","BlockRequest");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockRequest(*chain.blocks[5].hash());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","Block");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::Block(chain.blocks[5].clone());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","BlockHeadersRequest");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockHeadersRequest(chain.blocks.iter().map(|b| *b.hash()).collect());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","BlockHeaders");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockHeaders(chain.get_block_headers());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","SyncRoutingTable");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::SyncRoutingTable(data::make_routing_table(&mut rng));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","PartialEncodedChunkRequest");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::Routed(Box::new(outbound.routed_message(
        RoutedMessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
            chunk_hash: chain.blocks[5].chunks()[2].chunk_hash(),
            part_ords: vec![],
            tracking_shards: Default::default(),
        }),
        inbound.cfg.id(),
        1,    // ttl
        None, // TODO(gprusak): this should be clock.now_utc(), once borsh support is dropped.
    )));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","PartialEncodedChunkResponse");
    let mut events = inbound.events.from_now();
    let want_hash = chain.blocks[3].chunks()[0].chunk_hash();
    let want_parts = data::make_chunk_parts(chain.chunks[&want_hash].clone());
    let want = PeerMessage::Routed(Box::new(outbound.routed_message(
        RoutedMessageBody::PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg {
            chunk_hash: want_hash,
            parts: want_parts.clone(),
            receipts: vec![],
        }),
        inbound.cfg.id(),
        1,    // ttl
        None, // TODO(gprusak): this should be clock.now_utc(), once borsh support is dropped.
    )));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","Transaction");
    let mut events = inbound.events.from_now();
    let want = data::make_signed_transaction(&mut rng);
    let want = PeerMessage::Transaction(want);
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test","Challenge");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::Challenge(data::make_challenge(&mut rng));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    // TODO:
    // LastEdge, HandshakeFailure, Disconnect - affect the state of the PeerActor and are
    // observable only under specific conditions.
    Ok(())
}

#[tokio::test]
// Verifies that peers are able to establish a common encoding protocol.
async fn peer_communication() -> anyhow::Result<()> {
    init_test_logger();
    let encodings = [None, Some(Encoding::Proto), Some(Encoding::Borsh)];
    for outbound in &encodings {
        for inbound in &encodings {
            if let (Some(a), Some(b)) = (outbound, inbound) {
                if a != b {
                    continue;
                }
            }
            test_peer_communication(*outbound, *inbound)
                .await
                .with_context(|| format!("(outbound={outbound:?},inbound={inbound:?})"))?;
        }
    }
    Ok(())
}

async fn test_handshake(outbound_encoding: Option<Encoding>, inbound_encoding: Option<Encoding>) {
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig {
        network: chain.make_config(&mut rng),
        chain: chain.clone(),
        force_encoding: inbound_encoding,
    };
    let outbound_cfg = PeerConfig {
        network: chain.make_config(&mut rng),
        chain: chain.clone(),
        force_encoding: outbound_encoding,
    };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let inbound = PeerHandle::start_endpoint(clock.clock(), inbound_cfg, inbound_stream).await;
    let outbound_port = outbound_stream.local_addr.port();
    let mut outbound = Stream::new(outbound_encoding, outbound_stream);

    // Send too old PROTOCOL_VERSION, expect ProtocolVersionMismatch
    let mut handshake = Handshake {
        protocol_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION - 1,
        oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION - 1,
        sender_peer_id: outbound_cfg.id(),
        target_peer_id: inbound.cfg.id(),
        sender_listen_port: Some(outbound_port),
        sender_chain_info: outbound_cfg.chain.get_peer_chain_info(),
        partial_edge_info: outbound_cfg.partial_edge_info(&inbound.cfg.id(), 1),
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

#[tokio::test]
// Verifies that HandshakeFailures are served correctly.
async fn handshake() -> anyhow::Result<()> {
    init_test_logger();
    let encodings = [None, Some(Encoding::Proto), Some(Encoding::Borsh)];
    for outbound in &encodings {
        for inbound in &encodings {
            println!("oubound = {:?}, inbound = {:?}", outbound, inbound);
            if let (Some(a), Some(b)) = (outbound, inbound) {
                if a != b {
                    continue;
                }
            }
            test_handshake(*outbound, *inbound).await;
        }
    }
    Ok(())
}
