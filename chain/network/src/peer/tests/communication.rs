use crate::network_protocol::testonly as data;
use crate::network_protocol::Encoding;
use crate::peer::testonly::{Event, PeerConfig, PeerHandle};
use crate::peer_manager::peer_manager_actor;
use crate::testonly::fake_client::Event as CE;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use crate::types::{Handshake, HandshakeFailureReason, PeerMessage};
use anyhow::Context as _;
use assert_matches::assert_matches;
use near_logger_utils::init_test_logger;
use near_network_primitives::time;
use near_network_primitives::types::{
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, RoutedMessageBody,
};
use near_primitives::syncing::EpochSyncResponse;
use near_primitives::types::EpochId;
use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION};
use std::sync::Arc;

async fn test_peer_communication(
    outbound_encoding: Option<Encoding>,
    inbound_encoding: Option<Encoding>,
) -> anyhow::Result<()> {
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
        force_encoding: inbound_encoding,
        start_handshake_with: None,
        nonce: None,
    };
    let outbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
        force_encoding: outbound_encoding,
        start_handshake_with: Some(inbound_cfg.id()),
        nonce: None,
    };

    let (outbound_stream, inbound_stream) = PeerHandle::start_connection().await;
    let mut inbound = PeerHandle::start_endpoint(clock.clock(), inbound_cfg, inbound_stream).await;
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), outbound_cfg, outbound_stream).await;

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;

    // TODO(gprusak): In proto encoding SyncAccountsData exchange is part of the handshake.
    // As a workaround, in borsh encoding an empty RoutingTableUpdate is sent.
    // Once borsh support is removed, the initial SyncAccountsData should be consumed in
    // complete_handshake.
    let filter = |ev| match ev {
        Event::Network(peer_manager_actor::Event::MessageProcessed(
            PeerMessage::SyncAccountsData(_),
        )) => None,
        Event::RoutingTable(_) => None,
        ev => Some(ev),
    };

    // RequestUpdateNonce
    let want = data::make_partial_edge(&mut rng);
    outbound.send(PeerMessage::RequestUpdateNonce(want.clone())).await;
    let got = inbound.events.recv_until(filter).await;
    assert_eq!(Event::RequestUpdateNonce(want), got);

    // ReponseUpdateNonce
    let a = data::make_signer(&mut rng);
    let b = data::make_signer(&mut rng);
    let want = data::make_edge(&a, &b);
    outbound.send(PeerMessage::ResponseUpdateNonce(want.clone())).await;
    assert_eq!(Event::ResponseUpdateNonce(want), inbound.events.recv_until(filter).await);

    // PeersRequest -> PeersResponse
    // This test is different from the rest, because we cannot skip sending the response back.
    let want = inbound.cfg.peers.clone();
    outbound.send(PeerMessage::PeersRequest).await;
    assert_eq!(Event::PeersResponse(want), outbound.events.recv_until(filter).await);

    // BlockRequest
    let want = chain.blocks[5].hash().clone();
    outbound.send(PeerMessage::BlockRequest(want.clone())).await;
    assert_eq!(Event::Client(CE::BlockRequest(want)), inbound.events.recv_until(filter).await);

    // Block
    let want = chain.blocks[5].clone();
    outbound.send(PeerMessage::Block(want.clone())).await;
    assert_eq!(Event::Client(CE::Block(want)), inbound.events.recv_until(filter).await);

    // BlockHeadersRequest
    let want: Vec<_> = chain.blocks.iter().map(|b| b.hash().clone()).collect();
    outbound.send(PeerMessage::BlockHeadersRequest(want.clone())).await;
    assert_eq!(
        Event::Client(CE::BlockHeadersRequest(want)),
        inbound.events.recv_until(filter).await
    );

    // BlockHeaders
    let want = chain.get_block_headers();
    outbound.send(PeerMessage::BlockHeaders(want.clone())).await;
    assert_eq!(Event::Client(CE::BlockHeaders(want)), inbound.events.recv_until(filter).await);

    // SyncRoutingTable
    let want = data::make_routing_table(&mut rng);
    outbound.send(PeerMessage::SyncRoutingTable(want.clone())).await;
    assert_eq!(Event::RoutingTable(want), inbound.events.recv().await);

    // PartialEncodedChunkRequest
    let want = outbound.routed_message(
        RoutedMessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
            chunk_hash: chain.blocks[5].chunks()[2].chunk_hash(),
            part_ords: vec![],
            tracking_shards: Default::default(),
        }),
        inbound.cfg.id(),
        1,    // ttl
        None, // TODO(gprusak): this should be clock.now_utc(), once borsh support is dropped.
    );
    outbound.send(PeerMessage::Routed(want.clone())).await;
    assert_eq!(Event::Routed(want), inbound.events.recv_until(filter).await);

    // PartialEncodedChunkResponse
    let want_hash = chain.blocks[3].chunks()[0].chunk_hash();
    let want_parts = data::make_chunk_parts(chain.chunks[&want_hash].clone());
    let want = outbound.routed_message(
        RoutedMessageBody::PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg {
            chunk_hash: want_hash,
            parts: want_parts.clone(),
            receipts: vec![],
        }),
        inbound.cfg.id(),
        1,    // ttl
        None, // TODO(gprusak): this should be clock.now_utc(), once borsh support is dropped.
    );
    outbound.send(PeerMessage::Routed(want.clone())).await;
    assert_eq!(Event::Routed(want), inbound.events.recv_until(filter).await);

    // Transaction
    let want = data::make_signed_transaction(&mut rng);
    outbound.send(PeerMessage::Transaction(want.clone())).await;
    assert_eq!(Event::Client(CE::Transaction(want)), inbound.events.recv_until(filter).await);

    // Challenge
    let want = data::make_challenge(&mut rng);
    outbound.send(PeerMessage::Challenge(want.clone())).await;
    assert_eq!(Event::Client(CE::Challenge(want)), inbound.events.recv_until(filter).await);

    // EpochSyncRequest
    let want = EpochId(chain.blocks[1].hash().clone());
    outbound.send(PeerMessage::EpochSyncRequest(want.clone())).await;
    assert_eq!(Event::Client(CE::EpochSyncRequest(want)), inbound.events.recv_until(filter).await);

    // EpochSyncResponse
    let want = EpochSyncResponse::UpToDate;
    outbound.send(PeerMessage::EpochSyncResponse(Box::new(want.clone()))).await;
    assert_eq!(Event::Client(CE::EpochSyncResponse(want)), inbound.events.recv_until(filter).await);

    // EpochSyncFinalizationRequest
    let want = EpochId(chain.blocks[1].hash().clone());
    outbound.send(PeerMessage::EpochSyncFinalizationRequest(want.clone())).await;
    assert_eq!(
        Event::Client(CE::EpochSyncFinalizationRequest(want)),
        inbound.events.recv_until(filter).await
    );

    // TODO:
    // LastEdge, HandshakeFailure, Disconnect - affect the state of the PeerActor and are
    // observable only under specific conditions.
    // ExpochSyncFinalizationResponse - unused.
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
            test_peer_communication(outbound.clone(), inbound.clone())
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
        peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
        force_encoding: inbound_encoding,
        start_handshake_with: None,
        nonce: None,
    };
    let outbound_cfg = PeerConfig {
        network: chain.make_config(&mut rng),
        chain: chain.clone(),
        peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
        force_encoding: outbound_encoding,
        start_handshake_with: None,
        nonce: None,
    };
    let (outbound_stream, inbound_stream) = PeerHandle::start_connection().await;
    let inbound = PeerHandle::start_endpoint(clock.clock(), inbound_cfg, inbound_stream).await;
    let mut outbound = Stream::new(outbound_encoding, outbound_stream);

    // Send too old PROTOCOL_VERSION, expect ProtocolVersionMismatch
    let mut handshake = Handshake {
        protocol_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION - 1,
        oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION - 1,
        sender_peer_id: outbound_cfg.id(),
        target_peer_id: inbound.cfg.id(),
        sender_listen_port: Some(outbound.local_addr.port()),
        sender_chain_info: outbound_cfg.chain.get_peer_chain_info(),
        partial_edge_info: outbound_cfg.partial_edge_info(&inbound.cfg.id(), 1),
    };
    // We will also introduce chain_id mismatch, but ProtocolVersionMismatch is expected to take priority.
    handshake.sender_chain_info.genesis_id.chain_id = "unknown_chain".to_string();
    outbound.write(&PeerMessage::Handshake(handshake.clone())).await;
    let resp = outbound.read().await;
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::ProtocolVersionMismatch { .. })
    );

    // Send too new PROTOCOL_VERSION, expect ProtocolVersionMismatch
    handshake.protocol_version = PROTOCOL_VERSION + 1;
    handshake.oldest_supported_version = PROTOCOL_VERSION + 1;
    outbound.write(&PeerMessage::Handshake(handshake.clone())).await;
    let resp = outbound.read().await;
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::ProtocolVersionMismatch { .. })
    );

    // Send mismatching chain_id, expect GenesisMismatch.
    // We fix protocol_version, but chain_id is still mismatching.
    handshake.protocol_version = PROTOCOL_VERSION;
    handshake.oldest_supported_version = PROTOCOL_VERSION;
    outbound.write(&PeerMessage::Handshake(handshake.clone())).await;
    let resp = outbound.read().await;
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::GenesisMismatch(_))
    );

    // Send a correct Handshake, expect a matching Handshake response.
    handshake.sender_chain_info = chain.get_peer_chain_info();
    outbound.write(&PeerMessage::Handshake(handshake.clone())).await;
    let resp = outbound.read().await;
    assert_matches!(resp, PeerMessage::Handshake(_));
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
            test_handshake(outbound.clone(), inbound.clone()).await;
        }
    }
    Ok(())
}
