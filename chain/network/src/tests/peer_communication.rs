#![allow(unused_imports)]
#![allow(dead_code)]
use crate::network_protocol::Encoding;
use crate::peer::codec::Codec;
use crate::peer::peer_actor::PeerActor;
use crate::private_actix::{PeerRequestResult, RegisterPeer, RegisterPeerResponse, SendMessage};
use crate::stats::metrics::NetworkMetrics;
use crate::tests::data;
use crate::tests::peer_actor::{PeerConfig, PeerHandle, Response};
use crate::tests::util::{make_rng, FakeClock};
use crate::types::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
    PeerManagerMessageRequest, PeerManagerMessageResponse, PeerMessage, RoutingTableUpdate,
};
use actix::{Actor, Addr, Context, Handler, StreamHandler as _};
use anyhow::{anyhow, bail, Context as _};
use near_crypto::{InMemorySigner, KeyType, PublicKey, SecretKey};
use near_network_primitives::types::{
    Edge, NetworkViewClientMessages, NetworkViewClientResponses, PartialEdgeInfo,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, PeerInfo, PeerType,
    RoutedMessageBody,
};
use near_performance_metrics::framed_write::FramedWrite;
use near_primitives::block::{genesis_chunks, Block, BlockHeader, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::num_rational::Rational;
use near_primitives::sharding::ShardChunk;
use near_primitives::syncing::EpochSyncResponse;
use near_primitives::types::{AccountId, BlockHeight, EpochId, StateRoot};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
use near_rate_limiter::{
    ActixMessageResponse, ActixMessageWrapper, ThrottleController, ThrottleFramedRead,
    ThrottleToken,
};
use rand::Rng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time;
use tokio_stream::StreamExt;

async fn test_peer_communication(
    outbound_encoding: Option<Encoding>,
    inbound_encoding: Option<Encoding>,
) -> anyhow::Result<()> {
    let mut rng = make_rng(89028037453);
    let mut clock = FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let (mut outbound, mut inbound) = PeerHandle::start(
        PeerConfig {
            signer: data::make_signer(&mut rng),
            chain: chain.clone(),
            peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
            force_encoding: outbound_encoding,
        },
        PeerConfig {
            signer: data::make_signer(&mut rng),
            chain: chain.clone(),
            peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
            force_encoding: inbound_encoding,
        },
    )
    .await?;

    assert_eq!(Response::HandshakeDone, outbound.recv().await);
    assert_eq!(Response::HandshakeDone, inbound.recv().await);

    // RequestUpdateNonce
    let want = data::make_partial_edge(&mut rng);
    outbound.send(PeerMessage::RequestUpdateNonce(want.clone())).await;
    let got = inbound.recv().await;
    assert_eq!(Response::RequestUpdateNonce(want), got);

    // ReponseUpdateNonce
    let a = data::make_signer(&mut rng);
    let b = data::make_signer(&mut rng);
    let want = data::make_edge(&mut rng, &a, &b);
    outbound.send(PeerMessage::ResponseUpdateNonce(want.clone())).await;
    assert_eq!(Response::ResponseUpdateNonce(want), inbound.recv().await);

    // PeersRequest -> PeersResponse
    // This test is different from the rest, because we cannot skip sending the response back.
    let want = inbound.cfg.peers.clone();
    outbound.send(PeerMessage::PeersRequest).await;
    assert_eq!(Response::PeersResponse(want), outbound.recv().await);

    // BlockRequest
    let want = chain.blocks[5].hash().clone();
    outbound.send(PeerMessage::BlockRequest(want.clone())).await;
    assert_eq!(Response::BlockRequest(want), inbound.recv().await);

    // Block
    let want = chain.blocks[5].clone();
    outbound.send(PeerMessage::Block(want.clone())).await;
    assert_eq!(Response::Block(want), inbound.recv().await);

    // BlockHeadersRequest
    let want: Vec<_> = chain.blocks.iter().map(|b| b.hash().clone()).collect();
    outbound.send(PeerMessage::BlockHeadersRequest(want.clone())).await;
    assert_eq!(Response::BlockHeadersRequest(want), inbound.recv().await);

    // BlockHeaders
    let want = chain.get_block_headers();
    outbound.send(PeerMessage::BlockHeaders(want.clone())).await;
    assert_eq!(Response::BlockHeaders(want), inbound.recv().await);

    // SyncRoutingTable
    let want = data::make_routing_table(&mut rng);
    outbound.send(PeerMessage::SyncRoutingTable(want.clone())).await;
    assert_eq!(Response::RoutingTable(want), inbound.recv().await);

    // PartialEncodedChunkRequest
    let want = chain.blocks[5].chunks()[2].chunk_hash();
    let msg = outbound.routed_message(RoutedMessageBody::PartialEncodedChunkRequest(
        PartialEncodedChunkRequestMsg {
            chunk_hash: want.clone(),
            part_ords: vec![],
            tracking_shards: Default::default(),
        },
    ));
    outbound.send(PeerMessage::Routed(msg)).await;
    assert_eq!(Response::ChunkRequest(want), inbound.recv().await);

    // PartialEncodedChunkResponse
    let want_hash = chain.blocks[3].chunks()[0].chunk_hash();
    let want_parts = data::make_chunk_parts(chain.chunks[&want_hash].clone());
    let msg = outbound.routed_message(RoutedMessageBody::PartialEncodedChunkResponse(
        PartialEncodedChunkResponseMsg {
            chunk_hash: want_hash,
            parts: want_parts.clone(),
            receipts: vec![],
        },
    ));
    outbound.send(PeerMessage::Routed(msg)).await;
    assert_eq!(Response::Chunk(want_parts), inbound.recv().await);

    // Transaction
    let want = data::make_signed_transaction(&mut rng);
    outbound.send(PeerMessage::Transaction(want.clone())).await;
    assert_eq!(Response::Transaction(want), inbound.recv().await);

    // Challenge
    let want = data::make_challenge(&mut rng);
    outbound.send(PeerMessage::Challenge(want.clone())).await;
    assert_eq!(Response::Challenge(want), inbound.recv().await);

    // EpochSyncRequest
    let want = EpochId(chain.blocks[1].hash().clone());
    outbound.send(PeerMessage::EpochSyncRequest(want.clone())).await;
    assert_eq!(Response::EpochSyncRequest(want), inbound.recv().await);

    // EpochSyncResponse
    let want = EpochSyncResponse::UpToDate;
    outbound.send(PeerMessage::EpochSyncResponse(Box::new(want.clone()))).await;
    assert_eq!(Response::EpochSyncResponse(want), inbound.recv().await);

    // EpochSyncFinalizationRequest
    let want = EpochId(chain.blocks[1].hash().clone());
    outbound.send(PeerMessage::EpochSyncFinalizationRequest(want.clone())).await;
    assert_eq!(Response::EpochSyncFinalizationRequest(want), inbound.recv().await);

    // TODO:
    // LastEdge, HandshakeFailure, Disconnect - affect the state of the PeerActor and are
    // observable only under specific conditions.
    // ExpochSyncFinalizationResponse - needs some work to produce reasonable fake data.
    // RoutingTableSyncV2 - not used yet, available under some feature flag.
    Ok(())
}

#[tokio::test]
// Verifies that peers are able to establish a common encoding protocol.
async fn peer_communication() -> anyhow::Result<()> {
    let encodings = [None, Some(Encoding::Proto), Some(Encoding::Borsh)];
    for outbound in &encodings {
        for inbound in &encodings {
            if let (Some(a), Some(b)) = (outbound, inbound) {
                if *a != *b {
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
