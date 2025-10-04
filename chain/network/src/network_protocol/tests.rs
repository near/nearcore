use super::*;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, PeersResponse};
use crate::testonly::make_rng;
use crate::types::{Disconnect, HandshakeFailureReason, PeerMessage};
use crate::types::{PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg};
use anyhow::{Context as _, bail};
use itertools::Itertools as _;
use near_async::time;
use rand::Rng as _;

#[test]
fn deduplicate_edges() {
    let mut rng = make_rng(19385389);
    let rng = &mut rng;
    let a = data::make_secret_key(rng);
    let b = data::make_secret_key(rng);
    let c = data::make_secret_key(rng);
    let ab1 = data::make_edge(&a, &b, 1);
    let ab3 = data::make_edge(&a, &b, 3);
    let ab5 = data::make_edge(&a, &b, 5);
    let ac7 = data::make_edge(&a, &c, 7);
    let ac9 = data::make_edge(&a, &c, 9);
    let bc1 = data::make_edge(&b, &c, 1);
    let mut want = vec![ab5.clone(), ac9.clone(), bc1.clone()];
    want.sort_by_key(|e| e.key().clone());
    let input = [ab1, ab3, ab5, ac7, ac9, bc1];
    for p in input.iter().permutations(input.len()) {
        let mut got = Edge::deduplicate(p.into_iter().cloned().collect());
        got.sort_by_key(|e| e.key().clone());
        assert_eq!(got, want);
    }
}

#[test]
fn bad_account_data_size() {
    let mut rng = make_rng(19385389);
    let clock = time::FakeClock::default();
    // rule of thumb: 1000x IPv6 should be considered too much.
    let signer = data::make_validator_signer(&mut rng);

    let ad = VersionedAccountData {
        data: AccountData {
            proxies: (0..1000)
                .map(|_| {
                    let ip = data::make_ipv6(&mut rng);
                    data::make_peer_addr(&mut rng, ip)
                })
                .collect(),
            peer_id: data::make_peer_id(&mut rng),
        },
        account_key: signer.public_key(),
        version: rng.r#gen(),
        timestamp: clock.now_utc(),
    };
    assert!(ad.sign(&signer.into()).is_err());
}

#[test]
fn serialize_deserialize_protobuf_only() {
    let mut rng = make_rng(39521947542);
    let mut clock = time::FakeClock::default();
    let chain = data::Chain::make(&mut clock, &mut rng, 12);
    let msgs = [
        PeerMessage::Tier1Handshake(data::make_handshake(&mut rng, &chain)),
        PeerMessage::SyncAccountsData(SyncAccountsData {
            accounts_data: (0..4)
                .map(|_| Arc::new(data::make_signed_account_data(&mut rng, &clock.clock())))
                .collect(),
            incremental: true,
            requesting_full_sync: true,
        }),
    ];
    for m in msgs {
        let m2 = PeerMessage::deserialize(Encoding::Proto, &m.serialize(Encoding::Proto))
            .with_context(|| m.to_string())
            .unwrap();
        assert_eq!(m, m2);
    }
}

#[test]
fn serialize_deserialize() -> anyhow::Result<()> {
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = data::Chain::make(&mut clock, &mut rng, 12);
    let a = data::make_secret_key(&mut rng);
    let b = data::make_secret_key(&mut rng);
    let edge = data::make_edge(&a, &b, 1);

    let block3_chunks = chain.blocks[3].chunks();
    let chunk_hash = block3_chunks[0].chunk_hash();
    let routed_message1 = Box::new(data::make_routed_message(
        &mut rng,
        T2MessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
            chunk_hash: chunk_hash.clone(),
            part_ords: vec![],
            tracking_shards: Default::default(),
        })
        .into(),
    ));
    let routed_message2 = Box::new(data::make_routed_message(
        &mut rng,
        T2MessageBody::PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg {
            chunk_hash: chunk_hash.clone(),
            parts: data::make_chunk_parts(chain.chunks[chunk_hash].clone()),
            receipts: vec![],
        })
        .into(),
    ));
    let msgs = [
        PeerMessage::Tier2Handshake(data::make_handshake(&mut rng, &chain)),
        PeerMessage::HandshakeFailure(
            data::make_peer_info(&mut rng),
            HandshakeFailureReason::InvalidTarget,
        ),
        PeerMessage::LastEdge(edge),
        PeerMessage::SyncRoutingTable(data::make_routing_table(&mut rng)),
        PeerMessage::RequestUpdateNonce(data::make_partial_edge(&mut rng)),
        PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None }),
        PeerMessage::PeersResponse(PeersResponse {
            peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
            direct_peers: vec![], // TODO: populate this field once borsh support is dropped
        }),
        PeerMessage::BlockHeadersRequest(chain.blocks.iter().map(|b| *b.hash()).collect()),
        PeerMessage::BlockHeaders(chain.get_block_headers().map(Into::into).collect()),
        PeerMessage::BlockRequest(*chain.blocks[5].hash()),
        PeerMessage::Block(chain.blocks[5].clone()),
        PeerMessage::Transaction(data::make_signed_transaction(&mut rng)),
        PeerMessage::Routed(routed_message1),
        PeerMessage::Routed(routed_message2),
        PeerMessage::Disconnect(Disconnect { remove_from_connection_store: false }),
    ];

    // Check that serialize;deserialize = 1
    for enc in [Encoding::Proto, Encoding::Borsh] {
        for m in &msgs {
            (|| {
                let m2 = PeerMessage::deserialize(enc, &m.serialize(enc))
                    .with_context(|| m.to_string())?;
                if *m != m2 {
                    bail!("deserialize(serialize({m}) = {m2}");
                }
                anyhow::Ok(())
            })()
            .with_context(|| format!("encoding={enc:?}"))?;
        }
    }

    // Test the unambiguous parsing argument described in
    // https://docs.google.com/document/d/1gCWmt9O-h_-5JDXIqbKxAaSS3Q9pryB1f9DDY1mMav4/edit#heading=h.x1awbr2acslb
    for m in &msgs {
        let x = m.serialize(Encoding::Proto);
        assert!(x[0] >= 32, "serialize({},PROTO)[0] = {:?}, want >= 32", m, x.get(0));
        let y = m.serialize(Encoding::Borsh);
        assert!(y[0] <= 21, "serialize({},BORSH)[0] = {:?}, want <= 21", m, y.get(0));
    }

    // Encodings should never be compatible.
    for (from, to) in [(Encoding::Proto, Encoding::Borsh), (Encoding::Borsh, Encoding::Proto)] {
        for m in &msgs {
            let bytes = &m.serialize(from);
            match PeerMessage::deserialize(to, bytes) {
                Err(_) => {}
                Ok(m2) => {
                    bail!("from={from:?},to={to:?}: deserialize(serialize({m})) = {m2}, want error")
                }
            }
        }
    }

    Ok(())
}

fn make_block_approval_message() -> RoutedMessage {
    let mut rng = make_rng(19385389);
    let signer = data::make_validator_signer(&mut rng);
    let approval = Approval::new(CryptoHash::default(), 1, 1, &signer);
    data::make_routed_message(
        &mut rng,
        TieredMessageBody::T1(Box::new(T1MessageBody::BlockApproval(approval))),
    )
}

fn make_chunk_request_message() -> RoutedMessage {
    let mut rng = make_rng(19385389);
    data::make_routed_message(
        &mut rng,
        T2MessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
            chunk_hash: CryptoHash::default().into(),
            part_ords: vec![],
            tracking_shards: Default::default(),
        })
        .into(),
    )
}

#[test]
fn test_message_matching_hashes() {
    let message_v3 = make_chunk_request_message();
    let message_v1: RoutedMessage = message_v3.clone().msg_v1().into();
    assert_eq!(message_v3.hash(), message_v1.hash());
}

/// Tests that messages get upgraded to V3 when a field that is not present in V1 is mutated.
/// Also tests that RoutedMessageBody is upgraded to TieredMessageBody.
#[test]
fn test_upgrading_to_v3() {
    let message_v3 = make_chunk_request_message();
    let message_v1: RoutedMessage = message_v3.msg_v1().into();
    let message_v2: RoutedMessage =
        RoutedMessageV2 { msg: message_v1.clone().msg_v1(), created_at: None, num_hops: 0 }.into();

    // Tier 2
    let mut message = message_v1.clone();
    assert!(matches!(message, RoutedMessage::V1(_)));
    let _ = message.num_hops_mut();
    assert!(matches!(message, RoutedMessage::V3(_)));

    assert!(matches!(message_v1, RoutedMessage::V1(_)));
    let body = message_v1.body_owned();
    assert!(matches!(body, TieredMessageBody::T2(_)));

    let mut message: RoutedMessage = message_v2.clone();
    assert!(matches!(message, RoutedMessage::V2(_)));
    let _ = message.num_hops_mut();
    assert!(matches!(message, RoutedMessage::V3(_)));

    assert!(matches!(message_v2, RoutedMessage::V2(_)));
    let body = message_v2.body_owned();
    assert!(matches!(body, TieredMessageBody::T2(_)));

    // Tier 1
    let message_v3 = make_block_approval_message();

    let message_v1: RoutedMessage = message_v3.msg_v1().into();

    let mut message = message_v1.clone();
    assert!(matches!(message, RoutedMessage::V1(_)));
    let _ = message.num_hops_mut();
    assert!(matches!(message, RoutedMessage::V3(_)));

    assert!(matches!(message_v1, RoutedMessage::V1(_)));
    let body = message_v1.body_owned();
    assert!(matches!(body, TieredMessageBody::T1(_)));
}

#[test]
fn test_body_conversion() {
    let routed_body = RoutedMessageBody::Ping(Ping {
        nonce: 1,
        source: PeerId::new(PublicKey::empty(near_crypto::KeyType::ED25519)),
    });
    let tiered_body = TieredMessageBody::from_routed(routed_body.clone());
    assert!(matches!(tiered_body, TieredMessageBody::T2(_)));
    let routed_body2 = tiered_body.into();
    assert_eq!(routed_body, routed_body2);

    let mut rng = make_rng(19385389);
    let signer = data::make_validator_signer(&mut rng);
    let approval = Approval::new(CryptoHash::default(), 1, 1, &signer);
    let routed_body = RoutedMessageBody::BlockApproval(approval);
    let tiered_body = TieredMessageBody::from_routed(routed_body.clone());
    assert!(matches!(tiered_body, TieredMessageBody::T1(_)));
    let routed_body2 = tiered_body.into();
    assert_eq!(routed_body, routed_body2);
}

#[cfg(not(feature = "nightly"))]
#[test]
fn test_t1_is_signed() {
    let message = make_block_approval_message();
    assert!(message.signature().is_some());
}

#[test]
fn test_t2_is_signed() {
    let message = make_chunk_request_message();
    assert!(message.signature().is_some());
}

#[test]
fn test_body_variant_granularity() {
    let message_v3 = make_chunk_request_message();
    assert_eq!(message_v3.body_variant(), "PartialEncodedChunkRequest");
}
