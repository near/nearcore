use crate::network_protocol::testonly as data;
use crate::network_protocol::Encoding;
use crate::testonly::make_rng;
use crate::types::{HandshakeFailureReason, PeerMessage};
use anyhow::{bail, Context as _};
use near_network_primitives::time;
use near_network_primitives::types::{
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, RoutedMessageBody,
};
use near_primitives::syncing::EpochSyncResponse;
use near_primitives::types::EpochId;

// TODO: RoutingTableUpdate.validators field is supported only in proto encoding.
// Remove this test once borsh support is removed.
#[test]
fn serialize_deserialize_protobuf_only() {
    let mut rng = make_rng(39521947542);
    let clock = time::FakeClock::default();
    let rt = data::make_routing_table(&mut rng, &clock.clock());
    let m = PeerMessage::SyncRoutingTable(rt);
    let m2 = PeerMessage::deserialize(Encoding::Proto, &m.serialize(Encoding::Proto)).unwrap();
    assert_eq!(m, m2);
}

#[test]
fn serialize_deserialize() -> anyhow::Result<()> {
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = data::Chain::make(&mut clock, &mut rng, 12);
    let a = data::make_signer(&mut rng);
    let b = data::make_signer(&mut rng);
    let edge = data::make_edge(&a, &b);
    let epoch_id = EpochId(chain.blocks[1].hash().clone());

    let chunk_hash = chain.blocks[3].chunks()[0].chunk_hash();
    let routed_message1 = data::make_routed_message(
        &mut rng,
        RoutedMessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
            chunk_hash: chunk_hash.clone(),
            part_ords: vec![],
            tracking_shards: Default::default(),
        }),
    );
    let routed_message2 = data::make_routed_message(
        &mut rng,
        RoutedMessageBody::PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg {
            chunk_hash: chunk_hash.clone(),
            parts: data::make_chunk_parts(chain.chunks[&chunk_hash].clone()),
            receipts: vec![],
        }),
    );
    let mut routing_table = data::make_routing_table(&mut rng, &clock.clock());
    // TODO: validators field is supported only in proto encoding.
    // Remove this line once borsh support is removed.
    routing_table.validators = vec![];

    let msgs = [
        PeerMessage::Handshake(data::make_handshake(&mut rng, &chain)),
        PeerMessage::HandshakeFailure(
            data::make_peer_info(&mut rng),
            HandshakeFailureReason::InvalidTarget,
        ),
        PeerMessage::LastEdge(edge.clone()),
        PeerMessage::SyncRoutingTable(routing_table),
        PeerMessage::RequestUpdateNonce(data::make_partial_edge(&mut rng)),
        PeerMessage::ResponseUpdateNonce(edge),
        PeerMessage::PeersRequest,
        PeerMessage::PeersResponse((0..5).map(|_| data::make_peer_info(&mut rng)).collect()),
        PeerMessage::BlockHeadersRequest(chain.blocks.iter().map(|b| b.hash().clone()).collect()),
        PeerMessage::BlockHeaders(chain.get_block_headers()),
        PeerMessage::BlockRequest(chain.blocks[5].hash().clone()),
        PeerMessage::Block(chain.blocks[5].clone()),
        PeerMessage::Transaction(data::make_signed_transaction(&mut rng)),
        PeerMessage::Routed(routed_message1),
        PeerMessage::Routed(routed_message2),
        PeerMessage::Disconnect,
        PeerMessage::Challenge(data::make_challenge(&mut rng)),
        PeerMessage::EpochSyncRequest(epoch_id.clone()),
        PeerMessage::EpochSyncResponse(Box::new(EpochSyncResponse::UpToDate)),
        PeerMessage::EpochSyncFinalizationRequest(epoch_id),
        // TODO: EpochSyncFinalizationResponse
    ];

    // Check that serialize;deserialize = 1
    for enc in [Encoding::Proto, Encoding::Borsh] {
        for m in &msgs {
            (|| {
                let m2 = PeerMessage::deserialize(enc, &m.serialize(enc))
                    .with_context(|| format!("{m}"))?;
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
