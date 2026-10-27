use crate::config::SocketOptions;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{PeerMessage, T2MessageBody};
use crate::peer;
use crate::peer::testonly::PeerHandle;
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event;
use crate::tcp;
use crate::testonly::{abort_on_panic, make_rng};
use near_async::{ActorSystem, time};
use near_o11y::testonly::init_test_logger;
use near_primitives::transaction::{EccTransaction, TransactionEnvelope};
use std::sync::Arc;

/// The number of forwarded ECCs a node accepts between two blocks.
const ECC_LIMIT: usize = 500;

fn ecc(i: usize) -> TransactionEnvelope {
    EccTransaction::new("contract.near".parse().unwrap(), i.to_le_bytes().to_vec()).into()
}

/// Sends `tx` to the peer manager as a message forwarded by `peer`, and says whether it was
/// processed, as opposed to dropped.
async fn forward_tx(
    pm: &mut peer_manager::testonly::ActorHandler,
    peer: &PeerHandle,
    tx: TransactionEnvelope,
) -> bool {
    let body = T2MessageBody::ForwardTx(tx).into();
    let msg = Box::new(peer.routed_message(body, pm.cfg.node_id(), 100, None));
    peer.send(PeerMessage::Routed(msg)).await;
    pm.events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(tcp::Tier::T2, PeerMessage::Routed(_)) => Some(true),
            Event::RoutedMessageDropped => Some(false),
            _ => None,
        })
        .await
}

/// A node drops the ECCs forwarded to it past a limit between two blocks, and counts them anew
/// when a block arrives.
#[tokio::test]
async fn forwarded_eccs_are_limited_between_blocks() {
    init_test_logger();
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let mut pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let cfg = peer::testonly::PeerConfig { network: chain.make_config(rng), chain: chain.clone() };
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2, &SocketOptions::default())
        .await
        .unwrap();
    let mut peer = PeerHandle::start_endpoint(clock.clock(), ActorSystem::new(), cfg, stream);
    peer.complete_handshake().await;
    pm.wait_for_routing_table(&[(peer.cfg.id(), vec![peer.cfg.id()])]).await;

    for i in 0..ECC_LIMIT {
        assert!(forward_tx(&mut pm, &peer, ecc(i)).await, "ECC {i} was dropped");
    }
    assert!(!forward_tx(&mut pm, &peer, ecc(ECC_LIMIT)).await, "ECC past the limit was accepted");

    // The limit is only for ECCs: a signed transaction still gets through.
    let signed_tx = data::make_signed_transaction(rng).into();
    assert!(forward_tx(&mut pm, &peer, signed_tx).await);

    // A block starts the next count.
    peer.send(PeerMessage::Block(chain.blocks[0].clone())).await;
    pm.events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(tcp::Tier::T2, PeerMessage::Block(_)) => Some(()),
            _ => None,
        })
        .await;
    assert!(forward_tx(&mut pm, &peer, ecc(ECC_LIMIT + 1)).await);
}
