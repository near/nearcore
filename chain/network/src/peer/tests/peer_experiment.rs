use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, PeerMessage, PeersRequest};
use crate::network_protocol::{Ping, T2MessageBody, TieredMessageBody};
use crate::peer::testonly::{Event, PeerConfig, PeerHandle};
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::testonly::make_rng;
use crate::types::PartialEdgeInfo;
use near_async::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{SignedTransaction, Transaction, TransactionV0};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Simple experiment to connect two peer actors and send dummy data between them
#[tokio::test]
async fn peer_actor_experiment() {
    init_test_logger();

    // Create a shared chain for both peers
    let encoding = Encoding::Borsh;

    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        force_encoding: Some(encoding),
    };
    let outbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        force_encoding: Some(encoding),
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

    info!(target:"test","RequestUpdateNonce");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
        &outbound.cfg.network.node_id(),
        &inbound.cfg.network.node_id(),
        15,
        &outbound.cfg.network.node_key,
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;
}

/// Benchmark to measure maximum bytes sendable in 1 second between two peers
#[tokio::test]
async fn peer_throughput_benchmark() {
    // init_test_logger();

    let encoding = Encoding::Borsh;
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        force_encoding: Some(encoding),
    };
    let outbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: chain.make_config(&mut rng),
        force_encoding: Some(encoding),
    };

    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let mut inbound = PeerHandle::start_endpoint(clock.clock(), inbound_cfg, inbound_stream).await;
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), outbound_cfg, outbound_stream).await;

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;

    // Test 1: Ping/Pong throughput (small messages)
    println!("Testing Ping/Pong throughput...");

    let test_duration = Duration::from_secs(1);
    let mut total_pings = 0u64;
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < test_duration {
        let ping = Ping { nonce: total_pings, source: outbound.cfg.network.node_id() };

        let routed_msg = outbound.routed_message(
            TieredMessageBody::T2(Box::new(T2MessageBody::Ping(ping))),
            inbound.cfg.network.node_id(),
            10,
            None,
        );

        let message = PeerMessage::Routed(Box::new(routed_msg));
        outbound.send(message).await;
        total_pings += 1;

        // tokio::time::sleep(Duration::from_micros(10)).await;
    }

    let elapsed = start_time.elapsed();
    let pings_per_sec = total_pings as f64 / elapsed.as_secs_f64();

    println!("Ping messages sent: {}, Pings per second: {:.2}", total_pings, pings_per_sec);

    // Test 2: Transaction throughput
    println!("Testing Transaction message throughput...");

    let mut total_txs = 0u64;
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < test_duration {
        let transaction = Transaction::V0(TransactionV0 {
            signer_id: "test.near".parse().unwrap(),
            public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
            nonce: total_txs,
            receiver_id: "test.near".parse().unwrap(),
            block_hash: CryptoHash::default(),
            actions: vec![],
        });

        let signed_transaction = SignedTransaction::new(
            near_crypto::Signature::empty(near_crypto::KeyType::ED25519),
            transaction,
        );

        let message = PeerMessage::Transaction(signed_transaction);
        outbound.send(message).await;
        total_txs += 1;
        // tokio::time::sleep(Duration::from_micros(10)).await;
    }

    let elapsed = start_time.elapsed();
    let txs_per_sec = total_txs as f64 / elapsed.as_secs_f64();

    println!(
        "Transaction messages sent: {}, Transactions per second: {:.2}",
        total_txs, txs_per_sec
    );

    // Test 3: Maximum throughput test with alternating messages
    println!("Testing maximum message throughput...");

    let mut total_messages = 0u64;
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < test_duration {
        // Alternate between different message types to test overall throughput
        match total_messages % 4 {
            0 => {
                let ping = Ping { nonce: total_messages, source: outbound.cfg.network.node_id() };
                let routed_msg = outbound.routed_message(
                    TieredMessageBody::T2(Box::new(T2MessageBody::Ping(ping))),
                    inbound.cfg.network.node_id(),
                    10,
                    None,
                );
                let message = PeerMessage::Routed(Box::new(routed_msg));
                outbound.send(message).await;
            }
            1 => {
                let message = PeerMessage::BlockRequest(CryptoHash::default());
                outbound.send(message).await;
            }
            2 => {
                let peers_request = PeersRequest { max_peers: Some(10), max_direct_peers: Some(5) };
                let message = PeerMessage::PeersRequest(peers_request);
                outbound.send(message).await;
            }
            3 => {
                let transaction = Transaction::V0(TransactionV0 {
                    signer_id: "test.near".parse().unwrap(),
                    public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
                    nonce: total_messages,
                    receiver_id: "test.near".parse().unwrap(),
                    block_hash: CryptoHash::default(),
                    actions: vec![],
                });

                let signed_transaction = SignedTransaction::new(
                    near_crypto::Signature::empty(near_crypto::KeyType::ED25519),
                    transaction,
                );

                let message = PeerMessage::Transaction(signed_transaction);
                outbound.send(message).await;
            }
            _ => unreachable!(),
        }

        total_messages += 1;
    }

    let elapsed = start_time.elapsed();
    let messages_per_sec = total_messages as f64 / elapsed.as_secs_f64();

    println!(
        "Total messages sent: {}, Messages per second: {:.2}",
        total_messages, messages_per_sec
    );
}
