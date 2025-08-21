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

    // Pre-create a ping message and calculate its size
    let ping = Ping { nonce: 0, source: outbound.cfg.network.node_id() };
    let routed_msg = outbound.routed_message(
        TieredMessageBody::T2(Box::new(T2MessageBody::Ping(ping))),
        inbound.cfg.network.node_id(),
        10,
        None,
    );
    let ping_message = PeerMessage::Routed(Box::new(routed_msg));
    let ping_message_bytes = ping_message.serialize(encoding).len() as u64;

    let test_duration = Duration::from_secs(1);
    let mut total_pings = 0u64;
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < test_duration {
        outbound.send(ping_message.clone()).await;
        total_pings += 1;
        // tokio::time::sleep(Duration::from_micros(10)).await;
    }

    let elapsed = start_time.elapsed();
    let pings_per_sec = total_pings as f64 / elapsed.as_secs_f64();
    let ping_bytes_per_sec = (total_pings * ping_message_bytes) as f64 / elapsed.as_secs_f64();
    let ping_mb_per_sec = ping_bytes_per_sec / (1024.0 * 1024.0);

    println!(
        "Ping messages sent: {}, Pings per second: {:.2}, Bytes per second: {:.2}, MB/s: {:.2}",
        total_pings, pings_per_sec, ping_bytes_per_sec, ping_mb_per_sec
    );

    // Test 2: Transaction throughput
    println!("Testing Transaction message throughput...");

    // Pre-create a transaction message and calculate its size
    let transaction = Transaction::V0(TransactionV0 {
        signer_id: "test.near".parse().unwrap(),
        public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
        nonce: 0,
        receiver_id: "test.near".parse().unwrap(),
        block_hash: CryptoHash::default(),
        actions: vec![],
    });

    let signed_transaction = SignedTransaction::new(
        near_crypto::Signature::empty(near_crypto::KeyType::ED25519),
        transaction,
    );

    let tx_message = PeerMessage::Transaction(signed_transaction);
    let tx_message_bytes = tx_message.serialize(encoding).len() as u64;

    let mut total_txs = 0u64;
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < test_duration {
        outbound.send(tx_message.clone()).await;
        total_txs += 1;
        // tokio::time::sleep(Duration::from_micros(10)).await;
    }

    let elapsed = start_time.elapsed();
    let txs_per_sec = total_txs as f64 / elapsed.as_secs_f64();
    let tx_bytes_per_sec = (total_txs * tx_message_bytes) as f64 / elapsed.as_secs_f64();
    let tx_mb_per_sec = tx_bytes_per_sec / (1024.0 * 1024.0);

    println!(
        "Transaction messages sent: {}, Transactions per second: {:.2}, Bytes per second: {:.2}, MB/s: {:.2}",
        total_txs, txs_per_sec, tx_bytes_per_sec, tx_mb_per_sec
    );

    // Test 3: Maximum throughput test with alternating messages
    println!("Testing maximum message throughput...");

    // Pre-create all message types and calculate their sizes
    let ping = Ping { nonce: 0, source: outbound.cfg.network.node_id() };
    let routed_msg = outbound.routed_message(
        TieredMessageBody::T2(Box::new(T2MessageBody::Ping(ping))),
        inbound.cfg.network.node_id(),
        10,
        None,
    );
    let mixed_ping_message = PeerMessage::Routed(Box::new(routed_msg));
    let mixed_ping_bytes = mixed_ping_message.serialize(encoding).len() as u64;

    let block_request_message = PeerMessage::BlockRequest(CryptoHash::default());
    let block_request_bytes = block_request_message.serialize(encoding).len() as u64;

    let peers_request = PeersRequest { max_peers: Some(10), max_direct_peers: Some(5) };
    let peers_request_message = PeerMessage::PeersRequest(peers_request);
    let peers_request_bytes = peers_request_message.serialize(encoding).len() as u64;

    let mixed_transaction = Transaction::V0(TransactionV0 {
        signer_id: "test.near".parse().unwrap(),
        public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
        nonce: 0,
        receiver_id: "test.near".parse().unwrap(),
        block_hash: CryptoHash::default(),
        actions: vec![],
    });

    let mixed_signed_transaction = SignedTransaction::new(
        near_crypto::Signature::empty(near_crypto::KeyType::ED25519),
        mixed_transaction,
    );

    let mixed_tx_message = PeerMessage::Transaction(mixed_signed_transaction);
    let mixed_tx_bytes = mixed_tx_message.serialize(encoding).len() as u64;

    let mut total_messages = 0u64;
    let mut total_bytes = 0u64;
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < test_duration {
        // Alternate between different message types to test overall throughput
        let (message, message_bytes) = match total_messages % 4 {
            0 => (mixed_ping_message.clone(), mixed_ping_bytes),
            1 => (block_request_message.clone(), block_request_bytes),
            2 => (peers_request_message.clone(), peers_request_bytes),
            3 => (mixed_tx_message.clone(), mixed_tx_bytes),
            _ => unreachable!(),
        };

        total_bytes += message_bytes;
        outbound.send(message).await;
        total_messages += 1;
    }

    let elapsed = start_time.elapsed();
    let messages_per_sec = total_messages as f64 / elapsed.as_secs_f64();
    let bytes_per_sec = total_bytes as f64 / elapsed.as_secs_f64();
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);

    println!(
        "Total messages sent: {}, Messages per second: {:.2}, Bytes per second: {:.2}, MB/s: {:.2}",
        total_messages, messages_per_sec, bytes_per_sec, mb_per_sec
    );
}
