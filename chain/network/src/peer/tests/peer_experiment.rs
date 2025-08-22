use crate::network_protocol::testonly as data;
use crate::network_protocol::{Encoding, PeerMessage};
use crate::network_protocol::{Ping, T2MessageBody, TieredMessageBody};
use crate::peer::testonly::{PeerConfig, PeerHandle};
use crate::tcp;
use crate::testonly::make_rng;
use near_async::time;
use near_primitives::hash::CryptoHash;
use std::sync::Arc;
use std::time::Duration;

/// Benchmark to measure maximum bytes sendable in 1 second between two peers
#[tokio::test]
async fn peer_throughput_benchmark() {
    let mut rng = make_rng(123456);
    let encoding = Encoding::Proto;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 10));

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

    // Test 1: Ping/Pong throughput
    println!("Testing Ping/Pong message throughput...");

    // Pre-create a ping message and calculate its size
    let ping = Ping { nonce: 0, source: outbound.cfg.network.node_id() };
    let routed_msg = outbound.routed_message(
        TieredMessageBody::T2(Box::new(T2MessageBody::Ping(ping))),
        inbound.cfg.network.node_id(),
        10,
        None,
    );
    let ping_message = PeerMessage::Routed(Box::new(routed_msg));

    // Reset tracker stats before test
    let (outbound_sent_before, _outbound_received_before) = outbound.get_tracker_stats().await;
    let (_inbound_sent_before, inbound_received_before) = inbound.get_tracker_stats().await;

    let mut total_pings = 0u64;
    let start_time = time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(1) {
        outbound.send(ping_message.clone()).await;
        total_pings += 1;
    }

    let elapsed = start_time.elapsed();

    // Get final tracker stats
    let (outbound_sent_after, _outbound_received_after) = outbound.get_tracker_stats().await;
    let (_inbound_sent_after, inbound_received_after) = inbound.get_tracker_stats().await;

    // Calculate throughput from tracker stats
    let sender_bytes_sent = outbound_sent_after - outbound_sent_before;
    let receiver_bytes_received = inbound_received_after - inbound_received_before;

    let sender_bytes_per_sec = sender_bytes_sent as f64 / elapsed.as_secs_f64();
    let sender_mb_per_sec = sender_bytes_per_sec / (1024.0 * 1024.0);
    let receiver_bytes_per_sec = receiver_bytes_received as f64 / elapsed.as_secs_f64();
    let receiver_mb_per_sec = receiver_bytes_per_sec / (1024.0 * 1024.0);

    println!(
        "Ping messages sent: {}, Sender: {:.2} MB/s, Receiver: {:.2} MB/s",
        total_pings, sender_mb_per_sec, receiver_mb_per_sec
    );

    // Test 2: Maximum throughput test with alternating messages
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

    // Use larger message types for maximum bytes throughput
    let block_request_message = PeerMessage::BlockRequest(CryptoHash::default());

    let block_headers_request_message =
        PeerMessage::BlockHeadersRequest(vec![CryptoHash::default()]);

    // Reset tracker stats before test
    let (outbound_sent_before, _outbound_received_before) = outbound.get_tracker_stats().await;
    let (_inbound_sent_before, inbound_received_before) = inbound.get_tracker_stats().await;

    let mut total_messages = 0u64;
    let start_time = time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(1) {
        match total_messages % 3 {
            0 => outbound.send(mixed_ping_message.clone()).await,
            1 => outbound.send(block_request_message.clone()).await,
            2 => outbound.send(block_headers_request_message.clone()).await,
            _ => unreachable!(),
        }
        total_messages += 1;
    }

    let elapsed = start_time.elapsed();

    // Get final tracker stats
    let (outbound_sent_after, _outbound_received_after) = outbound.get_tracker_stats().await;
    let (_inbound_sent_after, inbound_received_after) = inbound.get_tracker_stats().await;

    // Calculate throughput from tracker stats
    let sender_bytes_sent = outbound_sent_after - outbound_sent_before;
    let receiver_bytes_received = inbound_received_after - inbound_received_before;

    let sender_bytes_per_sec = sender_bytes_sent as f64 / elapsed.as_secs_f64();
    let sender_mb_per_sec = sender_bytes_per_sec / (1024.0 * 1024.0);
    let receiver_bytes_per_sec = receiver_bytes_received as f64 / elapsed.as_secs_f64();
    let receiver_mb_per_sec = receiver_bytes_per_sec / (1024.0 * 1024.0);

    println!(
        "Elapsed: {:?}, Total messages sent: {}, Sender: {:.2} MB/s, Receiver: {:.2} MB/s",
        elapsed, total_messages, sender_mb_per_sec, receiver_mb_per_sec
    );
}
