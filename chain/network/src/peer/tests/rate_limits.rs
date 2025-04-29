use crate::broadcast::Receiver;
use crate::config::NetworkConfig;
use crate::network_protocol::{Encoding, PeerMessage};
use crate::network_protocol::{PartialEncodedChunkRequestMsg, RoutedMessageBody, testonly as data};
use crate::peer::testonly::{Event, PeerConfig, PeerHandle};
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::rate_limits::messages_limits;
use crate::tcp;
use crate::testonly::{Rng, make_rng};
use near_async::time::FakeClock;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use rand::Rng as _;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep, sleep_until};

#[tokio::test]
// Verifies that peer traffic is rate limited per message type. Not all messages are rate limited.
// This test works by sending many messages very quickly and then check how many of them
// were effectively processed by the receiver.
async fn test_message_rate_limits() -> anyhow::Result<()> {
    init_test_logger();
    tracing::info!("test_message_rate_limits");

    let mut clock = FakeClock::default();
    let mut rng = make_rng(89028037453);
    let (outbound, inbound) = setup_test_peers(&mut clock, &mut rng).await;

    const MESSAGES: u32 = 7;
    // Let's gather all events received from now on. We'll check them later, after producing messages.
    let mut events = inbound.events.from_now();
    let messages_samples = send_messages(&inbound, &outbound, &mut rng, MESSAGES).await;

    // Check how many messages of each type have been received.
    let messages_received =
        wait_for_similar_messages(&messages_samples, &mut events, Duration::from_secs(3)).await;
    tracing::debug!(target:"test","received {messages_received:?} messages");
    // BlockRequest gets rate limited (7 sent vs 5 bucket_start).
    assert!(messages_received[0] < MESSAGES);
    // PartialEncodedChunkRequest gets rate limited (7 sent vs 5 bucket_start).
    assert!(messages_received[1] < MESSAGES);
    // Transaction doesn't get rate limited (7 sent vs 50 bucket_start).
    assert_eq!(messages_received[2], MESSAGES);

    Ok(())
}

#[tokio::test]
// Verifies that peer traffic is not rate limited when messages are sent at regular intervals,
// and the total number of messages is below the limit.
async fn slow_test_message_rate_limits_over_time() -> anyhow::Result<()> {
    init_test_logger();
    tracing::info!("test_message_rate_limits_over_time");

    let mut clock = FakeClock::default();
    let mut rng = make_rng(89028037453);
    let (outbound, inbound) = setup_test_peers(&mut clock, &mut rng).await;

    const MESSAGES: u32 = 4;
    const INTERVAL: Duration = Duration::from_secs(2);
    // Let's gather all events received from now on. We'll check them later, after producing messages.
    let mut events = inbound.events.from_now();

    // Send 4 messages of each type every 2 seconds, three times.
    let mut messages_samples = Vec::new();
    let now = clock.now();
    for i in 0..3 {
        messages_samples = send_messages(&inbound, &outbound, &mut rng, MESSAGES).await;
        // Advance the fake clock to refresh rate limits.
        clock.advance_until(now + INTERVAL * (i + 1));
        // Give some time to peer actors to process messages.
        sleep(Duration::from_secs(1)).await;
    }

    let messages_received =
        wait_for_similar_messages(&messages_samples, &mut events, Duration::from_secs(3)).await;
    tracing::debug!(target:"test","received {messages_received:?} messages");
    // BlockRequest and PartialEncodedChunkRequest don't get rate limited
    // 12 sent vs 5 bucket_start + 2.5 refilled * 4s
    assert_eq!(messages_received[0], MESSAGES * 3);
    assert_eq!(messages_received[1], MESSAGES * 3);
    // Transaction doesn't get rate limited (12 sent vs 50 bucket_start).
    assert_eq!(messages_received[2], MESSAGES * 3);

    Ok(())
}

/// Waits up to `duration` and then checks how many events equal (in type only) to each one of `samples`
/// have been received.
///
/// Returns a vector of the same size of `samples`.
async fn wait_for_similar_messages(
    samples: &[PeerMessage],
    events: &mut Receiver<Event>,
    duration: Duration,
) -> Vec<u32> {
    let mut messages_received = vec![0; 3];
    sleep_until(Instant::now() + duration).await;
    while let Some(event) = events.try_recv() {
        match event {
            Event::Network(PME::MessageProcessed(_, got)) => {
                for (i, sample) in samples.iter().enumerate() {
                    if sample.msg_variant() == got.msg_variant() {
                        messages_received[i] += 1;
                    }
                }
            }
            _ => {}
        }
    }
    messages_received
}

/// Setup two connected peers.
///
/// Rate limits configuration:
/// - `BlockRequest`, `PartialEncodedChunkRequest`: bucket_start = 5, bucket_max = 10, refill_rate = 2.5/s
/// - `Transaction`: bucket_start = bucket_max = 50, refill_rate = 5/s
async fn setup_test_peers(clock: &FakeClock, mut rng: &mut Rng) -> (PeerHandle, PeerHandle) {
    let chain = Arc::new(data::Chain::make(clock, &mut rng, 12));

    // Customize the network configuration to set some arbitrary rate limits.
    let add_rate_limits = |mut network_config: NetworkConfig| {
        let rate_limits = &mut network_config.received_messages_rate_limits.rate_limits;
        use messages_limits::RateLimitedPeerMessageKey::*;
        rate_limits
            .insert(BlockRequest, messages_limits::SingleMessageConfig::new(10, 2.5, Some(5)));
        rate_limits.insert(
            PartialEncodedChunkRequest,
            messages_limits::SingleMessageConfig::new(10, 2.5, Some(5)),
        );
        rate_limits.insert(Transaction, messages_limits::SingleMessageConfig::new(50, 5.0, None));
        network_config
    };

    let inbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: add_rate_limits(chain.make_config(&mut rng)),
        force_encoding: Some(Encoding::Proto),
    };
    let outbound_cfg = PeerConfig {
        chain: chain.clone(),
        network: add_rate_limits(chain.make_config(&mut rng)),
        force_encoding: Some(Encoding::Proto),
    };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let mut inbound = PeerHandle::start_endpoint(clock.clock(), inbound_cfg, inbound_stream).await;
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), outbound_cfg, outbound_stream).await;

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;
    (outbound, inbound)
}

/// Sends samples of various messages:
/// - `BlockRequest`
/// - `PartialEncodedChunkRequest`
/// - `Transaction`
///
/// Messages are sent `count` times each.
///
/// Returns a vector with an example of one of each message above (useful for comparisons.)
async fn send_messages(
    inbound: &PeerHandle,
    outbound: &PeerHandle,
    rng: &mut Rng,
    count: u32,
) -> Vec<PeerMessage> {
    let mut messages_samples = Vec::new();

    tracing::info!(target:"test","send BlockRequest");
    let message = PeerMessage::BlockRequest(CryptoHash::default());
    for _ in 0..count {
        outbound.send(message.clone()).await;
    }
    messages_samples.push(message);

    tracing::info!(target:"test","send PartialEncodedChunkRequest");
    // Duplicated routed messages are filtered out so we must tweak each message to make it unique.

    for i in 0..count {
        let message = PeerMessage::Routed(Box::new(outbound.routed_message(
            RoutedMessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
                chunk_hash: outbound.cfg.chain.blocks[5].chunks()[2].chunk_hash(),
                part_ords: vec![rng.r#gen()],
                tracking_shards: Default::default(),
            }),
            inbound.cfg.id(),
            1,
            None,
        )));
        outbound.send(message.clone()).await;
        if i == count - 1 {
            messages_samples.push(message);
        }
    }

    tracing::info!(target:"test","send Transaction");
    let message = PeerMessage::Transaction(data::make_signed_transaction(rng));
    for _ in 0..count {
        outbound.send(message.clone()).await;
    }
    messages_samples.push(message);

    messages_samples
}
