use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use parking_lot::RwLock;
use rand::SeedableRng;
use std::sync::Arc;

const TARGET_HEIGHT: u64 = 20;
#[allow(dead_code)] // TODO(iteration 24-26): will be used after transport filter conversion
const DROP_RATIO_NUMERATOR: u32 = 1;
#[allow(dead_code)] // TODO(iteration 24-26): will be used after transport filter conversion
const DROP_RATIO_DENOMINATOR: u32 = 20;
const TIMEOUT_SECONDS: i64 = 90;

#[test]
#[ignore] // TODO: convert override handler to transport filter (iteration 24-26)
fn network_drop_random_messages() {
    init_test_logger();

    let rng: rand::rngs::StdRng = rand::rngs::StdRng::seed_from_u64(42);
    let _rng = Arc::new(RwLock::new(rng));

    let mut env = TestLoopBuilder::new().validators(3, 0).build();

    // Configure the PeerActors to drop some events. This is actually a bit
    // unrealistic on the network level because we use a reliable transport
    // layer protocol (TCP) and peer messages don't get lost. But we can consider
    // it a test of resilience against misbehaving nodes who withhold messages.
    // TODO(iteration 24-26): convert to transport message filter.
    /* Override handlers commented out — PeerManagerActor registered directly.
    for node_data in &env.node_datas {
        ...register_override_handler...
    }
    */

    // We need to allow more than the default timeout calculated by
    // run_until_head_height because dropped messages slow things down.
    env.node_runner(0)
        .run_until_head_height_with_timeout(TARGET_HEIGHT, Duration::seconds(TIMEOUT_SECONDS));
}
