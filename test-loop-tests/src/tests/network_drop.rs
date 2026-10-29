use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

const TARGET_HEIGHT: u64 = 20;
const DROP_RATIO_NUMERATOR: u32 = 1;
const DROP_RATIO_DENOMINATOR: u32 = 20;
const TIMEOUT_SECONDS: i64 = 90;

#[test]
fn network_drop_random_messages() {
    init_test_logger();

    let rng: rand::rngs::StdRng = rand::rngs::StdRng::seed_from_u64(42);
    let rng = Arc::new(RwLock::new(rng));

    let mut env = TestLoopBuilder::new().validators(3, 0).build();

    // Configure the PeerActors to drop some events. This is actually a bit
    // unrealistic on the network level because we use a reliable transport
    // layer protocol (TCP) and peer messages don't get lost. But we can consider
    // it a test of resilience against misbehaving nodes who withhold messages.
    for index in 0..env.node_datas.len() {
        let rng = rng.clone();
        env.drop_messages_from(index, move |_| {
            rng.write().gen_ratio(DROP_RATIO_NUMERATOR, DROP_RATIO_DENOMINATOR)
        });
    }

    // We need to allow more than the default timeout calculated by
    // run_until_head_height because dropped messages slow things down.
    env.node_runner(0)
        .run_until_head_height_with_timeout(TARGET_HEIGHT, Duration::seconds(TIMEOUT_SECONDS));
}
