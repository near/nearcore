use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::HandlerResult;
use near_async::time::Duration;
use near_network::types::NetworkResponses;
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
    for node_data in &env.node_datas {
        let rng = rng.clone();
        let peer_actor_handle = node_data.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| {
            let mut rng = rng.write();
            if rng.gen_ratio(DROP_RATIO_NUMERATOR, DROP_RATIO_DENOMINATOR) {
                return HandlerResult::Handled(NetworkResponses::NoResponse);
            }
            HandlerResult::Unhandled(request)
        }));
    }

    // We need to allow more than the default timeout calculated by
    // run_until_head_height because dropped messages slow things down.
    env.node_runner(0)
        .run_until_head_height_with_timeout(TARGET_HEIGHT, Duration::seconds(TIMEOUT_SECONDS));
}
