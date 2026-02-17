use std::sync::Arc;

use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use parking_lot::RwLock;
use rand::{Rng, SeedableRng};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};

const TARGET_HEIGHT: u64 = 20;
const DROP_RATIO_NUMERATOR: u32 = 1;
const DROP_RATIO_DENOMINATOR: u32 = 20;
const TIMEOUT_SECONDS: i64 = 90;

#[test]
fn network_drop_random_messages() {
    init_test_logger();

    let rng: rand::rngs::StdRng = rand::rngs::StdRng::seed_from_u64(42);
    let rng = Arc::new(RwLock::new(rng));

    let validators_spec = create_validators_spec(3, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder().validators_spec(validators_spec).build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

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
                return None;
            }
            Some(request)
        }));
    }

    // We need to allow more than the default timeout calculated by
    // run_until_head_height because dropped messages slow things down.
    env.node_runner(0)
        .run_until_head_height_with_timeout(TARGET_HEIGHT, Duration::seconds(TIMEOUT_SECONDS));

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
