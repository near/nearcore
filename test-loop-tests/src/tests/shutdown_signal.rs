use near_async::time::Duration;
use near_chain_configs::MutableConfigValue;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockHeight;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};

/// Test that the shutdown signal integration with test-loop works correctly.
/// When ClientActor's expected_shutdown triggers, it consumes the shutdown_signal,
/// which is detected by the test-loop and causes events for that node to be ignored.
#[test]
fn test_shutdown_signal_in_testloop() {
    init_test_logger();
    let epoch_length = 10;
    // Use 4 validators so the remaining 3 can continue producing blocks
    // after node 0 shuts down (need >2/3 stake for doomslug).
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);

    let shutdown_height: BlockHeight = 15;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .config_modifier(move |config, idx| {
            if idx == 0 {
                config.expected_shutdown =
                    MutableConfigValue::new(Some(shutdown_height), "expected_shutdown");
            }
        })
        .build()
        .warmup();

    // Run until node 1 advances well past the shutdown height.
    let target_height = shutdown_height + 10;
    env.node_runner(1).run_until_head_height(target_height);

    // Node 0's head should have stopped advancing around the shutdown height,
    // while node 1 continued past it.
    let node0_head = env.node(0).head().height;
    let node1_head = env.node(1).head().height;
    assert!(
        node0_head <= shutdown_height + 2,
        "node 0 head ({node0_head}) should be near shutdown height ({shutdown_height})"
    );
    assert!(
        node1_head >= target_height,
        "node 1 head ({node1_head}) should be at or past target height ({target_height})"
    );

    tracing::info!(node0_head, node1_head, "shutdown signal test passed");

    // Drain remaining events (node 0 events will be ignored since it's denylisted).
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
