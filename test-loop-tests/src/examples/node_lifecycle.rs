use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;

#[test]
fn test_restart_node() {
    init_test_logger();

    // 4 validators with equal stake means that with one unavailable node
    // the chain can still make progress
    let epoch_length = 4;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(20)
        .build()
        .warmup();

    let stable_node_idx = 1;
    let restart_node_data = &env.node_datas[0];
    let restart_account = restart_node_data.account_id.clone();
    let restart_identifier = restart_node_data.identifier.clone();

    let kill_height = 2 * epoch_length;
    env.runner_for_account(&restart_account).run_until_head_height(kill_height);

    let killed_node_state = env.kill_node(&restart_identifier);

    let restart_height = kill_height + 2 * epoch_length;
    env.node_runner(stable_node_idx).run_until_head_height(restart_height);

    let new_node_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_node_identifier, killed_node_state);

    assert_eq!(env.node_for_account(&restart_account).head().height, kill_height);

    // Give a few blocks for the restarted node to catch up
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);

    assert_eq!(env.node_for_account(&restart_account).head().height, env.node(1).head().height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
fn test_add_node() {
    init_test_logger();

    let mut env =
        TestLoopBuilder::new().validators(4, 0).gc_num_epochs_to_keep(20).build().warmup();

    // Let the chain progress for a few blocks
    env.node_runner(0).run_for_number_of_blocks(10);

    // Add a new non-validator tracking node
    let identifier = "new_node";
    let new_account_id = create_account_id(identifier);
    let new_node_state = env.node_state_builder().account_id(&new_account_id).build();
    env.add_node(identifier, new_node_state);

    // Let the network run so the new node can catch up
    env.node_runner(0).run_for_number_of_blocks(5);

    // Verify the new node synced to the same height as a validator
    assert_eq!(env.node_for_account(&new_account_id).head().height, env.node(0).head().height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
