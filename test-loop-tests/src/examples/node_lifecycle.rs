use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;

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
        .build();

    let stable_node_idx = 1;
    let restart_node_data = &env.node_datas[0];
    let restart_account = restart_node_data.account_id.clone();
    let restart_identifier = restart_node_data.identifier.clone();

    let kill_height = 2 * epoch_length;
    env.runner_for_account(&restart_account).run_until_head_height(kill_height);

    let killed_node_state = env.kill_node(&restart_identifier);

    // Advance by 1 epoch. Keep the gap small enough that the restarted node
    // stays within the epoch sync horizon (default: 2 epochs) so it enters
    // BlockSync instead of EpochSync. EpochSync for a node with existing
    // data triggers a data-reset shutdown, which is not supported in testloop.
    let restart_height = kill_height + epoch_length;
    env.node_runner(stable_node_idx).run_until_head_height(restart_height);

    let new_node_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_node_identifier, killed_node_state);

    assert_eq!(env.node_for_account(&restart_account).head().height, kill_height);

    // Wait for the restarted node to catch up to the stable node.
    let restart_idx = env.account_data_idx(&restart_account);
    let restart_handle = env.node_datas[restart_idx].client_sender.actor_handle();
    let stable_handle = env.node_datas[stable_node_idx].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let restart_h = data.get(&restart_handle).client.chain.head().unwrap().height;
            let stable_h = data.get(&stable_handle).client.chain.head().unwrap().height;
            restart_h >= stable_h
        },
        Duration::seconds(30),
    );
}

#[test]
fn test_add_node() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(4, 0).gc_num_epochs_to_keep(20).build();

    // Let the chain progress for a few blocks
    env.node_runner(0).run_for_number_of_blocks(10);

    // Add a new non-validator tracking node
    let identifier = "new_node";
    let new_account_id = create_account_id(identifier);
    let new_node_state = env.node_state_builder().account_id(&new_account_id).build();
    env.add_node(identifier, new_node_state);

    // Wait for the new node to catch up to a validator.
    let new_idx = env.account_data_idx(&new_account_id);
    let new_handle = env.node_datas[new_idx].client_sender.actor_handle();
    let validator_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_handle).client.chain.head().unwrap().height;
            let val_h = data.get(&validator_handle).client.chain.head().unwrap().height;
            new_h >= val_h
        },
        Duration::seconds(30),
    );
}
