use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};

#[test]
fn test_restart_node() {
    init_test_logger();

    // 4 validators with equal stake means that with one unavailable node
    // the chain can still make progress
    let num_validators = 4;
    let validators_spec = create_validators_spec(num_validators, 0);
    let clients = validators_spec_clients(&validators_spec);
    let epoch_length = 4;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
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

    /*
    // TODO(pugachag): add a separate test for adding new node based on the code below
    // Add new node
    let genesis = env.shared_state.genesis.clone();
    let tempdir_path = env.shared_state.tempdir.path().to_path_buf();
    let new_node_state = NodeStateBuilder::new(genesis, tempdir_path)
        .account_id(accounts[NUM_CLIENTS].clone())
        .build();
    env.add_node(accounts[NUM_CLIENTS].as_str(), new_node_state);
    env.test_loop.run_for(Duration::seconds(3 * epoch_length as i64));
    */

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
