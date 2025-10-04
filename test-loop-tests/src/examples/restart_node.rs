use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

#[test]
fn test_restart_node() {
    init_test_logger();

    // 4 validators with equal stake means that with one unavailable node
    // the chain can still make progress
    let num_validators = 4;
    let validators_spec = create_validators_spec(num_validators, 0);
    let clients = validators_spec_clients(&validators_spec);
    let client_to_restart = clients[0].clone();
    let stable_client = clients[1].clone();
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

    let kill_height = 2 * epoch_length;
    TestLoopNode::for_account(&env.node_datas, &client_to_restart)
        .run_until_head_height(&mut env.test_loop, kill_height);

    // kill node
    let node_identifier = env.node_datas[0].identifier.clone();
    let killed_node_state = env.kill_node(&node_identifier);

    let restart_height = kill_height + 2 * epoch_length;
    TestLoopNode::for_account(&env.node_datas, &stable_client)
        .run_until_head_height(&mut env.test_loop, restart_height);
    // restart node
    let new_node_identifier = format!("{}-restart", node_identifier);
    env.restart_node(&new_node_identifier, killed_node_state);

    let restarted_node = TestLoopNode::for_account(&env.node_datas, &client_to_restart);
    let stable_node = TestLoopNode::for_account(&env.node_datas, &stable_client);
    assert_eq!(restarted_node.head(env.test_loop_data()).height, kill_height);

    // Give a few blocks for the restarted node to catch up
    stable_node.run_for_number_of_blocks(&mut env.test_loop, 5);

    assert_eq!(
        restarted_node.head(env.test_loop_data()).height,
        stable_node.head(env.test_loop_data()).height,
    );

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
