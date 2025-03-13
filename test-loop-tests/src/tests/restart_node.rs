use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

use crate::setup::builder::{NodeStateBuilder, TestLoopBuilder};
use crate::utils::ONE_NEAR;

const NUM_CLIENTS: usize = 4;

#[test]
fn test_restart_node() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let epoch_length = 4;
    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .gas_limit_one_petagas()
        .build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .gc_num_epochs_to_keep(20)
        .build()
        .warmup();

    env.test_loop.run_for(Duration::seconds(2 * epoch_length as i64));

    // kill node
    let killed_node_state = env.kill_node("account0");
    env.test_loop.run_for(Duration::seconds(2 * epoch_length as i64));

    // restart node
    env.restart_node("account0-restart", killed_node_state);
    env.test_loop.run_for(Duration::seconds(3 * epoch_length as i64));

    // Add new node
    let genesis = env.shared_state.genesis.clone();
    let tempdir_path = env.shared_state.tempdir.path().to_path_buf();
    let new_node_state = NodeStateBuilder::new(genesis, tempdir_path)
        .account_id(accounts[NUM_CLIENTS].clone())
        .build();
    env.add_node(accounts[NUM_CLIENTS].as_str(), new_node_state);
    env.test_loop.run_for(Duration::seconds(3 * epoch_length as i64));

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
