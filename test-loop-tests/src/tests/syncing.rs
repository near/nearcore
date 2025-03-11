use crate::setup::builder::{NodeStateBuilder, TestLoopBuilder};
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::execute_money_transfers;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

const NUM_CLIENTS: usize = 4;

// Test that a new node that only has genesis can use whatever method available
// to sync up to the current state of the network.
#[test]
fn slow_test_sync_from_genesis() {
    init_test_logger();
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let first_epoch_tracked_shards = {
        let clients = node_datas
            .iter()
            .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
            .collect_vec();
        clients.tracked_shards_for_each_client()
    };
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    execute_money_transfers(&mut test_loop, &node_datas, &accounts).unwrap();

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height > 10050
        },
        Duration::seconds(50),
    );

    // Add new node
    let genesis = shared_state.genesis.clone();
    let tempdir_path = shared_state.tempdir.path().to_path_buf();
    let new_node_state = NodeStateBuilder::new(genesis, tempdir_path)
        .account_id(accounts[NUM_CLIENTS].clone())
        .build();
    let mut env = TestLoopEnv { test_loop, node_datas, shared_state };
    env.add_node(accounts[NUM_CLIENTS].as_str(), new_node_state);

    // Check that the new node will reach a high height as well.
    let new_node = env.node_datas.last().unwrap().client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| test_loop_data.get(&new_node).client.chain.head().unwrap().height > 10050,
        Duration::seconds(20),
    );
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
