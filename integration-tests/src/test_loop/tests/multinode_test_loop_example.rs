use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{
    GenesisAndEpochConfigParams, ValidatorsSpec, build_genesis_and_epoch_config_store,
};
use near_client::test_utils::test_loop::ClientQueries;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::execute_money_transfers;

const NUM_CLIENTS: usize = 4;

#[test]
fn slow_test_client_with_multi_test_loop() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let epoch_length = 10;
    let shard_layout = ShardLayout::simple_v1(&["account3", "account5", "account7"]);
    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);

    let (genesis, epoch_config_store) = build_genesis_and_epoch_config_store(
        GenesisAndEpochConfigParams {
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
            shard_layout,
            validators_spec,
            accounts: &accounts,
        },
        |genesis_builder| genesis_builder.genesis_height(10000).transaction_validity_period(1000),
        |epoch_config_builder| {
            epoch_config_builder.shuffle_shard_assignment_for_chunk_producers(true)
        },
    );

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } =
        builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build();

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
        Duration::seconds(10),
    );

    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let later_epoch_tracked_shards = clients.tracked_shards_for_each_client();
    tracing::info!("Later epoch tracked shards: {:?}", later_epoch_tracked_shards);
    assert_ne!(first_epoch_tracked_shards, later_epoch_tracked_shards);

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
