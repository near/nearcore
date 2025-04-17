use std::collections::BTreeMap;
use std::sync::Arc;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::setups::derive_new_epoch_config_from_boundary;

#[test]
fn resharding_example_test() {
    init_test_logger();

    // Note: having base layout version set to 3 is important
    let version = 3;
    let base_shard_layout = ShardLayout::multi_shard(3, version);
    let epoch_length = 5;
    let chunk_producer = "cp0";
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .epoch_length(epoch_length)
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let new_epoch_config = {
        let boundary_account: AccountId = "boundary".parse().unwrap();
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account)
    };
    let new_shard_layout = new_epoch_config.shard_layout.clone();

    let epoch_configs = vec![
        (genesis.config.protocol_version, Arc::new(base_epoch_config)),
        (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
    ];
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(vec![chunk_producer.parse().unwrap()])
        .epoch_config_store(epoch_config_store)
        .build()
        .warmup();

    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let chain_store = env.test_loop.data.get(&client_handle).client.chain.chain_store.clone();
    let epoch_manager = env.test_loop.data.get(&client_handle).client.epoch_manager.clone();

    let epoch_id = chain_store.head().unwrap().epoch_id;
    assert_eq!(epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout, base_shard_layout);

    env.test_loop.run_until(
        |_| {
            let epoch_id = chain_store.head().unwrap().epoch_id;
            epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout == new_shard_layout
        },
        Duration::seconds((3 * epoch_length) as i64),
    );

    let epoch_id = chain_store.head().unwrap().epoch_id;
    assert_eq!(epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout, new_shard_layout);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
