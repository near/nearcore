use std::collections::BTreeMap;
use std::sync::Arc;

use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;
use crate::utils::setups::derive_new_epoch_config_from_boundary;

#[test]
fn resharding_example_test() {
    init_test_logger();

    // Note: having base layout version set to 3 is important
    let version = 3;
    let base_shard_layout = ShardLayout::multi_shard(3, version);
    let epoch_length = 5;
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);
    let validator_id = clients[0].clone();
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
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .build()
        .warmup();

    let node = TestLoopNode::for_account(&env.node_datas, &validator_id);
    let epoch_manager = node.client(&env.test_loop.data).chain.epoch_manager.clone();
    let epoch_id = node.head(env.test_loop_data()).epoch_id;
    assert_eq!(epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout, base_shard_layout);

    env.test_loop.run_until(
        |test_loop_data| {
            let epoch_id = node.head(test_loop_data).epoch_id;
            epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout == new_shard_layout
        },
        Duration::seconds((3 * epoch_length) as i64),
    );

    let epoch_id = node.head(env.test_loop_data()).epoch_id;
    assert_eq!(epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout, new_shard_layout);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
