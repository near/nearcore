use std::collections::BTreeMap;
use std::sync::Arc;

use near_async::time::Duration;
use near_chain::spice_core::get_last_certified_block_header;
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

use super::spice_utils::delay_endorsements_propagation;

/// Regression test: verifies that block processing works across a resharding
/// boundary in SPICE mode. A bug in get_last_certified_execution_results_for_next_block
/// used block_header.epoch_id() instead of last_certified_block_header.epoch_id()
/// to compute num_shards. With resharding and certification lag, this caused a
/// panic when the shard count changed between the certified block's epoch and
/// the current block's epoch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_certified_results_across_resharding() {
    init_test_logger();

    let version = 3;
    let base_shard_layout = ShardLayout::multi_shard(1, version);
    let epoch_length = 5;
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout)
        .epoch_length(epoch_length)
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let boundary_account: AccountId = "boundary".parse().unwrap();
    let (new_epoch_config, new_shard_layout) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);

    let epoch_configs = vec![
        (genesis.config.protocol_version, Arc::new(base_epoch_config)),
        (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
    ];
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .build();

    let execution_delay = 2;
    delay_endorsements_propagation(&mut env, execution_delay);
    env = env.warmup();

    let node = TestLoopNode::from(&env.node_datas[0]);
    let epoch_manager = node.client(env.test_loop_data()).epoch_manager.clone();

    env.test_loop.run_until(
        |test_loop_data| {
            let epoch_id = node.head(test_loop_data).epoch_id;
            epoch_manager.get_shard_layout(&epoch_id).unwrap() == new_shard_layout
        },
        Duration::seconds((3 * epoch_length) as i64),
    );
    let new_epoch_start = node.head(env.test_loop_data()).height;

    // Run a few more blocks in the resharded epoch to exercise the code path
    // where last_certified_block is in the old epoch but current block is in the new epoch.
    node.run_for_number_of_blocks(&mut env.test_loop, 5);

    // Assert that the first block of the resharded epoch has its last certified
    // block in the previous epoch with a different number of shards.
    let chain_store = &node.client(env.test_loop_data()).chain.chain_store;
    let header = chain_store.get_block_header_by_height(new_epoch_start).unwrap();
    let last_certified = get_last_certified_block_header(chain_store, header.hash()).unwrap();
    let certified_shard_layout = epoch_manager.get_shard_layout(last_certified.epoch_id()).unwrap();
    assert_ne!(
        epoch_manager.get_shard_layout(header.epoch_id()).unwrap(),
        certified_shard_layout,
        "expected the first block of the resharded epoch to have its last certified block in the previous epoch with different shard count"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
