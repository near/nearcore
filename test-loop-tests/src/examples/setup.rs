use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::num_rational::Rational32;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;

/// Demonstrates the most basic single-validator test loop env setup.
/// Uses all defaults: one validator, one shard, no RPC.
#[test]
fn test_setup_default() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().build().warmup();

    env.validator_runner().run_for_number_of_blocks(1);

    env.shutdown_and_drain_remaining_events(Duration::seconds(1));
}

/// Demonstrates a multi-validator setup with an RPC node.
/// Creates 2 block+chunk producers, 1 chunk-only validator, and 1 RPC node.
#[test]
fn test_setup_multiple_validators_with_rpc() {
    init_test_logger();

    let num_block_and_chunk_producers = 2;
    let num_chunk_validators_only = 1;
    let mut env = TestLoopBuilder::new()
        .validators(num_block_and_chunk_producers, num_chunk_validators_only)
        .enable_rpc()
        .build()
        .warmup();

    // Block and chunk producer nodes
    env.node_runner(0).run_for_number_of_blocks(1);
    env.node_runner(1).run_for_number_of_blocks(1);
    // Chunk validator node
    env.node_runner(2).run_for_number_of_blocks(1);
    // RPC node
    env.rpc_runner().run_for_number_of_blocks(1);

    env.shutdown_and_drain_remaining_events(Duration::seconds(1));
}

/// Demonstrates a multi-shard setup with one chunk producer per shard.
/// Each validator tracks exactly one shard.
#[test]
fn test_setup_multishard() {
    init_test_logger();

    let num_shards = 2;
    let env =
        TestLoopBuilder::new().num_shards(num_shards).chunk_producer_per_shard().build().warmup();

    // One validator node per shard.
    assert_eq!(env.node_datas.len(), num_shards);

    // Each node tracks exactly one shard, and they track different shards.
    let node0_tracked_shards = env.node(0).tracked_shards();
    assert_eq!(node0_tracked_shards.len(), 1);
    let node1_tracked_shards = env.node(1).tracked_shards();
    assert_eq!(node1_tracked_shards.len(), 1);
    assert_ne!(&node0_tracked_shards[0], &node1_tracked_shards[0]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(1));
}

/// Demonstrates adding a user account via the builder API.
#[test]
fn test_setup_user_account() {
    init_test_logger();

    let user_account = create_account_id("user0");
    let initial_balance = Balance::from_near(10);
    let env =
        TestLoopBuilder::new().add_user_account(&user_account, initial_balance).build().warmup();

    assert_eq!(env.validator().query_balance(&user_account), initial_balance);

    env.shutdown_and_drain_remaining_events(Duration::seconds(1));
}

/// Demonstrates overriding genesis parameters with non-default values
#[test]
fn test_setup_genesis_overrides() {
    init_test_logger();

    let epoch_length = 42;
    let gas_limit = Gas::from_teragas(333);
    let protocol_version = PROTOCOL_VERSION - 1;
    let genesis_height = 100;
    let transaction_validity_period = 50;
    let max_inflation_rate = Rational32::new(1, 10);
    let minimum_stake_ratio = Rational32::new(1, 100);
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let max_gas_price = Balance::from_yoctonear(10_000_000_000);

    let env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .gas_limit(gas_limit)
        .protocol_version(protocol_version)
        .genesis_height(genesis_height)
        .transaction_validity_period(transaction_validity_period)
        .max_inflation_rate(max_inflation_rate)
        .minimum_stake_ratio(minimum_stake_ratio)
        .gas_prices(min_gas_price, max_gas_price)
        .build()
        .warmup();

    let genesis_config = &env.shared_state.genesis.config;
    assert_eq!(genesis_config.epoch_length, epoch_length);
    assert_eq!(genesis_config.gas_limit, gas_limit);
    assert_eq!(genesis_config.protocol_version, protocol_version);
    assert_eq!(genesis_config.genesis_height, genesis_height);
    assert_eq!(genesis_config.transaction_validity_period, transaction_validity_period);
    assert_eq!(genesis_config.max_inflation_rate, max_inflation_rate);
    assert_eq!(genesis_config.minimum_stake_ratio, minimum_stake_ratio);
    assert_eq!(genesis_config.min_gas_price, min_gas_price);
    assert_eq!(genesis_config.max_gas_price, max_gas_price);

    env.shutdown_and_drain_remaining_events(Duration::seconds(1));
}
