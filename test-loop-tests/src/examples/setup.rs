use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;

use crate::setup::builder::TestLoopBuilder;

/// Demonstrates the most basic single-validator test loop env setup.
#[test]
fn test_setup_default() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().build().warmup();

    env.validator_runner().run_for_number_of_blocks(1);

    env.shutdown_and_drain_remaining_events(Duration::seconds(1));
}

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

#[test]
fn test_setup_multishard() {
    init_test_logger();

    let num_shards = 2;
    let env =
        TestLoopBuilder::new().num_shards(num_shards).chunk_producer_per_shard().build().warmup();

    assert_eq!(env.node_datas.len(), num_shards);

    let node0_tracked_shards = env.node(0).tracked_shards();
    assert_eq!(node0_tracked_shards.len(), 1);
    let node1_tracked_shards = env.node(1).tracked_shards();
    assert_eq!(node1_tracked_shards.len(), 1);
    assert_ne!(&node0_tracked_shards[0], &node1_tracked_shards[0]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(1));
}
