use assert_matches::assert_matches;
use near_async::time::Duration;
use near_client::{Query, QueryError};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::{AccountId, Balance, BlockId, BlockReference};
use near_primitives::views::QueryRequest;
use std::sync::Arc;

use crate::setup::builder::TestLoopBuilder;

const VALIDATOR: &str = "validator0";

/// Verifies that querying a block whose header is known (it was received) but
/// whose `ChunkExtra` hasn't been written yet (block not yet applied) returns
/// `BlockNotProcessed` rather than the misleading `UnavailableShard`.
#[test]
fn test_block_not_processed_query_error() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().build().warmup();

    let head_block = env.rpc_node().head_block();
    let signer = Arc::new(create_test_signer(VALIDATOR));

    // Build a child block but save ONLY its header to the chain store —
    // no ChunkExtra. This simulates a block that's been received (header
    // known) but not yet applied.
    let new_block =
        TestBlockBuilder::from_prev_block(env.test_loop.clock(), &head_block, signer).build();
    let new_block_hash = *new_block.hash();

    {
        let rpc_idx = env.rpc_data_idx();
        let client_handle = env.node_datas[rpc_idx].client_sender.actor_handle();
        let client_actor = env.test_loop.data.get_mut(&client_handle);
        let mut chain_store_update = client_actor.client.chain.mut_chain_store().store_update();
        chain_store_update.save_block_header(new_block.header().clone()).unwrap();
        chain_store_update.commit().unwrap();
    }

    // Query any account on the block that has a header but no ChunkExtra.
    let account_id: AccountId = VALIDATOR.parse().unwrap();
    let result = env.rpc_node().query(Query::new(
        BlockReference::BlockId(BlockId::Hash(new_block_hash)),
        QueryRequest::ViewAccount { account_id },
    ));

    assert_matches!(
        result,
        Err(QueryError::BlockNotProcessed { block_hash, .. }) if block_hash == new_block_hash
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

/// Verifies that querying a block for a shard the node does not track returns
/// `UnavailableShard`.
#[test]
fn test_unavailable_shard_query_error() {
    init_test_logger();

    // 2 shards so we can have a tracked and an untracked shard on the same node.
    // With one chunk producer per shard, each validator only tracks the shard it
    // produces chunks for, so querying a validator about an account on the other
    // shard naturally returns UnavailableShard.
    let boundary_account: AccountId = "boundary".parse().unwrap();
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary_account], 1);

    let mut env = TestLoopBuilder::new()
        .shard_layout(shard_layout)
        .chunk_producer_per_shard()
        .build()
        .warmup();

    // "zzz" > "boundary" → shard at index 1, which validator0 (chunk producer
    // for shard at index 0) does not track.
    let account_on_untracked_shard: AccountId = "zzz".parse().unwrap();
    let result = env
        .validator()
        .runtime_query(QueryRequest::ViewAccount { account_id: account_on_untracked_shard });

    assert_matches!(result, Err(QueryError::UnavailableShard { .. }));

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
