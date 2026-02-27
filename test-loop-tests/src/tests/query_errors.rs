use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_client::{Query, QueryError};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::{AccountId, Balance, BlockId, BlockReference};
use near_primitives::views::QueryRequest;
use std::sync::Arc;

use crate::setup::builder::TestLoopBuilder;

const VALIDATOR: &str = "validator0";
/// Index of the RPC node when using 1 validator + enable_rpc().
const RPC_IDX: usize = 1;

/// Verifies that querying a block whose header is known (it was received) but
/// whose `ChunkExtra` hasn't been written yet (block not yet applied) returns
/// `BlockNotProcessed` rather than the misleading `UnavailableShard`.
#[test]
fn test_block_not_processed_query_error() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().build().warmup();

    // Get the head block from the RPC node's client.
    let rpc_idx = env.rpc_data_idx();
    let client_handle = env.node_datas[rpc_idx].client_sender.actor_handle();
    let view_client_handle = env.node_datas[rpc_idx].view_client_sender.actor_handle();

    let (head_block, signer) = {
        let client_actor = env.test_loop.data.get(&client_handle);
        let head = client_actor.client.chain.head().unwrap();
        let head_block = client_actor.client.chain.get_block(&head.last_block_hash).unwrap();
        let signer = Arc::new(create_test_signer(VALIDATOR));
        (head_block, signer)
    };

    // Build a child block but save ONLY its header to the chain store —
    // no ChunkExtra. This simulates a block that's been received (header
    // known) but not yet applied.
    let new_block =
        TestBlockBuilder::from_prev_block(env.test_loop.clock(), &head_block, signer).build();
    let new_block_hash = *new_block.hash();

    {
        let client_actor = env.test_loop.data.get_mut(&client_handle);
        let mut chain_store_update = client_actor.client.chain.mut_chain_store().store_update();
        chain_store_update.save_block_header(new_block.header().clone()).unwrap();
        chain_store_update.commit().unwrap();
    }

    // Query any account on the block that has a header but no ChunkExtra.
    let account_id: AccountId = VALIDATOR.parse().unwrap();
    let result = {
        let view_client = env.test_loop.data.get_mut(&view_client_handle);
        view_client.handle_query(Query::new(
            BlockReference::BlockId(BlockId::Hash(new_block_hash)),
            QueryRequest::ViewAccount { account_id },
        ))
    };

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
    let boundary_account: AccountId = "boundary".parse().unwrap();
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary_account], 1);

    // "aaa" is on shard 0 (before the boundary), "zzz" is on shard 1 (after).
    let account_on_shard0: AccountId = "aaa".parse().unwrap();
    let account_on_shard1: AccountId = "zzz".parse().unwrap();

    // The RPC node tracks only shard 0 (the one containing "aaa").
    let tracked_shard_uid = shard_layout.account_id_to_shard_uid(&account_on_shard0);

    let mut env = TestLoopBuilder::new()
        .shard_layout(shard_layout)
        .add_user_accounts(
            &[account_on_shard0.clone(), account_on_shard1.clone()],
            Balance::from_near(1_000_000),
        )
        .enable_rpc()
        .config_modifier(move |config, client_index| {
            if client_index != RPC_IDX {
                return;
            }
            // RPC node tracks only shard 0.
            config.tracked_shards_config = TrackedShardsConfig::Shards(vec![tracked_shard_uid]);
        })
        .build()
        .warmup();

    // Get the head block hash from the RPC node.
    let rpc_data_idx = env.rpc_data_idx();
    let view_client_handle = env.node_datas[rpc_data_idx].view_client_sender.actor_handle();
    let client_handle = env.node_datas[rpc_data_idx].client_sender.actor_handle();
    let head_hash =
        env.test_loop.data.get(&client_handle).client.chain.head().unwrap().last_block_hash;

    // Querying an account on the untracked shard should return UnavailableShard.
    let result = {
        let view_client = env.test_loop.data.get_mut(&view_client_handle);
        view_client.handle_query(Query::new(
            BlockReference::BlockId(BlockId::Hash(head_hash)),
            QueryRequest::ViewAccount { account_id: account_on_shard1 },
        ))
    };

    assert_matches!(result, Err(QueryError::UnavailableShard { .. }));

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
