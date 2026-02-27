use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_client::{Query, QueryError};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::{AccountId, Balance, BlockId, BlockReference};
use near_primitives::views::QueryRequest;
use std::sync::Arc;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_validators_spec, rpc_account_id, validators_spec_clients_with_rpc,
};

const VALIDATOR: &str = "validator0";

/// Verifies that querying a block whose header is known (it was received) but
/// whose `ChunkExtra` hasn't been written yet (block not yet applied) returns
/// `BlockNotProcessed` rather than the misleading `UnavailableShard`.
#[test]
fn test_block_not_processed_query_error() {
    init_test_logger();

    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let rpc_id = rpc_account_id();
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

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

    // Query for the block that has a header but no ChunkExtra.
    let result = {
        let view_client = env.test_loop.data.get_mut(&view_client_handle);
        view_client.handle_query(Query::new(
            BlockReference::BlockId(BlockId::Hash(new_block_hash)),
            QueryRequest::ViewAccount { account_id: rpc_id },
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

    let validator_id: AccountId = VALIDATOR.parse().unwrap();
    let validators_spec = ValidatorsSpec::desired_roles(&[VALIDATOR], &[]);
    // 2 shards so we can have a tracked and an untracked shard on the same node.
    let boundary_account: AccountId = "boundary".parse().unwrap();
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary_account.clone()], 1);

    // accounts[0] is on shard 0 (before the boundary), accounts[1] is on shard 1 (after).
    let account_on_shard0: AccountId = "aaa".parse().unwrap();
    let account_on_shard1: AccountId = "zzz".parse().unwrap();

    // Collect the shard UID for shard 0 so we can tell the RPC node to only track that one.
    let tracked_shard_uid = shard_layout.account_id_to_shard_uid(&account_on_shard0);

    let rpc_id = rpc_account_id();
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec.clone())
        .add_user_accounts_simple(
            &[
                validator_id.clone(),
                account_on_shard0.clone(),
                account_on_shard1.clone(),
                rpc_id.clone(),
            ],
            Balance::from_near(1_000_000),
        )
        .build();

    let all_clients = {
        let mut clients: Vec<AccountId> = match &validators_spec {
            ValidatorsSpec::DesiredRoles { block_and_chunk_producers, .. } => {
                block_and_chunk_producers.clone()
            }
            _ => vec![],
        };
        clients.push(rpc_id.clone());
        clients
    };
    let rpc_idx = all_clients.iter().position(|a| a == &rpc_id).unwrap();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(all_clients)
        .config_modifier(move |config, client_index| {
            if client_index != rpc_idx {
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
