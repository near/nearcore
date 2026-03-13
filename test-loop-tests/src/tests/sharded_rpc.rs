use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_jsonrpc_primitives::types::view_account::RpcViewAccountRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockReference, Finality};
use near_store::ShardUId;

/// 2 shards, 2 accounts, one on each shard
/// view_account jsonrpc queries should be forwarded to the right shard.
#[test]
fn test_rpc_view_account_forwarding() {
    init_test_logger();

    let shard_layout = ShardLayout::multi_shard(2, 1);
    let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();

    // "alice" < "test1" (boundary) and "zoe" >= "test1", so they land on different shards.
    let alice = create_account_id("alice");
    let zoe = create_account_id("zoe");
    let alice_shard = shard_layout.account_id_to_shard_id(&alice);
    let zoe_shard = shard_layout.account_id_to_shard_id(&zoe);
    assert_ne!(alice_shard, zoe_shard);

    let validator0: AccountId = create_account_id("validator");
    let validators_spec = ValidatorsSpec::desired_roles(&[validator0.as_str()], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_account_simple(alice.clone(), Balance::from_near(100))
        .add_user_account_simple(zoe.clone(), Balance::from_near(100))
        .build();

    let rpc0: AccountId = create_account_id("rpc0");
    let rpc0_shard = shard_uids[0];
    let rpc1: AccountId = create_account_id("rpc1");
    let rpc1_shard = shard_uids[1];
    let clients = vec![rpc0.clone(), rpc1.clone(), validator0];
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .config_modifier(move |config, client_index| {
            match client_index {
                0 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc0_shard]),
                1 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc1_shard]),
                _ => {} // validator keeps default
            }
        })
        .build();

    // Determine which node tracks which account.
    let (alice_node, zoe_node) =
        if alice_shard == rpc0_shard.shard_id() { (rpc0, rpc1) } else { (rpc1, rpc0) };

    // Query alice's account from the node that does NOT track her shard.
    // This forces the request to be forwarded to the other node.
    let result = env
        .runner_for_account(&zoe_node)
        .run_jsonrpc_query(
            |client| {
                client.EXPERIMENTAL_view_account(RpcViewAccountRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: alice.clone(),
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    assert_eq!(result.account.amount, Balance::from_near(100));

    // Also query from the node that DOES track alice's shard (should work locally).
    let result = env
        .runner_for_account(&alice_node)
        .run_jsonrpc_query(
            |client| {
                client.EXPERIMENTAL_view_account(RpcViewAccountRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: alice.clone(),
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    assert_eq!(result.account.amount, Balance::from_near(100));

    // Query zoe's account from the node that doesn't track her shard.
    let result = env
        .runner_for_account(&alice_node)
        .run_jsonrpc_query(
            |client| {
                client.EXPERIMENTAL_view_account(RpcViewAccountRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: zoe.clone(),
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    assert_eq!(result.account.amount, Balance::from_near(100));

    // Query zoe's account from the node that does track her shard.
    let result = env
        .runner_for_account(&zoe_node)
        .run_jsonrpc_query(
            |client| {
                client.EXPERIMENTAL_view_account(RpcViewAccountRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: zoe.clone(),
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    assert_eq!(result.account.amount, Balance::from_near(100));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
