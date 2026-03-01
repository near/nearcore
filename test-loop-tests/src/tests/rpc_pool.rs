use std::collections::HashMap;

use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_jsonrpc_primitives::types::view_account::RpcViewAccountRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockReference, Finality};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::rpc::inject_rpc_pool;

fn view_account_request(account_id: AccountId) -> RpcViewAccountRequest {
    RpcViewAccountRequest { account_id, block_reference: BlockReference::Finality(Finality::Final) }
}

/// Test that the RPC pool correctly routes queries to the right shard node.
///
/// Setup: 2-shard network with 2 validators and 2 RPC nodes.
/// rpc0 tracks shard0, rpc1 tracks shard1.
/// After warmup, wire pools so each RPC node can forward to the other.
/// Then query accounts on both shards through both RPC nodes.
#[test]
fn test_rpc_pool_routes_query_to_correct_shard() {
    init_test_logger();

    let boundary: AccountId = "account1".parse().unwrap();
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary.clone()], 1);
    let shard_uids: Vec<_> = shard_layout.shard_uids().collect();
    assert_eq!(shard_uids.len(), 2, "expected 2 shards");

    // Two accounts, one per shard.
    let account_shard0: AccountId = "aaa".parse().unwrap(); // < "account1" => first shard
    let account_shard1: AccountId = "zzz".parse().unwrap(); // >= "account1" => second shard

    let shard_uid_for_aaa = shard_layout.account_id_to_shard_uid(&account_shard0);
    let shard_uid_for_zzz = shard_layout.account_id_to_shard_uid(&account_shard1);
    assert_ne!(shard_uid_for_aaa, shard_uid_for_zzz, "accounts must be on different shards");

    // 2 validators + 2 RPC nodes
    let validators = vec!["validator0", "validator1"];
    let validators_spec = ValidatorsSpec::desired_roles(&validators, &[]);
    let rpc0: AccountId = "rpc0".parse().unwrap();
    let rpc1: AccountId = "rpc1".parse().unwrap();
    let clients: Vec<AccountId> =
        validators.iter().map(|v| v.parse().unwrap()).chain([rpc0.clone(), rpc1.clone()]).collect();

    let rpc0_idx = 2;
    let rpc1_idx = 3;
    let rpc0_shard = shard_uid_for_aaa;
    let rpc1_shard = shard_uid_for_zzz;

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(
            &[account_shard0.clone(), account_shard1.clone()],
            Balance::from_near(1_000),
        )
        .build();

    let rpc0_shard_clone = rpc0_shard;
    let rpc1_shard_clone = rpc1_shard;
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .config_modifier(move |config, idx| {
            if idx == rpc0_idx {
                config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc0_shard_clone]);
            } else if idx == rpc1_idx {
                config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc1_shard_clone]);
            }
        })
        .build()
        .warmup();

    // Wire pools: rpc0 can forward to rpc1 for rpc1's shard, and vice versa.
    let shard_layout_clone = shard_layout.clone();
    {
        let rpc1_transport = env.node_datas[rpc1_idx].jsonrpc_transport.clone();
        let mut peer_shards = HashMap::new();
        peer_shards.insert(rpc1_shard, rpc1_transport);
        let sl = shard_layout_clone.clone();
        inject_rpc_pool(
            &env.node_datas[rpc0_idx],
            peer_shards,
            Box::new(move || sl.clone()),
            TrackedShardsConfig::Shards(vec![rpc0_shard]),
        );
    }
    {
        let rpc0_transport = env.node_datas[rpc0_idx].jsonrpc_transport.clone();
        let mut peer_shards = HashMap::new();
        peer_shards.insert(rpc0_shard, rpc0_transport);
        let sl = shard_layout_clone.clone();
        inject_rpc_pool(
            &env.node_datas[rpc1_idx],
            peer_shards,
            Box::new(move || sl.clone()),
            TrackedShardsConfig::Shards(vec![rpc1_shard]),
        );
    }

    let timeout = Duration::seconds(5);

    // Query account on shard0 through rpc0 — local.
    let result = env.node_runner(rpc0_idx).run_jsonrpc_query(
        |client| client.EXPERIMENTAL_view_account(view_account_request(account_shard0.clone())),
        timeout,
    );
    assert!(result.is_ok(), "rpc0 local query for aaa failed: {:?}", result.err());

    // Query account on shard1 through rpc0 — forwarded to rpc1.
    let result = env.node_runner(rpc0_idx).run_jsonrpc_query(
        |client| client.EXPERIMENTAL_view_account(view_account_request(account_shard1.clone())),
        timeout,
    );
    assert!(result.is_ok(), "rpc0 forwarded query for zzz failed: {:?}", result.err());

    // Query account on shard1 through rpc1 — local.
    let result = env.node_runner(rpc1_idx).run_jsonrpc_query(
        |client| client.EXPERIMENTAL_view_account(view_account_request(account_shard1.clone())),
        timeout,
    );
    assert!(result.is_ok(), "rpc1 local query for zzz failed: {:?}", result.err());

    // Query account on shard0 through rpc1 — forwarded to rpc0.
    let result = env.node_runner(rpc1_idx).run_jsonrpc_query(
        |client| client.EXPERIMENTAL_view_account(view_account_request(account_shard0.clone())),
        timeout,
    );
    assert!(result.is_ok(), "rpc1 forwarded query for aaa failed: {:?}", result.err());

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
