//! Sharded RPC tests that involve dynamic resharding.
//!
//! These tests verify that RPC queries with different finality levels and
//! block references route correctly after a shard layout change.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_jsonrpc_primitives::types::query::{QueryResponseKind, RpcQueryRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::{
    DynamicReshardingConfig, EpochConfigStore, ShardLayoutConfig,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockId, BlockReference, Finality};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::QueryRequest;
use near_store::ShardUId;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Test harness for sharded RPC tests with dynamic resharding.
///
/// Sets up a multi-shard environment with RPC nodes, triggers a shard split via
/// `force_split_shards`, and records block heights before and after resharding.
#[allow(dead_code)]
struct ReshardingRpcHarness {
    env: TestLoopEnv,
    /// An account placed on the shard that will be split.
    test_account: AccountId,
    /// RPC node identifiers (used by #[ignore]d tests).
    rpc0: AccountId,
    rpc1: AccountId,
    /// Validator node (tracks all shards).
    validator: AccountId,
    /// Block height recorded before the resharding epoch.
    pre_resharding_height: BlockHeight,
    /// Block height recorded after resharding completes.
    post_resharding_height: BlockHeight,
}

impl ReshardingRpcHarness {
    fn new() -> Self {
        let base_shard_layout = ShardLayout::multi_shard(2, 3);
        let shard_uids: Vec<ShardUId> = base_shard_layout.shard_uids().collect();
        let epoch_length: u64 = 7;

        let test_account = create_account_id("alice");
        let shard_to_split = base_shard_layout.account_id_to_shard_id(&test_account);

        let validator0: AccountId = create_account_id("validator");
        let validators_spec = ValidatorsSpec::desired_roles(&[validator0.as_str()], &[]);

        let genesis = TestLoopBuilder::new_genesis_builder()
            .protocol_version(PROTOCOL_VERSION - 1)
            .shard_layout(base_shard_layout.clone())
            .validators_spec(validators_spec)
            .epoch_length(epoch_length)
            .add_user_account_simple(test_account.clone(), Balance::from_near(100))
            .build();

        let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();

        // Dynamic resharding config: force a split on the test account's shard.
        let dynamic_config = DynamicReshardingConfig {
            memory_usage_threshold: u64::MAX,
            min_child_memory_usage: u64::MAX,
            max_number_of_shards: 100,
            min_epochs_between_resharding: 0,
            force_split_shards: vec![shard_to_split],
            block_split_shards: vec![],
        };
        let mut dynamic_epoch_config = base_epoch_config.clone();
        dynamic_epoch_config.shard_layout_config =
            ShardLayoutConfig::Dynamic { dynamic_resharding_config: dynamic_config };

        let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
            (PROTOCOL_VERSION - 1, Arc::new(base_epoch_config)),
            (PROTOCOL_VERSION, Arc::new(dynamic_epoch_config)),
        ]));

        let rpc0 = create_account_id("rpc0");
        let rpc1 = create_account_id("rpc1");
        let clients = vec![rpc0.clone(), rpc1.clone(), validator0.clone()];

        let rpc0_shard = shard_uids[0];
        let rpc1_shard = shard_uids[1];
        let mut env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .epoch_config_store(epoch_config_store)
            .config_modifier(move |config, client_index| match client_index {
                0 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc0_shard]),
                1 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc1_shard]),
                _ => {}
            })
            .add_rpc_pool([rpc0.clone(), rpc1.clone()])
            .build();

        // Record height before resharding.
        let pre_resharding_height = env.node_for_account(&validator0).head().height;

        // Run until shard layout changes (resharding completes).
        let epoch_manager = env.node_for_account(&validator0).client().epoch_manager.clone();
        let initial_layout = base_shard_layout;
        // Dynamic resharding has a 2-epoch activation delay; multiply by 8
        // (2 epochs delay + buffer for catchup) with 1 second per block.
        let max_wait = Duration::seconds((epoch_length * 8) as i64);
        env.runner_for_account(&validator0).run_until(
            |node| {
                let epoch_id = node.head().epoch_id;
                match epoch_manager.get_shard_layout(&epoch_id) {
                    Ok(layout) => layout != initial_layout,
                    Err(_) => false,
                }
            },
            max_wait,
        );

        let post_resharding_height = env.node_for_account(&validator0).head().height;

        Self {
            env,
            test_account,
            rpc0,
            rpc1,
            validator: validator0,
            pre_resharding_height,
            post_resharding_height,
        }
    }
}

/// After dynamic resharding completes (2 → 3 shards), RPC queries with
/// different finality levels and block references should succeed from any RPC
/// node in the pool. Verifies the view client can resolve accounts in both old
/// and new shard layouts.
///
/// TODO(sharded-rpc): currently ignored because the RPC pool's tracked_shards
/// is static and becomes stale after resharding — child shards are unknown to
/// the pool, causing UNAVAILABLE_SHARD errors. Remove ignore once the pool
/// supports dynamic shard updates.
#[test]
#[ignore]
fn test_rpc_query_after_resharding() {
    init_test_logger();
    let mut h = ReshardingRpcHarness::new();

    let test_account = h.test_account.clone();
    let validator = h.validator.clone();

    // Verify resharding actually happened.
    let epoch_manager = h.env.node_for_account(&validator).client().epoch_manager.clone();
    let pre_hash = h
        .env
        .node_for_account(&validator)
        .client()
        .chain
        .get_block_hash_by_height(h.pre_resharding_height)
        .unwrap();
    let pre_epoch = epoch_manager.get_epoch_id_from_prev_block(&pre_hash).unwrap();
    let post_epoch = h.env.node_for_account(&validator).head().epoch_id;
    let pre_layout = epoch_manager.get_shard_layout(&pre_epoch).unwrap();
    let post_layout = epoch_manager.get_shard_layout(&post_epoch).unwrap();
    assert_ne!(
        pre_layout, post_layout,
        "resharding did not happen: shard layout is the same before and after",
    );
    // Advance a few more blocks so finality catches up to the post-resharding epoch.
    h.env.runner_for_account(&validator).run_for_number_of_blocks(5);

    // Query from both RPC nodes. Each tracks only one shard, so queries for
    // accounts on the other shard must be forwarded within the pool.
    let rpc_nodes = [h.rpc0.clone(), h.rpc1.clone()];
    let block_refs: Vec<(&str, BlockReference)> = vec![
        ("Finality::Final", BlockReference::Finality(Finality::Final)),
        ("Finality::None", BlockReference::Finality(Finality::None)),
        ("Height(pre)", BlockReference::BlockId(BlockId::Height(h.pre_resharding_height))),
        ("Height(post)", BlockReference::BlockId(BlockId::Height(h.post_resharding_height))),
    ];

    for (ref_name, block_ref) in &block_refs {
        for node_id in &rpc_nodes {
            let result = h
                .env
                .runner_for_account(node_id)
                .run_jsonrpc_query(
                    RpcQueryRequest {
                        block_reference: block_ref.clone(),
                        request: QueryRequest::ViewAccount { account_id: test_account.clone() },
                    },
                    Duration::seconds(5),
                )
                .unwrap_or_else(|e| panic!("{ref_name} from {node_id} failed: {e:?}"));
            match result.kind {
                QueryResponseKind::ViewAccount(view) => {
                    assert_eq!(view.amount, Balance::from_near(100), "{ref_name} from {node_id}");
                }
                other => {
                    panic!("{ref_name} from {node_id}: expected ViewAccount, got: {other:?}")
                }
            }
        }
    }
}
