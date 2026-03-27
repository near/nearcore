use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_jsonrpc_primitives::errors::{RpcError, RpcErrorKind};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight};
use near_store::ShardUId;

/// Test harness for sharded RPC tests.
///
/// Sets up a 2-shard environment with 2 RPC nodes (each tracking one shard)
/// and 1 validator. Two user accounts (alice, zoe) are placed on different shards.
pub(crate) struct TwoShardHarness {
    pub(crate) env: TestLoopEnv,
    pub(crate) alice: AccountId,
    pub(crate) zoe: AccountId,
    /// RPC node that tracks alice's shard.
    pub(crate) alice_node: AccountId,
    /// RPC node that tracks zoe's shard.
    pub(crate) zoe_node: AccountId,
    /// Validator node (tracks all shards by default).
    pub(crate) validator: AccountId,
}

impl TwoShardHarness {
    pub(crate) fn new() -> Self {
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
            .add_user_account_simple(zoe.clone(), Balance::from_near(200))
            .build();

        let rpc0: AccountId = create_account_id("rpc0");
        let rpc0_shard = shard_uids[0];
        let rpc1: AccountId = create_account_id("rpc1");
        let rpc1_shard = shard_uids[1];
        let clients = vec![rpc0.clone(), rpc1.clone(), validator0.clone()];
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .config_modifier(move |config, client_index| match client_index {
                0 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc0_shard]),
                1 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc1_shard]),
                _ => {}
            })
            .add_rpc_pool([rpc0.clone(), rpc1.clone()])
            .build();

        let (alice_node, zoe_node) =
            if alice_shard == rpc0_shard.shard_id() { (rpc0, rpc1) } else { (rpc1, rpc0) };

        Self { env, alice, zoe, alice_node, zoe_node, validator: validator0 }
    }

    /// Deploy the standard test contract to alice.
    pub(crate) fn deploy_contract_to_alice(&mut self) {
        let tx = self.env.node_for_account(&self.alice_node).tx_deploy_test_contract(&self.alice);
        self.env.runner_for_account(&self.alice_node).run_tx(tx, Duration::seconds(5));
    }

    /// Ensure the final head lags behind the consensus head, so that tests
    /// asserting on block heights for different finality levels are meaningful.
    /// Advances a few blocks if needed. Returns `(final_height, head_height)`.
    pub(crate) fn ensure_finality_lag(&mut self) -> (BlockHeight, BlockHeight) {
        let final_height =
            self.env.node_for_account(&self.validator).client().chain.final_head().unwrap().height;
        let head_height = self.env.node_for_account(&self.validator).head().height;
        if final_height >= head_height {
            self.env.runner_for_account(&self.validator).run_for_number_of_blocks(3);
        }
        let final_height =
            self.env.node_for_account(&self.validator).client().chain.final_head().unwrap().height;
        let head_height = self.env.node_for_account(&self.validator).head().height;
        assert!(
            final_height < head_height,
            "test precondition failed: final_height ({final_height}) must be < head_height ({head_height})",
        );
        (final_height, head_height)
    }
}

/// Assert that an RpcError is a HandlerError with the expected error name.
pub(crate) fn assert_rpc_error(err: &RpcError, expected_name: &str) {
    match err.error_struct.as_ref().expect("expected error_struct") {
        RpcErrorKind::HandlerError(val) => {
            assert_eq!(val["name"].as_str(), Some(expected_name), "unexpected error: {val}");
        }
        other => panic!("expected HandlerError({expected_name}), got: {other:?}"),
    }
}
