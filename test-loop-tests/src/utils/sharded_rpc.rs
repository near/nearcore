use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::rpc::RpcFaultHandle;
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

/// Test harness with a backup RPC node tracking alice's shard.
///
/// Topology (4 clients):
/// - `rpc0` — tracks alice's shard; first pick for alice's shard from rpc1's pool.
/// - `rpc1` — tracks zoe's shard; used as coordinator so shard-0 queries must
///   traverse the pool (rpc0 → rpc2) and exercise the retry loop.
/// - `rpc2` — tracks alice's shard; backup that answers after rpc0 is excluded.
/// - `validator0` — tracks all shards; baseline source of truth.
///
/// Both `rpc0` and `rpc2` are wired with fault handles so tests can simulate
/// a stale node (`RpcFault::Fail`) or a dead node (`RpcFault::Hang`) on either
/// of alice's shard candidates. Flipping both exercises the exhaustion path
/// where no candidate can serve the shard.
pub(crate) struct ThreeNodeHarness {
    pub(crate) env: TestLoopEnv,
    pub(crate) alice: AccountId,
    pub(crate) zoe: AccountId,
    #[allow(dead_code)]
    pub(crate) rpc0: AccountId,
    pub(crate) rpc1: AccountId,
    #[allow(dead_code)]
    pub(crate) rpc2: AccountId,
    pub(crate) validator: AccountId,
    /// Fault handle for rpc0's pool entry.
    pub(crate) rpc0_fault: RpcFaultHandle,
    /// Fault handle for rpc2's pool entry.
    pub(crate) rpc2_fault: RpcFaultHandle,
}

impl ThreeNodeHarness {
    pub(crate) fn new() -> Self {
        let shard_layout = ShardLayout::multi_shard(2, 1);
        let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();

        let alice = create_account_id("alice");
        let zoe = create_account_id("zoe");
        let alice_shard = shard_layout.account_id_to_shard_id(&alice);
        let zoe_shard = shard_layout.account_id_to_shard_id(&zoe);
        assert_ne!(alice_shard, zoe_shard);

        // Figure out which ShardUId is alice's and which is zoe's so that
        // rpc0 + rpc2 can be pinned to alice's shard and rpc1 to zoe's.
        let alice_shard_uid = shard_uids
            .iter()
            .copied()
            .find(|uid| uid.shard_id() == alice_shard)
            .expect("alice's shard uid");
        let zoe_shard_uid = shard_uids
            .iter()
            .copied()
            .find(|uid| uid.shard_id() == zoe_shard)
            .expect("zoe's shard uid");

        let validator0: AccountId = create_account_id("validator");
        let validators_spec = ValidatorsSpec::desired_roles(&[validator0.as_str()], &[]);
        let genesis = TestLoopBuilder::new_genesis_builder()
            .shard_layout(shard_layout)
            .validators_spec(validators_spec)
            .add_user_account_simple(alice.clone(), Balance::from_near(100))
            .add_user_account_simple(zoe.clone(), Balance::from_near(200))
            .build();

        let rpc0: AccountId = create_account_id("rpc0");
        let rpc1: AccountId = create_account_id("rpc1");
        let rpc2: AccountId = create_account_id("rpc2");
        let clients = vec![rpc0.clone(), rpc1.clone(), rpc2.clone(), validator0.clone()];
        let mut builder = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .config_modifier(move |config, client_index| match client_index {
                0 => {
                    config.tracked_shards_config =
                        TrackedShardsConfig::Shards(vec![alice_shard_uid])
                }
                1 => {
                    config.tracked_shards_config = TrackedShardsConfig::Shards(vec![zoe_shard_uid])
                }
                2 => {
                    config.tracked_shards_config =
                        TrackedShardsConfig::Shards(vec![alice_shard_uid])
                }
                _ => {}
            })
            .add_rpc_pool([rpc0.clone(), rpc1.clone(), rpc2.clone()]);
        let rpc0_fault = builder.rpc_pool_fault_handle(rpc0.clone());
        let rpc2_fault = builder.rpc_pool_fault_handle(rpc2.clone());
        let env = builder.build();

        Self { env, alice, zoe, rpc0, rpc1, rpc2, validator: validator0, rpc0_fault, rpc2_fault }
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
