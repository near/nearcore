//! Test that the compiled-contract cache is warmed during the epoch
//! preceding a protocol upgrade that changes the cache-key signature
//! (here: a `vm_kind` switch).

use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

/// Crossing protocol 83 → 84 changes `vm_kind` (NearVm → Wasmtime), so the
/// compiled-contract cache key changes across the epoch boundary. Deploy and
/// call a contract during the pre-upgrade epoch, then assert the deployed
/// contract's compiled artifact is already present in the on-disk cache
/// under the *new* protocol's `wasm_config` — the only mechanism that could
/// have populated that entry by this point is warming during the pre-upgrade
/// epoch.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_cache_warming_across_vm_kind_upgrade() {
    init_test_logger();

    let old_protocol = 83;
    let new_protocol = 84;
    let epoch_length = 10;
    let initial_balance = Balance::from_near(10_000);

    // Manual setup: one validator + one user account hosting the contract +
    // an "rpc" client (the framework recognizes the "rpc" account id and
    // exposes it via `env.rpc_node()`).
    let validator: AccountId = "validator0".parse().unwrap();
    let user: AccountId = "user".parse().unwrap();
    let rpc: AccountId = "rpc".parse().unwrap();
    let clients = vec![validator, user.clone(), rpc];
    let validators_spec = ValidatorsSpec::desired_roles(&["validator0"], &[]);
    let user_accounts = vec![user.clone()];
    let shard_layout = ShardLayout::single_shard();

    let builder = TestLoopBuilder::new();
    let genesis = TestGenesisBuilder::new()
        .protocol_version(old_protocol)
        .genesis_time_from_clock(&builder.clock())
        .genesis_height(10_000)
        .shard_layout(shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_spec(validators_spec.clone())
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();
    let genesis_epoch_info = TestEpochConfigBuilder::new()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .build();

    let mainnet = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let adjust = |proto| {
        let mut cfg: EpochConfig = mainnet.get_config(proto).deref().clone();
        cfg.epoch_length = epoch_length;
        cfg.num_block_producer_seats = genesis_epoch_info.num_block_producer_seats;
        cfg.num_chunk_producer_seats = genesis_epoch_info.num_chunk_producer_seats;
        cfg.num_chunk_validator_seats = genesis_epoch_info.num_chunk_validator_seats;
        cfg.with_shard_layout(shard_layout.clone())
    };
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter([
        (old_protocol, Arc::new(adjust(old_protocol))),
        (new_protocol, Arc::new(adjust(new_protocol))),
    ]));
    let protocol_upgrade_schedule = ProtocolUpgradeVotingSchedule::new_immediate(new_protocol);

    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(protocol_upgrade_schedule)
        .clients(clients)
        .build();

    // Deploy during the old-protocol epoch — exercises the deploy hook,
    // which spawns a warming compile when `apply_state.next_wasm_config` is
    // `Some` (i.e. the pre-upgrade window is open).
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(10));

    // A few calls in a row exercise the pipelining hook on the receipt
    // preparation path.
    for _ in 0..3 {
        let call_tx = env.rpc_node().tx_call(
            &user,
            &user,
            "log_something",
            vec![],
            Balance::ZERO,
            Gas::from_teragas(300),
        );
        env.rpc_runner().run_tx(call_tx, Duration::seconds(10));
    }

    // Wait until the chain crosses into the new protocol version.
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let client_handle_for_closure = client_handle.clone();
    env.test_loop.run_until(
        move |data| {
            let client = &data.get(&client_handle_for_closure).client;
            let head = client.chain.head().unwrap();
            client.epoch_manager.get_epoch_info(&head.epoch_id).unwrap().protocol_version()
                == new_protocol
        },
        Duration::seconds((6 * epoch_length) as i64),
    );

    // Deterministic check: the deployed contract's compiled artifact must
    // be in the on-disk cache under the new-protocol `wasm_config`. At this
    // exact point the chain has just crossed into `new_protocol`, the
    // post-upgrade sanity call below has not yet executed, and no other
    // contract activity in this test referenced `rs_contract` under the
    // new VM — so the only mechanism that could have populated the entry
    // is warming during the pre-upgrade epoch. (No reliance on
    // process-global Prometheus counters, which can be contaminated by
    // unrelated tests in the same nextest binary.)
    let code_hash = CryptoHash::hash_bytes(near_test_contracts::rs_contract());
    let client = &env.test_loop.data.get(&client_handle).client;
    let next_runtime_config = client.runtime_adapter.get_runtime_config(new_protocol);
    let cache = client.runtime_adapter.compiled_contract_cache();
    let warmed = near_vm_runner::contract_cached(
        Arc::clone(&next_runtime_config.wasm_config),
        cache,
        code_hash,
    )
    .expect("compiled-contract cache lookup failed");
    assert!(
        warmed,
        "expected deployed contract (code_hash={code_hash}) to be cached \
         under new-protocol wasm_config; warming did not produce the entry"
    );

    // Post-upgrade sanity: the same call succeeds under the new protocol —
    // either by hitting the warmed cache or by an on-demand compile.
    let post_call_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "log_something",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(post_call_tx, Duration::seconds(10)).unwrap();
    assert_eq!(outcome.receipts_outcome[0].outcome.logs, vec!["hello"]);
}
