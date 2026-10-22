//! Test that the compiled-contract cache is warmed during the epoch
//! preceding a protocol upgrade that changes the cache-key signature.

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::types::Balance;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, PROTOCOL_VERSION};
use std::sync::Arc;

/// Deploy and call a contract during a pre-upgrade epoch, then assert the
/// deployed contract's compiled artifact is already present in the on-disk
/// cache under the *new* protocol's `wasm_config`. The only mechanism that
/// could have populated that entry by this point is warming during the
/// pre-upgrade epoch.
#[test]
fn slow_test_cache_warming_across_vm_config_change() {
    init_test_logger();

    let runtime_config_store = RuntimeConfigStore::new();
    // Exercise the latest real VM transition supported by this build, rather
    // than injecting a config change at a version with no consensus generation bump.
    let new_protocol = runtime_config_store.vm_generation(PROTOCOL_VERSION).0;
    assert!(new_protocol > MIN_SUPPORTED_PROTOCOL_VERSION);
    let old_protocol = new_protocol - 1;
    let epoch_length = 10;
    let user = create_account_id("user");

    // The real config transition must change the cache key to trigger warming.
    assert_ne!(
        runtime_config_store.get_config(old_protocol).wasm_config.non_crypto_hash(),
        runtime_config_store.get_config(new_protocol).wasm_config.non_crypto_hash(),
    );
    let protocol_upgrade_schedule = ProtocolUpgradeVotingSchedule::new_immediate(new_protocol);

    let mut env = TestLoopBuilder::new()
        .protocol_version(old_protocol)
        .epoch_length(epoch_length)
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(10_000))
        .protocol_upgrade_schedule(protocol_upgrade_schedule)
        .runtime_config_store(runtime_config_store)
        .build();

    // Deploy during the old-protocol epoch, exercising the deploy warming hook.
    // Uses the backwards-compatible contract since this runs below the latest
    // protocol version, where `rs_contract`'s newest host-fn imports are absent.
    let deploy_tx = env.rpc_node().tx_deploy_contract(
        &user,
        near_test_contracts::backwards_compatible_rs_contract().to_vec(),
    );
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(10));

    // Calls exercise the pipelining warming path on receipt preparation.
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
    env.node_runner(0).run_until(
        |node| node.protocol_version_at_head() == new_protocol,
        Duration::seconds((6 * epoch_length) as i64),
    );

    // The contract must already be cached under the new protocol's wasm_config.
    // No contract activity has run under new_protocol yet, so this can only
    // have been populated by pre-upgrade warming.
    let code_hash = CryptoHash::hash_bytes(near_test_contracts::backwards_compatible_rs_contract());
    let client = env.node(0).client();
    let next_runtime_config = client.runtime_adapter.get_runtime_config(new_protocol);
    let cache = client.runtime_adapter.compiled_contract_cache();
    let warmed = near_vm_runner::contract_cached(
        Arc::clone(&next_runtime_config.wasm_config),
        cache,
        code_hash,
    )
    .expect("compiled-contract cache lookup failed");
    assert!(warmed, "contract not cached under new-protocol wasm_config; warming did not work");

    // Post-upgrade sanity: the call succeeds under the new protocol.
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
