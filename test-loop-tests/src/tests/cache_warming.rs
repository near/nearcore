//! Test that the compiled-contract cache is warmed during the epoch
//! preceding a protocol upgrade that changes the cache-key signature.

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::types::Balance;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Deploy and call a contract during a pre-upgrade epoch, then assert the
/// deployed contract's compiled artifact is already present in the on-disk
/// cache under the *new* protocol's `wasm_config`. The only mechanism that
/// could have populated that entry by this point is warming during the
/// pre-upgrade epoch.
#[test]
fn slow_test_cache_warming_across_vm_config_change() {
    init_test_logger();

    let old_protocol = PROTOCOL_VERSION - 1;
    let new_protocol = PROTOCOL_VERSION;
    let epoch_length = 10;
    let user = create_account_id("user");

    // Inject a RuntimeConfigStore where the new protocol has a different
    // wasm_config so that cache_keys_differ() returns true and triggers warming.
    let base_store = RuntimeConfigStore::new(None);
    let old_runtime_config = base_store.get_config(old_protocol).clone();
    let mut new_wasm = old_runtime_config.wasm_config.as_ref().clone();
    new_wasm.discard_custom_sections = !new_wasm.discard_custom_sections;
    let new_runtime_config = Arc::new(RuntimeConfig {
        wasm_config: Arc::new(new_wasm),
        ..old_runtime_config.as_ref().clone()
    });
    assert_ne!(
        old_runtime_config.wasm_config.non_crypto_hash(),
        new_runtime_config.wasm_config.non_crypto_hash(),
    );
    let runtime_config_store = RuntimeConfigStore::new_custom(BTreeMap::from([
        (old_protocol, old_runtime_config),
        (new_protocol, new_runtime_config),
    ]));
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

    // The contract must already be cached under the new protocol's wasm_config.
    // No contract activity has run under new_protocol yet, so this can only
    // have been populated by pre-upgrade warming.
    let code_hash = CryptoHash::hash_bytes(near_test_contracts::backwards_compatible_rs_contract());
    let client = &env.test_loop.data.get(&client_handle).client;
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
