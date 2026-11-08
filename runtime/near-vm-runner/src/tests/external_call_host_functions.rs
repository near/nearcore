/// How a host function behaves in an external call, decided for each one.
#[derive(Debug, Clone, Copy, PartialEq)]
enum InExternalCall {
    /// Fails with `ProhibitedInExternalCall`.
    Fails,
    /// Works, but sees a value that differs from an ordinary call's.
    Differs,
    /// Works as in an ordinary call.
    Same,
}

/// Every host function's behaviour in an external call. A host function added without a line
/// here fails `test_every_host_function_has_a_decision_for_external_calls`.
const EXTERNAL_CALL_DECISIONS: &[(&str, InExternalCall)] = {
    use InExternalCall::*;
    &[
        ("signer_account_pk", Fails),
        ("predecessor_account_id", Fails),
        ("prepaid_gas", Fails),
        ("random_seed", Fails),
        ("promise_yield_create", Fails),
        ("promise_yield_create_with_id", Fails),
        // The contract.
        ("signer_account_id", Differs),
        ("refund_to_account_id", Differs),
        // 0, and a nonzero deposit is refused before the run.
        ("attached_deposit", Differs),
        // Includes the gas the run owes.
        ("account_balance", Differs),
        // The weight is ignored.
        ("promise_batch_action_function_call_weight", Differs),
        // The chunk's parent block and its epoch.
        ("block_index", Differs),
        ("block_timestamp", Differs),
        ("epoch_height", Differs),
        ("validator_stake", Differs),
        ("validator_total_stake", Differs),
        ("read_register", Same),
        ("register_len", Same),
        ("write_register", Same),
        ("current_account_id", Same),
        ("chain_id", Same),
        ("input", Same),
        ("storage_usage", Same),
        ("account_locked_balance", Same),
        ("used_gas", Same),
        ("sha256", Same),
        ("keccak256", Same),
        ("keccak512", Same),
        ("sha3_256", Same),
        ("sha3_384", Same),
        ("sha3_512", Same),
        ("ed25519_verify", Same),
        ("p256_verify", Same),
        ("ml_dsa_verify", Same),
        ("ripemd160", Same),
        ("ecrecover", Same),
        ("value_return", Same),
        ("panic", Same),
        ("panic_utf8", Same),
        ("log_utf8", Same),
        ("log_utf16", Same),
        ("abort", Same),
        ("promise_create", Same),
        ("promise_then", Same),
        ("promise_and", Same),
        ("promise_batch_create", Same),
        ("promise_batch_then", Same),
        ("promise_set_refund_to", Same),
        ("promise_batch_action_state_init", Same),
        ("promise_batch_action_state_init_by_account_id", Same),
        ("set_state_init_data_entry", Same),
        ("current_contract_code", Same),
        ("universal_state_init_to_account_id", Same),
        ("promise_batch_action_universal_state_init", Same),
        ("promise_batch_action_create_account", Same),
        ("promise_batch_action_deploy_contract", Same),
        ("promise_batch_action_deploy_global_contract", Same),
        ("promise_batch_action_deploy_global_contract_by_account_id", Same),
        ("promise_batch_action_use_global_contract", Same),
        ("promise_batch_action_use_global_contract_by_account_id", Same),
        ("promise_batch_action_function_call", Same),
        ("promise_batch_action_transfer", Same),
        ("promise_batch_action_stake", Same),
        ("promise_batch_action_add_key_with_full_access", Same),
        ("promise_batch_action_add_key_with_function_call", Same),
        ("promise_batch_action_delete_key", Same),
        ("promise_batch_action_delete_account", Same),
        ("promise_batch_action_transfer_to_gas_key", Same),
        ("promise_batch_action_add_gas_key_with_full_access", Same),
        ("promise_batch_action_add_gas_key_with_function_call", Same),
        ("promise_yield_resume", Same),
        ("promise_yield_resume_with_yield_id", Same),
        ("promise_results_count", Same),
        ("promise_result", Same),
        ("promise_return", Same),
        ("storage_write", Same),
        ("storage_read", Same),
        ("storage_remove", Same),
        ("storage_has_key", Same),
        ("storage_iter_prefix", Same),
        ("storage_iter_range", Same),
        ("storage_iter_next", Same),
        ("gas", Same),
        ("alt_bn128_g1_multiexp", Same),
        ("alt_bn128_g1_sum", Same),
        ("alt_bn128_pairing_check", Same),
        ("bls12381_p1_sum", Same),
        ("bls12381_p2_sum", Same),
        ("bls12381_g1_multiexp", Same),
        ("bls12381_g2_multiexp", Same),
        ("bls12381_map_fp_to_g1", Same),
        ("bls12381_map_fp2_to_g2", Same),
        ("bls12381_pairing_check", Same),
        ("bls12381_p1_decompress", Same),
        ("bls12381_p2_decompress", Same),
    ]
};

/// NEP-667 promises this guard: a new host function works in an external call by default, so each
/// one needs its behaviour there decided before it ships.
#[test]
fn test_every_host_function_has_a_decision_for_external_calls() {
    let mut names: Vec<&'static str> = Vec::new();
    macro_rules! collect {
        (@in $mod:ident : $($rest:tt)*) => {};
        (@as $name:ident : $func:ident < $($rest:tt)*) => {
            names.push(stringify!($name));
        };
        ($func:ident < $($rest:tt)*) => {
            names.push(stringify!($func));
        };
    }
    crate::imports::for_each_import_item!(collect);
    let decided: std::collections::HashSet<&str> =
        EXTERNAL_CALL_DECISIONS.iter().map(|(name, _)| *name).collect();
    // Test-only and sandbox host functions never run on a real chain.
    let undecided: Vec<_> = names
        .iter()
        .filter(|name| !decided.contains(*name))
        .filter(|name| !["sandbox_debug_log", "sleep_nanos", "burn_gas"].contains(name))
        .collect();
    assert!(
        undecided.is_empty(),
        "decide how these host functions behave in an external call: {undecided:?}"
    );
}
