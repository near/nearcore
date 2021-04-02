#[cfg(test)]
mod test {
    use near_chain_configs::Genesis;
    use near_primitives::state_record::StateRecord;
    use near_primitives::version::PROTOCOL_VERSION;
    use neard::config::{GenesisExt, TESTING_INIT_BALANCE};
    use testlib::node::RuntimeNode;
    use testlib::runtime_utils::{add_test_contract, alice_account, bob_account};
    #[cfg(feature = "protocol_feature_evm")]
    use testlib::standard_evm_cases::*;
    use testlib::standard_test_cases::*;

    fn create_runtime_node() -> RuntimeNode {
        RuntimeNode::new(&alice_account())
    }

    fn create_free_runtime_node() -> RuntimeNode {
        RuntimeNode::free(&alice_account())
    }

    fn create_runtime_with_expensive_storage() -> RuntimeNode {
        let mut genesis = Genesis::test(vec![&alice_account(), &bob_account(), "carol.near"], 1);
        add_test_contract(&mut genesis, &bob_account());
        // Set expensive state requirements and add alice more money.
        genesis.config.runtime_config.storage_amount_per_byte = TESTING_INIT_BALANCE / 1000;
        match &mut genesis.records.as_mut()[0] {
            StateRecord::Account { account, .. } => {
                account.set_amount(TESTING_INIT_BALANCE * 10000)
            }
            _ => {
                panic!("the first record is expected to be alice account creation!");
            }
        }
        genesis.records.as_mut().push(StateRecord::Data {
            account_id: bob_account(),
            data_key: b"test".to_vec(),
            value: b"123".to_vec(),
        });
        RuntimeNode::new_from_genesis(&alice_account(), genesis)
    }

    #[test]
    fn test_smart_contract_simple_runtime() {
        let node = create_runtime_node();
        test_smart_contract_simple(node);
    }

    #[test]
    fn test_smart_contract_panic_runtime() {
        let node = create_runtime_node();
        test_smart_contract_panic(node);
    }

    #[test]
    fn test_smart_contract_self_call_runtime() {
        let node = create_runtime_node();
        test_smart_contract_self_call(node);
    }

    #[test]
    fn test_smart_contract_bad_method_name_runtime() {
        let node = create_runtime_node();
        test_smart_contract_bad_method_name(node);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_no_tokens_runtime() {
        let node = create_runtime_node();
        test_smart_contract_empty_method_name_with_no_tokens(node);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_tokens_runtime() {
        let node = create_runtime_node();
        test_smart_contract_empty_method_name_with_tokens(node);
    }

    #[test]
    fn test_smart_contract_with_args_runtime() {
        let node = create_runtime_node();
        test_smart_contract_with_args(node);
    }

    #[test]
    fn test_async_call_with_logs_runtime() {
        let node = create_runtime_node();
        test_async_call_with_logs(node);
    }

    #[test]
    fn test_nonce_update_when_deploying_contract_runtime() {
        let node = create_runtime_node();
        test_nonce_update_when_deploying_contract(node);
    }

    #[test]
    fn test_nonce_updated_when_tx_failed_runtime() {
        let node = create_runtime_node();
        test_nonce_updated_when_tx_failed(node);
    }

    #[test]
    fn test_upload_contract_runtime() {
        let node = create_runtime_node();
        test_upload_contract(node);
    }

    #[test]
    fn test_redeploy_contract_runtime() {
        let node = create_runtime_node();
        test_redeploy_contract(node);
    }

    #[test]
    fn test_send_money_runtime() {
        let node = create_runtime_node();
        test_send_money(node);
    }

    #[test]
    fn test_transfer_tokens_implicit_account_runtime() {
        let node = create_runtime_node();
        transfer_tokens_implicit_account(node);
    }

    #[test]
    fn test_trying_to_create_implicit_account_runtime() {
        let node = create_runtime_node();
        trying_to_create_implicit_account(node);
    }

    #[test]
    fn test_smart_contract_reward_runtime() {
        let node = create_runtime_node();
        test_smart_contract_reward(node);
    }

    #[test]
    fn test_send_money_over_balance_runtime() {
        let node = create_runtime_node();
        test_send_money_over_balance(node);
    }

    #[test]
    fn test_refund_on_send_money_to_non_existent_account_runtime() {
        let node = create_runtime_node();
        test_refund_on_send_money_to_non_existent_account(node);
    }

    #[test]
    fn test_create_account_runtime() {
        let node = create_runtime_node();
        test_create_account(node);
    }

    #[test]
    fn test_create_account_again_runtime() {
        let node = create_runtime_node();
        test_create_account_again(node);
    }

    #[test]
    fn test_create_account_failure_invalid_name_runtime() {
        let node = create_runtime_node();
        test_create_account_failure_invalid_name(node);
    }

    #[test]
    fn test_create_account_failure_no_funds_runtime() {
        let node = create_runtime_node();
        test_create_account_failure_no_funds(node);
    }

    #[test]
    fn test_create_account_failure_already_exists_runtime() {
        let node = create_runtime_node();
        test_create_account_failure_already_exists(node);
    }

    #[test]
    fn test_swap_key_runtime() {
        let node = create_runtime_node();
        test_swap_key(node);
    }

    #[test]
    fn test_add_key_runtime() {
        let node = create_runtime_node();
        test_add_key(node);
    }

    #[test]
    fn test_add_existing_key_runtime() {
        let node = create_runtime_node();
        test_add_existing_key(node);
    }

    #[test]
    fn test_delete_key_runtime() {
        let node = create_runtime_node();
        test_delete_key(node);
    }

    #[test]
    fn test_delete_key_not_owned_runtime() {
        let node = create_runtime_node();
        test_delete_key_not_owned(node);
    }

    #[test]
    fn test_delete_key_last_runtime() {
        let node = create_runtime_node();
        test_delete_key_last(node);
    }

    #[test]
    fn test_add_access_key_function_call_runtime() {
        let node = create_runtime_node();
        test_add_access_key_function_call(node);
    }

    #[test]
    fn test_delete_access_key_runtime() {
        let node = create_runtime_node();
        test_delete_access_key(node);
    }

    #[test]
    fn test_add_access_key_with_allowance_runtime() {
        let node = create_runtime_node();
        test_add_access_key_with_allowance(node);
    }

    #[test]
    fn test_delete_access_key_with_allowance_runtime() {
        let node = create_runtime_node();
        test_delete_access_key_with_allowance(node);
    }

    #[test]
    fn test_access_key_smart_contract_runtime() {
        let node = create_runtime_node();
        test_access_key_smart_contract(node);
    }

    #[test]
    fn test_access_key_smart_contract_reject_method_name_runtime() {
        let node = create_runtime_node();
        test_access_key_smart_contract_reject_method_name(node);
    }

    #[test]
    fn test_access_key_smart_contract_reject_contract_id_runtime() {
        let node = create_runtime_node();
        test_access_key_smart_contract_reject_contract_id(node);
    }

    #[test]
    fn test_access_key_reject_non_function_call_runtime() {
        let node = create_runtime_node();
        test_access_key_reject_non_function_call(node);
    }

    #[test]
    fn test_increase_stake_runtime() {
        let node = create_runtime_node();
        test_increase_stake(node);
    }

    #[test]
    fn test_decrease_stake_runtime() {
        let node = create_runtime_node();
        test_decrease_stake(node);
    }

    #[test]
    fn test_unstake_while_not_staked_runtime() {
        let node = create_runtime_node();
        test_unstake_while_not_staked(node);
    }

    #[test]
    fn test_fail_not_enough_balance_for_storage_runtime() {
        let node = create_runtime_with_expensive_storage();
        test_fail_not_enough_balance_for_storage(node);
    }

    #[test]
    fn test_delete_account_signer_is_receiver() {
        let node = create_runtime_node();
        test_delete_account_ok(node);
    }

    #[test]
    fn test_delete_account_has_enough_money_runtime() {
        let node = create_runtime_node();
        test_delete_account_fail(node, PROTOCOL_VERSION);
    }

    #[test]
    fn test_delete_account_no_account_runtime() {
        let node = create_runtime_node();
        test_delete_account_no_account(node);
    }

    #[test]
    fn test_delete_account_implicit_beneficiary_account_runtime() {
        let node = create_runtime_node();
        test_delete_account_implicit_beneficiary_account(node, PROTOCOL_VERSION);
    }

    #[test]
    fn test_delete_account_while_staking_runtime() {
        let node = create_runtime_node();
        test_delete_account_while_staking(node, PROTOCOL_VERSION);
    }

    #[test]
    fn test_smart_contract_free_runtime() {
        let node = create_free_runtime_node();
        test_smart_contract_free(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_deploy_call_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_deploy_call_runtime() {
        let node = create_runtime_node();
        test_evm_deploy_call(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_fibonacci_gas_limit_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_fibonacci_gas_limit_runtime() {
        let node = create_runtime_node();
        test_evm_fibonacci_gas_limit(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_infinite_loop_gas_limit_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_infinite_loop_gas_limit_runtime() {
        let node = create_runtime_node();
        test_evm_infinite_loop_gas_limit(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_fibonacci_16_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_fibonacci_16_runtime() {
        let node = create_runtime_node();
        test_evm_fibonacci_16(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_crypto_zombies_contract_ownership_transfer_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_crypto_zombies_contract_ownership_transfer_runtime() {
        let node = create_runtime_node();
        test_evm_crypto_zombies_contract_ownership_transfer(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_crypto_zombies_contract_level_up_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_crypto_zombies_contract_level_up_runtime() {
        let node = create_runtime_node();
        test_evm_crypto_zombies_contract_level_up(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_crypto_zombies_contract_transfer_erc721_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_crypto_zombies_contract_transfer_erc721_runtime() {
        let node = create_runtime_node();
        test_evm_crypto_zombies_contract_transfer_erc721(node);
    }

    // cargo test --test test_cases_runtime test::test_evm_call_standard_precompiles_runtime --features nightly_protocol_features -- --exact --nocapture
    #[cfg(feature = "protocol_feature_evm")]
    #[test]
    fn test_evm_call_standard_precompiles_runtime() {
        let node = create_runtime_node();
        test_evm_call_standard_precompiles(node);
    }
}
