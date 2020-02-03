#[cfg(test)]
mod test {
    use near::config::{GenesisConfigExt, TESTING_INIT_BALANCE};
    use near_chain_configs::GenesisConfig;
    use near_primitives::serialize::to_base64;
    use near_primitives::state_record::StateRecord;
    use near_primitives::utils::key_for_data;
    use testlib::node::RuntimeNode;
    use testlib::runtime_utils::{add_test_contract, alice_account, bob_account};
    use testlib::standard_test_cases::*;

    fn create_runtime_node() -> RuntimeNode {
        RuntimeNode::new(&alice_account())
    }

    fn create_free_runtime_node() -> RuntimeNode {
        RuntimeNode::free(&alice_account())
    }

    fn create_runtime_with_expensive_storage() -> RuntimeNode {
        let mut genesis_config =
            GenesisConfig::test(vec![&alice_account(), &bob_account(), "carol.near"], 1);
        add_test_contract(&mut genesis_config, &bob_account());
        // Set expensive state rent and add alice more money.
        genesis_config.runtime_config.storage_cost_byte_per_block = TESTING_INIT_BALANCE / 100000;
        genesis_config.runtime_config.poke_threshold = 10;
        match &mut genesis_config.records[0] {
            StateRecord::Account { account, .. } => account.amount = TESTING_INIT_BALANCE * 10000,
            _ => {
                panic!("the first record is expected to be alice account creation!");
            }
        }
        genesis_config.records.push(StateRecord::Data {
            key: to_base64(&key_for_data(&bob_account(), b"test")),
            value: to_base64(b"123"),
        });
        RuntimeNode::new_from_genesis(&alice_account(), genesis_config)
    }

    fn create_runtime_with_expensive_account_length() -> RuntimeNode {
        let mut genesis_config =
            GenesisConfig::test(vec![&alice_account(), &bob_account(), "carol.near"], 1);
        // Set expensive account length rent and add alice more money.
        // `bob.near` has 8 characters. Cost per block is `base / (3^6)`.
        // Need to have balance as least `10 * base / (3^6)`, so if we put `base` at least 73
        // it would be enough to delete bob's account.
        genesis_config.runtime_config.account_length_baseline_cost_per_block =
            73 * TESTING_INIT_BALANCE;
        genesis_config.runtime_config.storage_cost_byte_per_block = 1;
        genesis_config.runtime_config.poke_threshold = 10;
        match &mut genesis_config.records[0] {
            StateRecord::Account { account, .. } => account.amount = TESTING_INIT_BALANCE * 100,
            _ => {}
        }
        genesis_config.records.push(StateRecord::Data {
            key: to_base64(&key_for_data(&bob_account(), b"test")),
            value: to_base64(b"123"),
        });
        RuntimeNode::new_from_genesis(&alice_account(), genesis_config)
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
    fn test_fail_not_enough_rent_for_storage_runtime() {
        let node = create_runtime_with_expensive_storage();
        test_fail_not_enough_rent(node);
    }

    #[test]
    fn test_stake_fail_not_enough_rent_for_storage_runtime() {
        let node = create_runtime_with_expensive_storage();
        test_stake_fail_not_enough_rent_for_storage(node);
    }

    #[test]
    fn test_delete_account_for_storage_runtime() {
        let node = create_runtime_with_expensive_storage();
        test_delete_account_low_balance(node);
    }

    #[test]
    fn test_fail_not_enough_rent_for_account_id_runtime() {
        let node = create_runtime_with_expensive_account_length();
        test_fail_not_enough_rent(node);
    }

    #[test]
    fn test_stake_fail_not_enough_rent_for_account_id_runtime() {
        let node = create_runtime_with_expensive_account_length();
        test_stake_fail_not_enough_rent_for_account_id(node);
    }

    #[test]
    fn test_delete_account_for_account_id_runtime() {
        let node = create_runtime_with_expensive_account_length();
        test_delete_account_low_balance(node);
    }

    #[test]
    fn test_delete_account_has_enough_money_runtime() {
        let node = create_runtime_node();
        test_delete_account_fail(node);
    }

    #[test]
    fn test_delete_account_no_account_runtime() {
        let node = create_runtime_node();
        test_delete_account_no_account(node);
    }

    #[test]
    fn test_delete_account_while_staking_runtime() {
        let node = create_runtime_node();
        test_delete_account_while_staking(node);
    }

    #[test]
    fn test_smart_contract_free_runtime() {
        let node = create_free_runtime_node();
        test_smart_contract_free(node);
    }
}
