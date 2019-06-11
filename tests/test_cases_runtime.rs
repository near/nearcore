#[cfg(test)]
mod test {
    use testlib::node::RuntimeNode;
    use testlib::runtime_utils::alice_account;
    use testlib::standard_test_cases::*;

    fn create_runtime_node() -> RuntimeNode {
        RuntimeNode::new(&alice_account())
    }

    #[test]
    fn test_smart_contract_simple_runtime() {
        let node = create_runtime_node();
        test_smart_contract_simple(node);
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
    fn test_async_call_with_no_callback_runtime() {
        let node = create_runtime_node();
        test_async_call_with_no_callback(node);
    }

    #[test]
    fn test_async_call_with_callback_runtime() {
        let node = create_runtime_node();
        test_async_call_with_callback(node);
    }

    #[test]
    fn test_async_call_with_logs_runtime() {
        let node = create_runtime_node();
        test_async_call_with_logs(node);
    }

    #[test]
    fn test_callback_runtime() {
        let node = create_runtime_node();
        test_callback(node);
    }

    #[test]
    fn test_callback_failure_runtime() {
        let node = create_runtime_node();
        test_callback_failure(node);
    }

    #[test]
    fn test_deposit_with_callback_runtime() {
        let node = create_runtime_node();
        test_deposit_with_callback(node);
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
    fn test_add_access_key_runtime() {
        let node = create_runtime_node();
        test_add_access_key(node);
    }

    #[test]
    fn test_delete_access_key_runtime() {
        let node = create_runtime_node();
        test_delete_access_key(node);
    }

    #[test]
    fn test_add_access_key_with_funding_runtime() {
        let node = create_runtime_node();
        test_add_access_key_with_funding(node);
    }

    #[test]
    fn test_delete_access_key_with_owner_refund_runtime() {
        let node = create_runtime_node();
        test_delete_access_key_with_owner_refund(node);
    }

    #[test]
    fn test_delete_access_key_with_bob_refund_runtime() {
        let node = create_runtime_node();
        test_delete_access_key_with_bob_refund(node);
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
}
