#[cfg(feature = "old_tests")]
#[cfg(test)]
mod test {
    use configs::ClientConfig;
    use node_runtime::chain_spec::{AuthorityRotation, ChainSpec, DefaultIdType};
    use testlib::node::shard_client_node::ShardClientNode;
    use testlib::standard_test_cases::*;

    fn test_chain_spec() -> ChainSpec {
        ChainSpec::testing_spec(DefaultIdType::Named, 3, 3, AuthorityRotation::ProofOfAuthority).0
    }

    fn create_shard_client_node() -> ShardClientNode {
        let mut client_cfg = ClientConfig::default_devnet();
        client_cfg.chain_spec = test_chain_spec();
        ShardClientNode::new(client_cfg)
    }

    #[test]
    fn test_smart_contract_simple_shard_client() {
        let node = create_shard_client_node();
        test_smart_contract_simple(node);
    }

    #[test]
    fn test_smart_contract_bad_method_name_shard_client() {
        let node = create_shard_client_node();
        test_smart_contract_bad_method_name(node);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_no_tokens_shard_client() {
        let node = create_shard_client_node();
        test_smart_contract_empty_method_name_with_no_tokens(node);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_tokens_shard_client() {
        let node = create_shard_client_node();
        test_smart_contract_empty_method_name_with_tokens(node);
    }

    #[test]
    fn test_smart_contract_with_args_shard_client() {
        let node = create_shard_client_node();
        test_smart_contract_with_args(node);
    }

    #[test]
    fn test_async_call_with_no_callback_shard_client() {
        let node = create_shard_client_node();
        test_async_call_with_no_callback(node);
    }

    #[test]
    fn test_async_call_with_callback_shard_client() {
        let node = create_shard_client_node();
        test_async_call_with_callback(node);
    }

    #[test]
    fn test_async_call_with_logs_shard_client() {
        let node = create_shard_client_node();
        test_async_call_with_logs(node);
    }

    #[test]
    fn test_deposit_with_callback_shard_client() {
        let node = create_shard_client_node();
        test_deposit_with_callback(node);
    }

    #[test]
    fn test_nonce_update_when_deploying_contract_shard_client() {
        let node = create_shard_client_node();
        test_nonce_update_when_deploying_contract(node);
    }

    #[test]
    fn test_nonce_updated_when_tx_failed_shard_client() {
        let node = create_shard_client_node();
        test_nonce_updated_when_tx_failed(node);
    }

    #[test]
    fn test_upload_contract_shard_client() {
        let node = create_shard_client_node();
        test_upload_contract(node);
    }

    #[test]
    fn test_redeploy_contract_shard_client() {
        let node = create_shard_client_node();
        test_redeploy_contract(node);
    }

    #[test]
    fn test_send_money_shard_client() {
        let node = create_shard_client_node();
        test_send_money(node);
    }

    #[test]
    fn test_send_money_over_balance_shard_client() {
        let node = create_shard_client_node();
        test_send_money_over_balance(node);
    }

    #[test]
    fn test_refund_on_send_money_to_non_existent_account_shard_client() {
        let node = create_shard_client_node();
        test_refund_on_send_money_to_non_existent_account(node);
    }

    #[test]
    fn test_create_account_shard_client() {
        let node = create_shard_client_node();
        test_create_account(node);
    }

    #[test]
    fn test_create_account_again_shard_client() {
        let node = create_shard_client_node();
        test_create_account_again(node);
    }

    #[test]
    fn test_create_account_failure_invalid_name_shard_client() {
        let node = create_shard_client_node();
        test_create_account_failure_invalid_name(node);
    }

    #[test]
    fn test_create_account_failure_already_exists_shard_client() {
        let node = create_shard_client_node();
        test_create_account_failure_already_exists(node);
    }

    #[test]
    fn test_swap_key_shard_client() {
        let node = create_shard_client_node();
        test_swap_key(node);
    }

    #[test]
    fn test_add_key_shard_client() {
        let node = create_shard_client_node();
        test_add_key(node);
    }

    #[test]
    fn test_add_existing_key_shard_client() {
        let node = create_shard_client_node();
        test_add_existing_key(node);
    }

    #[test]
    fn test_delete_key_shard_client() {
        let node = create_shard_client_node();
        test_delete_key(node);
    }

    #[test]
    fn test_delete_key_not_owned_shard_client() {
        let node = create_shard_client_node();
        test_delete_key_not_owned(node);
    }

    #[test]
    fn test_delete_key_no_key_left_shard_client() {
        let node = create_shard_client_node();
        test_delete_key_no_key_left(node);
    }
}
