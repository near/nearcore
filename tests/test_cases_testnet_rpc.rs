//! Runs standard test cases against TestNet with several nodes running in separate threads.
//! The communication is performed through `RPCUser` that uses the standard RPC API to communicate.
#[cfg(feature = "old_tests")]
#[cfg(feature = "expensive_tests")]
#[cfg(test)]
mod test {
    use node_runtime::chain_spec::DefaultIdType;
    use std::sync::atomic::{AtomicU16, Ordering};
    use testlib::node::thread_node::ThreadNode;
    use testlib::node::{
        create_nodes_with_id_type, Node, NodeConfig, TEST_BLOCK_FETCH_LIMIT, TEST_BLOCK_MAX_SIZE,
    };
    use testlib::runtime_utils::alice_account;
    use testlib::standard_test_cases::*;
    use testlib::test_helpers::heavy_test;
    const NUM_TEST_NODE: usize = 4;
    static TEST_PORT: AtomicU16 = AtomicU16::new(6000);

    fn create_thread_nodes_rpc(test_prefix: &str, test_port: u16) -> Vec<ThreadNode> {
        let (_, account_names, nodes) = create_nodes_with_id_type(
            NUM_TEST_NODE,
            test_prefix,
            test_port,
            TEST_BLOCK_FETCH_LIMIT,
            TEST_BLOCK_MAX_SIZE,
            vec![],
            DefaultIdType::Named,
        );
        assert_eq!(account_names[0], alice_account());
        // Convert thread nodes to process nodes.
        let mut nodes: Vec<_> = nodes
            .into_iter()
            .map(|cfg| match cfg {
                NodeConfig::Thread(config) => ThreadNode::new(config),
                _ => unreachable!(),
            })
            .collect();
        for i in 0..NUM_TEST_NODE {
            nodes[i].use_rpc_user(true);
            nodes[i].start();
        }
        nodes
    }

    /// Macro for running testnet test but use RPC. Increment the atomic global counter for port,
    /// and get the test_prefix from the test name.
    macro_rules! run_testnet_test {
        ($f:expr) => {
            let port = TEST_PORT.fetch_add(NUM_TEST_NODE as u16, Ordering::SeqCst);
            let test_prefix = stringify!($f);
            let mut nodes = create_thread_nodes_rpc(test_prefix, port);
            let node = nodes.pop().unwrap();
            heavy_test(|| $f(node));
        };
    }

    #[test]
    fn test_smart_contract_simple_testnet() {
        run_testnet_test!(test_smart_contract_simple);
    }

    #[test]
    fn test_smart_contract_bad_method_name_testnet() {
        run_testnet_test!(test_smart_contract_bad_method_name);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_no_tokens_testnet() {
        run_testnet_test!(test_smart_contract_empty_method_name_with_no_tokens);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_tokens_testnet() {
        run_testnet_test!(test_smart_contract_empty_method_name_with_tokens);
    }

    #[test]
    fn test_smart_contract_with_args_testnet() {
        run_testnet_test!(test_smart_contract_with_args);
    }

    #[test]
    fn test_nonce_update_when_deploying_contract_testnet() {
        run_testnet_test!(test_nonce_update_when_deploying_contract);
    }

    #[test]
    fn test_nonce_updated_when_tx_failed_testnet() {
        run_testnet_test!(test_nonce_updated_when_tx_failed);
    }

    #[test]
    fn test_upload_contract_testnet() {
        run_testnet_test!(test_upload_contract);
    }

    #[test]
    fn test_redeploy_contract_testnet() {
        run_testnet_test!(test_redeploy_contract);
    }

    #[test]
    fn test_send_money_testnet() {
        run_testnet_test!(test_send_money);
    }

    #[test]
    fn test_send_money_over_balance_testnet() {
        run_testnet_test!(test_send_money_over_balance);
    }

    #[test]
    fn test_refund_on_send_money_to_non_existent_account_testnet() {
        run_testnet_test!(test_refund_on_send_money_to_non_existent_account);
    }

    #[test]
    fn test_create_account_testnet() {
        run_testnet_test!(test_create_account);
    }

    #[test]
    fn test_create_account_again_testnet() {
        run_testnet_test!(test_create_account_again);
    }

    #[test]
    fn test_create_account_failure_invalid_name_testnet() {
        run_testnet_test!(test_create_account_failure_invalid_name);
    }

    #[test]
    fn test_create_account_failure_already_exists_testnet() {
        run_testnet_test!(test_create_account_failure_already_exists);
    }

    #[test]
    fn test_swap_key_testnet() {
        run_testnet_test!(test_swap_key);
    }

    #[test]
    fn test_add_key_testnet() {
        run_testnet_test!(test_add_key);
    }

    #[test]
    fn test_add_existing_key_testnet() {
        run_testnet_test!(test_add_existing_key);
    }

    #[test]
    fn test_delete_key_testnet() {
        run_testnet_test!(test_delete_key);
    }

    #[test]
    fn test_delete_key_not_owned_testnet() {
        run_testnet_test!(test_delete_key_not_owned);
    }

    #[test]
    fn test_delete_key_last_testnet() {
        run_testnet_test!(test_delete_key_last);
    }

    #[test]
    fn test_add_access_key_testnet() {
        run_testnet_test!(test_add_access_key);
    }

    #[test]
    fn test_delete_access_key_testnet() {
        run_testnet_test!(test_delete_access_key);
    }

    #[test]
    fn test_add_access_key_with_funding_testnet() {
        run_testnet_test!(test_add_access_key_with_funding);
    }

    #[test]
    fn test_delete_access_key_with_owner_refund_testnet() {
        run_testnet_test!(test_delete_access_key_with_owner_refund);
    }

    #[test]
    fn test_delete_access_key_with_bob_refund_testnet() {
        run_testnet_test!(test_delete_access_key_with_bob_refund);
    }

    #[test]
    fn test_access_key_smart_contract_testnet() {
        run_testnet_test!(test_access_key_smart_contract);
    }

    #[test]
    fn test_access_key_smart_contract_reject_positive_amount_testnet() {
        run_testnet_test!(test_access_key_smart_contract_reject_positive_amount);
    }

    #[test]
    fn test_access_key_smart_contract_reject_method_name_testnet() {
        run_testnet_test!(test_access_key_smart_contract_reject_method_name);
    }

    #[test]
    fn test_access_key_smart_contract_reject_contract_id_testnet() {
        run_testnet_test!(test_access_key_smart_contract_reject_contract_id);
    }

    #[test]
    fn test_access_key_reject_non_function_call_testnet() {
        run_testnet_test!(test_access_key_reject_non_function_call);
    }
}
