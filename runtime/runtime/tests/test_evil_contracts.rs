use near_primitives::rpc::FinalTransactionStatus;
use near_primitives::types::Gas;
use testlib::node::{Node, RuntimeNode};

const FUNCTION_CALL_GAS_AMOUNT: Gas = 1_000_000_000;

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".to_string());
    let account_id = node.account_id().unwrap();
    let node_user = node.user();
    let transaction_result = node_user.create_account(
        account_id.clone(),
        "test_contract".to_string(),
        node.signer().public_key(),
        10,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);

    let transaction_result =
        node_user.deploy_contract("test_contract".to_string(), wasm_binary.to_vec());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);

    node
}

#[test]
fn test_evil_deep_trie() {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    (0..50).for_each(|i| {
        println!("insertStrings #{}", i);
        let input_data = format!("{{\"from\": {}, \"to\": {}}}", i * 10, (i + 1) * 10);
        node.user().function_call(
            "alice.near".to_string(),
            "test_contract".to_string(),
            "insertStrings",
            input_data.as_bytes().to_vec(),
            FUNCTION_CALL_GAS_AMOUNT,
            0,
        );
    });
    (0..50).rev().for_each(|i| {
        println!("deleteStrings #{}", i);
        let input_data = format!("{{\"from\": {}, \"to\": {}}}", i * 10, (i + 1) * 10);
        node.user().function_call(
            "alice.near".to_string(),
            "test_contract".to_string(),
            "deleteStrings",
            input_data.as_bytes().to_vec(),
            FUNCTION_CALL_GAS_AMOUNT,
            0,
        );
    });
}

#[test]
fn test_evil_deep_recursion() {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    [100, 1000, 10000, 100000, 1000000].iter().for_each(|n| {
        println!("{}", n);
        let input_data = format!("{{\"n\": {}}}", n);
        node.user().function_call(
            "alice.near".to_string(),
            "test_contract".to_string(),
            "recurse",
            input_data.as_bytes().to_vec(),
            FUNCTION_CALL_GAS_AMOUNT,
            0,
        );
    });
}
