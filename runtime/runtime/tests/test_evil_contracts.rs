use near_primitives::types::Gas;
use near_primitives::views::FinalTransactionStatus;
use std::mem::size_of;
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
        10u128.pow(14),
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
    let node =
        setup_test_contract(include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm"));
    (0..50).for_each(|i| {
        println!("insertStrings #{}", i);
        let from = i * 10 as u64;
        let to = (i + 1) * 10 as u64;
        let mut input_data = [0u8; 2 * size_of::<u64>()];
        input_data[..size_of::<u64>()].copy_from_slice(&from.to_le_bytes());
        input_data[size_of::<u64>()..].copy_from_slice(&to.to_le_bytes());
        let res = node.user().function_call(
            "alice.near".to_string(),
            "test_contract".to_string(),
            "insert_strings",
            input_data.to_vec(),
            FUNCTION_CALL_GAS_AMOUNT,
            0,
        );
        assert_eq!(res.status, FinalTransactionStatus::Completed, "{:?}", res);
    });
    (0..50).rev().for_each(|i| {
        println!("deleteStrings #{}", i);
        let from = i * 10 as u64;
        let to = (i + 1) * 10 as u64;
        let mut input_data = [0u8; 2 * size_of::<u64>()];
        input_data[..size_of::<u64>()].copy_from_slice(&from.to_le_bytes());
        input_data[size_of::<u64>()..].copy_from_slice(&to.to_le_bytes());
        let res = node.user().function_call(
            "alice.near".to_string(),
            "test_contract".to_string(),
            "delete_strings",
            input_data.to_vec(),
            FUNCTION_CALL_GAS_AMOUNT,
            0,
        );
        assert_eq!(res.status, FinalTransactionStatus::Completed, "{:?}", res);
    });
}

#[test]
fn test_evil_deep_recursion() {
    let node =
        setup_test_contract(include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm"));
    [100, 1000, 10000, 100000, 1000000].iter().for_each(|n| {
        println!("{}", n);
        let n = *n as u64;
        let n = n.to_le_bytes().to_vec();
        let res = node.user().function_call(
            "alice.near".to_string(),
            "test_contract".to_string(),
            "recurse",
            n,
            FUNCTION_CALL_GAS_AMOUNT,
            0,
        );
        assert_eq!(res.status, FinalTransactionStatus::Completed, "{:?}", res);
    });
}
