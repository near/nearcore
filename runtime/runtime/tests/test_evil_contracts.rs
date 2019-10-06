use near_primitives::serialize::to_base64;
use near_primitives::types::Gas;
use near_primitives::views::FinalExecutionStatus;
use std::mem::size_of;
use testlib::node::{Node, RuntimeNode};

const FUNCTION_CALL_GAS_AMOUNT: Gas = 1_000_000_000;

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".to_string());
    let account_id = node.account_id().unwrap();
    let node_user = node.user();
    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            "test_contract".to_string(),
            node.signer().public_key(),
            10u128.pow(14),
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);

    let transaction_result =
        node_user.deploy_contract("test_contract".to_string(), wasm_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);

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
        let res = node
            .user()
            .function_call(
                "alice.near".to_string(),
                "test_contract".to_string(),
                "insert_strings",
                input_data.to_vec(),
                FUNCTION_CALL_GAS_AMOUNT,
                0,
            )
            .unwrap();
        assert_eq!(res.status, FinalExecutionStatus::SuccessValue(to_base64(&[])), "{:?}", res);
    });
    (0..50).rev().for_each(|i| {
        println!("deleteStrings #{}", i);
        let from = i * 10 as u64;
        let to = (i + 1) * 10 as u64;
        let mut input_data = [0u8; 2 * size_of::<u64>()];
        input_data[..size_of::<u64>()].copy_from_slice(&from.to_le_bytes());
        input_data[size_of::<u64>()..].copy_from_slice(&to.to_le_bytes());
        let res = node
            .user()
            .function_call(
                "alice.near".to_string(),
                "test_contract".to_string(),
                "delete_strings",
                input_data.to_vec(),
                FUNCTION_CALL_GAS_AMOUNT,
                0,
            )
            .unwrap();
        assert_eq!(res.status, FinalExecutionStatus::SuccessValue(to_base64(&[])), "{:?}", res);
    });
}

#[test]
fn test_evil_deep_recursion() {
    let node =
        setup_test_contract(include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm"));
    [100, 1000, 10000, 100000, 1000000].iter().for_each(|n| {
        println!("{}", n);
        let n = *n as u64;
        let n_bytes = n.to_le_bytes().to_vec();
        let res = node
            .user()
            .function_call(
                "alice.near".to_string(),
                "test_contract".to_string(),
                "recurse",
                n_bytes.clone(),
                FUNCTION_CALL_GAS_AMOUNT,
                0,
            )
            .unwrap();
        if n <= 10000 {
            assert_eq!(
                res.status,
                FinalExecutionStatus::SuccessValue(to_base64(&n_bytes)),
                "{:?}",
                res
            );
        } else {
            assert_eq!(res.status, FinalExecutionStatus::Failure, "{:?}", res);
        }
    });
}
