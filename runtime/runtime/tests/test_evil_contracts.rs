use near_primitives::errors::ActionError;
use near_primitives::serialize::to_base64;
use near_primitives::types::Gas;
use near_primitives::views::FinalExecutionStatus;
use near_vm_errors::{FunctionExecError, HostError, MethodResolveError, VMError};
use std::mem::size_of;
use testlib::node::{Node, RuntimeNode};

#[cfg(test)]
use assert_matches::assert_matches;

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
        println!("Gas burnt: {}", res.receipts[0].outcome.gas_burnt);
        assert_eq!(res.status, FinalExecutionStatus::SuccessValue(to_base64(&[])), "{:?}", res);
    });
    let mut first_gas_burnt = 0;
    let mut last_gas_burnt = 0;
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
        if i == 0 {
            first_gas_burnt = res.receipts[0].outcome.gas_burnt;
        }
        if i == 49 {
            last_gas_burnt = res.receipts[0].outcome.gas_burnt;
        }
        println!("Gas burnt: {}", res.receipts[0].outcome.gas_burnt);
        assert_eq!(res.status, FinalExecutionStatus::SuccessValue(to_base64(&[])), "{:?}", res);
    });
    // storage_remove also has to get previous value from trie which is expensive
    // ExtCostsConfig.touching_trie_node should be high enough to be more noticeable than cpu costs
    assert!(last_gas_burnt > first_gas_burnt * 15);
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
        if n <= 1000 {
            assert_eq!(
                res.status,
                FinalExecutionStatus::SuccessValue(to_base64(&n_bytes)),
                "{:?}",
                res
            );
        } else {
            assert_matches!(res.status, FinalExecutionStatus::Failure(_), "{:?}", res);
        }
    });
}

#[test]
fn test_evil_abort() {
    let node =
        setup_test_contract(include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm"));
    let res = node
        .user()
        .function_call(
            "alice.near".to_string(),
            "test_contract".to_string(),
            "abort_with_zero",
            vec![],
            FUNCTION_CALL_GAS_AMOUNT,
            0,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::Failure(
            ActionError::FunctionCall("String encoding is bad UTF-16 sequence.".to_string()).into()
        ),
        "{:?}",
        res
    );
}
