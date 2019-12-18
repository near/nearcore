use near_primitives::errors::ActionError;
use near_primitives::serialize::to_base64;
use near_primitives::views::FinalExecutionStatus;
use std::mem::size_of;
use testlib::node::{Node, RuntimeNode};

#[cfg(test)]
use assert_matches::assert_matches;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".to_string());
    let account_id = node.account_id().unwrap();
    let node_user = node.user();
    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            "test_contract".to_string(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE / 2,
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
                10u64.pow(18),
                0,
            )
            .unwrap();
        println!("Gas burnt: {}", res.receipts[0].outcome.gas_burnt);
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
                10u64.pow(18),
                0,
            )
            .unwrap();
        println!("Gas burnt: {}", res.receipts[0].outcome.gas_burnt);
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
                10u64.pow(18),
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
            10u64.pow(18),
            0,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::Failure(
            ActionError::FunctionCallError("String encoding is bad UTF-16 sequence.".to_string())
                .into()
        ),
        "{:?}",
        res
    );
}
