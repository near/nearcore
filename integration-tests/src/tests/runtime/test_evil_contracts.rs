use crate::node::{Node, RuntimeNode};
use near_primitives::errors::{ActionError, ActionErrorKind, FunctionCallError};
use near_primitives::views::FinalExecutionStatus;
use std::mem::size_of;

use assert_matches::assert_matches;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

/// Max prepaid amount of gas.
const MAX_GAS: u64 = 300_000_000_000_000;

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".parse().unwrap());
    let account_id = node.account_id().unwrap();
    let node_user = node.user();
    let transaction_result = node_user
        .create_account(
            account_id,
            "test_contract".parse().unwrap(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE / 2,
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);

    let transaction_result =
        node_user.deploy_contract("test_contract".parse().unwrap(), wasm_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);

    node
}

#[test]
fn test_evil_deep_trie() {
    let node = setup_test_contract(near_test_contracts::rs_contract());
    for i in 0..50 {
        println!("insertStrings #{}", i);
        let from = i * 10 as u64;
        let to = (i + 1) * 10 as u64;
        let mut input_data = [0u8; 2 * size_of::<u64>()];
        input_data[..size_of::<u64>()].copy_from_slice(&from.to_le_bytes());
        input_data[size_of::<u64>()..].copy_from_slice(&to.to_le_bytes());
        let res = node
            .user()
            .function_call(
                "alice.near".parse().unwrap(),
                "test_contract".parse().unwrap(),
                "insert_strings",
                input_data.to_vec(),
                MAX_GAS,
                0,
            )
            .unwrap();
        println!("Gas burnt: {}", res.receipts_outcome[0].outcome.gas_burnt);
        assert_eq!(res.status, FinalExecutionStatus::SuccessValue(Vec::new()), "{:?}", res);
    }
    for i in (0..50).rev() {
        println!("deleteStrings #{}", i);
        let from = i * 10 as u64;
        let to = (i + 1) * 10 as u64;
        let mut input_data = [0u8; 2 * size_of::<u64>()];
        input_data[..size_of::<u64>()].copy_from_slice(&from.to_le_bytes());
        input_data[size_of::<u64>()..].copy_from_slice(&to.to_le_bytes());
        let res = node
            .user()
            .function_call(
                "alice.near".parse().unwrap(),
                "test_contract".parse().unwrap(),
                "delete_strings",
                input_data.to_vec(),
                MAX_GAS,
                0,
            )
            .unwrap();
        println!("Gas burnt: {}", res.receipts_outcome[0].outcome.gas_burnt);
        assert_eq!(res.status, FinalExecutionStatus::SuccessValue(Vec::new()), "{:?}", res);
    }
}

/// Test delaying the conclusion of a receipt for as long as possible through the use of self
/// cross-contract calls.
///
/// I hear that the protocol-level limit on the depth here is 64, so given the current fee
/// structure this limit cannot be reached by this contract, but once they decrease it might very
/// well be necessary to adjust the `expected_depth` to at most that limit.
#[test]
fn test_self_delay() {
    let node = setup_test_contract(near_test_contracts::rs_contract());
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract".parse().unwrap(),
            "max_self_recursion_delay",
            vec![0; 4],
            MAX_GAS,
            0,
        )
        .unwrap();

    // The exact expected depth varies depending on the set of enabled features.
    // When test_features are enabled, the test contract becomes larger and the calls to it are more expensive.
    // When nightly is enabled, the gas costs change a bit.
    // The test makes sure that the depth is within the expected range, but it doesn't check an exact value
    // to avoid having separate cases for every possible combination of features.
    let min_expected_depth = 56;
    let max_expected_depth = 62;
    match res.status {
        FinalExecutionStatus::SuccessValue(depth_bytes) => {
            let depth = u32::from_be_bytes(depth_bytes.try_into().unwrap());
            assert!(depth >= min_expected_depth, "The function has recursed fewer times than expected: {depth} < {min_expected_depth}",);
            assert!(depth <= max_expected_depth, "The function has recursed more times than expected: {depth} > {max_expected_depth}",);
        }
        _ => panic!("Expected success, got: {:?}", res),
    }
}

#[test]
fn test_evil_deep_recursion() {
    let node = setup_test_contract(near_test_contracts::rs_contract());
    for n in [100u64, 1000, 10000, 100000, 1000000] {
        println!("{}", n);
        let n_bytes = n.to_le_bytes().to_vec();
        let res = node
            .user()
            .function_call(
                "alice.near".parse().unwrap(),
                "test_contract".parse().unwrap(),
                "recurse",
                n_bytes.clone(),
                MAX_GAS,
                0,
            )
            .unwrap();
        if n <= 1000 {
            assert_eq!(res.status, FinalExecutionStatus::SuccessValue(n_bytes), "{:?}", res);
        } else {
            assert_matches!(res.status, FinalExecutionStatus::Failure(_), "{:?}", res);
        }
    }
}

#[test]
fn test_evil_abort() {
    let node = setup_test_contract(near_test_contracts::rs_contract());
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract".parse().unwrap(),
            "abort_with_zero",
            vec![],
            MAX_GAS,
            0,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
                    "String encoding is bad UTF-16 sequence.".to_string()
                ))
            }
            .into()
        ),
        "{:?}",
        res
    );
}
