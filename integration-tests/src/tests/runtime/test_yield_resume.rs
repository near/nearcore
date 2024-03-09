use crate::node::{Node, RuntimeNode};
use near_primitives::views::FinalExecutionStatus;

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
fn create_and_resume_in_one_call() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    let yield_payload = vec![23u8; 16];

    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract".parse().unwrap(),
            "call_yield_create_and_resume",
            yield_payload,
            MAX_GAS,
            0,
        )
        .unwrap();

    // the yield callback is expected to execute successfully,
    // returning twice the value of the first byte of the payload
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![46u8]),
        "{res:?} unexpected result; expected 46",
    );
}

#[test]
fn resume_without_yield() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    // payload followed by data id
    let args: Vec<u8> = vec![42u8; 12].into_iter().chain(vec![23u8; 32].into_iter()).collect();

    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract".parse().unwrap(),
            "call_yield_resume",
            args,
            MAX_GAS,
            0,
        )
        .unwrap();

    // expect the execution to suceed, but return 'false'
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
        "{res:?} unexpected result; expected 0",
    );
}
