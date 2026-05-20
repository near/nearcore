use crate::node::{Node, RuntimeNode};
use near_primitives::types::{Balance, Gas};
use near_primitives::views::FinalExecutionStatus;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = Balance::from_near(1_000_000_000);

/// Max prepaid amount of gas.
const MAX_GAS: Gas = Gas::from_teragas(300);

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".parse().unwrap());
    let account_id = node.account_id().unwrap();
    let node_user = node.user();
    let transaction_result = node_user
        .create_account(
            account_id,
            "test_contract.alice.near".parse().unwrap(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE.checked_div(2).unwrap(),
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);

    let transaction_result = node_user
        .deploy_contract("test_contract.alice.near".parse().unwrap(), wasm_binary.to_vec())
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);

    node
}

#[test]
fn create_then_resume() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![6u8; 16];

    // Hardcoded key under which the yield callback will write data.
    // We use this to observe whether the callback has been executed.
    let key = 123u64.to_le_bytes().to_vec();

    // Set up the yield execution
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_create_return_data_id",
            yield_payload.clone(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    let data_id = match res.status {
        FinalExecutionStatus::SuccessValue(data_id) => data_id,
        _ => {
            panic!("{res:?} unexpected result; expected some data id");
        }
    };

    // Confirm that the yield callback hasn't been executed yet
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "read_value",
            key.clone(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result",);

    // Call yield resume with the payload followed by the data id
    let args: Vec<u8> = yield_payload.into_iter().chain(data_id.into_iter()).collect();
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_resume",
            args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![1u8]),
        "{res:?} unexpected result; expected 1",
    );

    // Confirm that the yield callback was executed
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "read_value",
            key,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue("Resumed ".as_bytes().to_vec()),
        "{res:?} unexpected result",
    );
}

#[test]
fn create_and_resume_in_one_call() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![23u8; 16];

    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_create_and_resume",
            yield_payload,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    // the yield callback is expected to execute successfully,
    // returning twice the value of the first byte of the payload
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
        "{res:?} unexpected result; expected 16",
    );
}

#[test]
fn resume_without_yield() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    // payload followed by data id
    let args: Vec<u8> = vec![42u8; 12].into_iter().chain(vec![23u8; 32].into_iter()).collect();

    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_resume",
            args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    // expect the execution to succeed, but return 'false'
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
        "{res:?} unexpected result; expected 0",
    );
}

#[cfg(feature = "nightly")]
fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(feature = "nightly")]
fn yield_create_with_id_op(yield_id: &[u8; 32], payload: &[u8], id: i64) -> serde_json::Value {
    serde_json::json!({
        "yield_create_with_id": {
            "method_name": "check_promise_result_return_value",
            "arguments": b64(payload),
            "gas": 0,
            "gas_weight": 1,
            "yield_id": b64(yield_id),
        },
        "id": id,
    })
}

#[cfg(feature = "nightly")]
fn yield_resume_with_yield_id_op(yield_id: &[u8], payload: &[u8], id: i64) -> serde_json::Value {
    serde_json::json!({
        "yield_resume_with_yield_id": { "yield_id": b64(yield_id), "payload": b64(payload) },
        "id": id,
    })
}

#[test]
#[cfg(feature = "nightly")]
fn create_with_id_then_resume_with_yield_id() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    let yield_payload = vec![6u8; 16];
    let yield_id = [9u8; 32];

    // TX1: Create the yield using yield_create_with_id (via call_promise).
    // Use callback that writes to storage so we can observe execution.
    let create_args = serde_json::json!([{
        "yield_create_with_id": {
            "method_name": "check_promise_result_write_status",
            "arguments": b64(&yield_payload),
            "gas": 0,
            "gas_weight": 1,
            "yield_id": b64(&yield_id),
        },
        "id": 0,
    }]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&create_args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");

    // TX2: Resume using yield_resume_with_yield_id (expect success = 1).
    let resume_args =
        serde_json::json!([yield_resume_with_yield_id_op(&yield_id, &yield_payload, 1)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&resume_args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");

    // Confirm the yield callback executed
    let key = 123u64.to_le_bytes().to_vec();
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "read_value",
            key,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue("Resumed ".as_bytes().to_vec()),
        "{res:?} unexpected result",
    );
}

#[test]
#[cfg(feature = "nightly")]
fn create_with_id_and_resume_with_yield_id_in_one_call() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    let yield_payload = vec![23u8; 16];
    let yield_id = [3u8; 32];

    // Create yield (id=0, with promise_return), then resume in the same call (id=1 = success).
    let args = serde_json::json!([
        {
            "yield_create_with_id": {
                "method_name": "check_promise_result_return_value",
                "arguments": b64(&yield_payload),
                "gas": 0,
                "gas_weight": 1,
                "yield_id": b64(&yield_id),
            },
            "id": 0,
            "return": true,
        },
        yield_resume_with_yield_id_op(&yield_id, &yield_payload, 1),
    ]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    // The yield callback returns twice the first byte of the payload (23 * 2 - mod gives 16).
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
        "{res:?} unexpected result; expected 16",
    );
}

#[test]
#[cfg(feature = "nightly")]
fn resume_with_yield_id_without_yield() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    // Resume with a yield_id that was never created — expect failure (id=0).
    let args = serde_json::json!([yield_resume_with_yield_id_op(&[23u8; 32], &[42u8; 12], 0)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");
}

#[test]
#[cfg(feature = "nightly")]
fn create_with_id_duplicate_in_same_call_returns_sentinel() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    let yield_payload = vec![6u8; 16];
    let yield_id = [5u8; 32];

    // First create returns 0 (first promise idx); second call with same yield_id returns
    // u64::MAX (-1 as i64) without aborting.
    let args = serde_json::json!([
        yield_create_with_id_op(&yield_id, &yield_payload, 0),
        yield_create_with_id_op(&yield_id, &yield_payload, -1),
    ]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");
}

#[test]
#[cfg(feature = "nightly")]
fn create_with_id_then_resume_with_yield_id_fails() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    let yield_payload = vec![6u8; 16];
    let yield_id = [7u8; 32];

    // TX1: Create yield with yield_create_with_id.
    let create_args = serde_json::json!([yield_create_with_id_op(&yield_id, &yield_payload, 0)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&create_args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");

    // TX2: Try to resume using yield_id with the OLD yield_resume (data_id flavor).
    // The yield_id is not a valid data_id, so resume returns 0 (false).
    let resume_args: Vec<u8> = yield_payload.into_iter().chain(yield_id.into_iter()).collect();
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_resume",
            resume_args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
        "{res:?} unexpected result; expected 0",
    );
}

#[test]
#[cfg(feature = "nightly")]
fn create_then_resume_with_yield_id_fails() {
    let node = setup_test_contract(near_test_contracts::nightly_rs_contract());

    let yield_payload = vec![6u8; 16];

    // TX1: Create yield with the original yield_create (no yield_id mapping stored).
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_create_return_data_id",
            yield_payload.clone(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    let data_id = match res.status {
        FinalExecutionStatus::SuccessValue(data_id) => data_id,
        _ => panic!("{res:?} unexpected result"),
    };

    // TX2: Try resuming with yield_resume_with_yield_id using the data_id as a yield_id.
    // No yield_id mapping exists for this data_id → returns 0 (failure).
    let args = serde_json::json!([yield_resume_with_yield_id_op(&data_id, &yield_payload, 0)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");
}
