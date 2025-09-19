use crate::setup::env::TestLoopEnv;
use crate::utils::setups::standard_setup_1;
use crate::utils::transactions::{TransactionRunner, execute_tx, get_shared_block_hash};
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::types::Gas;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::ExecutionStatusView;

/// Redirect a balance refund using the host function `promise_set_refund_to`,
/// let the call fail and then check the refund got redirected.
///
/// This test uses the est contract's method "call_promise", which accepts a
/// promise description. The contract will use the host function accordingly.
///
/// Note that the `refund_to` field can only be use through host functions, not
/// directly by a transaction.
#[test]
fn test_refund_to() {
    init_test_logger();
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = standard_setup_1();

    let account0: AccountId = "account0".parse().unwrap();
    let account0_signer = &create_user_test_signer(&account0).into();
    let rpc_id = "account4".parse().unwrap();

    let test_contract = if ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        near_test_contracts::nightly_rs_contract()
    } else {
        near_test_contracts::rs_contract()
    };

    // Setup: Deploy test contract
    let deploy_tx = SignedTransaction::deploy_contract(
        100,
        &account0,
        test_contract.to_vec(),
        account0_signer,
        get_shared_block_hash(&node_datas, &test_loop.data),
    );
    execute_tx(
        &mut test_loop,
        &rpc_id,
        TransactionRunner::new(deploy_tx, true),
        &node_datas,
        Duration::seconds(5),
    )
    .expect("deployment should succeed")
    .assert_success();

    // Test: Submit a call that should use the refund_to host function
    let balance = 10;
    let arg = serde_json::json!([
        {
            "batch_create": {
                "account_id": "account1",
            },
            "id": 0
        },
        {
            "action_function_call": {
                "promise_index": 0,
                "method_name": "non_existing_function",
                "arguments": [],
                "amount": balance.to_string(),
                "gas": Gas::from_teragas(100),
            },
            "id": 0
        },
        {
            "set_refund_to": {
                "promise_index": 0,
                "beneficiary_id": "refund_receiver"
            }, "id": 0
        }
    ]);
    let fn_call_tx = SignedTransaction::call(
        101,
        account0.clone(),
        account0.clone(),
        &account0_signer,
        0,
        "call_promise".into(),
        arg.to_string().into(),
        Gas::from_teragas(300),
        get_shared_block_hash(&node_datas, &test_loop.data),
    );

    let outcome = execute_tx(
        &mut test_loop,
        &rpc_id,
        TransactionRunner::new(fn_call_tx, true),
        &node_datas,
        Duration::seconds(5),
    )
    .expect("should not fail to submit");

    assert_eq!(3, outcome.receipts_outcome.len(), "should have 3 receipt outcomes");

    let first_call_outcome = &outcome.receipts_outcome[0];
    let second_call_outcome = &outcome.receipts_outcome[1];
    let refund_outcome = &outcome.receipts_outcome[2];

    // we expect the call to fail, it should trigger a failure followed by a refund
    assert!(matches!(first_call_outcome.outcome.status, ExecutionStatusView::SuccessValue(_)));
    assert!(matches!(second_call_outcome.outcome.status, ExecutionStatusView::Failure(_)));

    // check refund went to the right place (depending on protocol version)
    let expected_receiver = if ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        "refund_receiver"
    } else {
        account0.as_str()
    };
    assert_eq!(refund_outcome.outcome.executor_id, expected_receiver);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
