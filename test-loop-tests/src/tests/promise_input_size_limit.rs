use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

fn limit_enabled() -> bool {
    ProtocolFeature::ReceiptPromiseInputSizeLimit.enabled(PROTOCOL_VERSION)
}

/// End-to-end: a contract fans out to two callees that each return 2.5 MiB and a
/// callback (joined via `promise_and`) awaits both results. The callback
/// receipt's combined promise inputs (~5 MiB) exceed the 4 MiB limit, so it must
/// fail with `TotalPromiseInputSizeExceeded` rather than execute.
#[test]
fn test_promise_input_size_limit_fails_callback() {
    init_test_logger();
    if !limit_enabled() {
        tracing::info!(
            "skipping: promise-input size limit not enabled at protocol version {PROTOCOL_VERSION}"
        );
        return;
    }

    let account = create_account_id("user0");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&account, Balance::from_near(100))
        .build();

    // Deploy the test contract (provides `call_promise` and `return_large_value`).
    let deploy_tx =
        env.rpc_node().tx_deploy_contract(&account, near_test_contracts::rs_contract().to_vec());
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Two callees each return 2.5 MiB (individually below `max_length_returned_data`
    // = 4 MiB, so each data receipt is valid), joined into a single callback whose
    // combined promise inputs (~5 MiB) exceed the 4 MiB limit.
    let value_size = 2_500_000u64;
    let callee_gas = 110_000_000_000_000u64;
    let callback_gas = 20_000_000_000_000u64;
    let args = serde_json::json!([
        {
            "create": {
                "account_id": account,
                "method_name": "return_large_value",
                "arguments": {"value_size": value_size},
                "amount": "0",
                "gas": callee_gas,
            },
            "id": 0,
        },
        {
            "create": {
                "account_id": account,
                "method_name": "return_large_value",
                "arguments": {"value_size": value_size},
                "amount": "0",
                "gas": callee_gas,
            },
            "id": 1,
        },
        {"and": [0, 1], "id": 2},
        {
            "then": {
                "promise_index": 2,
                "account_id": account,
                "method_name": "return_large_value",
                "arguments": {"value_size": 0},
                "amount": "0",
                "gas": callback_gas,
            },
            "id": 3,
            // Return the callback promise so the transaction's final status
            // reflects the callback receipt's outcome.
            "return": true,
        },
    ]);

    let call_tx = env.rpc_node().tx_call(
        &account,
        &account,
        "call_promise",
        serde_json::to_vec(&args).unwrap(),
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();

    assert_matches!(
        &outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::TotalPromiseInputSizeExceeded { .. },
            ..
        })),
        "callback with oversized combined promise inputs should fail"
    );
}
