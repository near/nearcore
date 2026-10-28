use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::transaction::{Action, ECC_METHOD_NAME, SignedTransaction};
use near_primitives::types::{AccountId, Balance};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{FinalExecutionStatus, QueryRequest, QueryResponseKind};

/// Value returned by the contract's ECC method.
const ECC_METHOD_RESULT: &[u8] = b"ecc";

/// A contract that exports the ECC method, which returns `ECC_METHOD_RESULT`.
fn ecc_method_contract() -> Vec<u8> {
    let data: String = ECC_METHOD_RESULT.iter().map(|byte| format!("\\{byte:02x}")).collect();
    let data_len = ECC_METHOD_RESULT.len();
    let wat = format!(
        r#"(module
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{data}")

  (func (export "{ECC_METHOD_NAME}")
    (call $value_return (i64.const {data_len}) (i64.const 0))
  )
)"#,
    );
    near_test_contracts::wat_contract(&wat)
}

/// Call the ECC method with an ordinary `FunctionCall` action.
fn ecc_method_tx(env: &TestLoopEnv, signer: &AccountId, contract: &AccountId) -> SignedTransaction {
    env.rpc_node().tx_from_actions(
        signer,
        contract,
        vec![Action::function_call(ECC_METHOD_NAME, vec![], Gas::from_teragas(30))],
    )
}

/// Test that the `ExternalContractCalls` protocol feature reserves the ECC method name. Before the
/// upgrade, a `FunctionCall` action runs the method like any other. After it, the action fails,
/// while a view call can still run the method.
// TODO(ecc): Remove the protocol-upgrade part of this test once `ExternalContractCalls` is stabilized.
#[test]
fn test_ecc_reserved_method_protocol_upgrade() {
    init_test_logger();

    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION) {
        return;
    }

    let new_protocol = ProtocolFeature::ExternalContractCalls.protocol_version();
    let old_protocol = new_protocol - 1;

    let signer = create_account_id("alice");
    let contract = create_account_id("contract");
    let epoch_length = 10;

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(old_protocol)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_protocol))
        .epoch_length(epoch_length)
        .add_user_account(&signer, Balance::from_near(1_000))
        .add_user_account(&contract, Balance::from_near(1_000))
        .build();

    let deploy_tx = env.rpc_node().tx_deploy_contract(&contract, ecc_method_contract());
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(10));

    // Before the upgrade: the reserved name runs like any other method. The upgrade takes ~2
    // epochs with an immediate voting schedule, so we are still on the old protocol here.
    assert_eq!(
        env.rpc_node().protocol_version_at_head(),
        old_protocol,
        "expected to start pre-upgrade"
    );
    let tx = ecc_method_tx(&env, &signer, &contract);
    let result = env.rpc_runner().run_tx(tx, Duration::seconds(10));
    assert_eq!(result, ECC_METHOD_RESULT);

    env.rpc_runner().run_until(
        |node| node.protocol_version_at_head() >= new_protocol,
        Duration::seconds((6 * epoch_length) as i64),
    );

    // After the upgrade: a `FunctionCall` action to the reserved name fails.
    let tx = ecc_method_tx(&env, &signer, &contract);
    let outcome = env
        .rpc_runner()
        .execute_tx(tx, Duration::seconds(10))
        .expect("ecc method tx admitted post-upgrade");
    let method_name = assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            index: Some(0),
            kind: ActionErrorKind::ReservedMethodName { method_name },
        })) => method_name
    );
    assert_eq!(method_name, ECC_METHOD_NAME);

    // A view call can still run the reserved method.
    let response = env
        .rpc_node()
        .runtime_query(QueryRequest::CallFunction {
            account_id: contract,
            method_name: ECC_METHOD_NAME.to_owned(),
            args: vec![].into(),
        })
        .expect("view call to the ecc method should succeed");
    let QueryResponseKind::CallResult(call_result) = response.kind else {
        panic!("expected CallResult")
    };
    assert_eq!(call_result.result, ECC_METHOD_RESULT);
}
