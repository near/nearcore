use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::version::MIN_SUPPORTED_PROTOCOL_VERSION;
use near_primitives::views::FinalExecutionStatus;

/// End-to-end: deploy a contract that calls `chain_id` and assert it returns the
/// genesis chain id (`test` for the default test-loop genesis).
#[test]
fn test_chain_id_returns_chain_id() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let deploy_tx =
        env.rpc_node().tx_deploy_contract(&user, near_test_contracts::rs_contract().to_vec());
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "ext_chain_id",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    match &outcome.status {
        FinalExecutionStatus::SuccessValue(bytes) => {
            assert_eq!(bytes, b"test", "expected the genesis chain id");
        }
        other => panic!("expected SuccessValue, got {other:?}"),
    }
}

/// Before the protocol version that activates `chain_id`, a contract that imports
/// `env.chain_id` must fail at call time (the import can't be linked against a
/// runtime that doesn't expose it).
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_chain_id_pre_activation_call_fails() {
    init_test_logger();

    // chain_id_host_fn is turned on at protocol version 85 (see 85.yaml).
    let pre_activation_pv = 84;
    // A node can only be built at pre_activation_pv while it stays at or above the
    // minimum supported version. Once that floor rises past it, chain_id is active
    // everywhere: delete this test and drop the `#[cfg(feature = "latest_protocol")]`
    // gate on the chain_id import in test-contract-rs.
    assert!(
        MIN_SUPPORTED_PROTOCOL_VERSION <= pre_activation_pv,
        "chain_id is active on all supported protocol versions; delete this test and \
         drop the latest_protocol gate on the chain_id import in test-contract-rs"
    );
    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(pre_activation_pv)
        .add_user_account(&user, Balance::from_near(100))
        .build();

    let deploy_tx =
        env.rpc_node().tx_deploy_contract(&user, near_test_contracts::rs_contract().to_vec());
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "ext_chain_id",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    // The exact error variant (LinkError vs CompilationError) is VM-specific and
    // protocol-dependent, so just assert that the function call failed.
    assert_matches!(
        &outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(_),
            ..
        }))
    );
}
