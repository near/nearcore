use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
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
