use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_client::QueryError;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{
    ActionError, ActionErrorKind, FunctionCallError, MethodResolveError, TxExecutionError,
};
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::views::{
    FinalExecutionStatus, QueryRequest, QueryResponse, QueryResponseKind,
};

/// A method listed in the `ecc_only_functions` custom section cannot be called
/// by a `FunctionCall` action, but can be called by a view call. Other methods
/// of the same contract are unaffected.
#[test]
fn test_ecc_only_functions() {
    init_test_logger();

    let user = create_account_id("user");
    let contract = create_account_id("contract");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_accounts([&user, &contract], Balance::from_near(10))
        .build();

    let deploy_tx = env
        .rpc_node()
        .tx_deploy_contract(&contract, near_test_contracts::ecc_only_functions_contract());
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call = |env: &mut TestLoopEnv, method: &str| {
        let tx = env.rpc_node().tx_call(
            &user,
            &contract,
            method,
            vec![],
            Balance::ZERO,
            Gas::from_teragas(30),
        );
        env.rpc_runner().execute_tx(tx, Duration::seconds(5)).unwrap().status
    };

    assert_matches!(call(&mut env, "normal"), FinalExecutionStatus::SuccessValue(_));
    assert_matches!(
        call(&mut env, "ecc"),
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodIsECCOnly
            )),
            ..
        }))
    );

    let view = |method: &str| -> Result<QueryResponse, QueryError> {
        env.rpc_node().runtime_query(QueryRequest::CallFunction {
            account_id: contract.clone(),
            method_name: method.to_owned(),
            args: vec![].into(),
        })
    };
    for method in ["normal", "ecc"] {
        let response = view(method).unwrap_or_else(|err| panic!("view call to {method}: {err}"));
        assert_matches!(response.kind, QueryResponseKind::CallResult(_));
    }
}
