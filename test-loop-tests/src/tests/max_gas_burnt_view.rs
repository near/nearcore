use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_client::QueryError;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::FunctionCallError;
use near_primitives::gas::Gas;
use near_primitives::types::Balance;
use near_primitives::views::{QueryRequest, QueryResponse, QueryResponseKind};

/// Spins up two validators with different `max_gas_burnt_view` client
/// configuration, deploys a smart contract and calls a view function against
/// both nodes, expecting the one with the low `max_gas_burnt_view` limit to
/// fail with `GasLimitExceeded` for expensive calls while still succeeding for
/// cheap ones.
#[test]
fn test_max_gas_burnt_view() {
    init_test_logger();

    let low_gas_limit = Gas::from_gigagas(200);

    let contract_account = create_account_id("contract");
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .add_user_account(&contract_account, Balance::from_near(10))
        // Apply the low view-gas limit to node 1 only; node 0 keeps the default.
        .config_modifier(move |config, idx| {
            if idx == 1 {
                config.max_gas_burnt_view = Some(low_gas_limit);
            }
        })
        .build();

    let deploy_tx = env.node(0).tx_deploy_test_contract(&contract_account);
    env.node_runner(0).run_tx(deploy_tx, Duration::seconds(5));

    // `fibonacci` takes a single byte argument and returns the result as
    // little-endian u64 bytes.
    let call_fib = |node_index: usize, n: u8| -> Result<QueryResponse, QueryError> {
        env.node(node_index).runtime_query(QueryRequest::CallFunction {
            account_id: contract_account.clone(),
            method_name: "fibonacci".to_owned(),
            args: vec![n].into(),
        })
    };
    let decode_result = |result: &[u8]| u64::from_le_bytes(result.try_into().unwrap());

    // Node 0 uses the default (high) limit: fibonacci(25) succeeds.
    let response = call_fib(0, 25).unwrap();
    let QueryResponseKind::CallResult(call_result) = response.kind else {
        panic!("expected CallResult")
    };
    assert_eq!(decode_result(&call_result.result), 75025);

    // Node 1 has the low limit: fibonacci(25) exceeds it.
    let error = call_fib(1, 25).unwrap_err();
    let QueryError::ContractExecutionError { error, vm_error, .. } = &error else {
        panic!("expected ContractExecutionError, got: {error:?}")
    };
    assert_matches!(error, FunctionCallError::ExecutionError(_));
    assert!(
        vm_error.contains("HostError(GasLimitExceeded)"),
        "expected GasLimitExceeded, got: {vm_error}"
    );

    // Node 1 still succeeds for cheap arguments.
    let response = call_fib(1, 5).unwrap();
    let QueryResponseKind::CallResult(call_result) = response.kind else {
        panic!("expected CallResult")
    };
    assert_eq!(decode_result(&call_result.result), 5);
}
