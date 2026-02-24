use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{ActionErrorKind, FunctionCallError, TxExecutionError};
use near_primitives::gas::Gas;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, Nonce};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::FinalExecutionStatus;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_id, create_validators_spec, validators_spec_clients_with_rpc,
};

/// Test that verifies gas limit behavior: burning 998 TGas succeeds while
/// burning 1001 TGas results in a failed receipt.
#[test]
fn test_gas_limit() {
    init_test_logger();

    let user_account = create_account_id("user");
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout_single_shard()
        .validators_spec(validators_spec)
        .add_user_account_simple(user_account.clone(), Balance::from_near(10))
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    // Assert that the gas limits are set to 1 PGas.
    let one_petagas = Gas::from_teragas(1000);
    let runtime_config =
        env.rpc_node().client().runtime_adapter.get_runtime_config(PROTOCOL_VERSION);
    let limit_config = &runtime_config.wasm_config.limit_config;
    assert_eq!(limit_config.max_gas_burnt, one_petagas);
    assert_eq!(limit_config.max_total_prepaid_gas, one_petagas);

    // Deploy the test contract.
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &create_user_test_signer(&user_account),
        env.rpc_node().head().last_block_hash,
    );
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    let prepaid_gas = Gas::from_teragas(1000);

    // Burning 998 TGas should succeed.
    let (status, gas_burnt) =
        burn_gas(&mut env, &user_account, 2, Gas::from_teragas(998), prepaid_gas);
    assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
    assert!(gas_burnt < Gas::from_teragas(1000));

    // Burning 1001 TGas should fail with gas exceeded error.
    let (status, gas_burnt) =
        burn_gas(&mut env, &user_account, 3, Gas::from_teragas(1001), prepaid_gas);
    assert_matches!(
        status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ref err))
            if matches!(
                err.kind,
                ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(ref msg))
                    if msg == "Exceeded the maximum amount of gas allowed to burn per contract."
            )
    );
    assert!(gas_burnt > Gas::from_teragas(1000));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn burn_gas(
    env: &mut TestLoopEnv,
    user_account: &near_primitives::types::AccountId,
    nonce: Nonce,
    gas_to_burn: Gas,
    prepaid_gas: Gas,
) -> (FinalExecutionStatus, Gas) {
    let tx = SignedTransaction::call(
        nonce,
        user_account.clone(),
        user_account.clone(),
        &create_user_test_signer(user_account),
        Balance::ZERO,
        "burn_gas_raw".to_owned(),
        gas_to_burn.as_gas().to_le_bytes().to_vec(),
        prepaid_gas,
        env.rpc_node().head().last_block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(5)).unwrap();
    let gas_burnt = outcome.receipts_outcome[0].outcome.gas_burnt;
    (outcome.status, gas_burnt)
}
