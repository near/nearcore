use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::errors::{
    ActionErrorKind, InvalidTxError, ReceiptValidationError, TxExecutionError,
};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::FinalExecutionStatus;

use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::setups::standard_setup_1;
use crate::test_loop::utils::transactions::{execute_tx, get_shared_block_hash, run_tx};
use crate::test_loop::utils::TGAS;

/// Generating receipts larger than the size limit should cause the transaction to fail.
#[test]
fn test_max_receipt_size() {
    init_test_logger();
    let mut env: TestLoopEnv = standard_setup_1();

    let account0: AccountId = "account0".parse().unwrap();
    let account0_signer = &create_user_test_signer(&account0).into();

    // We can't test receipt limit by submitting large transactions because we hit the transaction size limit
    // before hitting the receipt size limit.
    let large_tx = SignedTransaction::deploy_contract(
        100,
        &account0,
        vec![0u8; 2_000_000],
        account0_signer,
        get_shared_block_hash(&env.datas, &env.test_loop),
    );
    let large_tx_exec_res =
        execute_tx(&mut env.test_loop, large_tx, &env.datas, Duration::seconds(5));
    assert_matches!(large_tx_exec_res, Err(InvalidTxError::TransactionSizeExceeded { .. }));

    // Let's test it by running a contract that generates a large receipt.
    let deploy_contract_tx = SignedTransaction::deploy_contract(
        101,
        &account0,
        near_test_contracts::rs_contract().into(),
        &account0_signer,
        get_shared_block_hash(&env.datas, &env.test_loop),
    );
    run_tx(&mut env.test_loop, deploy_contract_tx, &env.datas, Duration::seconds(5));

    // Calling generate_large_receipt({"account_id": "account0", "method_name": "noop", "total_args_size": 3000000})
    // will generate a receipt that has ~3_000_000 bytes. It'll be a single receipt with multiple FunctionCall actions.
    // 3MB is still under the limit, so this should succeed.
    let large_receipt_tx = SignedTransaction::call(
        102,
        account0.clone(),
        account0.clone(),
        &account0_signer,
        0,
        "generate_large_receipt".into(),
        r#"{"account_id": "account0", "method_name": "noop", "total_args_size": 3000000}"#.into(),
        300 * TGAS,
        get_shared_block_hash(&env.datas, &env.test_loop),
    );
    run_tx(&mut env.test_loop, large_receipt_tx, &env.datas, Duration::seconds(5));

    // Generating a receipt that is 5 MB should fail, it's above the receipt size limit.
    let too_large_receipt_tx = SignedTransaction::call(
        103,
        account0.clone(),
        account0.clone(),
        &account0_signer,
        0,
        "generate_large_receipt".into(),
        r#"{"account_id": "account0", "method_name": "noop", "total_args_size": 5000000}"#.into(),
        300 * TGAS,
        get_shared_block_hash(&env.datas, &env.test_loop),
    );
    let too_large_receipt_tx_exec_res =
        execute_tx(&mut env.test_loop, too_large_receipt_tx, &env.datas, Duration::seconds(5))
            .unwrap();

    match too_large_receipt_tx_exec_res.status {
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(action_error)) => {
            match action_error.kind {
                ActionErrorKind::NewReceiptValidationError(
                    ReceiptValidationError::ReceiptSizeExceeded { .. },
                ) => {
                    // Ok, got the expected error
                }
                _ => panic!("Expected ReceiptSizeExceeded error, got: {:?}", action_error),
            }
        }
        _ => panic!(
            "Expected FinalExecutionStatus::Failure, got: {:?}",
            too_large_receipt_tx_exec_res
        ),
    };

    // The runtime shouldn't die when it encounters a large receipt.
    // Make sure that everything still works.

    // Calling sum_n(5) should return 10.
    // 1 + 2 + 3 + 4 = 10
    let sum_4_tx = SignedTransaction::call(
        104,
        account0.clone(),
        account0.clone(),
        &account0_signer,
        0,
        "sum_n".into(),
        5_u64.to_le_bytes().to_vec(),
        300 * TGAS,
        get_shared_block_hash(&env.datas, &env.test_loop),
    );
    let sum_4_res = run_tx(&mut env.test_loop, sum_4_tx, &env.datas, Duration::seconds(5));
    assert_eq!(sum_4_res, 10u64.to_le_bytes().to_vec());

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
