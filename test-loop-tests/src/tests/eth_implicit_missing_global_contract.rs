use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_ids;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_crypto::{KeyType, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::errors::{
    ActionError, ActionErrorKind, CompilationError, FunctionCallError, TxExecutionError,
};
use near_primitives::types::{Balance, Gas};
use near_primitives::utils::derive_eth_implicit_account_id;
use near_primitives::views::{FinalExecutionStatus, QueryRequest};

/// An ETH-implicit account can be created before its hard-coded global wallet contract has been
/// deployed. Calls to the account must fail with a regular execution error without preventing the
/// rest of the chain from making progress.
///
/// Before the runtime learned to recognize a global contract that was never deployed, such a call panicked in
/// `execute_function_call` on the chunk producer (debug assertions) or made chunk validators reject
/// the state witness with `MissingTrieValue`, stalling the shard.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_eth_implicit_account_with_missing_global_contract_does_not_halt_chain() {
    init_test_logger();

    let [relayer, receiver] = create_account_ids(["relayer", "receiver"]);
    // Include a chunk-validator-only node so the failed call also exercises state witness
    // validation, rather than only execution by the block and chunk producer. The producer holds
    // just over half of the stake, so the chunk is only endorsed if the validator agrees.
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .num_shards(1)
        .add_user_accounts([&relayer, &receiver], Balance::from_near(100))
        .build();

    let eth_secret_key = SecretKey::from_seed(KeyType::SECP256K1, "missing_global_contract");
    let eth_account =
        derive_eth_implicit_account_id(eth_secret_key.public_key().unwrap_as_secp256k1());
    let funded_balance = Balance::from_near(5);

    // Do not deploy the wallet global contract. The transfer still creates an account which
    // points at the chain-specific, hard-coded global contract hash.
    let fund_tx = env.validator().tx_send_money(&relayer, &eth_account, funded_balance);
    env.validator_runner().run_tx(fund_tx, Duration::seconds(5));

    let account = env.validator().view_account_query(&eth_account).unwrap();
    assert_eq!(account.amount, funded_balance);
    assert!(account.global_contract_hash.is_some());

    let global_contract_hash = account.global_contract_hash.unwrap();
    let global_code_query = env
        .validator()
        .runtime_query(QueryRequest::ViewGlobalContractCode { code_hash: global_contract_hash });
    assert!(global_code_query.is_err(), "wallet global contract must be absent in this test");

    // The arguments need not contain a valid signed Ethereum transaction: loading the missing
    // contract fails before rlp_execute can inspect them.
    let call_tx = env.validator().tx_from_actions(
        &relayer,
        &eth_account,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "rlp_execute".into(),
            args: b"{}".to_vec(),
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
    );
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::CodeDoesNotExist { .. }
            )),
            index: _
        }))
    );
    assert_eq!(env.validator().view_account_query(&eth_account).unwrap().amount, funded_balance);

    // A normal transaction after the failed wallet call must execute successfully.
    let receiver_balance = env.validator().view_account_query(&receiver).unwrap().amount;
    let transfer_amount = Balance::from_near(1);
    let transfer_tx = env.validator().tx_send_money(&relayer, &receiver, transfer_amount);
    env.validator_runner().run_tx(transfer_tx, Duration::seconds(5));
    assert_eq!(
        env.validator().view_account_query(&receiver).unwrap().amount,
        receiver_balance.checked_add(transfer_amount).unwrap()
    );

    // Both the producer and the chunk validator must keep accepting and executing blocks.
    let target_height = env.validator().head().height + 10;
    env.validator_runner().run_until_head_height(target_height);
    env.node_runner(1).run_until_executed_height(target_height);
    assert!(env.node(1).head().height >= target_height);
}
