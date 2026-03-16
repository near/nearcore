use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_client::QueryError;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::Action;
use near_primitives::receipt::{Receipt, VersionedActionReceipt, VersionedReceiptEnum};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, Gas};
use near_primitives::views::FinalExecutionStatus;

/// Tests that a receipt containing a single DeleteAccount action is processed
/// as an instant receipt (in the same block as the parent receipt that created it).
///
/// The test deploys the rs_contract to a user account, then calls `call_promise`
/// to create a batch promise on itself with a single `action_delete_account`.
/// It verifies that the delete account receipt was processed in the same block
/// as the parent `call_promise` receipt.
#[test]
fn test_instant_delete_account() {
    init_test_logger();

    let user_accounts = create_account_ids(["account0", "account1"]);
    let initial_balance = Balance::from_near(1_000_000);
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    let [contract_account, beneficiary] = &user_accounts;
    let contract_signer = create_user_test_signer(contract_account);

    // Deploy rs_contract.
    let nonce = 1;
    let block_hash = env.rpc_node().head().last_block_hash;
    let tx = SignedTransaction::deploy_contract(
        nonce,
        contract_account,
        near_test_contracts::rs_contract().to_vec(),
        &contract_signer,
        block_hash,
    );
    env.rpc_runner().run_tx(tx, Duration::seconds(5));

    // Call `call_promise` on the contract to create a batch promise on itself
    // with a single DeleteAccount action. The contract deletes its own account.
    // This produces a child receipt with only DeleteAccount, which should be instant.
    let nonce = 2;
    let block_hash = env.rpc_node().head().last_block_hash;
    let call_promise_args = serde_json::json!([
        {
            "batch_create": { "account_id": contract_account.as_str() },
            "id": 0
        },
        {
            "action_delete_account": {
                "promise_index": 0,
                "beneficiary_id": beneficiary.as_str()
            },
            "id": 0,
            "return": true
        }
    ]);
    let tx = SignedTransaction::call(
        nonce,
        contract_account.clone(),
        contract_account.clone(),
        &contract_signer,
        Balance::ZERO,
        "call_promise".to_string(),
        serde_json::to_vec(&call_promise_args).unwrap(),
        Gas::from_teragas(300),
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "transaction failed: {:?}",
        outcome.status
    );

    let call_promise_outcome = &outcome.receipts_outcome[0];
    let call_promise_receipt = env.rpc_node().receipt(call_promise_outcome.id);
    assert_matches!(as_action_receipt(&call_promise_receipt).actions(), [Action::FunctionCall(_)]);

    let delete_outcome = &outcome.receipts_outcome[1];
    assert_eq!(
        call_promise_outcome.outcome.receipt_ids,
        vec![delete_outcome.id],
        "call_promise expected to produce exactly the DeleteAccount receipt"
    );
    let delete_receipt = env.rpc_node().receipt(delete_outcome.id);
    assert_matches!(as_action_receipt(&delete_receipt).actions(), [Action::DeleteAccount(_)]);

    // The key assertion: the DeleteAccount receipt was processed in the same
    // block as the parent call_promise receipt, proving it was an instant receipt.
    assert_eq!(
        call_promise_outcome.block_hash, delete_outcome.block_hash,
        "DeleteAccount receipt should be processed in the same block as the parent (instant receipt)"
    );

    // Verify the account no longer exists.
    assert_matches!(
        env.rpc_node().view_account_query(contract_account),
        Err(QueryError::UnknownAccount { .. })
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn as_action_receipt(receipt: &Receipt) -> VersionedActionReceipt<'_> {
    let VersionedReceiptEnum::Action(action_receipt) = receipt.versioned_receipt() else {
        panic!("expected action receipt")
    };
    action_receipt
}
