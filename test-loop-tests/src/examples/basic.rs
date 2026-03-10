use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_client::QueryError;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::types::{Balance, BlockId};

/// Demonstrates sending tokens between two user accounts.
#[test]
fn test_basic_token_transfer() {
    init_test_logger();

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");
    let initial_balance = Balance::from_near(1_000);
    let transfer_amount = Balance::from_near(42);

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_accounts([&sender, &receiver], initial_balance)
        .build()
        .warmup();

    let tx = env.rpc_node().tx_send_money(&sender, &receiver, transfer_amount);
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    // Run for 1 more block for the transfer to be reflected in chunks prev state root.
    env.rpc_runner().run_for_number_of_blocks(1);

    assert_eq!(
        env.rpc_node().query_balance(&sender),
        initial_balance.checked_sub(transfer_amount).unwrap()
    );
    assert_eq!(
        env.rpc_node().query_balance(&receiver),
        initial_balance.checked_add(transfer_amount).unwrap()
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Demonstrates deploying a contract and calling a method on it.
#[test]
fn test_deploy_and_call_contract() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(10))
        .build()
        .warmup();

    // Deploy the test contract.
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call "log_something" which logs "hello".
    let call_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "log_something",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();

    assert_eq!(outcome.receipts_outcome[0].outcome.logs, vec!["hello"]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Demonstrates creating and deleting an account.
#[test]
fn test_create_and_delete_account() {
    init_test_logger();

    let originator = create_account_id("originator");
    // The new account needs to be a sub-accounts of the originator.
    let new_account = create_account_id("new_account.originator");
    let initial_balance = Balance::from_near(100);
    let new_account_balance = Balance::from_near(10);

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&originator, initial_balance)
        .build()
        .warmup();

    // Create a new account.
    let tx = env.rpc_node().tx_create_account(&originator, &new_account, new_account_balance);
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    assert_eq!(env.rpc_node().query_balance(&new_account), new_account_balance);

    // Delete the account, sending remaining balance to the originator.
    let tx = env.rpc_node().tx_delete_account(&new_account, &originator);
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Verify the account no longer exists.
    assert_matches!(
        env.rpc_node().view_account_query(&new_account),
        Err(QueryError::UnknownAccount { .. })
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Demonstrates running jsonrpc queries in TestLoop.
#[test]
fn test_jsonrpc_block_by_height() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().build().warmup();

    let result = env
        .rpc_runner()
        .run_jsonrpc_query(|client| client.block_by_id(BlockId::Height(1)), Duration::seconds(5))
        .unwrap();

    assert_eq!(result.header.height, 1, "expected block height 1, got {}", result.header.height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
