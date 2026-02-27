use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, BlockId};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;

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

    let block_hash = env.rpc_node().head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver.clone(),
        &create_user_test_signer(&sender),
        transfer_amount,
        block_hash,
    );
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

    let signer = create_user_test_signer(&user);

    // Deploy the test contract.
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.rpc_node().head().last_block_hash,
    );
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call "log_something" which logs "hello".
    let call_tx = SignedTransaction::call(
        2,
        user.clone(),
        user.clone(),
        &signer,
        Balance::ZERO,
        "log_something".to_owned(),
        vec![],
        Gas::from_teragas(300),
        env.rpc_node().head().last_block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();

    assert_eq!(outcome.receipts_outcome[0].outcome.logs, vec!["hello"]);

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
