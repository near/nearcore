use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::Balance;

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
