use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::Balance;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_ids;

// Demonstrates the most basic multinode test loop setup
// and sends tokens between accounts
#[test]
fn test_cross_shard_token_transfer() {
    init_test_logger();

    let boundary_accounts = create_account_ids(["account01"]).to_vec();
    let user_accounts = create_account_ids(["account0", "account1"]);
    let initial_balance = Balance::from_near(1_000_000);
    let mut env = TestLoopBuilder::new()
        .shard_layout(ShardLayout::multi_shard_custom(boundary_accounts, 1))
        .chunk_producer_per_shard()
        .enable_rpc()
        .add_user_accounts(&user_accounts, initial_balance)
        .build()
        .warmup();

    let sender_account = &user_accounts[0];
    let receiver_account = &user_accounts[1];
    let transfer_amount = Balance::from_near(42);

    let block_hash = env.rpc_node().head().last_block_hash;
    let nonce = 1;
    let tx = SignedTransaction::send_money(
        nonce,
        sender_account.clone(),
        receiver_account.clone(),
        &create_user_test_signer(&sender_account),
        transfer_amount,
        block_hash,
    );
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    // Run for 1 more block for the transfer to be reflected in chunks prev state root.
    env.rpc_runner().run_for_number_of_blocks(1);

    assert_eq!(
        env.rpc_node().view_account_query(&sender_account).unwrap().amount,
        initial_balance.checked_sub(transfer_amount).unwrap()
    );
    assert_eq!(
        env.rpc_node().view_account_query(&receiver_account).unwrap().amount,
        initial_balance.checked_add(transfer_amount).unwrap()
    );

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
