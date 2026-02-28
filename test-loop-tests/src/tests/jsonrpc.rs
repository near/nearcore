use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::serialize::to_base64;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockId};

use crate::setup::builder::TestLoopBuilder;

/// Get a block by height using jsonrpc
#[test]
fn test_rpc_block_by_height() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().epoch_length(10).build().warmup();

    let result = env
        .rpc_runner()
        .run_jsonrpc_query(|client| client.block_by_id(BlockId::Height(1)), Duration::seconds(5))
        .unwrap();

    assert_eq!(result.header.height, 1, "expected block height 1, got {}", result.header.height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Submit a simple transfer transaction using jsonrpc "broadcast_tx_commit" and wait for it to finish.
#[test]
fn test_rpc_broadcast_tx_commit_transfer() {
    init_test_logger();

    let validator_account: AccountId = "validator0".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .epoch_length(10)
        .add_user_account(&validator_account, Balance::from_near(1_000))
        .build()
        .warmup();

    let tx = SignedTransaction::send_money(
        1,
        validator_account.clone(),
        validator_account.clone(),
        &create_user_test_signer(&validator_account),
        Balance::from_near(1),
        env.rpc_node().head().last_block_hash,
    );
    let tx_bytes = borsh::to_vec(&tx).unwrap();
    let tx_base64 = to_base64(&tx_bytes);

    let result = env
        .rpc_runner()
        .run_jsonrpc_query(|client| client.broadcast_tx_commit(tx_base64), Duration::seconds(10))
        .unwrap();

    // Extract the execution outcome.
    let outcome =
        result.final_execution_outcome.expect("missing final_execution_outcome").into_outcome();

    // Verify the transaction succeeded.
    assert!(
        matches!(outcome.status, near_primitives::views::FinalExecutionStatus::SuccessValue(_)),
        "expected SuccessValue in status, got {:?}",
        outcome.status
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
