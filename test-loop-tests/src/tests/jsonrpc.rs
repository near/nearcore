use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::serialize::to_base64;
use near_primitives::types::{AccountId, Balance, BlockId};

/// Get a block by height using jsonrpc
#[test]
fn test_rpc_block_by_height() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().epoch_length(10).build();

    let result = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.block_by_id(BlockId::Height(1)),
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(result.header.height, 1, "expected block height 1, got {}", result.header.height);
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
        .build();

    let tx =
        env.rpc_node().tx_send_money(&validator_account, &validator_account, Balance::from_near(1));
    let tx_bytes = borsh::to_vec(&tx).unwrap();
    let tx_base64 = to_base64(&tx_bytes);

    let result = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.broadcast_tx_commit(tx_base64),
            Duration::seconds(10),
        )
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
}
