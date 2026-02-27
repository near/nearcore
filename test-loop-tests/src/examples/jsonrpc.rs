use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockId;

use crate::setup::builder::TestLoopBuilder;

/// This example shows how to run jsonrpc queries in TestLoop.
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
