use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, BlockId};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::validators_spec_clients_with_rpc;

/// This example shows how to run jsonrpc queries in TestLoop.
#[test]
fn test_jsonrpc_block_by_height() {
    init_test_logger();

    let accounts: Vec<AccountId> = (0..2).map(|i| format!("account{i}").parse().unwrap()).collect();
    let validators: Vec<&str> = accounts.iter().map(|a| a.as_str()).collect();
    let validators_spec = ValidatorsSpec::desired_roles(&validators, &[]);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let result = env
        .rpc_runner()
        .run_jsonrpc_query(|client| client.block_by_id(BlockId::Height(1)), Duration::seconds(5))
        .unwrap();

    assert_eq!(result.header.height, 1, "expected block height 1, got {}", result.header.height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
