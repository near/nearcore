use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_jsonrpc_client_internal::new_client;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockId;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::validators_spec_clients_with_rpc;
use crate::utils::node::TestLoopNode;

/// Retrieve block by height via JSON-RPC.
#[tokio::test]
async fn test_block_by_height() {
    init_test_logger();

    let validators_spec = ValidatorsSpec::desired_roles(&["test1"], &[]);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder().validators_spec(validators_spec).build();
    let env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .with_jsonrpc()
        .build()
        .warmup();

    let rpc_node = TestLoopNode::rpc(&env.node_datas);
    let jsonrpc_client = new_client(rpc_node.data().jsonrpc_addr());

    let head = rpc_node.head(env.test_loop_data());

    let block = jsonrpc_client.block_by_id(BlockId::Height(head.height)).await.unwrap();
    assert_eq!(block.header.hash, head.last_block_hash);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
