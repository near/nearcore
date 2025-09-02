use near_jsonrpc::client::new_http_client;
use near_jsonrpc_tests::{NodeType, create_test_setup_with_node_type};

#[actix::test]
async fn test_status() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);

    // Use the unified JSON-RPC client that works with both HTTP and TestServer
    let client = new_http_client(&setup.server_addr);
    let status_response = client.status().await.expect("Failed to get status");

    assert_eq!(status_response.chain_id, "unittest");
    assert_eq!(status_response.sync_info.syncing, false);
}
