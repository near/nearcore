use near_jsonrpc::client::new_http_client;
use near_o11y::testonly::init_test_logger;
use near_time::Clock;

use near_jsonrpc_tests as test_utils;
use near_store::db::RocksDB;

/// Retrieve client status via HTTP GET.
#[tokio::test]
async fn test_status() {
    init_test_logger();

    let actor_system = near_async::ActorSystem::new();
    let (_view_client_addr, addr, _runtime_temp_dir) = test_utils::start_all(
        Clock::real(),
        actor_system.clone(),
        test_utils::NodeType::NonValidator,
    );

    let client = new_http_client(&format!("http://{}", addr));
    let res = client.status().await.unwrap();
    assert_eq!(res.chain_id, "unittest");
    assert_eq!(res.sync_info.latest_block_height, 0);
    assert_eq!(res.sync_info.syncing, false);
    actor_system.stop();
    RocksDB::block_until_all_instances_are_dropped();
}
