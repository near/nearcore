use actix::{Actor, System};
use chrono::Utc;

use futures::future::Future;
use near::{start_with_config, NearConfig};
use near_client::GetBlock;
use near_network::test_utils::{convert_boot_nodes, WaitOrTimeout};
use primitives::test_utils::init_test_logger;
use primitives::transaction::SignedTransaction;

#[test]
fn sync_nodes() {
    init_test_logger();

    let genesis_timestamp = Utc::now();
    let mut near1 = NearConfig::new(genesis_timestamp.clone(), "test1", 25123);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = NearConfig::new(genesis_timestamp, "test2", 25124);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");
    let client1 = start_with_config(near1);
}