use std::sync::Arc;

use actix::{Actor, System, AsyncContext};
use log::LevelFilter;
use chrono::{DateTime, Utc};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_network::{test_utils::convert_boot_nodes, NetworkConfig, PeerInfo, PeerManagerActor};
use near_store::test_utils::create_test_store;
use primitives::transaction::SignedTransaction;

use near::{NearConfig, start_with_config};

#[test]
fn two_nodes() {
    env_logger::Builder::new().filter_module("tokio_reactor", LevelFilter::Info).filter(None, LevelFilter::Debug).init();

    let genesis_timestamp = Utc::now();
    let mut near1 = NearConfig::new(genesis_timestamp.clone(), "test1", 25123);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = NearConfig::new(genesis_timestamp, "test2", 25124);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");
    start_with_config(near1);
    start_with_config(near2);

    system.run().unwrap();
    // TODO: stop this test.
}
