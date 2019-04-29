use std::sync::{Arc, RwLock};

use actix::{Actor, System};
use chrono::Utc;
use kvdb::KeyValueDB;
use log::LevelFilter;

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_network::{test_utils::convert_boot_nodes, NetworkConfig, PeerInfo, PeerManagerActor};
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;

struct NearConfig {
    client_config: ClientConfig,
    network_config: NetworkConfig,
    block_producer: Option<BlockProducer>,
}

impl NearConfig {
    pub fn new(seed: &str, port: u16) -> Self {
        let signer = Arc::new(InMemorySigner::from_seed(seed, seed));
        NearConfig {
            client_config: ClientConfig::default(),
            network_config: NetworkConfig::from_seed(seed, port),
            block_producer: Some(signer.into()),
        }
    }
}

fn start_with_config(config: NearConfig) {
    // TODO: Replace with rocksdb.
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new_with_authorities(
        store.clone(),
        vec!["test1".to_string(), "test2".to_string()],
    ));

    let network_actor = PeerManagerActor::new(store.clone(), config.network_config).start();
    let client_actor = ClientActor::new(
        config.client_config,
        store.clone(),
        runtime,
        network_actor.recipient(),
        config.block_producer,
    )
    .unwrap();
    let addr = client_actor.start();
}

fn main() {
    env_logger::Builder::new().filter_module("tokio_reactor", LevelFilter::Info).filter(None, LevelFilter::Debug).init();

    let mut near1 = NearConfig::new("test1", 25123);
    near1.network_config.boot_nodes = convert_boot_nodes(vec![("test2", 25124)]);
    let mut near2 = NearConfig::new("test2", 25124);
    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);

    let system = System::new("NEAR");
    start_with_config(near1);
    start_with_config(near2);
    system.run().unwrap();
}
