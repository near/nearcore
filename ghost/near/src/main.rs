use std::sync::{Arc, RwLock};

use actix::{Actor, System};
use chrono::Utc;
use kvdb::KeyValueDB;
use log::LevelFilter;

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_network::{NetworkConfig, PeerManagerActor};
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;

fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Debug).init();

    let system = System::new("NEAR");

    // TODO: Replace with rocksdb.
    let store = create_test_store();
    let signer = Arc::new(InMemorySigner::from_seed("test", "test"));

    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));

    let network_config = NetworkConfig::from_seed("test", 25648);
    let network_actor = PeerManagerActor::new(store.clone(), network_config).start();
    let client_actor = ClientActor::new(
        ClientConfig::default(),
        store.clone(),
        runtime,
        network_actor.recipient(),
        Some(signer.into()),
    )
    .unwrap();
    let addr = client_actor.start();

    system.run().unwrap();
}
