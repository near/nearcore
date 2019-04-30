use std::sync::Arc;

use actix::{Actor, System, AsyncContext};
use log::LevelFilter;
use chrono::{DateTime, Utc};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_network::{test_utils::convert_boot_nodes, NetworkConfig, PeerInfo, PeerManagerActor};
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;

use near::{NearConfig, start_with_config};

fn main() {
    // TODO: implement.
//    env_logger::Builder::new().filter_module("tokio_reactor", LevelFilter::Info).filter(None, LevelFilter::Debug).init();
//
//    let genesis_timestamp = Utc::now();
//    let mut near = NearConfig::new(genesis_timestamp.clone(), "test1", 25123);
//
//    let system = System::new("NEAR");
//    start_with_config(near);
//    system.run().unwrap();
}
