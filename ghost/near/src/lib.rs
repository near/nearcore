use std::sync::Arc;

use actix::{Actor, Addr, AsyncContext, System};
use chrono::{DateTime, Utc};
use log::LevelFilter;

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_jsonrpc::JsonRpcServer;
use near_network::{test_utils::convert_boot_nodes, NetworkConfig, PeerInfo, PeerManagerActor};
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use std::net::SocketAddr;

pub struct NearConfig {
    pub client_config: ClientConfig,
    pub network_config: NetworkConfig,
    pub block_producer: Option<BlockProducer>,
    pub rpc_server_addr: SocketAddr,
}

impl NearConfig {
    pub fn new(genesis_timestamp: DateTime<Utc>, seed: &str, port: u16) -> Self {
        let signer = Arc::new(InMemorySigner::from_seed(seed, seed));
        NearConfig {
            client_config: ClientConfig::new(genesis_timestamp),
            network_config: NetworkConfig::from_seed(seed, port),
            block_producer: Some(signer.into()),
            rpc_server_addr: format!("127.0.0.1:{}", port + 100).parse().unwrap(),
        }
    }
}

pub fn start_with_config(config: NearConfig) -> Addr<ClientActor> {
    // TODO: Replace with rocksdb.
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new_with_authorities(
        store.clone(),
        vec!["test1".to_string(), "test2".to_string()],
    ));

    ClientActor::create(move |ctx| {
        let network_actor =
            PeerManagerActor::new(store.clone(), config.network_config, ctx.address().recipient())
                .start();

        JsonRpcServer::new(config.rpc_server_addr, ctx.address());

        ClientActor::new(
            config.client_config,
            store.clone(),
            runtime,
            network_actor.recipient(),
            config.block_producer,
        )
        .unwrap()
    })
}
