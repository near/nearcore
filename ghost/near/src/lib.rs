use std::sync::Arc;

use actix::{Actor, Addr, AsyncContext, System};
use chrono::{DateTime, Utc};
use log::LevelFilter;

use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_jsonrpc::JsonRpcServer;
use near_network::{test_utils::convert_boot_nodes, NetworkConfig, PeerInfo, PeerManagerActor};
use near_store::test_utils::create_test_store;
use node_runtime::chain_spec::ChainSpec;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance, ReadableBlsPublicKey, ReadablePublicKey};

pub use crate::config::{GenesisConfig, NearConfig};
use crate::runtime::NightshadeRuntime;

mod config;
mod runtime;

pub fn start_with_config(
    genesis_config: GenesisConfig,
    config: NearConfig,
    block_producer: Option<BlockProducer>,
) -> Addr<ClientActor> {
    // TODO: Replace with rocksdb.
    let store = create_test_store();
    let runtime = Arc::new(NightshadeRuntime::new(store.clone(), genesis_config));

    ClientActor::create(move |ctx| {
        let network_actor =
            PeerManagerActor::new(store.clone(), config.network_config, ctx.address().recipient())
                .start();

        JsonRpcServer::new(config.rpc_server_addr, ctx.address()).start();

        ClientActor::new(
            config.client_config,
            store.clone(),
            runtime,
            network_actor.recipient(),
            block_producer,
        )
        .unwrap()
    })
}
