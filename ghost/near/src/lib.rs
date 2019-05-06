use std::sync::Arc;

use actix::{Actor, Addr, AsyncContext};

use near_client::{BlockProducer, ClientActor};
use near_jsonrpc::start_http;
use near_network::PeerManagerActor;
use near_store::test_utils::create_test_store;
use node_runtime::chain_spec::ChainSpec;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance, ReadableBlsPublicKey, ReadablePublicKey};

pub use crate::config::{GenesisConfig, NearConfig};
pub use crate::runtime::NightshadeRuntime;

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

        start_http(config.rpc_server_addr, ctx.address());

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
