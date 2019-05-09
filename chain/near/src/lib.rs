use std::fs;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, Addr, AsyncContext};
use log::info;

use near_client::{BlockProducer, ClientActor, ViewClientActor};
use near_jsonrpc::start_http;
use near_network::PeerManagerActor;
use near_store::create_store;

pub use crate::config::{init_configs, load_configs, load_test_configs, GenesisConfig, NearConfig};
pub use crate::runtime::NightshadeRuntime;

pub mod config;
mod runtime;

const STORE_PATH: &str = "data";

fn get_store_path(base_path: &Path) -> String {
    let mut store_path = base_path.to_owned();
    store_path.push(STORE_PATH);
    match fs::canonicalize(store_path.clone()) {
        Ok(path) => info!(target: "near", "Opening store database at {:?}", path),
        _ => info!(target: "near", "Could not resolve {:?} path", store_path),
    };
    store_path.to_str().unwrap().to_owned()
}

pub fn start_with_config(
    home_dir: &Path,
    genesis_config: GenesisConfig,
    config: NearConfig,
    block_producer: Option<BlockProducer>,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let store = create_store(&get_store_path(home_dir));
    let runtime = Arc::new(NightshadeRuntime::new(home_dir, store.clone(), genesis_config.clone()));

    let view_client =
        ViewClientActor::new(store.clone(), genesis_config.genesis_time.clone(), runtime.clone())
            .unwrap()
            .start();
    let view_client1 = view_client.clone();
    let client = ClientActor::create(move |ctx| {
        let network_actor =
            PeerManagerActor::new(store.clone(), config.network_config, ctx.address().recipient())
                .start();

        start_http(config.rpc_config, ctx.address(), view_client1);

        ClientActor::new(
            config.client_config,
            store.clone(),
            genesis_config.genesis_time,
            runtime,
            network_actor.recipient(),
            block_producer,
        )
        .unwrap()
    });
    (client, view_client)
}
