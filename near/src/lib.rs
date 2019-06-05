use std::fs;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, Addr, AsyncContext};
use log::info;

use near_client::{ClientActor, ViewClientActor};
use near_jsonrpc::start_http;
use near_network::PeerManagerActor;
use near_store::create_store;

pub use crate::config::{init_configs, load_config, load_test_config, GenesisConfig, NearConfig, NEAR_TOKEN};
pub use crate::runtime::NightshadeRuntime;

pub mod config;
mod runtime;
mod validator_manager;

const STORE_PATH: &str = "data";

pub fn get_store_path(base_path: &Path) -> String {
    let mut store_path = base_path.to_owned();
    store_path.push(STORE_PATH);
    match fs::canonicalize(store_path.clone()) {
        Ok(path) => info!(target: "near", "Opening store database at {:?}", path),
        _ => {
            info!(target: "near", "Did not find {:?} path, will be creating new store database", store_path)
        }
    };
    store_path.to_str().unwrap().to_owned()
}

pub fn start_with_config(
    home_dir: &Path,
    config: NearConfig,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let store = create_store(&get_store_path(home_dir));
    let runtime =
        Arc::new(NightshadeRuntime::new(home_dir, store.clone(), config.genesis_config.clone()));

    let view_client = ViewClientActor::new(
        store.clone(),
        config.genesis_config.genesis_time.clone(),
        runtime.clone(),
    )
    .unwrap()
    .start();
    let view_client1 = view_client.clone();
    let client = ClientActor::create(move |ctx| {
        let network_actor =
            PeerManagerActor::new(store.clone(), config.network_config, ctx.address().recipient())
                .unwrap()
                .start();

        start_http(config.rpc_config, ctx.address(), view_client1);

        ClientActor::new(
            config.client_config,
            store.clone(),
            config.genesis_config.genesis_time,
            runtime,
            network_actor.recipient(),
            config.block_producer,
        )
        .unwrap()
    });
    (client, view_client)
}
