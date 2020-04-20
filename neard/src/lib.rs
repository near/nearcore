use std::fs;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, Addr};
use log::info;

use near_chain::ChainGenesis;
use near_client::{ClientActor, ViewClientActor};
use near_jsonrpc::start_http;
use near_network::{NetworkRecipient, PeerManagerActor};
use near_store::create_store;
use near_telemetry::TelemetryActor;
use tracing::trace;

pub use crate::config::{init_configs, load_config, load_test_config, NearConfig, NEAR_BASE};
pub use crate::runtime::NightshadeRuntime;

pub mod config;
pub mod genesis_validate;
mod runtime;
mod shard_tracker;

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

pub fn get_default_home() -> String {
    match std::env::var("NEAR_HOME") {
        Ok(home) => home,
        Err(_) => match dirs::home_dir() {
            Some(mut home) => {
                home.push(".near");
                home.as_path().to_str().unwrap().to_string()
            }
            None => "".to_string(),
        },
    }
}

pub fn start_with_config(
    home_dir: &Path,
    config: NearConfig,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let store = create_store(&get_store_path(home_dir));
    near_primitives::test_utils::init_stop_on_panic();
    let runtime = Arc::new(NightshadeRuntime::new(
        home_dir,
        Arc::clone(&store),
        Arc::clone(&config.genesis),
        config.client_config.tracked_accounts.clone(),
        config.client_config.tracked_shards.clone(),
    ));

    let telemetry = TelemetryActor::new(config.telemetry_config.clone()).start();
    let chain_genesis = ChainGenesis::from(&config.genesis);

    let node_id = config.network_config.public_key.clone().into();
    let network_adapter = Arc::new(NetworkRecipient::new());
    let view_client = ViewClientActor::new(
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        &chain_genesis,
        runtime.clone(),
        network_adapter.clone(),
        config.client_config.clone(),
    )
    .unwrap()
    .start();

    let client_actor = ClientActor::new(
        config.client_config,
        chain_genesis.clone(),
        runtime,
        node_id,
        network_adapter.clone(),
        config.validator_signer,
        telemetry,
        true,
    )
    .unwrap()
    .start();
    start_http(
        config.rpc_config,
        Arc::clone(&config.genesis),
        client_actor.clone(),
        view_client.clone(),
    );

    config.network_config.verify();

    let network_actor = PeerManagerActor::new(
        store.clone(),
        config.network_config,
        client_actor.clone().recipient(),
        view_client.clone().recipient(),
    )
    .unwrap()
    .start();

    network_adapter.set_recipient(network_actor.recipient());

    trace!(target: "diagnostic", key="log", "Starting NEAR node with diagnostic activated");

    (client_actor, view_client)
}
