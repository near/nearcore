use std::fs;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, Addr};
use log::{error, info};
use tracing::trace;

use near_chain::ChainGenesis;
use near_client::{ClientActor, ViewClientActor};
use near_jsonrpc::start_http;
use near_network::{NetworkRecipient, PeerManagerActor};
use near_store::{create_store, get_store_version, set_store_version, Store};
use near_telemetry::TelemetryActor;

pub use crate::config::{init_configs, load_config, load_test_config, NearConfig, NEAR_BASE};
pub use crate::runtime::NightshadeRuntime;

pub mod config;
pub mod genesis_validate;
mod runtime;
mod shard_tracker;

const STORE_PATH: &str = "data";

pub fn store_path_exists<P: AsRef<Path>>(path: P) -> bool {
    fs::canonicalize(path).is_ok()
}

pub fn get_store_path(base_path: &Path) -> String {
    let mut store_path = base_path.to_owned();
    store_path.push(STORE_PATH);
    if store_path_exists(&store_path) {
        info!(target: "near", "Opening store database at {:?}", store_path);
    } else {
        info!(target: "near", "Did not find {:?} path, will be creating new store database", store_path);
    }
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

/// Function checks current version of the database and applies migrations to the database.
pub fn apply_store_migrations(path: &String) {
    let db_version = get_store_version(path);
    if db_version > near_primitives::version::DB_VERSION {
        error!(target: "near", "DB version {} is created by a newer version of neard, please update neard or delete data", db_version);
        std::process::exit(1);
    }
    if db_version == near_primitives::version::DB_VERSION {
        return;
    }
    // Add migrations here based on `db_version`.
    if db_version == 1 {
        // version 1 => 2: add gc column
        // Does not need to do anything since open db with option `create_missing_column_families`
        // Nevertheless need to bump db version, because db_version 1 binary can't open db_version 2 db
        info!(target: "near", "Migrate DB from version 1 to 2");
        let store = create_store(&path);
        set_store_version(&store);
    }
}

pub fn init_and_migrate_store(home_dir: &Path) -> Arc<Store> {
    let path = get_store_path(home_dir);
    let store_exists = store_path_exists(&path);
    if store_exists {
        apply_store_migrations(&path);
    }
    let store = create_store(&path);
    if !store_exists {
        set_store_version(&store);
    }
    store
}

pub fn start_with_config(
    home_dir: &Path,
    config: NearConfig,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let store = init_and_migrate_store(home_dir);
    near_actix_utils::init_stop_on_panic();
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
        chain_genesis,
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
        store,
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
