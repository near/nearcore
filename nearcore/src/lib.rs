use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use actix::{Actor, Addr, Arbiter};
use actix_rt::ArbiterHandle;
use actix_web;
#[cfg(feature = "performance_stats")]
use near_rust_allocator_proxy::allocator::reset_memory_usage_max;
use tracing::{error, info, trace};

use near_chain::ChainGenesis;
#[cfg(feature = "test_features")]
use near_client::AdversarialControls;
use near_client::{start_client, start_view_client, ClientActor, ViewClientActor};
use near_network::{NetworkRecipient, PeerManagerActor};
#[cfg(feature = "rosetta_rpc")]
use near_rosetta_rpc::start_rosetta_rpc;
#[cfg(feature = "protocol_feature_block_header_v3")]
use near_store::migrations::migrate_18_to_new_validator_stake;
use near_store::migrations::{
    fill_col_outcomes_by_hash, fill_col_transaction_refcount, get_store_version, migrate_10_to_11,
    migrate_11_to_12, migrate_13_to_14, migrate_14_to_15, migrate_17_to_18, migrate_21_to_22,
    migrate_25_to_26, migrate_6_to_7, migrate_7_to_8, migrate_8_to_9, migrate_9_to_10,
    set_store_version,
};
use near_store::migrations::{migrate_20_to_21, migrate_26_to_27};
use near_store::{create_store, Store};
use near_telemetry::TelemetryActor;

pub use crate::config::{init_configs, load_config, load_test_config, NearConfig, NEAR_BASE};
use crate::migrations::{
    migrate_12_to_13, migrate_18_to_19, migrate_19_to_20, migrate_22_to_23, migrate_23_to_24,
    migrate_24_to_25,
};
pub use crate::runtime::NightshadeRuntime;
pub use crate::shard_tracker::TrackedConfig;
use near_network::test_utils::make_ibf_routing_pool;

pub mod append_only_map;
pub mod config;
pub mod migrations;
mod runtime;
mod shard_tracker;

const STORE_PATH: &str = "data";

pub fn store_path_exists<P: AsRef<Path>>(path: P) -> bool {
    fs::canonicalize(path).is_ok()
}

pub fn get_store_path(base_path: &Path) -> PathBuf {
    let mut store_path = base_path.to_owned();
    store_path.push(STORE_PATH);
    if store_path_exists(&store_path) {
        info!(target: "near", "Opening store database at {:?}", store_path);
    } else {
        info!(target: "near", "Did not find {:?} path, will be creating new store database", store_path);
    }
    store_path
}

pub fn get_default_home() -> PathBuf {
    if let Ok(near_home) = std::env::var("NEAR_HOME") {
        return near_home.into();
    }

    if let Some(mut home) = dirs::home_dir() {
        home.push(".near");
        return home;
    }

    PathBuf::default()
}

/// Function checks current version of the database and applies migrations to the database.
pub fn apply_store_migrations(path: &Path, near_config: &NearConfig) {
    let db_version = get_store_version(path);
    if db_version > near_primitives::version::DB_VERSION {
        error!(target: "near", "DB version {} is created by a newer version of neard, please update neard or delete data", db_version);
        std::process::exit(1);
    }
    if db_version == near_primitives::version::DB_VERSION {
        return;
    }

    // Add migrations here based on `db_version`.
    if db_version <= 1 {
        // version 1 => 2: add gc column
        // Does not need to do anything since open db with option `create_missing_column_families`
        // Nevertheless need to bump db version, because db_version 1 binary can't open db_version 2 db
        info!(target: "near", "Migrate DB from version 1 to 2");
        let store = create_store(&path);
        set_store_version(&store, 2);
    }
    if db_version <= 2 {
        // version 2 => 3: add ColOutcomesByBlockHash + rename LastComponentNonce -> ColLastComponentNonce
        // The column number is the same, so we don't need additional updates
        info!(target: "near", "Migrate DB from version 2 to 3");
        let store = create_store(&path);
        fill_col_outcomes_by_hash(&store);
        set_store_version(&store, 3);
    }
    if db_version <= 3 {
        // version 3 => 4: add ColTransactionRefCount
        info!(target: "near", "Migrate DB from version 3 to 4");
        let store = create_store(&path);
        fill_col_transaction_refcount(&store);
        set_store_version(&store, 4);
    }
    if db_version <= 4 {
        info!(target: "near", "Migrate DB from version 4 to 5");
        // version 4 => 5: add ColProcessedBlockHeights
        // we don't need to backfill the old heights since at worst we will just process some heights
        // again.
        let store = create_store(&path);
        set_store_version(&store, 5);
    }
    if db_version <= 5 {
        info!(target: "near", "Migrate DB from version 5 to 6");
        // version 5 => 6: add merge operator to ColState
        // we don't have merge records before so old storage works
        let store = create_store(&path);
        set_store_version(&store, 6);
    }
    if db_version <= 6 {
        info!(target: "near", "Migrate DB from version 6 to 7");
        // version 6 => 7:
        // - make ColState use 8 bytes for refcount (change to merge operator)
        // - move ColTransactionRefCount into ColTransactions
        // - make ColReceiptIdToShardId refcounted
        migrate_6_to_7(path);
    }
    if db_version <= 7 {
        info!(target: "near", "Migrate DB from version 7 to 8");
        // version 7 => 8:
        // delete values in column `StateColParts`
        migrate_7_to_8(path);
    }
    if db_version <= 8 {
        info!(target: "near", "Migrate DB from version 8 to 9");
        // version 8 => 9:
        // Repair `ColTransactions`, `ColReceiptIdToShardId`
        migrate_8_to_9(path);
    }
    if db_version <= 9 {
        info!(target: "near", "Migrate DB from version 9 to 10");
        // version 9 => 10;
        // populate partial encoded chunks for chunks that exist in storage
        migrate_9_to_10(path, near_config.client_config.archive);
    }
    if db_version <= 10 {
        info!(target: "near", "Migrate DB from version 10 to 11");
        // version 10 => 11
        // Add final head
        migrate_10_to_11(path);
    }
    if db_version <= 11 {
        info!(target: "near", "Migrate DB from version 11 to 12");
        // version 11 => 12;
        // populate ColReceipts with existing receipts
        migrate_11_to_12(path);
    }
    if db_version <= 12 {
        info!(target: "near", "Migrate DB from version 12 to 13");
        // version 12 => 13;
        // migrate ColTransactionResult to fix the inconsistencies there
        migrate_12_to_13(path, near_config);
    }
    if db_version <= 13 {
        info!(target: "near", "Migrate DB from version 13 to 14");
        // version 13 => 14;
        // store versioned enums for shard chunks
        migrate_13_to_14(path);
    }
    if db_version <= 14 {
        info!(target: "near", "Migrate DB from version 14 to 15");
        // version 14 => 15;
        // Change ColOutcomesByBlockHash to be ordered within each shard
        migrate_14_to_15(path);
    }
    if db_version <= 15 {
        info!(target: "near", "Migrate DB from version 15 to 16");
        // version 15 => 16: add column for compiled contracts
        let store = create_store(&path);
        set_store_version(&store, 16);
    }
    if db_version <= 16 {
        info!(target: "near", "Migrate DB from version 16 to 17");
        // version 16 => 17: add column for storing epoch validator info
        let store = create_store(&path);
        set_store_version(&store, 17);
    }
    if db_version <= 17 {
        info!(target: "near", "Migrate DB from version 17 to 18");
        // version 17 => 18: add `hash` to `BlockInfo` and ColHeaderHashesByHeight
        migrate_17_to_18(&path);
    }
    if db_version <= 18 {
        info!(target: "near", "Migrate DB from version 18 to 19");
        // version 18 => 19: populate ColEpochValidatorInfo for archival nodes
        migrate_18_to_19(&path, near_config);
    }
    if db_version <= 19 {
        info!(target: "near", "Migrate DB from version 19 to 20");
        // version 19 => 20: fix execution outcome
        migrate_19_to_20(&path, &near_config);
    }
    if db_version <= 20 {
        info!(target: "near", "Migrate DB from version 20 to 21");
        // version 20 => 21: delete genesis json hash due to change in Genesis::json_hash function
        migrate_20_to_21(&path);
    }
    if db_version <= 21 {
        info!(target: "near", "Migrate DB from version 21 to 22");
        // version 21 => 22: rectify inflation: add `timestamp` to `BlockInfo`
        migrate_21_to_22(&path);
    }
    if db_version <= 22 {
        info!(target: "near", "Migrate DB from version 22 to 23");
        migrate_22_to_23(&path, &near_config);
    }
    if db_version <= 23 {
        info!(target: "near", "Migrate DB from version 23 to 24");
        migrate_23_to_24(&path, &near_config);
    }
    if db_version <= 24 {
        info!(target: "near", "Migrate DB from version 24 to 25");
        migrate_24_to_25(&path);
    }
    if db_version <= 25 {
        info!(target: "near", "Migrate DB from version 25 to 26");
        migrate_25_to_26(&path);
    }
    if db_version <= 26 {
        info!(target: "near", "Migrate DB from version 26 to 27");
        migrate_26_to_27(&path, near_config.client_config.archive);
    }
    if db_version <= 27 {
        // version 27 => 28: add ColStateChangesForSplitStates
        // Does not need to do anything since open db with option `create_missing_column_families`
        // Nevertheless need to bump db version, because db_version 1 binary can't open db_version 2 db
        info!(target: "near", "Migrate DB from version 27 to 28");
        let store = create_store(&path);
        set_store_version(&store, 28);
    }
    #[cfg(feature = "nightly_protocol")]
    {
        let store = create_store(&path);

        #[cfg(feature = "protocol_feature_block_header_v3")]
        if db_version <= 18 {
            migrate_18_to_new_validator_stake(&store);
        }

        // set some dummy value to avoid conflict with other migrations from nightly features
        set_store_version(&store, 10000);
    }

    #[cfg(not(feature = "nightly_protocol"))]
    {
        let db_version = get_store_version(path);
        debug_assert_eq!(db_version, near_primitives::version::DB_VERSION);
    }
}

pub fn init_and_migrate_store(home_dir: &Path, near_config: &NearConfig) -> Arc<Store> {
    let path = get_store_path(home_dir);
    let store_exists = store_path_exists(&path);
    if store_exists {
        apply_store_migrations(&path, near_config);
    }
    let store = create_store(&path);
    if !store_exists {
        set_store_version(&store, near_primitives::version::DB_VERSION);
    }
    store
}

pub struct NearNode {
    pub client: Addr<ClientActor>,
    pub view_client: Addr<ViewClientActor>,
    pub arbiters: Vec<ArbiterHandle>,
    pub rpc_servers: Vec<(&'static str, actix_web::dev::Server)>,
}

pub fn start_with_config(home_dir: &Path, config: NearConfig) -> NearNode {
    let store = init_and_migrate_store(home_dir, &config);

    let runtime = Arc::new(NightshadeRuntime::with_config(
        home_dir,
        Arc::clone(&store),
        &config,
        config.client_config.trie_viewer_state_size_limit,
        config.client_config.max_gas_burnt_view,
    ));

    let telemetry = TelemetryActor::new(config.telemetry_config.clone()).start();
    let chain_genesis = ChainGenesis::from(&config.genesis);

    let node_id = config.network_config.public_key.clone().into();
    let network_adapter = Arc::new(NetworkRecipient::new());
    #[cfg(feature = "test_features")]
    let adv = Arc::new(std::sync::RwLock::new(AdversarialControls::default()));

    let view_client = start_view_client(
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        chain_genesis.clone(),
        runtime.clone(),
        network_adapter.clone(),
        config.client_config.clone(),
        #[cfg(feature = "test_features")]
        adv.clone(),
    );
    let (client_actor, client_arbiter_handle) = start_client(
        config.client_config,
        chain_genesis,
        runtime,
        node_id,
        network_adapter.clone(),
        config.validator_signer,
        telemetry,
        #[cfg(feature = "test_features")]
        adv.clone(),
    );

    #[allow(unused_mut)]
    let mut rpc_servers = Vec::new();
    let arbiter = Arbiter::new();
    let client_actor1 = client_actor.clone().recipient();
    let view_client1 = view_client.clone().recipient();
    config.network_config.verify();
    let network_config = config.network_config;
    let ibf_routing_pool = make_ibf_routing_pool();
    #[cfg(all(feature = "json_rpc", feature = "test_features"))]
    let ibf_routing_pool2 = ibf_routing_pool.clone();
    let network_actor = PeerManagerActor::start_in_arbiter(&arbiter.handle(), move |_ctx| {
        PeerManagerActor::new(store, network_config, client_actor1, view_client1, ibf_routing_pool)
            .unwrap()
    });

    #[cfg(feature = "json_rpc")]
    if let Some(rpc_config) = config.rpc_config {
        rpc_servers.extend_from_slice(&near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            client_actor.clone(),
            view_client.clone(),
            #[cfg(feature = "test_features")]
            network_actor.clone(),
            #[cfg(feature = "test_features")]
            ibf_routing_pool2,
        ));
    }

    #[cfg(feature = "rosetta_rpc")]
    if let Some(rosetta_rpc_config) = config.rosetta_rpc_config {
        rpc_servers.push((
            "Rosetta RPC",
            start_rosetta_rpc(
                rosetta_rpc_config,
                Arc::new(config.genesis.clone()),
                client_actor.clone(),
                view_client.clone(),
            ),
        ));
    }

    network_adapter.set_recipient(network_actor.recipient());

    rpc_servers.shrink_to_fit();

    trace!(target: "diagnostic", key="log", "Starting NEAR node with diagnostic activated");

    // We probably reached peak memory once on this thread, we want to see when it happens again.
    #[cfg(feature = "performance_stats")]
    reset_memory_usage_max();

    NearNode {
        client: client_actor,
        view_client,
        rpc_servers,
        arbiters: vec![client_arbiter_handle, arbiter.handle()],
    }
}
