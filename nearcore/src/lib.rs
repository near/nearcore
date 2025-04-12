pub use crate::config::NightshadeRuntimeExt;
pub use crate::config::{NearConfig, init_configs, load_config, load_test_config};
#[cfg(feature = "json_rpc")]
use crate::entity_debug::EntityDebugHandlerImpl;
use crate::metrics::spawn_trie_metrics_loop;

use crate::cold_storage::spawn_cold_store_loop;
use crate::state_sync::StateSyncDumper;
use actix::{Actor, Addr};
use actix_rt::ArbiterHandle;
use anyhow::Context;
use cold_storage::ColdStoreLoopHandle;
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::actix_wrapper::{ActixWrapper, spawn_actix_actor};
use near_async::futures::TokioRuntimeFutureSpawner;
use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender};
use near_async::time::{self, Clock};
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::resharding::resharding_actor::ReshardingActor;
pub use near_chain::runtime::NightshadeRuntime;
use near_chain::state_snapshot_actor::{
    SnapshotCallbacks, StateSnapshotActor, get_delete_snapshot_callback, get_make_snapshot_callback,
};
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis};
use near_chain_configs::ReshardingHandle;
use near_chunks::shards_manager_actor::start_shards_manager;
use near_client::adapter::client_sender_for_network;
use near_client::gc_actor::GCActor;
use near_client::{
    ClientActor, ConfigUpdater, PartialWitnessActor, RpcHandlerActor, RpcHandlerConfig,
    StartClientResult, ViewClientActor, ViewClientActorInner, spawn_rpc_handler_actor,
    start_client,
};
use near_epoch_manager::EpochManager;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::PeerManagerActor;
use near_primitives::genesis::GenesisId;
use near_primitives::types::EpochId;
use near_store::db::metadata::DbKind;
use near_store::genesis::initialize_sharded_genesis_state;
use near_store::metrics::spawn_db_metrics_loop;
use near_store::{NodeStorage, Store, StoreOpenerError};
use near_telemetry::TelemetryActor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::broadcast;

pub mod append_only_map;
pub mod cold_storage;
pub mod config;
#[cfg(test)]
mod config_duration_test;
mod config_validate;
mod download_file;
pub mod dyn_config;
#[cfg(feature = "json_rpc")]
pub mod entity_debug;
mod entity_debug_serializer;
mod metrics;
pub mod migrations;
pub mod state_sync;
#[cfg(feature = "tx_generator")]
use near_transactions_generator::actix_actor::TxGeneratorActor;

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

/// Opens node’s storage performing migrations and checks when necessary.
///
/// If opened storage is an RPC store and `near_config.config.archive` is true,
/// converts the storage to archival node.  Otherwise, if opening archival node
/// with that field being false, prints a warning and sets the field to `true`.
/// In other words, once store is archival, the node will act as archival nod
/// regardless of settings in `config.json`.
///
/// The end goal is to get rid of `archive` option in `config.json` file and
/// have the type of the node be determined purely based on kind of database
/// being opened.
pub fn open_storage(home_dir: &Path, near_config: &mut NearConfig) -> anyhow::Result<NodeStorage> {
    let migrator = migrations::Migrator::new(near_config);
    let opener = NodeStorage::opener(
        home_dir,
        &near_config.config.store,
        near_config.config.archival_config(),
    )
    .with_migrator(&migrator);
    let storage = match opener.open() {
        Ok(storage) => Ok(storage),
        Err(StoreOpenerError::IO(err)) => {
            Err(anyhow::anyhow!("{err}"))
        }
        // Cannot happen with Mode::ReadWrite
        Err(StoreOpenerError::DbDoesNotExist) => unreachable!(),
        // Cannot happen with Mode::ReadWrite
        Err(StoreOpenerError::DbAlreadyExists) => unreachable!(),
        Err(StoreOpenerError::HotColdExistenceMismatch) => {
            Err(anyhow::anyhow!(
                "Hot and cold databases must either both exist or both not exist.\n\
                 Note that at this moment it’s not possible to convert and RPC or legacy archive database into split hot+cold database.\n\
                 To set up node in that configuration, start with neither of the databases existing.",
            ))
        },
        Err(err @ StoreOpenerError::HotColdVersionMismatch { .. }) => {
            Err(anyhow::anyhow!("{err}"))
        },
        Err(StoreOpenerError::DbKindMismatch { which, got, want }) => {
            Err(if let Some(got) = got {
                anyhow::anyhow!("{which} database kind should be {want} but got {got}")
            } else {
                anyhow::anyhow!("{which} database kind should be {want} but none was set")
            })
        }
        Err(StoreOpenerError::SnapshotAlreadyExists(snap_path)) => {
            Err(anyhow::anyhow!(
                "Detected an existing database migration snapshot at ‘{}’.\n\
                 Probably a database migration got interrupted and your database is corrupted.\n\
                 Please replace files in ‘{}’ with contents of the snapshot, delete the snapshot and try again.",
                snap_path.display(),
                opener.path().display(),
            ))
        },
        Err(StoreOpenerError::SnapshotError(err)) => {
            use near_store::config::MigrationSnapshot;
            let path = std::path::PathBuf::from("/path/to/snapshot/dir");
            let on = MigrationSnapshot::Path(path).format_example();
            let off = MigrationSnapshot::Enabled(false).format_example();
            Err(anyhow::anyhow!(
                "Failed to create a database migration snapshot: {err}.\n\
                 To change the location of snapshot adjust \
                 ‘store.migration_snapshot’ property in ‘config.json’:\n{on}\n\
                 Alternatively, you can disable database migration snapshots \
                 in `config.json`:\n{off}"
            ))
        },
        Err(StoreOpenerError::SnapshotRemoveError { path, error }) => {
            let path = path.display();
            Err(anyhow::anyhow!(
                "The DB migration has succeeded but deleting of the snapshot \
                 at {path} has failed: {error}\n
                 Try renaming the snapshot directory to temporary name (e.g. \
                 by adding tilde to its name) and starting the node.  If that \
                 works, the snapshot can be deleted."))
        }
        // Cannot happen with Mode::ReadWrite
        Err(StoreOpenerError::DbVersionMismatchOnRead { .. }) => unreachable!(),
        // Cannot happen when migrator is specified.
        Err(StoreOpenerError::DbVersionMismatch { .. }) => unreachable!(),
        Err(StoreOpenerError::DbVersionMissing { .. }) => {
            Err(anyhow::anyhow!("Database version is missing!"))
        },
        Err(StoreOpenerError::DbVersionTooOld { got, latest_release, .. }) => {
            Err(anyhow::anyhow!(
                "Database version {got} is created by an old version \
                 of neard and is no longer supported, please migrate using \
                 {latest_release} release"
            ))
        },
        Err(StoreOpenerError::DbVersionTooNew { got, want }) => {
            Err(anyhow::anyhow!(
                "Database version {got} is higher than the expected version {want}. \
                It was likely created by newer version of neard. Please upgrade your neard."
            ))
        },
        Err(StoreOpenerError::MigrationError(err)) => {
            Err(err)
        },
        Err(StoreOpenerError::CheckpointError(err)) => {
            Err(err)
        },
    }.with_context(|| format!("unable to open database at {}", opener.path().display()))?;

    near_config.config.archive = storage.is_archive()?;
    Ok(storage)
}

// Safely get the split store while checking that all conditions to use it are met.
fn get_split_store(config: &NearConfig, storage: &NodeStorage) -> anyhow::Result<Option<Store>> {
    // SplitStore should only be used on archival nodes.
    if !config.config.archive {
        return Ok(None);
    }

    // SplitStore should only be used if cold store is configured.
    if config.config.cold_store.is_none() {
        return Ok(None);
    }

    // SplitStore should only be used in the view client if it is enabled.
    if !config.config.split_storage.as_ref().is_some_and(|c| c.enable_split_storage_view_client) {
        return Ok(None);
    }

    // SplitStore should only be used if the migration is finished. The
    // migration to cold store is finished when the db kind of the hot store is
    // changed from Archive to Hot.
    if storage.get_hot_store().get_db_kind()? != Some(DbKind::Hot) {
        return Ok(None);
    }

    Ok(storage.get_split_store())
}

pub struct NearNode {
    pub client: Addr<ClientActor>,
    pub view_client: Addr<ViewClientActor>,
    pub rpc_handler: Addr<RpcHandlerActor>,
    #[cfg(feature = "tx_generator")]
    pub tx_generator: Addr<TxGeneratorActor>,
    pub arbiters: Vec<ArbiterHandle>,
    pub rpc_servers: Vec<(&'static str, actix_web::dev::ServerHandle)>,
    /// The cold_store_loop_handle will only be set if the cold store is configured.
    /// It's a handle to a background thread that copies data from the hot store to the cold store.
    pub cold_store_loop_handle: Option<ColdStoreLoopHandle>,
    /// Contains handles to background threads that may be dumping state to S3.
    pub state_sync_dumper: StateSyncDumper,
    // A handle that allows the main process to interrupt resharding if needed.
    // This typically happens when the main process is interrupted.
    pub resharding_handle: ReshardingHandle,
    // The threads that state sync runs in.
    pub state_sync_runtime: Arc<tokio::runtime::Runtime>,
    /// Shard tracker, allows querying of which shards are tracked by this node.
    pub shard_tracker: ShardTracker,
}

pub fn start_with_config(home_dir: &Path, config: NearConfig) -> anyhow::Result<NearNode> {
    start_with_config_and_synchronization(home_dir, config, None, None)
}

pub fn start_with_config_and_synchronization(
    home_dir: &Path,
    mut config: NearConfig,
    // 'shutdown_signal' will notify the corresponding `oneshot::Receiver` when an instance of
    // `ClientActor` gets dropped.
    shutdown_signal: Option<broadcast::Sender<()>>,
    config_updater: Option<ConfigUpdater>,
) -> anyhow::Result<NearNode> {
    let storage = open_storage(home_dir, &mut config)?;
    let db_metrics_arbiter = if config.client_config.enable_statistics_export {
        let period = config.client_config.log_summary_period;
        let db_metrics_arbiter_handle = spawn_db_metrics_loop(&storage, period)?;
        Some(db_metrics_arbiter_handle)
    } else {
        None
    };

    let epoch_manager = EpochManager::new_arc_handle(
        storage.get_hot_store(),
        &config.genesis.config,
        Some(home_dir),
    );

    let trie_metrics_arbiter = spawn_trie_metrics_loop(
        config.clone(),
        storage.get_hot_store(),
        config.client_config.log_summary_period,
        epoch_manager.clone(),
    )?;

    let genesis_epoch_config = epoch_manager.get_epoch_config(&EpochId::default())?;
    // Initialize genesis_state in store either from genesis config or dump before other components.
    // We only initialize if the genesis state is not already initialized in store.
    // This sets up genesis_state_roots and genesis_hash in store.
    initialize_sharded_genesis_state(
        storage.get_hot_store(),
        &config.genesis,
        &genesis_epoch_config,
        Some(home_dir),
    );

    let shard_tracker = ShardTracker::new(
        config.client_config.tracked_shards_config.clone(),
        epoch_manager.clone(),
    );
    let runtime = NightshadeRuntime::from_config(
        home_dir,
        storage.get_hot_store(),
        &config,
        epoch_manager.clone(),
    )
    .context("could not create the transaction runtime")?;

    // Get the split store. If split store is some then create a new set of structures for
    // the view client. Otherwise just re-use the existing ones.
    let split_store = get_split_store(&config, &storage)?;
    let (view_epoch_manager, view_shard_tracker, view_runtime) =
        if let Some(split_store) = &split_store {
            let view_epoch_manager = EpochManager::new_arc_handle(
                split_store.clone(),
                &config.genesis.config,
                Some(home_dir),
            );
            let view_shard_tracker = ShardTracker::new(
                config.client_config.tracked_shards_config.clone(),
                epoch_manager.clone(),
            );
            let view_runtime = NightshadeRuntime::from_config(
                home_dir,
                split_store.clone(),
                &config,
                view_epoch_manager.clone(),
            )
            .context("could not create the transaction runtime")?;
            (view_epoch_manager, view_shard_tracker, view_runtime)
        } else {
            (epoch_manager.clone(), shard_tracker.clone(), runtime.clone())
        };

    let cold_store_loop_handle = spawn_cold_store_loop(&config, &storage, epoch_manager.clone())?;

    let telemetry = ActixWrapper::new(TelemetryActor::new(config.telemetry_config.clone())).start();
    let chain_genesis = ChainGenesis::new(&config.genesis.config);
    let state_roots = near_store::get_genesis_state_roots(runtime.store())?
        .expect("genesis should be initialized.");
    let (genesis_block, _genesis_chunks) = Chain::make_genesis_block(
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &chain_genesis,
        state_roots,
    )?;
    let genesis_id = GenesisId {
        chain_id: config.client_config.chain_id.clone(),
        hash: *genesis_block.header().hash(),
    };

    let node_id = config.network_config.node_id();
    let network_adapter = LateBoundSender::new();
    let shards_manager_adapter = LateBoundSender::new();
    let client_adapter_for_shards_manager = LateBoundSender::new();
    let client_adapter_for_partial_witness_actor = LateBoundSender::new();
    let adv = near_client::adversarial::Controls::new(config.client_config.archive);

    let view_client_addr = ViewClientActorInner::spawn_actix_actor(
        Clock::real(),
        config.validator_signer.clone(),
        chain_genesis.clone(),
        view_epoch_manager.clone(),
        view_shard_tracker.clone(),
        view_runtime.clone(),
        network_adapter.as_multi_sender(),
        config.client_config.clone(),
        adv.clone(),
    );

    let state_snapshot_sender = LateBoundSender::new();
    let state_snapshot_actor = StateSnapshotActor::new(
        runtime.get_flat_storage_manager(),
        network_adapter.as_multi_sender(),
        runtime.get_tries(),
    );
    let (state_snapshot_addr, state_snapshot_arbiter) = spawn_actix_actor(state_snapshot_actor);
    state_snapshot_sender.bind(state_snapshot_addr.clone().with_auto_span_context());

    let delete_snapshot_callback: Arc<dyn Fn() + Sync + Send> = get_delete_snapshot_callback(
        state_snapshot_addr.clone().with_auto_span_context().into_multi_sender(),
    );
    let make_snapshot_callback = get_make_snapshot_callback(
        state_snapshot_addr.with_auto_span_context().into_multi_sender(),
        runtime.get_flat_storage_manager(),
    );
    let snapshot_callbacks = SnapshotCallbacks { make_snapshot_callback, delete_snapshot_callback };

    let (partial_witness_actor, partial_witness_arbiter) =
        spawn_actix_actor(PartialWitnessActor::new(
            Clock::real(),
            network_adapter.as_multi_sender(),
            client_adapter_for_partial_witness_actor.as_multi_sender(),
            config.validator_signer.clone(),
            epoch_manager.clone(),
            runtime.clone(),
            Arc::new(RayonAsyncComputationSpawner),
            Arc::new(RayonAsyncComputationSpawner),
        ));

    let (_gc_actor, gc_arbiter) = spawn_actix_actor(GCActor::new(
        runtime.store().clone(),
        &chain_genesis,
        runtime.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        config.validator_signer.clone(),
        config.client_config.gc.clone(),
        config.client_config.archive,
    ));

    let (resharding_sender_addr, _) =
        spawn_actix_actor(ReshardingActor::new(runtime.store().clone(), &chain_genesis));
    let resharding_sender = resharding_sender_addr.with_auto_span_context();
    let state_sync_runtime =
        Arc::new(tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap());

    let state_sync_spawner = Arc::new(TokioRuntimeFutureSpawner(state_sync_runtime.clone()));
    let StartClientResult {
        client_actor,
        client_arbiter_handle,
        resharding_handle,
        tx_pool,
        chunk_endorsement_tracker,
    } = start_client(
        Clock::real(),
        config.client_config.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        node_id,
        state_sync_spawner.clone(),
        network_adapter.as_multi_sender(),
        shards_manager_adapter.as_sender(),
        config.validator_signer.clone(),
        telemetry.with_auto_span_context().into_sender(),
        Some(snapshot_callbacks),
        shutdown_signal,
        adv,
        config_updater,
        partial_witness_actor.clone().with_auto_span_context().into_multi_sender(),
        true,
        None,
        resharding_sender.into_multi_sender(),
    );
    client_adapter_for_shards_manager.bind(client_actor.clone().with_auto_span_context());
    client_adapter_for_partial_witness_actor.bind(client_actor.clone().with_auto_span_context());
    let (shards_manager_actor, shards_manager_arbiter_handle) = start_shards_manager(
        epoch_manager.clone(),
        view_epoch_manager.clone(),
        shard_tracker.clone(),
        network_adapter.as_sender(),
        client_adapter_for_shards_manager.as_sender(),
        config.validator_signer.clone(),
        split_store.unwrap_or_else(|| storage.get_hot_store()),
        config.client_config.chunk_request_retry_period,
    );
    shards_manager_adapter.bind(shards_manager_actor.with_auto_span_context());

    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: config.client_config.transaction_request_handler_threads,
        tx_routing_height_horizon: config.client_config.tx_routing_height_horizon,
        epoch_length: config.client_config.epoch_length,
        transaction_validity_period: config.genesis.config.transaction_validity_period,
    };
    let rpc_handler = spawn_rpc_handler_actor(
        rpc_handler_config,
        tx_pool,
        chunk_endorsement_tracker,
        view_epoch_manager.clone(),
        view_shard_tracker,
        config.validator_signer.clone(),
        view_runtime.clone(),
        network_adapter.as_multi_sender(),
    );

    let mut state_sync_dumper = StateSyncDumper {
        clock: Clock::real(),
        client_config: config.client_config.clone(),
        chain_genesis,
        epoch_manager,
        shard_tracker: shard_tracker.clone(),
        runtime,
        validator: config.validator_signer.clone(),
        future_spawner: state_sync_spawner,
        handle: None,
    };
    state_sync_dumper.start()?;

    let hot_store = storage.get_hot_store();
    let cold_store = storage.get_cold_store();

    let mut rpc_servers = Vec::new();
    let network_actor = PeerManagerActor::spawn(
        time::Clock::real(),
        storage.into_inner(near_store::Temperature::Hot),
        config.network_config,
        client_sender_for_network(
            client_actor.clone(),
            view_client_addr.clone(),
            rpc_handler.clone(),
        ),
        network_adapter.as_multi_sender(),
        shards_manager_adapter.as_sender(),
        partial_witness_actor.with_auto_span_context().into_multi_sender(),
        genesis_id,
    )
    .context("PeerManager::spawn()")?;
    network_adapter.bind(network_actor.clone().with_auto_span_context());
    #[cfg(feature = "json_rpc")]
    if let Some(rpc_config) = config.rpc_config {
        let entity_debug_handler = EntityDebugHandlerImpl {
            epoch_manager: view_epoch_manager,
            runtime: view_runtime,
            hot_store,
            cold_store,
        };
        rpc_servers.extend(near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            client_actor.clone().with_auto_span_context().into_multi_sender(),
            view_client_addr.clone().with_auto_span_context().into_multi_sender(),
            rpc_handler.clone().with_auto_span_context().into_multi_sender(),
            network_actor.into_multi_sender(),
            #[cfg(feature = "test_features")]
            _gc_actor.with_auto_span_context().into_multi_sender(),
            Arc::new(entity_debug_handler),
        ));
    }

    #[cfg(feature = "rosetta_rpc")]
    if let Some(rosetta_rpc_config) = config.rosetta_rpc_config {
        rpc_servers.push((
            "Rosetta RPC",
            near_rosetta_rpc::start_rosetta_rpc(
                rosetta_rpc_config,
                config.genesis,
                genesis_block.header().hash(),
                client_actor.clone(),
                view_client_addr.clone(),
                rpc_handler.clone(),
            ),
        ));
    }

    rpc_servers.shrink_to_fit();

    tracing::trace!(target: "diagnostic", key = "log", "Starting NEAR node with diagnostic activated");

    let mut arbiters = vec![
        client_arbiter_handle,
        shards_manager_arbiter_handle,
        trie_metrics_arbiter,
        state_snapshot_arbiter,
        gc_arbiter,
        partial_witness_arbiter,
    ];
    if let Some(db_metrics_arbiter) = db_metrics_arbiter {
        arbiters.push(db_metrics_arbiter);
    }

    #[cfg(feature = "tx_generator")]
    let tx_generator = near_transactions_generator::actix_actor::start_tx_generator(
        config.tx_generator.unwrap_or_default(),
        rpc_handler.clone().with_auto_span_context().into_multi_sender(),
        view_client_addr.clone().with_auto_span_context().into_multi_sender(),
    );

    Ok(NearNode {
        client: client_actor,
        view_client: view_client_addr,
        rpc_handler,
        #[cfg(feature = "tx_generator")]
        tx_generator,
        rpc_servers,
        arbiters,
        cold_store_loop_handle,
        state_sync_dumper,
        resharding_handle,
        state_sync_runtime,
        shard_tracker,
    })
}
