pub use crate::config::NightshadeRuntimeExt;
pub use crate::config::{NearConfig, init_configs, load_config, load_test_config};
#[cfg(feature = "json_rpc")]
use crate::entity_debug::EntityDebugHandlerImpl;
use crate::metrics::spawn_trie_metrics_loop;

use crate::state_sync::StateSyncDumper;
use anyhow::Context;
use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, noop};
use near_async::time::{self, Clock};
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::resharding::resharding_actor::ReshardingActor;
pub use near_chain::runtime::NightshadeRuntime;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain::state_snapshot_actor::{
    SnapshotCallbacks, StateSnapshotActor, get_delete_snapshot_callback, get_make_snapshot_callback,
};
use near_chain::types::RuntimeAdapter;
use near_chain::{
    ApplyChunksSpawner, Chain, ChainGenesis, PartialWitnessValidationThreadPool,
    WitnessCreationThreadPool,
};
use near_chain_configs::{CloudArchivalHandle, MutableValidatorSigner, ReshardingHandle};
use near_chunks::shards_manager_actor::start_shards_manager;
use near_client::adapter::client_sender_for_network;
use near_client::archive::cloud_archival_actor::create_cloud_archival_actor;
use near_client::archive::cold_store_actor::create_cold_store_actor;
use near_client::chunk_executor_actor::ChunkExecutorActor;
use near_client::gc_actor::GCActor;
use near_client::spice_chunk_validator_actor::SpiceChunkValidatorActor;
use near_client::spice_data_distributor_actor::SpiceDataDistributorActor;
use near_client::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;
use near_client::{
    ChunkValidationSenderForPartialWitness, ConfigUpdater, PartialWitnessActor, RpcHandler,
    RpcHandlerConfig, StartClientResult, StateRequestActor, ViewClientActorInner,
    spawn_rpc_handler_actor, start_client,
};
use near_epoch_manager::EpochManager;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::PeerManagerActor;
use near_network::types::PeerManagerAdapter;
use near_primitives::genesis::GenesisId;
use near_primitives::types::EpochId;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::db::metadata::DbKind;
use near_store::genesis::initialize_sharded_genesis_state;
use near_store::metrics::spawn_db_metrics_loop;
use near_store::{NodeStorage, Store, StoreOpenerError};
use near_telemetry::TelemetryActor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::broadcast;

pub mod append_only_map;
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
use near_async::ActorSystem;
use near_async::futures::FutureSpawner;
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::tokio::TokioRuntimeHandle;
use near_client::client_actor::{ClientActorInner, SpiceClientConfig};
#[cfg(feature = "tx_generator")]
use near_transactions_generator::actix_actor::GeneratorActorImpl;

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
/// converts the storage to archival node.
pub fn open_storage(home_dir: &Path, near_config: &NearConfig) -> anyhow::Result<NodeStorage> {
    let migrator = migrations::Migrator::new(near_config);
    let opener = NodeStorage::opener(
        home_dir,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
        near_config.config.cloud_storage_config(),
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

    assert_eq!(near_config.config.archive, storage.is_archive()?);
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

fn new_spice_client_config(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    chain_store: ChainStoreAdapter,
    chunk_executor_adapter: &Arc<LateBoundSender<TokioRuntimeHandle<ChunkExecutorActor>>>,
    spice_chunk_validator_adapter: &Arc<
        LateBoundSender<TokioRuntimeHandle<SpiceChunkValidatorActor>>,
    >,
    spice_data_distributor_adapter: &Arc<
        LateBoundSender<TokioRuntimeHandle<SpiceDataDistributorActor>>,
    >,
) -> SpiceClientConfig {
    let spice_client_config = if cfg!(feature = "protocol_feature_spice") {
        let core_processor = CoreStatementsProcessor::new(
            chain_store,
            epoch_manager,
            chunk_executor_adapter.as_sender(),
            spice_chunk_validator_adapter.as_sender(),
        );
        SpiceClientConfig {
            core_processor,
            chunk_executor_sender: chunk_executor_adapter.as_sender(),
            spice_chunk_validator_sender: spice_chunk_validator_adapter.as_sender(),
            spice_data_distributor_sender: spice_data_distributor_adapter.as_sender(),
        }
    } else {
        let core_processor =
            CoreStatementsProcessor::new_with_noop_senders(chain_store, epoch_manager);
        SpiceClientConfig {
            core_processor,
            chunk_executor_sender: noop().into_sender(),
            spice_chunk_validator_sender: noop().into_sender(),
            spice_data_distributor_sender: noop().into_sender(),
        }
    };
    spice_client_config
}

struct SpiceActorsConfig {
    validator_signer: MutableValidatorSigner,
    save_latest_witnesses: bool,
    save_invalid_witnesses: bool,
}

fn spawn_spice_actors(
    actor_system: &ActorSystem,
    config: SpiceActorsConfig,
    chain_genesis: &ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<NightshadeRuntime>,
    network_adapter: PeerManagerAdapter,
    core_processor: CoreStatementsProcessor,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
    chunk_executor_adapter: &Arc<LateBoundSender<TokioRuntimeHandle<ChunkExecutorActor>>>,
    spice_chunk_validator_adapter: &Arc<
        LateBoundSender<TokioRuntimeHandle<SpiceChunkValidatorActor>>,
    >,
    spice_data_distributor_adapter: &Arc<
        LateBoundSender<TokioRuntimeHandle<SpiceDataDistributorActor>>,
    >,
) {
    let spice_data_distributor_actor = SpiceDataDistributorActor::new(
        epoch_manager.clone(),
        runtime.store().chain_store(),
        core_processor.clone(),
        config.validator_signer.clone(),
        network_adapter.clone(),
        chunk_executor_adapter.as_sender(),
        spice_chunk_validator_adapter.as_sender(),
    );
    let spice_data_distributor_addr = actor_system.spawn_tokio_actor(spice_data_distributor_actor);
    spice_data_distributor_adapter.bind(spice_data_distributor_addr);

    let chunk_executor_actor = ChunkExecutorActor::new(
        runtime.store().clone(),
        chain_genesis,
        runtime.clone(),
        epoch_manager.clone(),
        shard_tracker,
        network_adapter.clone(),
        config.validator_signer.clone(),
        core_processor.clone(),
        chunk_endorsement_tracker.clone(),
        {
            let thread_limit = runtime.get_shard_layout(PROTOCOL_VERSION).num_shards() as usize;
            ApplyChunksSpawner::default().into_spawner(thread_limit)
        },
        Default::default(),
        chunk_executor_adapter.as_sender(),
        spice_data_distributor_adapter.as_multi_sender(),
        config.save_latest_witnesses,
    );
    let chunk_executor_addr = actor_system.spawn_tokio_actor(chunk_executor_actor);
    chunk_executor_adapter.bind(chunk_executor_addr);

    let spice_chunk_validator_actor = SpiceChunkValidatorActor::new(
        runtime.store().clone(),
        chain_genesis,
        runtime,
        epoch_manager,
        network_adapter,
        config.validator_signer,
        core_processor,
        chunk_endorsement_tracker,
        ApplyChunksSpawner::default(),
        config.save_latest_witnesses,
        config.save_invalid_witnesses,
    );
    let spice_chunk_validator_addr = actor_system.spawn_tokio_actor(spice_chunk_validator_actor);
    spice_chunk_validator_adapter.bind(spice_chunk_validator_addr);
}

pub struct NearNode {
    pub client: TokioRuntimeHandle<ClientActorInner>,
    pub view_client: MultithreadRuntimeHandle<ViewClientActorInner>,
    // TODO(darioush): Remove once we migrate `slow_test_state_sync_headers` and
    // `slow_test_state_sync_headers_no_tracked_shards` to testloop.
    pub state_request_client: MultithreadRuntimeHandle<StateRequestActor>,
    pub rpc_handler: MultithreadRuntimeHandle<RpcHandler>,
    #[cfg(feature = "tx_generator")]
    pub tx_generator: TokioRuntimeHandle<GeneratorActorImpl>,
    /// The cold_store_loop_handle will only be set if the cold store is configured.
    /// It's a handle to control the cold store actor that copies data from the hot store to the cold store.
    pub cold_store_loop_handle: Option<Arc<AtomicBool>>,
    /// The `cloud_archival_handle` will only be set if the cloud archival writer is configured. It's a handle
    /// to control the cloud archival actor that archives data from the hot store to the cloud archival.
    pub cloud_archival_handle: Option<CloudArchivalHandle>,
    /// Contains handles to background threads that may be dumping state to S3.
    pub state_sync_dumper: StateSyncDumper,
    // A handle that allows the main process to interrupt resharding if needed.
    // This typically happens when the main process is interrupted.
    pub resharding_handle: ReshardingHandle,
    /// Shard tracker, allows querying of which shards are tracked by this node.
    pub shard_tracker: ShardTracker,
}

pub fn start_with_config(
    home_dir: &Path,
    config: NearConfig,
    actor_system: ActorSystem,
) -> anyhow::Result<NearNode> {
    start_with_config_and_synchronization(home_dir, config, actor_system, None, None)
}

pub fn start_with_config_and_synchronization(
    home_dir: &Path,
    config: NearConfig,
    actor_system: ActorSystem,
    // 'shutdown_signal' will notify the corresponding `oneshot::Receiver` when an instance of
    // `ClientActor` gets dropped.
    shutdown_signal: Option<broadcast::Sender<()>>,
    config_updater: Option<ConfigUpdater>,
) -> anyhow::Result<NearNode> {
    let storage = open_storage(home_dir, &config)?;
    if config.client_config.enable_statistics_export {
        let period = config.client_config.log_summary_period;
        spawn_db_metrics_loop(actor_system.clone(), &storage, period);
    }

    let epoch_manager = EpochManager::new_arc_handle(
        storage.get_hot_store(),
        &config.genesis.config,
        Some(home_dir),
    );

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

    // Spawn this after initializing genesis, or else the metrics may fail to be exported.
    spawn_trie_metrics_loop(
        actor_system.clone(),
        config.clone(),
        storage.get_hot_store(),
        config.client_config.log_summary_period,
        epoch_manager.clone(),
    );

    let shard_tracker = ShardTracker::new(
        config.client_config.tracked_shards_config.clone(),
        epoch_manager.clone(),
        config.validator_signer.clone(),
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
                config.validator_signer.clone(),
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

    let result = create_cold_store_actor(
        config.config.save_trie_changes,
        &config.config.split_storage.clone().unwrap_or_default(),
        config.genesis.config.genesis_height,
        storage.get_hot_store(),
        storage.cold_db(),
        epoch_manager.clone(),
        shard_tracker.clone(),
    )?;
    let cold_store_loop_handle = if let Some((actor, keep_going)) = result {
        let _cold_store_addr = actor_system.spawn_tokio_actor(actor);
        Some(keep_going)
    } else {
        None
    };

    let result = create_cloud_archival_actor(
        config.config.cloud_archival_writer,
        config.genesis.config.genesis_height,
        &storage,
    )?;
    let cloud_archival_handle = if let Some((actor, handle)) = result {
        let _cloud_archival_addr = actor_system.spawn_tokio_actor(actor);
        Some(handle)
    } else {
        None
    };

    let telemetry =
        TelemetryActor::spawn_tokio_actor(actor_system.clone(), config.telemetry_config.clone());
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

    let view_client_addr = ViewClientActorInner::spawn_multithread_actor(
        Clock::real(),
        actor_system.clone(),
        chain_genesis.clone(),
        view_epoch_manager.clone(),
        view_shard_tracker.clone(),
        view_runtime.clone(),
        network_adapter.as_multi_sender(),
        config.client_config.clone(),
        adv.clone(),
        config.validator_signer.clone(),
    );

    // Use dedicated thread pool for StateRequestActor
    let state_request_addr = {
        let runtime = runtime.clone();
        let epoch_manager = epoch_manager.clone();
        actor_system.spawn_multithread_actor(
            config.client_config.state_request_server_threads,
            move || {
                StateRequestActor::new(
                    Clock::real(),
                    runtime.clone(),
                    epoch_manager.clone(),
                    genesis_id.hash,
                    config.client_config.state_request_throttle_period,
                    config.client_config.state_requests_per_throttle_period,
                )
            },
        )
    };

    let state_snapshot_sender = LateBoundSender::new();
    let state_snapshot_actor = StateSnapshotActor::new(
        runtime.get_flat_storage_manager(),
        network_adapter.as_multi_sender(),
        runtime.get_tries(),
    );
    let state_snapshot_addr = actor_system.spawn_tokio_actor(state_snapshot_actor);
    state_snapshot_sender.bind(state_snapshot_addr.clone());

    let delete_snapshot_callback: Arc<dyn Fn() + Sync + Send> =
        get_delete_snapshot_callback(state_snapshot_addr.clone().into_multi_sender());
    let make_snapshot_callback = get_make_snapshot_callback(
        state_snapshot_addr.into_multi_sender(),
        runtime.get_flat_storage_manager(),
    );
    let snapshot_callbacks = SnapshotCallbacks { make_snapshot_callback, delete_snapshot_callback };

    let partial_witness_actor = actor_system.spawn_tokio_actor(PartialWitnessActor::new(
        Clock::real(),
        network_adapter.as_multi_sender(),
        client_adapter_for_partial_witness_actor.as_multi_sender(),
        config.validator_signer.clone(),
        epoch_manager.clone(),
        runtime.clone(),
        Arc::new(RayonAsyncComputationSpawner),
        Arc::new(PartialWitnessValidationThreadPool::new()),
        Arc::new(WitnessCreationThreadPool::new()),
    ));

    let _gc_actor = actor_system.spawn_tokio_actor(GCActor::new(
        runtime.store().clone(),
        &chain_genesis,
        runtime.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        config.client_config.gc.clone(),
        config.client_config.archive,
    ));

    let resharding_handle = ReshardingHandle::new();
    let resharding_sender = actor_system.spawn_tokio_actor(ReshardingActor::new(
        epoch_manager.clone(),
        runtime.clone(),
        resharding_handle.clone(),
        config.client_config.resharding_config.clone(),
    ));

    let state_sync_spawner: Arc<dyn FutureSpawner> = actor_system.new_future_spawner().into();

    let chunk_executor_adapter = LateBoundSender::new();
    let spice_chunk_validator_adapter = LateBoundSender::new();
    let spice_data_distributor_adapter = LateBoundSender::new();
    let spice_client_config = new_spice_client_config(
        epoch_manager.clone(),
        runtime.store().chain_store(),
        &chunk_executor_adapter,
        &spice_chunk_validator_adapter,
        &spice_data_distributor_adapter,
    );
    let core_processor = spice_client_config.core_processor.clone();

    let StartClientResult {
        client_actor,
        tx_pool,
        chunk_endorsement_tracker,
        chunk_validation_actor,
    } = start_client(
        Clock::real(),
        actor_system.clone(),
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
        telemetry.into_sender(),
        Some(snapshot_callbacks),
        shutdown_signal,
        adv,
        config_updater,
        partial_witness_actor.clone().into_multi_sender(),
        true,
        None,
        resharding_sender.into_multi_sender(),
        spice_client_config,
    );
    client_adapter_for_shards_manager.bind(client_actor.clone());
    client_adapter_for_partial_witness_actor.bind(ChunkValidationSenderForPartialWitness {
        chunk_state_witness: chunk_validation_actor.into_sender(),
    });

    if cfg!(feature = "protocol_feature_spice") {
        spawn_spice_actors(
            &actor_system,
            SpiceActorsConfig {
                validator_signer: config.validator_signer.clone(),
                save_latest_witnesses: config.client_config.save_latest_witnesses,
                save_invalid_witnesses: config.client_config.save_invalid_witnesses,
            },
            &chain_genesis,
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            network_adapter.as_multi_sender(),
            core_processor,
            chunk_endorsement_tracker.clone(),
            &chunk_executor_adapter,
            &spice_chunk_validator_adapter,
            &spice_data_distributor_adapter,
        );
    }

    let shards_manager_actor = start_shards_manager(
        actor_system.clone(),
        epoch_manager.clone(),
        view_epoch_manager.clone(),
        shard_tracker.clone(),
        network_adapter.as_sender(),
        client_adapter_for_shards_manager.as_sender(),
        config.validator_signer.clone(),
        split_store.unwrap_or_else(|| storage.get_hot_store()),
        config.client_config.chunk_request_retry_period,
    );
    shards_manager_adapter.bind(shards_manager_actor);

    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: config.client_config.transaction_request_handler_threads,
        tx_routing_height_horizon: config.client_config.tx_routing_height_horizon,
        epoch_length: config.client_config.epoch_length,
        transaction_validity_period: config.genesis.config.transaction_validity_period,
    };
    let rpc_handler = spawn_rpc_handler_actor(
        actor_system.clone(),
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

    let network_actor = PeerManagerActor::spawn(
        time::Clock::real(),
        storage.into_inner(near_store::Temperature::Hot),
        config.network_config,
        client_sender_for_network(
            client_actor.clone(),
            view_client_addr.clone(),
            rpc_handler.clone(),
        ),
        state_request_addr.clone().into_multi_sender(),
        network_adapter.as_multi_sender(),
        shards_manager_adapter.as_sender(),
        partial_witness_actor.into_multi_sender(),
        if cfg!(feature = "protocol_feature_spice") {
            spice_data_distributor_adapter.as_multi_sender()
        } else {
            noop().into_multi_sender()
        },
        genesis_id,
    )
    .context("PeerManager::spawn()")?;
    network_adapter.bind(network_actor.clone());
    #[cfg(feature = "json_rpc")]
    if let Some(rpc_config) = config.rpc_config {
        let entity_debug_handler = EntityDebugHandlerImpl {
            epoch_manager: view_epoch_manager,
            runtime: view_runtime,
            hot_store,
            cold_store,
        };
        near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            client_actor.clone().into_multi_sender(),
            view_client_addr.clone().into_multi_sender(),
            rpc_handler.clone().into_multi_sender(),
            network_actor.into_multi_sender(),
            #[cfg(feature = "test_features")]
            _gc_actor.into_multi_sender(),
            Arc::new(entity_debug_handler),
            actor_system.new_future_spawner().as_ref(),
        );
    }

    #[cfg(feature = "rosetta_rpc")]
    if let Some(rosetta_rpc_config) = config.rosetta_rpc_config {
        near_rosetta_rpc::start_rosetta_rpc(
            rosetta_rpc_config,
            config.genesis,
            genesis_block.header().hash(),
            client_actor.clone(),
            view_client_addr.clone(),
            rpc_handler.clone(),
            actor_system.new_future_spawner().as_ref(),
        );
    }

    tracing::trace!(target: "diagnostic", key = "log", "Starting NEAR node with diagnostic activated");

    #[cfg(feature = "tx_generator")]
    let tx_generator = near_transactions_generator::actix_actor::start_tx_generator(
        actor_system.clone(),
        config.tx_generator.unwrap_or_default(),
        rpc_handler.clone().into_multi_sender(),
        view_client_addr.clone().into_multi_sender(),
    );

    // To avoid a clippy warning for redundant clones, due to the conditional feature tx_generator.
    drop(actor_system);

    Ok(NearNode {
        client: client_actor,
        view_client: view_client_addr,
        state_request_client: state_request_addr,
        rpc_handler,
        #[cfg(feature = "tx_generator")]
        tx_generator,
        cold_store_loop_handle,
        cloud_archival_handle,
        state_sync_dumper,
        resharding_handle,
        shard_tracker,
    })
}
