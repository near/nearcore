pub use crate::config::NightshadeRuntimeExt;
pub use crate::config::{init_configs, load_config, load_test_config, NearConfig};
use crate::entity_debug::EntityDebugHandlerImpl;
use crate::metrics::spawn_trie_metrics_loop;

use crate::cold_storage::spawn_cold_store_loop;
use crate::state_sync::StateSyncDumper;
use actix::{Actor, Addr};
use actix_rt::ArbiterHandle;
use anyhow::Context;
use cold_storage::ColdStoreLoopHandle;
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::actix_wrapper::{spawn_actix_actor, ActixWrapper};
use near_async::messaging::{noop, IntoMultiSender, IntoSender, LateBoundSender};
use near_async::time::{self, Clock};
pub use near_chain::runtime::NightshadeRuntime;
use near_chain::state_snapshot_actor::{
    get_delete_snapshot_callback, get_make_snapshot_callback, SnapshotCallbacks, StateSnapshotActor,
};
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis};
use near_chain_configs::ReshardingHandle;
use near_chain_configs::SyncConfig;
use near_chunks::shards_manager_actor::start_shards_manager;
use near_client::adapter::client_sender_for_network;
use near_client::gc_actor::GCActor;
use near_client::sync::adapter::SyncAdapter;
use near_client::{
    start_client, ClientActor, ConfigUpdater, PartialWitnessActor, StartClientResult,
    ViewClientActor, ViewClientActorInner,
};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_epoch_manager::EpochManagerAdapter;
use near_network::PeerManagerActor;
use near_primitives::block::GenesisId;
use near_primitives::types::EpochId;
use near_store::flat::FlatStateValuesInliningMigrationHandle;
use near_store::genesis::initialize_sharded_genesis_state;
use near_store::metadata::DbKind;
use near_store::metrics::spawn_db_metrics_loop;
use near_store::{DBCol, Mode, NodeStorage, Store, StoreOpenerError};
use near_telemetry::TelemetryActor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tracing::info;

pub mod append_only_map;
pub mod cold_storage;
pub mod config;
#[cfg(test)]
mod config_duration_test;
mod config_validate;
mod download_file;
pub mod dyn_config;
#[cfg(feature = "json_rpc")]
mod entity_debug;
mod entity_debug_serializer;
mod metrics;
pub mod migrations;
pub mod state_sync;
pub mod test_utils;

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
        near_config.client_config.archive,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
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
    pub arbiters: Vec<ArbiterHandle>,
    pub rpc_servers: Vec<(&'static str, actix_web::dev::ServerHandle)>,
    /// The cold_store_loop_handle will only be set if the cold store is configured.
    /// It's a handle to a background thread that copies data from the hot store to the cold store.
    pub cold_store_loop_handle: Option<ColdStoreLoopHandle>,
    /// Contains handles to background threads that may be dumping state to S3.
    pub state_sync_dumper: StateSyncDumper,
    /// A handle to control background flat state values inlining migration.
    /// Needed temporarily, will be removed after the migration is completed.
    pub flat_state_migration_handle: FlatStateValuesInliningMigrationHandle,
    // A handle that allows the main process to interrupt resharding if needed.
    // This typically happens when the main process is interrupted.
    pub resharding_handle: ReshardingHandle,
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

    let trie_metrics_arbiter = spawn_trie_metrics_loop(
        config.clone(),
        storage.get_hot_store(),
        config.client_config.log_summary_period,
    )?;

    let epoch_manager =
        EpochManager::new_arc_handle(storage.get_hot_store(), &config.genesis.config);
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

    let shard_tracker =
        ShardTracker::new(TrackedConfig::from_config(&config.client_config), epoch_manager.clone());
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
            let view_epoch_manager =
                EpochManager::new_arc_handle(split_store.clone(), &config.genesis.config);
            let view_shard_tracker = ShardTracker::new(
                TrackedConfig::from_config(&config.client_config),
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
    let genesis_block =
        Chain::make_genesis_block(epoch_manager.as_ref(), runtime.as_ref(), &chain_genesis)?;
    let genesis_id = GenesisId {
        chain_id: config.client_config.chain_id.clone(),
        hash: *genesis_block.header().hash(),
    };

    // State Sync actors
    let client_adapter_for_sync = LateBoundSender::new();
    let network_adapter_for_sync = LateBoundSender::new();
    let sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
        client_adapter_for_sync.as_sender(),
        network_adapter_for_sync.as_sender(),
        SyncAdapter::actix_actor_maker(),
    )));

    let node_id = config.network_config.node_id();
    let network_adapter = LateBoundSender::new();
    let shards_manager_adapter = LateBoundSender::new();
    let client_adapter_for_shards_manager = LateBoundSender::new();
    let client_adapter_for_partial_witness_actor = LateBoundSender::new();
    let adv = near_client::adversarial::Controls::new(config.client_config.archive);

    let view_client_addr = ViewClientActorInner::spawn_actix_actor(
        Clock::real(),
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        chain_genesis.clone(),
        view_epoch_manager.clone(),
        view_shard_tracker,
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
        state_snapshot_sender.as_multi_sender(),
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

    let (partial_witness_actor, partial_witness_arbiter) = if config.validator_signer.is_some() {
        let my_signer = config.validator_signer.clone().unwrap();
        let (partial_witness_actor, partial_witness_arbiter) =
            spawn_actix_actor(PartialWitnessActor::new(
                Clock::real(),
                network_adapter.as_multi_sender(),
                client_adapter_for_partial_witness_actor.as_multi_sender(),
                my_signer,
                epoch_manager.clone(),
            ));
        (Some(partial_witness_actor), Some(partial_witness_arbiter))
    } else {
        (None, None)
    };

    let (_gc_actor, gc_arbiter) = spawn_actix_actor(GCActor::new(
        runtime.store().clone(),
        chain_genesis.height,
        runtime.clone(),
        epoch_manager.clone(),
        config.client_config.gc,
        config.client_config.archive,
    ));

    let StartClientResult { client_actor, client_arbiter_handle, resharding_handle } = start_client(
        Clock::real(),
        config.client_config.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        node_id,
        sync_adapter,
        network_adapter.as_multi_sender(),
        shards_manager_adapter.as_sender(),
        config.validator_signer.clone(),
        telemetry.with_auto_span_context().into_sender(),
        Some(snapshot_callbacks),
        shutdown_signal,
        adv,
        config_updater,
        partial_witness_actor
            .clone()
            .map(|actor| actor.with_auto_span_context().into_multi_sender())
            .unwrap_or_else(|| noop().into_multi_sender()),
    );
    if let SyncConfig::Peers = config.client_config.state_sync.sync {
        client_adapter_for_sync.bind(client_actor.clone().with_auto_span_context())
    };
    client_adapter_for_shards_manager.bind(client_actor.clone().with_auto_span_context());
    client_adapter_for_partial_witness_actor.bind(client_actor.clone().with_auto_span_context());
    let (shards_manager_actor, shards_manager_arbiter_handle) = start_shards_manager(
        epoch_manager.clone(),
        shard_tracker.clone(),
        network_adapter.as_sender(),
        client_adapter_for_shards_manager.as_sender(),
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        split_store.unwrap_or_else(|| storage.get_hot_store()),
        config.client_config.chunk_request_retry_period,
    );
    shards_manager_adapter.bind(shards_manager_actor.with_auto_span_context());

    let flat_state_migration_handle =
        FlatStateValuesInliningMigrationHandle::start_background_migration(
            storage.get_hot_store(),
            runtime.get_flat_storage_manager(),
            config.client_config.client_background_migration_threads,
        );

    let mut state_sync_dumper = StateSyncDumper {
        clock: Clock::real(),
        client_config: config.client_config.clone(),
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        account_id: config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        dump_future_runner: StateSyncDumper::arbiter_dump_future_runner(),
        handle: None,
    };
    state_sync_dumper.start()?;

    let hot_store = storage.get_hot_store();

    let mut rpc_servers = Vec::new();
    let network_actor = PeerManagerActor::spawn(
        time::Clock::real(),
        storage.into_inner(near_store::Temperature::Hot),
        config.network_config,
        client_sender_for_network(client_actor.clone(), view_client_addr.clone()),
        shards_manager_adapter.as_sender(),
        partial_witness_actor
            .map(|actor| actor.with_auto_span_context().into_multi_sender())
            .unwrap_or_else(|| noop().into_multi_sender()),
        genesis_id,
    )
    .context("PeerManager::spawn()")?;
    network_adapter.bind(network_actor.clone().with_auto_span_context());
    if let SyncConfig::Peers = config.client_config.state_sync.sync {
        network_adapter_for_sync.bind(network_actor.clone().with_auto_span_context())
    }
    #[cfg(feature = "json_rpc")]
    if let Some(rpc_config) = config.rpc_config {
        let entity_debug_handler = EntityDebugHandlerImpl {
            epoch_manager: view_epoch_manager,
            runtime: view_runtime,
            store: hot_store,
        };
        rpc_servers.extend(near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            client_actor.clone().with_auto_span_context().into_multi_sender(),
            view_client_addr.clone().with_auto_span_context().into_multi_sender(),
            network_actor.into_multi_sender(),
            #[cfg(feature = "test_features")]
            _gc_actor.into_multi_sender(),
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
    ];
    if let Some(db_metrics_arbiter) = db_metrics_arbiter {
        arbiters.push(db_metrics_arbiter);
    }
    if let Some(partial_witness_arbiter) = partial_witness_arbiter {
        arbiters.push(partial_witness_arbiter);
    }

    Ok(NearNode {
        client: client_actor,
        view_client: view_client_addr,
        rpc_servers,
        arbiters,
        cold_store_loop_handle,
        state_sync_dumper,
        flat_state_migration_handle,
        resharding_handle,
    })
}

pub struct RecompressOpts {
    pub dest_dir: PathBuf,
    pub keep_partial_chunks: bool,
    pub keep_invalid_chunks: bool,
    pub keep_trie_changes: bool,
}

pub fn recompress_storage(home_dir: &Path, opts: RecompressOpts) -> anyhow::Result<()> {
    use strum::IntoEnumIterator;

    let config_path = home_dir.join(config::CONFIG_FILENAME);
    let config = config::Config::from_file(&config_path)
        .map_err(|err| anyhow::anyhow!("{}: {}", config_path.display(), err))?;
    let archive = config.archive;
    let mut skip_columns = Vec::new();
    if archive && !opts.keep_partial_chunks {
        skip_columns.push(DBCol::PartialChunks);
    }
    if archive && !opts.keep_invalid_chunks {
        skip_columns.push(DBCol::InvalidChunks);
    }
    if archive && !opts.keep_trie_changes {
        skip_columns.push(DBCol::TrieChanges);
    }

    let src_opener = NodeStorage::opener(home_dir, archive, &config.store, None);
    let src_path = src_opener.path();

    let mut dst_config = config.store.clone();
    dst_config.path = Some(opts.dest_dir);
    // Note: opts.dest_dir is resolved relative to current working directory
    // (since it’s a command line option) which is why we set home to cwd.
    let cwd = std::env::current_dir()?;
    let dst_opener = NodeStorage::opener(&cwd, archive, &dst_config, None);
    let dst_path = dst_opener.path();

    info!(target: "recompress",
          src = %src_path.display(), dest = %dst_path.display(),
          "Recompressing database");

    info!("Opening database at {}", src_path.display());
    let src_store = src_opener.open_in_mode(Mode::ReadOnly)?.get_hot_store();

    let final_head_height = if skip_columns.contains(&DBCol::PartialChunks) {
        let tip: Option<near_primitives::block::Tip> =
            src_store.get_ser(DBCol::BlockMisc, near_store::FINAL_HEAD_KEY)?;
        anyhow::ensure!(
            tip.is_some(),
            "{}: missing {}; is this a freshly set up node? note that recompress_storage makes no sense on those",
            src_path.display(),
            std::str::from_utf8(near_store::FINAL_HEAD_KEY).unwrap(),
        );
        tip.map(|tip| tip.height)
    } else {
        None
    };

    info!("Creating database at {}", dst_path.display());
    let dst_store = dst_opener.open_in_mode(Mode::Create)?.get_hot_store();

    const BATCH_SIZE_BYTES: u64 = 150_000_000;

    for column in DBCol::iter() {
        let skip = skip_columns.contains(&column);
        info!(
            target: "recompress",
            column_id = column as usize,
            %column,
            "{}",
            if skip { "Clearing  " } else { "Processing" }
        );
        if skip {
            continue;
        }

        let mut store_update = dst_store.store_update();
        let mut total_written: u64 = 0;
        let mut batch_written: u64 = 0;
        let mut count_keys: u64 = 0;
        for item in src_store.iter_raw_bytes(column) {
            let (key, value) = item.with_context(|| format!("scanning column {column}"))?;
            store_update.set_raw_bytes(column, &key, &value);
            total_written += value.len() as u64;
            batch_written += value.len() as u64;
            count_keys += 1;
            if batch_written >= BATCH_SIZE_BYTES {
                store_update.commit()?;
                info!(
                    target: "recompress",
                    column_id = column as usize,
                    %count_keys,
                    %total_written,
                    "Processing",
                );
                batch_written = 0;
                store_update = dst_store.store_update();
            }
        }
        info!(
            target: "recompress",
            column_id = column as usize,
            %count_keys,
            %total_written,
            "Done with "
        );
        store_update.commit()?;
    }

    // If we’re not keeping DBCol::PartialChunks, update chunk tail to point to
    // current final block.  If we don’t do that, the gc will try to work its
    // way from the genesis even though chunks at those heights have been
    // deleted.
    if skip_columns.contains(&DBCol::PartialChunks) {
        let chunk_tail = final_head_height.unwrap();
        info!(target: "recompress", %chunk_tail, "Setting chunk tail");
        let mut store_update = dst_store.store_update();
        store_update.set_ser(DBCol::BlockMisc, near_store::CHUNK_TAIL_KEY, &chunk_tail)?;
        store_update.commit()?;
    }

    core::mem::drop(dst_store);
    core::mem::drop(src_store);

    info!(target: "recompress", dest = %dst_path.display(), "Database recompressed");
    Ok(())
}
