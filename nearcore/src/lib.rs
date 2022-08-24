pub use crate::config::{init_configs, load_config, load_test_config, NearConfig, NEAR_BASE};
use crate::migrations::migrate_30_to_31;
pub use crate::runtime::NightshadeRuntime;
pub use crate::shard_tracker::TrackedConfig;
use actix::{Actor, Addr, Arbiter};
use actix_rt::ArbiterHandle;
use actix_web;
use anyhow::Context;
use near_chain::{Chain, ChainGenesis};
use near_client::{start_client, start_view_client, ClientActor, ViewClientActor};
use near_network::types::NetworkRecipient;
use near_network::PeerManagerActor;
use near_network_primitives::time;
use near_primitives::block::GenesisId;
use near_primitives::version::DbVersion;
#[cfg(feature = "performance_stats")]
use near_rust_allocator_proxy::reset_memory_usage_max;
use near_store::migrations::{migrate_28_to_29, migrate_29_to_30, set_store_version};
use near_store::{DBCol, Mode, Store, StoreOpener};
use near_telemetry::TelemetryActor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{info, trace};

pub mod append_only_map;
pub mod config;
mod download_file;
mod metrics;
pub mod migrations;
mod runtime;
mod shard_tracker;

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

/// Returns the path of the DB checkpoint.
/// Default location is the same as the database location: `path`.
fn db_checkpoint_path(path: &Path, near_config: &NearConfig) -> PathBuf {
    let root_path =
        if let Some(db_migration_snapshot_path) = &near_config.config.db_migration_snapshot_path {
            assert!(
                db_migration_snapshot_path.is_absolute(),
                "'db_migration_snapshot_path' must be an absolute path to an existing directory."
            );
            db_migration_snapshot_path.clone()
        } else {
            path.to_path_buf()
        };
    root_path.join(DB_CHECKPOINT_NAME)
}

const DB_CHECKPOINT_NAME: &str = "db_migration_snapshot";

/// Creates a consistent DB checkpoint and returns its path.
/// By default it creates checkpoints in the DB directory, but can be overridden by the config.
fn create_db_checkpoint(
    opener: &StoreOpener,
    near_config: &NearConfig,
) -> anyhow::Result<near_store::Snapshot> {
    match opener.new_migration_snapshot(db_checkpoint_path(opener.path(), near_config)) {
        Ok(snapshot) => Ok(snapshot),
        Err(near_store::SnapshotError::AlreadyExists(snap_path)) => {
            Err(anyhow::anyhow!(
                "Detected an existing database migration snapshot at ‘{}’.\n\
                 Probably a database migration got interrupted and your database is corrupted.\n\
                 Please replace files in ‘{}’ with contents of the snapshot, delete the snapshot and try again.",
                snap_path.display(),
                opener.path().display(),
            ))
        }
        Err(near_store::SnapshotError::IOError(err)) => {
            Err(anyhow::anyhow!(
                "Failed to create a database migration snapshot: {err}.\n\
                 You can change the location of the snapshot by adjusting `config.json`:\n\
                 \t\"db_migration_snapshot_path\": \"/absolute/path/to/existing/dir\",\n\
                 Alternatively, you can disable database migration snapshots in `config.json`:\n\
                 \t\"use_db_migration_snapshot\": false,"
            ))
        }
    }
}

/// Checks current version of the database and applies migrations if needed.
///
/// Returns whether the database exists in the first place.  If it doesn’t,
/// returns false and does nothing.  If it does, performs any necessary
/// migrations and returns true.
///
/// Other than regular database errors, returns an error if the database has
/// unsupported version: either too far in the past or a future version.
fn apply_store_migrations_if_exists(
    store_opener: &StoreOpener,
    near_config: &NearConfig,
) -> anyhow::Result<bool> {
    let db_version = match store_opener.get_version_if_exists()? {
        None => return Ok(false),
        Some(near_primitives::version::DB_VERSION) => return Ok(true),
        Some(db_version) => db_version,
    };

    anyhow::ensure!(
        db_version < near_primitives::version::DB_VERSION,
        "DB version {db_version} is created by a newer version of neard, \
         please update neard"
    );

    // For given db version, latest neard release which supported that version.
    // If you’re removing support for a database version from neard put an entry
    // here so that we can inform user which version they need.
    const LATEST_DB_SUPPORTED: [(DbVersion, &'static str); 1] = [(26, "1.26")];
    if let Some((_, release)) =
        LATEST_DB_SUPPORTED.iter().filter(|(ver, _)| db_version <= *ver).next()
    {
        anyhow::bail!(
            "DB version {db_version} is created by an ancient version of neard \
             and is no longer supported by this version, please migrate using \
             {release} release"
        );
    }

    // Before starting a DB migration, create a snapshot of the database.  If
    // the migration fails, the snapshot can be used to restore the database to
    // its original state.
    let snapshot = if near_config.config.use_db_migration_snapshot {
        Some(create_db_checkpoint(store_opener, near_config)?)
    } else {
        None
    };

    // Add migrations here based on `db_version`.
    if db_version <= 26 {
        // Unreachable since we should have bailed when checking
        // LATEST_DB_SUPPORTED above.
        unreachable!();
    }
    if db_version <= 27 {
        // version 27 => 28: add DBCol::StateChangesForSplitStates
        // Does not need to do anything since open db with option
        // `create_missing_column_families`.  Nevertheless need to bump db
        // version, because db_version 27 binary can't open db_version 28 db.
        // Combine it with migration from 28 to 29; don’t do anything here.
    }
    if db_version <= 28 {
        // version 28 => 29: delete ColNextBlockWithNewChunk, ColLastBlockWithNewChunk
        info!(target: "near", "Migrate DB from version 28 to 29");
        migrate_28_to_29(store_opener);
    }
    if db_version <= 29 {
        // version 29 => 30: migrate all structures that use ValidatorStake to versionized version
        info!(target: "near", "Migrate DB from version 29 to 30");
        migrate_29_to_30(store_opener);
    }
    if db_version <= 30 {
        // version 30 => 31: recompute block ordinal due to a bug fixed in #5761
        info!(target: "near", "Migrate DB from version 30 to 31");
        migrate_30_to_31(store_opener, &near_config);
    }

    if cfg!(feature = "nightly") || cfg!(feature = "nightly_protocol") {
        let store = store_opener
            .open()
            .with_context(|| format!("Opening database at {}", store_opener.path().display()))?;
        // set some dummy value to avoid conflict with other migrations from nightly features
        set_store_version(&store, 10000);
    } else {
        debug_assert_eq!(
            Some(near_primitives::version::DB_VERSION),
            store_opener.get_version_if_exists()?
        );
    }

    // DB migration was successful, remove the snapshot to avoid it taking up
    // precious disk space.
    snapshot.map(near_store::Snapshot::remove);

    Ok(true)
}

fn init_and_migrate_store(home_dir: &Path, near_config: &NearConfig) -> anyhow::Result<Store> {
    let opener = Store::opener(home_dir, &near_config.config.store);
    let exists = apply_store_migrations_if_exists(&opener, near_config)?;
    let store = opener
        .open()
        .with_context(|| format!("Opening database at {}", opener.path().display()))?;
    if !exists {
        set_store_version(&store, near_primitives::version::DB_VERSION);
    }

    // Check if the storage is an archive and if it is make sure we are too.
    // If the store is not marked as archive but we are an archival node that is
    // fine and we just need to mark the store as archival.
    let store_is_archive: bool =
        store.get_ser(DBCol::BlockMisc, near_store::db::IS_ARCHIVE_KEY)?.unwrap_or_default();
    let client_is_archive = near_config.client_config.archive;
    anyhow::ensure!(
        !store_is_archive || client_is_archive,
        "The node is configured as non-archival but is using database of an archival node."
    );
    if !store_is_archive && client_is_archive {
        let mut update = store.store_update();
        update.set_ser(DBCol::BlockMisc, near_store::db::IS_ARCHIVE_KEY, &true)?;
        update.commit()?;
    }

    Ok(store)
}

pub struct NearNode {
    pub client: Addr<ClientActor>,
    pub view_client: Addr<ViewClientActor>,
    pub arbiters: Vec<ArbiterHandle>,
    pub rpc_servers: Vec<(&'static str, actix_web::dev::ServerHandle)>,
}

pub fn start_with_config(home_dir: &Path, config: NearConfig) -> anyhow::Result<NearNode> {
    start_with_config_and_synchronization(home_dir, config, None)
}

pub fn start_with_config_and_synchronization(
    home_dir: &Path,
    config: NearConfig,
    // 'shutdown_signal' will notify the corresponding `oneshot::Receiver` when an instance of
    // `ClientActor` gets dropped.
    shutdown_signal: Option<oneshot::Sender<()>>,
) -> anyhow::Result<NearNode> {
    let store = init_and_migrate_store(home_dir, &config)?;

    let runtime = Arc::new(NightshadeRuntime::from_config(home_dir, store.clone(), &config));

    let telemetry = TelemetryActor::new(config.telemetry_config.clone()).start();
    let chain_genesis = ChainGenesis::new(&config.genesis);
    let genesis_block = Chain::make_genesis_block(&*runtime, &chain_genesis)?;
    let genesis_id = GenesisId {
        chain_id: config.client_config.chain_id.clone(),
        hash: genesis_block.header().hash().clone(),
    };

    let node_id = config.network_config.node_id();
    let network_adapter = Arc::new(NetworkRecipient::default());
    let adv = near_client::adversarial::Controls::new(config.client_config.archive);

    let view_client = start_view_client(
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        chain_genesis.clone(),
        runtime.clone(),
        network_adapter.clone(),
        config.client_config.clone(),
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
        shutdown_signal,
        adv,
    );

    #[allow(unused_mut)]
    let mut rpc_servers = Vec::new();
    let arbiter = Arbiter::new();
    let network_actor = PeerManagerActor::start_in_arbiter(&arbiter.handle(), {
        let client_actor = client_actor.clone();
        let view_client = view_client.clone();
        move |_ctx| {
            PeerManagerActor::new(
                time::Clock::real(),
                store,
                config.network_config,
                client_actor.recipient(),
                view_client.recipient(),
                genesis_id,
            )
            .unwrap()
        }
    });
    network_adapter.set_recipient(network_actor.clone());

    #[cfg(feature = "json_rpc")]
    if let Some(rpc_config) = config.rpc_config {
        rpc_servers.extend(near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            client_actor.clone(),
            view_client.clone(),
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
                view_client.clone(),
            ),
        ));
    }

    rpc_servers.shrink_to_fit();

    trace!(target: "diagnostic", key="log", "Starting NEAR node with diagnostic activated");

    // We probably reached peak memory once on this thread, we want to see when it happens again.
    #[cfg(feature = "performance_stats")]
    reset_memory_usage_max();

    Ok(NearNode {
        client: client_actor,
        view_client,
        rpc_servers,
        arbiters: vec![client_arbiter_handle, arbiter.handle()],
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

    let src_opener = Store::opener(home_dir, &config.store).mode(Mode::ReadOnly);
    let src_path = src_opener.path();
    if let Some(db_version) = src_opener.get_version_if_exists()? {
        anyhow::ensure!(
            db_version == near_primitives::version::DB_VERSION,
            "{}: expected DB version {} but got {}",
            src_path.display(),
            near_primitives::version::DB_VERSION,
            db_version
        );
    } else {
        anyhow::bail!("{}: source storage doesn’t exist", src_path.display());
    }

    let mut dst_config = config.store.clone();
    dst_config.path = Some(opts.dest_dir);
    // Note: opts.dest_dir is resolved relative to current working directory
    // (since it’s a command line option) which is why we set home to cwd.
    let cwd = std::env::current_dir()?;
    let dst_opener = Store::opener(&cwd, &dst_config);
    let dst_path = dst_opener.path();
    anyhow::ensure!(
        !dst_opener.check_if_exists(),
        "{}: directory already exists",
        dst_path.display()
    );

    info!(target: "recompress",
          src = %src_path.display(), dest = %dst_path.display(),
          "Recompressing database");

    let src_store = src_opener
        .open()
        .with_context(|| format!("Opening database at {}", src_opener.path().display()))?;

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

    let dst_store = dst_opener
        .open()
        .with_context(|| format!("Opening database at {}", dst_opener.path().display()))?;

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
