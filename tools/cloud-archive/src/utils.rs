use anyhow::Context;
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, noop};
use near_async::time::Clock;
use near_chain::ChainGenesis;
use near_chain_configs::GenesisValidationMode;
use near_client::ViewClientActor;
use near_client::adversarial::Controls;
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_jsonrpc::sharded_rpc::ShardedRpcPool;
use near_jsonrpc::start_http;
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use near_primitives::block::Tip;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::db::{FINAL_HEAD_KEY, HEAD_KEY};
use near_store::{DBCol, Mode, NodeStorage, Store};
use nearcore::config::load_config;
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::signal::ctrl_c;
use tokio::sync::watch;

/// What a reader command works through: the local store, the archive's bucket, and the
/// chain views that say which shards and epochs the reader is responsible for.
pub(crate) struct ReaderHandles {
    /// Holds the handle the bucket's HTTP client captured, so it outlives every retrieval.
    pub runtime: Runtime,
    pub near_config: NearConfig,
    pub store: Store,
    pub cloud_storage: Arc<CloudStorage>,
    pub shard_tracker: ShardTracker,
}

impl ReaderHandles {
    /// Loads the node's configuration and opens the store and the bucket it names.
    pub fn open(
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<Self> {
        let near_config =
            load_config(home_dir, genesis_validation).context("failed to load config")?;
        let cloud_storage_context = near_config
            .cloud_storage_context()
            .context("cloud_archival not configured in config.json")?;

        // Opening cloud storage builds an HTTP client that captures a runtime handle, and
        // this command is not async.
        let runtime = Runtime::new().expect("failed to create the tokio runtime");
        let storage = {
            let _runtime_guard = runtime.enter();
            NodeStorage::opener(
                home_dir,
                &near_config.config.store,
                near_config.config.cold_store.as_ref(),
                Some(cloud_storage_context),
            )
            .open_in_mode(Mode::ReadWrite)
            .context("failed to open storage")?
        };

        let store = storage.get_hot_store();
        let cloud_storage =
            storage.get_cloud_storage().context("cloud storage not available")?.clone();

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        let shard_tracker = ShardTracker::new(
            near_config.client_config.tracked_shards_config.clone(),
            epoch_manager.clone(),
            near_config.validator_signer.clone(),
        );

        Ok(Self { runtime, near_config, store, cloud_storage, shard_tracker })
    }
}

/// Answers read-only JSON-RPC from a store nothing is writing.
#[derive(clap::Parser)]
pub(crate) struct ServeCmd {}

impl ServeCmd {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        // Serving reaches no bucket, so this opens the store alone rather than through
        // `ReaderHandles`.
        let near_config =
            load_config(home_dir, genesis_validation).context("failed to load config")?;
        let runtime = Runtime::new().expect("failed to create the tokio runtime");
        let storage = {
            let _runtime_guard = runtime.enter();
            NodeStorage::opener(
                home_dir,
                &near_config.config.store,
                near_config.config.cold_store.as_ref(),
                near_config.cloud_storage_context(),
            )
            .open_in_mode(Mode::ReadOnly)
            .context("failed to open storage")?
        };
        let store = storage.get_hot_store();
        serve_store_while(home_dir, &near_config, store, &runtime, || {
            runtime.block_on(async { ctrl_c().await.ok() });
            Ok(())
        })
    }
}

/// Answers read-only JSON-RPC from `store`, then runs `block_until_done` on the caller's
/// thread, because `neard` installs a subcommand's log subscriber thread-locally.
pub(crate) fn serve_store_while(
    home_dir: &Path,
    near_config: &NearConfig,
    store: Store,
    runtime: &Runtime,
    block_until_done: impl FnOnce() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let rpc_config = near_config
        .rpc_config
        .clone()
        .ok_or_else(|| anyhow::anyhow!("JSON-RPC is not configured in config.json"))?;

    log_served_heads(&store);

    let epoch_manager =
        EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config, Some(home_dir));
    let shard_tracker = ShardTracker::new(
        near_config.client_config.tracked_shards_config.clone(),
        epoch_manager.clone(),
        near_config.validator_signer.clone(),
    );
    let nightshade_runtime = NightshadeRuntime::from_config(
        home_dir,
        store.clone(),
        near_config,
        epoch_manager.clone(),
    )?;

    let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
    let actor_system = ActorSystem::new();

    // Only the view path is wired, so an endpoint reaching for the client sender answers
    // with a dropped-send error rather than hanging.
    let view_client_addr = ViewClientActor::spawn_multithread_actor(
        Clock::real(),
        actor_system.clone(),
        chain_genesis,
        epoch_manager,
        shard_tracker.clone(),
        nightshade_runtime,
        noop().into_multi_sender(),
        near_config.client_config.clone(),
        Controls::new(near_config.client_config.archive),
        near_config.validator_signer.clone(),
    );

    let sharded_rpc_pool = Arc::new(RwLock::new(ShardedRpcPool::new(
        rpc_config.sharded_rpc.clone(),
        shard_tracker,
        store.chain_store(),
    )));

    runtime.block_on(async {
        start_http(
            Clock::real(),
            rpc_config,
            near_config.genesis.config.clone(),
            noop().into_multi_sender(),
            view_client_addr.into_multi_sender(),
            noop().into_multi_sender(),
            noop().into_multi_sender(),
            watch::channel(None).1,
            #[cfg(feature = "test_features")]
            noop().into_multi_sender(),
            Arc::new(DummyEntityDebugHandler {}),
            sharded_rpc_pool,
            actor_system.new_future_spawner("jsonrpc").as_ref(),
        )
        .await;
    });
    tracing::info!(target: "cloud_archival", "serving the cloud archive");

    block_until_done()
}

/// Logs the heads the store answers a query at, so an operator sees them before the first
/// request rather than inferring them from an answer.
fn log_served_heads(store: &Store) {
    let head: Option<Tip> = store.get_ser(DBCol::BlockMisc, HEAD_KEY);
    let final_head: Option<Tip> = store.get_ser(DBCol::BlockMisc, FINAL_HEAD_KEY);
    let (Some(head), Some(final_head)) = (&head, &final_head) else {
        tracing::warn!(
            target: "cloud_archival",
            head = head.is_some(),
            final_head = final_head.is_some(),
            "a head is missing, so queries answer at genesis"
        );
        return;
    };
    tracing::info!(
        target: "cloud_archival",
        head_height = head.height,
        final_head_height = final_head.height,
        "serving from this chain state"
    );
}
