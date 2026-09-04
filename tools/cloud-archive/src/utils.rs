use anyhow::Context;
use near_async::ActorSystem;
use near_async::messaging::{Actor, Handler, IntoMultiSender, noop};
use near_async::time::{Clock, Utc};
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, GenesisValidationMode};
#[cfg(feature = "test_features")]
use near_client::NetworkAdversarialMessage;
use near_client::ViewClientActor;
use near_client::adversarial::Controls;
use near_client_primitives::debug::{DebugStatus, DebugStatusResponse};
use near_client_primitives::types::{
    GetClientConfig, GetClientConfigError, GetNetworkInfo, NetworkInfoResponse, Status, StatusError,
};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_jsonrpc::sharded_rpc::ShardedRpcPool;
use near_jsonrpc::start_http;
use near_jsonrpc_primitives::types::entity_debug::DummyEntityDebugHandler;
use near_network::client::{ProcessTxRequest, ProcessTxResponse};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{StatusResponse, StatusSyncInfo};
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

/// Answers read-only JSON-RPC from a store no other process holds.
#[derive(clap::Parser)]
pub(crate) struct ServeCmd {}

impl ServeCmd {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        // Serving reaches no bucket, so this opens the store alone rather than through
        // `ReaderHandles`, and without a cloud storage context, which only builds a client
        // the serve path never calls and refuses a location it cannot support. The view
        // client writes the genesis congestion infos as it starts, and the write lock
        // keeps a second process off the store.
        let near_config =
            load_config(home_dir, genesis_validation).context("failed to load config")?;
        let runtime = Runtime::new().expect("failed to create the tokio runtime");
        let storage = {
            let _runtime_guard = runtime.enter();
            NodeStorage::opener(
                home_dir,
                &near_config.config.store,
                near_config.config.cold_store.as_ref(),
                None,
            )
            .open_in_mode(Mode::ReadWrite)
            .context("failed to open storage")?
        };
        let store = storage.get_hot_store();
        serve_store_while(home_dir, &near_config, store, &runtime, || {
            runtime.block_on(async { ctrl_c().await.ok() });
            Ok(())
        })
    }
}

/// The genesis block's hash, or the default when the store starts above genesis.
fn genesis_hash(store: &Store, near_config: &NearConfig) -> CryptoHash {
    store
        .chain_store()
        .get_block_hash_by_height(near_config.genesis.config.genesis_height)
        .unwrap_or_default()
}

/// Answers the node-level questions a reader can answer from its own store.
struct ReaderNodeActor {
    store: Store,
    near_config: NearConfig,
    genesis_hash: CryptoHash,
    started_at: Utc,
}

impl Actor for ReaderNodeActor {}

impl Handler<SpanWrapped<Status>, Result<StatusResponse, StatusError>> for ReaderNodeActor {
    fn handle(&mut self, _msg: SpanWrapped<Status>) -> Result<StatusResponse, StatusError> {
        let head: Tip = self.store.get_ser(DBCol::BlockMisc, HEAD_KEY).ok_or_else(|| {
            StatusError::InternalError { error_message: "the store has no head".to_string() }
        })?;
        let header = self
            .store
            .chain_store()
            .get_block_header(&head.last_block_hash)
            .map_err(|error| StatusError::InternalError { error_message: error.to_string() })?;
        Ok(StatusResponse {
            version: self.near_config.client_config.version.clone(),
            chain_id: self.near_config.genesis.config.chain_id.clone(),
            protocol_version: header.latest_protocol_version(),
            latest_protocol_version: PROTOCOL_VERSION,
            rpc_addr: self.near_config.rpc_config.as_ref().map(|rpc| rpc.addr.to_string()),
            validators: vec![],
            sync_info: StatusSyncInfo {
                latest_block_hash: *header.hash(),
                latest_block_height: header.height(),
                latest_state_root: *header.prev_state_root(),
                latest_block_time: header.timestamp(),
                // The head is what the reader copied, so it is behind the chain by design
                // and reporting it as syncing would keep a probe waiting forever.
                syncing: false,
                earliest_block_hash: None,
                earliest_block_height: None,
                earliest_block_time: None,
                epoch_id: Some(*header.epoch_id()),
                epoch_start_height: None,
            },
            validator_account_id: None,
            validator_public_key: None,
            node_public_key: self.near_config.network_config.node_key.public_key().clone(),
            node_key: None,
            uptime_sec: (Clock::real().now_utc() - self.started_at).whole_seconds(),
            genesis_hash: self.genesis_hash,
            detailed_debug_status: None,
        })
    }
}

impl Handler<SpanWrapped<GetClientConfig>, Result<ClientConfig, GetClientConfigError>>
    for ReaderNodeActor
{
    fn handle(
        &mut self,
        _msg: SpanWrapped<GetClientConfig>,
    ) -> Result<ClientConfig, GetClientConfigError> {
        Ok(self.near_config.client_config.clone())
    }
}

impl Handler<SpanWrapped<GetNetworkInfo>, Result<NetworkInfoResponse, String>> for ReaderNodeActor {
    fn handle(&mut self, _msg: SpanWrapped<GetNetworkInfo>) -> Result<NetworkInfoResponse, String> {
        Err("a cloud archive reader joins no network".to_string())
    }
}

impl Handler<DebugStatus, Result<DebugStatusResponse, StatusError>> for ReaderNodeActor {
    fn handle(&mut self, _msg: DebugStatus) -> Result<DebugStatusResponse, StatusError> {
        Err(StatusError::InternalError {
            error_message: "a cloud archive reader runs no client to report on".to_string(),
        })
    }
}

#[cfg(feature = "test_features")]
impl Handler<NetworkAdversarialMessage> for ReaderNodeActor {
    fn handle(&mut self, _msg: NetworkAdversarialMessage) {}
}

#[cfg(feature = "test_features")]
impl Handler<NetworkAdversarialMessage, Option<u64>> for ReaderNodeActor {
    fn handle(&mut self, _msg: NetworkAdversarialMessage) -> Option<u64> {
        None
    }
}

impl Handler<ProcessTxRequest, ProcessTxResponse> for ReaderNodeActor {
    fn handle(&mut self, _msg: ProcessTxRequest) -> ProcessTxResponse {
        ProcessTxResponse::DoesNotTrackShard
    }
}

impl Handler<ProcessTxRequest> for ReaderNodeActor {
    fn handle(&mut self, _msg: ProcessTxRequest) {}
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

    let reader_node = actor_system.spawn_tokio_actor(ReaderNodeActor {
        store: store.clone(),
        near_config: near_config.clone(),
        genesis_hash: genesis_hash(&store, &near_config),
        started_at: Clock::real().now_utc(),
    });

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
            reader_node.clone().into_multi_sender(),
            view_client_addr.into_multi_sender(),
            reader_node.into_multi_sender(),
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
