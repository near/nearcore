//! NEAR Indexer is a micro-framework, which provides you with a stream of blocks that are recorded on NEAR network.
//!
//! NEAR Indexer is useful to handle real-time "events" on the chain.
//!
//! NEAR Indexer is going to be used to build NEAR Explorer, augment NEAR Wallet, and provide overview of events in Rainbow Bridge.
//!
//! See the [example] for further details.
//!
//! [example]: https://github.com/nearprotocol/nearcore/tree/master/tools/indexer/example
use tokio::sync::mpsc;

pub use neard::{get_default_home, init_configs, NearConfig};
mod streamer;

pub use self::streamer::{
    IndexerChunkView, IndexerExecutionOutcomeWithOptionalReceipt,
    IndexerExecutionOutcomeWithReceipt, IndexerShard, IndexerTransactionWithOutcome,
    StreamerMessage,
};
pub use near_primitives;

/// Config wrapper to simplify signature and usage of `neard::init_configs`
/// function by making args more explicit via struct
#[derive(Debug, Clone)]
pub struct InitConfigArgs {
    /// chain/network id (localnet, testnet, devnet, betanet)
    pub chain_id: Option<String>,
    /// Account ID for the validator key
    pub account_id: Option<String>,
    /// Specify private key generated from seed (TESTING ONLY)
    pub test_seed: Option<String>,
    /// Number of shards to initialize the chain with
    pub num_shards: u64,
    /// Makes block production fast (TESTING ONLY)
    pub fast: bool,
    /// Genesis file to use when initialize testnet (including downloading)
    pub genesis: Option<String>,
    /// Download the verified NEAR genesis file automatically.
    pub download: bool,
    /// Specify a custom download URL for the genesis-file.
    pub download_genesis_url: Option<String>,
}

/// Enum to define a mode of syncing for NEAR Indexer
#[derive(Debug, Clone)]
pub enum SyncModeEnum {
    /// Real-time syncing, always taking the latest finalized block to stream
    LatestSynced,
    /// Starts syncing from the block NEAR Indexer was interrupted last time
    FromInterruption,
    /// Specific block height to start syncing from
    BlockHeight(u64),
}

/// Enum to define whether await for node to be fully synced or stream while syncing (useful for indexing from genesis)
#[derive(Debug, Clone)]
pub enum AwaitForNodeSyncedEnum {
    /// Don't stream until the node is fully synced
    WaitForFullSync,
    /// Stream while node is syncing
    StreamWhileSyncing,
}

/// NEAR Indexer configuration to be provided to `Indexer::new(IndexerConfig)`
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Path to `home_dir` where configs and keys can be found
    pub home_dir: std::path::PathBuf,
    /// Mode of syncing for NEAR Indexer instance
    pub sync_mode: SyncModeEnum,
    /// Whether await for node to be synced or not
    pub await_for_node_synced: AwaitForNodeSyncedEnum,
}

/// This is the core component, which handles `nearcore` and internal `streamer`.
pub struct Indexer {
    indexer_config: IndexerConfig,
    near_config: neard::NearConfig,
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
}

impl Indexer {
    /// Initialize Indexer by configuring `nearcore`
    pub fn new(indexer_config: IndexerConfig) -> Self {
        let near_config = neard::load_config(&indexer_config.home_dir);
        neard::genesis_validate::validate_genesis(&near_config.genesis);
        assert!(
            !&near_config.client_config.tracked_shards.is_empty(),
            "Indexer should track at least one shard. \n\
            Tip: You may want to update {} with `\"tracked_shards\": [0]`
            ",
            indexer_config.home_dir.join("config.json").display()
        );
        let (client, view_client, _) =
            neard::start_with_config(&indexer_config.home_dir, near_config.clone());
        Self { view_client, client, near_config, indexer_config }
    }

    /// Boots up `near_indexer::streamer`, so it monitors the new blocks with chunks, transactions, receipts, and execution outcomes inside. The returned stream handler should be drained and handled on the user side.
    pub fn streamer(&self) -> mpsc::Receiver<streamer::StreamerMessage> {
        let (sender, receiver) = mpsc::channel(16);
        actix::spawn(streamer::start(
            self.view_client.clone(),
            self.client.clone(),
            self.indexer_config.clone(),
            sender,
        ));
        receiver
    }

    /// Expose neard config
    pub fn near_config(&self) -> &neard::NearConfig {
        &self.near_config
    }

    /// Internal client actors just in case. Use on your own risk, backward compatibility is not guaranteed
    pub fn client_actors(
        &self,
    ) -> (actix::Addr<near_client::ViewClientActor>, actix::Addr<near_client::ClientActor>) {
        (self.view_client.clone(), self.client.clone())
    }
}

/// Function that initializes configs for the node which
/// accepts `InitConfigWrapper` and calls original `init_configs` from `neard`
pub fn indexer_init_configs(dir: &std::path::PathBuf, params: InitConfigArgs) {
    init_configs(
        dir,
        params.chain_id.as_deref(),
        params.account_id.as_deref(),
        params.test_seed.as_deref(),
        params.num_shards,
        params.fast,
        params.genesis.as_deref(),
        params.download,
        params.download_genesis_url.as_deref(),
    )
}
