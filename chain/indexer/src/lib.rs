#![doc = include_str!("../README.md")]

use anyhow::Context;
use tokio::sync::{mpsc, oneshot};

use near_chain_configs::GenesisValidationMode;
pub use near_primitives;
use near_primitives::types::Gas;
pub use nearcore::{get_default_home, init_configs, NearConfig};

pub use near_indexer_primitives::{
    IndexerChunkView, IndexerExecutionOutcomeWithOptionalReceipt,
    IndexerExecutionOutcomeWithReceipt, IndexerShard, IndexerTransactionWithOutcome,
    StreamerMessage,
};

pub use streamer::build_streamer_message;
pub use streamer::errors::IndexerError;

mod streamer;

pub const INDEXER: &str = "indexer";

/// Config wrapper to simplify signature and usage of `nearcore::init_configs`
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
    /// Genesis file to use when initializing testnet (including downloading)
    pub genesis: Option<String>,
    /// Download the verified NEAR genesis file automatically.
    pub download_genesis: bool,
    /// Specify a custom download URL for the genesis file.
    pub download_genesis_url: Option<String>,
    /// Specify a custom download URL for the records file.
    pub download_records_url: Option<String>,
    /// Download the verified NEAR config file automatically.
    pub download_config: bool,
    /// Specify a custom download URL for the config file.
    pub download_config_url: Option<String>,
    /// Specify the boot nodes to bootstrap the network
    pub boot_nodes: Option<String>,
    /// Specify a custom max_gas_burnt_view limit.
    pub max_gas_burnt_view: Option<Gas>,
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
    /// Tells whether to validate the genesis file before starting
    pub validate_genesis: bool,
    /// Makes indexer to skip block if the local delayed receipt is not found when execution outcome is observed
    pub ignore_missing_local_delayed_receipt: bool,
}

/// This is the core component, which handles `nearcore` and internal `streamer`.
pub struct Indexer {
    indexer_config: IndexerConfig,
    near_config: nearcore::NearConfig,
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
}

impl Indexer {
    /// Initialize Indexer by configuring `nearcore`
    pub fn new(indexer_config: IndexerConfig) -> Result<Self, anyhow::Error> {
        tracing::info!(
            target: INDEXER,
            "Load config from {}...",
            indexer_config.home_dir.display()
        );

        let genesis_validation_mode = if indexer_config.validate_genesis {
            GenesisValidationMode::Full
        } else {
            GenesisValidationMode::UnsafeFast
        };
        let near_config =
            nearcore::config::load_config(&indexer_config.home_dir, genesis_validation_mode)
                .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        assert!(
            !&near_config.client_config.tracked_shards.is_empty(),
            "Indexer should track at least one shard. \n\
            Tip: You may want to update {} with `\"tracked_shards\": [0]`
            ",
            indexer_config.home_dir.join("config.json").display()
        );
        let nearcore::NearNode { client, view_client, .. } =
            nearcore::start_with_config(&indexer_config.home_dir, near_config.clone())
                .with_context(|| "start_with_config")?;
        Ok(Self { view_client, client, near_config, indexer_config })
    }

    /// Boots up `near_indexer::streamer`, so it monitors the new blocks with chunks, transactions,
    /// receipts, and execution outcomes inside. The returned stream handler should be drained and handled on the user side.
    pub fn streamer(self) -> mpsc::Receiver<StreamerMessage> {
        let (sender, receiver) = self.streamer_channel();
        let (result_sender, result_receiver) = oneshot::channel();

        actix::spawn(async move {
            let result = streamer::start(
                self.view_client.clone(),
                self.client.clone(),
                self.indexer_config.clone(),
                self.near_config.config.store.clone(),
                self.near_config.config.archive,
                sender,
            )
            .await;

            // Send the result of the `streamer::start` to the oneshot channel
            let _ = result_sender.send(result);
        });

        // Spawn a separate task to log the error from the oneshot receiver
        // This is necessary because the `actix::spawn` will panic if the receiver is dropped
        actix::spawn(async move {
            match result_receiver.await {
                Ok(Err(e)) => {
                    tracing::error!(target: "INDEXER", "Indexer failed with error: {:?}", &e);
                    panic!("Indexer failed with error: {:?}", &e);
                }
                Err(err) => {
                    tracing::error!(target: "INDEXER", "Received unexpected error from oneshot channel: {:?}", err);
                    panic!(
                        "Indexer failed because of unexpected error from oneshot channel: {:?}",
                        err
                    );
                }
                Ok(Ok(_)) => {
                    // This branch is theoretically unreachable, but we handle it just in case
                    tracing::error!(target: "INDEXER", "Indexer stopped. Indexed blocks are not being streamed anymore.");
                    panic!("Indexer stopped. Indexed blocks are not being streamed anymore.");
                }
            }
        });

        // Return the receiver immediately
        receiver
    }

    /// Expose neard config
    pub fn near_config(&self) -> &nearcore::NearConfig {
        &self.near_config
    }

    /// Internal client actors just in case. Use on your own risk, backward compatibility is not guaranteed
    pub fn client_actors(
        &self,
    ) -> (actix::Addr<near_client::ViewClientActor>, actix::Addr<near_client::ClientActor>) {
        (self.view_client.clone(), self.client.clone())
    }

    /// Creates a pair of sender and receiver for the streamer
    pub fn streamer_channel(
        &self,
    ) -> (mpsc::Sender<StreamerMessage>, mpsc::Receiver<StreamerMessage>) {
        mpsc::channel(100)
    }

    /// Starts the streamer with the provided sender
    /// This function is useful when you want to control the streamer from the outside.
    /// This allows to catch errors, handle them and restart the streamer if needed
    ///
    /// Example:
    /// ```ignore
    /// let system = actix::System::new();
    ///     system.block_on(async move {
    ///         let indexer = near_indexer::Indexer::new(indexer_config).expect("Indexer::new()");
    ///         // Get the sender and receiver explicitly
    ///         let (sender, receiver) = indexer.streamer_channel();
    ///         // Spawning the job that will be listening to the blocks from the receiver
    ///         actix::spawn(listen_blocks(receiver)); // The function where you listen the receiver
    ///         // Start the streamer which will be pushing the StreamerMessage to the sender,
    ///         // and wait for it to finish (it's not going to happen though)
    ///         // but in might end up in an error, so we need to handle it
    ///         match indexer.start_streamer(sender).await {
    ///             Ok(_) => tracing::info!(target: "indexer_example", "Streamer finished successfully"),
    ///             Err(e) => {
    ///                 tracing::error!(target: "indexer_example", "Streamer finished with error: {}", e)
    ///             }
    ///         };
    ///    });
    /// system.run()?;
    /// ```
    ///
    /// Alternatively, you can use now use it in the async main function:
    /// ```ignore
    /// #[actix::main]
    /// async fn main() -> Result<(), anyhow::Error> {
    ///    let indexer = near_indexer::Indexer::new(indexer_config)?;
    ///    let (sender, receiver) = indexer.streamer_channel();
    ///    actix::spawn(listen_blocks(receiver));
    ///    match indexer.start_streamer(sender).await {
    ///        Ok(_) => tracing::info!(target: "indexer_example", "Streamer finished successfully"),
    ///        Err(e) => {
    ///            tracing::error!(target: "indexer_example", "Streamer finished with error: {}", e)
    ///        }
    ///    };
    ///    Ok(())
    /// }
    pub async fn start_streamer(
        &self,
        sender: mpsc::Sender<StreamerMessage>,
    ) -> Result<(), IndexerError> {
        streamer::start(
            self.view_client.clone(),
            self.client.clone(),
            self.indexer_config.clone(),
            self.near_config.config.store.clone(),
            self.near_config.config.archive,
            sender,
        )
        .await
    }
}

/// Function that initializes configs for the node which
/// accepts `InitConfigWrapper` and calls original `init_configs` from `neard`
pub fn indexer_init_configs(
    dir: &std::path::PathBuf,
    params: InitConfigArgs,
) -> Result<(), anyhow::Error> {
    init_configs(
        dir,
        params.chain_id,
        params.account_id.and_then(|account_id| account_id.parse().ok()),
        params.test_seed.as_deref(),
        params.num_shards,
        params.fast,
        params.genesis.as_deref(),
        params.download_genesis,
        params.download_genesis_url.as_deref(),
        params.download_records_url.as_deref(),
        params.download_config,
        params.download_config_url.as_deref(),
        params.boot_nodes.as_deref(),
        params.max_gas_burnt_view,
    )
}
