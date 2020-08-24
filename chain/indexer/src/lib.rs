//! NEAR Indexer is a micro-framework, which provides you with a stream of blocks that are recorded on NEAR network.
//!
//! NEAR Indexer is useful to handle real-time "events" on the chain.
//!
//! NEAR Indexer is going to be used to build NEAR Explorer, augment NEAR Wallet, and provide overview of events in Rainbow Bridge.
//!
//! See the [example] for further details.
//!
//! [example]: https://github.com/nearprotocol/nearcore/tree/master/tools/indexer/example
use actix::System;
use tokio::sync::mpsc;

pub use neard::{get_default_home, init_configs, NearConfig};
mod streamer;

pub use self::streamer::{Outcome, StreamerMessage};
pub use near_primitives;

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

/// NEAR Indexer configuration to be provided to `Indexer::new(IndexerConfig)`
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Path to `home_dir` where configs and keys can be found
    pub home_dir: std::path::PathBuf,
    /// Mode of syncing for NEAR Indexer instance
    pub sync_mode: SyncModeEnum,
}

/// This is the core component, which handles `nearcore` and internal `streamer`.
pub struct Indexer {
    indexer_config: IndexerConfig,
    near_config: neard::NearConfig,
    actix_runtime: actix::SystemRunner,
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
}

impl Indexer {
    /// Initialize Indexer by configuring `nearcore`
    pub fn new(indexer_config: IndexerConfig) -> Self {
        let near_config = neard::load_config(&indexer_config.home_dir);
        let system = System::new("NEAR Indexer");
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
        Self { actix_runtime: system, view_client, client, near_config, indexer_config }
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

    /// Start Indexer.
    pub fn start(self) {
        self.actix_runtime.run().unwrap();
    }
}
