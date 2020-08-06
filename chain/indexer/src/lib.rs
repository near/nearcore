//! NEAR Indexer is a micro-framework, which provides you with a stream of blocks that are recorded on NEAR network.
//!
//! NEAR Indexer is useful to handle real-time "events" on the chain.
//!
//! NEAR Indexer is going to be used to build NEAR Explorer, augment NEAR Wallet, and provide overview of events in Rainbow Bridge.
//!
//! See the [example] for further details.
//!
//! [example]: https://github.com/nearprotocol/nearcore/tree/master/tools/indexer/example
use std::path::PathBuf;

use actix::System;
use tokio::sync::mpsc;

pub use neard::{get_default_home, init_configs, NearConfig};
mod streamer;

pub use self::streamer::{BlockResponse, Outcome};
pub use near_primitives;

/// This is the core component, which handles `nearcore` and internal `streamer`.
pub struct Indexer {
    near_config: neard::NearConfig,
    actix_runtime: actix::SystemRunner,
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
}

impl Indexer {
    /// Initialize Indexer by configuring `nearcore`
    pub fn new(custom_home_dir: Option<&std::path::Path>) -> Self {
        let home_dir = if !custom_home_dir.is_some() {
            PathBuf::from(get_default_home())
        } else {
            PathBuf::from(custom_home_dir.unwrap())
        };

        let near_config = neard::load_config(&home_dir);
        let system = System::new("NEAR Indexer");
        neard::genesis_validate::validate_genesis(&near_config.genesis);
        assert!(
            !&near_config.client_config.tracked_shards.is_empty(),
            "Indexer should track at least one shard. \n\
            Tip: You may want to update {} with `\"tracked_shards\": [0]`
            ",
            home_dir.join("config.json").display()
        );
        let (client, view_client, _) = neard::start_with_config(&home_dir, near_config.clone());
        Self { actix_runtime: system, view_client, client, near_config }
    }

    /// Boots up `near_indexer::streamer`, so it monitors the new blocks with chunks, transactions, receipts, and execution outcomes inside. The returned stream handler should be drained and handled on the user side.
    pub fn streamer(&self) -> mpsc::Receiver<streamer::BlockResponse> {
        let (sender, receiver) = mpsc::channel(16);
        actix::spawn(streamer::start(self.view_client.clone(), self.client.clone(), sender));
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
