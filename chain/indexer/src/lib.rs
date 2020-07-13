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

use neard;
mod streamer;

pub use self::streamer::{BlockResponse, Outcome};
pub use near_primitives;

/// This is the core component, which handles `nearcore` and internal `streamer`.
pub struct Indexer {
    actix_runtime: actix::SystemRunner,
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
}

impl Indexer {
    /// Initialize Indexer by configuring `nearcore`
    pub fn new(custom_home_dir: Option<&str>) -> Self {
        let home_dir = if !custom_home_dir.is_some() {
            PathBuf::from(neard::get_default_home())
        } else {
            PathBuf::from(custom_home_dir.unwrap())
        };

        let near_config = neard::load_config(&home_dir);
        let system = System::new("NEAR Indexer");
        let (client, view_client) = neard::start_with_config(&home_dir, near_config.clone());
        neard::genesis_validate::validate_genesis(&near_config.genesis);
        Self { actix_runtime: system, view_client, client }
    }

    /// Boots up `near_indexer::streamer`, so it monitors the new blocks with chunks, transactions, receipts, and execution outcomes inside. The returned stream handler should be drained and handled on the user side.
    pub fn streamer(&self) -> mpsc::Receiver<streamer::BlockResponse> {
        let (sender, receiver) = mpsc::channel(16);
        actix::spawn(streamer::start(self.view_client.clone(), self.client.clone(), sender));
        receiver
    }

    /// Start Indexer.
    pub fn start(self) {
        self.actix_runtime.run().unwrap();
    }
}
