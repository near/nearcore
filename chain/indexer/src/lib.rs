//! Indexer creates a queue, starts the near-streamer, passing this queue in there.
//! Listens to that queue and returns near_streamer::BlockResponse for further handling
use std::path::PathBuf;

use actix::System;
use tokio::sync::mpsc;

use neard;
mod streamer;

pub use self::streamer::{BlockResponse, Outcome};
pub use near_primitives;

/// Creates runtime and runs `neard` and `streamer`.
pub struct Indexer {
    near_config: neard::config::NearConfig,
    system_runner: actix::SystemRunner,
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
}

impl Indexer {
    /// Build the Indexer struct
    pub fn new(custom_home_dir: Option<&str>) -> Self {
        let home_dir = if !custom_home_dir.is_some() {
            PathBuf::from(neard::get_default_home())
        } else {
            PathBuf::from(custom_home_dir.unwrap())
        };

        let near_config = neard::load_config(&home_dir);
        let system = System::new("NEAR Indexer");
        let (client, view_client) = neard::start_with_config(&home_dir, near_config.clone());
        Self { near_config, system_runner: system, view_client, client }
    }

    /// Setups `near_indexer::streamer` and returns Receiver
    pub fn receiver(&self) -> mpsc::Receiver<streamer::BlockResponse> {
        let (sender, receiver) = mpsc::channel(16);
        actix::spawn(streamer::start(self.view_client.clone(), self.client.clone(), sender));
        receiver
    }

    /// Starts runtime after validating genesis.
    pub fn start(self) {
        neard::genesis_validate::validate_genesis(&self.near_config.genesis);
        self.system_runner.run().unwrap();
    }
}
