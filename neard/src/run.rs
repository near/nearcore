use clap::Clap;
use std::{net::SocketAddr, path::Path};

#[derive(Clap)]
pub(super) struct RunCmd {
    /// Keep old blocks in the storage (default false).
    #[clap(long)]
    archive: bool,
    /// Set the boot nodes to bootstrap network from.
    #[clap(long)]
    boot_nodes: Option<String>,
    /// Minimum number of peers to start syncing/producing blocks
    #[clap(long)]
    min_peers: Option<usize>,
    /// Customize network listening address (useful for running multiple nodes on the same machine).
    #[clap(long)]
    network_addr: Option<SocketAddr>,
    /// Set this to false to only produce blocks when there are txs or receipts (default true).
    #[clap(long)]
    produce_empty_blocks: Option<bool>,
    /// Customize RPC listening address (useful for running multiple nodes on the same machine).
    #[clap(long)]
    rpc_addr: Option<String>,
    /// Customize telemetry url.
    #[clap(long)]
    telemetry_url: Option<String>,
}

impl RunCmd {
    pub(super) fn run(self, home_dir: &Path) {
        // Load configs from home.
        let mut near_config = nearcore::config::load_config_without_genesis_records(home_dir);
        nearcore::genesis_validate::validate_genesis(&near_config.genesis);
        // Set current version in client config.
        near_config.client_config.version = super::NEARD_VERSION.clone();
        // Override some parameters from command line.
        if let Some(produce_empty_blocks) = self.produce_empty_blocks {
            near_config.client_config.produce_empty_blocks = produce_empty_blocks;
        }
        if let Some(boot_nodes) = self.boot_nodes {
            if !boot_nodes.is_empty() {
                near_config.network_config.boot_nodes = boot_nodes
                    .split(',')
                    .map(|chunk| chunk.parse().expect("Failed to parse PeerInfo"))
                    .collect();
            }
        }
        if let Some(min_peers) = self.min_peers {
            near_config.client_config.min_num_peers = min_peers;
        }
        if let Some(network_addr) = self.network_addr {
            near_config.network_config.addr = Some(network_addr);
        }
        if let Some(rpc_addr) = self.rpc_addr {
            near_config.rpc_config.addr = rpc_addr;
        }
        if let Some(telemetry_url) = self.telemetry_url {
            if !telemetry_url.is_empty() {
                near_config.telemetry_config.endpoints.push(telemetry_url);
            }
        }
        if self.archive {
            near_config.client_config.archive = true;
        }

        let sys = actix::System::new();
        sys.block_on(async move {
            nearcore::start_with_config(home_dir, near_config);
        });
        sys.run().unwrap();
    }
}
