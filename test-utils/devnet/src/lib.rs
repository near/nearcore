extern crate node_cli;
extern crate consensus;
extern crate client;

pub use node_cli::service::NetworkConfig;
pub use client::ClientConfig;
use consensus::passthrough;

pub fn start_devnet(network_cfg: Option<NetworkConfig>,
client_cfg: Option<ClientConfig>, ) {
    let network_cfg = network_cfg.unwrap_or_else(NetworkConfig::default);
    let client_cfg = client_cfg.unwrap_or_else(ClientConfig::default);
    node_cli::service::start_service(
        network_cfg,
        client_cfg,
        passthrough::spawn_consensus,
    );
}
