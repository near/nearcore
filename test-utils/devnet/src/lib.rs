extern crate node_cli;
extern crate consensus;
extern crate client;

pub use node_cli::service::{NetworkConfig, DevNetConfig};
pub use client::ClientConfig;

pub fn start_devnet(
    network_cfg: Option<NetworkConfig>,
    client_cfg: Option<ClientConfig>,
    devnet_cfg: Option<DevNetConfig>,
) {
    let network_cfg = network_cfg.unwrap_or_else(NetworkConfig::default);
    let client_cfg = client_cfg.unwrap_or_else(ClientConfig::default);
    let devnet_cfg = Some(devnet_cfg.unwrap_or_else(DevNetConfig::default));
    node_cli::service::start_service(
        network_cfg,
        client_cfg,
        devnet_cfg,
    );
}
