extern crate devnet;
extern crate node_cli;

fn main () {
    let (network_cfg, client_cfg) = node_cli::get_service_configs();
    devnet::start_devnet(Some(network_cfg), Some(client_cfg));
}
