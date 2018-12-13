extern crate devnet;
extern crate node_cli;

fn main () {
    let service_config = node_cli::get_service_config();
    devnet::start_devnet(Some(service_config));
}
