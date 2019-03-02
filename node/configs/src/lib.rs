//! Various configs that can be parsed from a file or/and read from a command line.
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;

use clap::App;

pub mod authority;
pub mod chain_spec;
mod client;
mod devnet;
pub mod network;
mod rpc;

pub use crate::{
    authority::AuthorityConfig, chain_spec::ChainSpec, client::ClientConfig, devnet::DevNetConfig,
    network::NetworkConfig, rpc::RPCConfig,
};

pub fn get_alphanet_configs() -> (ClientConfig, NetworkConfig, RPCConfig) {
    let matches = App::new("Near Alphanet")
        .args(&client::get_args())
        .args(&network::get_args())
        .args(&rpc::get_args())
        .get_matches();

    let client_cfg = client::from_matches(&matches);
    let network_cfg = network::from_matches(&client_cfg, &matches);
    (client_cfg, network_cfg, rpc::from_matches(&matches))
}

pub fn get_devnet_configs() -> (ClientConfig, DevNetConfig, RPCConfig) {
    let matches = App::new("Near DevNet")
        .args(&client::get_args())
        .args(&devnet::get_args())
        .args(&rpc::get_args())
        .get_matches();
    (client::from_matches(&matches), devnet::from_matches(&matches), rpc::from_matches(&matches))
}
