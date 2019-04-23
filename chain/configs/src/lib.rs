//! Various configs that can be parsed from a file or/and read from a command line.
extern crate serde_derive;
extern crate serde_json;

use clap::App;

pub mod authority;
mod client;
mod devnet;
pub mod network;
mod rpc;

pub use crate::{
    authority::AuthorityConfig, client::ClientConfig, devnet::DevNetConfig, network::NetworkConfig,
    rpc::RPCConfig,
};
use node_runtime::chain_spec::ChainSpec;

pub fn get_alphanet_configs() -> (ClientConfig, NetworkConfig, RPCConfig) {
    let matches = App::new("Near TestNet")
        .args(&client::get_args())
        .args(&network::get_args())
        .args(&rpc::get_args())
        .get_matches();
    (
        client::from_matches(&matches, ChainSpec::default_poa()),
        network::from_matches(&matches),
        rpc::from_matches(&matches),
    )
}

pub fn get_devnet_configs() -> (ClientConfig, DevNetConfig, RPCConfig) {
    let matches = App::new("Near DevNet")
        .args(&client::get_args())
        .args(&devnet::get_args())
        .args(&rpc::get_args())
        .get_matches();
    (
        client::from_matches(&matches, ChainSpec::default_devnet()),
        devnet::from_matches(&matches),
        rpc::from_matches(&matches),
    )
}
