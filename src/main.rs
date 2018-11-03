extern crate env_logger;
#[macro_use]
extern crate log;
extern crate network;
extern crate substrate_network_libp2p;
extern crate libp2p;
extern crate futures;
extern crate tokio;

mod example;

use env_logger::Builder;

pub fn main() {
    let mut builder = Builder::new();
    builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();
    example::example_n_nodes(2);
}