extern crate substrate_network_libp2p;
extern crate primitives;

extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use substrate_network_libp2p::{start_service, NetworkConfiguration};

pub mod message;
pub mod protocol;

#[cfg(test)]
mod tests {

}