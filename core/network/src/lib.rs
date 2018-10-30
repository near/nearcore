extern crate substrate_network_libp2p;
extern crate primitives;

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate futures;
extern crate tokio;
extern crate bytes;
extern crate parking_lot;
#[macro_use]
extern crate log;

use substrate_network_libp2p::{start_service, NetworkConfiguration, ProtocolId, RegisteredProtocol};

pub mod message;
pub mod protocol;
pub mod service;
pub mod error;

#[cfg(test)]
mod tests {

    use super::*;
    
    #[test]
    fn test_service() {
        let protocol_id = ProtocolId::default();
        let versions = "1.0".as_bytes();
        let config = NetworkConfiguration::default();
        let registered = RegisteredProtocol::new(protocol_id, versions);
        let service = start_service(config, Some(registered));
        assert!(service.is_ok());

    }
}