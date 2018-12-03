extern crate futures;
extern crate substrate_network_libp2p;

mod protocol_config;

use substrate_network_libp2p::{start_service, RegisteredProtocol, NetworkConfiguration};
use protocol_config::ProtocolConfig;

/// Provides a task for starting the libp2p network.
/// TODO: Provide channels.
pub fn get_network_task(config: ProtocolConfig) {
    let version = [protocol_config::CURRENT_VERSION as u8];
    let registered = RegisteredProtocol::new(config.protocol_id, &version);
    let _service = start_service(NetworkConfiguration::default(), Some(registered))
        .expect("Failed to get the libp2p service.");
}
