#[macro_use]
extern crate log;
extern crate futures;
extern crate substrate_network_libp2p;

pub mod protocol_config;

use substrate_network_libp2p::{start_service, RegisteredProtocol, NetworkConfiguration,
                               NodeIndex, ServiceEvent};
use protocol_config::ProtocolConfig;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use futures::{future, Future, Stream};

static POISONED_PEERS_LOCK_ERR: &'static str = "The peers lock was poisoned.";

/// Provides a task for starting the libp2p network.
/// TODO: Provide channels.
pub fn create_network_task(config: ProtocolConfig) -> impl Future<Item=(), Error=()> {
    let peers: Arc<RwLock<HashSet<NodeIndex>>> = Arc::new(RwLock::new(HashSet::new()));
    let version = [protocol_config::CURRENT_VERSION as u8];
    let registered = RegisteredProtocol::new(config.protocol_id, &version);
    let service = start_service(NetworkConfiguration::default(), Some(registered))
        .expect("Failed to get the libp2p service.");
    service.map_err(|e| error!("libp2p service error {:?}", e))
        .fold(peers.clone(), |peers, event| {
            match event {
                ServiceEvent::CustomMessage { node_index, data, .. } => {
                    // TODO: Copy network handler and handle the message.
                    let _ = node_index;
                    let _ = data;
                }
                ServiceEvent::OpenedCustomProtocol { node_index, .. } => {
                    peers.write().expect(POISONED_PEERS_LOCK_ERR).insert(node_index);
                }
                ServiceEvent::ClosedCustomProtocol { node_index, .. } => {
                    peers.write().expect(POISONED_PEERS_LOCK_ERR).remove(&node_index);
                }
                _ => {
                    debug!("TODO");
                    ()
                }
            };
            future::ok(peers.clone())
        })
        .map(|_| ())
        .map_err(|e| error!("Error processing network event {:?}", e))
}
