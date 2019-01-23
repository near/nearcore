use std::sync::Arc;

use futures::sync::mpsc::Receiver;
use futures::{stream, Future, Stream};
use parking_lot::Mutex;
pub use substrate_network_libp2p::NetworkConfiguration;
use substrate_network_libp2p::{
    start_service, NodeIndex, RegisteredProtocol, Service as NetworkService, ServiceEvent, Severity,
};

use beacon::types::SignedBeaconBlock;
use primitives::serialize::Encode;
use primitives::types::Gossip;
use shard::SignedShardBlock;
use transaction::ChainPayload;

use crate::message::Message;
use crate::protocol::{self, Protocol, ProtocolConfig};

pub fn new_network_service(
    protocol_config: &ProtocolConfig,
    net_config: NetworkConfiguration,
) -> NetworkService {
    let version = [protocol::CURRENT_VERSION as u8];
    let registered = RegisteredProtocol::new(protocol_config.protocol_id, &version);
    start_service(net_config, Some(registered)).expect("Error starting network service")
}

pub fn spawn_network_tasks(
    network_service: Arc<Mutex<NetworkService>>,
    protocol_: Protocol,
    message_receiver: Receiver<(NodeIndex, Message)>,
    outgoing_block_tx: Receiver<(SignedBeaconBlock, SignedShardBlock)>,
    gossip_rx: Receiver<Gossip<ChainPayload>>,
) {
    let protocol = Arc::new(protocol_);

    // Handles messages coming from the network.
    let network = stream::poll_fn({
        let network_service1 = network_service.clone();
        move || network_service1.lock().poll()
    })
    .for_each({
        let network_service1 = network_service.clone();
        let protocol1 = protocol.clone();
        move |event| {
            // debug!(target: "network", "event: {:?}", event);
            match event {
                ServiceEvent::CustomMessage { node_index, data, .. } => {
                    if let Err((node_index, severity)) = protocol1.on_message(node_index, &data) {
                        match severity {
                            Severity::Bad(err) => {
                                error!("Banning bad node {:?}. {:?}", node_index, err);
                                network_service1.lock().ban_node(node_index);
                            }
                            Severity::Useless(err) => {
                                error!("Dropping useless node {:?}. {:?}", node_index, err);
                                network_service1.lock().drop_node(node_index);
                            }
                            Severity::Timeout => {
                                error!("Dropping timeouted node {:?}.", node_index);
                                network_service1.lock().drop_node(node_index);
                            }
                        }
                    }
                }
                ServiceEvent::OpenedCustomProtocol { node_index, .. } => {
                    protocol1.on_peer_connected(node_index);
                }
                ServiceEvent::ClosedCustomProtocol { node_index, .. } => {
                    protocol1.on_peer_disconnected(node_index);
                }
                ServiceEvent::NodeClosed { node_index, .. } => {
                    // TODO(#218): actually do something here
                    debug!(target: "network", "Node closed {}", node_index);
                }
                ServiceEvent::ClosedCustomProtocols { node_index, .. } => {
                    // TODO(#218): actually do something here
                    debug!(
                        target: "network",
                        "Protocols closed for {}",
                        node_index,
                    );
                }
            };
            Ok(())
        }
    })
    .map(|_| ())
    .map_err(|_| ());

    // Handles messages going into the network.
    let protocol_id = protocol.config.protocol_id;
    let network_service1 = network_service.clone();
    let messages_handler = message_receiver
        .for_each(move |(node_index, m)| {
            let data = Encode::encode(&m).expect("Error encoding message.");
            network_service1.lock().send_custom_message(node_index, protocol_id, data);
            Ok(())
        })
        .map(|_| ())
        .map_err(|_| ());

    let protocol1 = protocol.clone();
    let block_announce_handler = outgoing_block_tx.for_each(move |blocks| {
        protocol1.on_outgoing_blocks(blocks);
        Ok(())
    });

    tokio::spawn(network);

    tokio::spawn(messages_handler);
    tokio::spawn(block_announce_handler);

    let protocol3 = protocol.clone();
    let gossip_sender = gossip_rx.for_each(move |g| {
        //println!("About to send gossip {:?}", g);
        if let Some(node_index) = protocol3.get_node_index_by_uid(g.receiver_uid) {
            let m = Message::Gossip(Box::new(g));
            let data = Encode::encode(&m).expect("Error encoding message.");
            network_service.lock().send_custom_message(node_index, protocol_id, data);
        } else {
            error!("Node Index not found for UID: {}", g.receiver_uid);
        }
        Ok(())
    });
    tokio::spawn(gossip_sender);
}

#[cfg(test)]
mod tests {
    use substrate_network_libp2p::parse_str_addr;

    #[test]
    fn test_parse_str_addr_dns() {
        let addr = "/dns4/node-0/tcp/30333/p2p/\
                    QmQZ8TjTqeDj3ciwr93EJ95hxfDsb9pEYDizUAbWpigtQN";
        assert!(parse_str_addr(addr).is_ok());
    }
}
