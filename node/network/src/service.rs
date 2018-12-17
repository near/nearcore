use std::iter;
use std::mem;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use futures::{Future, stream, Stream};
use futures::sync::mpsc::Receiver;
use parking_lot::Mutex;
use substrate_network_libp2p::{
    Multiaddr, NodeIndex, Protocol as NetworkProtocol, RegisteredProtocol,
    Service as NetworkService, ServiceEvent, Severity, start_service
};
pub use substrate_network_libp2p::NetworkConfiguration;
use tokio::timer::Interval;
use substrate_network_libp2p::Secret;

use chain::{SignedBlock, SignedHeader as BlockHeader};
use message::Message;
use primitives::traits::{Encode, Payload};
use protocol::{self, Protocol, ProtocolConfig};

const TICK_TIMEOUT: Duration = Duration::from_millis(1000);

pub fn new_network_service(protocol_config: &ProtocolConfig, net_config: NetworkConfiguration) -> NetworkService {
    let version = [protocol::CURRENT_VERSION as u8];
    let registered = RegisteredProtocol::new(protocol_config.protocol_id, &version);
    start_service(net_config, Some(registered))
        .expect("Error starting network service")
}

pub fn spawn_network_tasks<B, Header, P>(
    network_service: Arc<Mutex<NetworkService>>,
    protocol_: Protocol<B, Header, P>,
    message_receiver: Receiver<(NodeIndex, Message<B, Header, P>)>
) where
    B: SignedBlock,
    Header: BlockHeader,
    P: Payload,
{
    let protocol = Arc::new(protocol_);
    // Interval for performing maintenance on the protocol handler.
    let timer = Interval::new_interval(TICK_TIMEOUT)
        .for_each({
            let network_service1 = network_service.clone();
            let protocol1 = protocol.clone();
            move |_| {
                for timed_out in protocol1.maintain_peers() {
                    error!("Dropping timeouted node {:?}.", timed_out);
                    network_service1.lock().drop_node(timed_out);
                }
                Ok(())
            }
        }).then(|res| {
            match res {
                Ok(()) => (),
                Err(err) => error!("Error in the propagation timer: {:?}", err),
            };
            Ok(())
        }).map(|_| ()).map_err(|_: ()| ());

    // Handles messages coming from the network.
    let network = stream::poll_fn({
        let network_service1 = network_service.clone();
        move || network_service1.lock().poll()
    }).for_each({
        let network_service1 = network_service.clone();
        let protocol1 = protocol.clone();
        move |event| {
            // debug!(target: "network", "event: {:?}", event);
            match event {
                ServiceEvent::CustomMessage { node_index, data, .. } => {
                    if let Err((node_index, severity))
                    = protocol1.on_message(node_index, &data) {
                        match severity {
                            Severity::Bad(err) => {
                                error!("Banning bad node {:?}. {:?}", node_index, err);
                                network_service1.lock().ban_node(node_index);
                            },
                            Severity::Useless(err) => {
                                error!("Dropping useless node {:?}. {:?}", node_index, err);
                                network_service1.lock().drop_node(node_index);
                            },
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
    }).map(|_| ()).map_err(|_|());

    // Handles messages going into the network.
    let protocol_id = protocol.config.protocol_id;
    let messages_handler = message_receiver.for_each(move |(node_index, m)| {
        let data = Encode::encode(&m).expect("Error encoding message.");
        let cloned = network_service.clone();
        cloned.lock().send_custom_message(node_index, protocol_id, data);
        Ok(())
    }).map(|_| ()).map_err(|_|());

    tokio::spawn(network.select(timer).and_then(|_| {
        info!("Networking stopped");
        Ok(())
    }).map_err(|(e, _)| debug!("Networking/Maintenance error {:?}", e)));

    tokio::spawn(messages_handler);
}

pub fn get_multiaddr(ip_addr: Ipv4Addr, port: u16) -> Multiaddr {
    iter::once(NetworkProtocol::Ip4(ip_addr))
        .chain(iter::once(NetworkProtocol::Tcp(port)))
        .collect()
}

pub fn get_test_secret_from_node_index(test_node_index: u32) -> Secret {
    // 0 is an invalid secret so we increment all values by 1
    let bytes: [u8; 4] = unsafe { mem::transmute(test_node_index + 1) };

    let mut array = [0; 32];
    for (count, b) in bytes.iter().enumerate() {
        array[array.len() - count - 1] = *b;
    }
    array
}

