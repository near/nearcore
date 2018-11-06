use futures::{self, stream, Future, Stream};
use tokio::{runtime::Runtime, timer::Interval};
use bytes::Bytes;
use std::thread;
use std::io;
use std::sync::Arc;
use parking_lot::Mutex;
use substrate_network_libp2p::{
    start_service, Service as NetworkService, ServiceEvent,
    NetworkConfiguration, ProtocolId, RegisteredProtocol,
    NodeIndex,
};
use protocol::{Protocol, ProtocolConfig};
use error::Error;

/// A service that wraps network service (which runs libp2p) and
/// protocol. It is thus responsible for hiding network details and
/// processing messages that nodes send to each other
pub struct Service {
    network: Arc<Mutex<NetworkService>>,
    protocol: Arc<Protocol>,
    bg_thread: Option<thread::JoinHandle<()>>,
}

impl Service {
    pub fn new(config: ProtocolConfig, net_config: NetworkConfiguration, protocol_id: ProtocolId) -> Result<Arc<Service>, Error> {
        let registered = RegisteredProtocol::new(protocol_id, config.version.as_bytes());
        let protocol = Arc::new(Protocol::new(config));
        let (thread, network) = start_thread(net_config, protocol.clone(), registered)?;
        Ok(Arc::new(Service {
            network: network,
            protocol: protocol,
            bg_thread: Some(thread)
        }))
    }
}


fn start_thread(config: NetworkConfiguration, protocol: Arc<Protocol>, registered: RegisteredProtocol)
   -> Result<(thread::JoinHandle<()>, Arc<Mutex<NetworkService>>), Error> {
    let protocol_id = registered.id();

    let service = match start_service(config, Some(registered)) {
        Ok(service) => Arc::new(Mutex::new(service)),
        Err(e) => return Err(e.into())
    };
    let service_clone = service.clone();
    let mut runtime = Runtime::new()?;
    let thread = thread::Builder::new().name("network".to_string()).spawn(move || {
        let future = run_thread(service_clone, protocol);

        match runtime.block_on(future) {
            Ok(()) => {
                debug!("Network thread finished");
            }
            Err(e) => {
                error!("Error occurred in network thread: {:?}", e);
            }
        }
    })?;

    Ok((thread, service))
}

fn run_thread(network_service: Arc<Mutex<NetworkService>>, protocol: Arc<Protocol>)
    -> impl Future<Item = (), Error = io::Error> {

    let network_service1 = network_service.clone();
    let network = stream::poll_fn(move || network_service1.lock().poll()).for_each(move |event| {
        debug!(target: "sub-libp2p", "event: {:?}", event);
        match event {
            ServiceEvent::CustomMessage { data, .. } => {
                protocol.on_message(&network_service, &data);
            },
            ServiceEvent::OpenedCustomProtocol { node_index, .. } => {
                protocol.on_peer_connected(node_index);
            },
            ServiceEvent::ClosedCustomProtocol { node_index, .. } => {
                protocol.on_peer_disconnected(node_index);
            },
            _ => {
                debug!("TODO");
                ()
            }
        };
        Ok(())
    });

    let futures: Vec<Box<Future<Item = (), Error = io::Error> + Send>> = vec![
        Box::new(network)
    ];

    futures::select_all(futures)
		.and_then(move |_| {
			info!("Networking ended");
			Ok(())
		})
		.map_err(|(r, _, _)| r)
}


#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::*;
    use libp2p::{Multiaddr, secio};
    use substrate_network_libp2p::{Secret, PeerId};
    use message::{Message, MessageBody};
    use primitives::{types, traits::{Encode, Decode}};
    use std::time;
    use log;

    impl Service {
        pub fn _new(config: ProtocolConfig, net_config: NetworkConfiguration, protocol_id: ProtocolId) -> Service {
            let registered = RegisteredProtocol::new(protocol_id, config.version.as_bytes());
            let protocol = Arc::new(Protocol::new(config));
            let (thread, network) = start_thread(net_config, protocol.clone(), registered).expect("start_thread shouldn't fail");
            Service {
                network: network,
                protocol: protocol,
                bg_thread: Some(thread)
            }
        }
    }

    fn parse_addr(addr: &str) -> Multiaddr {
        addr.parse().expect("cannot parse address")
    }

    fn test_config(addr: &str, boot_nodes: Vec<String>) -> NetworkConfiguration {
        let mut config = NetworkConfiguration::new();
        config.listen_addresses = vec![parse_addr(addr)];
        config.boot_nodes = boot_nodes;
        config
    }

    fn test_config_with_secret(addr: &str, boot_nodes: Vec<String>, secret: Secret)
        -> NetworkConfiguration {
            let mut config = test_config(addr, boot_nodes);
            config.use_secret = Some(secret);
            config
        }

    fn create_secret() -> Secret {
        let mut secret: Secret = [0; 32];
        secret[31] = 1;
        secret
    }

    fn raw_key_to_peer_id(raw_key: Secret) -> PeerId {
        let secret_key = secio::SecioKeyPair::secp256k1_raw_key(&raw_key)
            .expect("key with correct len should always be valid");
        secret_key.to_peer_id()
    }

    fn raw_key_to_peer_id_str(raw_key: Secret) -> String {
        let peer_id = raw_key_to_peer_id(raw_key);
        peer_id.to_base58()
    }

    fn fake_message() -> Message {
        let tx = types::SignedTransaction::new(0, 0, types::TransactionBody::new(0, 0, 0, 0));
        Message::new(0, 0, "shard0", MessageBody::TransactionMessage(tx))
    }

    fn create_services(num_services: u32) -> Vec<Service> {
        let base_address = "/ip4/127.0.0.1/tcp/".to_string();
        let base_port = 30000;
        let mut addresses = Vec::new();
        for i in 0..num_services {
            let port = base_port + i;
            addresses.push(base_address.clone() + &port.to_string());
        }
        // spin up a root service that does not have bootnodes and 
        // have other services have this service as their boot node
        // may want to abstract this out to enable different configurations
        let secret = create_secret();
        let root_config = test_config_with_secret(&addresses[0], vec![], secret);
        let root_service = Service::_new(ProtocolConfig::default(), root_config, ProtocolId::default());
        let boot_node = addresses[0].clone() + "/p2p/" + &raw_key_to_peer_id_str(secret);
        let mut services = vec![root_service];
        for i in 1..num_services {
            let config = test_config(&addresses[i as usize], vec![boot_node.clone()]);
            let service = Service::_new(ProtocolConfig::default(), config, ProtocolId::default());
            services.push(service);
        }
        services
    }

    #[test]
    fn test_send_message() {
        let mut builder = env_logger::Builder::new();
        builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
        builder.filter(None, log::LevelFilter::Info);
        builder.init();
        let services = create_services(2);
        thread::sleep(time::Duration::from_secs(2));
        for service in services {
            for peer in service.protocol.sample_peers(1) {
                let message = fake_message();
                service.protocol.send_message(&service.network, peer, message);
            }
        }
        thread::sleep(time::Duration::from_secs(2));
    }
}