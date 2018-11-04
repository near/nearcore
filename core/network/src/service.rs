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
    NodeIndex
};
use protocol::{Protocol, ProtocolConfig};
use error::Error;

/// The service that wraps network service (which runs libp2p) and
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
            Ok(()) => debug!("Network thread finished"),
            Err(e) => error!("Error occurred in network thread: {:?}", e)
        }
    })?;

    Ok((thread, service))
}

fn run_thread(network_service: Arc<Mutex<NetworkService>>, protocol: Arc<Protocol>)
    -> impl Future<Item = (), Error = io::Error> {

    let network_service1 = network_service.clone();
    let network = stream::poll_fn(move || network_service1.lock().poll()).for_each(move |event| {
        match event {
            ServiceEvent::CustomMessage { node_index, data, ..} => 
                protocol.on_message(&network_service, &data),
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