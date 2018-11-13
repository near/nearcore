use futures::{self, stream, Future, Stream};
use tokio::{runtime::Runtime, timer::Interval};
use std::thread;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::Mutex;
use substrate_network_libp2p::{
    start_service, Service as NetworkService, ServiceEvent,
    NetworkConfiguration, ProtocolId, RegisteredProtocol,
};
use protocol::{self, Protocol, ProtocolConfig, TransactionPool, Transaction};
use error::Error; 
use primitives::types;

const TICK_TIMEOUT: Duration = Duration::from_millis(1000);

impl Transaction for types::SignedTransaction {}

/// A service that wraps network service (which runs libp2p) and
/// protocol. It is thus responsible for hiding network details and
/// processing messages that nodes send to each other
pub struct Service<T: Transaction> {
    network: Arc<Mutex<NetworkService>>,
    protocol: Arc<Protocol<T>>,
    bg_thread: Option<thread::JoinHandle<()>>,
}

impl<T: Transaction> Service<T> {
    pub fn new(
        config: ProtocolConfig, 
        net_config: NetworkConfiguration, 
        protocol_id: ProtocolId,
        tx_pool: Arc<Mutex<TransactionPool<T>>>
    ) -> Result<Arc<Service<T>>, Error> {
        let version = [(protocol::CURRENT_VERSION) as u8];
        let registered = RegisteredProtocol::new(protocol_id, &version);
        let protocol = Arc::new(Protocol::new(config, tx_pool));
        let (thread, network) = start_thread(net_config, protocol.clone(), registered)?;
        Ok(Arc::new(Service {
            network: network,
            protocol: protocol,
            bg_thread: Some(thread)
        }))
    }
}

impl<T: Transaction> Drop for Service<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.bg_thread.take() {
            if let Err(e) = handle.join() {
                error!("Error while waiting on background thread: {:?}", e);
            }
        }
    }
}


pub fn start_thread<T: Transaction>(
    config: NetworkConfiguration, 
    protocol: Arc<Protocol<T>>, 
    registered: RegisteredProtocol
) -> Result<(thread::JoinHandle<()>, Arc<Mutex<NetworkService>>), Error> {

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

fn run_thread<T: Transaction>(
    network_service: Arc<Mutex<NetworkService>>, 
    protocol: Arc<Protocol<T>>
) -> impl Future<Item = (), Error = io::Error> {

    let network_service1 = network_service.clone();
    let network = stream::poll_fn(move || network_service1.lock().poll()).for_each({
        let protocol = protocol.clone();
        let network_service = network_service.clone();
        move |event| {
        debug!(target: "sub-libp2p", "event: {:?}", event);
        match event {
            ServiceEvent::CustomMessage { node_index, data, .. } => {
                protocol.on_message(node_index, &data);
            },
            ServiceEvent::OpenedCustomProtocol { node_index, .. } => {
                protocol.on_peer_connected(&network_service, node_index);
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
    }});

    // Interval for performing maintenance on the protocol handler.
	let timer = Interval::new_interval(TICK_TIMEOUT)
		.for_each({
			let protocol = protocol.clone();
			let network_service = network_service.clone();
			move |_| {
				protocol.maintain_peers(&network_service);
				Ok(())
			}
		})
		.then(|res| {
			match res {
				Ok(()) => (),
				Err(err) => error!("Error in the propagation timer: {:?}", err),
			};
			Ok(())
		});


    let futures: Vec<Box<Future<Item = (), Error = io::Error> + Send>> = vec![
        Box::new(network),
        Box::new(timer),
    ];

    futures::select_all(futures)
		.and_then(move |_| {
			info!("Networking ended");
			Ok(())
		})
		.map_err(|(r, _, _)| r)
}