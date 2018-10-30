use futures::{self, stream, Future, Stream};
use tokio::{runtime::Runtime, timer::Interval};
use bytes::Bytes;
use std::thread;
use std::io;
use std::sync::Arc;
use parking_lot::Mutex;

use substrate_network_libp2p::{
    start_service, Service, ServiceEvent,
    NetworkConfiguration, ProtocolId, RegisteredProtocol,
    NodeIndex
};
use protocol::Protocol;
use error::Error;

//pub enum NetworkEvent {
//    Message {
//        node_index: NodeIndex,
//        data: Bytes, 
//    },
//}

fn start_thread(config: NetworkConfiguration, protocol: Protocol, registered: RegisteredProtocol)
   -> Result<(thread::JoinHandle<()>, Arc<Mutex<Service>>), Error> {
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

fn run_thread(network_service: Arc<Mutex<Service>>, protocol: Protocol)
    -> impl Future<Item = (), Error = io::Error> {

    let network_service1 = network_service.clone();
    let network = stream::poll_fn(move || network_service1.lock().poll()).for_each(move |event| {
        match event {
            ServiceEvent::CustomMessage { node_index, data, ..} => 
                protocol.on_message(&network_service, &data),
            _ => panic!("TODO")
        };
        Ok(())
    });

    let futures = vec![
        Box::new(network)
    ];

    futures::select_all(futures)
		.and_then(move |_| {
			println!("Networking ended");
			Ok(())
		})
		.map_err(|(r, _, _)| r)
}