use network::test_utils::*;
use futures::{Stream, Future, stream, future::{select_all}};
use tokio::{runtime::Runtime, timer::Interval};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::str::from_utf8;

use substrate_network_libp2p::{
    start_service, NetworkConfiguration, RegisteredProtocol,
    ProtocolId, Service as NetworkService, NodeIndex, Secret, ServiceEvent
};

fn create_service(addr: &str, boot_nodes: Vec<String>) -> NetworkService {
    let config = test_config(addr, boot_nodes);
    let registered = RegisteredProtocol::default();
    start_service(config, Some(registered)).expect("start service failed")
}

fn create_service_with_secret(addr: &str, secret: Secret) -> NetworkService {
    let config = test_config_with_secret(addr, vec![], secret);
    let registered = RegisteredProtocol::default();
    start_service(config, Some(registered)).expect("start service failed")
}

/// spins up n network services and exchange some basic messages between them
/// TODO: add some sort of configuration?
pub fn example_n_nodes(num_nodes: u32) {
    let base_address = "/ip4/127.0.0.1/tcp/".to_string();
    let base_port = 30000;
    let mut addresses = Vec::new();
    for i in 0..num_nodes {
        let port = base_port + i;
        addresses.push(base_address.clone() + &port.to_string());
    }
    // spin up a root service that does not have bootnodes and 
    // have other services have this service as their boot node
    // may want to abstract this out to enable different configurations
    let secret = create_secret();
    let root_service = create_service_with_secret(&addresses[0], secret);
    let boot_node = addresses[0].clone() + "/p2p/" + &raw_key_to_peer_id_str(secret);
    let mut services = vec![root_service];
    for i in 1..num_nodes {
        let service = create_service(&addresses[i as usize], vec![boot_node.clone()]);
        services.push(service);
    }

    let mut runtime = Runtime::new().unwrap();
    for (i, service) in services.into_iter().enumerate() {
        let mut service = Arc::new(Mutex::new(service));
        let service1 = service.clone();
        let message_stream = Interval::new_interval(Duration::from_millis(1000)).for_each(move |_| {
            let mut guard = service1.lock().unwrap();
            let peers: Vec<NodeIndex> = guard.connected_peers().collect();
            info!("peers of service{}: {:?}", i, peers);
            for node_id in peers {
                let message = format!("{}{}", "hello from service", i).as_bytes().to_vec();
                guard.send_custom_message(node_id, ProtocolId::default(), message);
            }
            Ok(())
        })
        .then(|res| {
            match res {
                Ok(()) => (),
                Err(e) => error!("Error in message stream: {:?}", e)
            };
            Ok(())
        });
        let service2 = service.clone();
        let event_stream = stream::poll_fn(move || service.lock().unwrap().poll()).for_each(move |event| {
            info!("Event in service1: {:?}", event);
            match event {
                ServiceEvent::CustomMessage { node_index, protocol_id, data } => {
                    let received = match from_utf8(&data) {
                        Ok(msg) => msg,
                        Err(e) => {
                            debug!("Cannot decode received message: {:?}", e);
                            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e)));
                        }
                    };
                    if received.starts_with("hello") {
                        let message = format!("service{} received message", i).as_bytes().to_vec();
                        service2.lock().unwrap().send_custom_message(node_index, protocol_id, message);
                    }
                },
                _ => ()
            }
            Ok(())
        });
        let futures: Vec<Box<Future<Item = (), Error = io::Error> + Send>> = vec![
		    Box::new(message_stream) as Box<_>,
		    Box::new(event_stream) as Box<_>,
	    ];
        let future = select_all(futures)
            .and_then(move |_| {
                Ok(())
            })
            .map_err(|_| ());
        runtime.spawn(future);
    }
    runtime.shutdown_on_idle().wait().unwrap();
}