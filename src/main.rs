extern crate network;
extern crate substrate_network_libp2p;
extern crate libp2p;
extern crate futures;
extern crate tokio;
#[macro_use]
extern crate log;
extern crate env_logger;

use libp2p::{multiaddr::Protocol, Multiaddr, PeerId, secio};
use futures::{Stream, Future, stream};
use tokio::{runtime::Runtime, timer::Interval};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use env_logger::Builder;

use substrate_network_libp2p::{
    start_service, NetworkConfiguration, RegisteredProtocol,
    ProtocolId, Service, NodeIndex, Secret, ServiceEvent
};

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

fn create_service(addr: &str, boot_nodes: Vec<String>) -> Service {
    let config = test_config(addr, boot_nodes);
    let registered = RegisteredProtocol::default();
    start_service(config, Some(registered)).expect("start service failed")
}

fn create_service_from_addr(addr: &str) -> Service {
    let config = test_config(addr, vec![]);
    let registered = RegisteredProtocol::default();
    start_service(config, Some(registered)).expect("start service failed")
}

fn create_service_with_secret(addr: &str, secret: Secret) -> Service {
    let config = test_config_with_secret(addr, vec![], secret);
    let registered = RegisteredProtocol::default();
    start_service(config, Some(registered)).expect("start service failed")
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

pub fn example() {
    let addr1 = "/ip4/127.0.0.1/tcp/30000";
    let secret = create_secret();
    let addr2 = "/ip4/127.0.0.1/tcp/30001";
    //let boot_nodes1 = vec![addr2.to_string()];
    let mut service1 = create_service_with_secret(addr1, secret.clone());
    let peer_id1 = (*service1.peer_id()).clone();
    let boot_node = addr1.to_string() + "/p2p/" + &raw_key_to_peer_id_str(secret);
    let mut service2 = create_service(addr2, vec![boot_node]);
    let peer_id2 = (*service2.peer_id()).clone();
    info!("peer_id1: {:?} peer_id2: {:?}", peer_id1, peer_id2);
    let mut service1 = Arc::new(Mutex::new(service1));
    let mut service2 = Arc::new(Mutex::new(service2));
    let message_stream = Interval::new_interval(Duration::from_millis(1000)).for_each({
        let service = service1.clone();
        move |_| {
            let mut guard = service.lock().unwrap();
            let peers: Vec<NodeIndex> = guard.connected_peers().collect();
            info!("peers of service1: {:?}", peers);
            for node_id in peers {
                guard.send_custom_message(
                    node_id, ProtocolId::default(), "hello from service1".as_bytes().to_vec()
                );
            }
            Ok(())
        }
    })
    .then(|res| {
        match res {
            Ok(()) => (),
            Err(e) => error!("Error in message stream: {:?}", e)
        };
        Ok(())
    });
    let event_stream1 = stream::poll_fn(move || service1.lock().unwrap().poll()).for_each(move |x| {
        info!("Event in service1: {:?}", x);
        Ok(())
    });
    let service2_clone = service2.clone();
    let event_stream2 = stream::poll_fn(move || service2.lock().unwrap().poll()).for_each(move |x| {
        info!("Event in service2: {:?}", x);
        match x {
            ServiceEvent::CustomMessage { node_index, data, protocol_id } => {
                let message = "hello from service2".as_bytes().to_vec();
                service2_clone.lock().unwrap().send_custom_message(node_index, protocol_id, message);
            },
            _ => ()
        }
        Ok(())
    });
    let futures: Vec<Box<Future<Item = (), Error = io::Error> + Send>> = vec![
		Box::new(message_stream) as Box<_>,
		Box::new(event_stream1) as Box<_>,
        Box::new(event_stream2) as Box<_>,
	];
    let future = futures::future::select_all(futures)
        .and_then(move |_| {
            Ok(())
        })
        .map_err(|_| ());
    tokio::runtime::run(future);

}

pub fn main() {
    let mut builder = Builder::new();
    builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();
    example();
}