extern crate network;
extern crate substrate_network_libp2p;
extern crate libp2p;
extern crate futures;
extern crate tokio;
#[macro_use]
extern crate log;
extern crate env_logger;

use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use futures::{Stream, Future, stream};
use tokio::{runtime::Runtime, timer::Interval};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use env_logger::Builder;

use substrate_network_libp2p::{
    start_service, NetworkConfiguration, RegisteredProtocol,
    ProtocolId, Service, NodeIndex
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

pub fn example() {
    let addr1 = "/ip4/127.0.0.1/tcp/30000";
    let addr2 = "/ip4/127.0.0.1/tcp/30001";
    //let boot_nodes1 = vec![addr2.to_string()];
    let mut service1 = create_service_from_addr(addr1);
    let mut service2 = create_service_from_addr(addr2);
    let peer_id1 = (*service1.peer_id()).clone();
    let peer_id2 = (*service2.peer_id()).clone();
    println!("peer_id1: {:?} peer_id2: {:?}", peer_id1, peer_id2);
    service1.add_reserved_peer(peer_id2, parse_addr(addr2));
    service2.add_reserved_peer(peer_id1, parse_addr(addr1));
    println!("peer added");
    let mut service1 = Arc::new(Mutex::new(service1));
    let mut service2 = Arc::new(Mutex::new(service2));
    let message_stream = Interval::new_interval(Duration::from_millis(1000)).for_each({
        let service = service1.clone();
        move |_| {
            let mut guard = service.lock().unwrap();
            let peers: Vec<NodeIndex> = guard.connected_peers().collect();
            println!("peers of service1: {:?}", peers);
            for node_id in peers {
                guard.send_custom_message(node_id, ProtocolId::default(), "hello".as_bytes().to_vec());
            }
            Ok(())
        }
    })
    .then(|res| {
        match res {
            Ok(()) => (),
            Err(e) => println!("Error in message stream: {:?}", e)
        };
        Ok(())
    });
    let event_stream1 = stream::poll_fn(move || service1.lock().unwrap().poll()).for_each(move |x| {
        println!("Event in service1: {:?}", x);
        Ok(())
    });
    let event_stream2 = stream::poll_fn(move || service2.lock().unwrap().poll()).for_each(move |x| {
        println!("Event in service2: {:?}", x);
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
    builder.filter(Some("sub-libp2p"), log::LevelFilter::Trace);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();
    example();
}