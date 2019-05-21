use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use actix::actors::mocker::Mocker;
use actix::Actor;
use actix::System;
use futures::future;
use futures::future::Future;

use near_client::ClientActor;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkConfig, NetworkRequests,
    NetworkResponses, PeerManagerActor,
};
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;

type ClientMock = Mocker<ClientActor>;

fn make_peer_manager(seed: &str, port: u16, boot_nodes: Vec<(&str, u16)>) -> PeerManagerActor {
    let store = create_test_store();
    let mut config = NetworkConfig::from_seed(seed, port);
    config.boot_nodes = convert_boot_nodes(boot_nodes);
    let client_addr = ClientMock::mock(Box::new(move |msg, _ctx| {
        let msg = msg.downcast_ref::<NetworkClientMessages>().unwrap();
        match msg {
            NetworkClientMessages::GetChainInfo => {
                Box::new(Some(NetworkClientResponses::ChainInfo {
                    height: 1,
                    total_weight: 1.into(),
                }))
            }
            _ => Box::new(Some(NetworkClientResponses::NoResponse)),
        }
    }))
    .start();
    PeerManagerActor::new(store, config, client_addr.recipient()).unwrap()
}

#[test]
fn peer_handshake() {
    init_test_logger();

    System::run(|| {
        let (port1, port2) = (open_port(), open_port());
        let pm1 = make_peer_manager("test1", port1, vec![("test2", port2)]).start();
        let _pm2 = make_peer_manager("test2", port2, vec![("test1", port1)]).start();
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(pm1.send(NetworkRequests::FetchInfo).then(move |res| {
                    if let NetworkResponses::Info { num_active_peers, .. } = res.unwrap() {
                        if num_active_peers == 1 {
                            System::current().stop();
                        }
                    }
                    future::result(Ok(()))
                }));
            }),
            100,
            2000,
        )
        .start();
    })
    .unwrap();
}

#[test]
fn peers_connect_all() {
    init_test_logger();

    System::run(|| {
        let port = open_port();
        let _pm = make_peer_manager("test", port, vec![]).start();
        let mut peers = vec![];
        for i in 0..5 {
            peers.push(
                make_peer_manager(&format!("test{}", i), open_port(), vec![("test", port)]).start(),
            );
        }
        let flags = Arc::new(AtomicUsize::new(0));
        WaitOrTimeout::new(
            Box::new(move |_| {
                for i in 0..5 {
                    let flags1 = flags.clone();
                    actix::spawn(peers[i].send(NetworkRequests::FetchInfo).then(move |res| {
                        if let NetworkResponses::Info { num_active_peers, .. } = res.unwrap() {
                            if num_active_peers > 4 && (flags1.load(Ordering::Relaxed) >> i) % 2 == 0 {
                                println!("Peer {}: {}", i, num_active_peers);
                                flags1.fetch_add(1 << i, Ordering::Relaxed);
                            }
                        }
                        future::result(Ok(()))
                    }));
                }
                // Stop if all connected to all after exchanging peers.
                println!("Flags: {}", flags.load(Ordering::Relaxed));
                if flags.load(Ordering::Relaxed) == 0b11111 {
                    System::current().stop();
                }
            }),
            100,
            1000,
        )
        .start();
    })
    .unwrap()
}
