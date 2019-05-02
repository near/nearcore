use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{thread, time};

use actix::actors::mocker::Mocker;
use actix::Actor;
use actix::{ActorStream, System};
use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use log::LevelFilter;
use tokio::timer::Delay;

use near_client::ClientActor;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkConfig, NetworkRequests,
    NetworkResponses, PeerInfo, PeerManagerActor,
};
use near_store::test_utils::create_test_store;
use primitives::test_utils::{get_key_pair_from_seed, init_test_logger};

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
    PeerManagerActor::new(store, config, client_addr.recipient())
}

#[test]
fn peer_handshake() {
    init_test_logger();

    System::run(|| {
        let count1 = Arc::new(AtomicUsize::new(0));
        let (port1, port2) = (open_port(), open_port());
        let pm1 = make_peer_manager("test1", port1, vec![("test2", port2)]).start();
        let pm2 = make_peer_manager("test2", port2, vec![("test1", port1)]).start();
        let act_count1 = count1.clone();
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
        let pm = make_peer_manager("test", port, vec![]).start();
        let mut peers = vec![];
        for i in 0..5 {
            peers.push(
                make_peer_manager(&format!("test{}", i), open_port(), vec![("test", port)]).start(),
            );
        }
        WaitOrTimeout::new(
            Box::new(move |_| {
                // TODO: stop if all connected to all after exchanging peers.
                System::current().stop();
            }),
            100,
            100,
        )
        .start();
    })
    .unwrap()
}
