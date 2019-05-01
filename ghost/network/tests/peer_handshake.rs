use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{thread, time};

use actix::actors::mocker::Mocker;
use actix::utils::IntervalFunc;
use actix::Actor;
use actix::{ActorStream, Arbiter, System};
use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use log::LevelFilter;
use tokio::timer::{Delay, Interval};

use near_client::ClientActor;
use near_network::test_utils::{convert_boot_nodes, open_port};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkConfig, PeerInfo, PeerManagerActor,
};
use near_store::test_utils::create_test_store;
use primitives::test_utils::{get_key_pair_from_seed, init_test_logger};

type ClientMock = Mocker<ClientActor>;

fn make_peer_manager(seed: &str, port: u16, boot_nodes: Vec<(&str, u16)>) -> PeerManagerActor {
    let store = create_test_store();
    let mut config = NetworkConfig::from_seed(seed, port);
    config.boot_nodes = convert_boot_nodes(boot_nodes);
    let client_addr = ClientMock::mock(Box::new(move |msg, ctx| {
        let msg = msg.downcast_ref::<NetworkClientMessages>().unwrap();
        // TODO: add mock.
        Box::new(Some(NetworkClientResponses::NoResponse))
    }))
    .start();
    PeerManagerActor::new(store, config, client_addr.recipient())
}

fn wait<F>(f: F, check_interval_ms: u64, max_wait_ms: u64)
where
    F: Fn() -> bool,
{
    let mut ms_slept = 0;
    let mut stop = false;
    //    while !stop {
    //        actix::spawn(Delay::new(Instant::now() + Duration::from_secs(1)).then(|_| { stop = f(); future::result(Ok(())) }));
    //        ms_slept += check_interval_ms;
    //        if ms_slept > max_wait_ms {
    //            panic!("Timed out waiting for the condition");
    //        }
    //    }
}

#[test]
fn peer_handshake() {
    init_test_logger();

    System::run(|| {
        let (port1, port2) = (open_port(), open_port());
        let pm1 = make_peer_manager("test1", port1, vec![("test2", port2)]).start();
        let pm2 = make_peer_manager("test2", port2, vec![("test1", port1)]).start();
        // let num_peers = pm1.send(NumActivePeers {});
        //        wait(|| {}, 50, 100)
        //        actix::run(Interval::new(Instant::now(), Duration::from_millis(100)).then(|_| {
        ////            num_peers.map(|res| {
        ////                println!("!!! ");
        ////                if res == 1 {
        ////                    System::current().stop();
        ////                }
        ////            }).map_err(|e| {});
        //            future::result(Ok(()))
        //        }));
        //        wait(|| {
        //            let mut stop = false;
        //            pm1.send(NumActivePeers {}).map(|res| if res == 1 { stop = true; });
        //            stop
        //        }, 50, 1000);
        //        System::current().stop();
        //        let f2 = Interval::new(Instant::now(), Duration::from_millis(100)).then(|_| {
        //            future::result(Ok(()))
        //        }).finish();
        actix::spawn(Delay::new(Instant::now() + Duration::from_secs(1)).then(|_| {
            System::current().stop();
            future::result(Ok(()))
        }));
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
        actix::spawn(Delay::new(Instant::now() + Duration::from_secs(1)).then(|_| {
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap()
}
