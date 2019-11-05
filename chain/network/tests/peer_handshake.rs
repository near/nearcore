use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use actix::actors::mocker::Mocker;
use actix::Actor;
use actix::System;
use futures::future;
use futures::future::Future;

use near_client::ClientActor;
use near_network::test_utils::{convert_boot_nodes, open_port, WaitOrTimeout};
use near_network::types::{NetworkInfo, StopSignal};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkConfig, NetworkRequests,
    NetworkResponses, PeerManagerActor,
};
use near_primitives::test_utils::init_test_logger;
use near_store::test_utils::create_test_store;

type ClientMock = Mocker<ClientActor>;

fn make_peer_manager(
    seed: &str,
    port: u16,
    boot_nodes: Vec<(&str, u16)>,
    peer_max_count: u32,
) -> PeerManagerActor {
    let store = create_test_store();
    let mut config = NetworkConfig::from_seed(seed, port);
    config.boot_nodes = convert_boot_nodes(boot_nodes);
    config.peer_max_count = peer_max_count;
    let client_addr = ClientMock::mock(Box::new(move |msg, _ctx| {
        let msg = msg.downcast_ref::<NetworkClientMessages>().unwrap();
        match msg {
            NetworkClientMessages::GetChainInfo => {
                Box::new(Some(NetworkClientResponses::ChainInfo {
                    genesis_id: Default::default(),
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
        let pm1 = make_peer_manager("test1", port1, vec![("test2", port2)], 10).start();
        let _pm2 = make_peer_manager("test2", port2, vec![("test1", port1)], 10).start();
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(pm1.send(NetworkRequests::FetchInfo).then(move |res| {
                    if let NetworkResponses::Info(NetworkInfo { num_active_peers, .. }) =
                        res.unwrap()
                    {
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
        let _pm = make_peer_manager("test", port, vec![], 10).start();
        let mut peers = vec![];

        let num_peers = 5;
        for i in 0..num_peers {
            let pm =
                make_peer_manager(&format!("test{}", i), open_port(), vec![("test", port)], 10);
            peers.push(pm.start());
        }
        let flags = Arc::new(AtomicUsize::new(0));
        WaitOrTimeout::new(
            Box::new(move |_| {
                for i in 0..num_peers {
                    let flags1 = flags.clone();
                    actix::spawn(peers[i].send(NetworkRequests::FetchInfo).then(move |res| {
                        if let NetworkResponses::Info(NetworkInfo { num_active_peers, .. }) =
                            res.unwrap()
                        {
                            if num_active_peers > num_peers - 1
                                && (flags1.load(Ordering::Relaxed) >> i) % 2 == 0
                            {
                                println!("Peer {}: {}", i, num_active_peers);
                                flags1.fetch_add(1 << i, Ordering::Relaxed);
                            }
                        }
                        future::result(Ok(()))
                    }));
                }
                // Stop if all connected to all after exchanging peers.
                println!("Flags: {}", flags.load(Ordering::Relaxed));
                if flags.load(Ordering::Relaxed) == (1 << num_peers) - 1 {
                    System::current().stop();
                }
            }),
            100,
            10000,
        )
        .start();
    })
    .unwrap()
}

/// Check network is able to recover after node restart.
#[test]
fn peer_recover() {
    init_test_logger();

    System::run(|| {
        let port0 = open_port();
        let pm0 = Arc::new(make_peer_manager("test0", port0, vec![], 0).start());
        let _pm1 = make_peer_manager("test1", open_port(), vec![("test0", port0)], 1).start();

        let mut pm2 =
            Arc::new(make_peer_manager("test2", open_port(), vec![("test0", port0)], 1).start());

        let state = Arc::new(AtomicUsize::new(0));
        let flag = Arc::new(AtomicBool::new(false));

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                if state.load(Ordering::Relaxed) == 0 {
                    // Wait a small timeout for connection to be active.
                    state.store(1, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 1 {
                    // Stop node2.
                    let _ = pm2.do_send(StopSignal {});
                    state.store(2, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 2 {
                    // Wait until node0 removes node2 from active validators.
                    if !flag.load(Ordering::Relaxed) {
                        let flag1 = flag.clone();
                        actix::spawn(pm0.send(NetworkRequests::FetchInfo).then(move |res| {
                            if let Ok(NetworkResponses::Info(info)) = res {
                                if info.active_peers.len() == 1 {
                                    flag1.clone().store(true, Ordering::Relaxed);
                                }
                            }
                            future::result::<_, ()>(Ok(()))
                        }));
                    } else {
                        state.store(3, Ordering::Relaxed);
                    }
                } else if state.load(Ordering::Relaxed) == 3 {
                    // Start node2 from scratch again.
                    pm2 = Arc::new(
                        make_peer_manager("test2", open_port(), vec![("test0", port0)], 1).start(),
                    );

                    state.store(4, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 4 {
                    // Wait until node2 is connected with node0
                    actix::spawn(pm2.send(NetworkRequests::FetchInfo).then(|res| {
                        if let Ok(NetworkResponses::Info(info)) = res {
                            if info.active_peers.len() == 1 {
                                System::current().stop();
                            }
                        }
                        future::result::<_, ()>(Ok(()))
                    }));
                }
            }),
            100,
            10000,
        )
        .start();
    })
    .unwrap();
}
