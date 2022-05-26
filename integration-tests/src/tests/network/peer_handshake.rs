pub use crate::tests::network::runner::*;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::actors::mocker::Mocker;
use actix::System;
use actix::{Actor, Arbiter};
use futures::{future, FutureExt};

use near_actix_test_utils::run_actix;
use near_client::{ClientActor, ViewClientActor};
use near_logger_utils::init_test_logger;

use near_network::routing::start_routing_table_actor;

use near_network::test_utils::{
    convert_boot_nodes, open_port, wait_or_timeout, GetInfo, StopSignal, WaitOrTimeoutActor,
};
use near_network::types::NetworkClientResponses;
use near_network::PeerManagerActor;
use near_network_primitives::types::{
    NetworkConfig, NetworkViewClientMessages, NetworkViewClientResponses,
};
#[cfg(test)]
use near_store::test_utils::create_test_store;

type ClientMock = Mocker<ClientActor>;
type ViewClientMock = Mocker<ViewClientActor>;

#[cfg(test)]
fn make_peer_manager(
    seed: &str,
    port: u16,
    boot_nodes: Vec<(&str, u16)>,
    peer_max_count: u32,
) -> PeerManagerActor {
    let store = create_test_store();
    let mut config = NetworkConfig::from_seed(seed, port);
    config.boot_nodes = convert_boot_nodes(boot_nodes);
    config.max_num_peers = peer_max_count;
    let client_addr = ClientMock::mock(Box::new(move |_msg, _ctx| {
        Box::new(Some(NetworkClientResponses::NoResponse))
    }))
    .start();
    let view_client_addr = ViewClientMock::mock(Box::new(move |msg, _ctx| {
        let msg = msg.downcast_ref::<NetworkViewClientMessages>().unwrap();
        match msg {
            NetworkViewClientMessages::GetChainInfo => {
                Box::new(Some(NetworkViewClientResponses::ChainInfo {
                    genesis_id: Default::default(),
                    height: 1,
                    tracked_shards: vec![],
                    archival: false,
                }))
            }
            _ => Box::new(Some(NetworkViewClientResponses::NoResponse)),
        }
    }))
    .start();
    let routing_table_addr =
        start_routing_table_actor(config.node_id(), store.clone());

    PeerManagerActor::new(
        store,
        config,
        client_addr.recipient(),
        view_client_addr.recipient(),
        routing_table_addr,
    )
    .unwrap()
}

#[test]
fn peer_handshake() {
    init_test_logger();

    run_actix(async {
        let (port1, port2) = (open_port(), open_port());
        let pm1 = make_peer_manager("test1", port1, vec![("test2", port2)], 10).start();
        let _pm2 = make_peer_manager("test2", port2, vec![("test1", port1)], 10).start();
        wait_or_timeout(100, 2000, || async {
            let info = pm1.send(GetInfo {}).await.unwrap();
            if info.num_connected_peers == 1 {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        System::current().stop()
    });
}

#[test]
fn peers_connect_all() {
    init_test_logger();

    run_actix(async {
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
        WaitOrTimeoutActor::new(
            Box::new(move |_| {
                for i in 0..num_peers {
                    let flags1 = flags.clone();
                    actix::spawn(peers[i].send(GetInfo {}).then(move |res| {
                        let info = res.unwrap();
                        if info.num_connected_peers > num_peers - 1
                            && (flags1.load(Ordering::Relaxed) >> i) % 2 == 0
                        {
                            flags1.fetch_add(1 << i, Ordering::Relaxed);
                        }
                        future::ready(())
                    }));
                }
                // Stop if all connected to all after exchanging peers.
                if flags.load(Ordering::Relaxed) == (1 << num_peers) - 1 {
                    System::current().stop();
                }
            }),
            100,
            10000,
        )
        .start();
    });
}

/// Check network is able to recover after node restart.
#[test]
fn peer_recover() {
    init_test_logger();

    run_actix(async {
        let port0 = open_port();
        let pm0 = Arc::new(make_peer_manager("test0", port0, vec![], 2).start());
        let _pm1 = make_peer_manager("test1", open_port(), vec![("test0", port0)], 1).start();

        let mut pm2 =
            Arc::new(make_peer_manager("test2", open_port(), vec![("test0", port0)], 1).start());

        let state = Arc::new(AtomicUsize::new(0));
        let flag = Arc::new(AtomicBool::new(false));

        WaitOrTimeoutActor::new(
            Box::new(move |_ctx| {
                if state.load(Ordering::Relaxed) == 0 {
                    // Wait a small timeout for connection to be active.
                    state.store(1, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 1 {
                    // Stop node2.
                    let _ = pm2.do_send(StopSignal::default());
                    state.store(2, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 2 {
                    // Wait until node0 removes node2 from active validators.
                    if !flag.load(Ordering::Relaxed) {
                        let flag1 = flag.clone();
                        actix::spawn(pm0.send(GetInfo {}).then(move |res| {
                            if let Ok(info) = res {
                                if info.connected_peers.len() == 1 {
                                    flag1.store(true, Ordering::Relaxed);
                                }
                            }
                            future::ready(())
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
                    actix::spawn(pm2.send(GetInfo {}).then(|res| {
                        if let Ok(info) = res {
                            if info.connected_peers.len() == 1 {
                                System::current().stop();
                            }
                        }
                        future::ready(())
                    }));
                }
            }),
            100,
            10000,
        )
        .start();
    });
}

/// Create two nodes A and B and connect them.
/// Stop node B, change its identity (PeerId) and spawn it again.
/// B knows nothing about A (since store is wiped) and A knows old information from B.
/// A should learn new information from B and connect with it.
#[test]
fn check_connection_with_new_identity() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2).enable_outbound();

    // This is needed, because even if outbound is enabled, there is no booting nodes,
    // so A and B doesn't know each other yet.
    runner.push(Action::AddEdge { from: 0, to: 1, force: true });

    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    runner.push(Action::Stop(1));
    runner.push_action(change_account_id(1, "far".parse().unwrap()));
    runner.push(Action::CheckRoutingTable(0, vec![]));
    runner.push_action(restart(1));

    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    runner.push(Action::Wait(Duration::from_millis(2000)));

    // Check the no node tried to connect to itself in this process.
    #[cfg(feature = "test_features")]
    runner.push_action(wait_for(|| near_network::RECEIVED_INFO_ABOUT_ITSELF.get() == 0));

    start_test(runner)
}

#[test]
fn connection_spam_security_test() {
    init_test_logger();

    let vec: Arc<RwLock<Vec<TcpStream>>> = Arc::new(RwLock::new(Vec::new()));
    let vec2: Arc<RwLock<Vec<TcpStream>>> = vec.clone();
    run_actix(async move {
        let arbiter = Arbiter::new();
        let port = open_port();

        let pm = PeerManagerActor::start_in_arbiter(&arbiter.handle(), move |_ctx| {
            make_peer_manager("test1", port, vec![], 10)
        });

        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        while vec.read().unwrap().len() < 100 {
            if let Ok(stream) = TcpStream::connect_timeout(&addr.clone(), Duration::from_secs(10)) {
                vec.write().unwrap().push(stream);
            }
        }

        let iter = Arc::new(AtomicUsize::new(0));
        WaitOrTimeoutActor::new(
            Box::new(move |_| {
                let iter = iter.clone();
                actix::spawn(pm.send(GetInfo {}).then(move |res| {
                    let info = res.unwrap();
                    if info.peer_counter >= 70 {
                        iter.fetch_add(1, Ordering::SeqCst);
                        if iter.load(Ordering::SeqCst) >= 10 {
                            assert_eq!(info.peer_counter, 70);
                            System::current().stop();
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            500000,
        )
        .start();
    });
    assert_eq!(vec2.read().unwrap().len(), 100);
}
