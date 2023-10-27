use crate::tests::network::runner::*;
use actix::Actor;
use actix::System;
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_async::time;
use near_network::config;
use near_network::tcp;
use near_network::test_utils::{
    convert_boot_nodes, wait_or_timeout, GetInfo, StopSignal, WaitOrTimeoutActor,
};
use near_network::PeerManagerActor;
use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::block::GenesisId;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(test)]
fn make_peer_manager(
    seed: &str,
    node_addr: tcp::ListenerAddr,
    boot_nodes: Vec<(&str, std::net::SocketAddr)>,
    peer_max_count: u32,
) -> actix::Addr<PeerManagerActor> {
    use near_async::messaging::Sender;

    let mut config = config::NetworkConfig::from_seed(seed, node_addr);
    config.peer_store.boot_nodes = convert_boot_nodes(boot_nodes);
    config.max_num_peers = peer_max_count;
    config.ideal_connections_hi = peer_max_count;
    config.ideal_connections_lo = peer_max_count;

    PeerManagerActor::spawn(
        time::Clock::real(),
        near_store::db::TestDB::new(),
        config,
        Arc::new(near_network::client::Noop),
        Sender::noop(),
        GenesisId::default(),
    )
    .unwrap()
}

#[test]
fn peer_handshake() {
    init_test_logger();

    run_actix(async {
        let addr1 = tcp::ListenerAddr::reserve_for_test();
        let addr2 = tcp::ListenerAddr::reserve_for_test();
        let pm1 = make_peer_manager("test1", addr1, vec![("test2", *addr2)], 10);
        let _pm2 = make_peer_manager("test2", addr2, vec![("test1", *addr1)], 10);
        wait_or_timeout(100, 2000, || async {
            let info = pm1.send(GetInfo {}.with_span_context()).await.unwrap();
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
        let addr = tcp::ListenerAddr::reserve_for_test();
        let _pm = make_peer_manager("test", addr, vec![], 10);
        let mut peers = vec![];

        let num_peers = 5;
        for i in 0..num_peers {
            let pm = make_peer_manager(
                &format!("test{}", i),
                tcp::ListenerAddr::reserve_for_test(),
                vec![("test", *addr)],
                10,
            );
            peers.push(pm);
        }
        let flags = Arc::new(AtomicUsize::new(0));
        WaitOrTimeoutActor::new(
            Box::new(move |_| {
                for i in 0..num_peers {
                    let flags1 = flags.clone();
                    let actor = peers[i].send(GetInfo {}.with_span_context());
                    let actor = actor.then(move |res| {
                        let info = res.unwrap();
                        if info.num_connected_peers > num_peers - 1
                            && (flags1.load(Ordering::Relaxed) >> i) % 2 == 0
                        {
                            flags1.fetch_add(1 << i, Ordering::Relaxed);
                        }
                        future::ready(())
                    });
                    actix::spawn(actor);
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
        let addr0 = tcp::ListenerAddr::reserve_for_test();
        let pm0 = Arc::new(make_peer_manager("test0", addr0, vec![], 2));
        let _pm1 = make_peer_manager(
            "test1",
            tcp::ListenerAddr::reserve_for_test(),
            vec![("test0", *addr0)],
            1,
        );

        let mut pm2 = Arc::new(make_peer_manager(
            "test2",
            tcp::ListenerAddr::reserve_for_test(),
            vec![("test0", *addr0)],
            1,
        ));

        let state = Arc::new(AtomicUsize::new(0));
        let flag = Arc::new(AtomicBool::new(false));

        WaitOrTimeoutActor::new(
            Box::new(move |_ctx| {
                if state.load(Ordering::Relaxed) == 0 {
                    // Wait a small timeout for connection to be active.
                    state.store(1, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 1 {
                    // Stop node2.
                    let _ = pm2.do_send(StopSignal::default().with_span_context());
                    state.store(2, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 2 {
                    // Wait until node0 removes node2 from active validators.
                    if !flag.load(Ordering::Relaxed) {
                        let flag1 = flag.clone();
                        let actor = pm0.send(GetInfo {}.with_span_context());
                        let actor = actor.then(move |res| {
                            if let Ok(info) = res {
                                if info.connected_peers.len() == 1 {
                                    flag1.store(true, Ordering::Relaxed);
                                }
                            }
                            future::ready(())
                        });
                        actix::spawn(actor);
                    } else {
                        state.store(3, Ordering::Relaxed);
                    }
                } else if state.load(Ordering::Relaxed) == 3 {
                    // Start node2 from scratch again.
                    pm2 = Arc::new(make_peer_manager(
                        "test2",
                        tcp::ListenerAddr::reserve_for_test(),
                        vec![("test0", *addr0)],
                        1,
                    ));

                    state.store(4, Ordering::Relaxed);
                } else if state.load(Ordering::Relaxed) == 4 {
                    // Wait until node2 is connected with node0
                    let actor = pm2.send(GetInfo {}.with_span_context());
                    let actor = actor.then(|res| {
                        if let Ok(info) = res {
                            if info.connected_peers.len() == 1 {
                                System::current().stop();
                            }
                        }
                        future::ready(())
                    });
                    actix::spawn(actor);
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

    runner.push(Action::Wait(time::Duration::milliseconds(2000)));

    start_test(runner)
}
