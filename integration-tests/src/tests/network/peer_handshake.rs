use crate::tests::network::runner::*;
use near_async::messaging::{CanSend, CanSendAsync};
use near_async::tokio::TokioRuntimeHandle;
use near_async::{ActorSystem, time};
use near_network::PeerManagerActor;
use near_network::config;
use near_network::tcp;
use near_network::test_utils::{GetInfo, StopSignal, convert_boot_nodes, wait_or_timeout};
use near_o11y::testonly::init_test_logger;
use near_primitives::genesis::GenesisId;
use parking_lot::Mutex;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn make_peer_manager(
    actor_system: ActorSystem,
    seed: &str,
    node_addr: tcp::ListenerAddr,
    boot_nodes: Vec<(&str, std::net::SocketAddr)>,
    peer_max_count: u32,
) -> TokioRuntimeHandle<PeerManagerActor> {
    use near_async::messaging::{IntoMultiSender, IntoSender, noop};

    let mut config = config::NetworkConfig::from_seed(seed, node_addr);
    config.peer_store.boot_nodes = convert_boot_nodes(boot_nodes);
    config.max_num_peers = peer_max_count;
    config.ideal_connections_hi = peer_max_count;
    config.ideal_connections_lo = peer_max_count;

    PeerManagerActor::spawn(
        time::Clock::real(),
        actor_system,
        near_store::db::TestDB::new(),
        config,
        noop().into_multi_sender(),
        noop().into_multi_sender(),
        noop().into_multi_sender(),
        noop().into_sender(),
        noop().into_multi_sender(),
        noop().into_multi_sender(),
        GenesisId::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn peer_handshake() {
    init_test_logger();

    let addr1 = tcp::ListenerAddr::reserve_for_test();
    let addr2 = tcp::ListenerAddr::reserve_for_test();
    let actor_system = ActorSystem::new();
    let pm1 = make_peer_manager(actor_system.clone(), "test1", addr1, vec![("test2", *addr2)], 10);
    let _pm2 = make_peer_manager(actor_system.clone(), "test2", addr2, vec![("test1", *addr1)], 10);
    wait_or_timeout(100, 2000, || async {
        let info = pm1.send_async(GetInfo {}).await.unwrap();
        if info.num_connected_peers == 1 {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .await
    .unwrap();
    actor_system.stop();
}

#[tokio::test]
async fn peers_connect_all() {
    init_test_logger();

    let addr = tcp::ListenerAddr::reserve_for_test();
    let actor_system = ActorSystem::new();
    let _pm = make_peer_manager(actor_system.clone(), "test", addr, vec![], 10);
    let mut peers = vec![];

    let num_peers = 5;
    for i in 0..num_peers {
        let pm = make_peer_manager(
            actor_system.clone(),
            &format!("test{}", i),
            tcp::ListenerAddr::reserve_for_test(),
            vec![("test", *addr)],
            10,
        );
        peers.push(pm);
    }
    let flags = Arc::new(AtomicUsize::new(0));
    wait_or_timeout(100, 10000, move || {
        let flags = flags.clone();
        let peers = peers.clone();
        async move {
            for i in 0..num_peers {
                let flags1 = flags.clone();
                let info = peers[i].send_async(GetInfo {}).await.unwrap();
                if info.num_connected_peers > num_peers - 1
                    && (flags1.load(Ordering::Relaxed) >> i) % 2 == 0
                {
                    flags1.fetch_add(1 << i, Ordering::Relaxed);
                }
            }
            // Stop if all connected to all after exchanging peers.
            if flags.load(Ordering::Relaxed) == (1 << num_peers) - 1 {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    })
    .await
    .unwrap();
    actor_system.stop();
}

/// Check network is able to recover after node restart.
#[tokio::test]
async fn peer_recover() {
    init_test_logger();

    let addr0 = tcp::ListenerAddr::reserve_for_test();
    let actor_system = ActorSystem::new();
    let pm0 = Arc::new(make_peer_manager(actor_system.clone(), "test0", addr0, vec![], 2));
    let _pm1 = make_peer_manager(
        actor_system.clone(),
        "test1",
        tcp::ListenerAddr::reserve_for_test(),
        vec![("test0", *addr0)],
        1,
    );

    let pm2 = Arc::new(Mutex::new(Arc::new(make_peer_manager(
        actor_system.clone(),
        "test2",
        tcp::ListenerAddr::reserve_for_test(),
        vec![("test0", *addr0)],
        1,
    ))));

    let state = Arc::new(AtomicUsize::new(0));
    let flag = Arc::new(AtomicBool::new(false));

    let actor_system_clone = actor_system.clone();
    wait_or_timeout(100, 10000, move || {
        let state = state.clone();
        let flag = flag.clone();
        let pm0 = pm0.clone();
        let pm2 = pm2.clone();
        let actor_system = actor_system_clone.clone();
        async move {
            if state.load(Ordering::Relaxed) == 0 {
                // Wait a small timeout for connection to be active.
                state.store(1, Ordering::Relaxed);
            } else if state.load(Ordering::Relaxed) == 1 {
                // Stop node2.
                let _ = pm2.lock().send(StopSignal::default());
                state.store(2, Ordering::Relaxed);
            } else if state.load(Ordering::Relaxed) == 2 {
                // Wait until node0 removes node2 from active validators.
                if !flag.load(Ordering::Relaxed) {
                    if let Ok(info) = pm0.send_async(GetInfo {}).await {
                        if info.connected_peers.len() == 1 {
                            flag.store(true, Ordering::Relaxed);
                        }
                    }
                } else {
                    state.store(3, Ordering::Relaxed);
                }
            } else if state.load(Ordering::Relaxed) == 3 {
                // Start node2 from scratch again.
                *pm2.lock() = Arc::new(make_peer_manager(
                    actor_system.clone(),
                    "test2",
                    tcp::ListenerAddr::reserve_for_test(),
                    vec![("test0", *addr0)],
                    1,
                ));

                state.store(4, Ordering::Relaxed);
            } else if state.load(Ordering::Relaxed) == 4 {
                // Wait until node2 is connected with node0
                let pm2 = { pm2.lock().clone() };
                if let Ok(info) = pm2.send_async(GetInfo {}).await {
                    if info.connected_peers.len() == 1 {
                        return ControlFlow::Break(());
                    }
                }
            }
            ControlFlow::Continue(())
        }
    })
    .await
    .unwrap();
    actor_system.stop();
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
