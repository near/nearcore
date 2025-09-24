use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use near_async::messaging::{CanSend, CanSendAsync, IntoMultiSender, IntoSender, noop};
use tracing::info;

use near_async::{ActorSystem, time};
use near_network::tcp;
use near_o11y::testonly::init_test_logger_allow_panic;
use near_primitives::genesis::GenesisId;

use near_async::tokio::TokioRuntimeHandle;
use near_network::PeerManagerActor;
use near_network::config;
use near_network::test_utils::{GetInfo, StopSignal, convert_boot_nodes, wait_or_timeout};
use parking_lot::Mutex;

fn make_peer_manager(
    actor_system: ActorSystem,
    seed: &str,
    addr: tcp::ListenerAddr,
    boot_nodes: Vec<(&str, std::net::SocketAddr)>,
) -> TokioRuntimeHandle<PeerManagerActor> {
    let mut config = config::NetworkConfig::from_seed(seed, addr);
    config.peer_store.boot_nodes = convert_boot_nodes(boot_nodes);
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

/// This test spawns several (7) nodes but node 0 crash very frequently and restart.
/// Other nodes should not panic because node 0 behavior.
///
/// If everything goes well this test should panic after the timeout triggered by wait_or_timeout.
/// The test is stopped gracefully (no panic) if some node other than node0 panicked.
///
/// This was fixed in (#1954). To reproduce this bug:
/// ```
/// git checkout 1f5eab0344235960dfcf767d143fb90a02c7c567
/// cargo test --package near-network --test stress_network stress_test -- --exact --ignored
/// ```
///
/// Logs observed on failing commit:
/// ```
/// thread 'stress_test' panicked at 'called `Result::unwrap()` on an `Err` value: Os { code: 107, kind: NotConnected, message: "Transport endpoint is not connected" }', src/libcore/result.rs:1165:5
/// thread 'stress_test' panicked at 'Decoder error: Os { code: 104, kind: ConnectionReset, message: "Connection reset by peer" }', src/libcore/result.rs:1165:5
/// ```
#[tokio::test]
#[should_panic]
#[ignore]
async fn stress_test() {
    init_test_logger_allow_panic();

    let num_nodes = 7;
    let addrs: Vec<_> = (0..num_nodes).map(|_| tcp::ListenerAddr::reserve_for_test()).collect();

    let boot_nodes: Vec<_> =
        addrs.iter().enumerate().map(|(ix, addr)| (format!("test{}", ix), **addr)).collect();

    let actor_system = ActorSystem::new();
    let pms: Vec<_> = (0..num_nodes)
        .map(|ix| {
            Arc::new(make_peer_manager(
                actor_system.clone(),
                format!("test{}", ix).as_str(),
                addrs[ix],
                boot_nodes.iter().map(|(acc, addr)| (acc.as_str(), *addr)).collect(),
            ))
        })
        .collect();
    pms[0].send(StopSignal::should_panic());
    let pms = Arc::new(Mutex::new(pms));

    // States:
    // 0 -> Check other nodes health.
    // 1 -> Spawn node0 and schedule crash.
    // 2 -> Timeout.
    let state = Arc::new(AtomicUsize::new(0));
    let flags: Vec<_> = (0..num_nodes).map(|_| Arc::new(AtomicBool::new(false))).collect();
    let round = Arc::new(AtomicUsize::new(0));

    let actor_system_clone = actor_system.clone();
    wait_or_timeout(100, 10000, move || {
        let state = state.clone();
        let flags = flags.clone();
        let round = round.clone();
        let pms = pms.clone();
        let addrs = addrs.clone();
        let boot_nodes = boot_nodes.clone();
        let actor_system = actor_system_clone.clone();
        async move {
            let s = state.load(Ordering::Relaxed);
            if s == 0 {
                info!(target: "test", "Start round: {}", round.fetch_add(1, Ordering::Relaxed));

                for (ix, flag) in flags.iter().enumerate().skip(1) {
                    if !flag.load(Ordering::Relaxed) {
                        let flag1 = flag.clone();

                        let pm = (*pms.lock())[ix].clone();
                        if let Ok(info) = pm.send_async(GetInfo {}).await {
                            if info.num_connected_peers == num_nodes - 2 {
                                flag1.store(true, Ordering::Relaxed);
                            }
                        } else {
                            info!(target: "test", "Node {} have failed", ix);
                            return std::ops::ControlFlow::Break(());
                        }
                    }
                }

                if flags.iter().skip(1).all(|flag| flag.load(Ordering::Relaxed)) {
                    state.store(1, Ordering::Relaxed);
                }
            } else if s == 1 {
                state.store(2, Ordering::Relaxed);

                for flag in &flags {
                    flag.store(false, Ordering::Relaxed);
                }

                (*pms.lock())[0] = Arc::new(make_peer_manager(
                    actor_system.clone(),
                    "test0",
                    addrs[0],
                    boot_nodes.iter().map(|(acc, addr)| (acc.as_str(), *addr)).collect(),
                ));

                let pm0 = (*pms.lock())[0].clone();

                tokio::time::sleep(Duration::from_millis(10)).await;
                pm0.send(StopSignal::should_panic());

                tokio::time::sleep(Duration::from_millis(100)).await;
                state.store(0, Ordering::Relaxed);
            }
            std::ops::ControlFlow::Continue(())
        }
    })
    .await
    .unwrap();
    actor_system.stop();
}
