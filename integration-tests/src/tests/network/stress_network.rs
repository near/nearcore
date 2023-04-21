use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, AsyncContext, System};
use futures::FutureExt;
use near_async::messaging::Sender;
use tracing::info;

use near_actix_test_utils::run_actix;
use near_async::time;
use near_network::tcp;
use near_o11y::testonly::init_test_logger_allow_panic;
use near_primitives::block::GenesisId;

use near_network::config;
use near_network::test_utils::{convert_boot_nodes, GetInfo, StopSignal, WaitOrTimeoutActor};
use near_network::PeerManagerActor;
use near_o11y::WithSpanContextExt;

fn make_peer_manager(
    seed: &str,
    addr: tcp::ListenerAddr,
    boot_nodes: Vec<(&str, std::net::SocketAddr)>,
) -> actix::Addr<PeerManagerActor> {
    let mut config = config::NetworkConfig::from_seed(seed, addr);
    config.peer_store.boot_nodes = convert_boot_nodes(boot_nodes);
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

/// This test spawns several (7) nodes but node 0 crash very frequently and restart.
/// Other nodes should not panic because node 0 behavior.
///
/// If everything goes well this test should panic after the timeout triggered by WaitOrTimeout.
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
#[test]
#[should_panic]
#[ignore]
fn stress_test() {
    init_test_logger_allow_panic();

    run_actix(async {
        let num_nodes = 7;
        let addrs: Vec<_> = (0..num_nodes).map(|_| tcp::ListenerAddr::reserve_for_test()).collect();

        let boot_nodes: Vec<_> =
            addrs.iter().enumerate().map(|(ix, addr)| (format!("test{}", ix), **addr)).collect();

        let mut pms: Vec<_> = (0..num_nodes)
            .map(|ix| {
                Arc::new(make_peer_manager(
                    format!("test{}", ix).as_str(),
                    addrs[ix],
                    boot_nodes.iter().map(|(acc, addr)| (acc.as_str(), *addr)).collect(),
                ))
            })
            .collect();

        pms[0].do_send(StopSignal::should_panic().with_span_context());

        // States:
        // 0 -> Check other nodes health.
        // 1 -> Spawn node0 and schedule crash.
        // 2 -> Timeout.
        let state = Arc::new(AtomicUsize::new(0));
        let flags: Vec<_> = (0..num_nodes).map(|_| Arc::new(AtomicBool::new(false))).collect();
        let round = Arc::new(AtomicUsize::new(0));

        WaitOrTimeoutActor::new(
            Box::new(move |ctx| {
                let s = state.load(Ordering::Relaxed);
                if s == 0 {
                    info!(target: "test", "Start round: {}", round.fetch_add(1, Ordering::Relaxed));

                    for (ix, flag) in flags.iter().enumerate().skip(1) {
                        if !flag.load(Ordering::Relaxed) {
                            let flag1 = flag.clone();

                            let actor = pms[ix].send(GetInfo {}.with_span_context());
                            let actor = actor.then(move |info| {
                                if let Ok(info) = info {
                                    if info.num_connected_peers == num_nodes - 2 {
                                        flag1.store(true, Ordering::Relaxed);
                                    }
                                } else {
                                    info!(target: "test", "Node {} have failed", ix);
                                    System::current().stop();
                                }

                                futures::future::ready(())
                            });
                            actix::spawn(actor);
                        }
                    }

                    if flags.iter().skip(1).all(|flag| flag.load(Ordering::Relaxed)) {
                        state.store(1, Ordering::Relaxed);
                    }
                } else if s == 1 {
                    state.store(2, Ordering::Relaxed);

                    for flag in flags.iter() {
                        flag.store(false, Ordering::Relaxed);
                    }

                    pms[0] = Arc::new(make_peer_manager(
                        "test0",
                        addrs[0],
                        boot_nodes.iter().map(|(acc, addr)| (acc.as_str(), *addr)).collect(),
                    ));

                    let pm0 = pms[0].clone();

                    ctx.run_later(Duration::from_millis(10), move |_, _| {
                        pm0.do_send(StopSignal::should_panic().with_span_context());
                    });

                    let state1 = state.clone();
                    ctx.run_later(Duration::from_millis(100), move |_, _| {
                        state1.store(0, Ordering::Relaxed);
                    });
                }
            }),
            100,
            10000,
        )
        .start();
    });
}
