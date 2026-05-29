//! Demonstrates the test-loop breakpoint/yield-point mechanism. See
//! `near_async::test_loop::breakpoint` for the full API.
//!
//! The yield point here is artificial — we drive `test_loop_yield!`-containing closures
//! directly through the async-computation spawner. A real test would trigger a code path
//! already instrumented at the relevant site (e.g. partial witness validation).

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::futures::AsyncComputationSpawnerExt;
use near_async::test_loop_yield;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::Balance;
use parking_lot::Mutex;
use std::sync::Arc;

/// Forces a non-default interleaving between two tasks that share a vector. Default
/// (FIFO at t=0) would be `[a1, a2, b1, b2]`; with the breakpoint we pause A between its
/// two pushes, let B finish, then resume A, observing `[a1, b1, b2, a2]`.
#[test]
fn test_breakpoint_interleaves_two_tasks() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_yield_points()
        .add_user_account(&user, Balance::from_near(10))
        .build();

    let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    // The predicate uses `node()` — auto-populated from the spawner identifier, no need to
    // pass anything explicit at the yield site.
    let bp = env.test_loop.breakpoint("midway").when(|ctx| ctx.node() == Some("alice")).arm();

    let spawner_a = env.test_loop.async_computation_spawner("alice", |_| Duration::ZERO);
    let spawner_b = env.test_loop.async_computation_spawner("bob", |_| Duration::ZERO);

    let log_a = log.clone();
    spawner_a.spawn("task", move || {
        log_a.lock().push("a1");
        test_loop_yield!("midway");
        log_a.lock().push("a2");
    });

    let log_b = log.clone();
    spawner_b.spawn("task", move || {
        log_b.lock().push("b1");
        // Same yield-point name, but `bob` doesn't match the `alice` predicate, so B runs
        // straight through.
        test_loop_yield!("midway");
        log_b.lock().push("b2");
    });

    env.test_loop.run_until(|_| bp.hit_count() >= 1, Duration::seconds(5));

    // A is parked mid-flight; B has run to completion in the meantime.
    assert_eq!(*log.lock(), vec!["a1", "b1", "b2"]);

    let hit = bp.take_hit().expect("alice's task should be parked at the breakpoint");
    assert_eq!(hit.context().node(), Some("alice"));

    hit.resume();
    env.test_loop.run_for(Duration::milliseconds(1));

    assert_eq!(*log.lock(), vec!["a1", "b1", "b2", "a2"]);
}
