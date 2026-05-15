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
use std::sync::{Arc, Mutex};

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

    let bp = env.test_loop.breakpoint("midway").when(|ctx| ctx.get("task") == Some("a")).arm();

    let spawner = env.test_loop.async_computation_spawner("demo", |_| Duration::ZERO);

    let log_a = log.clone();
    spawner.spawn("task_a", move || {
        log_a.lock().unwrap().push("a1");
        test_loop_yield!("midway", task = "a");
        log_a.lock().unwrap().push("a2");
    });

    let log_b = log.clone();
    spawner.spawn("task_b", move || {
        log_b.lock().unwrap().push("b1");
        // Same yield-point name, different tag — the predicate filters this hit out, so B
        // runs straight through. Proves the matcher discriminates per-tag.
        test_loop_yield!("midway", task = "b");
        log_b.lock().unwrap().push("b2");
    });

    env.test_loop.run_until(|_| bp.hit_count() >= 1, Duration::seconds(5));

    // A is parked mid-flight; B has run to completion in the meantime.
    assert_eq!(*log.lock().unwrap(), vec!["a1", "b1", "b2"]);

    let hit = bp.take_hit().expect("task A should be parked at the breakpoint");
    assert_eq!(hit.context().get("task"), Some("a"));

    hit.resume();
    env.test_loop.run_for(Duration::milliseconds(1));

    assert_eq!(*log.lock().unwrap(), vec!["a1", "b1", "b2", "a2"]);
}
