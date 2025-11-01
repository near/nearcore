//! Utilities that let the deterministic TestLoop runtime drive async-style workloads.
//!
//! This module exposes two adapters that translate runtime interactions into TestLoop events:
//! - `TestLoopFutureSpawner` implements [`FutureSpawner`] by enqueueing futures onto the loop so
//!   any `spawn_boxed` call becomes a scheduled callback that the loop will poll to completion.
//! - `TestLoopAsyncComputationSpawner` implements [`AsyncComputationSpawner`] by scheduling
//!   blocking or synchronous computations through the loop, optionally delaying them via a
//!   caller-provided function.
//!
//! Using these adapters keeps all side effects observable and controllable by tests that advance
//! the loop manually.

use super::PendingEventsSender;
use super::data::TestLoopData;
use crate::futures::{AsyncComputationSpawner, FutureSpawner};
use futures::future::BoxFuture;
use futures::task::{ArcWake, waker_ref};
use near_time::Duration;
use parking_lot::Mutex;
use std::sync::Arc;
use std::task::Context;

/// Alias so the pending event sender can be handed to components that expect a [`FutureSpawner`].
pub type TestLoopFutureSpawner = PendingEventsSender;

impl FutureSpawner for TestLoopFutureSpawner {
    fn spawn_boxed(&self, description: &str, f: BoxFuture<'static, ()>) {
        let task = Arc::new(FutureTask {
            future: Mutex::new(Some(f)),
            sender: self.clone(),
            description: description.to_string(),
        });
        let callback = move |_: &mut TestLoopData| drive_futures(&task);
        self.send(format!("FutureSpawn({})", description), Box::new(callback));
    }
}

struct FutureTask {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    sender: PendingEventsSender,
    description: String,
}

impl ArcWake for FutureTask {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let clone = arc_self.clone();
        arc_self.sender.send(
            format!("FutureTask({})", arc_self.description),
            Box::new(move |_: &mut TestLoopData| drive_futures(&clone)),
        );
    }
}

fn drive_futures(task: &Arc<FutureTask>) {
    // Mirrors the single-threaded executor from the Rust async book. If the task still owns a
    // future, poll it; on Pending, stash it back so the next wake-up can retry.
    let mut future_slot = task.future.lock();
    if let Some(mut future) = future_slot.take() {
        let waker = waker_ref(&task);
        let context = &mut Context::from_waker(&*waker);
        if future.as_mut().poll(context).is_pending() {
            // Not done yet; store the future again so the waker can reschedule it.
            *future_slot = Some(future);
        }
    }
}

/// [`AsyncComputationSpawner`] implementation that schedules the computation via the TestLoop.
pub struct TestLoopAsyncComputationSpawner {
    sender: PendingEventsSender,
    artificial_delay: Box<dyn Fn(&str) -> Duration + Send + Sync>,
}

impl TestLoopAsyncComputationSpawner {
    pub fn new(
        sender: PendingEventsSender,
        artificial_delay: impl Fn(&str) -> Duration + Send + Sync + 'static,
    ) -> Self {
        Self { sender, artificial_delay: Box::new(artificial_delay) }
    }
}

impl AsyncComputationSpawner for TestLoopAsyncComputationSpawner {
    fn spawn_boxed(&self, name: &str, f: Box<dyn FnOnce() + Send>) {
        self.sender.send_with_delay(
            format!("AsyncComputation({})", name),
            Box::new(move |_| f()),
            (self.artificial_delay)(name),
        );
    }
}
