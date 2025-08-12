use std::ops::DerefMut;

use actix::Actor;
use futures::future::BoxFuture;
use near_time::Duration;

use crate::futures::{DelayedActionRunner, FutureSpawner};

/// A FutureSpawner that hands over the future to Actix.
pub struct ActixFutureSpawner;

impl FutureSpawner for ActixFutureSpawner {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        near_performance_metrics::actix::spawn(description, f);
    }
}

pub struct ActixArbiterHandleFutureSpawner(pub actix::ArbiterHandle);

impl FutureSpawner for ActixArbiterHandleFutureSpawner {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        if !self.0.spawn(f) {
            near_o11y::tracing::error!(
                "Failed to spawn future: {}, arbiter has exited",
                description
            );
        }
    }
}

/// Implementation of `DelayedActionRunner` for Actix. With this, any code
/// that used to take a `&mut actix::Context` can now take a
/// `&mut dyn DelayedActionRunner<T>` instead, which isn't actix-specific.
impl<T, Outer> DelayedActionRunner<T> for actix::Context<Outer>
where
    T: 'static,
    Outer: DerefMut<Target = T>,
    Outer: Actor<Context = actix::Context<Outer>>,
{
    fn run_later_boxed(
        &mut self,
        _name: &str,
        dur: Duration,
        f: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>,
    ) {
        near_performance_metrics::actix::run_later(
            self,
            dur.max(Duration::ZERO).unsigned_abs(),
            move |obj, ctx| f(&mut *obj, ctx),
        );
    }
}
