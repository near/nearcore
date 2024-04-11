use crate::time::Duration;
use actix::Actor;
pub use futures::future::BoxFuture; // pub for macros
use futures::FutureExt;
use std::ops::DerefMut;

/// Abstraction for something that can drive futures.
///
/// Rust futures don't run by itself. It needs a driver to execute it. This can
/// for example be a thread, a thread pool, or Actix. This trait abstracts over
/// the execution mechanism.
///
/// The reason why we need an abstraction is (1) we can intercept the future
/// spawning to add additional instrumentation (2) we can support driving the
/// future with TestLoop for testing.
pub trait FutureSpawner {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>);
}

pub trait FutureSpawnerExt {
    fn spawn<F>(&self, description: &'static str, f: F)
    where
        F: futures::Future<Output = ()> + Send + 'static;
}

impl<T: FutureSpawner> FutureSpawnerExt for T {
    fn spawn<F>(&self, description: &'static str, f: F)
    where
        F: futures::Future<Output = ()> + Send + 'static,
    {
        self.spawn_boxed(description, f.boxed());
    }
}

impl FutureSpawnerExt for dyn FutureSpawner + '_ {
    fn spawn<F>(&self, description: &'static str, f: F)
    where
        F: futures::Future<Output = ()> + Send + 'static,
    {
        self.spawn_boxed(description, f.boxed());
    }
}

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

/// Abstraction for something that can schedule something to run after.
/// This isn't the same as just delaying a closure. Rather, it has the
/// additional power of providing the closure a mutable reference to some
/// object. With the Actix framework, for example, the object (of type `T`)
/// would be the actor, and the `DelayedActionRunner<T>`` is implemented by
/// the actix `Context`.
pub trait DelayedActionRunner<T> {
    fn run_later_boxed(
        &mut self,
        name: &str,
        dur: Duration,
        f: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>,
    );
}

pub trait DelayedActionRunnerExt<T> {
    fn run_later(
        &mut self,
        name: &str,
        dur: Duration,
        f: impl FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static,
    );
}

impl<T, Runner> DelayedActionRunnerExt<T> for Runner
where
    Runner: DelayedActionRunner<T>,
{
    fn run_later(
        &mut self,
        name: &str,
        dur: Duration,
        f: impl FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static,
    ) {
        self.run_later_boxed(name, dur, Box::new(f));
    }
}

impl<T> DelayedActionRunnerExt<T> for dyn DelayedActionRunner<T> + '_ {
    fn run_later(
        &mut self,
        name: &str,
        dur: Duration,
        f: impl FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static,
    ) {
        self.run_later_boxed(name, dur, Box::new(f));
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
            move |obj, ctx| f(obj.deref_mut(), ctx),
        );
    }
}

/// Like `FutureSpawner`, but intended for spawning asynchronous computation-heavy tasks.
/// Rather than taking a future, it takes a function. For production, the function shall
/// be run on a separate thread (like `rayon::spawn`), but for test, it would run the
/// function as a TestLoop event, possibly with an artificial delay.
pub trait AsyncComputationSpawner: Send + Sync {
    fn spawn_boxed(&self, name: &str, f: Box<dyn FnOnce() + Send>);
}

pub trait AsyncComputationSpawnerExt {
    fn spawn(&self, name: &str, f: impl FnOnce() + Send + 'static);
}

impl<T: AsyncComputationSpawner> AsyncComputationSpawnerExt for T {
    fn spawn(&self, name: &str, f: impl FnOnce() + Send + 'static) {
        self.spawn_boxed(name, Box::new(f));
    }
}

impl AsyncComputationSpawnerExt for dyn AsyncComputationSpawner + '_ {
    fn spawn(&self, name: &str, f: impl FnOnce() + Send + 'static) {
        self.spawn_boxed(name, Box::new(f));
    }
}

pub struct StdThreadAsyncComputationSpawnerForTest;

impl AsyncComputationSpawner for StdThreadAsyncComputationSpawnerForTest {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        std::thread::spawn(f);
    }
}
