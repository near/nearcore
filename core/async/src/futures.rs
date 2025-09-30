use futures::FutureExt;
pub use futures::future::BoxFuture; // pub for macros
use near_time::Duration;
use std::sync::Arc;

/// Abstraction for something that can drive futures.
///
/// Rust futures don't run by itself. It needs a driver to execute it. This can
/// for example be a thread, a thread pool, or Actix. This trait abstracts over
/// the execution mechanism.
///
/// The reason why we need an abstraction is (1) we can intercept the future
/// spawning to add additional instrumentation (2) we can support driving the
/// future with TestLoop for testing.
pub trait FutureSpawner: Send + Sync {
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

/// Given a future, respawn it as an equivalent future but which does not block the
/// driver of the future. For example, if the given future directly performs
/// computation, normally the whoever drives the future (such as a buffered_unordered)
/// would be blocked by the computation, thereby not allowing computation of other
/// futures driven by the same driver to proceed. This function respawns the future
/// onto the FutureSpawner, so the driver of the returned future would not be blocked.
pub fn respawn_for_parallelism<T: Send + 'static>(
    future_spawner: &dyn FutureSpawner,
    name: &'static str,
    f: impl std::future::Future<Output = T> + Send + 'static,
) -> impl std::future::Future<Output = T> + Send + 'static {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    future_spawner.spawn(name, async move {
        sender.send(f.await).ok();
    });
    async move { receiver.await.unwrap() }
}

/// A FutureSpawner that gives futures to a tokio Runtime, possibly supporting
/// multiple threads.
pub struct TokioRuntimeFutureSpawner(pub Arc<tokio::runtime::Runtime>);

impl FutureSpawner for TokioRuntimeFutureSpawner {
    fn spawn_boxed(&self, _description: &'static str, f: BoxFuture<'static, ()>) {
        self.0.spawn(f);
    }
}

/// A FutureSpawner that directly spawns using tokio::spawn. Use only for tests.
/// This is undesirable in production code because it bypasses any tracking functionality.
pub struct DirectTokioFutureSpawnerForTest;

impl FutureSpawner for DirectTokioFutureSpawnerForTest {
    fn spawn_boxed(&self, _description: &'static str, f: BoxFuture<'static, ()>) {
        tokio::spawn(f);
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
        name: &'static str,
        dur: Duration,
        f: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>,
    );
}

pub trait DelayedActionRunnerExt<T> {
    fn run_later(
        &mut self,
        name: &'static str,
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
        name: &'static str,
        dur: Duration,
        f: impl FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static,
    ) {
        self.run_later_boxed(name, dur, Box::new(f));
    }
}

impl<T> DelayedActionRunnerExt<T> for dyn DelayedActionRunner<T> + '_ {
    fn run_later(
        &mut self,
        name: &'static str,
        dur: Duration,
        f: impl FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static,
    ) {
        self.run_later_boxed(name, dur, Box::new(f));
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
