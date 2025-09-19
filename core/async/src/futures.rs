use futures::FutureExt;
pub use futures::future::BoxFuture; // pub for macros
use near_time::Duration;
use std::sync::{Arc, mpsc};

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
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        std::thread::spawn(move || tracing::dispatcher::with_default(&dispatcher, f));
    }
}

pub fn spawn_and_collect<I, F, G, T>(
    spawner: Arc<dyn AsyncComputationSpawner>,
    name: &str,
    inputs: I,
    f: F,
    with_result: G,
) where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> T + Send + Sync + 'static,
    T: Send + 'static,
    G: FnOnce(Vec<T>) + Send + Sync + 'static,
{
    let inputs: Vec<I::Item> = inputs.into_iter().collect();
    let n = inputs.len();

    if n == 0 {
        spawner.spawn(name, move || with_result(vec![]));
        return;
    }

    let (tx, rx) = mpsc::channel();
    let tx = Arc::new(tx);
    let f = Arc::new(f);

    for (idx, item) in inputs.into_iter().enumerate() {
        // tracing::warn!(target: "async", idx, num_tasks=n, name, "Spawning task");
        let tx = tx.clone();
        let f = Arc::clone(&f);
        spawner.spawn(name, move || {
            // tracing::warn!(target: "async", idx, num_tasks=n, "Task started");
            let out = f(item);
            tx.send((idx, out)).unwrap();
        });
        // tracing::warn!(target: "async", idx, num_tasks=n, name, "Task spawned");
    }
    drop(tx); // no more sends

    // tracing::warn!(target: "async", num_tasks=n, "Spawning collector task");
    spawner.spawn(name, move || {
        // tracing::warn!(target: "async", num_tasks=n, "Collector task started");
        let mut results: Vec<Option<T>> = std::iter::repeat_with(|| None).take(n).collect();
        for (idx, val) in rx {
            // tracing::warn!(target: "async", idx, num_tasks=n, "Task done");
            results[idx] = Some(val);
        }
        // tracing::warn!(target: "async", num_tasks=n, "All tasks done");
        with_result(results.into_iter().map(|x| x.unwrap()).collect());
    });
    // tracing::warn!(target: "async", num_tasks=n, "Collector task spawned");
}
