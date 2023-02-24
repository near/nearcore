use futures::{future::BoxFuture, FutureExt};

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

impl<'a> dyn FutureSpawner + 'a {
    /// Spawns a future by automatically boxing it.
    pub fn spawn<F>(&self, description: &'static str, f: F)
    where
        F: futures::Future<Output = ()> + Send + 'static,
    {
        self.spawn_boxed(description, f.boxed())
    }
}

/// A FutureSpawner that hands over the future to Actix.
pub struct ActixFutureSpawner;

impl FutureSpawner for ActixFutureSpawner {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        near_performance_metrics::actix::spawn(description, f);
    }
}
