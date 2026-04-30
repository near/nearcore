use crate::futures::AsyncComputationSpawner;

/// `AsyncComputationSpawner` backed by the global rayon thread pool.
///
/// Preserves the caller's `tracing::dispatcher` across the rayon hop so
/// spans don't disappear from spawned work.
pub struct RayonAsyncComputationSpawner;

impl AsyncComputationSpawner for RayonAsyncComputationSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        rayon::spawn(move || tracing::dispatcher::with_default(&dispatcher, f))
    }
}
