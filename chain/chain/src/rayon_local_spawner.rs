use near_async::futures::AsyncComputationSpawner;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub struct RayonAsyncLocalComputationSpawner(ThreadPool);

impl RayonAsyncLocalComputationSpawner {
    pub fn new(num_threads: usize) -> Self {
        Self(
            ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .expect("Failed to create local rayon thread pool"),
        )
    }
}

impl AsyncComputationSpawner for RayonAsyncLocalComputationSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        self.0.spawn(move || tracing::dispatcher::with_default(&dispatcher, f))
    }
}
