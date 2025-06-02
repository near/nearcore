use near_async::futures::AsyncComputationSpawner;
use rayon::ThreadPoolBuilder;

pub struct RayonAsyncComputationSpawner;

impl AsyncComputationSpawner for RayonAsyncComputationSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        rayon::spawn(move || tracing::dispatcher::with_default(&dispatcher, f))
    }
}

pub struct DedicatedThreadsAsyncComputationSpawner {
    pool: rayon::ThreadPool,
}

impl DedicatedThreadsAsyncComputationSpawner {
    pub fn new(num_threads: usize) -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .expect("Failed to build thread pool");
        Self { pool }
    }
}

impl AsyncComputationSpawner for DedicatedThreadsAsyncComputationSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        self.pool.spawn(move || tracing::dispatcher::with_default(&dispatcher, f));
    }
}
