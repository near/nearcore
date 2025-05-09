use near_async::futures::AsyncComputationSpawner;

pub struct RayonAsyncComputationSpawner;

impl AsyncComputationSpawner for RayonAsyncComputationSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        rayon::spawn(move || tracing::dispatcher::with_default(&dispatcher, f))
    }
}

pub struct RayonAsyncComputationSpawnerWithDedicatedThreadPool {
    thread_pool: rayon::ThreadPool,
}

impl RayonAsyncComputationSpawnerWithDedicatedThreadPool {
    pub fn new(num_threads: usize) -> Self {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|index| format!("rayon-async-computation-{}", index))
            .build()
            .expect("Failed to create Rayon thread pool");
        Self { thread_pool }
    }
}

impl AsyncComputationSpawner for RayonAsyncComputationSpawnerWithDedicatedThreadPool {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        self.thread_pool.spawn(move || {
            tracing::dispatcher::with_default(&dispatcher, f);
        });
    }
}
