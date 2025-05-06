use std::sync::OnceLock;

use near_async::futures::AsyncComputationSpawner;

static CHAIN_POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

pub struct RayonAsyncComputationSpawner;

impl AsyncComputationSpawner for RayonAsyncComputationSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let pool = CHAIN_POOL.get_or_init(|| {
            rayon::ThreadPoolBuilder::new().build().expect("build async computation spawner pool")
        });
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        pool.spawn(move || tracing::dispatcher::with_default(&dispatcher, f))
    }
}
