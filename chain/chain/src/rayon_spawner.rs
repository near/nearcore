use near_async::futures::AsyncComputationSpawner;

pub struct RayonAsyncComputationSpawner;

impl AsyncComputationSpawner for RayonAsyncComputationSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        rayon::spawn(move || tracing::dispatcher::with_default(&dispatcher, f))
    }
}
