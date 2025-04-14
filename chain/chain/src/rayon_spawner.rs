use near_async::futures::AsyncComputationSpawner;

pub struct RayonAsyncComputationSpawner;

impl AsyncComputationSpawner for RayonAsyncComputationSpawner {
    fn spawn_boxed(&self, task_name: &str, f: Box<dyn FnOnce() + Send>) {
        let dispatcher = tracing::dispatcher::get_default(|it| it.clone());
        let tname = task_name.to_string();
        rayon::spawn(move || {
            let _span = tracing::debug_span!(
                target: "client",
                "rayon_async_computation",
                task_name = tname,
            )
            .entered();
            tracing::dispatcher::with_default(&dispatcher, f)
        })
    }
}
