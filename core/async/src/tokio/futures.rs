use tokio_util::sync::CancellationToken;

use crate::futures::FutureSpawner;
use crate::tokio::runtime::AsyncDroppableRuntime;

#[derive(Clone)]
pub struct CancellableFutureSpawner {
    pub(super) runtime_handle: tokio::runtime::Handle,
}

impl CancellableFutureSpawner {
    pub(crate) fn new(cancel: CancellationToken) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let runtime_handle = runtime.handle().clone();

        runtime_handle.spawn(async move {
            let _runtime = AsyncDroppableRuntime::new(runtime);
            cancel.cancelled().await;
        });
        Self { runtime_handle }
    }

    pub(crate) fn future_spawner(&self) -> Box<dyn FutureSpawner> {
        Box::new(self.clone())
    }
}

impl FutureSpawner for CancellableFutureSpawner {
    fn spawn_boxed(&self, description: &'static str, f: crate::futures::BoxFuture<'static, ()>) {
        tracing::trace!(target: "cancellable_tokio_runtime", description, "spawning future");
        self.runtime_handle.spawn(f);
    }
}
