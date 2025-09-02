use std::ops::Deref;

/// tokio::runtime::Runtime is not droppable within an async context because of blocking, but we do
/// need to drop runtimes for proper cleanup, so this wraps the runtime so that dropping it stops
/// the runtime in the background.
pub(crate) struct AsyncDroppableRuntime {
    runtime: Option<tokio::runtime::Runtime>,
}

impl AsyncDroppableRuntime {
    pub fn new(runtime: tokio::runtime::Runtime) -> Self {
        Self { runtime: Some(runtime) }
    }
}

impl Deref for AsyncDroppableRuntime {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        self.runtime.as_ref().unwrap()
    }
}

impl Drop for AsyncDroppableRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
