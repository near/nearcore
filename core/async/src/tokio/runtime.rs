use std::ops::Deref;

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
