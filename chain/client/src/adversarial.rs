use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct Inner {
    disable_header_sync: AtomicBool,
    disable_doomslug: AtomicBool,
    is_archival: bool,
}

#[derive(Default, Clone)]
pub struct Controls(Arc<Inner>);

impl Controls {
    pub fn new(is_archival: bool) -> Self {
        Self(Arc::new(Inner { is_archival, ..Inner::default() }))
    }

    pub fn disable_header_sync(&self) -> bool {
        self.0.disable_header_sync.load(Ordering::SeqCst)
    }

    pub fn set_disable_header_sync(&mut self, value: bool) {
        self.0.disable_header_sync.store(value, Ordering::SeqCst);
    }

    pub fn disable_doomslug(&self) -> bool {
        self.0.disable_doomslug.load(Ordering::SeqCst)
    }

    pub fn set_disable_doomslug(&self, value: bool) {
        self.0.disable_doomslug.store(value, Ordering::SeqCst);
    }

    pub fn is_archival(&self) -> bool {
        self.0.is_archival
    }
}
