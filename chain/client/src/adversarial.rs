use near_chain_configs::AdversarialMode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct Inner {
    adversarial_mode: AdversarialMode,
    disable_header_sync: AtomicBool,
    disable_doomslug: AtomicBool,
    is_archival: bool,
}

#[derive(Clone)]
pub struct Controls(Arc<Inner>);

impl Controls {
    pub fn new(is_archival: bool, adversarial_mode: AdversarialMode) -> Self {
        Self(Arc::new(Inner {
            adversarial_mode,
            disable_header_sync: AtomicBool::new(false),
            disable_doomslug: AtomicBool::new(false),
            is_archival,
        }))
    }

    pub fn adversarial_mode(&self) -> AdversarialMode {
        self.0.adversarial_mode
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
