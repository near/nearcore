use near_o11y::metrics::{try_create_int_counter_vec, IntCounterVec};
use near_parameters::vm::VMKind;
use once_cell::sync::Lazy;
use std::cell::RefCell;

thread_local! {
    static METRICS: RefCell<Metrics> = const { RefCell::new(Metrics { near_vm_compilations: 0 }) };
}

pub static COMPILATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_vm_runner_compilations_total",
        "How many compilations have occurred so far?",
        &["vm_kind", "shard_id"],
    )
    .unwrap()
});

#[derive(Default, Copy, Clone)]
pub struct Metrics {
    near_vm_compilations: u64,
}

impl Metrics {
    pub fn reset() {
        METRICS.with_borrow_mut(|m| *m = Self::default());
    }

    /// Get the current metrics.
    ///
    /// Note that this is a thread-local operation.
    pub fn get() -> Metrics {
        METRICS.with_borrow(|m| *m)
    }

    pub fn report(&mut self, shard_id: &str) {
        COMPILATIONS.with_label_values(&["near_vm", shard_id]).inc_by(self.near_vm_compilations);
        self.near_vm_compilations = 0;
    }
}

pub(crate) fn compilation_event(kind: VMKind) {
    METRICS.with_borrow_mut(|m| match kind {
        VMKind::Wasmer0 => {}
        VMKind::Wasmtime => {}
        VMKind::Wasmer2 => {}
        VMKind::NearVm => m.near_vm_compilations += 1,
    });
}
