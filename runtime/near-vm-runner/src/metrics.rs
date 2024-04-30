use near_o11y::metrics::{try_create_histogram_vec, HistogramVec};
use once_cell::sync::Lazy;
use std::{cell::RefCell, time::Duration};

thread_local! {
    static METRICS: RefCell<Metrics> = const { RefCell::new(Metrics {
        near_vm_compilation_time: Duration::new(0, 0),
        wasmtime_compilation_time: Duration::new(0, 0),
    }) };
}

pub static COMPILATION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_vm_runner_compilation_seconds",
        "Histogram of how long it takes to compile things",
        &["vm_kind", "shard_id"],
        None,
    )
    .unwrap()
});

#[derive(Default, Copy, Clone)]
pub struct Metrics {
    near_vm_compilation_time: Duration,
    wasmtime_compilation_time: Duration,
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
        if !self.near_vm_compilation_time.is_zero() {
            COMPILATION_TIME
                .with_label_values(&["near_vm", shard_id])
                .observe(self.near_vm_compilation_time.as_secs_f64());
            self.near_vm_compilation_time = Duration::default();
        }
        if !self.wasmtime_compilation_time.is_zero() {
            COMPILATION_TIME
                .with_label_values(&["wasmtime", shard_id])
                .observe(self.wasmtime_compilation_time.as_secs_f64());
            self.wasmtime_compilation_time = Duration::default();
        }
    }
}

#[cfg(any(feature = "near_vm", feature = "wasmtime_vm"))]
pub(crate) fn compilation_duration(kind: near_parameters::vm::VMKind, duration: Duration) {
    use near_parameters::vm::VMKind;
    METRICS.with_borrow_mut(|m| match kind {
        VMKind::Wasmer0 => {}
        VMKind::Wasmtime => m.wasmtime_compilation_time += duration,
        VMKind::Wasmer2 => {}
        VMKind::NearVm => m.near_vm_compilation_time += duration,
    });
}
