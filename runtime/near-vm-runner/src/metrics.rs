use near_o11y::metrics::{
    try_create_histogram_vec, try_create_int_counter_vec, HistogramVec, IntCounterVec,
};
use once_cell::sync::Lazy;
use std::{cell::RefCell, time::Duration};

thread_local! {
    static METRICS: RefCell<Metrics> = const { RefCell::new(Metrics {
        near_vm_compilation_time: Duration::new(0, 0),
        wasmtime_compilation_time: Duration::new(0, 0),
        compiled_contract_cache_hit: None,
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

pub static COMPILED_CONTRACT_CACHE_HIT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_vm_compiled_contract_cache_hits_total",
        "The number of times the runtime finds compiled code in cache for the given caller context and shard_id",
        &["context", "shard_id"],
    )
    .unwrap()
});

pub static COMPILED_CONTRACT_CACHE_MISS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_vm_compiled_contract_cache_misses_total",
        "The number of times the runtime cannot find compiled code in cache for the given caller context and shard_id",
        &["context", "shard_id"],
    )
    .unwrap()
});

#[derive(Default, Copy, Clone)]
pub struct Metrics {
    near_vm_compilation_time: Duration,
    wasmtime_compilation_time: Duration,
    /// True iff the runtime checked the compiled contract cache and found already-compiled code.
    compiled_contract_cache_hit: Option<bool>,
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

    /// Report the current metrics at the end of a single VM invocation (eg. to run a function call).
    pub fn report(&mut self, shard_id: &str, caller_context: &str) {
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
        match self.compiled_contract_cache_hit.take() {
            Some(true) => {
                COMPILED_CONTRACT_CACHE_HIT.with_label_values(&[caller_context, shard_id]).inc();
            }
            Some(false) => {
                COMPILED_CONTRACT_CACHE_MISS.with_label_values(&[caller_context, shard_id]).inc();
            }
            None => {}
        };
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

/// Updates metrics to record that the runtime has found an entry in compiled contract cache.
pub(crate) fn record_compiled_contract_cache_hit() {
    METRICS.with_borrow_mut(|m| {
        debug_assert!(
            m.compiled_contract_cache_hit.is_none(),
            "Compiled context cache hit/miss should be reported once."
        );
        m.compiled_contract_cache_hit = Some(true);
    });
}

/// Updates metrics to record that the runtime could not find an entry in compiled contract cache.
pub(crate) fn record_compiled_contract_cache_miss() {
    METRICS.with_borrow_mut(|m| {
        debug_assert!(
            m.compiled_contract_cache_hit.is_none(),
            "Compiled context cache hit/miss should be reported once."
        );
        m.compiled_contract_cache_hit = Some(false);
    });
}
