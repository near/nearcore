use near_o11y::metrics::{
    try_create_histogram_vec, try_create_int_counter_vec, HistogramVec, IntCounterVec,
};
use once_cell::sync::Lazy;
use std::{cell::RefCell, time::Duration};

thread_local! {
    static METRICS: RefCell<Metrics> = const { RefCell::new(Metrics {
        near_vm_compilation_time: Duration::new(0, 0),
        wasmtime_compilation_time: Duration::new(0, 0),
        compiled_contract_cache_lookups: 0,
        compiled_contract_cache_hits: 0,
    }) };
}

static COMPILATION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_vm_runner_compilation_seconds",
        "Histogram of how long it takes to compile things",
        &["vm_kind", "shard_id"],
        None,
    )
    .unwrap()
});

static COMPILED_CONTRACT_CACHE_LOOKUPS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_vm_compiled_contract_cache_lookups_total",
        "The number of times the runtime looks up for an entry in the compiled-contract cache for the given caller context and shard_id",
        &["context", "shard_id"],
    )
    .unwrap()
});

static COMPILED_CONTRACT_CACHE_HITS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_vm_compiled_contract_cache_hits_total",
        "The number of times the runtime finds an entry in the compiled-contract cache for the given caller context and shard_id",
        &["context", "shard_id"],
    )
    .unwrap()
});

#[derive(Default, Copy, Clone)]
struct Metrics {
    near_vm_compilation_time: Duration,
    wasmtime_compilation_time: Duration,
    /// Number of lookups from the compiled contract cache.
    compiled_contract_cache_lookups: u64,
    /// Number of times the lookup from the compiled contract cache finds a match.
    compiled_contract_cache_hits: u64,
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

/// Updates metrics to record a compiled-contract cache lookup,
/// where is_hit=true indicates that we found an entry in the cache.
pub(crate) fn record_compiled_contract_cache_lookup(is_hit: bool) {
    METRICS.with_borrow_mut(|m| {
        m.compiled_contract_cache_lookups += 1;
        if is_hit {
            m.compiled_contract_cache_hits += 1;
        }
    });
}

pub fn reset_metrics() {
    METRICS.with_borrow_mut(|m| *m = Metrics::default());
}

/// Reports the current metrics at the end of a single VM invocation (eg. to run a function call).
pub fn report_metrics(shard_id: &str, caller_context: &str) {
    METRICS.with_borrow_mut(|m| {
        if !m.near_vm_compilation_time.is_zero() {
            COMPILATION_TIME
                .with_label_values(&["near_vm", shard_id])
                .observe(m.near_vm_compilation_time.as_secs_f64());
        }
        if !m.wasmtime_compilation_time.is_zero() {
            COMPILATION_TIME
                .with_label_values(&["wasmtime", shard_id])
                .observe(m.wasmtime_compilation_time.as_secs_f64());
        }
        if m.compiled_contract_cache_lookups > 0 {
            COMPILED_CONTRACT_CACHE_LOOKUPS_TOTAL
                .with_label_values(&[caller_context, shard_id])
                .inc_by(m.compiled_contract_cache_lookups);
        }
        if m.compiled_contract_cache_hits > 0 {
            COMPILED_CONTRACT_CACHE_HITS_TOTAL
                .with_label_values(&[caller_context, shard_id])
                .inc_by(m.compiled_contract_cache_lookups);
        }

        *m = Metrics::default();
    });
}
