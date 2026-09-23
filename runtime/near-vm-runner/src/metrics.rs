use near_o11y::metrics::{
    HistogramVec, IntCounterVec, IntGaugeVec, try_create_histogram_vec, try_create_int_counter_vec,
    try_create_int_gauge_vec,
};
use std::sync::LazyLock;
use std::{cell::RefCell, time::Duration};

thread_local! {
    static METRICS: RefCell<Metrics> = const { RefCell::new(Metrics {
        compilation_time: Duration::new(0, 0),
        execution_time: Duration::new(0, 0),
        compiled_contract_cache_lookups: 0,
        compiled_contract_cache_hits: 0,
        compiled_contract_memory_cache_hits: 0,
    }) };
}

static COMPILATION_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_vm_runner_compilation_seconds",
        "Histogram of how long it takes to compile things",
        &["shard_id"],
        Some(vec![0.025, 0.05, 0.1, 0.5]),
    )
    .unwrap()
});

static EXECUTION_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_vm_runner_execution_seconds",
        "Histogram of how long it takes to execute a contract call",
        &["shard_id"],
        Some(vec![0.025, 0.05, 0.1, 0.5]),
    )
    .unwrap()
});

static COMPILED_CONTRACT_CACHE_LOOKUPS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_vm_compiled_contract_cache_lookups_total",
        "The number of times the runtime looks up for an entry in the compiled-contract cache for the given caller context and shard_id",
        &["context", "shard_id"],
    )
    .unwrap()
});

static COMPILED_CONTRACT_CACHE_ITEMS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec("near_any_cache_items", "Number of AnyCache items", &["cache_id"])
        .unwrap()
});

static COMPILED_CONTRACT_CACHE_WEIGHT_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_any_cache_weight_bytes",
        "Total weight in bytes of AnyCache items",
        &["cache_id"],
    )
    .unwrap()
});

static COMPILED_CONTRACT_CACHE_HITS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_vm_compiled_contract_cache_hits_total",
        "The number of times the runtime finds an entry in the compiled-contract cache for the given caller context and shard_id",
        &["context", "shard_id"],
    )
    .unwrap()
});

static COMPILED_CONTRACT_MEMORY_CACHE_HITS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_vm_compiled_contract_memory_cache_hits_total",
        "The number of times the runtime finds an entry in the in-memory contracts cache.",
        &["context", "shard_id"],
    )
    .unwrap()
});

#[derive(Default, Copy, Clone)]
struct Metrics {
    compilation_time: Duration,
    execution_time: Duration,
    /// Number of lookups from the compiled contract cache.
    compiled_contract_cache_lookups: u64,
    /// Number of times the lookup from the compiled contract cache finds a match.
    compiled_contract_cache_hits: u64,
    /// Number of times the in-memory cache had the contract (compiled + execution context).
    compiled_contract_memory_cache_hits: u64,
}

#[cfg(feature = "wasmtime_vm")]
pub(crate) fn compilation_duration(duration: Duration) {
    METRICS.with_borrow_mut(|m| m.compilation_time += duration);
}

pub(crate) fn record_execution_duration(duration: Duration) {
    METRICS.with_borrow_mut(|m| m.execution_time += duration);
}

/// Records the result of a compiled-contract cache lookup.
#[cfg(feature = "wasmtime_vm")]
pub(crate) fn record_compiled_contract_cache_lookup(is_hit: bool, is_memory_hit: bool) {
    METRICS.with_borrow_mut(|m| {
        m.compiled_contract_cache_lookups += 1;
        if is_hit {
            m.compiled_contract_cache_hits += 1;
        }
        if is_memory_hit {
            m.compiled_contract_memory_cache_hits += 1;
        }
    });
}

pub fn reset_metrics() {
    METRICS.with_borrow_mut(|m| *m = Metrics::default());
}

pub(crate) fn set_compiled_contract_cache_metrics(cache_id: &str, items: usize, weight: u64) {
    COMPILED_CONTRACT_CACHE_ITEMS.with_label_values(&[cache_id]).set(items as i64);
    COMPILED_CONTRACT_CACHE_WEIGHT_BYTES.with_label_values(&[cache_id]).set(weight as i64);
}

/// Reports the current metrics at the end of a single VM invocation (eg. to run a function call).
pub fn report_metrics(shard_id: impl std::fmt::Display, caller_context: &str) {
    METRICS.with_borrow_mut(|m| {
        let has_data = !m.compilation_time.is_zero()
            || !m.execution_time.is_zero()
            || m.compiled_contract_cache_lookups > 0;
        if !has_data {
            *m = Metrics::default();
            return;
        }
        let shard_id = shard_id.to_string();
        if !m.compilation_time.is_zero() {
            COMPILATION_TIME
                .with_label_values(&[&shard_id])
                .observe(m.compilation_time.as_secs_f64());
        }
        if !m.execution_time.is_zero() {
            EXECUTION_TIME.with_label_values(&[&shard_id]).observe(m.execution_time.as_secs_f64());
        }
        if m.compiled_contract_cache_lookups > 0 {
            COMPILED_CONTRACT_CACHE_LOOKUPS_TOTAL
                .with_label_values(&[caller_context, &shard_id])
                .inc_by(m.compiled_contract_cache_lookups);
        }
        if m.compiled_contract_cache_hits > 0 {
            COMPILED_CONTRACT_CACHE_HITS_TOTAL
                .with_label_values(&[caller_context, &shard_id])
                .inc_by(m.compiled_contract_cache_hits);
        }
        if m.compiled_contract_memory_cache_hits > 0 {
            COMPILED_CONTRACT_MEMORY_CACHE_HITS_TOTAL
                .with_label_values(&[caller_context, &shard_id])
                .inc_by(m.compiled_contract_memory_cache_hits);
        }

        *m = Metrics::default();
    });
}
