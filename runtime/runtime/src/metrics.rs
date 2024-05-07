use near_o11y::metrics::{
    exponential_buckets, linear_buckets, try_create_counter_vec, try_create_histogram_vec,
    try_create_histogram_with_buckets, try_create_int_counter, try_create_int_counter_vec,
    CounterVec, Histogram, HistogramVec, IntCounter, IntCounterVec,
};
use once_cell::sync::Lazy;
use std::time::Duration;

pub static ACTION_CALLED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_action_called_count",
        "Number of times given action has been called since starting this node",
        &["action"],
    )
    .unwrap()
});

pub static TRANSACTION_PROCESSED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_total",
        "The number of transactions processed since starting this node",
    )
    .unwrap()
});

pub static INCOMING_RECEIPT_PROCESSED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_incoming_receipt_processed_total",
        "The number of incoming receipts processed since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static INCOMING_RECEIPT_PROCESSING_SECONDS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    try_create_counter_vec(
        "near_incoming_receipt_processing_seconds_total",
        "The time spent on processing incoming receipts since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static DELAYED_RECEIPT_PROCESSED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_delayed_receipt_processed_total",
        "The number of delayed receipts processed since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static DELAYED_RECEIPT_PROCESSING_SECONDS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    try_create_counter_vec(
        "near_delayed_receipt_processing_seconds_total",
        "The time spent on processing delayed receipts since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static LOCAL_RECEIPT_PROCESSED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_local_receipt_processed_total",
        "The number of local receipts processed since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static LOCAL_RECEIPT_PROCESSING_SECONDS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    try_create_counter_vec(
        "near_local_receipt_processing_seconds_total",
        "The time spent on processing local receipts since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static YIELD_TIMEOUTS_PROCESSED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_yield_timeouts_processed_total",
        "The number of yield timeouts processed since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static YIELD_TIMEOUTS_PROCESSING_SECONDS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    try_create_counter_vec(
        "near_yield_timeouts_processing_seconds_total",
        "The time spent on processing yield timeouts since starting this node",
        &["shard_id"],
    )
    .unwrap()
});

pub static TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_successfully_total",
        "The number of transactions processed successfully since starting this node",
    )
    .unwrap()
});

pub static TRANSACTION_PROCESSED_FAILED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_failed_total",
        "The number of transactions processed and failed since starting this node",
    )
    .unwrap()
});
pub static PREFETCH_ENQUEUED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_enqueued",
        "Prefetch requests queued up",
        &["shard_id"],
    )
    .unwrap()
});
pub static PREFETCH_QUEUE_FULL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_prefetch_queue_full",
        "Prefetch requests failed to queue up",
        &["shard_id"],
    )
    .unwrap()
});
pub static FUNCTION_CALL_PROCESSED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_function_call_processed",
        "The number of function calls processed since starting this node",
        &["result"],
    )
    .unwrap()
});
pub static FUNCTION_CALL_PROCESSED_FUNCTION_CALL_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_function_call_processed_function_call_errors",
        "The number of function calls resulting in function call errors, since starting this node",
        &["error_type"],
    )
    .unwrap()
});
pub static FUNCTION_CALL_PROCESSED_COMPILATION_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_function_call_processed_compilation_errors",
        "The number of function calls resulting in compilation errors, since starting this node",
        &["error_type"],
    )
    .unwrap()
});
pub static FUNCTION_CALL_PROCESSED_METHOD_RESOLVE_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_function_call_processed_method_resolve_errors",
        "The number of function calls resulting in method resolve errors, since starting this node",
        &["error_type"],
    )
    .unwrap()
});
pub static FUNCTION_CALL_PROCESSED_WASM_TRAP_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_function_call_processed_wasm_trap_errors",
        "The number of function calls resulting in wasm trap errors, since starting this node",
        &["error_type"],
    )
    .unwrap()
});
pub static FUNCTION_CALL_PROCESSED_HOST_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_function_call_processed_host_errors",
        "The number of function calls resulting in host errors, since starting this node",
        &["error_type"],
    )
    .unwrap()
});
pub static FUNCTION_CALL_PROCESSED_CACHE_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_function_call_processed_cache_errors",
        "The number of function calls resulting in VM cache errors, since starting this node",
        &["error_type"],
    )
    .unwrap()
});
static CHUNK_COMPUTE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_compute",
        "Compute time by chunk, as a histogram in ms. Reported for all applied chunks, even when not included in a block.",
        &["shard_id"],
        buckets_for_compute(),
    )
    .unwrap()
});
static CHUNK_TGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_tgas",
        "Tgas burnt by chunk, as a histogram in ms. Reported for all applied chunks, even when not included in a block.",
        &["shard_id"],
        buckets_for_gas(),
    )
    .unwrap()
});
static CHUNK_LOCAL_RECEIPTS_COMPUTE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_local_receipt_compute",
        "Compute time for applying local receipts by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_compute(),
    )
    .unwrap()
});
static CHUNK_LOCAL_RECEIPTS_TGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_local_receipt_tgas",
        "Tgas burnt for applying local receipts by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_gas(),
    )
    .unwrap()
});
static CHUNK_DELAYED_RECEIPTS_COMPUTE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_delayed_receipt_compute",
        "Compute time for applying delayed receipts by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_compute(),
    )
    .unwrap()
});
static CHUNK_DELAYED_RECEIPTS_TGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_delayed_receipt_tgas",
        "Tgas burnt for applying delayed receipts by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_gas(),
    )
    .unwrap()
});
static CHUNK_INC_RECEIPTS_COMPUTE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_inc_receipt_compute",
        "Compute time for applying incoming receipts by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_compute(),
    )
    .unwrap()
});
static CHUNK_INC_RECEIPTS_TGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_inc_receipt_tgas",
        "Tgas burnt for applying incoming receipts by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_gas(),
    )
    .unwrap()
});
static CHUNK_YIELD_TIMEOUTS_COMPUTE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_yield_timeouts_compute",
        "Compute time for triggering timed-out yields by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_compute(),
    )
    .unwrap()
});
static CHUNK_YIELD_TIMEOUTS_TGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_yield_timeouts_tgas",
        "Tgas burnt for triggering timed-out yields by chunk, as a histogram in ms",
        &["shard_id"],
        buckets_for_gas(),
    )
    .unwrap()
});
static CHUNK_TX_COMPUTE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_tx_compute",
        "Compute time for transaction validation by chunk, as a histogram in ms",
        &["shard_id"],
        Some(vec![0., 50., 100., 200., 300., 400., 500., 600.0]),
    )
    .unwrap()
});
static CHUNK_TX_TGAS: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_tx_tgas",
        "Tgas burnt for transaction validation by chunk, as a histogram",
        &["shard_id"],
        Some(vec![0., 50., 100., 200., 300., 400., 500.]),
    )
    .unwrap()
});
pub static RECEIPT_RECORDED_SIZE: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_receipt_recorded_size",
        "Size of storage proof recorded when executing a receipt",
        buckets_for_receipt_storage_proof_size(),
    )
    .unwrap()
});
pub static RECEIPT_RECORDED_SIZE_UPPER_BOUND: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_receipt_recorded_size_upper_bound",
        "Upper bound estimation (e.g with extra size added for deletes) of storage proof size recorded when executing a receipt",
        buckets_for_receipt_storage_proof_size(),
    )
    .unwrap()
});
pub static RECEIPT_RECORDED_SIZE_UPPER_BOUND_RATIO: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_receipt_recorded_size_upper_bound_ratio",
        "Ratio of upper bound to true recorded size, calculated only for sizes larger than 100KB, equal to (near_receipt_recorded_size_upper_bound / near_receipt_recorded_size)",
        buckets_for_storage_proof_size_ratio(),
    )
    .unwrap()
});
pub static CHUNK_RECORDED_SIZE: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_chunk_recorded_size",
        "Total size of storage proof (recorded trie nodes for state witness, post-finalization) for a single chunk",
        buckets_for_chunk_storage_proof_size(),
    )
    .unwrap()
});
pub static CHUNK_RECORDED_SIZE_UPPER_BOUND: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_chunk_recorded_size_upper_bound",
        "Upper bound of storage proof size (recorded trie nodes size + estimated charges, pre-finalization) for a single chunk",
        buckets_for_chunk_storage_proof_size(),
    )
    .unwrap()
});
pub static CHUNK_RECORDED_SIZE_UPPER_BOUND_RATIO: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram_with_buckets(
        "near_chunk_recorded_size_upper_bound_ratio",
        "Ratio of upper bound to true storage proof size, equal to (near_chunk_recorded_size_upper_bound / near_chunk_recorded_size)",
        buckets_for_storage_proof_size_ratio(),
    )
    .unwrap()
});

/// Buckets used for burned gas in receipts.
///
/// The maximum possible is 1300 Tgas for a full chunk.
/// But due to the split between types of receipts, it should be quite rare to
/// see more than 1000.
fn buckets_for_gas() -> Option<Vec<f64>> {
    Some(vec![0., 50., 100., 200., 300., 400., 500., 600., 700., 800., 900., 1000., 1100., 1200.])
}
/// Buckets used for receipt compute time usage, in ms.
///
/// Ideally the range should be 0-1300 ms. But when we increase the compute cost
/// compared to gas cost, it can be higher than that.
///
/// Nevertheless, it should be on the rare side to have > 1300 ms and hence it
/// is not worth collecting at a high granularity above that. Here we pick one
/// bucket split at 2000 ms to easily single out heavy undercharging.
fn buckets_for_compute() -> Option<Vec<f64>> {
    Some(vec![
        0., 50., 100., 200., 300., 400., 500., 600., 700., 800., 900., 1000., 1100., 1200., 1300.,
        2000.,
    ])
}

/// Buckets from 0 to 10MB
fn buckets_for_receipt_storage_proof_size() -> Vec<f64> {
    // Precise buckets for the smaller, common values
    let mut buckets = vec![50_000., 100_000., 200_000., 300_000.];

    // Coarse buckets for the larger values
    buckets.extend(linear_buckets(500_000., 500_000., 20).unwrap());
    buckets
}

/// Buckets from 0 to 15.2MB
fn buckets_for_chunk_storage_proof_size() -> Vec<f64> {
    linear_buckets(0., 800_000., 20).unwrap()
}

/// Buckets from 1 to 12.84
fn buckets_for_storage_proof_size_ratio() -> Vec<f64> {
    // 1.2 ** 14 = 12.84
    exponential_buckets(1., 1.2, 15).unwrap()
}

/// Helper struct to collect partial costs of `Runtime::apply` and reporting it
/// atomically.
#[derive(Debug, Default)]
pub struct ApplyMetrics {
    accumulated_gas: u64,
    accumulated_compute: u64,
    tx_compute_usage: u64,
    tx_gas: u64,
    local_receipts_compute_usage: u64,
    local_receipts_gas: u64,
    local_receipts_processed_total: u64,
    local_receipts_processing_seconds_total: f64,
    delayed_receipts_compute_usage: u64,
    delayed_receipts_gas: u64,
    delayed_receipts_processed_total: u64,
    delayed_receipts_processing_seconds_total: f64,
    incoming_receipts_compute_usage: u64,
    incoming_receipts_gas: u64,
    incoming_receipts_processed_total: u64,
    incoming_receipts_processing_seconds_total: f64,
    yield_timeouts_compute_usage: u64,
    yield_timeouts_gas: u64,
    yield_timeouts_processed_total: u64,
    yield_timeouts_processing_seconds_total: f64,
}

impl ApplyMetrics {
    /// Updates the internal accumulated counters and returns the difference to
    /// the old counters.
    fn update_accumulated(&mut self, gas: u64, compute: u64) -> (u64, u64) {
        // Use saturating sub, wrong metrics are better than an overflow panic.
        let delta = (
            gas.saturating_sub(self.accumulated_gas),
            compute.saturating_sub(self.accumulated_compute),
        );
        self.accumulated_gas = gas;
        self.accumulated_compute = compute;
        delta
    }

    pub fn tx_processing_done(&mut self, accumulated_gas: u64, accumulated_compute: u64) {
        (self.tx_gas, self.tx_compute_usage) =
            self.update_accumulated(accumulated_gas, accumulated_compute);
    }

    pub fn local_receipts_done(
        &mut self,
        count: u64,
        time: Duration,
        accumulated_gas: u64,
        accumulated_compute: u64,
    ) {
        (self.local_receipts_gas, self.local_receipts_compute_usage) =
            self.update_accumulated(accumulated_gas, accumulated_compute);
        self.local_receipts_processed_total += count;
        self.local_receipts_processing_seconds_total += time.as_secs_f64();
    }

    pub fn delayed_receipts_done(
        &mut self,
        count: u64,
        time: Duration,
        accumulated_gas: u64,
        accumulated_compute: u64,
    ) {
        (self.delayed_receipts_gas, self.delayed_receipts_compute_usage) =
            self.update_accumulated(accumulated_gas, accumulated_compute);
        self.delayed_receipts_processed_total += count;
        self.delayed_receipts_processing_seconds_total += time.as_secs_f64();
    }

    pub fn incoming_receipts_done(
        &mut self,
        count: u64,
        time: Duration,
        accumulated_gas: u64,
        accumulated_compute: u64,
    ) {
        (self.incoming_receipts_gas, self.incoming_receipts_compute_usage) =
            self.update_accumulated(accumulated_gas, accumulated_compute);
        self.incoming_receipts_processed_total += count;
        self.incoming_receipts_processing_seconds_total += time.as_secs_f64();
    }

    pub fn yield_timeouts_done(
        &mut self,

        count: u64,
        time: Duration,
        accumulated_gas: u64,
        accumulated_compute: u64,
    ) {
        (self.yield_timeouts_gas, self.yield_timeouts_compute_usage) =
            self.update_accumulated(accumulated_gas, accumulated_compute);
        self.yield_timeouts_processed_total += count;
        self.yield_timeouts_processing_seconds_total += time.as_secs_f64();
    }

    /// Report statistics
    pub fn report(&mut self, shard_id: &str) {
        const TERA: f64 = 1_000_000_000_000_f64;

        LOCAL_RECEIPT_PROCESSED_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.local_receipts_processed_total);
        self.local_receipts_processed_total = 0;
        DELAYED_RECEIPT_PROCESSED_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.delayed_receipts_processed_total);
        self.delayed_receipts_processed_total = 0;
        INCOMING_RECEIPT_PROCESSED_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.incoming_receipts_processed_total);
        self.incoming_receipts_processed_total = 0;
        YIELD_TIMEOUTS_PROCESSED_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.yield_timeouts_processed_total);
        self.yield_timeouts_processed_total = 0;

        LOCAL_RECEIPT_PROCESSING_SECONDS_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.local_receipts_processing_seconds_total);
        self.local_receipts_processing_seconds_total = 0.0;
        DELAYED_RECEIPT_PROCESSING_SECONDS_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.delayed_receipts_processing_seconds_total);
        self.delayed_receipts_processing_seconds_total = 0.0;
        INCOMING_RECEIPT_PROCESSING_SECONDS_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.incoming_receipts_processing_seconds_total);
        self.incoming_receipts_processing_seconds_total = 0.0;
        YIELD_TIMEOUTS_PROCESSING_SECONDS_TOTAL
            .with_label_values(&[shard_id])
            .inc_by(self.yield_timeouts_processing_seconds_total);
        self.yield_timeouts_processing_seconds_total = 0.0;

        CHUNK_TX_TGAS.with_label_values(&[shard_id]).observe(self.tx_gas as f64 / TERA);
        CHUNK_TX_COMPUTE
            .with_label_values(&[shard_id])
            .observe(self.tx_compute_usage as f64 / TERA);

        CHUNK_LOCAL_RECEIPTS_TGAS
            .with_label_values(&[shard_id])
            .observe(self.local_receipts_gas as f64 / TERA);
        CHUNK_LOCAL_RECEIPTS_COMPUTE
            .with_label_values(&[shard_id])
            .observe(self.local_receipts_compute_usage as f64 / TERA);

        CHUNK_DELAYED_RECEIPTS_TGAS
            .with_label_values(&[shard_id])
            .observe(self.delayed_receipts_gas as f64 / TERA);
        CHUNK_DELAYED_RECEIPTS_COMPUTE
            .with_label_values(&[shard_id])
            .observe(self.delayed_receipts_compute_usage as f64 / TERA);

        CHUNK_INC_RECEIPTS_TGAS
            .with_label_values(&[shard_id])
            .observe(self.incoming_receipts_gas as f64 / TERA);
        CHUNK_INC_RECEIPTS_COMPUTE
            .with_label_values(&[shard_id])
            .observe(self.incoming_receipts_compute_usage as f64 / TERA);

        CHUNK_YIELD_TIMEOUTS_TGAS
            .with_label_values(&[shard_id])
            .observe(self.yield_timeouts_gas as f64 / TERA);
        CHUNK_YIELD_TIMEOUTS_COMPUTE
            .with_label_values(&[shard_id])
            .observe(self.yield_timeouts_compute_usage as f64 / TERA);

        CHUNK_TGAS.with_label_values(&[shard_id]).observe(self.accumulated_gas as f64 / TERA);
        CHUNK_COMPUTE
            .with_label_values(&[shard_id])
            .observe(self.accumulated_compute as f64 / TERA);
    }
}
