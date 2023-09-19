use core::fmt;
use std::collections::{BTreeMap, HashMap};
use crate::DBCol;
use std::time::Duration;

pub struct PerfContext {
    pub column_measurements: HashMap<DBCol, ColumnMeasurement>,
    pub measured_columns: Vec<DBCol>,
}

impl PerfContext {
    pub fn new(measured_columns: &[DBCol]) -> Self {
        Self { column_measurements: HashMap::new(), measured_columns: measured_columns.to_vec() }
    }

    // We call record for every read since rockdb_perf_context is local for
    // every thread and it is not possible to aggregate data at a given
    // point since threads are handled by rocksdb internally
    pub fn record(&mut self, col: DBCol, obs_latency: Duration) {
        let mut rocksdb_ctx = rocksdb::perf::PerfContext::default();
        rocksdb::perf::set_perf_stats(rocksdb::perf::PerfStatsLevel::EnableTime);
        let col_measurement = self.column_measurements.entry(col).or_default();

        // Add cache measurements
        col_measurement
            .block_cache
            .add_hits(rocksdb_ctx.metric(rocksdb::PerfMetric::BlockCacheHitCount));
        col_measurement
            .bloom_mem
            .add_hits(rocksdb_ctx.metric(rocksdb::PerfMetric::BloomMemtableHitCount));
        col_measurement
            .bloom_mem
            .add_miss(rocksdb_ctx.metric(rocksdb::PerfMetric::BloomMemtableMissCount));

        col_measurement
            .bloom_sst
            .add_hits(rocksdb_ctx.metric(rocksdb::PerfMetric::BloomSstHitCount));
        col_measurement
            .bloom_sst
            .add_hits(rocksdb_ctx.metric(rocksdb::PerfMetric::BloomSstMissCount));

        // Add block latencies measurements
        let block_read_cnt = rocksdb_ctx.metric(rocksdb::PerfMetric::BlockReadCount) as usize;
        let read_block_latency =
            Duration::from_nanos(rocksdb_ctx.metric(rocksdb::PerfMetric::BlockReadTime));
        let has_merge = rocksdb_ctx.metric(rocksdb::PerfMetric::MergeOperatorTimeNanos) > 0;

        col_measurement
            .measurements_per_block_reads
            .entry(block_read_cnt)
            .or_default()
            .add(read_block_latency, has_merge);
        col_measurement.measurements_overall.add(obs_latency, has_merge);

        rocksdb_ctx.reset();
    }

    pub fn reset(&mut self) {
        let mut rocksdb_ctx = rocksdb::perf::PerfContext::default();
        rocksdb_ctx.reset();
        self.column_measurements.clear();
    }
}

#[derive(Debug, Default)]
pub struct ColumnMeasurement {
    pub measurements_per_block_reads: BTreeMap<usize, Measurements>,
    pub measurements_overall: Measurements,
    pub block_cache: CacheUsage,
    pub bloom_mem: CacheUsage,
    pub bloom_sst: CacheUsage,
}

#[derive(Debug, Default)]
pub struct CacheUsage {
    pub hits: u64,
    pub miss: u64,
    pub count: u64,
}

impl CacheUsage {
    pub fn add_hits(&mut self, hits: u64) {
        self.hits += hits;
        self.count += hits;
    }

    pub fn add_miss(&mut self, miss: u64) {
        self.miss += miss;
        self.count += miss;
    }
}

#[derive(Default)]
pub struct Measurements {
    pub samples: usize,
    pub total_read_block_latency: Duration,
    pub samples_with_merge: usize,
    zeros: usize,
}

impl Measurements {
    pub fn add(&mut self, read_block_latency: Duration, has_merge: bool) {
        self.samples += 1;
        self.total_read_block_latency += read_block_latency;
        if self.total_read_block_latency.is_zero() {
            self.zeros += 1;
        }
        if has_merge {
            self.samples_with_merge += 1;
        }
    }

    pub fn avg_read_block_latency(&self) -> Duration {
        self.total_read_block_latency / (self.samples as u32)
    }
}

impl fmt::Debug for Measurements {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Measurements: samples: {}, total: {:?} zeros: {} avg: {:?}",
            self.samples,
            self.total_read_block_latency,
            self.zeros,
            self.avg_read_block_latency()
        )
    }
}

