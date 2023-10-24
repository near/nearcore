use near_o11y::metrics::{
    exponential_buckets, try_create_counter, try_create_gauge, try_create_histogram,
    try_create_histogram_vec, try_create_int_counter, try_create_int_counter_vec,
    try_create_int_gauge, try_create_int_gauge_vec, Counter, Gauge, Histogram, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};
use once_cell::sync::Lazy;

pub(crate) static BLOCK_PRODUCED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_block_produced_total",
        "Total number of blocks produced since starting this node",
    )
    .unwrap()
});

pub(crate) static CHUNK_PRODUCED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_chunk_produced_total",
        "Total number of chunks produced since starting this node",
    )
    .unwrap()
});

pub(crate) static IS_VALIDATOR: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_is_validator",
        "Bool to denote if it is validating in the current epoch",
    )
    .unwrap()
});

pub(crate) static IS_BLOCK_PRODUCER: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_is_block_producer",
        "Bool to denote if the node is a block producer in the current epoch",
    )
    .unwrap()
});

pub(crate) static IS_CHUNK_PRODUCER_FOR_SHARD: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_is_chunk_producer_for_shard",
        "Bool to denote if the node is a chunk producer for a shard in the current epoch",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static RECEIVED_BYTES_PER_SECOND: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_received_bytes_per_second",
        "Number of bytes per second received over the network overall",
    )
    .unwrap()
});

pub(crate) static SENT_BYTES_PER_SECOND: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_sent_bytes_per_second",
        "Number of bytes per second sent over the network overall",
    )
    .unwrap()
});

pub(crate) static CPU_USAGE: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_cpu_usage_ratio", "Percent of CPU usage").unwrap());

pub(crate) static MEMORY_USAGE: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_memory_usage_bytes", "Amount of RAM memory usage").unwrap()
});

pub(crate) static GC_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_gc_time", "Time taken to do garbage collection").unwrap()
});

pub(crate) static TGAS_USAGE_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_tgas_used_hist",
        "Number of Tgas (10^12 of gas) used by processed chunks, as a histogram",
        &["shard"],
        Some(vec![
            50., 100., 300., 500., 700., 800., 900., 950., 1000., 1050., 1100., 1150., 1200.,
            1250., 1300.,
        ]),
    )
    .unwrap()
});

pub(crate) static VALIDATORS_CHUNKS_PRODUCED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_chunks_produced",
        "Number of chunks produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_CHUNKS_EXPECTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_chunks_expected",
        "Number of chunks expected to be produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_CHUNKS_PRODUCED_BY_SHARD: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_chunks_produced_by_shard",
        "Number of chunks produced by a validator",
        &["account_id", "shard_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_CHUNKS_EXPECTED_BY_SHARD: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_chunks_expected_by_shard",
        "Number of chunks expected to be produced by a validator",
        &["account_id", "shard_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_CHUNKS_EXPECTED_IN_EPOCH: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_chunks_expected_in_epoch",
        "Number of chunks expected to be produced by a validator within current epoch",
        &["account_id", "shard_id", "epoch_height"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_BLOCKS_PRODUCED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_blocks_produced",
        "Number of blocks produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_BLOCKS_EXPECTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_blocks_expected",
        "Number of blocks expected to be produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_BLOCKS_EXPECTED_IN_EPOCH: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_validators_blocks_expected_in_epoch",
        "Number of blocks expected to be produced by a validator within current epoch",
        &["account_id", "epoch_height"],
    )
    .unwrap()
});

pub(crate) static BLOCK_PRODUCER_STAKE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_block_producer_stake",
        "Stake of each block producer in the network",
        &["account_id", "epoch_height"],
    )
    .unwrap()
});

pub(crate) static TRACKED_SHARDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_client_tracked_shards", "Tracked shards", &["shard_id"]).unwrap()
});

pub(crate) static SYNC_STATUS: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_sync_status", "Node sync status").unwrap());

pub(crate) static EPOCH_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_epoch_height", "Height of the epoch at the head of the blockchain")
        .unwrap()
});

pub(crate) static FINAL_BLOCK_HEIGHT_IN_EPOCH: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_final_block_height_in_epoch",
        "Height of the last block within the epoch.",
    )
    .unwrap()
});

pub(crate) static PROTOCOL_UPGRADE_BLOCK_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_protocol_upgrade_block_height",
        "Estimated block height of the protocol upgrade",
    )
    .unwrap()
});

pub(crate) static PEERS_WITH_INVALID_HASH: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_peers_with_invalid_hash", "Number of peers that are on invalid hash")
        .unwrap()
});

pub(crate) static CHUNK_SKIPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_chunk_skipped_total",
        "Number of skipped chunks",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static CHUNK_PRODUCER_BANNED_FOR_EPOCH: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_chunk_producer_banned_for_epoch",
        "Number of times we have banned a chunk producer for an epoch",
    )
    .unwrap()
});

pub(crate) static CHUNK_DROPPED_BECAUSE_OF_BANNED_CHUNK_PRODUCER: Lazy<IntCounter> =
    Lazy::new(|| {
        try_create_int_counter(
            "near_chunk_dropped_because_of_banned_chunk_producer",
            "Number of chunks we, as a block producer, 
                dropped, because the chunk is produced by a banned chunk producer",
        )
        .unwrap()
    });

pub(crate) static CLIENT_MESSAGES_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_client_messages_count",
        "Number of messages client actor received by message type",
        &["type"],
    )
    .unwrap()
});

pub(crate) static CLIENT_MESSAGES_PROCESSING_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_client_messages_processing_time",
        "Processing time of messages that client actor received, sorted by message type",
        &["type"],
        Some(exponential_buckets(0.0001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHECK_TRIGGERS_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_client_triggers_time",
        "Processing time of the check_triggers function in client",
    )
    .unwrap()
});

pub(crate) static CLIENT_TRIGGER_TIME_BY_TYPE: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_client_triggers_time_by_type",
        "Time spent on the different triggers in client",
        &["trigger"],
        Some(exponential_buckets(0.0001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static GAS_USED: Lazy<Counter> = Lazy::new(|| {
    try_create_counter("near_gas_used", "Gas used by processed blocks, measured in gas").unwrap()
});

pub(crate) static BLOCKS_PROCESSED: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_blocks_processed", "Number of processed blocks").unwrap()
});

pub(crate) static CHUNKS_PROCESSED: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_chunks_processed", "Number of processed chunks").unwrap()
});

pub(crate) static GAS_PRICE: Lazy<Gauge> = Lazy::new(|| {
    try_create_gauge("near_gas_price", "Gas price of the latest processed block").unwrap()
});

pub(crate) static BALANCE_BURNT: Lazy<Counter> = Lazy::new(|| {
    try_create_counter("near_balance_burnt", "Balance burnt by processed blocks in NEAR tokens")
        .unwrap()
});

pub(crate) static TOTAL_SUPPLY: Lazy<Gauge> = Lazy::new(|| {
    try_create_gauge("near_total_supply", "Gas price of the latest processed block").unwrap()
});

pub(crate) static FINAL_BLOCK_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_final_block_height", "Last block that has full BFT finality")
        .unwrap()
});

pub(crate) static FINAL_DOOMSLUG_BLOCK_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_final_doomslug_block_height",
        "Last block that has Doomslug finality",
    )
    .unwrap()
});

static NODE_DB_VERSION: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_node_db_version", "DB version used by the node").unwrap()
});

static NODE_BUILD_INFO: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_build_info",
        "Metric whose labels indicate node’s version; see \
             <https://www.robustperception.io/exposing-the-software-version-to-prometheus>.",
        &["release", "build", "rustc_version"],
    )
    .unwrap()
});

pub(crate) static TRANSACTION_RECEIVED_VALIDATOR: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_transaction_received_validator", "Validator received a transaction")
        .unwrap()
});

pub(crate) static TRANSACTION_RECEIVED_NON_VALIDATOR: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_transaction_received_non_validator",
        "Non-validator received a transaction",
    )
    .unwrap()
});

pub(crate) static TRANSACTION_RECEIVED_NON_VALIDATOR_FORWARDED: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_transaction_received_non_validator_forwarded",
        "Non-validator received a forwarded transaction",
    )
    .unwrap()
});

pub(crate) static NODE_PROTOCOL_VERSION: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_node_protocol_version", "Max protocol version supported by the node")
        .unwrap()
});

pub(crate) static CURRENT_PROTOCOL_VERSION: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_current_protocol_version", "Protocol version of the current epoch")
        .unwrap()
});

pub(crate) static NODE_PROTOCOL_UPGRADE_VOTING_START: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_node_protocol_upgrade_voting_start",
        "Time in seconds since Unix epoch determining when node will start voting for the protocol upgrade; zero if there is no schedule for the voting")
        .unwrap()
});

pub(crate) static PRODUCE_CHUNK_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_produce_chunk_time",
        "Time taken to produce a chunk",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
    )
    .unwrap()
});

pub(crate) static VIEW_CLIENT_MESSAGE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_view_client_messages_processing_time",
        "Time that view client takes to handle different messages",
        &["message"],
        Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
    )
    .unwrap()
});

pub(crate) static PRODUCE_AND_DISTRIBUTE_CHUNK_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_produce_and_distribute_chunk_time",
        "Time to produce a chunk and distribute it to peers",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
    )
    .unwrap()
});
/// Exports neard, protocol and database versions via Prometheus metrics.
///
/// Sets metrics which export node’s max supported protocol version, used
/// database version and build information.  The latter is taken from
/// `neard_version` argument.
pub(crate) fn export_version(neard_version: &near_primitives::version::Version) {
    NODE_PROTOCOL_VERSION.set(near_primitives::version::PROTOCOL_VERSION.into());
    NODE_PROTOCOL_UPGRADE_VOTING_START
        .set(near_primitives::version::PROTOCOL_UPGRADE_SCHEDULE.timestamp());
    NODE_DB_VERSION.set(near_store::metadata::DB_VERSION.into());
    NODE_BUILD_INFO.reset();
    NODE_BUILD_INFO
        .with_label_values(&[
            &neard_version.version,
            &neard_version.build,
            &neard_version.rustc_version,
        ])
        .inc();
}

pub(crate) static STATE_SYNC_STAGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_stage",
        "Stage of state sync per shard",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_RETRY_PART: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_state_sync_retry_part_total",
        "Number of part requests retried",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_PARTS_DONE: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_parts_done",
        "Number of parts downloaded",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_PARTS_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_parts_per_shard",
        "Number of parts in the shard",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DISCARD_PARTS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_state_sync_discard_parts_total",
        "Number of times all downloaded parts were discarded to try again",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_EXTERNAL_PARTS_DONE: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_state_sync_external_parts_done_total",
        "Number of parts retrieved from external storage",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_EXTERNAL_PARTS_FAILED: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_state_sync_external_parts_failed_total",
        "Failed retrieval attempts from external storage",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_EXTERNAL_PARTS_REQUEST_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_external_parts_request_delay_sec",
        "Latency of state part requests to external storage",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_EXTERNAL_PARTS_SIZE_DOWNLOADED: Lazy<IntCounterVec> =
    Lazy::new(|| {
        try_create_int_counter_vec(
            "near_state_sync_external_parts_size_downloaded_bytes_total",
            "Bytes downloaded from an external storage",
            &["shard_id"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_dump_put_object_elapsed_sec",
        "Latency of writes to external storage",
        &["shard_id", "result"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_LIST_OBJECT_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_dump_list_object_elapsed_sec",
        "Latency of ls in external storage",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});
