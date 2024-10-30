use near_o11y::metrics::{
    exponential_buckets, linear_buckets, try_create_counter, try_create_counter_vec,
    try_create_gauge, try_create_histogram, try_create_histogram_vec, try_create_int_counter,
    try_create_int_counter_vec, try_create_int_gauge, try_create_int_gauge_vec, Counter,
    CounterVec, Gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};
use std::sync::LazyLock;

pub(crate) static BLOCK_PRODUCED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_block_produced_total",
        "Total number of blocks produced since starting this node",
    )
    .unwrap()
});

pub(crate) static CHUNK_PRODUCED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_chunk_produced_total",
        "Total number of chunks produced since starting this node",
    )
    .unwrap()
});

pub(crate) static PRODUCED_CHUNKS_SOME_POOL_TRANSACTIONS_DIDNT_FIT: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        try_create_int_counter_vec(
        "near_produced_chunks_some_pool_transactions_didnt_fit",
        "Total number of produced chunks where some transactions from the pool didn't fit in the chunk \
        (since starting this node). The limited_by label specifies which limit was hit.",
        &["shard_id", "limited_by"],
    )
    .unwrap()
    });

pub(crate) static IS_VALIDATOR: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_is_validator",
        "Bool to denote if it is validating in the current epoch",
    )
    .unwrap()
});

pub(crate) static IS_BLOCK_PRODUCER: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_is_block_producer",
        "Bool to denote if the node is a block producer in the current epoch",
    )
    .unwrap()
});

pub(crate) static IS_CHUNK_PRODUCER_FOR_SHARD: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_is_chunk_producer_for_shard",
        "Bool to denote if the node is a chunk producer for a shard in the current epoch",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static RECEIVED_BYTES_PER_SECOND: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_received_bytes_per_second",
        "Number of bytes per second received over the network overall",
    )
    .unwrap()
});

pub(crate) static SENT_BYTES_PER_SECOND: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_sent_bytes_per_second",
        "Number of bytes per second sent over the network overall",
    )
    .unwrap()
});

pub(crate) static CPU_USAGE: LazyLock<IntGauge> =
    LazyLock::new(|| try_create_int_gauge("near_cpu_usage_ratio", "Percent of CPU usage").unwrap());

pub(crate) static MEMORY_USAGE: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_memory_usage_bytes", "Amount of RAM memory usage").unwrap()
});

pub(crate) static GC_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram("near_gc_time", "Time taken to do garbage collection").unwrap()
});

pub(crate) static TGAS_USAGE_HIST: LazyLock<HistogramVec> = LazyLock::new(|| {
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

pub(crate) static VALIDATORS_CHUNKS_PRODUCED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_validators_chunks_produced",
        "Number of chunks produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_CHUNKS_EXPECTED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_validators_chunks_expected",
        "Number of chunks expected to be produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_CHUNKS_PRODUCED_BY_SHARD: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_validators_chunks_produced_by_shard",
            "Number of chunks produced by a validator",
            &["account_id", "shard_id"],
        )
        .unwrap()
    });

pub(crate) static VALIDATORS_CHUNKS_EXPECTED_BY_SHARD: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_validators_chunks_expected_by_shard",
            "Number of chunks expected to be produced by a validator",
            &["account_id", "shard_id"],
        )
        .unwrap()
    });

pub(crate) static VALIDATORS_CHUNK_ENDORSEMENTS_PRODUCED_BY_SHARD: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_validators_chunk_endorsements_produced_by_shard",
            "Number of chunk endorsements produced by a chunk validator (currently calculated solely based on chunk production)",
            &["account_id", "shard_id"],
        )
        .unwrap()
    });

pub(crate) static VALIDATORS_CHUNK_ENDORSEMENTS_EXPECTED_BY_SHARD: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_validators_chunk_endorsements_expected_by_shard",
            "Number of chunk endorsements expected to be produced by a chunk validator (currently calculated solely based on chunk production)",
            &["account_id", "shard_id"],
        )
        .unwrap()
    });

pub(crate) static VALIDATORS_CHUNKS_EXPECTED_IN_EPOCH: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_validators_chunks_expected_in_epoch",
            "Number of chunks expected to be produced by a validator within current epoch",
            &["account_id", "shard_id", "epoch_height"],
        )
        .unwrap()
    });

pub(crate) static VALIDATORS_BLOCKS_PRODUCED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_validators_blocks_produced",
        "Number of blocks produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_BLOCKS_EXPECTED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_validators_blocks_expected",
        "Number of blocks expected to be produced by a validator",
        &["account_id"],
    )
    .unwrap()
});

pub(crate) static VALIDATORS_BLOCKS_EXPECTED_IN_EPOCH: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_validators_blocks_expected_in_epoch",
            "Number of blocks expected to be produced by a validator within current epoch",
            &["account_id", "epoch_height"],
        )
        .unwrap()
    });

pub(crate) static BLOCK_PRODUCER_STAKE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_block_producer_stake",
        "Stake of each block producer in the network",
        &["account_id", "epoch_height"],
    )
    .unwrap()
});

pub(crate) static TRACKED_SHARDS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec("near_client_tracked_shards", "Tracked shards", &["shard_id"]).unwrap()
});

pub(crate) static SYNC_STATUS: LazyLock<IntGauge> =
    LazyLock::new(|| try_create_int_gauge("near_sync_status", "Node sync status").unwrap());

pub(crate) static EPOCH_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_epoch_height", "Height of the epoch at the head of the blockchain")
        .unwrap()
});

pub(crate) static FINAL_BLOCK_HEIGHT_IN_EPOCH: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_final_block_height_in_epoch",
        "Height of the last block within the epoch.",
    )
    .unwrap()
});

pub(crate) static PROTOCOL_UPGRADE_BLOCK_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_protocol_upgrade_block_height",
        "Estimated block height of the protocol upgrade",
    )
    .unwrap()
});

pub(crate) static PEERS_WITH_INVALID_HASH: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_peers_with_invalid_hash", "Number of peers that are on invalid hash")
        .unwrap()
});

pub(crate) static CHUNK_SKIPPED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_chunk_skipped_total",
        "Number of skipped chunks",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static CHUNK_PRODUCER_BANNED_FOR_EPOCH: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_chunk_producer_banned_for_epoch",
        "Number of times we have banned a chunk producer for an epoch",
    )
    .unwrap()
});

pub(crate) static CHUNK_DROPPED_BECAUSE_OF_BANNED_CHUNK_PRODUCER: LazyLock<IntCounter> =
    LazyLock::new(|| {
        try_create_int_counter(
            "near_chunk_dropped_because_of_banned_chunk_producer",
            "Number of chunks we, as a block producer,
                dropped, because the chunk is produced by a banned chunk producer",
        )
        .unwrap()
    });

pub(crate) static CLIENT_MESSAGES_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_client_messages_count",
        "Number of messages client actor received by message type",
        &["type"],
    )
    .unwrap()
});

pub(crate) static CLIENT_MESSAGES_PROCESSING_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_client_messages_processing_time",
        "Processing time of messages that client actor received, sorted by message type",
        &["type"],
        Some(exponential_buckets(0.0001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHECK_TRIGGERS_TIME: LazyLock<Histogram> = LazyLock::new(|| {
    try_create_histogram(
        "near_client_triggers_time",
        "Processing time of the check_triggers function in client",
    )
    .unwrap()
});

pub(crate) static CLIENT_TRIGGER_TIME_BY_TYPE: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_client_triggers_time_by_type",
        "Time spent on the different triggers in client",
        &["trigger"],
        Some(exponential_buckets(0.0001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static GAS_USED: LazyLock<Counter> = LazyLock::new(|| {
    try_create_counter("near_gas_used", "Gas used by processed blocks, measured in gas").unwrap()
});

pub(crate) static BLOCKS_PROCESSED: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter("near_blocks_processed", "Number of processed blocks").unwrap()
});

pub(crate) static CHUNKS_PROCESSED: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter("near_chunks_processed", "Number of processed chunks").unwrap()
});

pub(crate) static GAS_PRICE: LazyLock<Gauge> = LazyLock::new(|| {
    try_create_gauge("near_gas_price", "Gas price of the latest processed block").unwrap()
});

pub(crate) static BALANCE_BURNT: LazyLock<Counter> = LazyLock::new(|| {
    try_create_counter("near_balance_burnt", "Balance burnt by processed blocks in NEAR tokens")
        .unwrap()
});

pub(crate) static TOTAL_SUPPLY: LazyLock<Gauge> = LazyLock::new(|| {
    try_create_gauge("near_total_supply", "Gas price of the latest processed block").unwrap()
});

pub(crate) static FINAL_BLOCK_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_final_block_height", "Last block that has full BFT finality")
        .unwrap()
});

pub(crate) static FINAL_DOOMSLUG_BLOCK_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_final_doomslug_block_height",
        "Last block that has Doomslug finality",
    )
    .unwrap()
});

static NODE_DB_VERSION: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_node_db_version", "DB version used by the node").unwrap()
});

static NODE_BUILD_INFO: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_build_info",
        "Metric whose labels indicate node’s version; see \
             <https://www.robustperception.io/exposing-the-software-version-to-prometheus>.",
        &["release", "build", "rustc_version"],
    )
    .unwrap()
});

pub(crate) static TRANSACTION_RECEIVED_VALIDATOR: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_transaction_received_validator", "Validator received a transaction")
        .unwrap()
});

pub(crate) static TRANSACTION_RECEIVED_NON_VALIDATOR: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_transaction_received_non_validator",
        "Non-validator received a transaction",
    )
    .unwrap()
});

pub(crate) static TRANSACTION_RECEIVED_NON_VALIDATOR_FORWARDED: LazyLock<IntGauge> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "near_transaction_received_non_validator_forwarded",
            "Non-validator received a forwarded transaction",
        )
        .unwrap()
    });

pub(crate) static NODE_PROTOCOL_VERSION: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_node_protocol_version", "Max protocol version supported by the node")
        .unwrap()
});

pub(crate) static CURRENT_PROTOCOL_VERSION: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge("near_current_protocol_version", "Protocol version of the current epoch")
        .unwrap()
});

pub(crate) static NODE_PROTOCOL_UPGRADE_VOTING_START: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_node_protocol_upgrade_voting_start",
        "Time in seconds since Unix epoch determining when node will start voting for the protocol upgrade; zero if there is no schedule for the voting",
     &["protocol_version"])
        .unwrap()
});

pub(crate) static PRODUCE_CHUNK_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_produce_chunk_time",
        "Time taken to produce a chunk",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
    )
    .unwrap()
});

pub(crate) static VIEW_CLIENT_MESSAGE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_view_client_messages_processing_time",
        "Time that view client takes to handle different messages",
        &["message"],
        Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
    )
    .unwrap()
});

pub(crate) static PRODUCE_AND_DISTRIBUTE_CHUNK_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
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
    let schedule = near_primitives::version::PROTOCOL_UPGRADE_SCHEDULE;
    for (datetime, protocol_version) in schedule.schedule().iter() {
        NODE_PROTOCOL_UPGRADE_VOTING_START
            .with_label_values(&[&protocol_version.to_string()])
            .set(datetime.timestamp());
    }
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

pub(crate) static EPOCH_SYNC_LAST_GENERATED_COMPRESSED_PROOF_SIZE: LazyLock<IntGauge> =
    LazyLock::new(|| {
        try_create_int_gauge(
            "near_epoch_sync_last_generated_compressed_proof_size",
            "Size of the last generated compressed epoch sync proof, in bytes",
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_STAGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_stage",
        "Stage of state sync per shard",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DOWNLOAD_RESULT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_state_sync_header_download_result",
        "Count of number of state sync downloads by type (header, part),
               source (network, external), and result (timeout, error, success)",
        &["shard_id", "type", "source", "result"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_PARTS_TOTAL: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_parts_per_shard",
        "Number of parts in the shard",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_P2P_REQUEST_DELAY: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_state_sync_p2p_request_delay_sec",
        "Latency of state requests to peers",
        &["shard_id", "type"],
        Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_EXTERNAL_PARTS_REQUEST_DELAY: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_state_sync_external_parts_request_delay_sec",
            "Latency of state part requests to external storage",
            &["shard_id", "type"],
            Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_EXTERNAL_PARTS_SIZE_DOWNLOADED: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        try_create_int_counter_vec(
            "near_state_sync_external_parts_size_downloaded_bytes_total",
            "Bytes downloaded from an external storage",
            &["shard_id", "type"],
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_state_sync_dump_put_object_elapsed_sec",
            "Latency of writes to external storage",
            &["shard_id", "result", "type"],
            Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_DUMP_LIST_OBJECT_ELAPSED: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_state_sync_dump_list_object_elapsed_sec",
            "Latency of ls in external storage",
            &["shard_id"],
            Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
        )
        .unwrap()
    });

pub(crate) static SYNC_REQUIREMENT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_sync_requirements_total",
        "Number of sync was required",
        &["state"],
    )
    .unwrap()
});

pub(crate) static SYNC_REQUIREMENT_CURRENT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_sync_requirements_current",
        "The latest SyncRequirement",
        &["state"],
    )
    .unwrap()
});

pub(crate) static ORPHAN_CHUNK_STATE_WITNESSES_TOTAL_COUNT: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        try_create_int_counter_vec(
            "near_orphan_chunk_state_witness_total_count",
            "Total number of orphaned chunk state witnesses that were saved for later processing",
            &["shard_id"],
        )
        .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_NETWORK_ROUNDTRIP_TIME: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_network_roundtrip_time",
            "Time in seconds between sending state witness through the network to chunk producer and receiving the corresponding ack message",
            &["witness_size_bucket"],
            Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
        )
        .unwrap()
    });

pub(crate) static ORPHAN_CHUNK_STATE_WITNESS_POOL_SIZE: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_orphan_chunk_state_witness_pool_size",
            "Number of orphaned witnesses kept in OrphanStateWitnessPool (by shard_id)",
            &["shard_id"],
        )
        .unwrap()
    });

pub(crate) static ORPHAN_CHUNK_STATE_WITNESS_POOL_MEMORY_USED: LazyLock<IntGaugeVec> =
    LazyLock::new(|| {
        try_create_int_gauge_vec(
            "near_orphan_chunk_state_witness_pool_memory_used",
            "Memory in bytes consumed by the OrphanStateWitnessPool (by shard_id)",
            &["shard_id"],
        )
        .unwrap()
    });

pub(crate) static BLOCK_PRODUCER_ENDORSED_STAKE_RATIO: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_block_producer_endorsed_stake_ratio",
            "Ratio (the value is between 0.0 and 1.0) of the endorsed stake for the produced block",
            &["shard_id"],
            Some(linear_buckets(0.0, 0.05, 20).unwrap()),
        )
        .unwrap()
    });

pub(crate) static BLOCK_PRODUCER_MISSING_ENDORSEMENT_COUNT: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_block_producer_missing_endorsement_count",
            "Number of validators from which the block producer has not received endorsements",
            &["shard_id"],
            Some({
                let mut buckets = vec![0.0, 1.0, 2.0, 3.0, 4.0];
                buckets.append(&mut exponential_buckets(5.0, 1.5, 10).unwrap());
                buckets
            }),
        )
        .unwrap()
    });

pub(crate) static PARTIAL_WITNESS_ENCODE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_partial_witness_encode_time",
        "Partial state witness generation from encoded state witness time in seconds",
        &["shard_id"],
        Some(linear_buckets(0.0, 0.005, 20).unwrap()),
    )
    .unwrap()
});

pub(crate) static PARTIAL_WITNESS_TIME_TO_LAST_PART: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_partial_witness_time_to_last_part",
        "Time taken from receiving first partial witness part to receiving enough parts to decode the state witness",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 13).unwrap()),
    )
    .unwrap()
});

pub(crate) static PARTIAL_WITNESS_CACHE_SIZE: LazyLock<Gauge> = LazyLock::new(|| {
    try_create_gauge(
        "near_partial_witness_cache_size",
        "Total size in bytes of all currently cached witness parts",
    )
    .unwrap()
});

pub(crate) static RECEIVE_WITNESS_ACCESSED_CONTRACT_CODES_TIME: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_receive_witness_accessed_contract_codes_time",
            "Time it takes to retrieve missing contract codes",
            &["shard_id"],
            Some(linear_buckets(0.025, 0.025, 40).unwrap()),
        )
        .unwrap()
    });

pub(crate) static WITNESS_ACCESSED_CONTRACT_CODES_DELAY: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_witness_accessed_contract_codes_delay",
            "Delay in witness processing caused by waiting for accessed contract codes",
            &["shard_id"],
            Some(linear_buckets(0.025, 0.025, 40).unwrap()),
        )
        .unwrap()
    });

pub(crate) static DECODE_PARTIAL_WITNESS_ACCESSED_CONTRACTS_STATE_COUNT: LazyLock<CounterVec> =
    LazyLock::new(|| {
        try_create_counter_vec(
            "near_decode_partial_witness_accessed_contracts_state_count",
            "State of the accessed contracts when collected enough parts to decode the witness",
            &["shard_id", "state"],
        )
        .unwrap()
    });
