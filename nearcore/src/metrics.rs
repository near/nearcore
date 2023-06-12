use std::rc::Rc;

use actix_rt::ArbiterHandle;
use near_chain::{Block, ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManager;
use near_o11y::metrics::{
    exponential_buckets, linear_buckets, try_create_histogram_vec, try_create_int_counter_vec,
    try_create_int_gauge, try_create_int_gauge_vec, HistogramVec, IntCounterVec, IntGauge,
    IntGaugeVec,
};

use near_primitives::{shard_layout::ShardLayout, state_record::StateRecord, trie_key};
use near_store::{ShardUId, Store, Trie, TrieDBStorage};
use once_cell::sync::Lazy;

use crate::NearConfig;

pub(crate) static APPLY_CHUNK_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_apply_chunk_delay_seconds",
        "Time to process a chunk. Gas used by the chunk is a metric label, rounded up to 100 teragas.",
        &["tgas_ceiling"],
        Some(linear_buckets(0.0, 0.05, 50).unwrap()),
    )
        .unwrap()
});

pub(crate) static DELAYED_RECEIPTS_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_delayed_receipts_count",
        "The length of the delayed receipts queue. Indicator of congestion.",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static POSTPONED_RECEIPTS_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_postponed_receipts_count",
        "The length of the postponed receipts queue. Indicator of congestion.",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static CONFIG_CORRECT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_config_correct",
        "Are the current dynamically loadable configs correct",
    )
    .unwrap()
});

pub(crate) static COLD_STORE_COPY_RESULT: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_cold_store_copy_result",
        "The result of a cold store copy iteration in the cold store loop.",
        &["copy_result"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_ITERATION_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_sync_dump_iteration_elapsed_sec",
        "Time needed to obtain and write a part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 25).unwrap()),
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_NUM_PARTS_TOTAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_num_parts_total",
        "Total number of parts in the epoch that being dumped",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_NUM_PARTS_DUMPED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_num_parts_dumped",
        "Number of parts dumped in the epoch that is being dumped",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_SIZE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_state_sync_dump_size_total",
        "Total size of parts written to S3",
        &["epoch_height", "shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_DUMP_EPOCH_HEIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_state_sync_dump_epoch_height",
        "Epoch Height of an epoch being dumped",
        &["shard_id"],
    )
    .unwrap()
});

pub(crate) static STATE_SYNC_APPLY_PART_DELAY: Lazy<near_o11y::metrics::HistogramVec> =
    Lazy::new(|| {
        try_create_histogram_vec(
            "near_state_sync_apply_part_delay_sec",
            "Latency of applying a state part",
            &["shard_id"],
            Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
        )
        .unwrap()
    });

pub(crate) static STATE_SYNC_OBTAIN_PART_DELAY: Lazy<near_o11y::metrics::HistogramVec> =
    Lazy::new(|| {
        try_create_histogram_vec(
            "near_state_sync_obtain_part_delay_sec",
            "Latency of applying a state part",
            &["shard_id", "result"],
            Some(exponential_buckets(0.001, 2.0, 20).unwrap()),
        )
        .unwrap()
    });

fn log_trie_item(
    item: Result<(Vec<u8>, Vec<u8>), near_store::StorageError>,
) -> Result<(), anyhow::Error> {
    if !tracing::level_enabled!(tracing::Level::TRACE) {
        return Ok(());
    }
    let (key, value) = item?;
    let state_record = StateRecord::from_raw_key_value(key, value);
    match state_record {
        Some(StateRecord::PostponedReceipt(receipt)) => {
            tracing::trace!(
                target: "metrics",
                "trie-stats - PostponedReceipt(predecessor_id: {:?}, receiver_id: {:?})",
                receipt.predecessor_id,
                receipt.receiver_id,
            );
        }
        _ => {
            tracing::trace!(target: "metrics", "trie-stats - {state_record:?}" );
        }
    }
    Ok(())
}

fn export_postponed_receipt_count(near_config: &NearConfig, store: &Store) -> anyhow::Result<()> {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)?;

    let head = chain_store.final_head()?;
    let block = chain_store.get_block(&head.last_block_hash)?;
    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id())?;

    for chunk_header in block.chunks().iter() {
        let shard_id = chunk_header.shard_id();
        if chunk_header.height_included() != block.header().height() {
            tracing::trace!(target: "metrics", "trie-stats - chunk for shard {shard_id} is missing, skipping it.");
            POSTPONED_RECEIPTS_COUNT.with_label_values(&[&shard_id.to_string()]).set(0);
            continue;
        }

        let count = get_postponed_receipt_count_for_shard(
            shard_id,
            &shard_layout,
            &chain_store,
            &block,
            store,
        );
        let count = match count {
            Ok(count) => count,
            Err(err) => {
                tracing::trace!(target: "metrics", "trie-stats - error when getting the postponed receipt count {err:?}");
                0
            }
        };
        POSTPONED_RECEIPTS_COUNT.with_label_values(&[&shard_id.to_string()]).set(count);
    }

    Ok(())
}

fn get_postponed_receipt_count_for_shard(
    shard_id: u64,
    shard_layout: &ShardLayout,
    chain_store: &ChainStore,
    block: &Block,
    store: &Store,
) -> Result<i64, anyhow::Error> {
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, shard_layout);
    let chunk_extra = chain_store.get_chunk_extra(block.hash(), &shard_uid)?;
    let state_root = chunk_extra.state_root();
    let storage = TrieDBStorage::new(store.clone(), shard_uid);
    let storage = Rc::new(storage);
    let flat_storage_chunk_view = None;
    let trie = Trie::new(storage, *state_root, flat_storage_chunk_view);
    let seek_count;
    let prune_count;
    {
        tracing::trace!(target: "metrics", "trie stats - seek");
        let mut iter = trie.iter()?;
        iter.seek_prefix([trie_key::col::POSTPONED_RECEIPT])?;
        let mut count = 0;
        for item in iter {
            count += 1;
            log_trie_item(item)?;
        }
        seek_count = count;
        tracing::trace!(target: "metrics", "trie-stats - seek postponed receipt count {count}");
    }
    {
        tracing::trace!(target: "metrics", "trie stats - prune");
        let prune_condition: Box<dyn Fn(&Vec<u8>) -> bool> =
            Box::new(postponed_receipt_prune_condition);
        let iter = trie.iter_with_prune_condition(Some(prune_condition))?;
        let mut count = 0;
        for item in iter {
            count += 1;
            log_trie_item(item)?;
        }
        prune_count = count;
        tracing::trace!(target: "metrics", "trie-stats - prune postponed receipt count {count}");
    }
    if seek_count != prune_count {
        tracing::error!(target: "metrics", "trie-stats - ERROR, the seek count {} != the prune count {}", seek_count, prune_count);
    }
    Ok(seek_count)
}

fn postponed_receipt_prune_condition(key_nibbles: &Vec<u8>) -> bool {
    if key_nibbles.len() < 2 {
        return false;
    }
    let col = key_nibbles[0] * 16 + key_nibbles[1] * 1;
    if col == trie_key::col::POSTPONED_RECEIPT {
        return false;
    }
    true
}

/// Spawns a background loop that will periodically log trie related metrics.
pub fn spawn_trie_metrics_loop(
    near_config: NearConfig,
    store: Store,
    period: std::time::Duration,
) -> anyhow::Result<ArbiterHandle> {
    tracing::debug!(target:"metrics", "Spawning the trie metrics loop.");
    let arbiter = actix_rt::Arbiter::new();

    let start = tokio::time::Instant::now();
    let mut interval = actix_rt::time::interval_at(start, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    arbiter.spawn(async move {
        tracing::debug!(target:"metrics", "Starting the spawn metrics loop.");
        loop {
            interval.tick().await;

            let result = export_postponed_receipt_count(&near_config, &store);
            if let Err(err) = result {
                tracing::error!("Error when exporting postponed receipts count {err}");
            };
        }
    });

    Ok(arbiter.handle())
}
