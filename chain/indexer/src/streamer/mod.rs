use self::errors::FailedToFetchData;
use self::utils::convert_transactions_sir_into_local_receipts;
use crate::INDEXER;
use crate::{AwaitForNodeSyncedEnum, IndexerConfig};
pub use fetchers::{IndexerClientFetcher, IndexerViewClientFetcher};
use near_async::time::{Clock, Duration};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_indexer_primitives::{
    IndexerChunkView, IndexerExecutionOutcomeWithOptionalReceipt,
    IndexerExecutionOutcomeWithReceipt, IndexerShard, IndexerTransactionWithOutcome,
    StreamerMessage,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptSource;
use near_primitives::types::{BlockHeight, EpochId, ShardId};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::{BlockView, ChunkView, ReceiptView};
use rocksdb::DB;
use std::collections::HashMap;
use tokio::sync::mpsc;

mod errors;
mod fetchers;
mod metrics;
mod utils;

const INTERVAL: Duration = Duration::milliseconds(250);

/// This function supposed to return the entire `StreamerMessage`.
/// It fetches the block and all related parts (chunks, outcomes, state changes etc.)
/// and returns everything together in one struct
pub async fn build_streamer_message(
    client: &IndexerViewClientFetcher,
    block: BlockView,
    shard_tracker: &ShardTracker,
) -> Result<StreamerMessage, FailedToFetchData> {
    let _timer = metrics::BUILD_STREAMER_MESSAGE_TIME.start_timer();
    let chunks = client.fetch_block_new_chunks(&block, shard_tracker).await?;

    let protocol_config_view = client.fetch_protocol_config(block.header.hash).await?;
    let protocol_version = protocol_config_view.protocol_version;
    let shard_ids = protocol_config_view.shard_layout.shard_ids();
    let gas_price = if block.header.prev_hash == CryptoHash::default() {
        block.header.gas_price
    } else {
        let prev_block = client.fetch_block(block.header.prev_hash).await?;
        prev_block.header.gas_price
    };
    let runtime_config_store = near_parameters::RuntimeConfigStore::new(None);
    let runtime_config = runtime_config_store.get_config(protocol_config_view.protocol_version);

    let mut shards_outcomes = client.fetch_outcomes_with_receipts(block.header.hash).await?;
    let mut state_changes =
        client.fetch_state_changes(block.header.hash, EpochId(block.header.epoch_id)).await?;
    let mut indexer_shards = shard_ids
        .map(|shard_id| IndexerShard {
            shard_id,
            chunk: None,
            receipt_execution_outcomes: vec![],
            state_changes: state_changes.remove(&shard_id).unwrap_or_default(),
        })
        .collect::<Vec<_>>();

    // TODO(spice): Add indexer support for spice.
    if ProtocolFeature::Spice.enabled(protocol_version) {
        return Ok(StreamerMessage { block, shards: indexer_shards });
    }

    for chunk in chunks {
        let ChunkView { transactions, author, header, receipts: chunk_prev_outgoing_receipts } =
            chunk;

        let outcomes = shards_outcomes
            .remove(&header.shard_id)
            .expect("execution outcomes for given shard should be present");
        let outcome_count = outcomes.len();
        let outcome_order: Vec<CryptoHash> =
            outcomes.iter().map(|o| o.execution_outcome.id).collect();
        let mut outcomes: HashMap<_, _> =
            outcomes.into_iter().map(|outcome| (outcome.execution_outcome.id, outcome)).collect();
        debug_assert_eq!(outcomes.len(), outcome_count);
        let indexer_transactions = transactions
            .into_iter()
            .filter_map(|transaction| {
                let outcome = outcomes.remove(&transaction.hash);
                if outcome.is_none() {
                    tracing::error!(
                        target: INDEXER,
                        tx_hash = %transaction.hash,
                        shard_id = %header.shard_id,
                        block_hash = %block.header.hash,
                        "unexpected missing transaction outcome"
                    );
                }
                outcome.map(|outcome| IndexerTransactionWithOutcome { outcome, transaction })
            })
            .collect::<Vec<IndexerTransactionWithOutcome>>();
        // All transaction outcomes have been removed.
        let mut receipt_outcomes = outcomes;

        let chunk_local_receipts = convert_transactions_sir_into_local_receipts(
            indexer_transactions
                .iter()
                .filter(|tx| tx.transaction.signer_id == tx.transaction.receiver_id),
            &runtime_config,
            gas_price,
        );

        // Pair same-block local receipts (executed in the chunk that produced
        // them) with their outcomes as a fallback. Delayed local receipts -
        // executed in a later block - are served from `DBCol::Receipts` by
        // `fetch_outcomes_with_receipts` instead.
        for receipt in &chunk_local_receipts {
            if let Some(outcome) = receipt_outcomes.get_mut(&receipt.receipt_id) {
                if outcome.receipt.is_none() {
                    outcome.receipt = Some(receipt.clone());
                }
            }
        }

        let mut receipt_execution_outcomes: Vec<IndexerExecutionOutcomeWithReceipt> = vec![];
        for outcome_id in outcome_order {
            let Some(outcome) = receipt_outcomes.remove(&outcome_id) else {
                // outcome_id corresponds to a transaction, already handled above
                continue;
            };

            let IndexerExecutionOutcomeWithOptionalReceipt { execution_outcome, receipt } = outcome;
            let Some(receipt) = receipt else {
                // A receipt-execution outcome must have its receipt. A `None` here is
                // unexpected; return an error so the streamer handles the error.
                return Err(FailedToFetchData::String(format!(
                    "missing receipt for execution outcome {} in block {}",
                    execution_outcome.id, block.header.hash,
                )));
            };
            receipt_execution_outcomes
                .push(IndexerExecutionOutcomeWithReceipt { execution_outcome, receipt });
        }

        let instant_receipts =
            fetch_instant_receipts(client, block.header.hash, header.shard_id).await;

        // Find the shard index for the chunk by shard_id
        let shard_index = protocol_config_view
            .shard_layout
            .get_shard_index(header.shard_id)
            .map_err(|e| FailedToFetchData::String(e.to_string()))?;

        // Add receipt_execution_outcomes into corresponding indexer shard
        indexer_shards[shard_index].receipt_execution_outcomes = receipt_execution_outcomes;
        // Put the chunk into corresponding indexer shard
        indexer_shards[shard_index].chunk = Some(IndexerChunkView {
            author,
            header,
            transactions: indexer_transactions,
            receipts: chunk_prev_outgoing_receipts,
            local_receipts: chunk_local_receipts,
            instant_receipts,
        });
    }

    // We expect `shards_outcomes` to be empty by this time: outcomes for shards
    // the indexer streams were consumed by the per-chunk loop above, and
    // `fetch_block_new_chunks` surfaces epoch-lookup errors.
    //
    // Leftovers can still occur when the indexer's `ShardTracker` excludes a
    // shard the node itself tracked (so the node has its outcomes but the chunk
    // was not streamed), or for the post-resharding edge case where a stale
    // shard id is no longer part of the new layout. We surface the receipt
    // execution outcomes for such shards.
    for (shard_id, outcomes) in shards_outcomes {
        let Ok(shard_index) = protocol_config_view.shard_layout.get_shard_index(shard_id) else {
            continue;
        };
        for outcome in outcomes {
            let IndexerExecutionOutcomeWithOptionalReceipt { execution_outcome, receipt } = outcome;
            let Some(receipt) = receipt else {
                // Transaction outcomes do not have a receipt; other outcomes missing a receipt are unexpected.
                if !matches!(
                    execution_outcome.outcome.status,
                    near_primitives::views::ExecutionStatusView::SuccessReceiptId(_)
                ) {
                    return Err(FailedToFetchData::String(format!(
                        "missing receipt for execution outcome {} in block {} (leftover shard {})",
                        execution_outcome.id, block.header.hash, shard_id,
                    )));
                }
                continue;
            };
            indexer_shards[shard_index]
                .receipt_execution_outcomes
                .push(IndexerExecutionOutcomeWithReceipt { execution_outcome, receipt });
        }
    }

    Ok(StreamerMessage { block, shards: indexer_shards })
}

/// Fetches instant receipts for a given block and shard.
///
/// Instant receipts (e.g. PromiseYield) may not have execution outcomes in the
/// block where they are processed (they can be postponed and executed later),
/// so each receipt is fetched directly from `DBCol::Receipts`.
async fn fetch_instant_receipts(
    view_client: &IndexerViewClientFetcher,
    block_hash: CryptoHash,
    shard_id: ShardId,
) -> Vec<ReceiptView> {
    let instant_receipt_ids: Vec<CryptoHash> =
        match view_client.fetch_processed_receipt_ids(block_hash, shard_id).await {
            Ok(metadata) => metadata
                .into_iter()
                .filter(|m| matches!(m.source(), ReceiptSource::Instant))
                .map(|m| *m.receipt_id())
                .collect(),
            Err(err) => {
                tracing::warn!(
                    target: INDEXER,
                    ?err,
                    %block_hash,
                    %shard_id,
                    "unable to fetch processed receipt ids, instant_receipts will be empty",
                );
                return vec![];
            }
        };

    let mut instant_receipts: Vec<ReceiptView> = vec![];
    for receipt_id in instant_receipt_ids {
        match view_client.fetch_receipt_by_id(receipt_id).await {
            Ok(Some(receipt)) => instant_receipts.push(receipt),
            Ok(None) => {
                tracing::warn!(
                    target: INDEXER,
                    ?receipt_id,
                    "instant receipt not found in store",
                );
            }
            Err(err) => {
                tracing::warn!(
                    target: INDEXER,
                    ?receipt_id,
                    ?err,
                    "unable to fetch instant receipt",
                );
            }
        }
    }
    instant_receipts
}

/// Function that starts Streamer's busy loop. Every half a seconds it fetches the status
/// compares to already fetched block height and in case it differs fetches new block of given height.
pub async fn start(
    view_client: IndexerViewClientFetcher,
    client: IndexerClientFetcher,
    shard_tracker: ShardTracker,
    indexer_config: IndexerConfig,
    store_config: near_store::StoreConfig,
    blocks_sink: mpsc::Sender<StreamerMessage>,
    clock: Clock,
) {
    tracing::info!(target: INDEXER, "starting streamer");
    let indexer_db_path =
        near_store::NodeStorage::opener(&indexer_config.home_dir, &store_config, None, None)
            .path()
            .join("indexer");

    let db = match DB::open_default(indexer_db_path) {
        Ok(db) => db,
        Err(err) => panic!("Unable to open indexer db: {:?}", err),
    };

    let mut last_synced_block_height: Option<BlockHeight> = None;

    'main: loop {
        clock.sleep(INTERVAL).await;
        match indexer_config.await_for_node_synced {
            AwaitForNodeSyncedEnum::WaitForFullSync => {
                let status = client.fetch_status().await;
                let Ok(status) = status else {
                    tracing::error!(target: INDEXER, ?status, "failed to fetch node status, retrying");
                    continue;
                };
                if status.sync_info.syncing {
                    tracing::debug!(target: INDEXER, ?status, "the node is syncing, waiting");
                    continue;
                }
            }
            AwaitForNodeSyncedEnum::StreamWhileSyncing => {}
        };

        tracing::debug!(target: INDEXER, "starting streaming the next block range");
        let block = view_client.fetch_latest_block(indexer_config.finality.clone()).await;
        let Ok(block) = block else {
            tracing::error!(target: INDEXER, ?block, "failed to fetch latest block, retrying");
            continue;
        };

        let latest_block_height = block.header.height;
        let start_syncing_block_height = get_start_syncing_block_height(
            &db,
            &indexer_config,
            last_synced_block_height,
            latest_block_height,
        );

        tracing::debug!(
            target: INDEXER,
            %start_syncing_block_height,
            %latest_block_height,
            "streaming is about to start",
        );
        metrics::START_BLOCK_HEIGHT.set(start_syncing_block_height as i64);
        metrics::LATEST_BLOCK_HEIGHT.set(latest_block_height as i64);
        for block_height in start_syncing_block_height..=latest_block_height {
            metrics::CURRENT_BLOCK_HEIGHT.set(block_height as i64);

            let block = match view_client.fetch_block_by_height(block_height).await {
                Ok(Some(block)) => block,
                Ok(None) => {
                    tracing::debug!(target: INDEXER, ?block_height, "skip height - missing block");
                    continue;
                }
                Err(err) => {
                    tracing::error!(target: INDEXER, ?block_height, ?err, "skip height - failed to fetch block");
                    continue;
                }
            };

            let streamer_message =
                Box::pin(build_streamer_message(&view_client, block, &shard_tracker)).await;
            let Ok(streamer_message) = streamer_message else {
                // `break`, not `continue`: the error may be transient (#15867), so
                // retry this height on the next outer iteration instead of advancing
                // `last_synced_block_height` past it and dropping the block.
                tracing::error!(target: INDEXER, ?block_height, ?streamer_message, "failed to build streamer message, retrying the same height");
                break;
            };

            tracing::debug!(target: INDEXER, ?block_height, "sending streamer message to the listener");
            let send_result = blocks_sink.send(streamer_message).await;
            if send_result.is_err() {
                tracing::error!(
                    target: INDEXER,
                    ?block_height,
                    ?send_result,
                    "unable to send streamer message to listener, listener doesn't listen, terminating",
                );
                break 'main;
            };

            metrics::NUM_STREAMER_MESSAGES_SENT.inc();
            db.put(b"last_synced_block_height", &block_height.to_string()).unwrap();
            last_synced_block_height = Some(block_height);
        }
    }
}

fn get_start_syncing_block_height(
    db: &rocksdb::DB,
    indexer_config: &IndexerConfig,
    last_synced_block_height: Option<u64>,
    latest_block_height: u64,
) -> u64 {
    // If last synced is set, start from the next height
    if let Some(last_synced_block_height) = last_synced_block_height {
        return last_synced_block_height + 1;
    }

    // Otherwise determine the start height based on the sync mode
    match indexer_config.sync_mode {
        crate::SyncModeEnum::FromInterruption => {
            match db.get(b"last_synced_block_height").unwrap() {
                Some(value) => String::from_utf8(value).unwrap().parse::<u64>().unwrap(),
                None => latest_block_height,
            }
        }
        crate::SyncModeEnum::LatestSynced => latest_block_height,
        crate::SyncModeEnum::BlockHeight(height) => height,
    }
}
