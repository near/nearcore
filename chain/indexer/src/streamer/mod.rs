use std::time::Duration;

use actix::Addr;
use rocksdb::DB;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info};

pub use near_primitives::views;

use crate::IndexerConfig;

use self::errors::FailedToFetchData;
use self::fetchers::{
    fetch_block_by_height, fetch_chunks, fetch_latest_block, fetch_outcomes, fetch_state_changes,
    fetch_status,
};
pub use self::types::{
    ExecutionOutcomesWithReceipts, IndexerChunkView, IndexerExecutionOutcomeWithReceipt,
    IndexerTransactionWithOutcome, StreamerMessage,
};
use self::utils::convert_transactions_sir_into_local_receipts;

mod errors;
mod fetchers;
mod types;
mod utils;

const INDEXER: &str = "indexer";
const INTERVAL: Duration = Duration::from_millis(500);

/// This function supposed to return the entire `StreamerMessage`.
/// It fetches the block and all related parts (chunks, outcomes, state changes etc.)
/// and returns everything together in one struct
async fn build_streamer_message(
    client: &Addr<near_client::ViewClientActor>,
    block: views::BlockView,
    near_config: &neard::NearConfig,
) -> Result<StreamerMessage, FailedToFetchData> {
    let chunks_to_fetch = block
        .chunks
        .iter()
        .filter_map(|c| {
            if c.height_included == block.header.height {
                Some(c.chunk_hash)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let chunks = fetch_chunks(&client, chunks_to_fetch).await?;

    let mut local_receipts: Vec<views::ReceiptView> = vec![];
    let mut outcomes = fetch_outcomes(&client, block.header.hash).await?;
    let mut indexer_chunks: Vec<IndexerChunkView> = vec![];

    for chunk in chunks {
        let views::ChunkView { transactions, author, header, receipts: chunk_non_local_receipts } =
            chunk;

        let indexer_transactions = transactions
            .into_iter()
            .map(|transaction| {
                IndexerTransactionWithOutcome {
                    outcome: outcomes.remove(&transaction.hash).expect("The transaction execution outcome should always present in the same block as the transaction itself"),
                    transaction,
                }
            })
            .collect::<Vec<IndexerTransactionWithOutcome>>();

        let chunk_local_receipts = convert_transactions_sir_into_local_receipts(
            &client,
            near_config,
            indexer_transactions
                .iter()
                .filter(|tx| tx.transaction.signer_id == tx.transaction.receiver_id)
                .collect::<Vec<&IndexerTransactionWithOutcome>>(),
            &block,
        )
        .await?;

        local_receipts.extend_from_slice(&chunk_local_receipts);

        let mut chunk_receipts = chunk_local_receipts;
        chunk_receipts.extend(chunk_non_local_receipts);

        indexer_chunks.push(IndexerChunkView {
            author,
            header,
            transactions: indexer_transactions,
            receipts: chunk_receipts,
        });
    }

    // Add local receipts to corresponding outcomes
    for receipt in &local_receipts {
        if let Some(outcome) = outcomes.get_mut(&receipt.receipt_id) {
            debug_assert!(outcome.receipt.is_none());
            outcome.receipt = Some(receipt.clone());
        }
    }

    let state_changes = fetch_state_changes(&client, block.header.hash).await?;

    Ok(StreamerMessage {
        block,
        chunks: indexer_chunks,
        receipt_execution_outcomes: outcomes,
        state_changes,
    })
}

/// Function that starts Streamer's busy loop. Every half a seconds it fetches the status
/// compares to already fetched block height and in case it differs fetches new block of given height.
///
/// We have to pass `client: Addr<near_client::ClientActor>` and `view_client: Addr<near_client::ViewClientActor>`.
pub(crate) async fn start(
    view_client: Addr<near_client::ViewClientActor>,
    client: Addr<near_client::ClientActor>,
    near_config: neard::NearConfig,
    indexer_config: IndexerConfig,
    mut blocks_sink: mpsc::Sender<StreamerMessage>,
) {
    info!(target: INDEXER, "Starting Streamer...");
    let mut indexer_db_path = neard::get_store_path(&indexer_config.home_dir);
    indexer_db_path.push_str("/indexer");

    // TODO: implement proper error handling
    let db = DB::open_default(indexer_db_path).unwrap();
    let mut last_synced_block_height: Option<near_primitives::types::BlockHeight> = None;

    'main: loop {
        time::delay_for(INTERVAL).await;
        let status = fetch_status(&client).await;
        if let Ok(status) = status {
            if status.sync_info.syncing {
                continue;
            }
        }

        let block = if let Ok(block) = fetch_latest_block(&view_client).await {
            block
        } else {
            continue;
        };

        let latest_block_height = block.header.height;
        let start_syncing_block_height = if let Some(last_synced_block_height) =
            last_synced_block_height
        {
            last_synced_block_height + 1
        } else {
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
        };

        debug!(
            target: INDEXER,
            "Streaming is about to start from block #{} and the latest block is #{}",
            start_syncing_block_height,
            latest_block_height
        );
        for block_height in start_syncing_block_height..=latest_block_height {
            if let Ok(block) = fetch_block_by_height(&view_client, block_height).await {
                let response = build_streamer_message(&view_client, block, &near_config).await;

                match response {
                    Ok(streamer_message) => {
                        debug!(target: INDEXER, "{:#?}", &streamer_message);
                        if blocks_sink.send(streamer_message).await.is_err() {
                            info!(
                                target: INDEXER,
                                "Unable to send StreamerMessage to listener, listener doesn't listen. terminating..."
                            );
                            break 'main;
                        }
                    }
                    Err(err) => {
                        debug!(
                            target: INDEXER,
                            "Missing data, skipping block #{}...", block_height
                        );
                        debug!(target: INDEXER, "{:#?}", err);
                    }
                }
            }
            db.put(b"last_synced_block_height", &block_height.to_string()).unwrap();
            last_synced_block_height = Some(block_height);
        }
    }
}
