//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use std::time::Duration;

use actix::{Addr, MailboxError};
use futures::stream::StreamExt;
use rocksdb::DB;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info};

use near_client;
pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};

use crate::IndexerConfig;

const INTERVAL: Duration = Duration::from_millis(500);
const INDEXER: &str = "indexer";

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub enum FailedToFetchData {
    MailboxError(MailboxError),
    String(String),
}

impl From<MailboxError> for FailedToFetchData {
    fn from(actix_error: MailboxError) -> Self {
        FailedToFetchData::MailboxError(actix_error)
    }
}

struct FetchBlockResponse {
    block_response: StreamerMessage,
    new_outcomes_to_get: Vec<types::TransactionOrReceiptId>,
}

/// Resulting struct represents block with chunks
#[derive(Debug)]
pub struct StreamerMessage {
    pub block: views::BlockView,
    pub chunks: Vec<views::ChunkView>,
    pub outcomes: Vec<Outcome>,
    pub state_changes: views::StateChangesKindsView,
}

#[derive(Clone, Debug)]
pub enum Outcome {
    Receipt(views::ExecutionOutcomeWithIdView),
    Transaction(views::ExecutionOutcomeWithIdView),
}

async fn fetch_status(
    client: &Addr<near_client::ClientActor>,
) -> Result<near_primitives::views::StatusResponse, FailedToFetchData> {
    client
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
async fn fetch_latest_block(
    client: &Addr<near_client::ViewClientActor>,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(types::BlockReference::Finality(types::Finality::Final)))
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches specific block by it's height
async fn fetch_block_by_height(
    client: &Addr<near_client::ViewClientActor>,
    height: u64,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockReference::BlockId(
            near_primitives::types::BlockId::Height(height),
        )))
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// This function supposed to return the entire `BlockResponse`.
/// It fetches the block and fetches all the chunks for the block
/// and returns everything together in one struct
async fn fetch_block_response(
    client: &Addr<near_client::ViewClientActor>,
    block: views::BlockView,
    outcomes_to_get: impl Iterator<Item = types::TransactionOrReceiptId>,
) -> Result<FetchBlockResponse, FailedToFetchData> {
    let chunks = fetch_chunks(&client, &block.chunks).await?;
    let (outcomes, mut outcomes_to_retry) = fetch_outcomes(&client, outcomes_to_get).await;

    for chunk in &chunks {
        outcomes_to_retry.extend(chunk.transactions.iter().map(|transaction| {
            types::TransactionOrReceiptId::Transaction {
                transaction_hash: transaction.hash.clone(),
                sender_id: transaction.signer_id.to_string(),
            }
        }));

        outcomes_to_retry.extend(chunk.receipts.iter().map(|receipt| {
            types::TransactionOrReceiptId::Receipt {
                receipt_id: receipt.receipt_id,
                receiver_id: receipt.receiver_id.to_string(),
            }
        }));
    }

    let state_changes = fetch_state_changes(&client, block.header.hash).await?;

    Ok(FetchBlockResponse {
        block_response: StreamerMessage { block, chunks, outcomes, state_changes },
        new_outcomes_to_get: outcomes_to_retry,
    })
}

async fn fetch_state_changes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
) -> Result<views::StateChangesKindsView, FailedToFetchData> {
    client
        .send(near_client::GetStateChangesInBlock { block_hash })
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches single chunk (as `near_primitives::views::ChunkView`) by provided `near_client::GetChunk` enum
async fn fetch_single_chunk(
    client: &Addr<near_client::ViewClientActor>,
    get_chunk: near_client::GetChunk,
) -> Result<views::ChunkView, FailedToFetchData> {
    client.send(get_chunk).await?.map_err(|err| FailedToFetchData::String(err))
}

/// Fetch ExecutionOutcomeWithId for receipts and transactions from previous block
/// Returns fetched Outcomes and Vec of failed to fetch outcome which should be retried to fetch
/// in the next block
async fn fetch_outcomes(
    client: &Addr<near_client::ViewClientActor>,
    mut outcomes_drain: impl Iterator<Item = types::TransactionOrReceiptId>,
) -> (Vec<Outcome>, Vec<types::TransactionOrReceiptId>) {
    let mut outcomes: Vec<Outcome> = vec![];
    let mut outcomes_to_retry: Vec<types::TransactionOrReceiptId> = vec![];
    while let Some(transaction_or_receipt_id) = outcomes_drain.next() {
        match client
            .send(near_client::GetExecutionOutcome { id: transaction_or_receipt_id.clone() })
            .await
            .map_err(|err| FailedToFetchData::MailboxError(err))
        {
            Ok(Ok(outcome)) => match &transaction_or_receipt_id {
                types::TransactionOrReceiptId::Transaction { .. } => {
                    outcomes.push(Outcome::Transaction(outcome.outcome_proof));
                }
                types::TransactionOrReceiptId::Receipt { .. } => {
                    outcomes.push(Outcome::Receipt(outcome.outcome_proof));
                }
            },
            _ => {
                outcomes_to_retry.push(transaction_or_receipt_id);
            }
        }
    }
    (outcomes, outcomes_to_retry)
}

/// Fetches all the chunks by their hashes.
/// Includes transactions and receipts in custom struct (to provide more info).
/// Returns Chunks as a `Vec`
async fn fetch_chunks(
    client: &Addr<near_client::ViewClientActor>,
    chunks: &[views::ChunkHeaderView],
) -> Result<Vec<views::ChunkView>, FailedToFetchData> {
    let mut chunks: futures::stream::FuturesUnordered<_> = chunks
        .iter()
        .map(|chunk| {
            fetch_single_chunk(&client, near_client::GetChunk::ChunkHash(chunk.chunk_hash.into()))
        })
        .collect();
    let mut response = Vec::<views::ChunkView>::with_capacity(chunks.len());

    while let Some(chunk) = chunks.next().await {
        response.push(chunk?);
    }

    Ok(response)
}

/// Function that starts Streamer's busy loop. Every half a seconds it fetches the status
/// compares to already fetched block height and in case it differs fetches new block of given height.
///
/// We have to pass `client: Addr<near_client::ClientActor>` and `view_client: Addr<near_client::ViewClientActor>`.
pub(crate) async fn start(
    view_client: Addr<near_client::ViewClientActor>,
    client: Addr<near_client::ClientActor>,
    indexer_config: IndexerConfig,
    mut blocks_sink: mpsc::Sender<StreamerMessage>,
) {
    info!(target: INDEXER, "Starting Streamer...");
    let mut indexer_db_path = neard::get_store_path(&indexer_config.home_dir);
    indexer_db_path.push_str("/indexer");

    // TODO: implement proper error handling
    let db = DB::open_default(indexer_db_path).unwrap();
    let mut outcomes_to_get = Vec::<types::TransactionOrReceiptId>::new();
    let mut last_synced_block_height: Option<types::BlockHeight> = None;

    info!(
        target: INDEXER,
        "Last synced block height in db is {}",
        last_synced_block_height.unwrap_or(0)
    );
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
                let response =
                    fetch_block_response(&view_client, block, outcomes_to_get.drain(..)).await;

                match response {
                    Ok(fetch_block_response) => {
                        outcomes_to_get = fetch_block_response.new_outcomes_to_get;
                        debug!(target: INDEXER, "{:#?}", &fetch_block_response.block_response);
                        if let Err(_) = blocks_sink.send(fetch_block_response.block_response).await
                        {
                            info!(
                                        target: INDEXER,
                                        "Unable to send BlockResponse to listener, listener doesn't listen. terminating..."
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
