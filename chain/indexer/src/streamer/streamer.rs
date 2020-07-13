//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use std::time::Duration;

use actix::{Addr, MailboxError};
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use near_client;
pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};

const INTERVAL: Duration = Duration::from_millis(500);
const INDEXER: &str = "indexer";

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub struct FailedToFetchData;

impl From<MailboxError> for FailedToFetchData {
    fn from(_actix_error: MailboxError) -> Self {
        FailedToFetchData
    }
}

struct FetchBlockResponse {
    block_response: BlockResponse,
    new_outcomes_to_get: Vec<types::TransactionOrReceiptId>,
}

/// Resulting struct represents block with chunks
#[derive(Debug)]
pub struct BlockResponse {
    pub block: views::BlockView,
    pub chunks: Vec<views::ChunkView>,
    pub outcomes: Vec<Outcome>,
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
        .map_err(|_| FailedToFetchData)
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
async fn fetch_latest_block(
    client: &Addr<near_client::ViewClientActor>,
) -> Result<views::BlockView, FailedToFetchData> {
    client.send(near_client::GetBlock::latest()).await?.map_err(|_| FailedToFetchData)
}

/// Fetches specific block by it's height
async fn fetch_block_by_height(
    client: &Addr<near_client::ViewClientActor>,
    height: u64,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockIdOrFinality::BlockId(
            near_primitives::types::BlockId::Height(height),
        )))
        .await?
        .map_err(|_| FailedToFetchData)
}

/// This function supposed to return the entire `BlockResponse`.
/// It calls fetches the block and fetches all the chunks for the block
/// and returns everything together in one struct
async fn fetch_block_with_chunks(
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
                receiver_id: receipt.receiver_id.to_string().clone(),
            }
        }));
    }

    Ok(FetchBlockResponse {
        block_response: BlockResponse { block, chunks, outcomes },
        new_outcomes_to_get: outcomes_to_retry,
    })
}

/// Fetches single chunk (as `near_primitives::views::ChunkView`) by provided `near_client::GetChunk` enum
async fn fetch_single_chunk(
    client: &Addr<near_client::ViewClientActor>,
    get_chunk: near_client::GetChunk,
) -> Result<views::ChunkView, FailedToFetchData> {
    client.send(get_chunk).await?.map_err(|_| FailedToFetchData)
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
            .map_err(|_| FailedToFetchData)
        {
            Ok(Ok(outcome)) => match &transaction_or_receipt_id {
                types::TransactionOrReceiptId::Transaction { .. } => {
                    outcomes.push(Outcome::Transaction(outcome.outcome_proof));
                }
                _ => {
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
    let chunks_hashes =
        chunks.iter().map(|chunk| near_client::GetChunk::ChunkHash(chunk.chunk_hash.into()));
    let mut chunks: futures::stream::FuturesUnordered<_> =
        chunks_hashes.map(|get_chunk| fetch_single_chunk(&client, get_chunk)).collect();
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
pub async fn start(
    view_client: Addr<near_client::ViewClientActor>,
    client: Addr<near_client::ClientActor>,
    mut queue: mpsc::Sender<BlockResponse>,
) {
    info!(target: INDEXER, "Starting Streamer...");
    let mut outcomes_to_get: Vec<types::TransactionOrReceiptId> = vec![];
    let mut last_synced_block_height: types::BlockHeight = 0;
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
        if last_synced_block_height == 0 {
            last_synced_block_height = latest_block_height;
        }
        debug!(target: INDEXER, "{:?}", latest_block_height);
        for block_height in (last_synced_block_height + 1)..=latest_block_height {
            match fetch_block_by_height(&view_client, block_height).await {
                Ok(block) => {
                    let response =
                        fetch_block_with_chunks(&view_client, block, outcomes_to_get.drain(..))
                            .await;

                    if let Ok(fetch_block_response) = response {
                        outcomes_to_get = fetch_block_response.new_outcomes_to_get;
                        debug!(target: INDEXER, "{:#?}", &fetch_block_response.block_response);
                        match queue.send(fetch_block_response.block_response).await {
                            Ok(_) => {}
                            _ => {
                                error!(
                                        target: INDEXER,
                                        "Unable to send BlockResponse to listener, listener doesn't listen. terminating..."
                                    );
                                break 'main;
                            }
                        };
                    } else {
                        debug!(target: INDEXER, "Missing data, skipping...");
                    }
                }
                _ => {}
            }
            last_synced_block_height = block_height;
        }
    }
}
