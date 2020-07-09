//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use std::time::Duration;

use actix::Addr;
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info};

use near_client;
pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};

const INTERVAL: Duration = Duration::from_millis(500);
const INDEXER: &str = "indexer";

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub struct FailedToFetchData;

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
        .await
        .map_err(|_| FailedToFetchData)?
        .map_err(|_| FailedToFetchData)
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
async fn fetch_latest_block(
    client: &Addr<near_client::ViewClientActor>,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock::latest())
        .await
        .map_err(|_| FailedToFetchData)?
        .map_err(|_| FailedToFetchData)
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
        .await
        .map_err(|_| FailedToFetchData)?
        .map_err(|_| FailedToFetchData)
}

/// This function supposed to return the entire `BlockResponse`.
/// It calls fetches the block and fetches all the chunks for the block
/// and returns everything together in one struct
async fn fetch_block_with_chunks(
    client: &Addr<near_client::ViewClientActor>,
    block: views::BlockView,
    outcomes_to_get: std::vec::Drain<'_, types::TransactionOrReceiptId>,
) -> Result<(BlockResponse, Vec<types::TransactionOrReceiptId>), FailedToFetchData> {
    let chunks = fetch_chunks(&client, &block.chunks).await?;
    let (outcomes, outcomes_to_retry) = fetch_outcomes(&client, outcomes_to_get).await;

    let mut transaction_or_receipt_ids: Vec<types::TransactionOrReceiptId> = chunks
        .iter()
        .map(|chunk| {
            let mut transaction_ids: Vec<types::TransactionOrReceiptId> = chunk
                .transactions
                .iter()
                .map(|transaction| types::TransactionOrReceiptId::Transaction {
                    transaction_hash: transaction.hash.clone(),
                    sender_id: transaction.signer_id.to_string(),
                })
                .collect::<Vec<types::TransactionOrReceiptId>>();
            let receipt_ids: Vec<types::TransactionOrReceiptId> = chunk
                .receipts
                .iter()
                .map(|receipt| types::TransactionOrReceiptId::Receipt {
                    receipt_id: receipt.receipt_id,
                    receiver_id: receipt.receiver_id.to_string().clone(),
                })
                .collect::<Vec<types::TransactionOrReceiptId>>();
            transaction_ids.extend(receipt_ids);
            transaction_ids
        })
        .flatten()
        .collect();

    transaction_or_receipt_ids.extend(outcomes_to_retry);

    Ok((BlockResponse { block, chunks, outcomes }, transaction_or_receipt_ids))
}

/// Fetches single chunk (as `near_primitives::views::ChunkView`) by provided `near_client::GetChunk` enum
async fn fetch_single_chunk(
    client: &Addr<near_client::ViewClientActor>,
    get_chunk: near_client::GetChunk,
) -> Result<views::ChunkView, FailedToFetchData> {
    client.send(get_chunk).await.map_err(|_| FailedToFetchData)?.map_err(|_| FailedToFetchData)
}

/// Fetch ExecutionOutcomeWithId for receipts and transactions from previous block
/// Returns fetched Outcomes and Vec of failed to fetch outcome which should be retried to fetch
/// in the next block
async fn fetch_outcomes(
    client: &Addr<near_client::ViewClientActor>,
    mut outcomes_drain: std::vec::Drain<'_, types::TransactionOrReceiptId>,
) -> (Vec<Outcome>, Vec<types::TransactionOrReceiptId>) {
    let mut outcomes: Vec<Outcome> = vec![];
    let mut outcomes_to_retry: Vec<types::TransactionOrReceiptId> = vec![];
    while let Some(transaction_or_receipt_id) = outcomes_drain.next() {
        let outcome_type = match &transaction_or_receipt_id {
            types::TransactionOrReceiptId::Transaction { .. } => "transaction",
            _ => "receipt",
        };
        match client
            .send(near_client::GetExecutionOutcome { id: transaction_or_receipt_id.clone() })
            .await
            .map_err(|_| FailedToFetchData)
            .map_err(|_| FailedToFetchData)
        {
            Ok(Ok(outcome)) => {
                if outcome_type == "transaction" {
                    outcomes.push(Outcome::Transaction(outcome.outcome_proof));
                } else {
                    outcomes.push(Outcome::Receipt(outcome.outcome_proof));
                }
            }
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
    let mut response: Vec<views::ChunkView> = vec![];

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
    loop {
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

                    if let Ok((block_response, new_outcome_to_get)) = response {
                        outcomes_to_get = new_outcome_to_get;
                        debug!(target: INDEXER, "{:#?}", &block_response);
                        match queue.send(block_response).await {
                            _ => {} // TODO: handle error somehow
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
