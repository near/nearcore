//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use actix::Addr;
use futures::stream::StreamExt;
use tracing::warn;

pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};

use super::errors::FailedToFetchData;
use super::types::{ExecutionOutcomesWithReceipts, IndexerExecutionOutcomeWithReceipt};
use super::INDEXER;

pub(crate) async fn fetch_status(
    client: &Addr<near_client::ClientActor>,
) -> Result<near_primitives::views::StatusResponse, FailedToFetchData> {
    client
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(FailedToFetchData::String)
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
pub(crate) async fn fetch_latest_block(
    client: &Addr<near_client::ViewClientActor>,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(types::BlockReference::Finality(types::Finality::Final)))
        .await?
        .map_err(FailedToFetchData::String)
}

/// Fetches specific block by it's height
pub(crate) async fn fetch_block_by_height(
    client: &Addr<near_client::ViewClientActor>,
    height: u64,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockReference::BlockId(
            near_primitives::types::BlockId::Height(height),
        )))
        .await?
        .map_err(FailedToFetchData::String)
}

/// Fetches specific block by it's hash
pub(crate) async fn fetch_block_by_hash(
    client: &Addr<near_client::ViewClientActor>,
    hash: CryptoHash,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockId::Hash(hash).into()))
        .await?
        .map_err(FailedToFetchData::String)
}

pub(crate) async fn fetch_state_changes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
) -> Result<views::StateChangesKindsView, FailedToFetchData> {
    client
        .send(near_client::GetStateChangesInBlock { block_hash })
        .await?
        .map_err(FailedToFetchData::String)
}

/// Fetches single chunk (as `near_primitives::views::ChunkView`) by provided `near_client::GetChunk` enum
async fn fetch_single_chunk(
    client: &Addr<near_client::ViewClientActor>,
    get_chunk: near_client::GetChunk,
) -> Result<views::ChunkView, FailedToFetchData> {
    client.send(get_chunk).await?.map_err(FailedToFetchData::String)
}

/// Fetch all ExecutionOutcomeWithId for current block
/// Returns a HashMap where the key is Receipt id or Transaction hash and the value is ExecutionOutcome wth id and proof
pub(crate) async fn fetch_outcomes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
) -> Result<ExecutionOutcomesWithReceipts, FailedToFetchData> {
    let outcomes = client
        .send(near_client::GetExecutionOutcomesForBlock { block_hash })
        .await?
        .map_err(FailedToFetchData::String)?;

    let mut outcomes_with_receipts = ExecutionOutcomesWithReceipts::new();
    for (_, shard_outcomes) in outcomes {
        for outcome in shard_outcomes {
            let receipt = match fetch_receipt_by_id(&client, outcome.id).await {
                Ok(res) => res,
                Err(e) => {
                    warn!(
                        target: INDEXER,
                        "Unable to fetch Receipt with id {}. Skipping it in ExecutionOutcome \n {:#?}",
                        outcome.id,
                        e,
                    );
                    None
                }
            };
            outcomes_with_receipts.insert(
                outcome.id,
                IndexerExecutionOutcomeWithReceipt { execution_outcome: outcome, receipt },
            );
        }
    }

    Ok(outcomes_with_receipts)
}

async fn fetch_receipt_by_id(
    client: &Addr<near_client::ViewClientActor>,
    receipt_id: CryptoHash,
) -> Result<Option<views::ReceiptView>, FailedToFetchData> {
    client.send(near_client::GetReceipt { receipt_id }).await?.map_err(FailedToFetchData::String)
}

/// Fetches all the chunks by their hashes.
/// Includes transactions and receipts in custom struct (to provide more info).
/// Returns Chunks as a `Vec`
pub(crate) async fn fetch_chunks(
    client: &Addr<near_client::ViewClientActor>,
    chunk_hashes: Vec<CryptoHash>,
) -> Result<Vec<views::ChunkView>, FailedToFetchData> {
    let mut chunks: futures::stream::FuturesUnordered<_> = chunk_hashes
        .into_iter()
        .map(|chunk_hash| {
            fetch_single_chunk(&client, near_client::GetChunk::ChunkHash(chunk_hash.into()))
        })
        .collect();
    let mut response = Vec::<views::ChunkView>::with_capacity(chunks.len());

    while let Some(chunk) = chunks.next().await {
        response.push(chunk?);
    }

    Ok(response)
}
