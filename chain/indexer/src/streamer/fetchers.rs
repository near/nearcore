//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use std::collections::HashMap;

use actix::Addr;
use futures::stream::StreamExt;
use tracing::warn;

use near_indexer_primitives::IndexerExecutionOutcomeWithOptionalReceipt;
use near_primitives::hash::CryptoHash;
use near_primitives::{types, views};

use super::errors::FailedToFetchData;
use super::INDEXER;

pub(crate) async fn fetch_status(
    client: &Addr<near_client::ClientActor>,
) -> Result<near_primitives::views::StatusResponse, FailedToFetchData> {
    client
        .send(near_client::Status { is_health_check: false, detailed: false })
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
pub(crate) async fn fetch_latest_block(
    client: &near_client::ViewClientHandle,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        )))
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches specific block by it's height
pub(crate) async fn fetch_block_by_height(
    client: &near_client::ViewClientHandle,
    height: u64,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockId::Height(height).into()))
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches specific block by it's hash
pub(crate) async fn fetch_block_by_hash(
    client: &near_client::ViewClientHandle,
    hash: CryptoHash,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockId::Hash(hash).into()))
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

pub(crate) async fn fetch_state_changes(
    client: &near_client::ViewClientHandle,
    block_hash: CryptoHash,
    epoch_id: near_primitives::types::EpochId,
) -> Result<HashMap<near_primitives::types::ShardId, views::StateChangesView>, FailedToFetchData> {
    client
        .send(near_client::GetStateChangesWithCauseInBlockForTrackedShards { block_hash, epoch_id })
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetch all ExecutionOutcomeWithId for current block
/// Returns a HashMap where the key is shard id IndexerExecutionOutcomeWithOptionalReceipt
pub(crate) async fn fetch_outcomes(
    client: &near_client::ViewClientHandle,
    block_hash: CryptoHash,
) -> Result<
    HashMap<near_primitives::types::ShardId, Vec<IndexerExecutionOutcomeWithOptionalReceipt>>,
    FailedToFetchData,
> {
    let outcomes = client
        .send(near_client::GetExecutionOutcomesForBlock { block_hash })
        .await?
        .map_err(FailedToFetchData::String)?;

    let mut shard_execution_outcomes_with_receipts: HashMap<
        near_primitives::types::ShardId,
        Vec<IndexerExecutionOutcomeWithOptionalReceipt>,
    > = HashMap::new();
    for (shard_id, shard_outcomes) in outcomes {
        let mut outcomes_with_receipts: Vec<IndexerExecutionOutcomeWithOptionalReceipt> = vec![];
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
            outcomes_with_receipts.push(IndexerExecutionOutcomeWithOptionalReceipt {
                execution_outcome: outcome,
                receipt,
            });
        }
        shard_execution_outcomes_with_receipts.insert(shard_id, outcomes_with_receipts);
    }

    Ok(shard_execution_outcomes_with_receipts)
}

async fn fetch_receipt_by_id(
    client: &near_client::ViewClientHandle,
    receipt_id: CryptoHash,
) -> Result<Option<views::ReceiptView>, FailedToFetchData> {
    client
        .send(near_client::GetReceipt { receipt_id })
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches single chunk (as `near_primitives::views::ChunkView`) by provided
/// chunk hash.
async fn fetch_single_chunk(
    client: &near_client::ViewClientHandle,
    chunk_hash: near_primitives::hash::CryptoHash,
) -> Result<views::ChunkView, FailedToFetchData> {
    client
        .send(near_client::GetChunk::ChunkHash(chunk_hash.into()))
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches all chunks belonging to given block.
/// Includes transactions and receipts in custom struct (to provide more info).
pub(crate) async fn fetch_block_chunks(
    client: &near_client::ViewClientHandle,
    block: &views::BlockView,
) -> Result<Vec<views::ChunkView>, FailedToFetchData> {
    let mut futures: futures::stream::FuturesUnordered<_> = block
        .chunks
        .iter()
        .filter(|chunk| chunk.height_included == block.header.height)
        .map(|chunk| fetch_single_chunk(&client, chunk.chunk_hash))
        .collect();
    let mut chunks = Vec::<views::ChunkView>::with_capacity(futures.len());
    while let Some(chunk) = futures.next().await {
        chunks.push(chunk?);
    }
    Ok(chunks)
}

pub(crate) async fn fetch_protocol_config(
    client: &near_client::ViewClientHandle,
    block_hash: near_primitives::hash::CryptoHash,
) -> Result<near_chain_configs::ProtocolConfigView, FailedToFetchData> {
    Ok(client
        .send(near_client::GetProtocolConfig(types::BlockReference::from(types::BlockId::Hash(
            block_hash,
        ))))
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))?)
}
