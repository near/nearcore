//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in to the given queue
use std::collections::HashMap;

use actix::Addr;
use futures::stream::StreamExt;
use tracing::warn;

use near_indexer_primitives::IndexerExecutionOutcomeWithOptionalReceipt;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::{types, views};

use super::errors::FailedToFetchData;
use super::INDEXER;
use near_epoch_manager::shard_tracker::ShardTracker;

pub(crate) async fn fetch_status(
    client: &Addr<near_client::ClientActor>,
) -> Result<near_primitives::views::StatusResponse, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching status");
    client
        .send(near_client::Status { is_health_check: false, detailed: false }.with_span_context())
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
pub(crate) async fn fetch_latest_block(
    client: &Addr<near_client::ViewClientActor>,
) -> Result<views::BlockView, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching latest block");
    client
        .send(
            near_client::GetBlock(near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::Final,
            ))
            .with_span_context(),
        )
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches specific block by it's height
pub(crate) async fn fetch_block_by_height(
    client: &Addr<near_client::ViewClientActor>,
    height: u64,
) -> Result<views::BlockView, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching block by height: {}", height);
    client
        .send(
            near_client::GetBlock(near_primitives::types::BlockId::Height(height).into())
                .with_span_context(),
        )
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches specific block by it's hash
pub(crate) async fn fetch_block(
    client: &Addr<near_client::ViewClientActor>,
    hash: CryptoHash,
) -> Result<views::BlockView, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching block by hash: {}", hash);
    client
        .send(
            near_client::GetBlock(near_primitives::types::BlockId::Hash(hash).into())
                .with_span_context(),
        )
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

pub(crate) async fn fetch_state_changes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
    epoch_id: near_primitives::types::EpochId,
) -> Result<HashMap<near_primitives::types::ShardId, views::StateChangesView>, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching state changes for block: {}, epoch_id: {:?}", block_hash, epoch_id);
    client
        .send(
            near_client::GetStateChangesWithCauseInBlockForTrackedShards { block_hash, epoch_id }
                .with_span_context(),
        )
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetch all ExecutionOutcomeWithId for current block
/// Returns a HashMap where the key is shard id IndexerExecutionOutcomeWithOptionalReceipt
pub(crate) async fn fetch_outcomes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
) -> Result<
    HashMap<near_primitives::types::ShardId, Vec<IndexerExecutionOutcomeWithOptionalReceipt>>,
    FailedToFetchData,
> {
    tracing::debug!(target: INDEXER, "Fetching outcomes for block: {}", block_hash);
    let outcomes = client
        .send(near_client::GetExecutionOutcomesForBlock { block_hash }.with_span_context())
        .await?
        .map_err(FailedToFetchData::String)?;

    let mut shard_execution_outcomes_with_receipts: HashMap<
        near_primitives::types::ShardId,
        Vec<IndexerExecutionOutcomeWithOptionalReceipt>,
    > = HashMap::new();
    for (shard_id, shard_outcomes) in outcomes {
        tracing::debug!(target: INDEXER, "Fetching outcomes with receipts for shard: {}", shard_id);
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
    client: &Addr<near_client::ViewClientActor>,
    receipt_id: CryptoHash,
) -> Result<Option<views::ReceiptView>, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching receipt by id: {}", receipt_id);
    client
        .send(near_client::GetReceipt { receipt_id }.with_span_context())
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches single chunk (as `near_primitives::views::ChunkView`) by provided
/// chunk hash.
async fn fetch_single_chunk(
    client: &Addr<near_client::ViewClientActor>,
    chunk_hash: near_primitives::hash::CryptoHash,
) -> Result<views::ChunkView, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching chunk by hash: {}", chunk_hash);
    client
        .send(near_client::GetChunk::ChunkHash(chunk_hash.into()).with_span_context())
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))
}

/// Fetches all chunks belonging to given block.
/// Includes transactions and receipts in custom struct (to provide more info).
pub(crate) async fn fetch_block_chunks(
    client: &Addr<near_client::ViewClientActor>,
    block: &views::BlockView,
    shard_tracker: &ShardTracker,
) -> Result<Vec<views::ChunkView>, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching chunks for block #{}", block.header.height);
    let mut futures: futures::stream::FuturesUnordered<_> = block
        .chunks
        .iter()
        .filter(|chunk| {
            shard_tracker.care_about_shard(None, &block.header.prev_hash, chunk.shard_id, false)
                && chunk.height_included == block.header.height
        })
        .map(|chunk| fetch_single_chunk(&client, chunk.chunk_hash))
        .collect();
    let mut chunks = Vec::<views::ChunkView>::with_capacity(futures.len());
    while let Some(chunk) = futures.next().await {
        chunks.push(chunk?);
    }
    Ok(chunks)
}

pub(crate) async fn fetch_protocol_config(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: near_primitives::hash::CryptoHash,
) -> Result<near_chain_configs::ProtocolConfigView, FailedToFetchData> {
    tracing::debug!(target: INDEXER, "Fetching protocol config for block: {}", block_hash);
    Ok(client
        .send(
            near_client::GetProtocolConfig(types::BlockReference::from(types::BlockId::Hash(
                block_hash,
            )))
            .with_span_context(),
        )
        .await?
        .map_err(|err| FailedToFetchData::String(err.to_string()))?)
}
