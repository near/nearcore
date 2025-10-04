use std::collections::HashMap;

use futures::StreamExt;
use near_async::messaging::{AsyncSender, IntoMultiSender, SendAsync};
use near_chain_configs::ProtocolConfigView;
use near_client::{
    GetBlock, GetChunk, GetExecutionOutcomesForBlock, GetProtocolConfig, GetReceipt,
    GetStateChangesWithCauseInBlockForTrackedShards, Status, StatusResponse,
};
use near_client_primitives::types::{
    GetBlockError, GetChunkError, GetProtocolConfigError, GetReceiptError, GetStateChangesError,
    StatusError,
};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_indexer_primitives::IndexerExecutionOutcomeWithOptionalReceipt;
use near_o11y::span_wrapped_msg::{SpanWrapped, SpanWrappedMessageExt};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockId, BlockReference, EpochId, Finality, ShardId};
use near_primitives::views::{
    BlockView, ChunkView, ExecutionOutcomeWithIdView, ReceiptView, StateChangesView,
};

use crate::INDEXER;
use crate::streamer::errors::FailedToFetchData;

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
struct IndexerViewClientSender {
    pub get_block_sender: AsyncSender<GetBlock, Result<BlockView, GetBlockError>>,
    pub get_chunk_sender: AsyncSender<GetChunk, Result<ChunkView, GetChunkError>>,
    pub get_protocol_config_sender:
        AsyncSender<GetProtocolConfig, Result<ProtocolConfigView, GetProtocolConfigError>>,
    pub get_execution_outcomes_for_block_sender: AsyncSender<
        GetExecutionOutcomesForBlock,
        Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdView>>, String>,
    >,
    pub get_receipt_sender: AsyncSender<GetReceipt, Result<Option<ReceiptView>, GetReceiptError>>,
    pub get_state_changes_with_cause_in_block_for_tracked_shards_sender: AsyncSender<
        GetStateChangesWithCauseInBlockForTrackedShards,
        Result<HashMap<ShardId, StateChangesView>, GetStateChangesError>,
    >,
}

#[derive(Clone)]
pub struct IndexerViewClientFetcher {
    sender: IndexerViewClientSender,
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
struct IndexerClientSender {
    pub status_sender: AsyncSender<SpanWrapped<Status>, Result<StatusResponse, StatusError>>,
}

#[derive(Clone)]
pub struct IndexerClientFetcher {
    sender: IndexerClientSender,
}

impl IndexerViewClientFetcher {
    pub(crate) async fn fetch_block(
        &self,
        hash: CryptoHash,
    ) -> Result<BlockView, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?hash, "fetch block by hash");
        self.sender
            .send_async(GetBlock(BlockId::Hash(hash).into()))
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }

    pub(crate) async fn fetch_latest_block(
        &self,
        finality: Finality,
    ) -> Result<BlockView, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?finality, "fetch latest block");
        self.sender
            .send_async(GetBlock(BlockReference::Finality(finality)))
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }

    pub(crate) async fn fetch_block_by_height(
        &self,
        height: u64,
    ) -> Result<BlockView, FailedToFetchData> {
        tracing::debug!(target: INDEXER, %height, "fetch block by height");
        self.sender
            .send_async(GetBlock(BlockId::Height(height).into()))
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }

    /// Fetches all chunks belonging to given block.
    /// Includes transactions and receipts in custom struct (to provide more info).
    pub(crate) async fn fetch_block_new_chunks(
        &self,
        block: &BlockView,
        shard_tracker: &ShardTracker,
    ) -> Result<Vec<ChunkView>, FailedToFetchData> {
        tracing::debug!(target: INDEXER, height = block.header.height,  "fetch chunks for block");
        let mut futures: futures::stream::FuturesUnordered<_> = block
            .chunks
            .iter()
            .filter(|chunk| {
                shard_tracker.cares_about_shard(&block.header.prev_hash, chunk.shard_id)
                    && chunk.is_new_chunk(block.header.height)
            })
            .map(|chunk| self.fetch_single_chunk(chunk.chunk_hash))
            .collect();
        let mut chunks = Vec::<ChunkView>::with_capacity(futures.len());
        while let Some(chunk) = futures.next().await {
            chunks.push(chunk?);
        }
        Ok(chunks)
    }

    pub(crate) async fn fetch_protocol_config(
        &self,
        block_hash: CryptoHash,
    ) -> Result<ProtocolConfigView, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?block_hash, "fetch protocol config");
        Ok(self
            .sender
            .send_async(GetProtocolConfig(BlockReference::from(BlockId::Hash(block_hash))))
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))?)
    }

    /// Fetch all ExecutionOutcomeWithId for current block
    /// Returns a HashMap where the key is shard id IndexerExecutionOutcomeWithOptionalReceipt
    pub(crate) async fn fetch_outcomes(
        &self,
        block_hash: CryptoHash,
    ) -> Result<HashMap<ShardId, Vec<IndexerExecutionOutcomeWithOptionalReceipt>>, FailedToFetchData>
    {
        tracing::debug!(target: INDEXER, ?block_hash, "fetching outcomes for block");
        let outcomes = self
            .sender
            .send_async(GetExecutionOutcomesForBlock { block_hash })
            .await?
            .map_err(FailedToFetchData::String)?;

        let mut shard_execution_outcomes_with_receipts: HashMap<
            ShardId,
            Vec<IndexerExecutionOutcomeWithOptionalReceipt>,
        > = HashMap::new();
        for (shard_id, shard_outcomes) in outcomes {
            tracing::debug!(target: INDEXER, "Fetching outcomes with receipts for shard: {}", shard_id);
            let mut outcomes_with_receipts: Vec<IndexerExecutionOutcomeWithOptionalReceipt> =
                vec![];
            for outcome in shard_outcomes {
                let receipt = match self.fetch_receipt_by_id(outcome.id).await {
                    Ok(res) => res,
                    Err(err) => {
                        tracing::warn!(
                            target: INDEXER,
                            ?err,
                            outcome_id = ?outcome.id,
                            "unable to fetch Receipt by outcome id, skipping it in ExecutionOutcome",
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

    pub(crate) async fn fetch_state_changes(
        &self,
        block_hash: CryptoHash,
        epoch_id: EpochId,
    ) -> Result<HashMap<ShardId, StateChangesView>, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?block_hash, ?epoch_id, "fetch state changes");
        self.sender
            .send_async(GetStateChangesWithCauseInBlockForTrackedShards { block_hash, epoch_id })
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }

    async fn fetch_single_chunk(
        &self,
        chunk_hash: CryptoHash,
    ) -> Result<ChunkView, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?chunk_hash, "fetch chunk by hash");
        self.sender
            .send_async(GetChunk::ChunkHash(chunk_hash.into()))
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }

    async fn fetch_receipt_by_id(
        &self,
        receipt_id: CryptoHash,
    ) -> Result<Option<ReceiptView>, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?receipt_id, "fetch receipt by id");
        self.sender
            .send_async(GetReceipt { receipt_id })
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }
}

impl<T: IntoMultiSender<IndexerViewClientSender>> From<T> for IndexerViewClientFetcher {
    fn from(value: T) -> Self {
        Self { sender: value.into_multi_sender() }
    }
}

impl IndexerClientFetcher {
    pub(crate) async fn fetch_status(&self) -> Result<StatusResponse, FailedToFetchData> {
        tracing::debug!(target: INDEXER, "fetch status");
        self.sender
            .send_async(Status { is_health_check: false, detailed: false }.span_wrap())
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }
}

impl<T: IntoMultiSender<IndexerClientSender>> From<T> for IndexerClientFetcher {
    fn from(value: T) -> Self {
        Self { sender: value.into_multi_sender() }
    }
}
