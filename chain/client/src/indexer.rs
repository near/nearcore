//! Streamer message assembly shared by node-side indexer consumers.
//!
//! Polling, checkpoints and message delivery remain in `near-indexer`.

use crate::{
    GetBlock, GetChunk, GetExecutionOutcomesForBlock, GetProcessedReceiptIds, GetProtocolConfig,
    GetReceipt, GetStateChangesWithCauseInBlockForTrackedShards,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use near_async::messaging::{AsyncSendError, AsyncSender, CanSendAsync, IntoMultiSender};
use near_chain_configs::ProtocolConfigView;
use near_client_primitives::types::{
    GetBlockError, GetChunkError, GetProcessedReceiptIdsError, GetProtocolConfigError,
    GetReceiptError, GetStateChangesError,
};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_indexer_primitives::{
    IndexerChunkView, IndexerExecutionOutcomeWithOptionalReceipt,
    IndexerExecutionOutcomeWithReceipt, IndexerShard, IndexerTransactionWithOutcome,
    StreamerMessage,
};
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ProcessedReceiptMetadata, Receipt, ReceiptSource};
use near_primitives::types::{Balance, BlockId, BlockReference, EpochId, Finality, ShardId};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::{
    BlockView, ChunkView, ExecutionOutcomeWithIdView, ExecutionStatusView, ReceiptEnumView,
    ReceiptView, StateChangesView,
};
use node_runtime::config::calculate_tx_cost;
use std::collections::HashMap;

const INDEXER: &str = "indexer";

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub enum FailedToFetchData {
    String(String),
}

impl From<AsyncSendError> for FailedToFetchData {
    fn from(async_send_error: AsyncSendError) -> Self {
        match async_send_error {
            AsyncSendError::Closed => FailedToFetchData::String("Actor is closed".to_string()),
            AsyncSendError::Timeout => {
                FailedToFetchData::String("Actor send timed out".to_string())
            }
            AsyncSendError::Dropped => {
                FailedToFetchData::String("Actor send was dropped".to_string())
            }
        }
    }
}

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
    pub get_processed_receipt_ids_sender: AsyncSender<
        GetProcessedReceiptIds,
        Result<Vec<ProcessedReceiptMetadata>, GetProcessedReceiptIdsError>,
    >,
}

#[derive(Clone)]
pub struct IndexerViewClientFetcher {
    sender: IndexerViewClientSender,
}

/// This function supposed to return the entire `StreamerMessage`.
/// It fetches the block and all related parts (chunks, outcomes, state changes etc.)
/// and returns everything together in one struct
pub async fn build_streamer_message(
    client: &IndexerViewClientFetcher,
    block: BlockView,
    shard_tracker: &ShardTracker,
) -> Result<StreamerMessage, FailedToFetchData> {
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
    let runtime_config_store = RuntimeConfigStore::new(None);
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
            .ok_or_else(|| FailedToFetchData::String("missing shard execution outcomes".into()))?;
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

        // Local receipts recovered from shard-outcomes would miss the delayed ones.
        let chunk_local_receipts = convert_transactions_sir_into_local_receipts(
            indexer_transactions
                .iter()
                .filter(|tx| tx.transaction.signer_id == tx.transaction.receiver_id),
            &runtime_config,
            gas_price,
        )?;

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

    // By this point every shard the indexer streams has had its outcomes
    // consumed by the per-chunk loop above. Any leftover in `shards_outcomes` is
    // an outcome for a shard whose chunk was not streamed, which we can only
    // observe in two situations:
    //   (a) the indexer's `ShardTracker` excludes a shard the node itself
    //       tracked (the node has the outcomes but the chunk was not streamed);
    //   (b) the post-resharding edge case where a stale shard id is no longer
    //       part of the new layout.
    //
    // Both are unexpected and would require a proper fix to surface correctly
    // (reliably classifying transaction vs receipt outcomes, aligning the
    // indexer's `ShardTracker` with the shards the node tracked, and handling
    // the stale-shard-id case in the per-chunk loop). For now we log
    // a warning so the indexer operator knows something is off.
    //
    // TODO: eliminate leftovers entirely by addressing (a) and (b) above
    // and emitting these outcomes through the per-chunk loop.
    if !shards_outcomes.is_empty() {
        let leftover_outcomes: usize = shards_outcomes.values().map(Vec::len).sum();
        tracing::warn!(
            target: INDEXER,
            block_hash = %block.header.hash,
            leftover_shards = ?shards_outcomes.keys().collect::<Vec<_>>(),
            leftover_outcomes,
            "execution outcomes left after streaming all chunks; they are not included in the streamer message",
        );
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

impl IndexerViewClientFetcher {
    async fn fetch_block(&self, hash: CryptoHash) -> Result<BlockView, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?hash, "fetch block by hash");
        self.sender
            .send_async(GetBlock(BlockId::Hash(hash).into()))
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }

    pub async fn fetch_latest_block(
        &self,
        finality: Finality,
    ) -> Result<BlockView, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?finality, "fetch latest block");
        self.sender
            .send_async(GetBlock(BlockReference::Finality(finality)))
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }

    pub async fn fetch_block_by_height(
        &self,
        height: u64,
    ) -> Result<Option<BlockView>, FailedToFetchData> {
        tracing::debug!(target: INDEXER, %height, "fetch block by height");
        match self.sender.send_async(GetBlock(BlockId::Height(height).into())).await? {
            Ok(block) => Ok(Some(block)),
            Err(GetBlockError::UnknownBlock { .. }) => Ok(None),
            Err(err) => Err(FailedToFetchData::String(err.to_string())),
        }
    }

    /// Fetches all chunks belonging to given block.
    /// Includes transactions and receipts in custom struct (to provide more info).
    async fn fetch_block_new_chunks(
        &self,
        block: &BlockView,
        shard_tracker: &ShardTracker,
    ) -> Result<Vec<ChunkView>, FailedToFetchData> {
        tracing::debug!(target: INDEXER, height = block.header.height,  "fetch chunks for block");
        let mut futures = FuturesUnordered::new();
        for chunk in &block.chunks {
            if !chunk.is_new_chunk(block.header.height) {
                continue;
            }
            let cares_about_shard = shard_tracker
                .cares_about_shard_checked(&block.header.prev_hash, chunk.shard_id)
                .map_err(|err| {
                    FailedToFetchData::String(format!(
                        "failed to determine shard tracking for shard {} at block {}: {err}",
                        chunk.shard_id, block.header.hash,
                    ))
                })?;
            if !cares_about_shard {
                continue;
            }
            futures.push(self.fetch_single_chunk(chunk.chunk_hash));
        }
        let mut chunks = Vec::<ChunkView>::with_capacity(futures.len());
        while let Some(chunk) = futures.next().await {
            chunks.push(chunk?);
        }
        Ok(chunks)
    }

    async fn fetch_protocol_config(
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

    async fn fetch_outcomes(
        &self,
        block_hash: CryptoHash,
    ) -> Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdView>>, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?block_hash, "fetch outcomes for block");
        self.sender
            .send_async(GetExecutionOutcomesForBlock { block_hash })
            .await?
            .map_err(FailedToFetchData::String)
    }

    async fn fetch_outcomes_with_receipts(
        &self,
        block_hash: CryptoHash,
    ) -> Result<HashMap<ShardId, Vec<IndexerExecutionOutcomeWithOptionalReceipt>>, FailedToFetchData>
    {
        tracing::debug!(target: INDEXER, ?block_hash, "fetch outcomes with receipts for block");
        let outcomes = self.fetch_outcomes(block_hash).await?;
        let mut shard_execution_outcomes_with_receipts: HashMap<
            ShardId,
            Vec<IndexerExecutionOutcomeWithOptionalReceipt>,
        > = HashMap::new();
        for (shard_id, shard_outcomes) in outcomes {
            tracing::debug!(target: INDEXER, %shard_id, "fetch outcomes with receipts for shard");
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
                            "unable to fetch receipt by outcome id, skipping it in execution outcome",
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

    async fn fetch_state_changes(
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

    async fn fetch_processed_receipt_ids(
        &self,
        block_hash: CryptoHash,
        shard_id: ShardId,
    ) -> Result<Vec<ProcessedReceiptMetadata>, FailedToFetchData> {
        tracing::debug!(target: INDEXER, ?block_hash, %shard_id, "fetch processed receipt ids");
        self.sender
            .send_async(GetProcessedReceiptIds { block_hash, shard_id })
            .await?
            .map_err(|err| FailedToFetchData::String(err.to_string()))
    }
}

impl<T: IntoMultiSender<IndexerViewClientSender>> From<T> for IndexerViewClientFetcher {
    fn from(value: T) -> Self {
        Self { sender: value.into_multi_sender() }
    }
}

fn convert_transactions_sir_into_local_receipts<'a>(
    tx_iter: impl IntoIterator<Item = &'a IndexerTransactionWithOutcome>,
    runtime_config: &RuntimeConfig,
    gas_price: Balance,
) -> Result<Vec<ReceiptView>, FailedToFetchData> {
    let mut local_receipts = Vec::new();
    for indexer_tx in tx_iter {
        let tx = &indexer_tx.transaction;
        assert_eq!(tx.signer_id, tx.receiver_id);
        let outcome = &indexer_tx.outcome.execution_outcome.outcome;
        let ExecutionStatusView::SuccessReceiptId(receipt_id) = outcome.status else {
            tracing::debug!(
                target: INDEXER,
                block_hash = %indexer_tx.outcome.execution_outcome.block_hash,
                tx_hash = %tx.hash,
                status = ?outcome.status,
                "skip failed local tx",
            );
            continue;
        };
        let actions: Vec<_> =
            tx.actions.iter().cloned().map(Action::try_from).collect::<Result<_, _>>().map_err(
                |error| FailedToFetchData::String(format!("invalid local action: {error}")),
            )?;
        let cost = calculate_tx_cost(
            &tx.receiver_id,
            &tx.signer_id,
            &tx.public_key,
            &actions,
            runtime_config,
            gas_price,
        )
        .map_err(|error| {
            FailedToFetchData::String(format!("invalid local transaction cost: {error}"))
        })?;
        // Use empty actions here and clone actions from transactions later.
        // Note that we cannot just pass `actions` here since conversion
        // ActionView -> Action -> ActionView does not always preserve the
        // content of the action.
        let receipt = Receipt::from_tx(
            receipt_id,
            tx.signer_id.clone(),
            tx.receiver_id.clone(),
            tx.public_key.clone(),
            cost.receipt_gas_price,
            vec![],
        );
        let mut receipt_view: ReceiptView = receipt.into();
        let ReceiptEnumView::Action { actions, .. } = &mut receipt_view.receipt else {
            unreachable!("transaction is expected to be converted to an action receipt");
        };
        actions.clone_from(&indexer_tx.transaction.actions);
        local_receipts.push(receipt_view);
    }
    Ok(local_receipts)
}
