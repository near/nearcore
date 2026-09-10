use crate::{JsonRpcHandler, RpcFrom};
use near_client::indexer::{IndexerViewClientFetcher, build_streamer_message};
use near_client::{GetBlock, GetClientConfig, GetProtocolConfig};
use near_indexer_primitives::StreamerMessage;
use near_jsonrpc_primitives::types::indexer::{
    RpcIndexerBlockError, RpcIndexerBlockRequest, RpcIndexerBlockResponse,
};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptSource;
use near_primitives::shard_layout::{ShardUId, get_block_shard_uid};
use near_primitives::types::BlockId;
use near_primitives::utils::get_block_shard_id;
use near_primitives::version::ProtocolFeature;
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use serde_json::to_vec;
use std::collections::HashSet;
use std::fmt::Display;

const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

fn unavailable(error: impl Display) -> RpcIndexerBlockError {
    RpcIndexerBlockError::DataUnavailable { error_message: error.to_string() }
}

fn incomplete(error: impl Display) -> RpcIndexerBlockError {
    RpcIndexerBlockError::IncompleteData { error_message: error.to_string() }
}

impl JsonRpcHandler {
    pub(crate) async fn indexer_block(
        &self,
        request: RpcIndexerBlockRequest,
    ) -> Result<RpcIndexerBlockResponse, RpcIndexerBlockError> {
        let _permit =
            self.indexer_requests.try_acquire().map_err(|_| RpcIndexerBlockError::Busy)?;
        let config = self.client_send(GetClientConfig {}.span_wrap()).await?;
        if !config.save_tx_outcomes || !config.save_state_changes {
            return Err(RpcIndexerBlockError::Unsupported {
                error_message: "save_tx_outcomes and save_state_changes must be enabled".into(),
            });
        }
        let block =
            self.view_client_send(GetBlock(BlockId::Hash(request.block_hash).into())).await?;
        let protocol = self
            .view_client_send(GetProtocolConfig(BlockId::Hash(request.block_hash).into()))
            .await?;
        if ProtocolFeature::Spice.enabled(protocol.protocol_version) {
            return Err(RpcIndexerBlockError::Unsupported {
                error_message: "spice indexer execution is not supported yet".into(),
            });
        }
        let (tracker, store) = {
            let pool = self.pool.read();
            (pool.shard_tracker.clone(), pool.chain_store.clone())
        };
        let mut tracked = Vec::new();
        for uid in protocol.shard_layout.shard_uids() {
            if tracker
                .cares_about_shard_checked(&block.header.prev_hash, uid.shard_id())
                .map_err(unavailable)?
            {
                tracked.push(uid);
            }
        }
        self.ensure_chunks_applied(&request.block_hash, &tracked).await.map_err(unavailable)?;
        let fetcher = IndexerViewClientFetcher::from(self.view_client_sender.clone());
        let message = build_streamer_message(&fetcher, block, &tracker)
            .await
            .map_err(RpcIndexerBlockError::rpc_from)?;
        validate_message(&store, &message, &tracked)?;
        let response = RpcIndexerBlockResponse {
            message,
            tracked_shards: tracked.iter().map(ShardUId::shard_id).collect(),
        };
        if to_vec(&response)
            .map_err(|error| RpcIndexerBlockError::InternalError {
                error_message: error.to_string(),
            })?
            .len()
            > MAX_MESSAGE_BYTES
        {
            return Err(RpcIndexerBlockError::LimitExceeded);
        }
        Ok(response)
    }
}

fn validate_message(
    chain_store: &ChainStoreAdapter,
    message: &StreamerMessage,
    tracked: &[ShardUId],
) -> Result<(), RpcIndexerBlockError> {
    let store = chain_store.store_ref();
    let block_hash = message.block.header.hash;
    let genesis = message.block.header.prev_hash == CryptoHash::default();
    if message.shards.iter().map(|shard| shard.shard_id).collect::<Vec<_>>()
        != message.block.chunks.iter().map(|chunk| chunk.shard_id).collect::<Vec<_>>()
    {
        return Err(incomplete("message shard order does not match block layout"));
    }
    for (shard, header) in message.shards.iter().zip(&message.block.chunks) {
        let included = tracked.iter().any(|uid| uid.shard_id() == shard.shard_id)
            && header.is_new_chunk(message.block.header.height);
        if !included {
            if shard.chunk.is_some() || !shard.receipt_execution_outcomes.is_empty() {
                return Err(incomplete("unexpected execution data outside tracked new chunks"));
            }
            continue;
        }
        let chunk = shard.chunk.as_ref().ok_or_else(|| incomplete("missing tracked new chunk"))?;
        let stored_chunk =
            store.chunk_store().get_chunk(&header.chunk_hash.into()).map_err(unavailable)?;
        let tx_ids =
            stored_chunk.to_transactions().iter().map(|tx| tx.get_hash()).collect::<Vec<_>>();
        if chunk.transactions.iter().map(|tx| tx.transaction.hash).collect::<Vec<_>>() != tx_ids
            || chunk
                .transactions
                .iter()
                .any(|tx| tx.outcome.execution_outcome.id != tx.transaction.hash)
        {
            return Err(incomplete("missing or reordered transaction outcomes"));
        }
        // Genesis was never executed and has no execution/processed indexes.
        let outcome_ids = if genesis {
            vec![]
        } else {
            store
                .get_ser::<Vec<CryptoHash>>(
                    DBCol::OutcomeIds,
                    &get_block_shard_id(&block_hash, shard.shard_id),
                )
                .ok_or_else(|| unavailable("outcome index is unavailable"))?
        };
        let transactions = tx_ids.into_iter().collect::<HashSet<_>>();
        let mut receipt_ids = Vec::new();
        for id in outcome_ids {
            chain_store
                .get_outcome_by_id_and_block_hash(&id, &block_hash)
                .ok_or_else(|| incomplete(format!("missing execution outcome {id}")))?;
            if !transactions.contains(&id) {
                chain_store
                    .get_receipt(&id)
                    .ok_or_else(|| incomplete(format!("missing receipt {id}")))?;
                receipt_ids.push(id);
            }
        }
        if shard
            .receipt_execution_outcomes
            .iter()
            .map(|outcome| outcome.execution_outcome.id)
            .collect::<Vec<_>>()
            != receipt_ids
            || shard
                .receipt_execution_outcomes
                .iter()
                .any(|outcome| outcome.receipt.receipt_id != outcome.execution_outcome.id)
        {
            return Err(incomplete("missing or reordered receipt execution outcomes"));
        }
        let instant_ids = if genesis {
            vec![]
        } else {
            let metadata = chain_store
                .get_processed_receipt_ids(&block_hash, shard.shard_id)
                .map_err(unavailable)?;
            let ids = metadata
                .iter()
                .filter(|entry| matches!(entry.source(), ReceiptSource::Instant))
                .map(|entry| *entry.receipt_id())
                .collect::<Vec<_>>();
            for id in &ids {
                chain_store
                    .get_receipt(id)
                    .ok_or_else(|| incomplete(format!("missing instant receipt {id}")))?;
            }
            ids
        };
        if chunk.instant_receipts.iter().map(|receipt| receipt.receipt_id).collect::<Vec<_>>()
            != instant_ids
        {
            return Err(incomplete("missing or reordered instant receipts"));
        }
    }
    // Bypass cached ChunkExtra entries to detect concurrent garbage collection.
    for uid in tracked {
        if !store.exists(DBCol::ChunkExtra, &get_block_shard_uid(&block_hash, uid)) {
            return Err(unavailable("shard data disappeared during assembly"));
        }
    }
    Ok(())
}
