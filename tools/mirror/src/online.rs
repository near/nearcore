use crate::{ChainError, SourceBlock, SourceChunk};
use actix::Addr;
use anyhow::Context;
use async_trait::async_trait;
use near_chain_configs::GenesisValidationMode;
use near_client::ViewClientActor;
use near_client_primitives::types::{
    GetBlock, GetBlockError, GetChunk, GetChunkError, GetExecutionOutcome, GetReceipt, Query,
};
use near_crypto::PublicKey;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, Finality, TransactionOrReceiptId,
};
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionOutcomeWithIdView, QueryRequest, QueryResponseKind,
};
use near_primitives_core::types::ShardId;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct ChainAccess {
    view_client: Addr<ViewClientActor>,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;

        let node = nearcore::start_with_config(home.as_ref(), config)
            .context("failed to start NEAR node")?;
        Ok(Self { view_client: node.view_client })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    async fn init(
        &self,
        last_height: BlockHeight,
        num_initial_blocks: usize,
    ) -> anyhow::Result<Vec<BlockHeight>> {
        // first wait until HEAD moves. We don't really need it to be fully synced.
        let mut first_height = None;
        loop {
            match self.head_height().await {
                Ok(head) => match first_height {
                    Some(h) => {
                        if h != head {
                            break;
                        }
                    }
                    None => {
                        first_height = Some(head);
                    }
                },
                Err(ChainError::Unknown) => {}
                Err(ChainError::Other(e)) => return Err(e),
            };

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let mut block_heights = Vec::with_capacity(num_initial_blocks);
        let mut height = last_height;

        loop {
            // note that here we are using the fact that get_next_block_height() for this struct
            // allows passing a height that doesn't exist in the chain. This is not true for the offline
            // version
            match self.get_next_block_height(height).await {
                Ok(h) => {
                    block_heights.push(h);
                    height = h;
                    if block_heights.len() >= num_initial_blocks {
                        return Ok(block_heights);
                    }
                }
                Err(ChainError::Unknown) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(ChainError::Other(e)) => {
                    return Err(e)
                        .with_context(|| format!("failed fetching next block after #{}", height))
                }
            }
        }
    }

    async fn block_height_to_hash(&self, height: BlockHeight) -> Result<CryptoHash, ChainError> {
        Ok(self
            .view_client
            .send(GetBlock(BlockReference::BlockId(BlockId::Height(height))).with_span_context())
            .await
            .unwrap()?
            .header
            .hash)
    }

    async fn head_height(&self) -> Result<BlockHeight, ChainError> {
        Ok(self
            .view_client
            .send(GetBlock(BlockReference::Finality(Finality::Final)).with_span_context())
            .await
            .unwrap()?
            .header
            .height)
    }

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<SourceBlock, ChainError> {
        let block_hash = self.block_height_to_hash(height).await?;
        let mut chunks = Vec::new();
        for shard_id in shards.iter() {
            let chunk = match self
                .view_client
                .send(GetChunk::Height(height, *shard_id).with_span_context())
                .await
                .unwrap()
            {
                Ok(c) => c,
                Err(e) => match e {
                    GetChunkError::UnknownChunk { .. } => {
                        tracing::error!(
                            "Can't fetch source chain shard {} chunk at height {}. Are we tracking all shards?",
                            shard_id, height
                        );
                        continue;
                    }
                    _ => return Err(e.into()),
                },
            };
            if chunk.header.height_included == height {
                chunks.push(SourceChunk {
                    shard_id: *shard_id,
                    transactions: chunk.transactions.into_iter().map(Into::into).collect(),
                    receipts: chunk.receipts.into_iter().map(|r| r.try_into().unwrap()).collect(),
                })
            }
        }

        Ok(SourceBlock { hash: block_hash, chunks })
    }

    async fn get_next_block_height(
        &self,
        mut height: BlockHeight,
    ) -> Result<BlockHeight, ChainError> {
        let head = self.head_height().await?;

        if height >= head {
            // let's only return finalized heights
            Err(ChainError::Unknown)
        } else if height + 1 == head {
            Ok(head)
        } else {
            loop {
                height += 1;
                if height >= head {
                    break Err(ChainError::Unknown);
                }
                match self
                    .view_client
                    .send(
                        GetBlock(BlockReference::BlockId(BlockId::Height(height)))
                            .with_span_context(),
                    )
                    .await
                    .unwrap()
                {
                    Ok(b) => break Ok(b.header.height),
                    Err(GetBlockError::UnknownBlock { .. }) => {}
                    Err(e) => break Err(ChainError::other(e)),
                }
            }
        }
    }

    async fn get_outcome(
        &self,
        id: TransactionOrReceiptId,
    ) -> Result<ExecutionOutcomeWithIdView, ChainError> {
        Ok(self
            .view_client
            .send(GetExecutionOutcome { id }.with_span_context())
            .await
            .unwrap()?
            .outcome_proof)
    }

    async fn get_receipt(&self, id: &CryptoHash) -> Result<Arc<Receipt>, ChainError> {
        self.view_client
            .send(GetReceipt { receipt_id: *id }.with_span_context())
            .await
            .unwrap()?
            .map(|r| Arc::new(r.try_into().unwrap()))
            .ok_or(ChainError::Unknown)
    }

    async fn get_full_access_keys(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> Result<Vec<PublicKey>, ChainError> {
        let mut ret = Vec::new();
        match self
            .view_client
            .send(
                Query {
                    block_reference: BlockReference::BlockId(BlockId::Hash(*block_hash)),
                    request: QueryRequest::ViewAccessKeyList { account_id: account_id.clone() },
                }
                .with_span_context(),
            )
            .await
            .unwrap()?
            .kind
        {
            QueryResponseKind::AccessKeyList(l) => {
                for k in l.keys {
                    if k.access_key.permission == AccessKeyPermissionView::FullAccess {
                        ret.push(k.public_key);
                    }
                }
            }
            _ => unreachable!(),
        };
        Ok(ret)
    }
}
