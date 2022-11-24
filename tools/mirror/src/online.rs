use crate::{ChainError, SourceBlock};
use actix::Addr;
use anyhow::Context;
use async_trait::async_trait;
use near_chain_configs::GenesisValidationMode;
use near_client::ViewClientActor;
use near_client_primitives::types::{
    GetBlock, GetChunk, GetChunkError, GetExecutionOutcome, GetReceipt, Query,
};
use near_crypto::PublicKey;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, Finality, TransactionOrReceiptId,
};
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionOutcomeWithIdView, QueryRequest, QueryResponseKind,
    ReceiptView,
};
use near_primitives_core::types::ShardId;
use std::path::Path;
use std::time::Duration;

pub(crate) struct ChainAccess {
    view_client: Addr<ViewClientActor>,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;

        let node = nearcore::start_with_config(home.as_ref(), config.clone())
            .context("failed to start NEAR node")?;
        Ok(Self { view_client: node.view_client })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    // wait until HEAD moves. We don't really need it to be fully synced.
    async fn init(&self) -> anyhow::Result<()> {
        let mut first_height = None;
        loop {
            match self.head_height().await {
                Ok(head) => match first_height {
                    Some(h) => {
                        if h != head {
                            return Ok(());
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
    ) -> Result<Vec<SourceBlock>, ChainError> {
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
                chunks.push(SourceBlock {
                    shard_id: *shard_id,
                    transactions: chunk.transactions.clone(),
                    receipts: chunk.receipts.clone(),
                })
            }
        }

        Ok(chunks)
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

    async fn get_receipt(&self, id: &CryptoHash) -> Result<ReceiptView, ChainError> {
        self.view_client
            .send(GetReceipt { receipt_id: id.clone() }.with_span_context())
            .await
            .unwrap()?
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
                    block_reference: BlockReference::BlockId(BlockId::Hash(block_hash.clone())),
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
