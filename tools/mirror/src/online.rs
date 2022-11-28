use crate::{ChainError, ChunkTxs};
use actix::Addr;
use anyhow::Context;
use async_trait::async_trait;
use near_chain_configs::GenesisValidationMode;
use near_client::{ClientActor, ViewClientActor};
use near_client_primitives::types::{GetChunk, GetChunkError};
use near_o11y::WithSpanContextExt;
use near_primitives::types::BlockHeight;
use near_primitives_core::types::ShardId;
use std::path::Path;
use std::time::Duration;

pub(crate) struct ChainAccess {
    view_client: Addr<ViewClientActor>,
    client: Addr<ClientActor>,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;

        let node = nearcore::start_with_config(home.as_ref(), config.clone())
            .context("failed to start NEAR node")?;
        Ok(Self { view_client: node.view_client, client: node.client })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    // wait until HEAD moves. We don't really need it to be fully synced.
    async fn init(&self) -> anyhow::Result<()> {
        let mut first_height = None;
        loop {
            let head = self.head_height().await?;
            match first_height {
                Some(h) => {
                    if h != head {
                        return Ok(());
                    }
                }
                None => {
                    first_height = Some(head);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn head_height(&self) -> anyhow::Result<BlockHeight> {
        self.client
            .send(
                near_client::Status { is_health_check: false, detailed: false }.with_span_context(),
            )
            .await
            .unwrap()
            .map(|s| s.sync_info.latest_block_height)
            .map_err(Into::into)
    }

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<Vec<ChunkTxs>, ChainError> {
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
                chunks.push(ChunkTxs {
                    shard_id: *shard_id,
                    transactions: chunk.transactions.clone(),
                })
            }
        }

        Ok(chunks)
    }
}
