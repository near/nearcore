use crate::{ChainError, ChunkTxs};
use anyhow::Context;
use async_trait::async_trait;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_primitives::types::BlockHeight;
use near_primitives_core::types::ShardId;
use std::path::Path;

pub(crate) struct ChainAccess {
    chain: ChainStore,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;
        // leave it ReadWrite since otherwise there are problems with the compiled contract cache
        let store_opener =
            near_store::NodeStorage::opener(home.as_ref(), &config.config.store, None);
        let store = store_opener
            .open()
            .with_context(|| format!("Error opening store in {:?}", home.as_ref()))?
            .get_store(near_store::Temperature::Hot);
        let chain = ChainStore::new(
            store,
            config.genesis.config.genesis_height,
            !config.client_config.archive,
        );
        Ok(Self { chain })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    async fn head_height(&self) -> anyhow::Result<BlockHeight> {
        Ok(self.chain.head().context("Could not fetch chain head")?.height)
    }

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<Vec<ChunkTxs>, ChainError> {
        let block_hash = self.chain.get_block_hash_by_height(height)?;
        let block = self
            .chain
            .get_block(&block_hash)
            .with_context(|| format!("Can't get block {} at height {}", &block_hash, height))?;

        // of course simpler/faster to just have an array of bools but this is a one liner and who cares :)
        let shards = shards.iter().collect::<std::collections::HashSet<_>>();

        let mut chunks = Vec::new();
        for chunk in block.chunks().iter() {
            if !shards.contains(&chunk.shard_id()) {
                continue;
            }
            let chunk = match self.chain.get_chunk(&chunk.chunk_hash()) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        "Can't fetch source chain shard {} chunk at height {}. Are we tracking all shards?: {:?}",
                        chunk.shard_id(), height, e
                    );
                    continue;
                }
            };
            chunks.push(ChunkTxs {
                shard_id: chunk.shard_id(),
                transactions: chunk.transactions().iter().map(|t| t.clone().into()).collect(),
            })
        }
        Ok(chunks)
    }
}
