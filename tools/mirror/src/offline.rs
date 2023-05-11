use crate::{ChainError, SourceBlock, SourceChunk};
use anyhow::Context;
use async_trait::async_trait;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_chain_primitives::error::EpochErrorResultToChainError;
use near_crypto::PublicKey;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::types::{AccountId, BlockHeight, TransactionOrReceiptId};
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionOutcomeWithIdView, QueryRequest, QueryResponseKind,
};
use near_primitives_core::types::ShardId;
use nearcore::NightshadeRuntime;
use std::path::Path;
use std::sync::Arc;

fn is_on_current_chain(
    chain: &ChainStore,
    header: &BlockHeader,
) -> Result<bool, near_chain_primitives::Error> {
    let chain_header = chain.get_block_header_by_height(header.height())?;
    Ok(chain_header.hash() == header.hash())
}

pub(crate) struct ChainAccess {
    chain: ChainStore,
    epoch_manager: Arc<EpochManagerHandle>,
    runtime: Arc<NightshadeRuntime>,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let mut config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;
        let node_storage =
            nearcore::open_storage(home.as_ref(), &mut config).context("failed opening storage")?;
        let store = node_storage.get_hot_store();
        let chain = ChainStore::new(
            store.clone(),
            config.genesis.config.genesis_height,
            config.client_config.save_trie_changes,
        );
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &config.genesis.config);
        let runtime =
            NightshadeRuntime::from_config(home.as_ref(), store, &config, epoch_manager.clone());
        Ok(Self { chain, epoch_manager, runtime })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    async fn init(
        &self,
        last_height: BlockHeight,
        num_initial_blocks: usize,
    ) -> anyhow::Result<Vec<BlockHeight>> {
        let mut block_heights = Vec::with_capacity(num_initial_blocks);
        let head = self.head_height().await?;

        let mut height = last_height + 1;
        loop {
            if height > head {
                return Ok(block_heights);
            }
            match self.chain.get_block_hash_by_height(height) {
                Ok(hash) => {
                    block_heights.push(
                        self.chain
                            .get_block_header(&hash)
                            .with_context(|| format!("failed fetching block header for {}", &hash))?
                            .height(),
                    );
                    break;
                }
                Err(near_chain_primitives::Error::DBNotFoundErr(_)) => {
                    height += 1;
                }
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("failed fetching block hash for #{}", height))
                }
            };
        }
        while block_heights.len() < num_initial_blocks {
            let last_height = *block_heights.iter().next_back().unwrap();
            match self.get_next_block_height(last_height).await {
                Ok(h) => block_heights.push(h),
                Err(ChainError::Unknown) => break,
                Err(ChainError::Other(e)) => {
                    return Err(e).with_context(|| {
                        format!("failed getting next block height after {}", last_height)
                    })
                }
            };
        }
        Ok(block_heights)
    }

    async fn block_height_to_hash(&self, height: BlockHeight) -> Result<CryptoHash, ChainError> {
        Ok(self.chain.get_block_hash_by_height(height)?)
    }

    async fn head_height(&self) -> Result<BlockHeight, ChainError> {
        Ok(self.chain.head()?.height)
    }

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<SourceBlock, ChainError> {
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
            chunks.push(SourceChunk {
                shard_id: chunk.shard_id(),
                transactions: chunk.transactions().iter().map(|t| t.clone().into()).collect(),
                receipts: chunk.receipts().iter().cloned().collect(),
            })
        }
        Ok(SourceBlock { hash: block_hash, chunks })
    }

    async fn get_next_block_height(&self, height: BlockHeight) -> Result<BlockHeight, ChainError> {
        let hash = self.chain.get_block_hash_by_height(height)?;
        let hash = self.chain.get_next_block_hash(&hash)?;
        Ok(self.chain.get_block_header(&hash)?.height())
    }

    async fn get_outcome(
        &self,
        id: TransactionOrReceiptId,
    ) -> Result<ExecutionOutcomeWithIdView, ChainError> {
        let id = match id {
            TransactionOrReceiptId::Receipt { receipt_id, .. } => receipt_id,
            TransactionOrReceiptId::Transaction { transaction_hash, .. } => transaction_hash,
        };
        let outcomes = self.chain.get_outcomes_by_id(&id)?;
        // this implements the same logic as in Chain::get_execution_outcome(). We will rewrite
        // that here because it makes more sense for us to have just the ChainStore and not the Chain,
        // since we're just reading data, not doing any protocol related stuff
        outcomes
            .into_iter()
            .find(|outcome| match self.chain.get_block_header(&outcome.block_hash) {
                Ok(header) => is_on_current_chain(&self.chain, &header).unwrap_or(false),
                Err(_) => false,
            })
            .map(Into::into)
            .ok_or(ChainError::Unknown)
    }

    async fn get_receipt(&self, id: &CryptoHash) -> Result<Arc<Receipt>, ChainError> {
        self.chain.get_receipt(id)?.ok_or(ChainError::Unknown)
    }

    async fn get_full_access_keys(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> Result<Vec<PublicKey>, ChainError> {
        let mut ret = Vec::new();
        let header = self.chain.get_block_header(block_hash)?;
        let shard_id = self
            .epoch_manager
            .account_id_to_shard_id(account_id, header.epoch_id())
            .into_chain_error()?;
        let shard_uid =
            self.epoch_manager.shard_id_to_uid(shard_id, header.epoch_id()).into_chain_error()?;
        let chunk_extra = self.chain.get_chunk_extra(header.hash(), &shard_uid)?;
        match self
            .runtime
            .query(
                shard_uid,
                chunk_extra.state_root(),
                header.height(),
                header.raw_timestamp(),
                header.prev_hash(),
                header.hash(),
                header.epoch_id(),
                &QueryRequest::ViewAccessKeyList { account_id: account_id.clone() },
            )?
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
        }
        Ok(ret)
    }
}
