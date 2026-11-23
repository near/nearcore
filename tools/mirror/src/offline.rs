use crate::key_util::ACCESS_KEY_PAGE_SIZE;
use crate::{ChainError, SourceBlock, SourceChunk};
use anyhow::Context;
use async_trait::async_trait;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_chain_primitives::Error as ChainStoreError;
use near_chain_primitives::error::{EpochErrorResultToChainError, QueryError};
use near_client::archive::cloud_recent_reader::PullLimit;
use near_crypto::PublicKeyHandle;
use near_epoch_manager::shard_assignment::{account_id_to_shard_id, shard_id_to_uid};
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::transaction::TransactionEnvelope;
use near_primitives::types::{AccountId, BlockHeight, TransactionOrReceiptId};
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionOutcomeWithIdView, QueryRequest, QueryResponseKind,
};
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::genesis::initialize_genesis_state;
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Pause between two looks at the head of a store another task appends to.
const WAIT_FOR_BLOCKS_INTERVAL: Duration = Duration::from_millis(500);

/// How many heights a block must be below the reader head before the mirror reads it from
/// a store another task appends to. The create account lookahead follows a block's receipts
/// to their outcomes, which land in later blocks. Above the reader head those outcomes are
/// not in the store yet, and the extra transactions would be lost.
const OUTCOME_MARGIN: BlockHeight = 100;

/// A source store another task keeps appending to (the cloud archive reader).
pub(crate) struct Follow {
    /// Cancelled on shutdown. A read that waits for blocks stops then.
    pub(crate) shutdown: CancellationToken,
    /// Raised as the mirror asks for heights, so the reader stays at most
    /// `read_ahead` blocks above them.
    pub(crate) pull_limit: PullLimit,
    pub(crate) read_ahead: BlockHeight,
}

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
    /// Set when another task keeps appending blocks to the store (the cloud archive
    /// reader): reads past the head then wait for them until shutdown, instead of
    /// ending the run.
    follow: Option<Follow>,
    /// Set when the store was built without state (`--source-skip-state`): a state
    /// query that fails in the trie then reports `ChainError::NoState`, which the
    /// mirror falls back from, instead of the error aborting the run.
    skip_state: bool,
}

impl ChainAccess {
    /// Opens the store under `home` and serves it as it is.
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let home = home.as_ref();
        let config = nearcore::config::load_config(home, GenesisValidationMode::UnsafeFast)
            .with_context(|| format!("Error loading config from {:?}", home))?;
        let node_storage =
            nearcore::open_storage(home, &config).context("failed opening storage")?;
        let store = node_storage.get_hot_store();
        initialize_genesis_state(store.clone(), &config.genesis, Some(home));
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &config.genesis.config, Some(home));
        let runtime =
            NightshadeRuntime::from_config(home, store.clone(), &config, epoch_manager.clone())
                .context("could not create the transaction runtime")?;
        Ok(Self::from_parts(store, &config, epoch_manager, runtime, None, false))
    }

    /// Serves a store the caller opened and built the chain views for. With `follow`, the
    /// store is one another task appends to, and reads past its head wait for that task
    /// until shutdown. With `skip_state`, the store is known to hold no
    /// state, and a state query failing in the trie is reported as `ChainError::NoState`.
    pub(crate) fn from_parts(
        store: Store,
        config: &NearConfig,
        epoch_manager: Arc<EpochManagerHandle>,
        runtime: Arc<NightshadeRuntime>,
        follow: Option<Follow>,
        skip_state: bool,
    ) -> Self {
        let chain = ChainStore::new(
            store,
            config.client_config.save_trie_changes,
            config.genesis.config.transaction_validity_period,
        );
        Self { chain, epoch_manager, runtime, follow, skip_state }
    }

    /// The last height the store holds in full. With `follow`, that is the reader head:
    /// the reader writes a window's block rows and height index before its chunks, and
    /// moves the reader head (and the chain heads with it) once the whole window is in.
    /// The height index above the reader head can name blocks whose chunks are not in
    /// yet. Without `follow`, the chain head.
    fn complete_head(&self) -> Result<BlockHeight, ChainError> {
        if self.follow.is_none() {
            return Ok(self.chain.head()?.height);
        }
        let reader_head = self
            .chain
            .store()
            .cloud_archival_store()
            .reader_head()
            .context("the followed source store has no reader head")?;
        Ok(reader_head.height)
    }

    /// Waits for the appending task to land more blocks. `Ok(false)` on a static store,
    /// where the caller reports the end of the chain instead.
    async fn wait_for_blocks(&self) -> anyhow::Result<bool> {
        let Some(follow) = &self.follow else {
            return Ok(false);
        };
        tokio::select! {
            _ = follow.shutdown.cancelled() => {
                anyhow::bail!("shutdown requested while waiting for source blocks")
            }
            _ = tokio::time::sleep(WAIT_FOR_BLOCKS_INTERVAL) => Ok(true),
        }
    }

    /// Whether the mirror can send the block at `block_hash` from this store. With
    /// `follow`, that needs the block's new chunks, which the block the bootstrap anchored
    /// on does not have: the bootstrap only copies what lies above it.
    fn holds_block_data(&self, block_hash: &CryptoHash) -> Result<bool, ChainError> {
        if self.follow.is_none() {
            return Ok(true);
        }
        let block = match self.chain.get_block(block_hash) {
            Ok(block) => block,
            Err(ChainStoreError::DBNotFoundErr(_)) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let height = block.header().height();
        Ok(block
            .chunks()
            .iter()
            .filter(|chunk| chunk.height_included() == height)
            .all(|chunk| self.chain.get_chunk(&chunk.chunk_hash()).is_ok()))
    }

    /// Lets the appending task pull up to `read_ahead` blocks above `height`, the
    /// highest height the mirror has asked about.
    fn raise_pull_limit(&self, height: BlockHeight) {
        if let Some(follow) = &self.follow {
            follow.pull_limit.raise_to(height + follow.read_ahead);
        }
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
        // On a restart the reader head can be at `last_height`, and the wait below
        // needs the reader to pull past it.
        self.raise_pull_limit(last_height);

        let mut height = last_height + 1;
        let mut logged_wait = false;
        loop {
            let head = self.head_height().await?;
            if height > head {
                if !self.wait_for_blocks().await? {
                    return Ok(block_heights);
                }
                if !logged_wait {
                    tracing::info!(
                        target: "mirror",
                        height,
                        head,
                        "waiting for the source store to reach the next height to send"
                    );
                    logged_wait = true;
                }
                continue;
            }
            match self.chain.get_block_hash_by_height(height) {
                Ok(block_hash) if self.holds_block_data(&block_hash)? => {
                    block_heights.push(height);
                    break;
                }
                Ok(_) => {
                    tracing::info!(
                        target: "mirror",
                        height,
                        "the source store holds no chunks for this block; starting above it"
                    );
                    height += 1;
                }
                Err(ChainStoreError::DBNotFoundErr(_)) => {
                    height += 1;
                }
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("failed fetching block hash for #{}", height));
                }
            };
        }
        while block_heights.len() < num_initial_blocks {
            let last_height = *block_heights.iter().next_back().unwrap();
            match self.get_next_block_height(last_height).await {
                Ok(h) => block_heights.push(h),
                Err(ChainError::Unknown) => {
                    if !self.wait_for_blocks().await? {
                        break;
                    }
                }
                Err(e) => {
                    return Err(anyhow::Error::from(e)).with_context(|| {
                        format!("failed getting next block height after {}", last_height)
                    });
                }
            };
        }
        Ok(block_heights)
    }

    async fn block_height_to_hash(&self, height: BlockHeight) -> Result<CryptoHash, ChainError> {
        Ok(self.chain.get_block_hash_by_height(height)?)
    }

    async fn head_height(&self) -> Result<BlockHeight, ChainError> {
        self.complete_head()
    }

    fn source_grows(&self) -> bool {
        self.follow.is_some()
    }

    async fn get_txs(&self, height: BlockHeight) -> Result<SourceBlock, ChainError> {
        let block_hash = self.chain.get_block_hash_by_height(height)?;
        let block = self
            .chain
            .get_block(&block_hash)
            .with_context(|| format!("Can't get block {} at height {}", block_hash, height))?;

        let mut chunks = Vec::new();
        for chunk in block.chunks().iter() {
            let chunk = match self.chain.get_chunk(&chunk.chunk_hash()) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        shard_id = %chunk.shard_id(),
                        %height,
                        ?e,
                        "can't fetch source chain shard chunk at height, are we tracking all shards?"
                    );
                    continue;
                }
            };
            chunks.push(SourceChunk {
                shard_id: chunk.shard_id(),
                // TODO(ecc): decide how to mirror ECCs. They have no signer to re-sign with, so they are
                // skipped for now.
                transactions: chunk
                    .to_transactions()
                    .iter()
                    .filter_map(TransactionEnvelope::as_signed)
                    .cloned()
                    .collect(),
                receipts: chunk.prev_outgoing_receipts().to_vec(),
            })
        }
        Ok(SourceBlock { hash: block_hash, chunks })
    }

    async fn get_next_block_height(&self, height: BlockHeight) -> Result<BlockHeight, ChainError> {
        self.raise_pull_limit(height);
        let hash = self.chain.get_block_hash_by_height(height)?;
        let hash = self.chain.get_next_block_hash(&hash)?;
        let next_height = self.chain.get_block_header(&hash)?.height();
        // Not there yet, as far as the caller is concerned, when its chunks or the outcomes
        // of its receipts may not be.
        if self.follow.is_some() && next_height + OUTCOME_MARGIN > self.complete_head()? {
            return Err(ChainError::Unknown);
        }
        Ok(next_height)
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
        let mut rejected = Vec::new();
        for outcome in outcomes {
            if !outcome.outcome_with_id.outcome.status.has_changed_state() {
                rejected.push(outcome);
                continue;
            }
            let Ok(header) = self.chain.get_block_header(&outcome.block_hash) else { continue };
            if is_on_current_chain(&self.chain, &header).unwrap_or(false) {
                return Ok(outcome.into());
            }
        }
        let mut earliest: Option<(BlockHeight, _)> = None;
        for outcome in rejected {
            let Ok(header) = self.chain.get_block_header(&outcome.block_hash) else { continue };
            if !is_on_current_chain(&self.chain, &header).unwrap_or(false) {
                continue;
            }
            if earliest.as_ref().is_none_or(|(height, _)| header.height() < *height) {
                earliest = Some((header.height(), outcome));
            }
        }
        earliest.map(|(_, outcome)| outcome.into()).ok_or(ChainError::Unknown)
    }

    async fn get_receipt(&self, id: &CryptoHash) -> Result<Arc<Receipt>, ChainError> {
        self.chain.get_receipt(id).ok_or(ChainError::Unknown)
    }

    async fn get_full_access_keys(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> Result<Vec<PublicKeyHandle>, ChainError> {
        let mut ret = Vec::new();
        let header = self.chain.get_block_header(block_hash)?;
        let shard_id =
            account_id_to_shard_id(self.epoch_manager.as_ref(), account_id, header.epoch_id())
                .into_chain_error()?;
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, header.epoch_id())
            .into_chain_error()?;
        // A missing chunk extra is a `DBNotFoundErr`, which already maps to `ChainError::Unknown`.
        let chunk_extra = self.chain.get_chunk_extra(header.hash(), &shard_uid)?;
        let mut after_key = None;
        loop {
            let response = match self.runtime.query(
                shard_uid,
                chunk_extra.state_root(),
                header.height(),
                header.raw_timestamp(),
                header.prev_hash(),
                header.hash(),
                header.epoch_id(),
                &QueryRequest::ViewAccessKeyList {
                    account_id: account_id.clone(),
                    after_key,
                    limit: ACCESS_KEY_PAGE_SIZE,
                },
            ) {
                Ok(response) => response,
                Err(
                    QueryError::UnknownAccount { .. } | QueryError::UninitializedAccount { .. },
                ) => {
                    return Ok(ret);
                }
                // For `ViewAccessKeyList`, `InternalError` wraps a `StorageError` raised while
                // reading the trie (e.g. `MissingTrieValue`). On a store built without state
                // (`--source-skip-state`) that is expected at every block, and the caller
                // falls back; on any other store it is corruption or state garbage-collected
                // under the mirror, and it aborts the run like every other error.
                Err(QueryError::InternalError { error_message, .. }) if self.skip_state => {
                    tracing::debug!(
                        target: "mirror",
                        %account_id,
                        %block_hash,
                        %error_message,
                        "source store has no state for the access key query",
                    );
                    return Err(ChainError::NoState);
                }
                Err(e) => return Err(e.into()),
            };
            let QueryResponseKind::AccessKeyList(l) = response.kind else {
                unreachable!();
            };
            for k in l.keys {
                if k.access_key.permission == AccessKeyPermissionView::FullAccess {
                    ret.push(k.public_key);
                }
            }
            match l.last_key {
                Some(cursor) => after_key = Some(cursor),
                None => return Ok(ret),
            }
        }
    }
}
