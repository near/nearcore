use anyhow::{anyhow, bail, Context, Result};
use clap;
use near_chain::chain::{NewChunkData, OldChunkData, ShardContext, StorageContext};
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::StorageDataSource;
use near_chain::update_shard::{process_shard_update, ShardUpdateReason};
use near_chain::validate::validate_chunk_with_chunk_extra;
use near_chain::{Block, BlockHeader, Chain, ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_state_viewer::progress_reporter::{timestamp_ms, ProgressReporter};
use near_store::{Mode, NodeStorage, ShardUId, Store};
use nearcore::{load_config, NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(clap::Parser)]
pub struct ReplayArchiveCommand {
    #[clap(long)]
    start_height: Option<BlockHeight>,
    #[clap(long)]
    end_height: Option<BlockHeight>,
}

impl ReplayArchiveCommand {
    pub fn run(self, home_dir: &Path) -> Result<()> {
        let near_config = load_config(home_dir, GenesisValidationMode::Full)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        if !near_config.config.archive {
            bail!("This must be an archival node.".to_string());
        }
        if !near_config.config.cold_store.is_none() {
            bail!("Cold storage is not configured for the archival node.".to_string());
        }

        let mut controller =
            ReplayController::new(home_dir, near_config, self.start_height, self.end_height)?;

        while controller.replay_next_block()? {}
        Ok(())
    }
}

struct ReplayController {
    chain_store: ChainStore,
    runtime: Arc<NightshadeRuntime>,
    epoch_manager: Arc<EpochManagerHandle>,
    progress_reporter: ProgressReporter,
    next_height: BlockHeight,
    end_height: BlockHeight,
}

impl ReplayController {
    fn new(
        home_dir: &Path,
        near_config: NearConfig,
        start_height: Option<BlockHeight>,
        end_heigth: Option<BlockHeight>,
    ) -> Result<Self> {
        let store = Self::open_split_store_read_only(home_dir, &near_config)?;

        let genesis_height = near_config.genesis.config.genesis_height;
        let chain_store = ChainStore::new(store.clone(), genesis_height, false);

        let head_height = chain_store.head().context("Failed to get head of the chain")?.height;
        let tail_height = chain_store.tail().context("Failed to get tail of the chain")?;
        assert_eq!(
            tail_height, genesis_height,
            "Tail height of archival node must be equal to the genesis height."
        );
        assert!(
            tail_height <= head_height,
            "Head height must be greater or equal to the tail heigth."
        );

        let start_height = start_height.unwrap_or(tail_height);
        let end_height = end_heigth.unwrap_or(head_height);

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);

        let runtime =
            NightshadeRuntime::from_config(home_dir, store, &near_config, epoch_manager.clone())
                .context("Failed to create runtime")?;

        let progress_reporter = ProgressReporter {
            cnt: AtomicU64::new(0),
            ts: AtomicU64::new(timestamp_ms()),
            all: (end_height + 1).saturating_sub(start_height),
            skipped: AtomicU64::new(0),
            empty_blocks: AtomicU64::new(0),
            non_empty_blocks: AtomicU64::new(0),
            tgas_burned: AtomicU64::new(0),
        };

        Ok(Self {
            chain_store,
            runtime,
            epoch_manager,
            progress_reporter,
            next_height: start_height,
            end_height,
        })
    }

    fn open_split_store_read_only(home_dir: &Path, near_config: &NearConfig) -> Result<Store> {
        let opener = NodeStorage::opener(
            home_dir,
            near_config.client_config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );
        let storage = opener.open_in_mode(Mode::ReadOnly).context("Failed to open storage")?;
        match storage.get_split_store() {
            Some(store) => Ok(store),
            None => Err(anyhow!("Failed to get split store for archival node")),
        }
    }

    fn replay_next_block(&mut self) -> Result<bool> {
        if self.next_height > self.end_height {
            bail!("End height is reached");
        }
        self.replay_block(self.next_height)?;
        self.next_height += 1;
        Ok(self.next_height <= self.end_height)
    }

    fn replay_block(&mut self, height: BlockHeight) -> Result<()> {
        let block_hash = match self.chain_store.get_block_hash_by_height(height) {
            Ok(block_hash) => block_hash,
            Err(_) => {
                tracing::debug!("Skipping non-available block at height {}", height);
                self.progress_reporter.inc_and_report_progress(0);
                return Ok(());
            }
        };

        let block = self.chain_store.get_block(&block_hash)?;

        let is_genesis_block = block.header().is_genesis();
        if is_genesis_block {
            tracing::debug!("Skipping genesis block at height {}", height);
            self.progress_reporter.inc_and_report_progress(0);
            return Ok(());
        }

        let prev_block_hash = block.header().prev_hash();
        let prev_block = self
            .chain_store
            .get_block(prev_block_hash)
            .context("Failed to get previous block to determine gas price")?;

        let prev_chunk_headers =
            Chain::get_prev_chunk_headers(self.epoch_manager.as_ref(), &prev_block)?;

        let chunks = block.chunks();
        for shard_id in 0..chunks.len() {
            let chunk_header = &chunks[shard_id];
            let prev_chunk_header = &prev_chunk_headers[shard_id];
            self.replay_chunk(
                &block,
                &prev_block,
                shard_id.try_into()?,
                chunk_header,
                prev_chunk_header,
            )
            .context("Failed to replay the chunk")?;
        }

        Ok(())
    }

    fn replay_chunk(
        &mut self,
        block: &Block,
        prev_block: &Block,
        shard_id: ShardId,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: &ShardChunkHeader,
    ) -> Result<()> {
        let span = tracing::debug_span!(target: "replay_archive", "replay_chunk").entered();

        let block_header = block.header();
        let height = block_header.height();
        let epoch_id = block_header.epoch_id();

        let prev_block_header = prev_block.header();
        let prev_block_hash = prev_block_header.hash();

        let shard_uid = self
            .epoch_manager
            .shard_id_to_uid(shard_id, epoch_id)
            .context("Failed to get shard UID from shard id")?;

        let prev_chunk_extra = self.chain_store.get_chunk_extra(prev_block_hash, &shard_uid)?;
        validate_chunk_with_chunk_extra(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            prev_block_hash,
            prev_chunk_extra.as_ref(),
            prev_chunk_header.height_included(),
            chunk_header,
        )
        .context("Failed to validate chunk with chunk extra")?;
        // TODO: validate_chunk_transactions

        // Collect receipts and transactions.
        let chunk_hash = chunk_header.chunk_hash();
        let chunk = self
            .chain_store
            .get_chunk(&chunk_hash)
            .context("Failed to get chunk from chunk hash")?;

        let is_new_chunk: bool = chunk_header.is_new_chunk(height);

        let block_context = Chain::get_apply_chunk_block_context(
            self.epoch_manager.as_ref(),
            block,
            prev_block.header(),
            is_new_chunk,
        )?;

        let shard_context = self.get_shard_context(block_header, shard_uid)?;
        let resharding_state_roots = if shard_context.need_to_reshard {
            Some(Chain::get_resharding_state_roots(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                block,
                shard_id,
            )?)
        } else {
            None
        };

        let storage_context = StorageContext {
            storage_data_source: StorageDataSource::DbTrieOnly,
            state_patch: Default::default(),
        };

        let update_reason = if is_new_chunk {
            let is_first_block_with_chunk_of_version =
                check_if_block_is_first_with_chunk_of_version(
                    &self.chain_store,
                    self.epoch_manager.as_ref(),
                    prev_block_hash,
                    shard_id,
                )?;

            let receipts = vec![];
            ShardUpdateReason::NewChunk(NewChunkData {
                chunk_header: chunk_header.clone(),
                transactions: chunk.transactions().to_vec(),
                receipts,
                block: block_context,
                is_first_block_with_chunk_of_version,
                resharding_state_roots,
                storage_context,
            })
        } else {
            ShardUpdateReason::OldChunk(OldChunkData {
                block: block_context,
                prev_chunk_extra: ChunkExtra::clone(prev_chunk_extra.as_ref()),
                resharding_state_roots,
                storage_context,
            })
        };

        let _shard_update_result = process_shard_update(
            &span,
            self.runtime.as_ref(),
            self.epoch_manager.as_ref(),
            update_reason,
            shard_context,
        )?;

        Ok(())
    }

    fn get_shard_context(
        &self,
        block_header: &BlockHeader,
        shard_uid: ShardUId,
    ) -> Result<ShardContext> {
        let prev_hash = block_header.prev_hash();
        let should_reshard = self.epoch_manager.will_shard_layout_change(prev_hash)?;
        Ok(ShardContext {
            shard_uid,
            cares_about_shard_this_epoch: true,
            will_shard_layout_change: should_reshard,
            should_apply_chunk: true,
            need_to_reshard: should_reshard,
        })
    }
}
