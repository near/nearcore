use crate::replaydb::{open_storage_for_replay, ReplayDB};
use anyhow::{anyhow, bail, Context, Result};
use clap;
use itertools::Itertools;
use near_chain::chain::{
    collect_receipts_from_response, NewChunkData, NewChunkResult, OldChunkData, OldChunkResult,
    ShardContext, StorageContext,
};
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::stateless_validation::chunk_endorsement::validate_chunk_endorsements_in_block;
use near_chain::types::StorageDataSource;
use near_chain::update_shard::{process_shard_update, ShardUpdateReason, ShardUpdateResult};
use near_chain::validate::{
    validate_chunk_proofs, validate_chunk_with_chunk_extra, validate_transactions_order,
};
use near_chain::{Block, BlockHeader, Chain, ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::types::BlockHeaderInfo;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, Gas, ProtocolVersion, ShardId};
use near_primitives::version::ProtocolFeature;
use near_state_viewer::progress_reporter::{timestamp_ms, ProgressReporter};
use near_state_viewer::util::resulting_chunk_extra;
use near_store::{get_genesis_state_roots, ShardUId, Store};
use nearcore::{load_config, NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// This command assumes that it is run from an archival node
/// and not all the operations data that is available for a
/// regular validator may not be available in the archival database.
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
        if near_config.config.cold_store.is_none() {
            bail!("Cold storage is not configured for the archival node.".to_string());
        }

        let mut controller =
            ReplayController::new(home_dir, near_config, self.start_height, self.end_height)?;

        // Replay all the blocks until we reach the end block height.
        while controller.replay_next_block()? {}

        println!(
            "Columns read during replay: {}",
            controller.storage.get_columns_read().iter().join(", ")
        );
        println!(
            "Columns written during replay: {}",
            controller.storage.get_columns_written().iter().join(", ")
        );

        Ok(())
    }
}

/// Result of replaying a block. It is used to decide on
/// the right post-processing steps after replaying the block.
enum ReplayBlockOutput {
    Genesis(Block),
    Missing(BlockHeight),
    Replayed(Block, Gas),
}

/// Result of replaying a chunk.
struct ReplayChunkOutput {
    chunk_extra: ChunkExtra,
}

struct ReplayController {
    storage: Arc<ReplayDB>,
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
        end_height: Option<BlockHeight>,
    ) -> Result<Self> {
        let storage = open_storage_for_replay(home_dir, &near_config)?;
        let store = Store::new(storage.clone());

        let genesis_height = near_config.genesis.config.genesis_height;
        let chain_store = ChainStore::new(store.clone(), genesis_height, false);

        let head_height = chain_store.head().context("Failed to get head of the chain")?.height;
        let start_height = start_height.unwrap_or(genesis_height);
        let end_height = end_height.unwrap_or(head_height).min(head_height);

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
            storage,
            chain_store,
            runtime,
            epoch_manager,
            progress_reporter,
            next_height: start_height,
            end_height,
        })
    }

    /// Replays the next block if any. Returns true if there are still blocks to replay
    /// and false if it reached end block height.
    fn replay_next_block(&mut self) -> Result<bool> {
        if self.next_height > self.end_height {
            bail!("End height is reached");
        }
        let mut total_gas_burnt: Option<Gas> = None;
        match self.replay_block(self.next_height)? {
            ReplayBlockOutput::Genesis(block) => {
                tracing::debug!(target: "replay-archive", "Skipping genesis block at height {}", block.header().height());
            }
            ReplayBlockOutput::Missing(height) => {
                tracing::debug!(target: "replay-archive", "Skipping missing block at height {}", height);
            }
            ReplayBlockOutput::Replayed(block, gas_burnt) => {
                tracing::debug!(target: "replay-archive", "Replayed block at height {}", block.header().height());
                total_gas_burnt = Some(gas_burnt);
            }
        }
        self.progress_reporter.inc_and_report_progress(total_gas_burnt.unwrap_or(0));
        self.next_height += 1;
        Ok(self.next_height <= self.end_height)
    }

    fn replay_block(&mut self, height: BlockHeight) -> Result<ReplayBlockOutput> {
        tracing::info!(target: "replay-archive", "Replaying block at height {}", self.next_height);

        let Ok(block_hash) = self.chain_store.get_block_hash_by_height(height) else {
            return Ok(ReplayBlockOutput::Missing(height));
        };

        let block = self.chain_store.get_block(&block_hash)?;

        let epoch_id = block.header().epoch_id();
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;

        self.validate_block(&block, protocol_version)?;

        // TODO: This should be done after applying the chunks. However, running it before helps to
        // initialize BlockInfo and EpochInfo, which are needed to collect the receipts from previous
        // chunks. Revisit the logic for collecting the receipts and call this to after applying the chunks.
        self.update_epoch_manager(&block)?;

        if block.header().is_genesis() {
            // Save chunk extras for the genesis block.
            for (shard_uid, chunk_extra) in self.genesis_chunk_extras(&block)? {
                let mut store_update = self.chain_store.store_update();
                store_update.save_chunk_extra(&block.hash(), &shard_uid, chunk_extra);
                let _ = store_update.commit();
            }
            return Ok(ReplayBlockOutput::Genesis(block));
        }

        let prev_block_hash = block.header().prev_hash();
        let prev_block = self
            .chain_store
            .get_block(prev_block_hash)
            .context("Failed to get previous block to determine gas price")?;

        let prev_chunk_headers =
            Chain::get_prev_chunk_headers(self.epoch_manager.as_ref(), &prev_block)?;

        let chunks = block.chunks();
        let mut total_gas_burnt: u64 = 0;
        // TODO: Parallelize this loop.
        for shard_id in 0..chunks.len() {
            let chunk_header = &chunks[shard_id];
            let prev_chunk_header = &prev_chunk_headers[shard_id];
            let replay_output = self
                .replay_chunk(
                    &block,
                    &prev_block,
                    shard_id.try_into()?,
                    chunk_header,
                    prev_chunk_header,
                    protocol_version,
                )
                .context("Failed to replay the chunk")?;
            total_gas_burnt += replay_output.chunk_extra.gas_used();
        }

        Ok(ReplayBlockOutput::Replayed(block, total_gas_burnt))
    }

    fn replay_chunk(
        &mut self,
        block: &Block,
        prev_block: &Block,
        shard_id: ShardId,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: &ShardChunkHeader,
        protocol_version: ProtocolVersion,
    ) -> Result<ReplayChunkOutput> {
        let span = tracing::debug_span!(target: "replay-archive", "replay_chunk").entered();

        // Collect receipts and transactions.
        let chunk_hash = chunk_header.chunk_hash();
        let chunk = self
            .chain_store
            .get_chunk(&chunk_hash)
            .context("Failed to get chunk from chunk hash")?;

        let block_header = block.header();
        let block_hash = block_header.hash();

        let prev_block_header = prev_block.header();
        let prev_block_hash = prev_block_header.hash();

        let epoch_id = block_header.epoch_id();
        let shard_uid = self
            .epoch_manager
            .shard_id_to_uid(shard_id, epoch_id)
            .context("Failed to get shard UID from shard id")?;
        let prev_chunk_extra = self.chain_store.get_chunk_extra(prev_block_hash, &shard_uid)?;

        let height = block_header.height();
        let is_new_chunk: bool = chunk_header.is_new_chunk(height);

        self.validate_chunk(
            is_new_chunk,
            chunk.as_ref(),
            chunk_header,
            prev_block_hash,
            prev_chunk_header,
            prev_chunk_extra.as_ref(),
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

        let block_context = Chain::get_apply_chunk_block_context(
            self.epoch_manager.as_ref(),
            block,
            prev_block.header(),
            is_new_chunk,
        )?;

        let update_reason = if is_new_chunk {
            let receipts = self.collect_incoming_receipts(
                block_header,
                shard_id,
                prev_chunk_header.height_included(),
            )?;

            let is_first_block_with_chunk_of_version =
                check_if_block_is_first_with_chunk_of_version(
                    &self.chain_store,
                    self.epoch_manager.as_ref(),
                    prev_block_hash,
                    shard_id,
                )?;

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

        let shard_update_result = process_shard_update(
            &span,
            self.runtime.as_ref(),
            self.epoch_manager.as_ref(),
            update_reason,
            shard_context,
        )?;

        let output = match shard_update_result {
            ShardUpdateResult::NewChunk(NewChunkResult {
                gas_limit,
                shard_uid: _,
                apply_result,
                resharding_results: _,
            }) => {
                let chunk_extra = resulting_chunk_extra(&apply_result, gas_limit, protocol_version);
                ReplayChunkOutput { chunk_extra }
            }
            ShardUpdateResult::OldChunk(OldChunkResult {
                shard_uid: _,
                apply_result,
                resharding_results: _,
            }) => {
                let mut chunk_extra = ChunkExtra::clone(&prev_chunk_extra.as_ref());
                *chunk_extra.state_root_mut() = apply_result.new_root;
                ReplayChunkOutput { chunk_extra }
            }
            ShardUpdateResult::Resharding(_) => bail!("Unexpected apply result for resharding"),
        };

        // Save chunk extra.
        let mut store_update = self.chain_store.store_update();
        store_update.save_chunk_extra(&block_hash, &shard_uid, output.chunk_extra.clone());
        let _ = store_update.commit()?;

        Ok(output)
    }

    /// Returns the incoming receipts to the given shard.
    fn collect_incoming_receipts(
        &self,
        block_header: &BlockHeader,
        shard_id: ShardId,
        prev_chunk_height_included: BlockHeight,
    ) -> Result<Vec<Receipt>> {
        let receipt_response = &self.chain_store.get_incoming_receipts_for_shard(
            self.epoch_manager.as_ref(),
            shard_id,
            *block_header.hash(),
            prev_chunk_height_included,
        )?;
        let receipts = collect_receipts_from_response(receipt_response);
        Ok(receipts)
    }

    /// Validates a given block. The current set of checks may be extended later.
    fn validate_block(&self, block: &Block, protocol_version: ProtocolVersion) -> Result<()> {
        // Chunk endorsements will only exist for a non-genesis block generated with stateless validation.
        if !block.header().is_genesis() {
            if ProtocolFeature::StatelessValidation.enabled(protocol_version) {
                validate_chunk_endorsements_in_block(self.epoch_manager.as_ref(), block)?;
            }
        }
        Ok(())
    }

    /// Validates a given chunk. The current set of checks may be extended later.
    fn validate_chunk(
        &self,
        is_new_chunk: bool,
        chunk: &ShardChunk,
        chunk_header: &ShardChunkHeader,
        prev_block_hash: &CryptoHash,
        prev_chunk_header: &ShardChunkHeader,
        prev_chunk_extra: &ChunkExtra,
    ) -> Result<()> {
        if is_new_chunk {
            validate_chunk_with_chunk_extra(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                prev_block_hash,
                prev_chunk_extra,
                prev_chunk_header.height_included(),
                chunk_header,
            )
            .context("Failed to validate chunk with chunk extra")?;
        }
        if !validate_chunk_proofs(&chunk, self.epoch_manager.as_ref())
            .context("Failed to validate the chunk proofs")?
        {
            bail!("Failed to validate chunk proofs");
        }
        if !validate_transactions_order(chunk.transactions()) {
            bail!("Failed to validate transactions order in the chunk");
        }
        Ok(())
    }

    fn update_epoch_manager(&mut self, block: &Block) -> Result<()> {
        let last_finalized_height =
            self.chain_store.get_block_height(block.header().last_final_block())?;
        let store_update = self
            .epoch_manager
            .add_validator_proposals(BlockHeaderInfo::new(block.header(), last_finalized_height))?;
        let _ = store_update.commit()?;
        Ok(())
    }

    /// Generates a ShardContext specific to replaying the blocks, which indicates that
    /// we care about all the shards and should always apply chunk.
    fn get_shard_context(
        &self,
        block_header: &BlockHeader,
        shard_uid: ShardUId,
    ) -> Result<ShardContext> {
        let prev_hash = block_header.prev_hash();
        let should_reshard = self.epoch_manager.will_shard_layout_change(prev_hash)?;
        let shard_context = ShardContext {
            shard_uid,
            cares_about_shard_this_epoch: true,
            will_shard_layout_change: should_reshard,
            should_apply_chunk: true,
            need_to_reshard: should_reshard,
        };
        Ok(shard_context)
    }

    fn genesis_chunk_extras(&self, genesis_block: &Block) -> Result<Vec<(ShardUId, ChunkExtra)>> {
        let epoch_id = genesis_block.header().epoch_id();
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let congestion_infos = genesis_block.block_congestion_info();
        let state_roots = get_genesis_state_roots(self.chain_store.store())?
            .ok_or_else(|| anyhow!("genesis state roots do not exist in the db".to_owned()))?;

        let mut chunk_extras = vec![];
        let chunks = genesis_block.chunks();
        for shard_id in 0..chunks.len() {
            let chunk_header = &chunks[shard_id];
            let state_root = state_roots
                .get(shard_id)
                .ok_or_else(|| anyhow!("genesis state root does not exist for shard {shard_id}"))?;
            let congestion_info =
                congestion_infos.get(&(shard_id as u64)).map(|info| info.congestion_info);
            let shard_uid = self
                .epoch_manager
                .shard_id_to_uid(shard_id.try_into()?, epoch_id)
                .context("Failed to get shard UID from shard id")?;
            chunk_extras.push((
                shard_uid,
                ChunkExtra::new(
                    protocol_version,
                    state_root,
                    CryptoHash::default(),
                    vec![],
                    0,
                    chunk_header.gas_limit(),
                    0,
                    congestion_info,
                ),
            ));
        }
        Ok(chunk_extras)
    }
}
