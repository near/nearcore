use crate::replaydb::{ReplayDB, open_storage_for_replay};
use anyhow::{Context, Result, anyhow, bail};
use clap;
use itertools::Itertools;
use near_chain::chain::{
    NewChunkData, NewChunkResult, OldChunkData, OldChunkResult, ShardContext, StorageContext,
    collect_receipts_from_response,
};
use near_chain::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use near_chain::stateless_validation::chunk_endorsement::validate_chunk_endorsements_in_block;
use near_chain::stateless_validation::chunk_validation::apply_result_to_chunk_extra;
use near_chain::types::StorageDataSource;
use near_chain::update_shard::{ShardUpdateReason, ShardUpdateResult, process_shard_update};
use near_chain::validate::{validate_chunk_proofs, validate_chunk_with_chunk_extra};
use near_chain::{
    Block, BlockHeader, Chain, ChainStore, ChainStoreAccess, ReceiptFilter,
    get_incoming_receipts_for_shard,
};
use near_chain_configs::GenesisValidationMode;
use near_chunks::logic::make_outgoing_receipts_proofs;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader, ShardProof};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, Gas, ShardId};
use near_state_viewer::progress_reporter::ProgressReporter;
use near_store::{ShardUId, Store, get_genesis_state_roots};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt, load_config};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// This command assumes that it is run from an archival node
/// and not all the operations data that is available for a
/// regular validator might not be available in the archival database.
#[derive(clap::Parser)]
pub struct ReplayArchiveCommand {
    #[clap(long)]
    start_height: Option<BlockHeight>,
    #[clap(long)]
    end_height: Option<BlockHeight>,
}

impl ReplayArchiveCommand {
    pub fn run(self, home_dir: &Path, genesis_validation: GenesisValidationMode) -> Result<()> {
        let near_config = load_config(home_dir, genesis_validation)
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
    outgoing_receipts: Vec<Receipt>,
}

struct ReplayController {
    storage: Arc<ReplayDB>,
    chain_store: ChainStore,
    runtime: Arc<NightshadeRuntime>,
    epoch_manager: Arc<EpochManagerHandle>,
    progress_reporter: ProgressReporter,
    start_height: BlockHeight,
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
        let chain_store = ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        );

        let head_height = chain_store.head().context("Failed to get head of the chain")?.height;
        let start_height = start_height.unwrap_or(genesis_height);
        let end_height = end_height.unwrap_or(head_height).min(head_height);

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        let runtime =
            NightshadeRuntime::from_config(home_dir, store, &near_config, epoch_manager.clone())
                .context("Failed to create runtime")?;

        let progress_reporter = ProgressReporter {
            cnt: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            empty_blocks: AtomicU64::new(0),
            non_empty_blocks: AtomicU64::new(0),
            tgas_burned: AtomicU64::new(0),
            indicatif: near_state_viewer::progress_reporter::default_indicatif(
                (end_height + 1).checked_sub(start_height),
            ),
        };

        Ok(Self {
            storage,
            chain_store,
            runtime,
            epoch_manager,
            progress_reporter,
            start_height,
            next_height: start_height,
            end_height,
        })
    }

    fn init_start_block(&mut self) -> Result<()> {
        let block_hash =
            self.chain_store.get_block_hash_by_height(self.start_height).map_err(|e| {
                anyhow!("Failed to get block hash for start height {}: {:#}", self.start_height, e)
            })?;
        let block = self
            .chain_store
            .get_block(&block_hash)
            .context("Failed to get block for start height")?;
        if block.header().is_genesis() {
            // Generate and save chunk extras for the genesis block so that we use them in the next block.
            self.save_genesis_chunk_extras(&block)?;
        }
        Ok(())
    }

    /// Replays the next block if any. Returns true if there are still blocks to replay
    /// and false if it reached end block height.
    fn replay_next_block(&mut self) -> Result<bool> {
        if self.next_height > self.end_height {
            bail!("End height is reached");
        }
        if self.next_height == self.start_height {
            // Initialize the DB columns that are not archival but need to be there for the replay.
            self.init_start_block().context("Failed to initialize store")?;
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
        self.progress_reporter
            .inc_and_report_progress(self.next_height, total_gas_burnt.unwrap_or(0));
        self.next_height += 1;
        Ok(self.next_height <= self.end_height)
    }

    fn replay_block(&mut self, height: BlockHeight) -> Result<ReplayBlockOutput> {
        tracing::info!(target: "replay-archive", "Replaying block at height {}", self.next_height);

        let Ok(block_hash) = self.chain_store.get_block_hash_by_height(height) else {
            return Ok(ReplayBlockOutput::Missing(height));
        };

        let block = self.chain_store.get_block(&block_hash)?;

        self.validate_block(&block)?;

        self.update_epoch_manager(&block)?;
        if !block.header().is_genesis() {
            self.update_incoming_receipts(&block)?;
        }

        if block.header().is_genesis() {
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
            let epoch_id = block.header().epoch_id();
            let shard_id: ShardId = shard_id.try_into()?;
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, epoch_id)
                .context("Failed to get shard UID from shard id")?;
            let replay_output = self
                .replay_chunk(&block, &prev_block, shard_uid, chunk_header, prev_chunk_header)
                .context("Failed to replay the chunk")?;
            total_gas_burnt += replay_output.chunk_extra.gas_used();

            // Save chunk extra and outgoing receipts for future reads.
            let mut store_update = self.chain_store.store_update();
            store_update.save_chunk_extra(&block_hash, &shard_uid, replay_output.chunk_extra);
            store_update.save_outgoing_receipt(
                &block_hash,
                shard_id,
                replay_output.outgoing_receipts,
            );
            let _ = store_update.commit()?;
        }

        Ok(ReplayBlockOutput::Replayed(block, total_gas_burnt))
    }

    fn replay_chunk(
        &self,
        block: &Block,
        prev_block: &Block,
        shard_uid: ShardUId,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: &ShardChunkHeader,
    ) -> Result<ReplayChunkOutput> {
        let span = tracing::debug_span!(target: "replay-archive", "replay_chunk").entered();

        // Collect receipts and transactions.
        let chunk_hash = chunk_header.chunk_hash();
        let chunk = self
            .chain_store
            .get_chunk(&chunk_hash)
            .context("Failed to get chunk from chunk hash")?;

        let block_header = block.header();

        let prev_block_header = prev_block.header();
        let prev_block_hash = prev_block_header.hash();

        let prev_chunk_extra = self.chain_store.get_chunk_extra(prev_block_hash, &shard_uid)?;

        let height = block_header.height();
        let is_new_chunk: bool = chunk_header.is_new_chunk(height);

        self.validate_chunk(
            is_new_chunk,
            &chunk,
            chunk_header,
            prev_block_hash,
            prev_chunk_header,
            prev_chunk_extra.as_ref(),
        )?;

        let shard_id = shard_uid.shard_id();
        let shard_context = self.get_shard_context(shard_uid)?;

        let storage_context = StorageContext {
            storage_data_source: StorageDataSource::DbTrieOnly,
            state_patch: Default::default(),
        };

        let block_context =
            Chain::get_apply_chunk_block_context(block, prev_block.header(), is_new_chunk)?;

        let update_reason = if is_new_chunk {
            let receipts = self.collect_incoming_receipts(
                block_header,
                shard_id,
                prev_chunk_header.height_included(),
            )?;

            ShardUpdateReason::NewChunk(NewChunkData {
                chunk_header: chunk_header.clone(),
                transactions: chunk.to_transactions().to_vec(),
                // FIXME: see the `validate_chunk` thing above.
                transaction_validity_check_results: vec![true; chunk.to_transactions().len()],
                receipts,
                block: block_context,
                storage_context,
            })
        } else {
            ShardUpdateReason::OldChunk(OldChunkData {
                block: block_context,
                prev_chunk_extra: ChunkExtra::clone(prev_chunk_extra.as_ref()),
                storage_context,
            })
        };

        let shard_update_result =
            process_shard_update(&span, self.runtime.as_ref(), update_reason, shard_context)?;

        let output = match shard_update_result {
            ShardUpdateResult::NewChunk(NewChunkResult {
                gas_limit: _,
                shard_uid: _,
                apply_result,
            }) => {
                let outgoing_receipts = apply_result.outgoing_receipts.clone();
                let chunk_extra = apply_result_to_chunk_extra(apply_result, &chunk_header);
                ReplayChunkOutput { chunk_extra, outgoing_receipts }
            }
            ShardUpdateResult::OldChunk(OldChunkResult { shard_uid: _, apply_result }) => {
                let mut chunk_extra = ChunkExtra::clone(&prev_chunk_extra.as_ref());
                *chunk_extra.state_root_mut() = apply_result.new_root;
                let outgoing_receipts = apply_result.outgoing_receipts;
                ReplayChunkOutput { chunk_extra, outgoing_receipts }
            }
        };

        Ok(output)
    }

    /// Returns the incoming receipts to the given shard.
    fn collect_incoming_receipts(
        &self,
        block_header: &BlockHeader,
        shard_id: ShardId,
        prev_chunk_height_included: BlockHeight,
    ) -> Result<Vec<Receipt>> {
        let shard_layout =
            self.epoch_manager.get_shard_layout_from_prev_block(block_header.prev_hash())?;
        let receipt_response = get_incoming_receipts_for_shard(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            shard_id,
            &shard_layout,
            *block_header.hash(),
            prev_chunk_height_included,
            ReceiptFilter::TargetShard,
        )?;
        let receipts = collect_receipts_from_response(&receipt_response);
        Ok(receipts)
    }

    /// Validates a given block. The current set of checks may be extended later.
    fn validate_block(&self, block: &Block) -> Result<()> {
        // Chunk endorsements will only exist for a non-genesis block generated with stateless validation.
        if !block.header().is_genesis() {
            validate_chunk_endorsements_in_block(self.epoch_manager.as_ref(), block)?;
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
        // Check if the information in the ChunkExtra recorded after applying the previous chunk matches the information in the new chunk.
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
        // FIXME: should we be calling Chain::validate_chunk_transactions here as well?

        Ok(())
    }

    fn update_epoch_manager(&self, block: &Block) -> Result<()> {
        let last_finalized_height =
            self.chain_store.get_block_height(block.header().last_final_block())?;
        let store_update = self.epoch_manager.add_validator_proposals(
            BlockInfo::from_header(block.header(), last_finalized_height),
            *block.header().random_value(),
        )?;
        let _ = store_update.commit()?;
        Ok(())
    }

    pub fn update_incoming_receipts(&mut self, block: &Block) -> Result<()> {
        let block_height = block.header().height();
        let block_hash = block.header().hash();
        let mut receipt_proofs_by_shard_id: HashMap<ShardId, Vec<ReceiptProof>> = HashMap::new();
        for chunk_header in block.chunks().iter_deprecated() {
            if !chunk_header.is_new_chunk(block_height) {
                continue;
            }
            let chunk_hash = chunk_header.chunk_hash();
            let chunk = self
                .chain_store
                .get_chunk(&chunk_hash)
                .context("Failed to get chunk from chunk hash")?;
            for receipt in make_outgoing_receipts_proofs(
                chunk_header,
                chunk.prev_outgoing_receipts().to_vec(),
                self.epoch_manager.as_ref(),
            )? {
                let ReceiptProof(_, ref shard_proof) = receipt;
                let ShardProof { to_shard_id, .. } = shard_proof;
                receipt_proofs_by_shard_id
                    .entry(*to_shard_id)
                    .or_insert_with(Vec::new)
                    .push(receipt.clone());
            }
        }

        let mut store_update = self.chain_store.store_update();
        let receipts_shuffle_salt = get_receipts_shuffle_salt(self.epoch_manager.as_ref(), block)?;
        for (shard_id, mut receipts) in receipt_proofs_by_shard_id {
            shuffle_receipt_proofs(&mut receipts, receipts_shuffle_salt);
            store_update.save_incoming_receipt(&block_hash, shard_id, Arc::new(receipts));
        }
        store_update.commit().unwrap();
        Ok(())
    }

    /// Generates a ShardContext specific to replaying the blocks, which indicates that
    /// we care about all the shards and should always apply chunk.
    fn get_shard_context(&self, shard_uid: ShardUId) -> Result<ShardContext> {
        let shard_context = ShardContext { shard_uid, should_apply_chunk: true };
        Ok(shard_context)
    }

    /// Saves the ChunkExtras for the shards in the genesis block.
    /// Note that there are no chunks in the genesis block, so we directly generate the ChunkExtras
    /// from the information in the genesis block without applying any transactions or receipts.
    fn save_genesis_chunk_extras(&mut self, genesis_block: &Block) -> Result<()> {
        let state_roots = get_genesis_state_roots(&self.chain_store.store())?
            .ok_or_else(|| anyhow!("genesis state roots do not exist in the db".to_owned()))?;
        let mut store_update = self.chain_store.store_update();
        Chain::save_genesis_chunk_extras(
            genesis_block,
            &state_roots,
            self.epoch_manager.as_ref(),
            &mut store_update,
        )?;
        let _ = store_update.commit()?;
        Ok(())
    }
}
