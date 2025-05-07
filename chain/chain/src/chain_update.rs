use crate::approval_verification::verify_approvals_and_threshold_orphan;
use crate::block_processing_utils::BlockPreprocessInfo;
use crate::chain::collect_receipts_from_response;
use crate::metrics::{SHARD_LAYOUT_NUM_SHARDS, SHARD_LAYOUT_VERSION};
use crate::store::utils::get_block_header_on_chain_by_height;
use crate::store::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
use crate::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, RuntimeAdapter,
    RuntimeStorageConfig,
};
use crate::update_shard::{NewChunkResult, OldChunkResult, ShardUpdateResult};
use crate::{Chain, Doomslug};
use crate::{DoomslugThresholdMode, metrics};
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::block::{Block, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ShardChunk;
use near_primitives::state_sync::{ReceiptProofResponse, ShardStateSyncResponseHeader};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::views::LightClientBlockView;
use node_runtime::SignedValidPeriodTransactions;
use std::sync::Arc;
use tracing::{debug, warn};

/// Chain update helper, contains information that is needed to process block
/// and decide to accept it or reject it.
/// If rejected nothing will be updated in underlying storage.
/// Safe to stop process mid way (Ctrl+C or crash).
pub struct ChainUpdate<'a> {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    chain_store_update: ChainStoreUpdate<'a>,
    doomslug_threshold_mode: DoomslugThresholdMode,
}

impl<'a> ChainUpdate<'a> {
    pub fn new(
        chain_store: &'a mut ChainStore,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        doomslug_threshold_mode: DoomslugThresholdMode,
    ) -> Self {
        let chain_store_update: ChainStoreUpdate<'_> = chain_store.store_update();
        Self::new_impl(epoch_manager, runtime_adapter, doomslug_threshold_mode, chain_store_update)
    }

    fn new_impl(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        doomslug_threshold_mode: DoomslugThresholdMode,
        chain_store_update: ChainStoreUpdate<'a>,
    ) -> Self {
        ChainUpdate { epoch_manager, runtime_adapter, chain_store_update, doomslug_threshold_mode }
    }

    /// Commit changes to the chain into the database.
    pub fn commit(self) -> Result<(), Error> {
        self.chain_store_update.commit()
    }

    pub(crate) fn apply_chunk_postprocessing(
        &mut self,
        block: &Block,
        apply_results: Vec<ShardUpdateResult>,
        should_save_state_transition_data: bool,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "chain", "apply_chunk_postprocessing", height=block.header().height()).entered();
        Self::bandwidth_scheduler_state_sanity_check(&apply_results);
        for result in apply_results {
            self.process_apply_chunk_result(block, result, should_save_state_transition_data)?;
        }
        Ok(())
    }

    /// Process results of applying chunk
    fn process_apply_chunk_result(
        &mut self,
        block: &Block,
        result: ShardUpdateResult,
        should_save_state_transition_data: bool,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let prev_hash = block.header().prev_hash();
        let height = block.header().height();
        match result {
            ShardUpdateResult::NewChunk(NewChunkResult { gas_limit, shard_uid, apply_result }) => {
                let (outcome_root, outcome_paths) =
                    ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
                let shard_id = shard_uid.shard_id();

                // Save state root after applying transactions.
                self.chain_store_update.save_chunk_extra(
                    block_hash,
                    &shard_uid,
                    ChunkExtra::new(
                        &apply_result.new_root,
                        outcome_root,
                        apply_result.validator_proposals,
                        apply_result.total_gas_burnt,
                        gas_limit,
                        apply_result.total_balance_burnt,
                        apply_result.congestion_info,
                        apply_result.bandwidth_requests,
                    ),
                );

                let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
                let store_update = flat_storage_manager.save_flat_state_changes(
                    *block_hash,
                    *prev_hash,
                    height,
                    shard_uid,
                    apply_result.trie_changes.state_changes(),
                )?;
                self.chain_store_update.merge(store_update.into());

                self.chain_store_update.save_trie_changes(*block_hash, apply_result.trie_changes);
                self.chain_store_update.save_outgoing_receipt(
                    block_hash,
                    shard_id,
                    apply_result.outgoing_receipts,
                );
                // Save receipt and transaction results.
                self.chain_store_update.save_outcomes_with_proofs(
                    block_hash,
                    shard_id,
                    apply_result.outcomes,
                    outcome_paths,
                );
                if should_save_state_transition_data {
                    self.chain_store_update.save_state_transition_data(
                        *block_hash,
                        shard_id,
                        apply_result.proof,
                        apply_result.applied_receipts_hash,
                        apply_result.contract_updates,
                    );
                }
                self.chain_store_update.save_chunk_apply_stats(
                    *block_hash,
                    shard_id,
                    apply_result.stats,
                );
            }
            ShardUpdateResult::OldChunk(OldChunkResult { shard_uid, apply_result }) => {
                // The chunk is missing but some fields may need to be updated
                // anyway. Prepare a chunk extra as a copy of the old chunk
                // extra and apply changes to it.
                let old_extra = self.chain_store_update.get_chunk_extra(prev_hash, &shard_uid)?;
                let mut new_extra = ChunkExtra::clone(&old_extra);
                *new_extra.state_root_mut() = apply_result.new_root;

                let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
                let store_update = flat_storage_manager.save_flat_state_changes(
                    *block_hash,
                    *prev_hash,
                    height,
                    shard_uid,
                    apply_result.trie_changes.state_changes(),
                )?;
                self.chain_store_update.merge(store_update.into());

                self.chain_store_update.save_chunk_extra(block_hash, &shard_uid, new_extra);
                self.chain_store_update.save_trie_changes(*block_hash, apply_result.trie_changes);
                if should_save_state_transition_data {
                    self.chain_store_update.save_state_transition_data(
                        *block_hash,
                        shard_uid.shard_id(),
                        apply_result.proof,
                        apply_result.applied_receipts_hash,
                        apply_result.contract_updates,
                    );
                }
                self.chain_store_update.save_chunk_apply_stats(
                    *block_hash,
                    shard_uid.shard_id(),
                    apply_result.stats,
                );
            }
        };
        Ok(())
    }

    /// Extra sanity check for bandwidth scheduler - the scheduler state should be the same on all shards.
    fn bandwidth_scheduler_state_sanity_check(apply_results: &[ShardUpdateResult]) {
        let state_hashes: Vec<CryptoHash> = apply_results
            .iter()
            .map(|r| match r {
                ShardUpdateResult::NewChunk(new_res) => {
                    new_res.apply_result.bandwidth_scheduler_state_hash
                }
                ShardUpdateResult::OldChunk(old_res) => {
                    old_res.apply_result.bandwidth_scheduler_state_hash
                }
            })
            .collect();
        for hash in &state_hashes {
            assert_eq!(*hash, state_hashes[0]);
        }
    }

    /// This is the last step of process_block_single, where we take the preprocess block info
    /// apply chunk results and store the results on chain.
    #[tracing::instrument(
        level = "debug",
        target = "chain",
        "ChainUpdate::postprocess_block",
        skip_all
    )]
    pub(crate) fn postprocess_block(
        &mut self,
        block: &Block,
        block_preprocess_info: BlockPreprocessInfo,
        apply_chunks_results: Vec<(ShardId, Result<ShardUpdateResult, Error>)>,
        should_save_state_transition_data: bool,
    ) -> Result<Option<Tip>, Error> {
        let prev_hash = block.header().prev_hash();
        let results = apply_chunks_results.into_iter().map(|(shard_id, x)| {
            if let Err(err) = &x {
                warn!(target: "chain", ?shard_id, hash = %block.hash(), %err, "Error in applying chunk for block");
            }
            x
        }).collect::<Result<Vec<_>, Error>>()?;
        self.apply_chunk_postprocessing(block, results, should_save_state_transition_data)?;

        let BlockPreprocessInfo { is_caught_up, state_sync_info, incoming_receipts, .. } =
            block_preprocess_info;

        if !is_caught_up {
            debug!(target: "chain", %prev_hash, hash = %*block.hash(), "Add block to catch up");
            self.chain_store_update.add_block_to_catchup(*prev_hash, *block.hash());
        }

        for (shard_id, receipt_proofs) in incoming_receipts {
            self.chain_store_update.save_incoming_receipt(
                block.hash(),
                shard_id,
                Arc::new(receipt_proofs),
            );
        }
        if let Some(state_sync_info) = state_sync_info {
            self.chain_store_update.add_state_sync_info(state_sync_info);
        }

        self.chain_store_update.save_block_header(block.header().clone())?;
        self.update_header_head(block.header())?;

        // If block checks out, record validator proposals for given block.
        let last_final_block = block.header().last_final_block();
        let last_finalized_height = if last_final_block == &CryptoHash::default() {
            self.chain_store_update.get_genesis_height()
        } else {
            self.chain_store_update.get_block_header(last_final_block)?.height()
        };

        let epoch_manager_update = self.epoch_manager.add_validator_proposals(
            BlockInfo::from_header(block.header(), last_finalized_height),
            *block.header().random_value(),
        )?;
        self.chain_store_update.merge(epoch_manager_update);

        // Add validated block to the db, even if it's not the canonical fork.
        self.chain_store_update.save_block(block.clone());
        self.chain_store_update.inc_block_refcount(prev_hash)?;

        // Update the chain head if it's the new tip
        let res = self.update_head(block.header())?;

        if res.is_some() {
            // On the epoch switch record the epoch light client block
            // Note that we only do it if `res.is_some()`, i.e. if the current block is the head.
            // This is necessary because the computation of the light client block relies on
            // `ColNextBlockHash`-es populated, and they are only populated for the canonical
            // chain. We need to be careful to avoid a situation when the first block of the epoch
            // never becomes a tip of the canonical chain.
            // Presently the epoch boundary is defined by the height, and the fork choice rule
            // is also just height, so the very first block to cross the epoch end is guaranteed
            // to be the head of the chain, and result in the light client block produced.
            let prev = self.chain_store_update.get_previous_header(block.header())?;
            let prev_epoch_id = *prev.epoch_id();
            if block.header().epoch_id() != &prev_epoch_id {
                if prev.last_final_block() != &CryptoHash::default() {
                    let light_client_block = self.create_light_client_block(&prev)?;
                    self.chain_store_update
                        .save_epoch_light_client_block(&prev_epoch_id.0, light_client_block);
                }
            }

            let shard_layout = self.epoch_manager.get_shard_layout_from_prev_block(prev.hash())?;
            SHARD_LAYOUT_VERSION.set(shard_layout.version() as i64);
            SHARD_LAYOUT_NUM_SHARDS.set(shard_layout.shard_ids().count() as i64);
        }
        Ok(res)
    }

    pub fn create_light_client_block(
        &mut self,
        header: &BlockHeader,
    ) -> Result<LightClientBlockView, Error> {
        // First update the last next_block, since it might not be set yet
        self.chain_store_update.save_next_block_hash(header.prev_hash(), *header.hash());

        Chain::create_light_client_block(
            header,
            self.epoch_manager.as_ref(),
            &mut self.chain_store_update,
        )
    }

    #[allow(dead_code)]
    fn verify_orphan_header_approvals(&self, header: &BlockHeader) -> Result<(), Error> {
        let prev_hash = header.prev_hash();
        let prev_height = match header.prev_height() {
            None => {
                // this will accept orphans of V1 and V2
                // TODO: reject header V1 and V2 after a certain height
                return Ok(());
            }
            Some(prev_height) => prev_height,
        };
        let height = header.height();
        let epoch_id = header.epoch_id();
        let approvals = header.approvals();
        let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
        verify_approvals_and_threshold_orphan(
            &|approvals, stakes| {
                Doomslug::can_approved_block_be_produced(
                    self.doomslug_threshold_mode,
                    approvals,
                    stakes,
                )
            },
            prev_hash,
            prev_height,
            height,
            approvals,
            epoch_info,
        )
    }

    /// Update the header head if this header has most work.
    pub(crate) fn update_header_head(
        &mut self,
        header: &BlockHeader,
    ) -> Result<Option<Tip>, Error> {
        let header_head = self.chain_store_update.header_head()?;
        if header.height() > header_head.height {
            let tip = Tip::from_header(header);
            self.chain_store_update.save_header_head(&tip)?;
            debug!(target: "chain", "Header head updated to {} at {}", tip.last_block_hash, tip.height);
            metrics::HEADER_HEAD_HEIGHT.set(tip.height as i64);

            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    fn update_final_head_from_block(&mut self, header: &BlockHeader) -> Result<Option<Tip>, Error> {
        let final_head = self.chain_store_update.final_head()?;
        let last_final_block_header =
            match self.chain_store_update.get_block_header(header.last_final_block()) {
                Ok(final_header) => final_header,
                Err(Error::DBNotFoundErr(_)) => return Ok(None),
                Err(err) => return Err(err),
            };
        if last_final_block_header.height() > final_head.height {
            let tip = Tip::from_header(&last_final_block_header);
            self.chain_store_update.save_final_head(&tip)?;
            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// Directly updates the head if we've just appended a new block to it or handle
    /// the situation where the block has higher height to have a fork
    fn update_head(&mut self, header: &BlockHeader) -> Result<Option<Tip>, Error> {
        // if we made a fork with higher height than the head (which should also be true
        // when extending the head), update it
        self.update_final_head_from_block(header)?;
        let head = self.chain_store_update.head()?;
        if header.height() > head.height {
            let tip = Tip::from_header(header);

            self.chain_store_update.save_body_head(&tip)?;
            metrics::BLOCK_HEIGHT_HEAD.set(tip.height as i64);
            metrics::BLOCK_ORDINAL_HEAD.set(header.block_ordinal() as i64);
            debug!(target: "chain", "Head updated to {} at {}", tip.last_block_hash, tip.height);
            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// This method is called when the state sync is finished for a shard. It
    /// applies the chunk at the height included of the chunk in the sync hash
    /// and stores the results in the db.
    pub fn set_state_finalize(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        shard_state_header: ShardStateSyncResponseHeader,
    ) -> Result<ShardUId, Error> {
        let _span =
            tracing::debug_span!(target: "sync", "chain_update_set_state_finalize", ?shard_id, ?sync_hash).entered();
        let (chunk, incoming_receipts_proofs) = match shard_state_header {
            ShardStateSyncResponseHeader::V1(shard_state_header) => (
                ShardChunk::V1(shard_state_header.chunk),
                shard_state_header.incoming_receipts_proofs,
            ),
            ShardStateSyncResponseHeader::V2(shard_state_header) => {
                (shard_state_header.chunk, shard_state_header.incoming_receipts_proofs)
            }
        };

        // Note that block headers are already synced and can be taken
        // from store on disk.
        let block_header = get_block_header_on_chain_by_height(
            &self.chain_store_update.chain_store(),
            &sync_hash,
            chunk.height_included(),
        )?;

        // Getting actual incoming receipts.
        let mut receipt_proof_responses: Vec<ReceiptProofResponse> = vec![];
        for incoming_receipt_proof in &incoming_receipts_proofs {
            let ReceiptProofResponse(hash, _) = incoming_receipt_proof;
            let block_header = self.chain_store_update.get_block_header(hash)?;
            if block_header.height() <= chunk.height_included() {
                receipt_proof_responses.push(incoming_receipt_proof.clone());
            }
        }
        let receipts = collect_receipts_from_response(&receipt_proof_responses);
        let is_genesis = block_header.height() == self.chain_store_update.get_genesis_height();
        let prev_block_header = (!is_genesis)
            .then(|| self.chain_store_update.get_block_header(block_header.prev_hash()))
            .transpose()?;

        // Prev block header should be present during state sync, since headers have been synced at
        // this point, except for genesis.
        let gas_price = if let Some(prev_block_header) = &prev_block_header {
            prev_block_header.next_gas_price()
        } else {
            block_header.next_gas_price()
        };

        let chunk_header = chunk.cloned_header();
        let gas_limit = chunk_header.gas_limit();
        let block = self.chain_store_update.get_block(block_header.hash())?;
        let transactions = chunk.to_transactions().to_vec();
        let transaction_validity = if let Some(prev_block_header) = prev_block_header {
            self.chain_store_update
                .chain_store()
                .compute_transaction_validity(&prev_block_header, &chunk)
        } else {
            vec![true; transactions.len()]
        };
        let transactions = SignedValidPeriodTransactions::new(transactions, transaction_validity);
        let apply_result = self.runtime_adapter.apply_chunk(
            RuntimeStorageConfig::new(chunk_header.prev_state_root(), true),
            ApplyChunkReason::UpdateTrackedShard,
            ApplyChunkShardContext {
                shard_id,
                gas_limit,
                last_validator_proposals: chunk_header.prev_validator_proposals(),
                is_new_chunk: true,
            },
            ApplyChunkBlockContext {
                height: chunk_header.height_included(),
                block_hash: *block_header.hash(),
                prev_block_hash: *chunk_header.prev_block_hash(),
                block_timestamp: block_header.raw_timestamp(),
                gas_price,
                random_seed: *block_header.random_value(),
                congestion_info: block.block_congestion_info(),
                bandwidth_requests: block.block_bandwidth_requests(),
            },
            &receipts,
            transactions,
        )?;

        let (outcome_root, outcome_proofs) =
            ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);

        self.chain_store_update.save_chunk(chunk);

        let shard_uid =
            shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, block_header.epoch_id())?;
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        let store_update = flat_storage_manager.save_flat_state_changes(
            *block_header.hash(),
            *chunk_header.prev_block_hash(),
            chunk_header.height_included(),
            shard_uid,
            apply_result.trie_changes.state_changes(),
        )?;
        self.chain_store_update.merge(store_update.into());

        self.chain_store_update.save_trie_changes(*block_header.hash(), apply_result.trie_changes);

        let chunk_extra = ChunkExtra::new(
            &apply_result.new_root,
            outcome_root,
            apply_result.validator_proposals,
            apply_result.total_gas_burnt,
            gas_limit,
            apply_result.total_balance_burnt,
            apply_result.congestion_info,
            apply_result.bandwidth_requests,
        );
        self.chain_store_update.save_chunk_extra(block_header.hash(), &shard_uid, chunk_extra);

        self.chain_store_update.save_outgoing_receipt(
            block_header.hash(),
            shard_id,
            apply_result.outgoing_receipts,
        );
        // Saving transaction results.
        self.chain_store_update.save_outcomes_with_proofs(
            block_header.hash(),
            shard_id,
            apply_result.outcomes,
            outcome_proofs,
        );
        // Saving all incoming receipts.
        for receipt_proof_response in receipt_proof_responses {
            self.chain_store_update.save_incoming_receipt(
                &receipt_proof_response.0,
                shard_id,
                receipt_proof_response.1,
            );
        }
        Ok(shard_uid)
    }

    /// This method is called when the state sync is finished for a shard. It is
    /// used for applying chunks from after the height included, up until the
    /// sync hash, and storing the results. Those chunks are old (missing).
    pub fn set_state_finalize_on_height(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<bool, Error> {
        let _span =
            tracing::debug_span!(target: "sync", "set_state_finalize_on_height", height, ?shard_id)
                .entered();
        // Note that block headers are already synced and can be taken
        // from store on disk.
        let block_header_result = get_block_header_on_chain_by_height(
            &self.chain_store_update.chain_store(),
            &sync_hash,
            height,
        );
        if let Err(_) = block_header_result {
            // No such height, go ahead.
            return Ok(true);
        }
        let block_header = block_header_result?;
        if block_header.hash() == &sync_hash {
            // Don't continue
            return Ok(false);
        }
        let block = self.chain_store_update.get_block(block_header.hash())?;

        let prev_hash = block_header.prev_hash();
        let prev_block_header = self.chain_store_update.get_block_header(prev_hash)?;

        let shard_uid =
            shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, block_header.epoch_id())?;
        let chunk_extra = self.chain_store_update.get_chunk_extra(prev_hash, &shard_uid)?;

        let apply_result = self.runtime_adapter.apply_chunk(
            RuntimeStorageConfig::new(*chunk_extra.state_root(), true),
            ApplyChunkReason::UpdateTrackedShard,
            ApplyChunkShardContext {
                shard_id,
                last_validator_proposals: chunk_extra.validator_proposals(),
                gas_limit: chunk_extra.gas_limit(),
                is_new_chunk: false,
            },
            ApplyChunkBlockContext::from_header(
                &block_header,
                prev_block_header.next_gas_price(),
                block.block_congestion_info(),
                block.block_bandwidth_requests(),
            ),
            &[],
            SignedValidPeriodTransactions::empty(),
        )?;
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        let store_update = flat_storage_manager.save_flat_state_changes(
            *block_header.hash(),
            *prev_block_header.hash(),
            height,
            shard_uid,
            apply_result.trie_changes.state_changes(),
        )?;
        self.chain_store_update.merge(store_update.into());
        self.chain_store_update.save_trie_changes(*block_header.hash(), apply_result.trie_changes);

        // The chunk is missing but some fields may need to be updated
        // anyway. Prepare a chunk extra as a copy of the old chunk
        // extra and apply changes to it.
        let mut new_chunk_extra = ChunkExtra::clone(&chunk_extra);
        *new_chunk_extra.state_root_mut() = apply_result.new_root;

        self.chain_store_update.save_chunk_extra(block_header.hash(), &shard_uid, new_chunk_extra);
        Ok(true)
    }
}
