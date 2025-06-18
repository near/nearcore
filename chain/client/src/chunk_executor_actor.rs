use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use itertools::Itertools as _;
use lru::LruCache;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_chain::chain::{
    ApplyChunksMode, NewChunkData, NewChunkResult, OldChunkResult, ShardContext, StorageContext,
    UpdateShardJob, do_apply_chunks, get_should_apply_chunk,
};
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::types::{ApplyChunkBlockContext, RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::{ShardUpdateReason, ShardUpdateResult, process_shard_update};
use near_chain::{
    Block, Chain, ChainGenesis, ChainStore, ChainUpdate, DoomslugThresholdMode, Error,
    collect_receipts, get_chunk_clone_from_header,
};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::block::Chunks;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::merklize;
use near_primitives::optimistic_block::{BlockToApply, CachedShardUpdateKey};
use near_primitives::receipt::Receipt;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardProof;
use near_primitives::types::{AccountId, EpochId, ShardId, ShardIndex};
use near_store::Store;
use node_runtime::SignedValidPeriodTransactions;
use tracing::instrument;

pub struct ChunkExecutorActor {
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Contains validator info about this node. This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    validator_signer: MutableValidatorSigner,
    shard_tracker: ShardTracker,
    network_adapter: PeerManagerAdapter,

    /// Receipts originating from block keyed by block hash.
    block_receipts_cache: LruCache<CryptoHash, HashMap<(ShardId, ShardId), ReceiptProof>>,
    /// Next block hashes keyed by block hash.
    next_block_hashes: LruCache<CryptoHash, Vec<CryptoHash>>,

    // Hash of the genesis block.
    genesis_hash: CryptoHash,
}

impl ChunkExecutorActor {
    pub fn new(
        store: Store,
        genesis: &ChainGenesis,
        genesis_hash: CryptoHash,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        validator_signer: MutableValidatorSigner,
        shard_tracker: ShardTracker,
        network_adapter: PeerManagerAdapter,
        block_receipts_cache_capacity: NonZeroUsize,
        next_block_hashes_cache_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            validator_signer,
            shard_tracker,
            network_adapter,
            block_receipts_cache: LruCache::new(block_receipts_cache_capacity),
            next_block_hashes: LruCache::new(next_block_hashes_cache_capacity),
            genesis_hash,
        }
    }
}

impl near_async::messaging::Actor for ChunkExecutorActor {}

/// Message with incoming receipts corresponding to the block.
/// Eventually this would be handled properly with data availability layer.
/// For now this is useful to do testing with test loop.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ExecutorIncomingReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proofs: Vec<ReceiptProof>,
}

/// Message that should be sent once block is processed to indicate that it's available for
/// execution.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ExecutorBlock {
    pub block_hash: CryptoHash,
}

impl Handler<ExecutorIncomingReceipts> for ChunkExecutorActor {
    fn handle(
        &mut self,
        ExecutorIncomingReceipts { block_hash, receipt_proofs }: ExecutorIncomingReceipts,
    ) {
        let block_receipts =
            self.block_receipts_cache.get_or_insert_mut(block_hash, || HashMap::new());
        for proof in receipt_proofs {
            block_receipts.insert((proof.1.from_shard_id, proof.1.to_shard_id), proof);
        }

        let me = self.validator_signer.get().map(|signer| signer.validator_id().clone());

        let Some(next_block_hashes) = self.next_block_hashes.get(&block_hash) else {
            // Next block wasn't processed yet.
            tracing::debug!(target: "chunk_executor", %block_hash, "no next block hash is available");
            return;
        };
        for next_block_hash in next_block_hashes.clone() {
            // TODO(spice): Avoid storing the same incoming receipts several times.. With many
            // forks we would be saving the same incoming receipts associated with different blocks
            // which is redundant.
            if let Err(err) =
                self.try_save_incoming_receipts(me.as_ref(), &block_hash, &next_block_hash)
            {
                tracing::warn!(target: "chunk_executor", %block_hash, ?err, "failed to save incoming receipts");
            }

            if let Err(err) = self.try_apply_chunks(&next_block_hash, me.as_ref()) {
                tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to apply chunk for block hash");
            };
        }
    }
}

impl Handler<ExecutorBlock> for ChunkExecutorActor {
    fn handle(&mut self, ExecutorBlock { block_hash }: ExecutorBlock) {
        let me = self.validator_signer.get().map(|signer| signer.validator_id().clone());
        // We may have received receipts before the corresponding block.
        let block = match self.chain_store.get_block(&block_hash) {
            Ok(block) => block,
            Err(err) => {
                tracing::error!(target: "chunk_executor", %block_hash, ?err, "failed to get block");
                return;
            }
        };
        let header = block.header();
        let prev_block_hash = header.prev_hash();

        self.next_block_hashes.get_or_insert_mut(*prev_block_hash, || Vec::new()).push(block_hash);

        if let Err(err) =
            self.try_save_incoming_receipts(me.as_ref(), &prev_block_hash, &block_hash)
        {
            tracing::warn!(target: "chunk_executor", %prev_block_hash, ?err, "failed to save incoming receipts");
        }

        if let Err(err) = self.try_apply_chunks(&block_hash, me.as_ref()) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to apply chunk for block hash");
        };
    }
}

impl ChunkExecutorActor {
    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%block_hash, ?me))]
    fn try_apply_chunks(
        &mut self,
        block_hash: &CryptoHash,
        me: Option<&AccountId>,
    ) -> Result<(), Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        let block = self.chain_store.get_block(block_hash)?;
        let header = block.header();
        let prev_block_hash = header.prev_hash();
        // Genesis block has no outgoing receipts.
        if *prev_block_hash != self.genesis_hash {
            for shard_id in self.epoch_manager.shard_ids(&epoch_id)? {
                let is_me = true;
                if self
                    .shard_tracker
                    .cares_about_shard_this_or_next_epoch(me, block_hash, shard_id, is_me)
                    && !self.chain_store.incoming_receipts_exist(&block_hash, shard_id)?
                {
                    tracing::debug!(target: "chunk_executor", %block_hash, %prev_block_hash, "missing receipts to apply all tracked chunks for a block");
                    return Ok(());
                }
            }
        }
        self.apply_chunks(me, block, SandboxStatePatch::default())
    }

    fn get_incoming_receipts(
        &self,
        block_hash: &CryptoHash,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error> {
        // Genesis block has no outgoing receipts.
        if *prev_block_hash == self.genesis_hash {
            return Ok(Arc::new(Vec::new()));
        }
        self.chain_store.get_incoming_receipts(block_hash, shard_id)
    }

    // Logic here is based on Chain::apply_chunk_preprocessing
    fn apply_chunks(
        &mut self,
        me: Option<&AccountId>,
        block: Arc<Block>,
        mut state_patch: SandboxStatePatch,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let header = block.header();
        let prev_hash = header.prev_hash();
        let prev_block = self.chain_store.get_block(prev_hash)?;

        let prev_chunk_headers = self.epoch_manager.get_prev_chunk_headers(&prev_block)?;

        let epoch_id = block.header().epoch_id();
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;

        let chunk_headers = &block.chunks();
        let mut jobs = Vec::new();
        // TODO(spice-resharding): Make sure shard logic is correct with resharding.
        for (shard_index, _prev_chunk_header) in prev_chunk_headers.iter().enumerate() {
            // XXX: This is a bit questionable -- sandbox state patching works
            // only for a single shard. This so far has been enough.
            let state_patch = state_patch.take();
            let shard_id = shard_layout.get_shard_id(shard_index)?;

            let is_new_chunk = true;
            let block_context = Chain::get_apply_chunk_block_context_from_block_header(
                block.header(),
                &chunk_headers,
                prev_block.header(),
                is_new_chunk,
            )?;

            // If we don't care about shard we wouldn't have relevant incoming receipts.
            let is_me = true;
            if !self
                .shard_tracker
                .cares_about_shard_this_or_next_epoch(me, block_hash, shard_id, is_me)
            {
                continue;
            }
            // TODO(spice-resharding): We may need to take resharding into account here.
            let receipt_proofs = self.get_incoming_receipts(block_hash, prev_hash, shard_id)?;
            let incoming_receipts = Some(&*receipt_proofs);

            let storage_context =
                StorageContext { storage_data_source: StorageDataSource::Db, state_patch };

            let cached_shard_update_key =
                Chain::get_cached_shard_update_key(&block_context, chunk_headers, shard_id)?;

            let job = self.get_update_shard_job(
                me,
                cached_shard_update_key,
                block_context,
                chunk_headers,
                shard_index,
                &prev_block,
                ApplyChunksMode::IsCaughtUp,
                incoming_receipts,
                storage_context,
            );
            match job {
                Ok(Some(job)) => jobs.push(job),
                Ok(None) => {}
                Err(e) => panic!("{e:?}"),
            }
        }

        let apply_result =
            do_apply_chunks(BlockToApply::Normal(*block.hash()), block.header().height(), jobs);
        let apply_result = apply_result.into_iter().map(|res| (res.0, res.2)).collect_vec();
        let results = apply_result.into_iter().map(|(shard_id, x)| {
            if let Err(err) = &x {
                tracing::warn!(target: "chunk_executor", ?shard_id, hash = %block.hash(), %err, "error in applying chunk for block");
            }
            x
        }).collect::<Result<Vec<_>, Error>>()?;

        for result in &results {
            let (shard_uid, apply_result) = match result {
                ShardUpdateResult::NewChunk(NewChunkResult {
                    shard_uid,
                    gas_limit: _,
                    apply_result,
                }) => (shard_uid, apply_result),
                ShardUpdateResult::OldChunk(OldChunkResult { shard_uid, apply_result }) => {
                    (shard_uid, apply_result)
                }
            };
            let shard_id = shard_uid.shard_id();
            let receipt_proofs = make_outgoing_receipts_proofs(
                &shard_layout,
                shard_id,
                apply_result.outgoing_receipts.clone(),
            )?;
            self.send_outgoing_receipts(*block_hash, receipt_proofs);
        }

        let mut chain_update = self.chain_update();
        let should_save_state_transition_data = false;
        chain_update.apply_chunk_postprocessing(
            &block,
            results,
            should_save_state_transition_data,
        )?;
        chain_update.commit()?;
        Ok(())
    }

    fn send_outgoing_receipts(&self, block_hash: CryptoHash, receipt_proofs: Vec<ReceiptProof>) {
        tracing::debug!(target: "chunk_executor", %block_hash, ?receipt_proofs, "sending outgoing receipts");
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::TestonlySpiceIncomingReceipts { block_hash, receipt_proofs },
        ));
    }

    fn get_update_shard_job(
        &self,
        me: Option<&AccountId>,
        cached_shard_update_key: CachedShardUpdateKey,
        block: ApplyChunkBlockContext,
        chunk_headers: &Chunks,
        shard_index: ShardIndex,
        prev_block: &Block,
        mode: ApplyChunksMode,
        incoming_receipts: Option<&Vec<ReceiptProof>>,
        storage_context: StorageContext,
    ) -> Result<Option<UpdateShardJob>, Error> {
        let prev_block_hash = prev_block.hash();
        let block_height = block.height;
        let _span =
            tracing::debug_span!(target: "chunk_executor", "get_update_shard_job", ?prev_block_hash, block_height)
                .entered();

        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let shard_id = shard_layout.get_shard_id(shard_index)?;
        let shard_context =
            self.get_shard_context(me, prev_block_hash, &epoch_id, shard_id, mode)?;

        if !shard_context.should_apply_chunk {
            return Ok(None);
        }

        let chunk_header = chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
        let shard_update_reason = {
            let chunk = get_chunk_clone_from_header(&self.chain_store, chunk_header)?;
            let tx_valid_list =
                self.chain_store.compute_transaction_validity(prev_block.header(), &chunk);
            let receipts = collect_receipts(incoming_receipts.unwrap());

            let shard_uid = &shard_context.shard_uid;
            let chunk_extra = self.chain_store.get_chunk_extra(prev_block_hash, shard_uid)?;
            let chunk_header = chunk_header.clone().into_spice_chunk_execution_header(&chunk_extra);

            let transactions =
                SignedValidPeriodTransactions::new(chunk.into_transactions(), tx_valid_list);
            ShardUpdateReason::NewChunk(NewChunkData {
                chunk_header,
                transactions,
                receipts,
                block,
                storage_context,
            })
        };

        let runtime = self.runtime_adapter.clone();
        Ok(Some((
            shard_id,
            cached_shard_update_key,
            Box::new(move |parent_span| -> Result<ShardUpdateResult, Error> {
                Ok(process_shard_update(
                    parent_span,
                    runtime.as_ref(),
                    shard_update_reason,
                    shard_context,
                )?)
            }),
        )))
    }

    fn get_shard_context(
        &self,
        me: Option<&AccountId>,
        prev_hash: &CryptoHash,
        epoch_id: &EpochId,
        shard_id: ShardId,
        mode: ApplyChunksMode,
    ) -> Result<ShardContext, Error> {
        let cares_about_shard_this_epoch =
            self.shard_tracker.cares_about_shard(me, prev_hash, shard_id, true);
        let cares_about_shard_next_epoch =
            self.shard_tracker.will_care_about_shard(me, prev_hash, shard_id, true);
        let cared_about_shard_prev_epoch =
            self.shard_tracker.cared_about_shard_in_prev_epoch(me, prev_hash, shard_id, true);
        let should_apply_chunk = get_should_apply_chunk(
            mode,
            cares_about_shard_this_epoch,
            cares_about_shard_next_epoch,
            cared_about_shard_prev_epoch,
        );
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, epoch_id)?;
        Ok(ShardContext { shard_uid, should_apply_chunk })
    }

    fn chain_update(&mut self) -> ChainUpdate {
        ChainUpdate::new(
            &mut self.chain_store,
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            // Since we don't produce blocks, this argument is irrelevant.
            DoomslugThresholdMode::NoApprovals,
        )
    }

    /// Returns keys from block_receipts_cache for prev_block_hash that correspond to all incoming
    /// receipts that we care about grouped by destination shard id.
    /// Returns None if some of the receipts are still missing.
    fn all_incoming_receipts_keys(
        &mut self,
        me: Option<&AccountId>,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
    ) -> Result<Option<Vec<(ShardId, ShardId)>>, Error> {
        let Some(block_receipts) = self.block_receipts_cache.get(prev_block_hash) else {
            return Ok(None);
        };
        let epoch_id = self.epoch_manager.get_epoch_id(prev_block_hash)?;
        let shard_ids = self.epoch_manager.shard_ids(&epoch_id)?;

        let mut keys = Vec::<(ShardId, ShardId)>::new();
        for to_shard_id in &shard_ids {
            let is_me = true;
            // We cannot filter out correctly when receiving receipts since we may receive them
            // before we know about the corresponding block and can decide which receipts we care
            // about.
            if !self.shard_tracker.cares_about_shard_this_or_next_epoch(
                me,
                &block_hash,
                *to_shard_id,
                is_me,
            ) {
                continue;
            }
            for from_shard_id in &shard_ids {
                if !block_receipts.contains_key(&(*from_shard_id, *to_shard_id)) {
                    return Ok(None);
                }
                keys.push((*from_shard_id, *to_shard_id));
            }
        }
        Ok(Some(keys))
    }

    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%prev_block_hash, %block_hash, ?me))]
    fn try_save_incoming_receipts(
        &mut self,
        me: Option<&AccountId>,
        prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let Some(receipt_keys) =
            self.all_incoming_receipts_keys(me, prev_block_hash, block_hash)?
        else {
            tracing::debug!(target: "chunk_executor", ?prev_block_hash, ?block_hash, "haven't received all receipts yet");
            return Ok(());
        };

        let mut block_receipts = self.block_receipts_cache.get(&prev_block_hash).unwrap().clone();
        let mut chain_update = self.chain_update();
        for keys in receipt_keys.chunk_by(|a, b| a.1 == b.1) {
            let to_shard_id = keys[0].1;
            let mut proofs = keys.iter().filter_map(|key| block_receipts.remove(key)).collect();
            let shuffle_salt = block_hash;
            shuffle_receipt_proofs(&mut proofs, shuffle_salt);

            tracing::debug!(target: "chunk_executor", %prev_block_hash, ?to_shard_id, ?proofs, "saving incoming receipts");
            chain_update.save_incoming_receipt(&block_hash, to_shard_id, Arc::new(proofs));
        }
        chain_update.commit()?;
        Ok(())
    }
}

/// Returns receipt proofs for specified chunk.
fn make_outgoing_receipts_proofs(
    shard_layout: &ShardLayout,
    shard_id: ShardId,
    outgoing_receipts: Vec<Receipt>,
) -> Result<Vec<ReceiptProof>, EpochError> {
    let hashes = Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)?;
    let (_root, proofs) = merklize(&hashes);

    let mut receipts_by_shard = Chain::group_receipts_by_shard(outgoing_receipts, &shard_layout)?;
    let mut result = vec![];
    for (proof_shard_index, proof) in proofs.into_iter().enumerate() {
        let proof_shard_id = shard_layout.get_shard_id(proof_shard_index)?;
        let receipts = receipts_by_shard.remove(&proof_shard_id).unwrap_or_else(Vec::new);
        let shard_proof =
            ShardProof { from_shard_id: shard_id, to_shard_id: proof_shard_id, proof };
        result.push(ReceiptProof(receipts, shard_proof));
    }
    Ok(result)
}
