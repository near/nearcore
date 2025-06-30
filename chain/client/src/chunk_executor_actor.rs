use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use itertools::Itertools as _;
use lru::LruCache;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_chain::chain::{
    NewChunkData, NewChunkResult, ShardContext, StorageContext, UpdateShardJob, do_apply_chunks,
};
use near_chain::sharding::get_receipts_shuffle_salt;
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::types::{ApplyChunkBlockContext, RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::{ShardUpdateReason, ShardUpdateResult, process_shard_update};
use near_chain::{
    Block, Chain, ChainGenesis, ChainStore, ChainUpdate, DoomslugThresholdMode, Error,
    collect_receipts, get_chunk_clone_from_header,
};
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::block::Chunks;
use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::{BlockToApply, CachedShardUpdateKey};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardProof;
use near_primitives::types::EpochId;
use near_primitives::types::{ShardId, ShardIndex};
use near_primitives::utils::get_receipt_proof_key;
use near_primitives::utils::get_receipt_proof_target_shard_prefix;
use near_store::DBCol;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use tracing::instrument;

pub struct ChunkExecutorActor {
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    network_adapter: PeerManagerAdapter,

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
        shard_tracker: ShardTracker,
        network_adapter: PeerManagerAdapter,
        next_block_hashes_cache_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            network_adapter,
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
        for proof in receipt_proofs {
            // TODO(spice): receipt proofs should be saved to the database by the distribution layer
            if let Err(err) = save_receipt_proof(&self.chain_store.store(), &block_hash, &proof) {
                tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to save receipt proof");
                return;
            }
        }

        let Some(next_block_hashes) = self.next_block_hashes.get(&block_hash) else {
            // Next block wasn't processed yet.
            tracing::debug!(target: "chunk_executor", %block_hash, "no next block hash is available");
            return;
        };
        for next_block_hash in next_block_hashes.clone() {
            if let Err(err) = self.try_apply_chunks(&next_block_hash) {
                tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to apply chunk for block hash");
            };
        }
    }
}

impl Handler<ExecutorBlock> for ChunkExecutorActor {
    fn handle(&mut self, ExecutorBlock { block_hash }: ExecutorBlock) {
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

        if let Err(err) = self.try_apply_chunks(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to apply chunk for block hash");
        };
    }
}

impl ChunkExecutorActor {
    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%block_hash))]
    fn try_apply_chunks(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let header = block.header();
        let prev_block_hash = header.prev_hash();
        let mut all_receipts: HashMap<ShardId, Vec<ReceiptProof>> = HashMap::new();
        let store = self.chain_store.store();
        let prev_block_epoch_id = self.epoch_manager.get_epoch_id(prev_block_hash)?;
        let prev_block_shard_ids = self.epoch_manager.shard_ids(&prev_block_epoch_id)?;
        for &prev_block_shard_id in &prev_block_shard_ids {
            // TODO(spice-resharding): convert `prev_block_shard_id` into `shard_id` for
            // the current shard layout
            if self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                prev_block_hash,
                prev_block_shard_id,
            ) {
                // Genesis block has no outgoing receipts.
                if *prev_block_hash == self.genesis_hash {
                    all_receipts.insert(prev_block_shard_id, vec![]);
                    continue;
                }

                let proofs =
                    get_receipt_proofs_for_shard(&store, prev_block_hash, prev_block_shard_id)?;
                if proofs.len() != prev_block_shard_ids.len() {
                    tracing::debug!(target: "chunk_executor", %block_hash, %prev_block_hash, "missing receipts to apply all tracked chunks for a block");
                    return Ok(());
                }
                all_receipts.insert(prev_block_shard_id, proofs);
            }
        }
        self.apply_chunks(block, all_receipts, SandboxStatePatch::default())
    }

    // Logic here is based on Chain::apply_chunk_preprocessing
    fn apply_chunks(
        &mut self,
        block: Arc<Block>,
        mut incoming_receipts: HashMap<ShardId, Vec<ReceiptProof>>,
        mut state_patch: SandboxStatePatch,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let header = block.header();
        let prev_hash = header.prev_hash();
        let prev_block = self.chain_store.get_block(prev_hash)?;

        let prev_chunk_headers = self.epoch_manager.get_prev_chunk_headers(&prev_block)?;

        let epoch_id = block.header().epoch_id();
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;

        let receipts_shuffle_salt = get_receipts_shuffle_salt(self.epoch_manager.as_ref(), &block)?;

        let chunk_headers = &block.chunks();
        let mut jobs = Vec::new();
        // TODO(spice-resharding): Make sure shard logic is correct with resharding.
        for (shard_index, _prev_chunk_header) in prev_chunk_headers.iter().enumerate() {
            // XXX: This is a bit questionable -- sandbox state patching works
            // only for a single shard. This so far has been enough.
            let state_patch = state_patch.take();
            let shard_id = shard_layout.get_shard_id(shard_index)?;
            // If we don't care about shard we wouldn't have relevant incoming receipts.
            if !self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                block_hash,
                shard_id,
            ) {
                continue;
            }

            let is_new_chunk = true;
            let block_context = Chain::get_apply_chunk_block_context_from_block_header(
                block.header(),
                &chunk_headers,
                prev_block.header(),
                is_new_chunk,
            )?;

            // TODO(spice-resharding): We may need to take resharding into account here.
            let mut receipt_proofs = incoming_receipts
                .remove(&shard_id)
                .expect("expected receipts for all tracked shards");
            shuffle_receipt_proofs(&mut receipt_proofs, receipts_shuffle_salt);

            let storage_context =
                StorageContext { storage_data_source: StorageDataSource::Db, state_patch };

            let cached_shard_update_key =
                Chain::get_cached_shard_update_key(&block_context, chunk_headers, shard_id)?;

            let job = self.get_update_shard_job(
                cached_shard_update_key,
                block_context,
                chunk_headers,
                shard_index,
                &prev_block,
                ApplyChunksMode::IsCaughtUp,
                &receipt_proofs,
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
                ShardUpdateResult::OldChunk(..) => {
                    panic!("missing chunks are not expected in SPICE");
                }
            };
            let shard_id = shard_uid.shard_id();
            let (_, receipt_proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
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
        cached_shard_update_key: CachedShardUpdateKey,
        block: ApplyChunkBlockContext,
        chunk_headers: &Chunks,
        shard_index: ShardIndex,
        prev_block: &Block,
        mode: ApplyChunksMode,
        incoming_receipts: &[ReceiptProof],
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
        let shard_context = self.get_shard_context(prev_block_hash, &epoch_id, shard_id, mode)?;

        if !shard_context.should_apply_chunk {
            return Ok(None);
        }

        let chunk_header = chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
        let shard_update_reason = {
            let chunk = get_chunk_clone_from_header(&self.chain_store, chunk_header)?;
            let tx_valid_list =
                self.chain_store.compute_transaction_validity(prev_block.header(), &chunk);
            let receipts = collect_receipts(incoming_receipts);

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
        prev_hash: &CryptoHash,
        epoch_id: &EpochId,
        shard_id: ShardId,
        mode: ApplyChunksMode,
    ) -> Result<ShardContext, Error> {
        let should_apply_chunk = self.shard_tracker.should_apply_chunk(mode, prev_hash, shard_id);
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
}

fn save_receipt_proof(
    store: &Store,
    block_hash: &CryptoHash,
    receipt_proof: &ReceiptProof,
) -> Result<(), std::io::Error> {
    let &ReceiptProof(_, ShardProof { from_shard_id, to_shard_id, .. }) = receipt_proof;
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    let value = borsh::to_vec(&receipt_proof)?;
    let mut store_update = store.store_update();
    store_update.set(DBCol::receipt_proofs(), &key, &value);
    store_update.commit()?;
    Ok(())
}

fn get_receipt_proofs_for_shard(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
) -> Result<Vec<ReceiptProof>, std::io::Error> {
    let prefix = get_receipt_proof_target_shard_prefix(block_hash, to_shard_id);
    store
        .iter_prefix_ser::<ReceiptProof>(DBCol::receipt_proofs(), &prefix)
        .map(|res| res.map(|kv| kv.1))
        .collect()
}
