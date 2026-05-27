use crate::spice::chunk_executor_coordinator::ChunkApplied;
use crate::spice::data_distributor_actor::{
    SpiceDataDistributorAdapter, SpiceDistributorOutgoingReceipts,
};
use crate::spice::executor_shared::{
    ChunkExecutorConfig, distribute_witness, get_new_chunk_if_valid, get_receipt_proofs_for_shard,
    is_descendant_of_final_execution_head, optional, save_receipt_proof, send_chunk_endorsement,
};
use crate::spice::persist::commit_per_shard_outputs;
use crate::spice::unverified_receipts::UnverifiedReceiptTracker;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::chain::{NewChunkData, NewChunkResult, StorageContext};
use near_chain::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use near_chain::spice::chunk_application::build_spice_apply_chunk_block_context;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::types::{RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::spice_apply_new_chunk;
use near_chain::{Block, Chain, Error, check_transaction_validity_period, collect_receipts};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::sharding::{ReceiptProof, ShardChunk};
use near_primitives::types::{BlockHeight, NumBlocks};
use near_store::ShardUId;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

/// SPICE-only twin of `ChainStore::compute_transaction_validity`, derived off the
/// `ChainStoreAdapter` so the per-shard executor needn't hold a `ChainStore`. Same
/// logic as the shared method (a thin loop over `check_transaction_validity_period`),
/// just adapter-based — byte-identical by construction.
fn spice_compute_transaction_validity(
    chain_store: &ChainStoreAdapter,
    transaction_validity_period: NumBlocks,
    prev_block_header: &BlockHeader,
    chunk: &ShardChunk,
) -> Vec<bool> {
    chunk
        .to_transactions()
        .iter()
        .map(|signed_tx| {
            check_transaction_validity_period(
                chain_store,
                prev_block_header,
                signed_tx.transaction.block_hash(),
                transaction_validity_period,
            )
            .is_ok()
        })
        .collect()
}

/// The coordinator's handle to one per-shard executor. Built from the actor's
/// runtime handle via `.as_multi_sender()`, so the coordinator is agnostic to
/// how the actor was spawned (production tokio actor vs. test-loop).
#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ChunkExecutorActorSender {
    pub processed_block: Sender<ProcessedBlock>,
    pub incoming_receipt: Sender<IncomingReceipt>,
    pub execution_result_endorsed: Sender<ExecutionResultEndorsed>,
}

/// How a receipt proof reached this shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptSource {
    /// Produced by a sibling shard on this node — already written to disk by the
    /// sender, so this is a wake-up only (no write).
    LocallyVerified,
    /// Reconstructed from the network; must be verified against the source
    /// block's CER and written by this actor.
    FromNetwork,
}

/// A receipt proof addressed to this shard.
#[derive(Debug)]
pub struct IncomingReceipt {
    pub block_hash: CryptoHash,
    pub proof: ReceiptProof,
    pub source: ReceiptSource,
}

/// Per-shard half of the per-shard SPICE chunk-execution subsystem: applies one
/// shard's chunks inline on its own dedicated thread. One instance per tracked
/// shard, spawned by [`super::chunk_executor_coordinator::ChunkExecutorCoordinatorActor`].
///
/// Persistence is adapter-native: this shard's apply outputs are written via
/// [`commit_per_shard_outputs`] (purpose-built SPICE store-adapter helpers). It
/// holds a read-only [`ChainStoreAdapter`] (no `ChainStore`).
pub struct ChunkExecutorActor {
    /// This executor is bound to one shard for its whole life; we store the richer
    /// `ShardUId` so chunk-store reads need no per-call `epoch_manager → uid`
    /// conversion. A resharding event retires this executor and spawns fresh ones
    /// for the child shards (lifecycle handles it), so the stored value stays valid.
    shard_uid: ShardUId,
    chain_store: ChainStoreAdapter,
    /// Scalar executor config: persistence flags (threaded into
    /// [`commit_per_shard_outputs`], backing trie-replay/GC and RPC features, see
    /// #34) plus the transaction validity period.
    config: ChunkExecutorConfig,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    core_reader: SpiceCoreReader,
    validator_signer: MutableValidatorSigner,
    network_adapter: PeerManagerAdapter,
    core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    data_distributor_adapter: SpiceDataDistributorAdapter,
    coordinator_sender: Sender<ChunkApplied>,
    /// Blocks announced but not yet applied, ordered by `(height, block_hash)`
    /// so a fork (two blocks at one height) keeps both entries.
    pending: BTreeSet<(BlockHeight, CryptoHash)>,
    /// Network-path receipt proofs buffered until their source block's CER lands.
    /// Local-path proofs never go here (already on disk). See [`UnverifiedReceiptTracker`].
    unverified_receipts: UnverifiedReceiptTracker,
}

enum ApplyOutcome {
    Applied,
    Dropped,
    NotReady,
}

impl ChunkExecutorActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_uid: ShardUId,
        store: Store,
        config: ChunkExecutorConfig,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        core_reader: SpiceCoreReader,
        validator_signer: MutableValidatorSigner,
        network_adapter: PeerManagerAdapter,
        core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
        data_distributor_adapter: SpiceDataDistributorAdapter,
        coordinator_sender: Sender<ChunkApplied>,
    ) -> Self {
        let chain_store = store.chain_store();
        Self {
            shard_uid,
            chain_store,
            config,
            runtime_adapter,
            epoch_manager,
            core_reader,
            validator_signer,
            network_adapter,
            core_writer_sender,
            data_distributor_adapter,
            coordinator_sender,
            pending: BTreeSet::new(),
            unverified_receipts: UnverifiedReceiptTracker::default(),
        }
    }

    /// Apply every parked chunk that is ready, lowest height first. Applying a
    /// block can make the next height ready (its prev is now executed), and the
    /// forward scan picks that up in the same pass; a higher fork block can be
    /// ready while a lower one waits, so we continue past `NotReady`. Cross-shard
    /// dependencies (a sibling's receipts, a prev block's CER) are re-driven by
    /// the next `IncomingReceipt` / `ExecutionResultEndorsed`, which re-enters here.
    fn try_apply_pending(&mut self) {
        // `pending` is height-ordered (BTreeSet); scan low-to-high. Snapshot it so
        // we can remove applied entries from `pending` while iterating.
        let parked: Vec<(BlockHeight, CryptoHash)> = self.pending.iter().copied().collect();
        for entry in parked {
            match self.try_apply(&entry.1) {
                Ok(ApplyOutcome::NotReady) => continue,
                Ok(ApplyOutcome::Applied | ApplyOutcome::Dropped) => {
                    self.pending.remove(&entry);
                }
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, block_hash=%entry.1, shard_id=%self.shard_uid.shard_id(), "per-shard apply failed");
                    return;
                }
            }
        }
    }

    fn try_apply(&self, block_hash: &CryptoHash) -> Result<ApplyOutcome, Error> {
        let shard_id = self.shard_uid.shard_id();
        let block = self.chain_store.get_block(block_hash)?;
        if !is_descendant_of_final_execution_head(&self.chain_store, block.header()) {
            return Ok(ApplyOutcome::Dropped);
        }
        // Resharding: this shard may be absent from `block`'s layout — split/merged
        // away at an epoch boundary, or (for a child shard) not yet existent before
        // it. Such a block carries no chunk for this shard, so drop it.
        // NOTE(spice-resharding): execution actually crossing the boundary (a child
        // shard inheriting parent state via `prev_chunk_extra`) is still a follow-up;
        // today's test relies on execution lagging behind the boundary.
        let shard_layout = self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        if !shard_layout.shard_ids().any(|id| id == shard_id) {
            return Ok(ApplyOutcome::Dropped);
        }
        let chunk_store = self.chain_store.chunk_store();
        if optional(chunk_store.get_chunk_extra(block_hash, &self.shard_uid))?.is_some() {
            return Ok(ApplyOutcome::Dropped);
        }

        let header = block.header();
        let prev_block_hash = *header.prev_hash();
        let prev_block = self.chain_store.get_block(&prev_block_hash)?;

        let Some(prev_block_execution_results) =
            self.core_reader.get_block_execution_results(prev_block.header())?
        else {
            return Ok(ApplyOutcome::NotReady);
        };

        // The coordinator only spawns per-shard actors for shards this node
        // tracks, so this actor always applies its shard's chunk.
        let Some(prev_chunk_extra) =
            optional(chunk_store.get_chunk_extra(&prev_block_hash, &self.shard_uid))?
        else {
            return Ok(ApplyOutcome::NotReady);
        };

        let prev_epoch_id = self.epoch_manager.get_epoch_id(&prev_block_hash)?;
        let prev_block_shard_ids = self.epoch_manager.shard_ids(&prev_epoch_id)?;
        let mut incoming_receipts = if prev_block.header().is_genesis() {
            vec![]
        } else {
            let proofs =
                get_receipt_proofs_for_shard(&self.chain_store.store(), &prev_block_hash, shard_id);
            if proofs.len() != prev_block_shard_ids.len() {
                return Ok(ApplyOutcome::NotReady);
            }
            proofs
        };

        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_header =
            block.chunks().get(shard_index).ok_or(Error::InvalidShardId(shard_id))?.clone();
        let shard_uid = self.shard_uid;

        // Inline apply, run synchronously on this shard's own thread (no spawner).
        let block_context = build_spice_apply_chunk_block_context(
            block.header(),
            &prev_block_execution_results,
            self.epoch_manager.as_ref(),
        )?;
        shuffle_receipt_proofs(&mut incoming_receipts, get_receipts_shuffle_salt(&block));
        let receipts = collect_receipts(&incoming_receipts);
        let prev_block_header = self.chain_store.get_block_header(&prev_block_hash)?;
        let (transactions, chunk_hash) =
            match get_new_chunk_if_valid(&self.chain_store, &chunk_header, block_context.height)? {
                Some(chunk) => {
                    let chunk_hash = chunk.chunk_hash().clone();
                    let tx_valid_list = spice_compute_transaction_validity(
                        &self.chain_store,
                        self.config.transaction_validity_period,
                        &prev_block_header,
                        &chunk,
                    );
                    (
                        SignedValidPeriodTransactions::new(
                            chunk.into_transactions(),
                            tx_valid_list,
                        ),
                        Some(chunk_hash),
                    )
                }
                None => (SignedValidPeriodTransactions::new(vec![], vec![]), None),
            };
        let prev_validator_proposals =
            self.core_reader.prev_validator_proposals(&prev_block_hash, shard_id)?;
        let memtrie_pin = self
            .runtime_adapter
            .get_tries()
            .maybe_pin_memtrie_root(shard_uid, *prev_chunk_extra.state_root())?;
        let new_chunk_data = NewChunkData {
            gas_limit: prev_chunk_extra.gas_limit(),
            prev_state_root: *prev_chunk_extra.state_root(),
            prev_validator_proposals,
            chunk_hash,
            transactions,
            receipts,
            block: block_context,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Db,
                state_patch: SandboxStatePatch::default(),
            },
        };
        let span = tracing::Span::current();
        let new_chunk_result = spice_apply_new_chunk(
            &span,
            self.runtime_adapter.as_ref(),
            new_chunk_data,
            shard_uid,
            memtrie_pin,
        )?;

        let (outgoing_receipts_root, receipt_proofs) =
            Chain::create_receipts_proofs_from_outgoing_receipts(
                &shard_layout,
                shard_id,
                new_chunk_result.apply_result.outgoing_receipts.clone(),
            )?;
        let bandwidth_scheduler_state_hash =
            new_chunk_result.apply_result.bandwidth_scheduler_state_hash;

        // Sender writes its own outgoing receipt proofs durably (matches the old
        // executor's save_produced_receipts) before any outbound message.
        self.save_produced_receipts(block_hash, &receipt_proofs);
        self.emit(&block, &new_chunk_result, &receipt_proofs, outgoing_receipts_root)?;

        // Persist this shard's apply artifacts via the SPICE adapter-native write
        // helper (atomic ChunkExtra sentinel + refcounted State/Receipts). Head
        // advance is the coordinator's job, so we do NOT touch the spice heads here.
        commit_per_shard_outputs(
            &self.chain_store.store(),
            self.runtime_adapter.as_ref(),
            &block,
            new_chunk_result,
            self.config.save_trie_changes,
            self.config.save_tx_outcomes,
            self.config.save_receipt_to_tx,
        )?;

        self.coordinator_sender.send(ChunkApplied {
            block_hash: *block_hash,
            shard_id,
            outgoing_receipt_proofs: receipt_proofs,
            bandwidth_scheduler_state_hash,
        });
        Ok(ApplyOutcome::Applied)
    }

    /// Emit this shard's outputs after a successful apply, mirroring the old
    /// executor's per-result loop: endorse if this node is a chunk validator,
    /// and (producer-gated) distribute outgoing receipts + the state witness.
    fn emit(
        &self,
        block: &Block,
        new_chunk_result: &NewChunkResult,
        receipt_proofs: &[ReceiptProof],
        outgoing_receipts_root: CryptoHash,
    ) -> Result<(), Error> {
        let Some(my_signer) = self.validator_signer.get() else {
            return Ok(());
        };
        let shard_id = self.shard_uid.shard_id();
        let epoch_id = self.epoch_manager.get_epoch_id(block.hash())?;

        // Endorse if we are a chunk validator (regardless of producer status).
        let validators = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            shard_id,
            block.header().height(),
        )?;
        if validators.contains(my_signer.validator_id()) {
            let NewChunkResult { gas_limit, apply_result, .. } = new_chunk_result;
            send_chunk_endorsement(
                self.epoch_manager.as_ref(),
                &self.network_adapter,
                &self.core_writer_sender,
                &my_signer,
                block,
                apply_result,
                *gas_limit,
                shard_id,
                outgoing_receipts_root,
            );
        }

        // Distribute the witness + outgoing receipts only if we are the chunk
        // producer for this shard (the data distributor asserts this).
        let producers =
            self.epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id)?;
        if producers.contains(my_signer.validator_id()) {
            self.data_distributor_adapter.send(SpiceDistributorOutgoingReceipts {
                block_hash: *block.hash(),
                receipt_proofs: receipt_proofs.to_vec(),
            });
            let NewChunkResult { gas_limit, apply_result, .. } = new_chunk_result;
            distribute_witness(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                &self.data_distributor_adapter,
                block,
                apply_result,
                *gas_limit,
                shard_id,
                outgoing_receipts_root,
            )?;
        }
        Ok(())
    }

    fn save_produced_receipts(&self, block_hash: &CryptoHash, receipt_proofs: &[ReceiptProof]) {
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        for proof in receipt_proofs {
            save_receipt_proof(&mut store_update, block_hash, proof);
        }
        store_update.commit();
    }
}

impl Actor for ChunkExecutorActor {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        // Self-bootstrap: park every block above the execution head and start
        // applying. A freshly-spawned shard catches up from disk on its own, so
        // the coordinator never has to replay historical blocks to it (which
        // would block the test-loop thread on a not-yet-registered mailbox).
        if let Err(err) = self.bootstrap() {
            tracing::error!(target: "chunk_executor", ?err, shard_id=%self.shard_uid.shard_id(), "per-shard bootstrap failed");
        }
    }
}

impl ChunkExecutorActor {
    fn bootstrap(&mut self) -> Result<(), Error> {
        // Always set: initialized to the genesis tip at genesis and advanced
        // forward-only by the coordinator (see `ChunkExecutorCoordinatorActor::execution_head`).
        let head = self.chain_store.spice_execution_head()?.last_block_hash;
        let mut queue: VecDeque<CryptoHash> =
            self.chain_store.get_all_next_block_hashes(&head).into();
        while let Some(block_hash) = queue.pop_front() {
            let height = self.chain_store.get_block_header(&block_hash)?.height();
            self.pending.insert((height, block_hash));
            queue.extend(self.chain_store.get_all_next_block_hashes(&block_hash));
        }
        self.try_apply_pending();
        Ok(())
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        // ProcessedBlock is routed by the coordinator, which already read this
        // header in dispatch_block — so it is always on disk by the time we get here.
        let height = self
            .chain_store
            .get_block_header(&block_hash)
            .expect("block header must be stored before ProcessedBlock is routed")
            .height();
        self.pending.insert((height, block_hash));
        self.try_apply_pending();
    }
}

impl Handler<IncomingReceipt> for ChunkExecutorActor {
    fn handle(&mut self, IncomingReceipt { block_hash, proof, source }: IncomingReceipt) {
        match source {
            // Sender already wrote the proof to disk; just re-check.
            ReceiptSource::LocallyVerified => {}
            ReceiptSource::FromNetwork => {
                self.unverified_receipts.buffer(block_hash, proof);
                if let Err(err) = self.unverified_receipts.try_drain(
                    &self.chain_store,
                    &self.core_reader,
                    &block_hash,
                ) {
                    tracing::error!(target: "chunk_executor", ?err, %block_hash, "receipt verification failed");
                }
            }
        }
        self.try_apply_pending();
    }
}

impl Handler<ExecutionResultEndorsed> for ChunkExecutorActor {
    fn handle(&mut self, ExecutionResultEndorsed { block_hash }: ExecutionResultEndorsed) {
        if let Err(err) =
            self.unverified_receipts.try_drain(&self.chain_store, &self.core_reader, &block_hash)
        {
            tracing::error!(target: "chunk_executor", ?err, %block_hash, "receipt verification failed");
        }
        self.try_apply_pending();
    }
}
