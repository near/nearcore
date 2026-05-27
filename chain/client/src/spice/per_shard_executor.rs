use crate::spice::chunk_executor_actor::{
    chunk_extra_exists, distribute_witness, get_chunk_extra, get_new_chunk_if_valid,
    get_receipt_proofs_for_shard, is_descendant_of_final_execution_head, save_receipt_proof,
    send_chunk_endorsement,
};
use crate::spice::chunk_executor_coordinator::PerShardChunkApplied;
use crate::spice::data_distributor_actor::{
    SpiceDataDistributorAdapter, SpiceDistributorOutgoingReceipts,
};
use crate::spice::persist::commit_per_shard_outputs;
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
use near_primitives::types::{BlockHeight, NumBlocks, ShardId};
use near_store::ShardUId;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::{BTreeSet, HashMap, VecDeque};
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
pub struct PerShardExecutorSender {
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
/// shard, spawned by [`super::chunk_executor_coordinator::ChunkExecutorCoordinator`].
///
/// Persistence is adapter-native: this shard's apply outputs are written via
/// [`commit_per_shard_outputs`] (purpose-built SPICE store-adapter helpers), not
/// the monolithic `ChainUpdate` / `apply_chunk_postprocessing` path. It holds a
/// read-only [`ChainStoreAdapter`] (no `ChainStore`).
pub struct PerShardExecutor {
    shard_id: ShardId,
    chain_store: ChainStoreAdapter,
    transaction_validity_period: NumBlocks,
    /// Persistence config flags threaded into [`commit_per_shard_outputs`]; back
    /// trie-replay/GC and RPC features (see #34).
    save_trie_changes: bool,
    save_tx_outcomes: bool,
    save_receipt_to_tx: bool,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    core_reader: SpiceCoreReader,
    validator_signer: MutableValidatorSigner,
    network_adapter: PeerManagerAdapter,
    core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    data_distributor_adapter: SpiceDataDistributorAdapter,
    coordinator_sender: Sender<PerShardChunkApplied>,
    /// Blocks announced but not yet applied, ordered by `(height, block_hash)`
    /// so a fork (two blocks at one height) keeps both entries.
    pending: BTreeSet<(BlockHeight, CryptoHash)>,
    /// Network-path receipt proofs buffered until the source block's CER lands,
    /// keyed by source block. Local-path proofs are never buffered (already on
    /// disk). See `ReceiptSource`.
    pending_unverified_receipts: HashMap<CryptoHash, Vec<ReceiptProof>>,
}

enum ApplyOutcome {
    Applied,
    Dropped,
    NotReady,
}

impl PerShardExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_id: ShardId,
        store: Store,
        transaction_validity_period: NumBlocks,
        save_trie_changes: bool,
        save_tx_outcomes: bool,
        save_receipt_to_tx: bool,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        core_reader: SpiceCoreReader,
        validator_signer: MutableValidatorSigner,
        network_adapter: PeerManagerAdapter,
        core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
        data_distributor_adapter: SpiceDataDistributorAdapter,
        coordinator_sender: Sender<PerShardChunkApplied>,
    ) -> Self {
        let chain_store = store.chain_store();
        Self {
            shard_id,
            chain_store,
            transaction_validity_period,
            save_trie_changes,
            save_tx_outcomes,
            save_receipt_to_tx,
            runtime_adapter,
            epoch_manager,
            core_reader,
            validator_signer,
            network_adapter,
            core_writer_sender,
            data_distributor_adapter,
            coordinator_sender,
            pending: BTreeSet::new(),
            pending_unverified_receipts: HashMap::new(),
        }
    }

    /// Verify any buffered `FromNetwork` receipts from `source_block` against its
    /// CER and write the valid ones. No-op until the CER is on chain.
    fn try_verify(&mut self, source_block: &CryptoHash) -> Result<(), Error> {
        let header = self.chain_store.get_block_header(source_block)?;
        let Some(cer) = self.core_reader.get_block_execution_results(&header)? else {
            return Ok(()); // CER not on chain yet — wait for ExecutionResultEndorsed.
        };
        let Some(proofs) = self.pending_unverified_receipts.remove(source_block) else {
            return Ok(());
        };
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        for proof in proofs {
            let from_shard_id = proof.1.from_shard_id;
            let Some(result) = cer.0.get(&from_shard_id) else {
                continue;
            };
            if proof.verify_against_receipt_root(result.outgoing_receipts_root) {
                save_receipt_proof(&mut store_update, source_block, &proof);
            } else {
                tracing::warn!(target: "chunk_executor", %source_block, ?from_shard_id, "dropping invalid network receipt proof");
            }
        }
        store_update.commit();
        Ok(())
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
                    tracing::error!(target: "chunk_executor", ?err, block_hash=%entry.1, shard_id=%self.shard_id, "per-shard apply failed");
                    return;
                }
            }
        }
    }

    fn try_apply(&self, block_hash: &CryptoHash) -> Result<ApplyOutcome, Error> {
        let block = self.chain_store.get_block(block_hash)?;
        if !is_descendant_of_final_execution_head(&self.chain_store, block.header()) {
            return Ok(ApplyOutcome::Dropped);
        }
        // Resharding: this shard may be absent from `block`'s layout — split/merged
        // away at an epoch boundary, or (for a child shard) not yet existent before
        // it. Such a block carries no chunk for this shard, so drop it. This also
        // keeps the height scan from computing `ShardUId(self.shard_id, block.epoch)`
        // below, which would panic in `from_shard_id_and_layout`.
        // NOTE(spice-resharding): execution actually crossing the boundary (a child
        // shard inheriting parent state via `prev_chunk_extra`) is still a follow-up
        // — see the old executor's `TODO(spice-resharding)`; today's test relies on
        // execution lagging behind the boundary.
        let shard_layout = self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        if !shard_layout.shard_ids().any(|id| id == self.shard_id) {
            return Ok(ApplyOutcome::Dropped);
        }
        if chunk_extra_exists(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            block_hash,
            self.shard_id,
        )? {
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
        let Some(prev_chunk_extra) = get_chunk_extra(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &prev_block_hash,
            self.shard_id,
        )?
        else {
            return Ok(ApplyOutcome::NotReady);
        };

        let prev_epoch_id = self.epoch_manager.get_epoch_id(&prev_block_hash)?;
        let prev_block_shard_ids = self.epoch_manager.shard_ids(&prev_epoch_id)?;
        let mut incoming_receipts = if prev_block.header().is_genesis() {
            vec![]
        } else {
            let proofs = get_receipt_proofs_for_shard(
                &self.chain_store.store(),
                &prev_block_hash,
                self.shard_id,
            );
            if proofs.len() != prev_block_shard_ids.len() {
                return Ok(ApplyOutcome::NotReady);
            }
            proofs
        };

        let shard_index = shard_layout.get_shard_index(self.shard_id)?;
        let chunk_header =
            block.chunks().get(shard_index).ok_or(Error::InvalidShardId(self.shard_id))?.clone();
        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);

        // Inline apply (mirrors ChunkExecutorActor::get_update_shard_job, but
        // run synchronously on this shard's own thread — no spawner).
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
                        self.transaction_validity_period,
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
            self.core_reader.prev_validator_proposals(&prev_block_hash, self.shard_id)?;
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
                self.shard_id,
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
            self.save_trie_changes,
            self.save_tx_outcomes,
            self.save_receipt_to_tx,
        )?;

        self.coordinator_sender.send(PerShardChunkApplied {
            block_hash: *block_hash,
            shard_id: self.shard_id,
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
        let epoch_id = self.epoch_manager.get_epoch_id(block.hash())?;

        // Endorse if we are a chunk validator (regardless of producer status).
        let validators = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            self.shard_id,
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
                self.shard_id,
                outgoing_receipts_root,
            );
        }

        // Distribute the witness + outgoing receipts only if we are the chunk
        // producer for this shard (the data distributor asserts this).
        let producers =
            self.epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, self.shard_id)?;
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
                self.shard_id,
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

impl Actor for PerShardExecutor {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        // Self-bootstrap: park every block above the execution head and start
        // applying. A freshly-spawned shard catches up from disk on its own, so
        // the coordinator never has to replay historical blocks to it (which
        // would block the test-loop thread on a not-yet-registered mailbox).
        if let Err(err) = self.bootstrap() {
            tracing::error!(target: "chunk_executor", ?err, shard_id=%self.shard_id, "per-shard bootstrap failed");
        }
    }
}

impl PerShardExecutor {
    fn bootstrap(&mut self) -> Result<(), Error> {
        let head = match self.chain_store.spice_execution_head() {
            Ok(tip) => tip.last_block_hash,
            Err(Error::DBNotFoundErr(_)) => self.chain_store.genesis_hash()?,
            Err(err) => return Err(err),
        };
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

impl Handler<ProcessedBlock> for PerShardExecutor {
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

impl Handler<IncomingReceipt> for PerShardExecutor {
    fn handle(&mut self, IncomingReceipt { block_hash, proof, source }: IncomingReceipt) {
        match source {
            // Sender already wrote the proof to disk; just re-check.
            ReceiptSource::LocallyVerified => {}
            ReceiptSource::FromNetwork => {
                self.pending_unverified_receipts.entry(block_hash).or_default().push(proof);
                if let Err(err) = self.try_verify(&block_hash) {
                    tracing::error!(target: "chunk_executor", ?err, %block_hash, "receipt verification failed");
                }
            }
        }
        self.try_apply_pending();
    }
}

impl Handler<ExecutionResultEndorsed> for PerShardExecutor {
    fn handle(&mut self, ExecutionResultEndorsed { block_hash }: ExecutionResultEndorsed) {
        if let Err(err) = self.try_verify(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, %block_hash, "receipt verification failed");
        }
        self.try_apply_pending();
    }
}
