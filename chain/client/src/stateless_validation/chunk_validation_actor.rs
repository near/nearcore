//! An actor for validating chunk state witnesses and orphan witnesses.
//!
//! # Orphan witnesses
//! To process a ChunkStateWitness we need its previous block, but sometimes
//! the witness shows up before the previous block is available, so it can't be
//! processed immediately. In such cases the witness becomes an orphaned witness
//! and it's kept in the pool until the required block arrives. Once the block
//! arrives, all witnesses that were waiting for it can be processed.

use crate::stateless_validation::chunk_validator::orphan_witness_pool::OrphanStateWitnessPool;
use crate::stateless_validation::chunk_validator::send_chunk_endorsement_to_block_producers;
use itertools::Itertools;
use lru::LruCache;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{Actor, Handler, Sender};
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::{ActorSystem, MultiSend, MultiSenderFrom};
use near_chain::chain::{ChunkStateWitnessMessage, NewChunkData, StorageContext};
use near_chain::stateless_validation::chunk_validation::{
    self, MainStateTransitionCache, MainTransition, PendingValidateWitnessCache,
    PreValidationOutput,
};
use near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL;
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
use near_chain::types::{RuntimeAdapter, StorageDataSource};
use near_chain::validate::validate_chunk_with_chunk_extra_and_roots;
use near_chain::{Chain, ChainStore, ChainStoreAccess, Error};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_performance_metrics_macros::perf;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessAck, ChunkStateWitnessSize, ChunkStateWitnessV3,
};
use near_primitives::stateless_validation::{WitnessProductionKey, WitnessType};
use near_primitives::types::BlockHeight;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::PartialStorage;
use node_runtime::SignedValidPeriodTransactions;
use parking_lot::Mutex;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

/// We keep only orphan witnesses that are within this distance of
/// the current chain head. This helps to reduce the size of
/// OrphanStateWitnessPool and protects against spam attacks.
/// The range starts at 2 because a witness at height of head+1 would
/// have the previous block available (the chain head), so it wouldn't be an orphan.
const ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD: Range<BlockHeight> = 2..6;

/// Outcome of processing an orphaned witness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOrphanWitnessOutcome {
    SavedToPool,
    TooBig(usize),
    TooFarFromHead { head_height: BlockHeight, witness_height: BlockHeight },
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ChunkValidationSenderForPartialWitness {
    pub chunk_state_witness: Sender<ChunkStateWitnessMessage>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ChunkValidationSender {
    pub chunk_state_witness: Sender<ChunkStateWitnessMessage>,
    pub block_notification: Sender<BlockNotificationMessage>,
}

/// Message to notify the chunk validation actor about new blocks
/// so it can process orphan witnesses that were waiting for these blocks.
#[derive(Debug)]
pub struct BlockNotificationMessage {
    pub block: Arc<Block>,
}

/// An actor for validating chunk state witnesses and orphan witnesses.
#[derive(Clone)]
pub struct ChunkValidationActorInner {
    chain_store: ChainStore,
    genesis_block: Arc<Block>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Sender<PeerManagerMessageRequest>,
    validator_signer: MutableValidatorSigner,
    save_latest_witnesses: bool,
    save_invalid_witnesses: bool,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,
    main_state_transition_result_cache: MainStateTransitionCache,
    pending_validate_witness_cache: PendingValidateWitnessCache,
    orphan_witness_pool: Arc<Mutex<OrphanStateWitnessPool>>,
    max_orphan_witness_size: u64,
    rs: Arc<ReedSolomon>,
}

impl Actor for ChunkValidationActorInner {}

impl ChunkValidationActorInner {
    pub fn new(
        chain_store: ChainStore,
        genesis_block: Arc<Block>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Sender<PeerManagerMessageRequest>,
        validator_signer: MutableValidatorSigner,
        save_latest_witnesses: bool,
        save_invalid_witnesses: bool,
        validation_spawner: Arc<dyn AsyncComputationSpawner>,
        orphan_witness_pool_size: usize,
        max_orphan_witness_size: u64,
    ) -> Self {
        let data_parts = epoch_manager.num_data_parts();
        let parity_parts = epoch_manager.num_total_parts() - data_parts;
        let rs = Arc::new(ReedSolomon::new(data_parts, parity_parts).unwrap());
        Self {
            chain_store,
            genesis_block,
            epoch_manager,
            runtime_adapter,
            network_adapter,
            validator_signer,
            save_latest_witnesses,
            save_invalid_witnesses,
            validation_spawner,
            main_state_transition_result_cache: MainStateTransitionCache::default(),
            pending_validate_witness_cache: PendingValidateWitnessCache::default(),
            orphan_witness_pool: Arc::new(Mutex::new(OrphanStateWitnessPool::new(
                orphan_witness_pool_size,
            ))),
            max_orphan_witness_size,
            rs,
        }
    }

    /// Creates a new actor with shared orphan witness pool (for multiple actor instances).
    pub fn new_with_shared_pool(
        chain_store: ChainStore,
        genesis_block: Arc<Block>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Sender<PeerManagerMessageRequest>,
        validator_signer: MutableValidatorSigner,
        save_latest_witnesses: bool,
        save_invalid_witnesses: bool,
        validation_spawner: Arc<dyn AsyncComputationSpawner>,
        shared_orphan_pool: Arc<Mutex<OrphanStateWitnessPool>>,
        main_state_transition_result_cache: MainStateTransitionCache,
        pending_validate_witness_cache: PendingValidateWitnessCache,
        max_orphan_witness_size: u64,
    ) -> Self {
        let data_parts = epoch_manager.num_data_parts();
        let parity_parts = epoch_manager.num_total_parts() - data_parts;
        let rs = Arc::new(ReedSolomon::new(data_parts, parity_parts).unwrap());
        Self {
            chain_store,
            genesis_block,
            epoch_manager,
            runtime_adapter,
            network_adapter,
            validator_signer,
            save_latest_witnesses,
            save_invalid_witnesses,
            validation_spawner,
            main_state_transition_result_cache,
            pending_validate_witness_cache,
            orphan_witness_pool: shared_orphan_pool,
            max_orphan_witness_size,
            rs,
        }
    }

    /// Spawns multiple chunk validation actors using a multithreaded actor.
    pub fn spawn_multithread_actor(
        actor_system: ActorSystem,
        chain_store: ChainStore,
        genesis_block: Arc<Block>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Sender<PeerManagerMessageRequest>,
        validator_signer: MutableValidatorSigner,
        save_latest_witnesses: bool,
        save_invalid_witnesses: bool,
        validation_spawner: Arc<dyn AsyncComputationSpawner>,
        orphan_witness_pool_size: usize,
        max_orphan_witness_size: u64,
        num_actors: usize,
    ) -> MultithreadRuntimeHandle<ChunkValidationActorInner> {
        // Create shared orphan witness pool
        let shared_orphan_pool =
            Arc::new(Mutex::new(OrphanStateWitnessPool::new(orphan_witness_pool_size)));
        let main_state_transition_result_cache = MainStateTransitionCache::default();
        let pending_validate_witness_cache = PendingValidateWitnessCache::default();

        actor_system.spawn_multithread_actor(num_actors, move || {
            ChunkValidationActorInner::new_with_shared_pool(
                chain_store.clone(),
                genesis_block.clone(),
                epoch_manager.clone(),
                runtime_adapter.clone(),
                network_adapter.clone(),
                validator_signer.clone(),
                save_latest_witnesses,
                save_invalid_witnesses,
                validation_spawner.clone(),
                shared_orphan_pool.clone(),
                main_state_transition_result_cache.clone(),
                pending_validate_witness_cache.clone(),
                max_orphan_witness_size,
            )
        })
    }

    fn send_state_witness_ack(&self, witness: &ChunkStateWitness) -> Result<(), Error> {
        let chunk_producer = self
            .epoch_manager
            .get_chunk_producer_info(&witness.production_key().chunk)?
            .account_id()
            .clone();

        // Skip sending ack to self.
        if let Some(validator_signer) = self.validator_signer.get() {
            if chunk_producer == *validator_signer.validator_id() {
                return Ok(());
            }
        }

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitnessAck(
                chunk_producer,
                ChunkStateWitnessAck::new(witness),
            ),
        ));
        Ok(())
    }

    pub fn handle_orphan_witness(
        &mut self,
        witness: ChunkStateWitness,
        witness_size: ChunkStateWitnessSize,
    ) -> Result<HandleOrphanWitnessOutcome, Error> {
        let chunk_header = witness.latest_chunk_header();
        let witness_height = chunk_header.height_created();
        let witness_shard = chunk_header.shard_id();

        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "handle_orphan_witness",
            witness_height,
            ?witness_shard,
            witness_chunk = ?chunk_header.chunk_hash(),
            witness_prev_block = ?chunk_header.prev_block_hash(),
        )
        .entered();

        // Don't save orphaned state witnesses which are far away from the current chain head.
        let chain_head = self.chain_store.head()?;
        let head_distance = witness_height.saturating_sub(chain_head.height);

        if !ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD.contains(&head_distance) {
            tracing::debug!(
                target: "chunk_validation",
                head_height = chain_head.height,
                "Not saving an orphaned ChunkStateWitness because its height isn't within the allowed height range"
            );
            return Ok(HandleOrphanWitnessOutcome::TooFarFromHead {
                witness_height,
                head_height: chain_head.height,
            });
        }

        // Don't save orphaned state witnesses which are bigger than the allowed limit.
        let witness_size_u64: u64 = witness_size as u64;
        if witness_size_u64 > self.max_orphan_witness_size {
            tracing::warn!(
                target: "chunk_validation",
                witness_height,
                ?witness_shard,
                witness_chunk = ?chunk_header.chunk_hash(),
                witness_prev_block = ?chunk_header.prev_block_hash(),
                witness_size = witness_size_u64,
                "Not saving an orphaned ChunkStateWitness because it's too big. This is unexpected."
            );
            return Ok(HandleOrphanWitnessOutcome::TooBig(witness_size_u64 as usize));
        }

        // Orphan witness is OK, save it to the pool
        tracing::debug!(target: "chunk_validation", "Saving an orphaned ChunkStateWitness to orphan pool");
        self.orphan_witness_pool
            .lock()
            .add_orphan_state_witness(witness, witness_size_u64 as usize);
        Ok(HandleOrphanWitnessOutcome::SavedToPool)
    }

    /// Processes orphan witnesses that are now ready because their previous block has arrived.
    fn process_ready_orphan_witnesses(&self, new_block: &Block) {
        let ready_witnesses = self
            .orphan_witness_pool
            .lock()
            .take_state_witnesses_waiting_for_block(new_block.hash());

        for witness in ready_witnesses {
            let header = witness.latest_chunk_header();
            tracing::debug!(
                target: "chunk_validation",
                witness_height = header.height_created(),
                witness_shard = ?header.shard_id(),
                witness_chunk = ?header.chunk_hash(),
                witness_prev_block = ?header.prev_block_hash(),
                "Processing an orphaned ChunkStateWitness, its previous block has arrived."
            );

            // Validate that block hash matches
            if header.prev_block_hash() != new_block.hash() {
                let err = Error::Other(format!(
                    "Previous block hash mismatch: witness={}, block={}",
                    header.prev_block_hash(),
                    new_block.hash()
                ));
                tracing::error!(target: "chunk_validation", ?err, "Error processing orphan chunk state witness");
            } else if let Err(err) = self.process_chunk_state_witness(witness, None) {
                tracing::error!(target: "chunk_validation", ?err, "Error processing orphan chunk state witness");
            }
        }
    }

    /// Cleans old orphan witnesses and process ready ones when a new block arrives.
    fn handle_block_notification(&self, new_block: &Block) {
        if self.validator_signer.get().is_some() {
            self.process_ready_orphan_witnesses(new_block);
        }

        // Remove all orphan witnesses that are below the last final block
        let last_final_block = new_block.header().last_final_block();
        if last_final_block == &CryptoHash::default() {
            return;
        }

        if let Ok(last_final_block_header) = self.chain_store.get_block_header(last_final_block) {
            self.orphan_witness_pool
                .lock()
                .remove_witnesses_below_final_height(last_final_block_header.height());
        } else {
            tracing::error!(
                target: "chunk_validation",
                ?last_final_block,
                "Error getting last final block header for orphan witness cleanup"
            );
        }
    }

    fn process_chunk_state_witness(
        &self,
        witness: ChunkStateWitness,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let chunk_header = witness.latest_chunk_header();
        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "process_chunk_state_witness",
            chunk_hash = ?chunk_header.chunk_hash(),
            height = %chunk_header.height_created(),
            shard_id = %chunk_header.shard_id(),
        )
        .entered();

        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("No validator signer available".to_string()));
        };

        // brand new caching logic
        let shard_uid = shard_id_to_uid(
            self.epoch_manager.as_ref(),
            chunk_header.shard_id(),
            &witness.epoch_id(),
        )?;
        let witness = {
            let witness_key = witness.production_key();
            let witness_type = witness_key.witness_type;
            if witness_type == WitnessType::Validate {
                // Check if we received the Apply witness for this Validate witness
                let mut apply_witness_key = witness_key.clone();
                apply_witness_key.witness_type = WitnessType::Optimistic;
                let mut shard_cache = self.pending_validate_witness_cache.lock();
                let cache = shard_cache
                    .entry(shard_uid)
                    .or_insert_with(|| LruCache::new(NonZeroUsize::new(20).unwrap()));
                if cache.contains(&apply_witness_key) {
                    // well very likely apply chunk is in processing so we are good to go
                    println!(
                        "Validate - found Optimistic, height: {}",
                        chunk_header.height_created()
                    );
                    witness
                } else {
                    // Store Validate witness as pending
                    println!("Validate - pending, height: {}", chunk_header.height_created());
                    cache.put(witness_key, witness);
                    return Ok(());
                }
            } else if witness_type == WitnessType::Optimistic {
                // Check if we received the Validate witness for this Optimistic witness
                let mut validate_witness_key = witness_key.clone();
                validate_witness_key.witness_type = WitnessType::Validate;
                let mut shard_cache = self.pending_validate_witness_cache.lock();
                let cache = shard_cache
                    .entry(shard_uid)
                    .or_insert_with(|| LruCache::new(NonZeroUsize::new(20).unwrap()));
                if let Some(validate_witness) = cache.pop(&validate_witness_key) {
                    // We have both witnesses, combine them
                    println!("Optimistic - merging, height: {}", chunk_header.height_created());
                    let ChunkStateWitness::V3(ChunkStateWitnessV3 {
                        chunk_apply_witness: Some(chunk_apply_witness),
                        ..
                    }) = witness
                    else {
                        return Err(Error::Other("Invalid apply witness".to_string()));
                    };
                    let ChunkStateWitness::V3(ChunkStateWitnessV3 {
                        chunk_validate_witness: Some(chunk_validate_witness),
                        ..
                    }) = validate_witness
                    else {
                        return Err(Error::Other("Invalid validate witness".to_string()));
                    };
                    println!("Optimistic - merged");
                    ChunkStateWitness::V3(ChunkStateWitnessV3 {
                        chunk_apply_witness: Some(chunk_apply_witness),
                        chunk_validate_witness: Some(chunk_validate_witness),
                    })
                } else {
                    // put whatever. we don't need real value.
                    println!("Optimistic - executing, height: {}", chunk_header.height_created());
                    cache.put(
                        witness_key,
                        ChunkStateWitness::new_dummy(
                            chunk_header.height_created(),
                            chunk_header.shard_id(),
                            *chunk_header.prev_block_hash(),
                        ),
                    );
                    // and process witness right away
                    witness
                }
            } else {
                println!("Full - executing, height: {}", chunk_header.height_created());
                witness
            }
        };

        self.start_validating_chunk(
            witness,
            &signer,
            self.save_invalid_witnesses,
            processing_done_tracker,
        )
    }

    fn try_validate_chunk_with_chunk_extra(
        &self,
        state_witness: ChunkStateWitness,
        signer: &Arc<ValidatorSigner>,
    ) -> Result<bool, Error> {
        // If we have the chunk extra for the previous block, we can validate
        // the chunk without state witness.
        // This usually happens because we are a chunk producer and
        // therefore have the chunk extra for the previous block saved on disk.
        // We can also skip validating the chunk state witness in this case.
        // We don't need to switch to parent shard uid, because resharding
        // creates chunk extra for new shard uid.

        let witness_production_key = state_witness.production_key();
        if witness_production_key.witness_type == WitnessType::Optimistic {
            // Don't bother trying to find block hash for chunk extra.
            return Ok(false);
        }

        let chunk_production_key = witness_production_key.chunk;
        let chunk_producer_name =
            self.epoch_manager.get_chunk_producer_info(&chunk_production_key)?.take_account_id();
        let chunk_header = state_witness.chunk_header().clone();
        let prev_block_hash = chunk_header.prev_block_hash();
        let shard_id = chunk_header.shard_id();
        let epoch_id = state_witness.epoch_id();
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, epoch_id)?;
        let prev_block = self.chain_store.get_block(prev_block_hash)?;
        let last_header = self.epoch_manager.get_prev_chunk_header(&prev_block, shard_id)?;

        let Ok(prev_chunk_extra) = self.chain_store.get_chunk_extra(prev_block_hash, &shard_uid)
        else {
            // We don't have chunk extra so we can't quickly validate the chunk.
            return Ok(false);
        };

        let Some(new_transactions) = state_witness.new_transactions() else {
            return Err(Error::Other("No new transactions in state witness".to_string()));
        };

        match validate_chunk_with_chunk_extra_and_roots(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            prev_block_hash,
            &prev_chunk_extra,
            last_header.height_included(),
            &chunk_header,
            new_transactions,
            self.rs.as_ref(),
        ) {
            Ok(()) => {
                send_chunk_endorsement_to_block_producers(
                    &chunk_header,
                    self.epoch_manager.as_ref(),
                    signer.as_ref(),
                    &self.network_adapter,
                );
                Ok(true)
            }
            Err(err) => {
                tracing::error!(
                    target: "chunk_validation",
                    ?err,
                    ?chunk_producer_name,
                    ?chunk_production_key,
                    "Failed to validate chunk using existing chunk extra",
                );
                CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                    .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                    .inc();
                Err(err)
            }
        }
    }

    fn start_validating_chunk(
        &self,
        state_witness: ChunkStateWitness,
        signer: &Arc<ValidatorSigner>,
        save_witness_if_invalid: bool,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let WitnessProductionKey { chunk: chunk_production_key, witness_type } =
            state_witness.production_key();
        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "start_validating_chunk",
            height = %chunk_production_key.height_created,
            shard_id = %chunk_production_key.shard_id,
            validator = %signer.validator_id(),
            tag_block_production = true,
            tag_witness_distribution = true,
        )
        .entered();

        // If we are not optimistic, this will be the right chunk header to endorse.
        let prev_block_hash = *state_witness.latest_chunk_header().prev_block_hash();
        let shard_id = state_witness.latest_chunk_header().shard_id();
        let chunk_header = state_witness.latest_chunk_header().clone();
        let chunk_producer_name =
            self.epoch_manager.get_chunk_producer_info(&chunk_production_key)?.take_account_id();

        let pre_validation_result = if witness_type != WitnessType::Optimistic {
            let expected_epoch_id =
                self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
            if expected_epoch_id != chunk_production_key.epoch_id {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "Invalid EpochId {:?} for previous block {}, expected {:?}",
                    chunk_production_key.epoch_id, prev_block_hash, expected_epoch_id
                )));
            }
            chunk_validation::pre_validate_chunk_state_witness(
                &state_witness,
                &self.chain_store,
                self.genesis_block.clone(),
                self.epoch_manager.as_ref(),
            )
            .map_err(|err| {
                CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                    .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                    .inc();
                tracing::error!(
                    target: "chunk_validation",
                    ?err,
                    ?chunk_producer_name,
                    ?chunk_production_key,
                    "Failed to pre-validate chunk state witness"
                );
                err
            })?
        } else {
            let prev_block_header = self.chain_store.get_block_header(&prev_block_hash)?;
            let transaction_validity_check_results = state_witness
                .transactions()
                .iter()
                .map(|t| {
                    self.chain_store
                        .check_transaction_validity_period(
                            &prev_block_header,
                            t.transaction.block_hash(),
                        )
                        .is_ok()
                })
                .collect_vec();
            let transactions = SignedValidPeriodTransactions::new(
                state_witness.transactions().clone(),
                transaction_validity_check_results,
            );
            let block_context = state_witness.block_context();
            let chunk_headers = state_witness.chunks();
            let receipts = state_witness.raw_receipts();
            let main_transition_params = MainTransition::NewChunk {
                new_chunk_data: NewChunkData {
                    chunk: Some(chunk_header.clone()),
                    gas_limit: chunk_header.gas_limit(),
                    prev_state_root: chunk_header.prev_state_root(),
                    chunk_hash: Some(chunk_header.chunk_hash().clone()),
                    prev_validator_proposals: chunk_header.prev_validator_proposals().collect(),
                    transactions,
                    receipts: receipts.clone(),
                    block: block_context.clone(),
                    storage_context: StorageContext {
                        storage_data_source: StorageDataSource::Recorded(PartialStorage {
                            nodes: state_witness.main_state_transition().base_state.clone(),
                        }),
                        state_patch: Default::default(),
                    },
                },
                prev_hash: prev_block_hash,
                shard_id: chunk_header.shard_id(),
            };
            let cached_shard_update_key = Chain::get_cached_shard_update_key(
                &block_context.to_key_source(),
                chunk_headers.iter(),
                chunk_header.shard_id(),
            )?;
            PreValidationOutput {
                main_transition_params,
                implicit_transition_params: vec![],
                cached_shard_update_key,
            }
        };

        // if self.try_validate_chunk_with_chunk_extra(state_witness.clone(), signer)? {
        //     return Ok(());
        // }

        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        let cache = self.main_state_transition_result_cache.clone();
        let store = self.chain_store.store();
        let network_adapter = self.network_adapter.clone();
        let signer = signer.clone();
        let rs = self.rs.clone();

        self.validation_spawner.spawn("stateless_validation", move || {
            // Capture the processing_done_tracker here - it will be dropped when this closure completes
            let _processing_done_tracker = processing_done_tracker;

            if let Err(err) = chunk_validation::validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                &cache,
                store,
                save_witness_if_invalid,
                rs,
            ) {
                near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                    .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                    .inc();
                println!("FAILED TO VALIDATE CHUNK STATE WITNESS: {:?}", err);
                tracing::error!(
                    target: "chunk_validation",
                    ?err,
                    ?chunk_producer_name,
                    ?chunk_production_key,
                    "Failed to validate chunk state witness"
                );
                return;
            }

            println!("SUCCESSFULLY VALIDATED WITNESS: {:?}", witness_type);
            if witness_type != WitnessType::Optimistic {
                println!("SENDING ENDORSEMENT FOR WITNESS: {:?}", witness_type);
                send_chunk_endorsement_to_block_producers(
                    &chunk_header,
                    epoch_manager.as_ref(),
                    signer.as_ref(),
                    &network_adapter,
                );
            }
        });

        Ok(())
    }

    pub fn process_chunk_state_witness_message(
        &mut self,
        msg: ChunkStateWitnessMessage,
    ) -> Result<(), Error> {
        let ChunkStateWitnessMessage { witness, raw_witness_size, processing_done_tracker } = msg;

        // Check if we're a validator
        if self.validator_signer.get().is_none() {
            const ERROR_MSG: &str = "Received chunk state witness but this is not a validator node";
            tracing::warn!(
                target: "chunk_validation",
                ERROR_MSG
            );
            return Err(Error::Other(ERROR_MSG.to_string()));
        }

        // Send acknowledgement back to the chunk producer
        if let Err(err) = self.send_state_witness_ack(&witness) {
            tracing::error!(target: "chunk_validation", ?err, "Failed to send state witness ack");
            return Err(err);
        }

        // Save the witness if configured to do so
        if self.save_latest_witnesses {
            if let Err(err) = self.chain_store.save_latest_chunk_state_witness(&witness) {
                tracing::error!(target: "chunk_validation", ?err, "Failed to save latest witness");
            }
        }

        // Check if previous block exists to know whether or not this witness is an orphan
        // new: do separate orphan check for OW and W
        let witness_type = witness.production_key().witness_type;
        let can_process = if witness_type != WitnessType::Optimistic {
            let prev_block_hash = *witness.latest_chunk_header().prev_block_hash();
            if let Err(err) = self.chain_store.get_block(&prev_block_hash) {
                tracing::error!(target: "chunk_validation", ?err, "Error getting previous block");
                Err(err)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };

        match can_process {
            Ok(()) => match self.process_chunk_state_witness(witness, processing_done_tracker) {
                Ok(()) => {
                    tracing::debug!(target: "chunk_validation", "Chunk witness validation started successfully");
                    Ok(())
                }
                Err(err) => {
                    tracing::error!(target: "chunk_validation", ?err, "Failed to start chunk witness validation");
                    Err(err)
                }
            },
            Err(_) => match self.handle_orphan_witness(witness, raw_witness_size) {
                Ok(outcome) => {
                    tracing::debug!(target: "chunk_validation", ?outcome, "Orphan witness handled");
                    Ok(())
                }
                Err(err) => {
                    tracing::error!(target: "chunk_validation", ?err, "Failed to handle orphan witness");
                    Err(err)
                }
            },
        }
    }
}

impl Handler<ChunkStateWitnessMessage> for ChunkValidationActorInner {
    #[perf]
    fn handle(&mut self, msg: ChunkStateWitnessMessage) {
        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "handle_chunk_state_witness",
            chunk_hash = ?msg.witness.latest_chunk_header().chunk_hash(),
            height = %msg.witness.latest_chunk_header().height_created(),
            shard_id = %msg.witness.latest_chunk_header().shard_id(),
            tag_witness_distribution = true,
        )
        .entered();

        // Error handling is done inside the called function
        let _ = self.process_chunk_state_witness_message(msg);
    }
}

impl Handler<BlockNotificationMessage> for ChunkValidationActorInner {
    #[perf]
    fn handle(&mut self, msg: BlockNotificationMessage) {
        let BlockNotificationMessage { block } = msg;

        self.handle_block_notification(&block);
    }
}
