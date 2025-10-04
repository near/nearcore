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
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{Actor, Handler, Sender};
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::{ActorSystem, Message, MultiSend, MultiSenderFrom};
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::stateless_validation::chunk_validation::{self, MainStateTransitionCache};
use near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL;
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
use near_chain::types::RuntimeAdapter;
use near_chain::validate::validate_chunk_with_chunk_extra_and_roots;
use near_chain::{ChainStore, ChainStoreAccess, Error};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_performance_metrics_macros::perf;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessAck, ChunkStateWitnessSize,
};
use near_primitives::types::BlockHeight;
use near_primitives::validator_signer::ValidatorSigner;
use parking_lot::Mutex;
use reed_solomon_erasure::galois_8::ReedSolomon;
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
#[derive(Message, Debug)]
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
                max_orphan_witness_size,
            )
        })
    }

    fn send_state_witness_ack(&self, witness: &ChunkStateWitness) -> Result<(), Error> {
        let chunk_producer = self
            .epoch_manager
            .get_chunk_producer_info(&witness.chunk_production_key())?
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
        let chunk_header = witness.chunk_header();
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
            let header = witness.chunk_header();
            tracing::debug!(
                target: "chunk_validation",
                witness_height = header.height_created(),
                witness_shard = ?header.shard_id(),
                witness_chunk = ?header.chunk_hash(),
                witness_prev_block = ?header.prev_block_hash(),
                "Processing an orphaned ChunkStateWitness, its previous block has arrived."
            );

            if let Err(err) = self.process_chunk_state_witness(witness, new_block, None) {
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
        prev_block: &Block,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "process_chunk_state_witness",
            chunk_hash = ?witness.chunk_header().chunk_hash(),
            height = %witness.chunk_header().height_created(),
            shard_id = %witness.chunk_header().shard_id(),
        )
        .entered();

        // Validate that block hash matches
        if witness.chunk_header().prev_block_hash() != prev_block.hash() {
            return Err(Error::Other(format!(
                "Previous block hash mismatch: witness={}, block={}",
                witness.chunk_header().prev_block_hash(),
                prev_block.hash()
            )));
        }

        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("No validator signer available".to_string()));
        };

        self.start_validating_chunk(
            witness,
            &signer,
            self.save_invalid_witnesses,
            processing_done_tracker,
        )
    }

    fn start_validating_chunk(
        &self,
        state_witness: ChunkStateWitness,
        signer: &Arc<ValidatorSigner>,
        save_witness_if_invalid: bool,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "start_validating_chunk",
            height = %state_witness.chunk_production_key().height_created,
            shard_id = %state_witness.chunk_production_key().shard_id,
            validator = %signer.validator_id(),
            tag_block_production = true,
            tag_witness_distribution = true,
        )
        .entered();

        let prev_block_hash = *state_witness.chunk_header().prev_block_hash();
        let chunk_production_key = state_witness.chunk_production_key();
        let shard_id = state_witness.chunk_header().shard_id();
        let chunk_header = state_witness.chunk_header().clone();
        let chunk_producer_name =
            self.epoch_manager.get_chunk_producer_info(&chunk_production_key)?.take_account_id();

        let expected_epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
        if expected_epoch_id != chunk_production_key.epoch_id {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Invalid EpochId {:?} for previous block {}, expected {:?}",
                chunk_production_key.epoch_id, prev_block_hash, expected_epoch_id
            )));
        }

        let pre_validation_result = chunk_validation::pre_validate_chunk_state_witness(
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
        })?;

        // If we have the chunk extra for the previous block, we can validate
        // the chunk without state witness.
        // This usually happens because we are a chunk producer and
        // therefore have the chunk extra for the previous block saved on disk.
        // We can also skip validating the chunk state witness in this case.
        // We don't need to switch to parent shard uid, because resharding
        // creates chunk extra for new shard uid.
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &expected_epoch_id)?;
        let prev_block = self.chain_store.get_block(&prev_block_hash)?;
        let last_header = self.epoch_manager.get_prev_chunk_header(&prev_block, shard_id)?;

        if let Ok(prev_chunk_extra) = self.chain_store.get_chunk_extra(&prev_block_hash, &shard_uid)
        {
            match validate_chunk_with_chunk_extra_and_roots(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                &prev_block_hash,
                &prev_chunk_extra,
                last_header.height_included(),
                &chunk_header,
                state_witness.new_transactions(),
                self.rs.as_ref(),
            ) {
                Ok(()) => {
                    send_chunk_endorsement_to_block_producers(
                        &chunk_header,
                        self.epoch_manager.as_ref(),
                        signer.as_ref(),
                        &self.network_adapter,
                    );
                    return Ok(());
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
                    return Err(err);
                }
            }
        }

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

            match chunk_validation::validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                &cache,
                store,
                save_witness_if_invalid,
                rs,
            ) {
                Ok(_) => {
                    send_chunk_endorsement_to_block_producers(
                        &chunk_header,
                        epoch_manager.as_ref(),
                        signer.as_ref(),
                        &network_adapter,
                    );
                }
                Err(err) => {
                    near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                        .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                        .inc();
                    tracing::error!(
                        target: "chunk_validation",
                        ?err,
                        ?chunk_producer_name,
                        ?chunk_production_key,
                        "Failed to validate chunk state witness"
                    );
                }
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
        let prev_block_hash = *witness.chunk_header().prev_block_hash();
        match self.chain_store.get_block(&prev_block_hash) {
            Ok(prev_block) => {
                // Previous block exists
                match self.process_chunk_state_witness(
                    witness,
                    &prev_block,
                    processing_done_tracker,
                ) {
                    Ok(()) => {
                        tracing::debug!(target: "chunk_validation", "Chunk witness validation started successfully");
                        Ok(())
                    }
                    Err(err) => {
                        tracing::error!(target: "chunk_validation", ?err, "Failed to start chunk witness validation");
                        Err(err)
                    }
                }
            }
            Err(Error::DBNotFoundErr(_)) => {
                // Previous block isn't available at the moment - handle as orphan
                tracing::debug!(
                    target: "chunk_validation",
                    "Previous block not found - handling as orphan witness"
                );
                match self.handle_orphan_witness(witness, raw_witness_size) {
                    Ok(outcome) => {
                        tracing::debug!(target: "chunk_validation", ?outcome, "Orphan witness handled");
                        Ok(())
                    }
                    Err(err) => {
                        tracing::error!(target: "chunk_validation", ?err, "Failed to handle orphan witness");
                        Err(err)
                    }
                }
            }
            Err(err) => {
                tracing::error!(target: "chunk_validation", ?err, "Failed to get previous block");
                Err(err)
            }
        }
    }
}

impl Handler<ChunkStateWitnessMessage> for ChunkValidationActorInner {
    #[perf]
    fn handle(&mut self, msg: ChunkStateWitnessMessage) {
        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "handle_chunk_state_witness",
            chunk_hash = ?msg.witness.chunk_header().chunk_hash(),
            height = %msg.witness.chunk_header().height_created(),
            shard_id = %msg.witness.chunk_header().shard_id(),
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
