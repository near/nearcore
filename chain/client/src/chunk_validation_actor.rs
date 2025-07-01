use crate::stateless_validation::chunk_validator::orphan_witness_handling::ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD;
use crate::stateless_validation::chunk_validator::orphan_witness_pool::OrphanStateWitnessPool;
use crate::stateless_validation::chunk_validator::send_chunk_endorsement_to_block_producers;
use actix::Actor as ActixActor;
use near_async::actix_wrapper::ActixWrapper;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{Actor, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::stateless_validation::chunk_validation::{self, MainStateTransitionCache};
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
use near_chain::types::RuntimeAdapter;
use near_chain::validate::validate_chunk_with_chunk_extra;
use near_chain::{ChainStore, ChainStoreAccess, Error};
use near_chain_configs::{MutableValidatorSigner, default_orphan_state_witness_pool_size};
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
use std::sync::Arc;

pub type ChunkValidationActor = ActixWrapper<ChunkValidationActorInner>;

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
    pub orphan_witness: Sender<OrphanWitnessMessage>,
    pub block_notification: Sender<BlockNotificationMessage>,
}

/// Message for handling orphan witnesses that arrive before their previous block
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct OrphanWitnessMessage {
    pub witness: ChunkStateWitness,
    pub witness_size: ChunkStateWitnessSize,
}

/// Message to notify the chunk validation actor about new blocks
/// so it can process orphan witnesses that were waiting for these blocks
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct BlockNotificationMessage {
    pub block: Arc<Block>,
}

/// An actor for validating chunk state witnesses and orphan witnesses.
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
    orphan_witness_pool: OrphanStateWitnessPool,
    max_orphan_witness_size: u64,
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
        max_orphan_witness_size: u64,
    ) -> Self {
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
            orphan_witness_pool: OrphanStateWitnessPool::new(
                default_orphan_state_witness_pool_size(),
            ),
            max_orphan_witness_size,
        }
    }

    pub fn spawn_actix_actor(self) -> actix::Addr<ChunkValidationActor> {
        let actix_wrapper = ActixWrapper::new(self);
        let arbiter = actix::Arbiter::new().handle();
        ActixActor::start_in_arbiter(&arbiter, |_| actix_wrapper)
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

    /// Handle an orphan witness that arrived before its previous block
    fn handle_orphan_witness(
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

        // Get chain head to check distance
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

        // Check witness size limit
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
        self.orphan_witness_pool.add_orphan_state_witness(witness, witness_size_u64 as usize);
        Ok(HandleOrphanWitnessOutcome::SavedToPool)
    }

    /// Process orphan witnesses that are now ready because their previous block has arrived
    fn process_ready_orphan_witnesses(&mut self, new_block: &Block) {
        let ready_witnesses =
            self.orphan_witness_pool.take_state_witnesses_waiting_for_block(new_block.hash());

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

            if let Err(err) = self.process_chunk_state_witness_with_prev_block(witness, new_block) {
                tracing::error!(target: "chunk_validation", ?err, "Error processing orphan chunk state witness");
            }
        }
    }

    /// Clean old orphan witnesses and process ready ones when a new block arrives
    fn handle_block_notification(&mut self, new_block: &Block) {
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
        &mut self,
        witness: ChunkStateWitness,
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

        // Save the witness if configured to do so
        if self.save_latest_witnesses {
            if let Err(err) = self.chain_store.save_latest_chunk_state_witness(&witness) {
                tracing::error!(target: "chunk_validation", ?err, "Failed to save latest witness");
            }
        }

        // Get the previous block
        let prev_block_hash = *witness.chunk_header().prev_block_hash();
        let prev_block = self.chain_store.get_block(&prev_block_hash)?;

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

    /// Process a chunk state witness when we already have the previous block
    fn process_chunk_state_witness_with_prev_block(
        &mut self,
        witness: ChunkStateWitness,
        prev_block: &Block,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "process_chunk_state_witness_with_prev_block",
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

        // Save the witness if configured to do so
        if self.save_latest_witnesses {
            if let Err(err) = self.chain_store.save_latest_chunk_state_witness(&witness) {
                tracing::error!(target: "chunk_validation", ?err, "Failed to save latest witness");
            }
        }

        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("No validator signer available".to_string()));
        };

        self.start_validating_chunk(witness, &signer, self.save_invalid_witnesses, None)
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

        let network_sender = self.network_adapter.clone();
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        let chain_store = self.chain_store.clone();
        let genesis_block = self.genesis_block.clone();
        let store = self.chain_store.store();
        let cache = self.main_state_transition_result_cache.clone();
        let signer = signer.clone();

        self.validation_spawner.spawn("stateless_validation", move || {
            // Capture the processing_done_tracker here - it will be dropped when this closure completes
            let _processing_done_tracker = processing_done_tracker;
            let _span = tracing::debug_span!(
                target: "chunk_validation",
                "async_validating_chunk",
                height = %state_witness.chunk_production_key().height_created,
                shard_id = %state_witness.chunk_production_key().shard_id,
                validator = %signer.validator_id(),
                tag_block_production = true,
                tag_witness_distribution = true,
            )
            .entered();

            // Helper macro to avoid verbose error handling
            macro_rules! try_or_return {
                ($expr:expr) => {
                    match $expr {
                        Ok(val) => val,
                        Err(err) => {
                            tracing::error!(target: "chunk_validation", ?err, "Async stateless validation error");
                            return;
                        }
                    }
                };
            }

            // All expensive operations happen here in async task
            let expected_epoch_id = try_or_return!(epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash));

            if expected_epoch_id != chunk_production_key.epoch_id {
                tracing::error!(
                    target: "chunk_validation",
                    "Invalid EpochId {:?} for previous block {}, expected {:?}",
                    chunk_production_key.epoch_id, prev_block_hash, expected_epoch_id
                );
                return;
            }

            let shard_uid = try_or_return!(shard_id_to_uid(epoch_manager.as_ref(), shard_id, &expected_epoch_id));
            let prev_block = try_or_return!(chain_store.get_block(&prev_block_hash));
            let last_header = try_or_return!(epoch_manager.get_prev_chunk_header(&prev_block, shard_id));
            let chunk_producer_name = try_or_return!(epoch_manager.get_chunk_producer_info(&chunk_production_key)).take_account_id();

            // First check if we can validate using existing chunk extra (fast path)
            if let Ok(prev_chunk_extra) = chain_store.get_chunk_extra(&prev_block_hash, &shard_uid) {
                match validate_chunk_with_chunk_extra(
                    &chain_store,
                    epoch_manager.as_ref(),
                    &prev_block_hash,
                    &prev_chunk_extra,
                    last_header.height_included(),
                    &chunk_header,
                ) {
                    Ok(()) => {
                        send_chunk_endorsement_to_block_producers(
                            &chunk_header,
                            epoch_manager.as_ref(),
                            signer.as_ref(),
                            &network_sender,
                        );
                        return;
                    }
                    Err(err) => {
                        tracing::error!(
                            target: "chunk_validation",
                            ?err,
                            ?chunk_producer_name,
                            ?chunk_production_key,
                            "Failed to validate chunk using existing chunk extra",
                        );
                        near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                            .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                            .inc();
                        return;
                    }
                }
            }

            // If chunk extra validation failed or wasn't available, do full witness validation
            let pre_validation_result = match chunk_validation::pre_validate_chunk_state_witness(
                &state_witness,
                &chain_store,
                genesis_block,
                epoch_manager.as_ref(),
            ) {
                Ok(result) => result,
                Err(err) => {
                    near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                        .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                        .inc();
                    tracing::error!(
                        target: "chunk_validation",
                        ?err,
                        ?chunk_producer_name,
                        ?chunk_production_key,
                        "Failed to pre-validate chunk state witness"
                    );
                    return;
                }
            };

            match chunk_validation::validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                &cache,
                store,
                save_witness_if_invalid,
            ) {
                Ok(()) => {
                    send_chunk_endorsement_to_block_producers(
                        &chunk_header,
                        epoch_manager.as_ref(),
                        signer.as_ref(),
                        &network_sender,
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
                        "Failed to validate chunk"
                    );
                }
            }
        });
        Ok(())
    }
}

impl Handler<ChunkStateWitnessMessage> for ChunkValidationActorInner {
    #[perf]
    fn handle(&mut self, msg: ChunkStateWitnessMessage) {
        let ChunkStateWitnessMessage { witness, raw_witness_size: _, processing_done_tracker } =
            msg;

        let _span = tracing::debug_span!(
            target: "chunk_validation",
            "handle_chunk_state_witness",
            chunk_hash = ?witness.chunk_header().chunk_hash(),
            height = %witness.chunk_header().height_created(),
            shard_id = %witness.chunk_header().shard_id(),
            tag_witness_distribution = true,
        )
        .entered();

        // Check if we're a validator
        if self.validator_signer.get().is_none() {
            tracing::warn!(
                target: "chunk_validation",
                "Received chunk state witness but this is not a validator node"
            );
            return;
        }

        // Send acknowledgement back to the chunk producer
        if let Err(err) = self.send_state_witness_ack(&witness) {
            tracing::error!(target: "chunk_validation", ?err, "Failed to send state witness ack");
            return;
        }

        // Process the witness
        match self.process_chunk_state_witness(witness.clone(), processing_done_tracker) {
            Ok(()) => {
                tracing::debug!(target: "chunk_validation", "Chunk witness validation started successfully");
            }
            Err(Error::DBNotFoundErr(_)) => {
                // Previous block isn't available at the moment - handle as orphan
                tracing::debug!(
                    target: "chunk_validation",
                    "Previous block not found - handling as orphan witness"
                );
                let witness_size = msg.raw_witness_size;
                match self.handle_orphan_witness(witness, witness_size) {
                    Ok(outcome) => {
                        tracing::debug!(target: "chunk_validation", ?outcome, "Orphan witness handled");
                    }
                    Err(err) => {
                        tracing::error!(target: "chunk_validation", ?err, "Failed to handle orphan witness");
                    }
                }
            }
            Err(err) => {
                tracing::error!(target: "chunk_validation", ?err, "Failed to start chunk witness validation");
            }
        }
    }
}

impl Handler<OrphanWitnessMessage> for ChunkValidationActorInner {
    #[perf]
    fn handle(&mut self, msg: OrphanWitnessMessage) {
        let OrphanWitnessMessage { witness, witness_size } = msg;

        if let Err(err) = self.handle_orphan_witness(witness, witness_size) {
            tracing::error!(target: "chunk_validation", ?err, "Error handling orphan witness");
        }
    }
}

impl Handler<BlockNotificationMessage> for ChunkValidationActorInner {
    #[perf]
    fn handle(&mut self, msg: BlockNotificationMessage) {
        let BlockNotificationMessage { block } = msg;

        self.handle_block_notification(&block);
    }
}
