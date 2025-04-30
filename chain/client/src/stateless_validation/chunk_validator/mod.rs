pub mod orphan_witness_handling;
pub mod orphan_witness_pool;

use crate::Client;
use itertools::Itertools;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Sender};
use near_chain::stateless_validation::chunk_validation;
use near_chain::stateless_validation::processing_tracker::ProcessingDoneTracker;
use near_chain::types::RuntimeAdapter;
use near_chain::validate::validate_chunk_with_chunk_extra;
use near_chain::{Block, Chain};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_o11y::log_assert;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessAck, ChunkStateWitnessSize,
};
use near_primitives::validator_signer::ValidatorSigner;
use orphan_witness_pool::OrphanStateWitnessPool;
use std::sync::Arc;

// After validating a chunk state witness, we ideally need to send the chunk endorsement
// to just the next block producer at height h. However, it's possible that blocks at height
// h may be skipped and block producer at height h+1 picks up the chunk. We need to ensure
// that these later block producers also receive the chunk endorsement.
// Keeping a threshold of 5 block producers should be sufficient for most scenarios.
const NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT: u64 = 5;

/// A module that handles chunk validation logic. Chunk validation refers to a
/// critical process of stateless validation, where chunk validators (certain
/// validators selected to validate the chunk) verify that the chunk's state
/// witness is correct, and then send chunk endorsements to the block producer
/// so that the chunk can be included in the block.
pub struct ChunkValidator {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_sender: Sender<PeerManagerMessageRequest>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    orphan_witness_pool: OrphanStateWitnessPool,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,
    main_state_transition_result_cache: chunk_validation::MainStateTransitionCache,
}

impl ChunkValidator {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        orphan_witness_pool_size: usize,
        validation_spawner: Arc<dyn AsyncComputationSpawner>,
    ) -> Self {
        Self {
            epoch_manager,
            network_sender,
            runtime_adapter,
            orphan_witness_pool: OrphanStateWitnessPool::new(orphan_witness_pool_size),
            validation_spawner,
            main_state_transition_result_cache: chunk_validation::MainStateTransitionCache::default(
            ),
        }
    }

    /// Performs the chunk validation logic. When done, it will send the chunk
    /// endorsement message to the block producer. The actual validation logic
    /// happens in a separate thread.
    /// The chunk is validated asynchronously, if you want to wait for the processing to finish
    /// you can use the `processing_done_tracker` argument (but it's optional, it's safe to pass None there).
    fn start_validating_chunk(
        &self,
        state_witness: ChunkStateWitness,
        chain: &Chain,
        processing_done_tracker: Option<ProcessingDoneTracker>,
        signer: &Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let prev_block_hash = state_witness.chunk_header().prev_block_hash();
        let ChunkProductionKey { epoch_id, .. } = state_witness.chunk_production_key();
        let shard_id = state_witness.chunk_header().shard_id();
        let expected_epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        if expected_epoch_id != epoch_id {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Invalid EpochId {:?} for previous block {}, expected {:?}",
                epoch_id, prev_block_hash, expected_epoch_id
            )));
        }

        let pre_validation_result = chunk_validation::pre_validate_chunk_state_witness(
            &state_witness,
            chain,
            self.epoch_manager.as_ref(),
        )?;

        let chunk_header = state_witness.chunk_header().clone();
        let network_sender = self.network_sender.clone();
        let epoch_manager = self.epoch_manager.clone();

        // If we have the chunk extra for the previous block, we can validate
        // the chunk without state witness.
        // This usually happens because we are a chunk producer and
        // therefore have the chunk extra for the previous block saved on disk.
        // We can also skip validating the chunk state witness in this case.
        // We don't need to switch to parent shard uid, because resharding
        // creates chunk extra for new shard uid.
        let shard_uid = shard_id_to_uid(epoch_manager.as_ref(), shard_id, &expected_epoch_id)?;
        let prev_block = chain.get_block(prev_block_hash)?;
        let last_header =
            Chain::get_prev_chunk_header(epoch_manager.as_ref(), &prev_block, shard_id)?;

        let chunk_production_key = ChunkProductionKey {
            shard_id,
            epoch_id: expected_epoch_id,
            height_created: chunk_header.height_created(),
        };
        let chunk_producer_name =
            epoch_manager.get_chunk_producer_info(&chunk_production_key)?.take_account_id();

        if let Ok(prev_chunk_extra) = chain.get_chunk_extra(prev_block_hash, &shard_uid) {
            match validate_chunk_with_chunk_extra(
                chain.chain_store(),
                self.epoch_manager.as_ref(),
                prev_block_hash,
                &prev_chunk_extra,
                last_header.height_included(),
                &chunk_header,
            ) {
                Ok(()) => {
                    send_chunk_endorsement_to_block_producers(
                        &chunk_header,
                        epoch_manager.as_ref(),
                        signer,
                        &network_sender,
                    );
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!(
                        target: "client",
                        ?err,
                        ?chunk_producer_name,
                        ?chunk_production_key,
                        "Failed to validate chunk using existing chunk extra",
                    );
                    near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                        .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                        .inc();
                    return Err(err);
                }
            }
        }

        let runtime_adapter = self.runtime_adapter.clone();
        let cache = self.main_state_transition_result_cache.clone();
        let signer = signer.clone();
        self.validation_spawner.spawn("stateless_validation", move || {
            // processing_done_tracker must survive until the processing is finished.
            let _processing_done_tracker_capture: Option<ProcessingDoneTracker> =
                processing_done_tracker;

            match chunk_validation::validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                &cache,
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
                        target: "client",
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

/// Sends the chunk endorsement to the next
/// `NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT` block producers.
/// Additionally returns chunk endorsement if the signer is one of these block
/// producers, to be able to process it immediately.
pub(crate) fn send_chunk_endorsement_to_block_producers(
    chunk_header: &ShardChunkHeader,
    epoch_manager: &dyn EpochManagerAdapter,
    signer: &ValidatorSigner,
    network_sender: &Sender<PeerManagerMessageRequest>,
) -> Option<ChunkEndorsement> {
    let epoch_id =
        epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash()).unwrap();

    // Send the chunk endorsement to the next NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT block producers.
    // It's possible we may reach the end of the epoch, in which case, ignore the error from get_block_producer.
    // It is possible that the same validator appears multiple times in the upcoming block producers,
    // thus we collect the unique set of account ids.
    let block_height = chunk_header.height_created();
    let block_producers = (0..NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT)
        .map_while(|i| epoch_manager.get_block_producer(&epoch_id, block_height + i).ok())
        .unique()
        .collect_vec();
    assert!(!block_producers.is_empty());

    let chunk_hash = chunk_header.chunk_hash();
    tracing::debug!(
        target: "client",
        chunk_hash=?chunk_hash,
        shard_id=?chunk_header.shard_id(),
        ?block_producers,
        "send_chunk_endorsement",
    );

    let endorsement = ChunkEndorsement::new(epoch_id, chunk_header, signer);
    let mut send_to_itself = None;
    for block_producer in block_producers {
        if &block_producer == signer.validator_id() {
            send_to_itself = Some(endorsement.clone());
        }
        network_sender.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkEndorsement(block_producer, endorsement.clone()),
        ));
    }
    send_to_itself
}

impl Client {
    /// Responds to a network request to verify a `ChunkStateWitness`, which is
    /// sent by chunk producers after they produce a chunk.
    /// State witness is processed asynchronously, if you want to wait for the processing to finish
    /// you can use the `processing_done_tracker` argument (but it's optional, it's safe to pass None there).
    pub fn process_chunk_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        raw_witness_size: ChunkStateWitnessSize,
        processing_done_tracker: Option<ProcessingDoneTracker>,
        signer: Option<Arc<ValidatorSigner>>,
    ) -> Result<(), Error> {
        tracing::debug!(
            target: "client",
            chunk_hash=?witness.chunk_header().chunk_hash(),
            shard_id=?witness.chunk_header().shard_id(),
            "process_chunk_state_witness",
        );

        // Chunk producers should not receive state witness from themselves.
        log_assert!(
            signer.is_some(),
            "Received a chunk state witness but this is not a validator node. Witness={:?}",
            witness
        );

        // Send the acknowledgement for the state witness back to the chunk producer.
        // This is currently used for network roundtrip time measurement, so we do not need to
        // wait for validation to finish.
        self.send_state_witness_ack(&witness)?;

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&witness)?;
        }

        let signer = signer.unwrap();
        match self.chain.get_block(witness.chunk_header().prev_block_hash()) {
            Ok(block) => self.process_chunk_state_witness_with_prev_block(
                witness,
                &block,
                processing_done_tracker,
                &signer,
            ),
            Err(Error::DBNotFoundErr(_)) => {
                // Previous block isn't available at the moment, add this witness to the orphan pool.
                self.handle_orphan_state_witness(witness, raw_witness_size)?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn send_state_witness_ack(&self, witness: &ChunkStateWitness) -> Result<(), Error> {
        let chunk_producer = self
            .epoch_manager
            .get_chunk_producer_info(&witness.chunk_production_key())?
            .account_id()
            .clone();
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitnessAck(
                chunk_producer,
                ChunkStateWitnessAck::new(witness),
            ),
        ));
        Ok(())
    }

    pub fn process_chunk_state_witness_with_prev_block(
        &mut self,
        witness: ChunkStateWitness,
        prev_block: &Block,
        processing_done_tracker: Option<ProcessingDoneTracker>,
        signer: &Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        if witness.chunk_header().prev_block_hash() != prev_block.hash() {
            return Err(Error::Other(format!(
                "process_chunk_state_witness_with_prev_block - prev_block doesn't match ({} != {})",
                witness.chunk_header().prev_block_hash(),
                prev_block.hash()
            )));
        }

        self.chunk_validator.start_validating_chunk(
            witness,
            &self.chain,
            processing_done_tracker,
            signer,
        )
    }
}
