pub mod orphan_witness_handling;
pub mod orphan_witness_pool;

use super::processing_tracker::ProcessingDoneTracker;
use crate::stateless_validation::chunk_endorsement_tracker::ChunkEndorsementTracker;
use crate::Client;
use itertools::Itertools;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Sender};
use near_chain::stateless_validation::state_witness;
use near_chain::stateless_validation::state_witness::MainStateTransitionCache;
use near_chain::types::RuntimeAdapter;
use near_chain::validate::validate_chunk_with_chunk_extra;
use near_chain::{Block, Chain};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::{
    ChunkEndorsement, ChunkStateWitness, ChunkStateWitnessAck, ChunkStateWitnessSize,
    EncodedChunkStateWitness, SignedEncodedChunkStateWitness,
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
    /// The signer for our own node, if we are a validator. If not, this is None.
    my_signer: Option<Arc<dyn ValidatorSigner>>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_sender: Sender<PeerManagerMessageRequest>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
    orphan_witness_pool: OrphanStateWitnessPool,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,
    main_state_transition_result_cache: MainStateTransitionCache,
}

impl ChunkValidator {
    pub fn new(
        my_signer: Option<Arc<dyn ValidatorSigner>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
        orphan_witness_pool_size: usize,
        validation_spawner: Arc<dyn AsyncComputationSpawner>,
    ) -> Self {
        Self {
            my_signer,
            epoch_manager,
            network_sender,
            runtime_adapter,
            chunk_endorsement_tracker,
            orphan_witness_pool: OrphanStateWitnessPool::new(orphan_witness_pool_size),
            validation_spawner,
            main_state_transition_result_cache: MainStateTransitionCache::default(),
        }
    }

    /// Performs the chunk validation logic. When done, it will send the chunk
    /// endorsement message to the block producer. The actual validation logic
    /// happens in a separate thread.
    /// The chunk is validated asynchronously, if you want to wait for the processing to finish
    /// you can use the `processing_done_tracker` argument (but it's optional, it's safe to pass None there).
    pub fn start_validating_chunk(
        &self,
        state_witness: ChunkStateWitness,
        chain: &Chain,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let prev_block_hash = state_witness.chunk_header.prev_block_hash();
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        if epoch_id != state_witness.epoch_id {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Invalid EpochId {:?} for previous block {}, expected {:?}",
                state_witness.epoch_id, prev_block_hash, epoch_id
            )));
        }

        let pre_validation_result = state_witness::pre_validate_chunk_state_witness(
            &state_witness,
            chain,
            self.epoch_manager.as_ref(),
            self.runtime_adapter.as_ref(),
        )?;

        let chunk_header = state_witness.chunk_header.clone();
        let network_sender = self.network_sender.clone();
        let signer = self.my_signer.as_ref().ok_or(Error::NotAValidator)?.clone();
        let chunk_endorsement_tracker = self.chunk_endorsement_tracker.clone();
        let epoch_manager = self.epoch_manager.clone();
        // If we have the chunk extra for the previous block, we can validate the chunk without state witness.
        // This usually happens because we are a chunk producer and
        // therefore have the chunk extra for the previous block saved on disk.
        // We can also skip validating the chunk state witness in this case.
        let prev_block = chain.get_block(prev_block_hash)?;
        let last_header = Chain::get_prev_chunk_header(
            epoch_manager.as_ref(),
            &prev_block,
            chunk_header.shard_id(),
        )?;
        let shard_uid = epoch_manager.shard_id_to_uid(last_header.shard_id(), &epoch_id)?;

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
                        signer.as_ref(),
                        &network_sender,
                        chunk_endorsement_tracker.as_ref(),
                    );
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!(
                        "Failed to validate chunk using existing chunk extra: {:?}",
                        err
                    );
                    return Err(err);
                }
            }
        }

        let runtime_adapter = self.runtime_adapter.clone();
        let cache = self.main_state_transition_result_cache.clone();
        self.validation_spawner.spawn("stateless_validation", move || {
            // processing_done_tracker must survive until the processing is finished.
            let _processing_done_tracker_capture: Option<ProcessingDoneTracker> =
                processing_done_tracker;

            match state_witness::validate_chunk_state_witness(
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
                        chunk_endorsement_tracker.as_ref(),
                    );
                }
                Err(err) => {
                    tracing::error!("Failed to validate chunk: {:?}", err);
                }
            }
        });
        Ok(())
    }
}

pub(crate) fn send_chunk_endorsement_to_block_producers(
    chunk_header: &ShardChunkHeader,
    epoch_manager: &dyn EpochManagerAdapter,
    signer: &dyn ValidatorSigner,
    network_sender: &Sender<PeerManagerMessageRequest>,
    chunk_endorsement_tracker: &ChunkEndorsementTracker,
) {
    let epoch_id =
        epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash()).unwrap();

    // Send the chunk endorsement to the next NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT block producers.
    // It's possible we may reach the end of the epoch, in which case, ignore the error from get_block_producer.
    let block_height = chunk_header.height_created();
    let block_producers = (0..NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT)
        .map_while(|i| epoch_manager.get_block_producer(&epoch_id, block_height + i).ok())
        .collect_vec();
    assert!(!block_producers.is_empty());

    let chunk_hash = chunk_header.chunk_hash();
    tracing::debug!(
        target: "client",
        chunk_hash=?chunk_hash,
        ?block_producers,
        "send_chunk_endorsement",
    );

    let endorsement = ChunkEndorsement::new(chunk_header.chunk_hash(), signer);
    for block_producer in block_producers {
        if signer.validator_id() == &block_producer {
            // Unwrap here as we always expect our own endorsements to be valid
            chunk_endorsement_tracker
                .process_chunk_endorsement(chunk_header, endorsement.clone())
                .unwrap();
        } else {
            network_sender.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::ChunkEndorsement(block_producer, endorsement.clone()),
            ));
        }
    }
}

impl Client {
    // TODO(stateless_validation): Remove this function after partial state witness impl
    pub fn process_signed_chunk_state_witness(
        &mut self,
        signed_witness: SignedEncodedChunkStateWitness,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        // TODO(stateless_validation): Inefficient, we are decoding the witness twice, but fine for temporary measure
        let (witness, _) = signed_witness.witness_bytes.decode()?;
        if !self.epoch_manager.verify_chunk_state_witness_signature(
            &signed_witness,
            &witness.chunk_producer,
            &witness.epoch_id,
        )? {
            return Err(Error::InvalidChunkStateWitness("Invalid signature".to_string()));
        }

        self.process_chunk_state_witness(signed_witness.witness_bytes, processing_done_tracker)?;

        Ok(())
    }

    /// Responds to a network request to verify a `ChunkStateWitness`, which is
    /// sent by chunk producers after they produce a chunk.
    /// State witness is processed asynchronously, if you want to wait for the processing to finish
    /// you can use the `processing_done_tracker` argument (but it's optional, it's safe to pass None there).
    pub fn process_chunk_state_witness(
        &mut self,
        encoded_witness: EncodedChunkStateWitness,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let (witness, raw_witness_size) =
            self.partially_validate_state_witness(&encoded_witness)?;

        tracing::debug!(
            target: "client",
            chunk_hash=?witness.chunk_header.chunk_hash(),
            shard_id=witness.chunk_header.shard_id(),
            "process_chunk_state_witness",
        );

        // Send the acknowledgement for the state witness back to the chunk producer.
        // This is currently used for network roundtrip time measurement, so we do not need to
        // wait for validation to finish.
        self.send_state_witness_ack(&witness);

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&witness)?;
        }

        // Avoid processing state witness for old chunks.
        // In particular it is impossible for a chunk created at a height
        // that doesn't exceed the height of the current final block to be
        // included in the chain. This addresses both network-delayed messages
        // as well as malicious behavior of a chunk producer.
        if let Ok(final_head) = self.chain.final_head() {
            if witness.chunk_header.height_created() <= final_head.height {
                tracing::warn!(
                    target: "client",
                    chunk_hash=?witness.chunk_header.chunk_hash(),
                    shard_id=witness.chunk_header.shard_id(),
                    witness_height=witness.chunk_header.height_created(),
                    final_height=final_head.height,
                    "Skipping state witness below the last final block",
                );
                return Ok(());
            }
        }

        match self.chain.get_block(witness.chunk_header.prev_block_hash()) {
            Ok(block) => self.process_chunk_state_witness_with_prev_block(
                witness,
                &block,
                processing_done_tracker,
            ),
            Err(Error::DBNotFoundErr(_)) => {
                // Previous block isn't available at the moment, add this witness to the orphan pool.
                self.handle_orphan_state_witness(witness, raw_witness_size)?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn send_state_witness_ack(&self, witness: &ChunkStateWitness) {
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitnessAck(
                witness.chunk_producer.clone(),
                ChunkStateWitnessAck::new(witness),
            ),
        ));
    }

    pub fn process_chunk_state_witness_with_prev_block(
        &mut self,
        witness: ChunkStateWitness,
        prev_block: &Block,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        if witness.chunk_header.prev_block_hash() != prev_block.hash() {
            return Err(Error::Other(format!(
                "process_chunk_state_witness_with_prev_block - prev_block doesn't match ({} != {})",
                witness.chunk_header.prev_block_hash(),
                prev_block.hash()
            )));
        }

        self.chunk_validator.start_validating_chunk(witness, &self.chain, processing_done_tracker)
    }

    /// Performs state witness decoding and partial validation without requiring the previous block.
    /// Here we rely on epoch_id provided as part of the state witness. Later we verify that this
    /// epoch_id actually corresponds to the chunk's previous block.
    fn partially_validate_state_witness(
        &self,
        encoded_witness: &EncodedChunkStateWitness,
    ) -> Result<(ChunkStateWitness, ChunkStateWitnessSize), Error> {
        let decode_start = std::time::Instant::now();
        let (witness, raw_witness_size) = encoded_witness.decode()?;
        let decode_elapsed_seconds = decode_start.elapsed().as_secs_f64();
        let chunk_header = &witness.chunk_header;
        let witness_height = chunk_header.height_created();
        let witness_shard = chunk_header.shard_id();

        if !self
            .epoch_manager
            .get_shard_layout(&witness.epoch_id)?
            .shard_ids()
            .contains(&witness_shard)
        {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Invalid shard_id in ChunkStateWitness: {}",
                witness_shard
            )));
        }

        let chunk_producer = self.epoch_manager.get_chunk_producer(
            &witness.epoch_id,
            witness_height,
            witness_shard,
        )?;
        if witness.chunk_producer != chunk_producer {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Incorrect chunk producer for epoch {:?} at height {}: expected {}, got {}",
                witness.epoch_id, witness_height, chunk_producer, witness.chunk_producer,
            )));
        }

        // Reject witnesses for chunks for which this node isn't a validator.
        // It's an error, as chunk producer shouldn't send the witness to a non-validator node.
        let my_signer = self.chunk_validator.my_signer.as_ref().ok_or(Error::NotAValidator)?;
        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &witness.epoch_id,
            witness_shard,
            witness_height,
        )?;
        if !chunk_validator_assignments.contains(my_signer.validator_id()) {
            return Err(Error::NotAChunkValidator);
        }

        // Record metrics after validating the witness
        near_chain::metrics::CHUNK_STATE_WITNESS_DECODE_TIME
            .with_label_values(&[&witness_shard.to_string()])
            .observe(decode_elapsed_seconds);

        Ok((witness, raw_witness_size))
    }
}
