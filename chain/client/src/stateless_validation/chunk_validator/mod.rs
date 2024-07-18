pub mod orphan_witness_handling;
pub mod orphan_witness_pool;

use crate::stateless_validation::chunk_endorsement_tracker::ChunkEndorsementTracker;
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
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_o11y::log_assert;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::{
    ChunkEndorsement, ChunkStateWitness, ChunkStateWitnessAck, ChunkStateWitnessSize,
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
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
    orphan_witness_pool: OrphanStateWitnessPool,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,
    main_state_transition_result_cache: chunk_validation::MainStateTransitionCache,
    /// If true, a chunk-witness validation error will lead to a panic.
    /// This is used for non-production environments, eg. mocknet and localnet,
    /// to quickly detect issues in validation code, and must NOT be set to true
    /// for mainnet and testnet.
    panic_on_validation_error: bool,
}

impl ChunkValidator {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
        orphan_witness_pool_size: usize,
        validation_spawner: Arc<dyn AsyncComputationSpawner>,
        panic_on_validation_error: bool,
    ) -> Self {
        Self {
            epoch_manager,
            network_sender,
            runtime_adapter,
            chunk_endorsement_tracker,
            orphan_witness_pool: OrphanStateWitnessPool::new(orphan_witness_pool_size),
            validation_spawner,
            main_state_transition_result_cache: chunk_validation::MainStateTransitionCache::default(
            ),
            panic_on_validation_error,
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
        let prev_block_hash = state_witness.chunk_header.prev_block_hash();
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        if epoch_id != state_witness.epoch_id {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Invalid EpochId {:?} for previous block {}, expected {:?}",
                state_witness.epoch_id, prev_block_hash, epoch_id
            )));
        }

        let pre_validation_result = chunk_validation::pre_validate_chunk_state_witness(
            &state_witness,
            chain,
            self.epoch_manager.as_ref(),
            self.runtime_adapter.as_ref(),
        )?;

        let chunk_header = state_witness.chunk_header.clone();
        let network_sender = self.network_sender.clone();
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
        let panic_on_validation_error = self.panic_on_validation_error;

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
                        chunk_endorsement_tracker.as_ref(),
                    );
                    return Ok(());
                }
                Err(err) => {
                    if panic_on_validation_error {
                        panic!("Failed to validate chunk using existing chunk extra: {:?}", err);
                    } else {
                        tracing::error!(
                            "Failed to validate chunk using existing chunk extra: {:?}",
                            err
                        );
                    }
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
                        chunk_endorsement_tracker.as_ref(),
                    );
                }
                Err(err) => {
                    if panic_on_validation_error {
                        panic!("Failed to validate chunk: {:?}", err);
                    } else {
                        tracing::error!("Failed to validate chunk: {:?}", err);
                    }
                }
            }
        });
        Ok(())
    }

    /// TESTING ONLY: Used to override the value of panic_on_validation_error, for example,
    /// when the chunks validation errors are expected when testing adversarial behavior and
    /// the test should not panic for the invalid chunks witnesses.
    #[cfg(feature = "test_features")]
    pub fn set_should_panic_on_validation_error(&mut self, value: bool) {
        self.panic_on_validation_error = value;
    }
}

pub(crate) fn send_chunk_endorsement_to_block_producers(
    chunk_header: &ShardChunkHeader,
    epoch_manager: &dyn EpochManagerAdapter,
    signer: &ValidatorSigner,
    network_sender: &Sender<PeerManagerMessageRequest>,
    chunk_endorsement_tracker: &ChunkEndorsementTracker,
) {
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
        ?block_producers,
        "send_chunk_endorsement",
    );

    let endorsement = ChunkEndorsement::new(chunk_header.chunk_hash(), signer);
    for block_producer in block_producers {
        if signer.validator_id() == &block_producer {
            // Our own endorsements are not always valid (see issue #11750).
            if let Err(err) = chunk_endorsement_tracker
                .process_chunk_endorsement(chunk_header, endorsement.clone())
            {
                tracing::warn!(
                    target: "client",
                    ?chunk_hash,
                    ?endorsement,
                    "Failed to process self chunk endorsement ({err:?})");
            }
        } else {
            network_sender.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::ChunkEndorsement(block_producer, endorsement.clone()),
            ));
        }
    }
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
            chunk_hash=?witness.chunk_header.chunk_hash(),
            shard_id=witness.chunk_header.shard_id(),
            "process_chunk_state_witness",
        );

        // Chunk producers should not receive state witness from themselves.
        log_assert!(
            signer.is_some(),
            "Received a chunk state witness but this is not a validator node. Witness={:?}",
            witness
        );
        let signer = signer.unwrap();

        // Send the acknowledgement for the state witness back to the chunk producer.
        // This is currently used for network roundtrip time measurement, so we do not need to
        // wait for validation to finish.
        self.send_state_witness_ack(&witness, &signer);

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&witness)?;
        }

        match self.chain.get_block(witness.chunk_header.prev_block_hash()) {
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

    fn send_state_witness_ack(&self, witness: &ChunkStateWitness, signer: &Arc<ValidatorSigner>) {
        // In production PartialWitnessActor does not forward a state witness to the chunk producer that
        // produced the witness. However some tests bypass PartialWitnessActor, thus when a chunk producer
        // receives its own state witness, we log a warning instead of panicking.
        // TODO: Make sure all tests run with "test_features" and panic for non-test builds.
        if signer.validator_id() == &witness.chunk_producer {
            tracing::warn!(
                "Validator {:?} received state witness from itself. Witness={:?}",
                signer.validator_id(),
                witness
            );
            return;
        }
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
        signer: &Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        if witness.chunk_header.prev_block_hash() != prev_block.hash() {
            return Err(Error::Other(format!(
                "process_chunk_state_witness_with_prev_block - prev_block doesn't match ({} != {})",
                witness.chunk_header.prev_block_hash(),
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
