use std::sync::Arc;

use near_async::messaging::{CanSend, Sender};
use near_chain::types::RuntimeAdapter;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::checked_feature;
use near_primitives::chunk_validation::{
    ChunkEndorsement, ChunkEndorsementInner, ChunkEndorsementMessage, ChunkStateWitness,
};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::EpochId;
use near_primitives::validator_signer::ValidatorSigner;

use crate::Client;

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
}

impl ChunkValidator {
    pub fn new(
        my_signer: Option<Arc<dyn ValidatorSigner>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        Self { my_signer, epoch_manager, network_sender, runtime_adapter }
    }

    /// Performs the chunk validation logic. When done, it will send the chunk
    /// endorsement message to the block producer. The actual validation logic
    /// happens in a separate thread.
    pub fn start_validating_chunk(&self, state_witness: ChunkStateWitness) -> Result<(), Error> {
        let Some(my_signer) = self.my_signer.as_ref() else {
            return Err(Error::NotAValidator);
        };
        let chunk_header = state_witness.chunk_header.clone();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        // We will only validate something if we are a chunk validator for this chunk.
        // Note this also covers the case before the protocol upgrade for chunk validators,
        // because the chunk validators will be empty.
        let chunk_validators = self.epoch_manager.get_chunk_validators(
            &epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )?;
        if !chunk_validators.contains_key(my_signer.validator_id()) {
            return Err(Error::NotAChunkValidator);
        }
        let block_producer =
            self.epoch_manager.get_block_producer(&epoch_id, chunk_header.height_created())?;

        let network_sender = self.network_sender.clone();
        let signer = self.my_signer.clone().unwrap();
        let runtime_adapter = self.runtime_adapter.clone();
        rayon::spawn(move || match validate_chunk(&state_witness, runtime_adapter.as_ref()) {
            Ok(()) => {
                tracing::debug!(
                    target: "chunk_validation",
                    chunk_hash=?chunk_header.chunk_hash(),
                    block_producer=%block_producer,
                    "Chunk validated successfully, sending endorsement",
                );
                let endorsement_to_sign = ChunkEndorsementInner::new(chunk_header.chunk_hash());
                network_sender.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::ChunkEndorsement(ChunkEndorsementMessage {
                        endorsement: ChunkEndorsement {
                            account_id: signer.validator_id().clone(),
                            signature: signer.sign_chunk_endorsement(&endorsement_to_sign),
                            inner: endorsement_to_sign,
                        },
                        target: block_producer,
                    }),
                ));
            }
            Err(err) => {
                tracing::error!("Failed to validate chunk: {:?}", err);
            }
        });
        Ok(())
    }
}

/// The actual chunk validation logic.
fn validate_chunk(
    state_witness: &ChunkStateWitness,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<(), Error> {
    // TODO: Replace this with actual stateless validation logic.
    if state_witness.state_root != state_witness.chunk_header.prev_state_root() {
        return Err(Error::InvalidChunkStateWitness);
    }
    // We'll need the runtime no matter what so just leaving it here to avoid an
    // unused variable warning.
    runtime_adapter.get_tries();
    Ok(())
}

impl Client {
    /// Responds to a network request to verify a `ChunkStateWitness`, which is
    /// sent by chunk producers after they produce a chunk.
    pub fn process_chunk_state_witness(&mut self, witness: ChunkStateWitness) -> Result<(), Error> {
        // TODO(#10265): We'll need to fetch some data from the chain; at the very least we need
        // the previous block to exist, and we need the previous chunks' receipt roots.
        // Some of this depends on delayed chunk execution. Also, if the previous block
        // does not exist, we should queue this (similar to orphans) to retry later.
        // For now though, we just pass it to the chunk validation logic.
        self.chunk_validator.start_validating_chunk(witness)
    }

    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        chunk_header: &ShardChunkHeader,
    ) -> Result<(), Error> {
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        if !checked_feature!("stable", ChunkValidation, protocol_version) {
            return Ok(());
        }
        let chunk_validators = self.epoch_manager.get_chunk_validators(
            epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )?;
        let witness = ChunkStateWitness {
            chunk_header: chunk_header.clone(),
            state_root: chunk_header.prev_state_root(),
        };
        tracing::debug!(
            target: "chunk_validation",
            "Sending chunk state witness for chunk {:?} to chunk validators {:?}",
            chunk_header.chunk_hash(),
            chunk_validators.keys(),
        );
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitness(chunk_validators.into_keys().collect(), witness),
        ));
        Ok(())
    }
}
