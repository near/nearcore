use std::sync::Arc;
use tracing::Span;

use near_async::messaging::{CanSend, Sender};
use near_chain::ChainStoreAccess;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::challenge::PartialState;
use near_primitives::checked_feature;
use near_primitives::chunk_validation::{
    ChunkEndorsement, ChunkEndorsementInner, ChunkEndorsementMessage, ChunkStateWitness,
};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::EpochId;
use near_primitives::utils::get_block_shard_id;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::DBCol;

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
}

impl ChunkValidator {
    pub fn new(
        my_signer: Option<Arc<dyn ValidatorSigner>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
    ) -> Self {
        Self { my_signer, epoch_manager, network_sender }
    }

    /// Performs the chunk validation logic. When done, it will send the chunk
    /// endorsement message to the block producer. The actual validation logic
    /// happens in a separate thread.
    pub fn start_validating_chunk(
        &self,
        chunk_header: ShardChunkHeader,
        job: Box<dyn FnOnce(&Span) -> Result<(), Error> + Send + 'static>,
    ) -> Result<(), Error> {
        let Some(my_signer) = self.my_signer.as_ref() else {
            return Err(Error::NotAValidator);
        };
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
        rayon::spawn(move || {
            let parent_span =
                tracing::debug_span!(target: "chain", "start_validating_chunk").entered();
            match job(&parent_span) {
                Ok(_) => {
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
            }
        });
        Ok(())
    }
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
        let chunk_header = witness.chunk_header.clone();
        let prev_block = self.chain.get_block(chunk_header.prev_block_hash())?;
        let prev_chunk_header = &prev_block.chunks()[chunk_header.shard_id() as usize];
        let job = self.chain.get_chunk_validation_job(
            witness,
            &prev_block,
            &chunk_header,
            prev_chunk_header,
        )?;
        match job {
            Some(job) => self.chunk_validator.start_validating_chunk(chunk_header, job)?,
            None => {
                tracing::debug!(
                    target: "chunk_validation",
                    "No job to validate state witness",
                );
            }
        }
        Ok(())
    }

    /// Collect state proofs to produce state witness for `chunk_header`.
    fn collect_state_proofs(
        &mut self,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: ShardChunkHeader,
    ) -> Result<Vec<PartialState>, Error> {
        let shard_id = chunk_header.shard_id();
        let prev_chunk_height_included = prev_chunk_header.height_included();
        let prev_chunk_prev_hash = *prev_chunk_header.prev_block_hash();

        // TODO(#9292): previous chunk is genesis chunk - consider proper
        // result for this corner case.
        if prev_chunk_prev_hash == CryptoHash::default() {
            return Ok(vec![]);
        }

        let mut prev_blocks = self.chain.get_blocks_until_height(
            *chunk_header.prev_block_hash(),
            prev_chunk_height_included,
            true,
        )?;
        prev_blocks.reverse();

        let mut state_proofs = vec![];
        let store = self.chain.store().store();
        for block_hash in prev_blocks {
            state_proofs.push(
                store
                    .get_ser(DBCol::StateProofs, &get_block_shard_id(&block_hash, shard_id))?
                    .ok_or(Error::Other(format!(
                        "Missing state proof for block {block_hash} and shard {shard_id}"
                    )))?,
            );
        }

        Ok(state_proofs)
    }

    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: ShardChunkHeader,
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
        let state_proofs = self.collect_state_proofs(chunk_header, prev_chunk_header)?;
        let witness = ChunkStateWitness { chunk_header: chunk_header.clone(), state_proofs };
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
