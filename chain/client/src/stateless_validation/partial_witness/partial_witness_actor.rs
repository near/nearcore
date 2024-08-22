use std::sync::Arc;

use itertools::Itertools;
use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_async::time::Clock;
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::Error;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::state_witness::{
    ChunkStateWitnessAckMessage, PartialEncodedStateWitnessForwardMessage,
    PartialEncodedStateWitnessMessage,
};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_performance_metrics_macros::perf;
use near_primitives::block::Tip;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::partial_witness::{
    PartialEncodedStateWitness, MAX_COMPRESSED_STATE_WITNESS_SIZE,
};
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessAck, EncodedChunkStateWitness,
};
use near_primitives::types::{AccountId, BlockHeightDelta, EpochId};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::{DBCol, Store, FINAL_HEAD_KEY, HEAD_KEY};

use crate::client_actor::ClientSenderForPartialWitness;
use crate::metrics;
use crate::stateless_validation::state_witness_tracker::ChunkStateWitnessTracker;

use super::encoding::{witness_part_length, WitnessEncoderCache};
use super::partial_witness_tracker::PartialEncodedStateWitnessTracker;

pub struct PartialWitnessActor {
    /// Adapter to send messages to the network.
    network_adapter: PeerManagerAdapter,
    /// Validator signer to sign the state witness. This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    my_signer: MutableValidatorSigner,
    /// Epoch manager to get the set of chunk validators
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Tracks the parts of the state witness sent from chunk producers to chunk validators.
    partial_witness_tracker: PartialEncodedStateWitnessTracker,
    /// Tracks a collection of state witnesses sent from chunk producers to chunk validators.
    state_witness_tracker: ChunkStateWitnessTracker,
    /// Reed Solomon encoder for encoding state witness parts.
    /// We keep one wrapper for each length of chunk_validators to avoid re-creating the encoder.
    encoders: WitnessEncoderCache,
    /// Currently used to find the chain HEAD when validating partial witnesses,
    /// but should be removed if we implement retrieving this info from the client
    store: Store,
}

/// This is taken to be the same value as near_chunks::chunk_cache::MAX_HEIGHTS_AHEAD, and we
/// reject partial witnesses with height more than this value above the height of our current HEAD
const MAX_HEIGHTS_AHEAD: BlockHeightDelta = 5;

impl Actor for PartialWitnessActor {}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeStateWitnessRequest {
    pub epoch_id: EpochId,
    pub chunk_header: ShardChunkHeader,
    pub state_witness: ChunkStateWitness,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct PartialWitnessSenderForClient {
    pub distribute_chunk_state_witness: Sender<DistributeStateWitnessRequest>,
}

impl Handler<DistributeStateWitnessRequest> for PartialWitnessActor {
    #[perf]
    fn handle(&mut self, msg: DistributeStateWitnessRequest) {
        if let Err(err) = self.handle_distribute_state_witness_request(msg) {
            tracing::error!(target: "client", ?err, "Failed to handle distribute chunk state witness request");
        }
    }
}

impl Handler<ChunkStateWitnessAckMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ChunkStateWitnessAckMessage) {
        self.handle_chunk_state_witness_ack(msg.0);
    }
}

impl Handler<PartialEncodedStateWitnessMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedStateWitnessMessage) {
        if let Err(err) = self.handle_partial_encoded_state_witness(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle PartialEncodedStateWitnessMessage");
        }
    }
}

impl Handler<PartialEncodedStateWitnessForwardMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedStateWitnessForwardMessage) {
        if let Err(err) = self.handle_partial_encoded_state_witness_forward(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle PartialEncodedStateWitnessForwardMessage");
        }
    }
}

impl PartialWitnessActor {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        client_sender: ClientSenderForPartialWitness,
        my_signer: MutableValidatorSigner,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        store: Store,
    ) -> Self {
        let partial_witness_tracker =
            PartialEncodedStateWitnessTracker::new(client_sender, epoch_manager.clone());
        Self {
            network_adapter,
            my_signer,
            epoch_manager,
            partial_witness_tracker,
            state_witness_tracker: ChunkStateWitnessTracker::new(clock),
            encoders: WitnessEncoderCache::new(),
            store,
        }
    }

    pub fn handle_distribute_state_witness_request(
        &mut self,
        msg: DistributeStateWitnessRequest,
    ) -> Result<(), Error> {
        let DistributeStateWitnessRequest { epoch_id, chunk_header, state_witness } = msg;

        tracing::debug!(
            target: "client",
            chunk_hash=?chunk_header.chunk_hash(),
            "distribute_chunk_state_witness",
        );

        let signer = match self.my_signer.get() {
            Some(signer) => signer,
            None => {
                return Err(Error::NotAValidator(format!("distribute state witness")));
            }
        };

        let witness_bytes = compress_witness(&state_witness)?;

        self.send_state_witness_parts(epoch_id, chunk_header, witness_bytes, &signer)?;

        Ok(())
    }

    // Function to generate the parts of the state witness and return them as a tuple of chunk_validator and part.
    fn generate_state_witness_parts(
        &mut self,
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        signer: &ValidatorSigner,
    ) -> Result<Vec<(AccountId, PartialEncodedStateWitness)>, Error> {
        let chunk_validators = self
            .epoch_manager
            .get_chunk_validator_assignments(
                &epoch_id,
                chunk_header.shard_id(),
                chunk_header.height_created(),
            )?
            .ordered_chunk_validators();

        tracing::debug!(
            target: "client",
            chunk_hash=?chunk_header.chunk_hash(),
            ?chunk_validators,
            "generate_state_witness_parts",
        );

        // Break the state witness into parts using Reed Solomon encoding.
        let encoder = self.encoders.entry(chunk_validators.len());
        let (parts, encoded_length) = encoder.encode(&witness_bytes);

        Ok(chunk_validators
            .iter()
            .zip_eq(parts)
            .enumerate()
            .map(|(part_ord, (chunk_validator, part))| {
                // It's fine to unwrap part here as we just constructed the parts above and we expect
                // all of them to be present.
                let partial_witness = PartialEncodedStateWitness::new(
                    epoch_id,
                    chunk_header.clone(),
                    part_ord,
                    part.unwrap().to_vec(),
                    encoded_length,
                    signer,
                );
                (chunk_validator.clone(), partial_witness)
            })
            .collect_vec())
    }

    // Break the state witness into parts and send each part to the corresponding chunk validator owner.
    // The chunk validator owner will then forward the part to all other chunk validators.
    // Each chunk validator would collect the parts and reconstruct the state witness.
    fn send_state_witness_parts(
        &mut self,
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        signer: &ValidatorSigner,
    ) -> Result<(), Error> {
        // Capture these values first, as the sources are consumed before calling record_witness_sent.
        let chunk_hash = chunk_header.chunk_hash();
        let witness_size_in_bytes = witness_bytes.size_bytes();

        // Record time taken to encode the state witness parts.
        let shard_id_label = chunk_header.shard_id().to_string();
        let encode_timer = metrics::PARTIAL_WITNESS_ENCODE_TIME
            .with_label_values(&[shard_id_label.as_str()])
            .start_timer();
        let validator_witness_tuple =
            self.generate_state_witness_parts(epoch_id, chunk_header, witness_bytes, signer)?;
        encode_timer.observe_duration();

        // Record the witness in order to match the incoming acks for measuring round-trip times.
        // See process_chunk_state_witness_ack for the handling of the ack messages.
        self.state_witness_tracker.record_witness_sent(
            chunk_hash,
            witness_size_in_bytes,
            validator_witness_tuple.len(),
        );

        // Send the parts to the corresponding chunk validator owners.
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple),
        ));
        Ok(())
    }

    /// Sends the witness part to the chunk validators, except for the following:
    /// 1) The current validator, 2) Chunk producer that originally generated the witness part.
    fn forward_state_witness_part(
        &self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        let chunk_validators = self
            .epoch_manager
            .get_chunk_validator_assignments(
                partial_witness.epoch_id(),
                partial_witness.shard_id(),
                partial_witness.height_created(),
            )?
            .ordered_chunk_validators();
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedStateWitnessForward(chunk_validators, partial_witness),
        ));
        Ok(())
    }

    /// Function to handle receiving partial_encoded_state_witness message from chunk producer.
    pub fn handle_partial_encoded_state_witness(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_witness, "Receive PartialEncodedStateWitnessMessage");

        let signer = self
            .my_signer
            .get()
            .ok_or(Error::NotAValidator(format!("handle partial encoded state witness")))?;

        // Validate the partial encoded state witness and forward the part to all the chunk validators.
        if self.validate_partial_encoded_state_witness(&partial_witness, &signer)? {
            self.forward_state_witness_part(partial_witness)?;
        }

        Ok(())
    }

    /// Function to handle receiving partial_encoded_state_witness_forward message from chunk producer.
    pub fn handle_partial_encoded_state_witness_forward(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        let signer = match self.my_signer.get() {
            Some(signer) => signer,
            None => {
                return Err(Error::NotAValidator(format!(
                    "handle partial encoded state witness forward"
                )));
            }
        };

        // Validate the partial encoded state witness and store the partial encoded state witness.
        if self.validate_partial_encoded_state_witness(&partial_witness, &signer)? {
            self.partial_witness_tracker.store_partial_encoded_state_witness(partial_witness)?;
        }

        Ok(())
    }

    /// Function to validate the partial encoded state witness. We check the following
    /// - shard_id is valid
    /// - we are one of the validators for the chunk
    /// - height_created is in (last_final_height..chain_head_height + MAX_HEIGHTS_AHEAD] range
    /// - epoch_id is within epoch_manager's possible_epochs_of_height_around_tip
    /// - part_ord is valid and within range of the number of expected parts for this chunk
    /// - partial_witness signature is valid and from the expected chunk_producer
    /// TODO(stateless_validation): Include checks from handle_orphan_state_witness in orphan_witness_handling.rs
    /// These include checks based on epoch_id validity, witness size, height_created, distance from chain head, etc.
    /// Returns:
    /// - Ok(true) if partial witness is valid and we should process it.
    /// - Ok(false) if partial witness is potentially valid, but at this point we
    ///   should not process it. One example of that is if the witness is too old.
    /// - Err if partial witness is invalid which most probably indicates malicious
    ///   behavior.
    fn validate_partial_encoded_state_witness(
        &self,
        partial_witness: &PartialEncodedStateWitness,
        signer: &ValidatorSigner,
    ) -> Result<bool, Error> {
        if !self
            .epoch_manager
            .get_shard_layout(&partial_witness.epoch_id())?
            .shard_ids()
            .contains(&partial_witness.shard_id())
        {
            return Err(Error::InvalidPartialChunkStateWitness(format!(
                "Invalid shard_id in PartialEncodedStateWitness: {}",
                partial_witness.shard_id()
            )));
        }

        // Reject witnesses for chunks for which this node isn't a validator.
        // It's an error, as chunk producer shouldn't send the witness to a non-validator node.
        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &partial_witness.epoch_id(),
            partial_witness.shard_id(),
            partial_witness.height_created(),
        )?;
        if !chunk_validator_assignments.contains(signer.validator_id()) {
            return Err(Error::NotAChunkValidator);
        }

        // The expected number of parts for the Reed Solomon encoding is the number of chunk validators.
        let num_parts = chunk_validator_assignments.len();
        if partial_witness.part_ord() >= num_parts {
            return Err(Error::InvalidPartialChunkStateWitness(format!(
                "Invalid part_ord in PartialEncodedStateWitness: {}",
                partial_witness.part_ord()
            )));
        }

        let max_part_len =
            witness_part_length(MAX_COMPRESSED_STATE_WITNESS_SIZE.as_u64() as usize, num_parts);
        if partial_witness.part_size() > max_part_len {
            return Err(Error::InvalidPartialChunkStateWitness(format!(
                "Part size {} exceed limit of {} (total parts: {})",
                partial_witness.part_size(),
                max_part_len,
                num_parts
            )));
        }

        // TODO(https://github.com/near/nearcore/issues/11301): replace these direct DB accesses with messages
        // sent to the client actor. for a draft, see https://github.com/near/nearcore/commit/e186dc7c0b467294034c60758fe555c78a31ef2d
        let head = self.store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        let final_head = self.store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;

        // Avoid processing state witness for old chunks.
        // In particular it is impossible for a chunk created at a height
        // that doesn't exceed the height of the current final block to be
        // included in the chain. This addresses both network-delayed messages
        // as well as malicious behavior of a chunk producer.
        if let Some(final_head) = final_head {
            if partial_witness.height_created() <= final_head.height {
                tracing::debug!(
                    target: "client",
                    ?partial_witness,
                    final_head_height = final_head.height,
                    "Skipping partial witness because its height created is not greater than final head height",
                );
                return Ok(false);
            }
        }
        if let Some(head) = head {
            if partial_witness.height_created() > head.height + MAX_HEIGHTS_AHEAD {
                tracing::debug!(
                    target: "client",
                    ?partial_witness,
                    head_height = head.height,
                    "Skipping partial witness because its height created is more than {} blocks ahead of head height",
                    MAX_HEIGHTS_AHEAD
                );
                return Ok(false);
            }

            // Try to find the EpochId to which this witness will belong based on its height.
            // It's not always possible to determine the exact epoch_id because the exact
            // starting height of the next epoch isn't known until it actually starts,
            // so things can get unclear around epoch boundaries.
            // Let's collect the epoch_ids in which the witness might possibly be.
            let possible_epochs = self
                .epoch_manager
                .possible_epochs_of_height_around_tip(&head, partial_witness.height_created())?;
            if !possible_epochs.contains(&partial_witness.epoch_id()) {
                tracing::debug!(
                    target: "client",
                    ?partial_witness,
                    ?possible_epochs,
                    "Skipping partial witness because its EpochId is not in the possible list of epochs",
                );
                return Ok(false);
            }
        }

        if !self.epoch_manager.verify_partial_witness_signature(&partial_witness)? {
            return Err(Error::InvalidPartialChunkStateWitness("Invalid signature".to_string()));
        }

        Ok(true)
    }

    /// Handles the state witness ack message from the chunk validator.
    /// It computes the round-trip time between sending the state witness and receiving
    /// the ack message and updates the corresponding metric with it.
    /// Currently we do not raise an error for handling of witness-ack messages,
    /// as it is used only for tracking some networking metrics.
    pub fn handle_chunk_state_witness_ack(&mut self, witness_ack: ChunkStateWitnessAck) {
        self.state_witness_tracker.on_witness_ack_received(witness_ack);
    }
}

fn compress_witness(witness: &ChunkStateWitness) -> Result<EncodedChunkStateWitness, Error> {
    let shard_id_label = witness.chunk_header.shard_id().to_string();
    let encode_timer = near_chain::stateless_validation::metrics::CHUNK_STATE_WITNESS_ENCODE_TIME
        .with_label_values(&[shard_id_label.as_str()])
        .start_timer();
    let (witness_bytes, raw_witness_size) = EncodedChunkStateWitness::encode(&witness)?;
    encode_timer.observe_duration();

    near_chain::stateless_validation::metrics::record_witness_size_metrics(
        raw_witness_size,
        witness_bytes.size_bytes(),
        witness,
    );
    Ok(witness_bytes)
}
