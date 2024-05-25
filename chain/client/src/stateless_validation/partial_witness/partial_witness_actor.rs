use std::sync::Arc;

use itertools::Itertools;
use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_async::time::Clock;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::state_witness::{
    ChunkStateWitnessAckMessage, PartialEncodedStateWitnessForwardMessage,
    PartialEncodedStateWitnessMessage,
};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_performance_metrics_macros::perf;
use near_primitives::reed_solomon::reed_solomon_encode;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::{
    ChunkStateWitness, ChunkStateWitnessAck, EncodedChunkStateWitness, PartialEncodedStateWitness,
};
use near_primitives::types::{AccountId, EpochId};
use near_primitives::validator_signer::ValidatorSigner;

use crate::client_actor::ClientSenderForPartialWitness;
use crate::metrics;
use crate::stateless_validation::state_witness_tracker::ChunkStateWitnessTracker;

use super::partial_witness_tracker::{PartialEncodedStateWitnessTracker, RsMap};

pub struct PartialWitnessActor {
    /// Adapter to send messages to the network.
    network_adapter: PeerManagerAdapter,
    /// Validator signer to sign the state witness.
    my_signer: Arc<dyn ValidatorSigner>,
    /// Epoch manager to get the set of chunk validators
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Tracks the parts of the state witness sent from chunk producers to chunk validators.
    partial_witness_tracker: PartialEncodedStateWitnessTracker,
    /// Tracks a collection of state witnesses sent from chunk producers to chunk validators.
    state_witness_tracker: ChunkStateWitnessTracker,
    /// Reed Solomon encoder for encoding state witness parts.
    /// We keep one wrapper for each length of chunk_validators to avoid re-creating the encoder.
    rs_map: RsMap,
}

impl Actor for PartialWitnessActor {}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeStateWitnessRequest {
    pub epoch_id: EpochId,
    pub chunk_header: ShardChunkHeader,
    pub state_witness: ChunkStateWitness,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
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
        my_signer: Arc<dyn ValidatorSigner>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        let partial_witness_tracker =
            PartialEncodedStateWitnessTracker::new(client_sender, epoch_manager.clone());
        Self {
            network_adapter,
            my_signer,
            epoch_manager,
            partial_witness_tracker,
            state_witness_tracker: ChunkStateWitnessTracker::new(clock),
            rs_map: RsMap::new(),
        }
    }

    pub fn handle_distribute_state_witness_request(
        &mut self,
        msg: DistributeStateWitnessRequest,
    ) -> Result<(), Error> {
        let DistributeStateWitnessRequest { epoch_id, chunk_header, state_witness } = msg;

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
            "distribute_chunk_state_witness",
        );

        let witness_bytes = compress_witness(&state_witness)?;

        // Record the witness in order to match the incoming acks for measuring round-trip times.
        // See process_chunk_state_witness_ack for the handling of the ack messages.
        self.state_witness_tracker.record_witness_sent(
            &state_witness,
            witness_bytes.size_bytes(),
            chunk_validators.len(),
        );

        self.send_state_witness_parts(epoch_id, chunk_header, witness_bytes, chunk_validators)?;

        Ok(())
    }

    // Function to generate the parts of the state witness and return them as a tuple of chunk_validator and part.
    fn generate_state_witness_parts(
        &mut self,
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        chunk_validators: Vec<AccountId>,
    ) -> Vec<(AccountId, PartialEncodedStateWitness)> {
        // Break the state witness into parts using Reed Solomon encoding.
        let rs = self.rs_map.entry(chunk_validators.len());

        // For the case when we are the only validator for the chunk, we don't need to do Reed Solomon encoding.
        let (parts, encoded_length) = match rs.as_ref() {
            Some(rs) => reed_solomon_encode(&rs, witness_bytes),
            None => (
                vec![Some(witness_bytes.as_slice().to_vec().into_boxed_slice())],
                witness_bytes.size_bytes(),
            ),
        };

        chunk_validators
            .iter()
            .zip_eq(parts)
            .enumerate()
            .map(|(part_ord, (chunk_validator, part))| {
                // It's fine to unwrap part here as we just constructed the parts above and we expect
                // all of them to be present.
                let partial_witness = PartialEncodedStateWitness::new(
                    epoch_id.clone(),
                    chunk_header.clone(),
                    part_ord,
                    part.unwrap().to_vec(),
                    encoded_length,
                    self.my_signer.as_ref(),
                );
                (chunk_validator.clone(), partial_witness)
            })
            .collect_vec()
    }

    // Break the state witness into parts and send each part to the corresponding chunk validator owner.
    // The chunk validator owner will then forward the part to all other chunk validators.
    // Each chunk validator would collect the parts and reconstruct the state witness.
    fn send_state_witness_parts(
        &mut self,
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        chunk_validators: Vec<AccountId>,
    ) -> Result<(), Error> {
        // Record time taken to encode the state witness parts.
        let shard_id_label = chunk_header.shard_id().to_string();
        let encode_timer = metrics::PARTIAL_WITNESS_ENCODE_TIME
            .with_label_values(&[shard_id_label.as_str()])
            .start_timer();
        let validator_witness_tuple = self.generate_state_witness_parts(
            epoch_id,
            chunk_header,
            witness_bytes,
            chunk_validators.clone(),
        );
        encode_timer.observe_duration();

        // Since we can't send network message to ourselves, we need to send the PartialEncodedStateWitnessForward
        // message for our part.
        if let Some((_, partial_witness)) = validator_witness_tuple
            .iter()
            .find(|(validator, _)| validator == self.my_signer.validator_id())
        {
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::PartialEncodedStateWitnessForward(
                    chunk_validators,
                    partial_witness.clone(),
                ),
            ));
        }

        // Send the parts to the corresponding chunk validator owners.
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple),
        ));
        Ok(())
    }

    /// Handles the state witness ack message from the chunk validator.
    /// It computes the round-trip time between sending the state witness and receiving
    /// the ack message and updates the corresponding metric with it.
    /// Currently we do not raise an error for handling of witness-ack messages,
    /// as it is used only for tracking some networking metrics.
    pub fn handle_chunk_state_witness_ack(&mut self, witness_ack: ChunkStateWitnessAck) {
        self.state_witness_tracker.on_witness_ack_received(witness_ack);
    }

    /// Function to handle receiving partial_encoded_state_witness message from chunk producer.
    pub fn handle_partial_encoded_state_witness(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_witness, "Receive PartialEncodedStateWitnessMessage");

        // Validate the partial encoded state witness.
        self.validate_partial_encoded_state_witness(&partial_witness)?;

        // Store the partial encoded state witness for self.
        self.partial_witness_tracker
            .store_partial_encoded_state_witness(partial_witness.clone())?;

        // Forward the part to all the chunk validators.
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

    /// Function to handle receiving partial_encoded_state_witness_forward message from chunk producer.
    pub fn handle_partial_encoded_state_witness_forward(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        // Validate the partial encoded state witness.
        self.validate_partial_encoded_state_witness(&partial_witness)?;

        // Store the partial encoded state witness for self.
        self.partial_witness_tracker.store_partial_encoded_state_witness(partial_witness)?;

        Ok(())
    }

    /// Function to validate the partial encoded state witness. We check the following
    /// - shard_id is valid
    /// - we are one of the validators for the chunk
    /// - part_ord is valid and within range of the number of expected parts for this chunk
    /// - partial_witness signature is valid and from the expected chunk_producer
    /// TODO(stateless_validation): Include checks from handle_orphan_state_witness in orphan_witness_handling.rs
    /// These include checks based on epoch_id validity, witness size, height_created, distance from chain head, etc.
    fn validate_partial_encoded_state_witness(
        &self,
        partial_witness: &PartialEncodedStateWitness,
    ) -> Result<(), Error> {
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
        if !chunk_validator_assignments.contains(self.my_signer.validator_id()) {
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

        if !self.epoch_manager.verify_partial_witness_signature(&partial_witness)? {
            return Err(Error::InvalidPartialChunkStateWitness("Invalid signature".to_string()));
        }

        Ok(())
    }
}

fn compress_witness(witness: &ChunkStateWitness) -> Result<EncodedChunkStateWitness, Error> {
    let shard_id_label = witness.chunk_header.shard_id().to_string();
    let encode_timer = metrics::CHUNK_STATE_WITNESS_ENCODE_TIME
        .with_label_values(&[shard_id_label.as_str()])
        .start_timer();
    let (witness_bytes, raw_witness_size) = EncodedChunkStateWitness::encode(&witness)?;
    encode_timer.observe_duration();

    metrics::record_witness_size_metrics(raw_witness_size, witness_bytes.size_bytes(), witness);
    Ok(witness_bytes)
}
