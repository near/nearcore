use std::sync::Arc;

use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::time::Clock;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::{
    ChunkStateWitness, ChunkStateWitnessAck, EncodedChunkStateWitness, PartialEncodedStateWitness,
    SignedEncodedChunkStateWitness,
};
use near_primitives::types::{AccountId, EpochId};
use near_primitives::validator_signer::ValidatorSigner;

use crate::metrics;

use super::partial_witness_tracker::{PartialEncodedStateWitnessTracker, RsMap};
use super::state_witness_actor::DistributeStateWitnessRequest;
use super::state_witness_tracker::ChunkStateWitnessTracker;

pub struct StateWitnessActions {
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

impl StateWitnessActions {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        my_signer: Arc<dyn ValidatorSigner>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self {
            network_adapter,
            my_signer,
            epoch_manager,
            partial_witness_tracker: PartialEncodedStateWitnessTracker::new(),
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
            target: "stateless_validation",
            "Sending chunk state witness for chunk {:?} to chunk validators {:?}",
            chunk_header.chunk_hash(),
            chunk_validators,
        );

        let witness_bytes = compress_witness(&state_witness)?;

        // Record the witness in order to match the incoming acks for measuring round-trip times.
        // See process_chunk_state_witness_ack for the handling of the ack messages.
        self.state_witness_tracker.record_witness_sent(
            &state_witness,
            witness_bytes.size_bytes(),
            chunk_validators.len(),
        );

        // TODO(stateless_validation): Replace with call to send_state_witness_parts after full implementation
        self.send_state_witness(witness_bytes, chunk_validators);

        Ok(())
    }

    // TODO(stateless_validation): Deprecate once we send state witness in parts.
    // This is the original way of sending out state witness where the chunk producer sends the whole witness
    // to all chunk validators.
    fn send_state_witness(
        &self,
        witness_bytes: EncodedChunkStateWitness,
        mut chunk_validators: Vec<AccountId>,
    ) {
        // Remove ourselves from the list of chunk validators. Network can't send messages to ourselves.
        chunk_validators.retain(|validator| validator != self.my_signer.validator_id());

        let signed_witness = SignedEncodedChunkStateWitness {
            signature: self.my_signer.sign_chunk_state_witness(&witness_bytes),
            witness_bytes,
        };

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitness(chunk_validators, signed_witness),
        ));
    }

    // Break the state witness into parts and send each part to the corresponding chunk validator owner.
    // The chunk validator owner will then forward the part to all other chunk validators.
    // Each chunk validator would collect the parts and reconstruct the state witness.
    #[allow(unused)]
    fn send_state_witness_parts(
        &mut self,
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        witness_bytes: EncodedChunkStateWitness,
        chunk_validators: Vec<AccountId>,
    ) -> Result<(), Error> {
        // Break the state witness into parts using Reed Solomon encoding.
        let rs = self.rs_map.entry(chunk_validators.len());
        let (parts, encoded_length) = rs.encode(witness_bytes);

        let validator_witness_tuple = chunk_validators
            .iter()
            .zip_eq(parts)
            .enumerate()
            .map(|(part_ord, (chunk_validator, part))| {
                // It's fine to unwrap part here as we just constructed the parts above and we expect
                // all of them to be present.
                let partial_witness = PartialEncodedStateWitness::new(
                    epoch_id.clone(),
                    chunk_header.clone(),
                    rs.total_parts(),
                    part_ord,
                    part.unwrap().to_vec(),
                    encoded_length,
                    self.my_signer.as_ref(),
                );
                (chunk_validator.clone(), partial_witness)
            })
            .collect_vec();

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
                partial_witness.chunk_header().shard_id(),
                partial_witness.chunk_header().height_created(),
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

    fn validate_partial_encoded_state_witness(
        &self,
        partial_witness: &PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        unimplemented!("{:?}", partial_witness)
    }
}

fn compress_witness(witness: &ChunkStateWitness) -> Result<EncodedChunkStateWitness, Error> {
    let shard_id_label = witness.chunk_header.shard_id().to_string();
    let encode_timer = metrics::CHUNK_STATE_WITNESS_ENCODE_TIME
        .with_label_values(&[shard_id_label.as_str()])
        .start_timer();
    let (witness_bytes, raw_witness_size) = EncodedChunkStateWitness::encode(&witness)?;
    encode_timer.observe_duration();

    metrics::CHUNK_STATE_WITNESS_TOTAL_SIZE
        .with_label_values(&[shard_id_label.as_str()])
        .observe(witness_bytes.size_bytes() as f64);
    metrics::CHUNK_STATE_WITNESS_RAW_SIZE
        .with_label_values(&[shard_id_label.as_str()])
        .observe(raw_witness_size as f64);
    Ok(witness_bytes)
}
