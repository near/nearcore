use std::sync::Arc;

use near_async::messaging::CanSend;
use near_async::time::Clock;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::stateless_validation::{
    ChunkStateWitness, ChunkStateWitnessAck, EncodedChunkStateWitness,
    SignedEncodedChunkStateWitness,
};
use near_primitives::validator_signer::ValidatorSigner;

use crate::metrics;

use super::state_witness_actor::DistributeStateWitnessRequest;
use super::state_witness_tracker::ChunkStateWitnessTracker;

pub struct StateWitnessActions {
    /// Adapter to send messages to the network.
    network_adapter: PeerManagerAdapter,
    /// Validator signer to sign the state witness.
    my_signer: Arc<dyn ValidatorSigner>,
    /// Epoch manager to get the set of chunk validators
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Tracks a collection of state witnesses sent from chunk producers to chunk validators.
    state_witness_tracker: ChunkStateWitnessTracker,
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
            state_witness_tracker: ChunkStateWitnessTracker::new(clock),
        }
    }

    pub fn handle_distribute_state_witness_request(
        &mut self,
        msg: DistributeStateWitnessRequest,
    ) -> Result<(), Error> {
        let DistributeStateWitnessRequest { state_witness } = msg;

        let signed_witness = create_signed_witness(&state_witness, self.my_signer.as_ref())?;

        let mut chunk_validators = self
            .epoch_manager
            .get_chunk_validator_assignments(
                &state_witness.epoch_id,
                state_witness.chunk_header.shard_id(),
                state_witness.chunk_header.height_created(),
            )?
            .ordered_chunk_validators();

        tracing::debug!(
            target: "stateless_validation",
            "Sending chunk state witness for chunk {:?} to chunk validators {:?}",
            state_witness.chunk_header.chunk_hash(),
            chunk_validators,
        );

        // Record the witness in order to match the incoming acks for measuring round-trip times.
        // See process_chunk_state_witness_ack for the handling of the ack messages.
        self.state_witness_tracker.record_witness_sent(
            &state_witness,
            signed_witness.witness_bytes.size_bytes(),
            chunk_validators.len(),
        );

        // Remove ourselves from the list of chunk validators. Network can't send messages to ourselves.
        chunk_validators.retain(|validator| validator != self.my_signer.validator_id());

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitness(chunk_validators, signed_witness),
        ));

        Ok(())
    }

    /// Handles the state witness ack message from the chunk validator.
    /// It computes the round-trip time between sending the state witness and receiving
    /// the ack message and updates the corresponding metric with it.
    /// Currently we do not raise an error for handling of witness-ack messages,
    /// as it is used only for tracking some networking metrics.
    pub fn handle_chunk_state_witness_ack(&mut self, witness_ack: ChunkStateWitnessAck) -> () {
        self.state_witness_tracker.on_witness_ack_received(witness_ack);
    }
}

fn create_signed_witness(
    witness: &ChunkStateWitness,
    my_signer: &dyn ValidatorSigner,
) -> Result<SignedEncodedChunkStateWitness, Error> {
    let shard_id_label = witness.chunk_header.shard_id().to_string();
    let encode_timer = metrics::CHUNK_STATE_WITNESS_ENCODE_TIME
        .with_label_values(&[shard_id_label.as_str()])
        .start_timer();
    let (witness_bytes, raw_witness_size) = EncodedChunkStateWitness::encode(&witness)?;
    encode_timer.observe_duration();
    let signed_witness = SignedEncodedChunkStateWitness {
        signature: my_signer.sign_chunk_state_witness(&witness_bytes),
        witness_bytes,
    };
    metrics::CHUNK_STATE_WITNESS_TOTAL_SIZE
        .with_label_values(&[shard_id_label.as_str()])
        .observe(signed_witness.witness_bytes.size_bytes() as f64);
    metrics::CHUNK_STATE_WITNESS_RAW_SIZE
        .with_label_values(&[shard_id_label.as_str()])
        .observe(raw_witness_size as f64);
    Ok(signed_witness)
}
