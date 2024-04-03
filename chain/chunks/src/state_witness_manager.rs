use near_primitives::stateless_validation::PartialEncodedStateWitness;

pub struct StateWitnessManager {}

impl StateWitnessManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn track_partial_encoded_state_witness(&mut self, _witness: PartialEncodedStateWitness) {
        // TODO
    }
}
