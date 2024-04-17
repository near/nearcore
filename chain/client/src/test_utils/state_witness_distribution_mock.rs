use std::sync::{Arc, Mutex};

use near_async::messaging::CanSend;

use crate::stateless_validation::state_witness_distribution_actions::StateWitnessDistributionActions;
use crate::stateless_validation::state_witness_distribution_actor::DistributeChunkStateWitnessRequest;

#[derive(Clone)]
pub struct SynchronousStateWitnessDistributionAdapter {
    actions: Arc<Mutex<StateWitnessDistributionActions>>,
}

impl SynchronousStateWitnessDistributionAdapter {
    pub fn new(actions: StateWitnessDistributionActions) -> Self {
        Self { actions: Arc::new(Mutex::new(actions)) }
    }
}

impl CanSend<DistributeChunkStateWitnessRequest> for SynchronousStateWitnessDistributionAdapter {
    fn send(&self, msg: DistributeChunkStateWitnessRequest) {
        let actions = self.actions.lock().unwrap();
        actions.handle_distribute_chunk_state_witness_request(msg).unwrap();
    }
}
